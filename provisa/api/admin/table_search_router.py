# Copyright (c) 2026 Kenneth Stott
# Canary: c4d5e6f7-a8b9-0123-cdef-456789012345
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the COPYRIGHT holder.

"""Admin REST endpoint: NL-assisted table search within a source (REQ-464)."""

from __future__ import annotations

from fastapi import APIRouter, Query, Request

from provisa.api.admin.engine_auth import run_admin_catalog_sql

from provisa.discovery.table_search import TableCandidate, search_tables
from provisa.api.admin.capabilities import require_capability_request

router = APIRouter(prefix="/admin/sources", tags=["admin", "table-search"])

# Requirements: REQ-464


async def _candidates_from_cache(  # REQ-464
    source_id: str, schema_name: str, state
) -> list[TableCandidate] | None:
    """Return TableCandidates from the cache, or None if cache is cold. The cache is a state
    table, read through the org's state store."""
    from provisa.discovery.catalog_cache import read_cache

    cached = await read_cache(state.tenant_db, source_id, schema_name)
    if cached is None:
        return None
    return [
        TableCandidate(
            name=c.table_name,
            comment=c.comment,
            columns=c.column_names,
            schema_name=c.schema_name,
        )
        for c in cached
    ]


async def _candidates_live(
    source_id: str, schema_name: str, state
) -> list[TableCandidate]:  # REQ-464
    """Fetch candidates live from native introspection + the engine (cache-miss path)."""
    from provisa.api.admin.introspect import native_columns, native_tables, require_live_attach
    from provisa.api.admin.schema import _get_pool
    from provisa.api.admin.discovery_resilience import discovery_fallback

    source_type = state.source_types.get(source_id, "")
    pool = await _get_pool()
    raw_tables = None
    async with pool.acquire() as config_conn:
        with discovery_fallback(f"native tables for {source_id!r}.{schema_name}"):
            raw_tables = await native_tables(
                source_id,
                source_type,
                schema_name,
                state.source_pools,
                config_conn,
                state,
            )

    attached: list[str] | None = None
    if raw_tables is None:
        # A source the engine attaches is listed through the attach seam, as the Register Table
        # form and the background index list it: no engine catalog is named after it.
        from provisa.discovery.catalog_cache import _SeamSource

        attached = await _SeamSource(state.federation_engine, source_id).tables(schema_name)
    if attached is not None:
        candidates = [
            TableCandidate(name=t, comment=None, columns=[], schema_name=schema_name)
            for t in attached
        ]
    elif raw_tables is None:
        # REQ-1912: the engine lists only a source it holds a live attach of.
        await require_live_attach(state, source_id, "tables")
        catalog = state.catalog_for(source_id)
        raw_tables_list: list[str] = []
        # Through the sanctioned discovery boundary, not a bare swallow: on a shard that has just
        # scaled from zero this read is the one that answers CATALOG_NOT_FOUND, and swallowing it in
        # place rendered the search as "this source has no tables" with nothing in the log.
        with discovery_fallback(f"engine tables for {source_id!r}.{schema_name}"):
            res = await run_admin_catalog_sql(
                state,
                state.federation_engine,
                f'SELECT table_name FROM "{catalog}".information_schema.tables '
                f"WHERE table_schema = '{schema_name}' "
                f"AND table_type = 'BASE TABLE' ORDER BY table_name",
                "table search",
            )
            raw_tables_list = [row[0] for row in res.rows]
        candidates = [
            TableCandidate(name=t, comment=None, columns=[], schema_name=schema_name)
            for t in raw_tables_list
        ]
    else:
        candidates = [
            TableCandidate(name=t.name, comment=t.comment, columns=[], schema_name=schema_name)
            for t in raw_tables
        ]

    # Enrich with column names (best-effort): the source's own driver first (REQ-1912); the
    # engine's catalog only for a table the driver cannot list, on a source the engine holds a
    # live attach of.
    from provisa.api.admin.introspect import unattached_source
    from provisa.discovery.catalog_cache import loads_columns_lazily

    if loads_columns_lazily(source_type):
        # REQ-464: its column names are loaded on first search of an indexed schema, in one
        # statement for the schema; never one statement per table on a request.
        return candidates
    engine_lists = await unattached_source(state, source_id) is None
    for c in candidates:
        native = None
        async with pool.acquire() as config_conn:
            with discovery_fallback(f"native columns for {source_id!r}.{schema_name}.{c.name}"):
                native = await native_columns(
                    source_id, source_type, schema_name, c.name, state.source_pools, config_conn
                )
        if native is not None:
            c.columns = [name for name, _dtype in native]
            continue
        if not engine_lists:
            continue  # no driver listing and no engine catalog: the names stay unenriched
        catalog = state.catalog_for(source_id)
        with discovery_fallback(f"engine columns for {source_id!r}.{schema_name}.{c.name}"):
            res = await run_admin_catalog_sql(
                state,
                state.federation_engine,
                f'SELECT column_name FROM "{catalog}".information_schema.columns '
                f"WHERE table_schema = '{schema_name}' AND table_name = '{c.name}' "
                f"ORDER BY ordinal_position",
                "table search",
            )
            c.columns = [row[0] for row in res.rows]

    return candidates


@router.get("/{source_id}/tables/search")
async def search_source_tables(
    request: Request,  # REQ-464
    source_id: str,
    q: str = Query(..., description="Natural language search query"),
    schema_name: str = Query("public", description="Schema to search within"),
) -> dict:
    """Search tables in a source using NL query.

    Reads from the background-populated catalog cache when warm.
    Falls back to live the engine introspection on a cold cache.
    Two-pass ranking: token overlap pre-filter, then haiku LLM (if ANTHROPIC_API_KEY set).

    The answer says what was searched: ``table_names`` is always ``complete``; ``column_names``
    is ``loading`` while the schema's column names are still being loaded (a table that
    matches only by a column may be missing: ask again), ``unavailable`` when they could not be
    loaded, else ``complete``.
    """
    require_capability_request(request, "source_registration")
    from provisa.api.app import state
    from provisa.core.org_secrets import read_org_api_keys
    from provisa.discovery.catalog_cache import column_names_state, request_column_fill

    source_type = state.source_types.get(source_id, "")
    candidates = await _candidates_from_cache(source_id, schema_name, state)
    cache_warm = candidates is not None
    if cache_warm:
        # REQ-464: column names of a source listed through its adapter are loaded on first
        # search, off this request; the answer below ranks on what is known now.
        request_column_fill(source_id, source_type, schema_name, candidates, state)
    else:
        candidates = await _candidates_live(source_id, schema_name, state)
    columns = column_names_state(source_id, source_type, schema_name, candidates)

    assert state.model_db is not None
    api_keys = await read_org_api_keys(state.model_db)
    ranked = await search_tables(q, candidates, api_keys=api_keys)
    return {
        "table_names": "complete",
        "column_names": columns,
        "candidates": [
            {
                "schema_name": r.schema_name,
                "table_name": r.name,
                "comment": r.comment,
                "confidence": r.confidence,
                "reasoning": r.reasoning,
                "cache_warm": cache_warm,
            }
            for r in ranked
        ],
    }
