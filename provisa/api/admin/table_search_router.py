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

from typing import Annotated

from fastapi import APIRouter, Query, Request

from provisa.api.admin.engine_auth import run_admin_catalog_sql

from provisa.discovery.table_search import TableCandidate, search_tables
from provisa.api.admin.capabilities import require_capability_request

router = APIRouter(prefix="/admin/sources", tags=["admin", "table-search"])

# Requirements: REQ-464, REQ-2253

#: REQ-2253: the navbar filter's selection, as repeated ``domains=`` parameters (as the glossary's
#: AI assistants take it, glossary_router._view_scope). Absent is no selection.
Selected = Annotated[list[str] | None, Query()]


def _selection(request: Request, selected: list[str] | None) -> frozenset[str] | None:
    """REQ-2253: the caller's authority intersected with the selection, or ``None`` when nothing
    is selected, which leaves the search as it was."""
    from provisa.api.admin.glossary_router import _view_scope

    return None if selected is None else _view_scope(request, selected)


def _require_source_within(state, source_id: str, scope: frozenset[str]) -> None:  # REQ-2253
    """Refuse a search of a source none of whose tables may be registered in a selected domain:
    its ``allowed_domains`` (empty: every domain) and the selection share none."""
    from provisa.api.errors import ApiError

    allowed = state.source_allowed_domains.get(source_id)
    if allowed and not scope.intersection(allowed):
        raise ApiError(
            403,
            "table_search.outside_selection",
            f"source {source_id!r} is outside the domains selected in the domain filter",
            source=source_id,
        )


def _within_selection(  # REQ-2253
    state, source_id: str, schema_name: str, candidates: list[TableCandidate], scope: frozenset[str]
) -> list[TableCandidate]:
    """``candidates`` less the tables registered only in domains outside the selection, so none of
    them is ranked or sent to the model. A table not yet registered is in no domain; it stays, to be
    registered in a selected one."""
    registered: dict[str, set[str]] = {}
    for t in state.tables:
        if t["source_id"] == source_id and t["schema_name"] == schema_name:
            registered.setdefault(t["table_name"], set()).add(t["domain_id"])
    return [c for c in candidates if c.name not in registered or registered[c.name] & scope]


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
            group=c.group,
        )
        for c in cached
    ]


async def _candidates_live(
    source_id: str, schema_name: str, state
) -> list[TableCandidate]:  # REQ-464
    """Fetch candidates live from native introspection + the engine (cache-miss path)."""
    from provisa.api.admin.introspect import native_tables, require_live_attach
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
            TableCandidate(
                name=t.name, comment=t.comment, columns=[], schema_name=schema_name, group=t.group
            )
            for t in raw_tables
        ]

    # Enrich with column names (best-effort): the source's own driver first (REQ-1912); the
    # engine's catalog only for a table the driver cannot list, on a source the engine holds a
    # live attach of.
    from provisa.discovery.catalog_cache import loads_columns_lazily

    if loads_columns_lazily(source_type):
        # REQ-464: its column names are loaded on first search of an indexed schema, in one
        # statement for the schema; never one statement per table on a request.
        return candidates
    # A source registered from a specification: its tables' fields are in the specification
    # this process holds, read with no call to the source and no statement.
    from provisa.api.admin.introspect import specification_columns

    fields = await specification_columns(source_id, source_type, schema_name, state)
    for c in candidates:
        c.columns = list((fields or {}).get(c.name, []))
    return candidates


@router.get("/{source_id}/tables/search")
async def search_source_tables(
    request: Request,  # REQ-464
    source_id: str,
    q: str = Query(..., description="Natural language search query"),
    schema_name: str = Query("public", description="Schema to search within"),
    domains: Selected = None,
) -> dict:
    """Search tables in a source using NL query.

    Reads from the background-populated catalog cache when warm.
    Falls back to live the engine introspection on a cold cache.
    Two-pass ranking: token overlap pre-filter, then the table selection model (when its vendor has a key).

    The answer says what was searched: ``table_names`` is always ``complete``; ``column_names``
    is ``loading`` while the schema's column names are still being loaded (a table that
    matches only by a column may be missing: ask again), ``unavailable`` when they could not be
    loaded, else ``complete``.

    REQ-2253: with the filter's selection (the ``domains`` parameters) the search works only within
    it: a source whose tables may be registered in none of the selected domains is refused, and a
    table registered only outside them is neither ranked nor sent to the model.
    """
    require_capability_request(request, "source_registration")
    from provisa.api.app import state
    from provisa.core.org_secrets import read_org_api_keys
    from provisa.discovery.catalog_cache import (
        column_names_state,
        loads_columns_lazily,
        request_column_fill,
    )

    scope = _selection(request, domains)
    if scope is not None:
        _require_source_within(state, source_id, scope)
    source_type = state.source_types.get(source_id, "")
    # A source registered from a specification is searched over the tables its specification
    # offers now -- the list the Register Table form offers -- read with no call to the source.
    # Its cached catalog is only what an earlier walk listed: a walk before the specification
    # was held, or of a source since re-added, left the search over none of them (REQ-464).
    candidates = (
        None
        if not loads_columns_lazily(source_type)
        else await _candidates_from_cache(source_id, schema_name, state)
    )
    cache_warm = candidates is not None
    if cache_warm:
        # REQ-464: column names of a source listed through its adapter are loaded on first
        # search, off this request; the answer below ranks on what is known now.
        request_column_fill(source_id, source_type, schema_name, candidates, state)
    else:
        candidates = await _candidates_live(source_id, schema_name, state)
    if scope is not None:
        candidates = _within_selection(state, source_id, schema_name, candidates, scope)
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
