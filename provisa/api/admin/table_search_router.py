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

from fastapi import APIRouter, Query

from provisa.discovery.table_search import TableCandidate, search_tables

router = APIRouter(prefix="/admin/sources", tags=["admin", "table-search"])

# Requirements: REQ-464


async def _candidates_from_cache(  # REQ-464
    source_id: str, schema_name: str, pool
) -> list[TableCandidate] | None:
    """Return TableCandidates from the cache, or None if cache is cold."""
    from provisa.discovery.catalog_cache import read_cache

    cached = await read_cache(pool, source_id, schema_name)
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
    """Fetch candidates live from native introspection + Trino (cache-miss path)."""
    from provisa.api.admin.introspect import native_tables
    from provisa.api.admin.schema import _get_pool, source_to_catalog

    source_type = state.source_types.get(source_id, "")
    pool = await _get_pool()
    async with pool.acquire() as config_conn:
        try:
            raw_tables = await native_tables(
                source_id,
                source_type,
                schema_name,
                state.source_pools,
                config_conn,
                state,
            )
        except Exception:
            raw_tables = None

    if raw_tables is None:
        catalog = source_to_catalog(source_id)
        try:
            res = await state.federation_engine.execute_engine(
                f'SELECT table_name FROM "{catalog}".information_schema.tables '
                f"WHERE table_schema = '{schema_name}' "
                f"AND table_type = 'BASE TABLE' ORDER BY table_name"
            )
            raw_tables_list = [row[0] for row in res.rows]
        except Exception:
            raw_tables_list = []
        candidates = [
            TableCandidate(name=t, comment=None, columns=[], schema_name=schema_name)
            for t in raw_tables_list
        ]
    else:
        candidates = [
            TableCandidate(name=t.name, comment=t.comment, columns=[], schema_name=schema_name)
            for t in raw_tables
        ]

    # Enrich with column names (best-effort)
    from provisa.api.admin.schema import source_to_catalog

    catalog = source_to_catalog(source_id)
    for c in candidates:
        try:
            res = await state.federation_engine.execute_engine(
                f'SELECT column_name FROM "{catalog}".information_schema.columns '
                f"WHERE table_schema = '{schema_name}' AND table_name = '{c.name}' "
                f"ORDER BY ordinal_position"
            )
            c.columns = [row[0] for row in res.rows]
        except Exception:
            pass

    return candidates


@router.get("/{source_id}/tables/search")
async def search_source_tables(  # REQ-464
    source_id: str,
    q: str = Query(..., description="Natural language search query"),
    schema_name: str = Query("public", description="Schema to search within"),
) -> list[dict]:
    """Search tables in a source using NL query.

    Reads from the background-populated catalog cache when warm.
    Falls back to live Trino introspection on a cold cache.
    Two-pass ranking: token overlap pre-filter, then haiku LLM (if ANTHROPIC_API_KEY set).
    """
    from provisa.api.app import state
    from provisa.api.admin.schema import _get_pool

    pool = await _get_pool()
    candidates = await _candidates_from_cache(source_id, schema_name, pool)
    cache_warm = candidates is not None

    if not cache_warm:
        candidates = await _candidates_live(source_id, schema_name, state)

    ranked = await search_tables(q, candidates)
    return [
        {
            "schema_name": r.schema_name,
            "table_name": r.name,
            "comment": r.comment,
            "confidence": r.confidence,
            "reasoning": r.reasoning,
            "cache_warm": cache_warm,
        }
        for r in ranked
    ]
