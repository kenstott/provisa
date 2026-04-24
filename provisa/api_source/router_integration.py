# Copyright (c) 2026 Kenneth Stott
# Canary: a193053c-3aee-46af-8c37-69f4366b965b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the COPYRIGHT holder.

"""Routing integration for API sources (Phase U).

Flow: check Trino cache → hit? return cache reference → miss? call API
→ flatten → materialize → schedule TTL DROP → return rows.

Phase 2 SQL (WHERE/ORDER BY/LIMIT) is applied by the caller via rewrite_from_cache().
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import logging

from provisa.api_source.cache import resolve_ttl
from provisa.api_source.caller import call_api
from provisa.api_source.flattener import flatten_response
from provisa.api_source.models import ApiEndpoint, ApiSource, ApiSourceType
from provisa.api_source.trino_cache import (
    CacheLocation,
    cache_location,
    cache_table_name,
    create_and_insert,
    schedule_drop,
    table_exists,
)

log = logging.getLogger(__name__)


@dataclass
class QueryResult:
    rows: list[dict]
    from_cache: bool
    cache_table: str | None = field(default=None)


def is_api_source(source_id: str, source_types: dict[str, str]) -> bool:
    """Check if a source_id corresponds to an API source type."""
    stype = source_types.get(source_id, "")
    return stype in {e.value for e in ApiSourceType}


async def handle_api_query(
    endpoint: ApiEndpoint,
    params: dict,
    conn,
    source: ApiSource | None = None,
    source_ttl: int | None = None,
    global_ttl: int | None = None,
    loc: CacheLocation | None = None,
) -> QueryResult:
    """Execute an API query with Trino cache.

    1. Derive stable table name from source + path + native params
    2. If table exists in Trino: return cache reference (phase 2 SQL applied by caller)
    3. On miss: call API → flatten → materialize → schedule DROP after TTL
    """
    ttl = resolve_ttl(endpoint.ttl, source_ttl, global_ttl)
    tbl = cache_table_name(endpoint.source_id, endpoint.table_name, params)

    if loc is None:
        _cc = getattr(source, "cache_catalog", None) if source else None
        _cs = getattr(source, "cache_schema", "api_cache") if source else "api_cache"
        loc = cache_location(endpoint.source_id, _cc, _cs)

    if table_exists(conn, loc, tbl, ttl=ttl):
        log.info("[API CACHE] hit — %s.%s.%s", loc.catalog, loc.schema, tbl)
        return QueryResult(rows=[], from_cache=True, cache_table=tbl)

    # Cache miss: call API
    base_url = source.base_url if source else ""
    auth = source.auth if source else None

    pages = await call_api(endpoint, params, base_url=base_url, auth=auth)

    all_rows: list[dict] = []
    for page_data in pages:
        rows = flatten_response(page_data, endpoint.response_root, endpoint.columns)
        all_rows.extend(rows)

    create_and_insert(conn, loc, tbl, all_rows, endpoint.columns)
    log.info("[API CACHE] miss — %d rows materialized → %s.%s.%s (ttl=%ds)", len(all_rows), loc.catalog, loc.schema, tbl, ttl)
    asyncio.ensure_future(schedule_drop(conn, loc, tbl, ttl))

    return QueryResult(rows=all_rows, from_cache=False, cache_table=tbl)
