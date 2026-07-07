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

Flow: check the engine cache → hit? return cache reference → miss? call API
→ flatten → materialize → schedule TTL DROP → return rows.

Phase 2 SQL (WHERE/ORDER BY/LIMIT) is applied by the caller via rewrite_from_cache().
"""

# Requirements: REQ-119, REQ-295, REQ-297, REQ-298, REQ-299, REQ-318

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import logging

from provisa.api_source.cache import resolve_ttl
from provisa.api_source.caller import call_api
from provisa.api_source.flattener import flatten_response
from provisa.api_source.models import ApiEndpoint, ApiSource, ApiSourceType
from provisa.api_source.engine_cache import (
    CacheLocation,
    cache_location,
    cache_table_name,
    create_and_insert,
    schedule_drop,
    table_exists,
)
from provisa.otel_compat import get_tracer as _get_tracer

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)


@dataclass
class QueryResult:  # REQ-318
    rows: list[dict]
    from_cache: bool
    cache_table: str | None = field(default=None)


async def _apply_cache_promotions(
    loc: CacheLocation, tbl: str, endpoint: ApiEndpoint
) -> None:  # REQ-119
    """Run JSONB→generated-column promotion DDL on the PG-backed api-cache table (REQ-119)."""
    from provisa.api.app import state
    from provisa.api_source.promotions import apply_promotions

    target = f'{loc.schema}."{tbl}"'
    assert state.tenant_db is not None
    async with state.tenant_db.acquire() as pgc:
        await apply_promotions(pgc, target, endpoint.promotions, cast_source=True)


def is_api_source(source_id: str, source_types: dict[str, str]) -> bool:  # REQ-295, REQ-297
    """Check if a source_id corresponds to an API source type."""
    stype = source_types.get(source_id, "")
    return stype in {e.value for e in ApiSourceType}


async def handle_api_query(  # REQ-119, REQ-295, REQ-297, REQ-298, REQ-299, REQ-318, REQ-698
    endpoint: ApiEndpoint,
    params: dict,
    engine,
    source: ApiSource | None = None,
    source_ttl: int | None = None,
    global_ttl: int | None = None,
    loc: CacheLocation | None = None,
    org_id: str = "default",
) -> QueryResult:
    """Execute an API query with the engine cache.

    1. Derive stable table name from source + path + native params
    2. If table exists in the engine: return cache reference (phase 2 SQL applied by caller)
    3. On miss: call API → flatten → materialize → schedule DROP after TTL
    """
    with _tracer.start_as_current_span("api_source.handle_api_query") as span:
        span.set_attribute("api_source.source_id", endpoint.source_id)
        span.set_attribute("api_source.table", endpoint.table_name)

        ttl = resolve_ttl(endpoint.ttl, source_ttl, global_ttl)
        tbl = cache_table_name(endpoint.source_id, endpoint.table_name, params)

        if loc is None:
            _cc = getattr(source, "cache_catalog", None) if source else None
            _default_cs = f"org_{org_id}_api_cache"
            _cs = getattr(source, "cache_schema", _default_cs) if source else _default_cs
            loc = cache_location(endpoint.source_id, _cc, _cs, engine=engine)

        with engine.isolated_sync() as _c:
            _hit = table_exists(_c, loc, tbl, ttl=ttl)
        if _hit:
            log.info("[API CACHE] hit — %s.%s.%s", loc.catalog, loc.schema, tbl)
            span.set_attribute("api_source.cache_hit", True)
            return QueryResult(rows=[], from_cache=True, cache_table=tbl)

        span.set_attribute("api_source.cache_hit", False)

        # Cache miss: call API
        base_url = source.base_url if source else ""
        auth = source.auth if source else None

        pages = await call_api(endpoint, params, base_url=base_url, auth=auth)

        all_rows: list[dict] = []
        for page_data in pages:
            rows = flatten_response(
                page_data, endpoint.response_root, endpoint.columns, endpoint.response_normalizer
            )
            all_rows.extend(rows)

        with engine.isolated_sync() as _c:
            create_and_insert(_c, loc, tbl, all_rows, endpoint.columns)
        log.info(
            "[API CACHE] miss — %d rows materialized → %s.%s.%s (ttl=%ds)",
            len(all_rows),
            loc.catalog,
            loc.schema,
            tbl,
            ttl,
        )
        span.set_attribute("api_source.rows_materialized", len(all_rows))

        # REQ-119: promote JSONB fields to generated columns on the (PG-backed) cache table.
        # The cache stores JSON as varchar, so cast the source column to jsonb. Iceberg
        # tables have no PG generated columns and are skipped.
        if endpoint.promotions and loc.backend != "iceberg":
            await _apply_cache_promotions(loc, tbl, endpoint)

        asyncio.ensure_future(schedule_drop(engine, loc, tbl, ttl))

        return QueryResult(rows=all_rows, from_cache=False, cache_table=tbl)
