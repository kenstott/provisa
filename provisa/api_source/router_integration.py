# Copyright (c) 2025 Kenneth Stott
# Canary: a193053c-3aee-46af-8c37-69f4366b965b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Routing integration for API sources (Phase U).

Flow: check cache -> hit? return cached rows -> miss? call API -> flatten -> write cache -> return.
"""

from __future__ import annotations

from dataclasses import dataclass

import asyncpg

from provisa.api_source.cache import check_cache, resolve_ttl, write_cache
from provisa.api_source.caller import call_api
from provisa.api_source.flattener import flatten_response
from provisa.api_source.models import ApiEndpoint, ApiSource, ApiSourceType


@dataclass
class QueryResult:
    rows: list[dict]
    from_cache: bool


def is_api_source(source_id: str, source_types: dict[str, str]) -> bool:
    """Check if a source_id corresponds to an API source type."""
    stype = source_types.get(source_id, "")
    return stype in {e.value for e in ApiSourceType}


async def handle_api_query(
    endpoint: ApiEndpoint,
    params: dict,
    conn: asyncpg.Connection,
    source: ApiSource | None = None,
    source_ttl: int | None = None,
    global_ttl: int | None = None,
) -> QueryResult:
    """Execute an API query with caching.

    1. Check cache (if endpoint has an ID)
    2. On hit: return cached rows
    3. On miss: call API -> flatten response -> write cache -> return
    """
    ttl = resolve_ttl(endpoint.ttl, source_ttl, global_ttl)

    # Check cache
    if endpoint.id is not None:
        cached = await check_cache(conn, endpoint, params, ttl)
        if cached is not None:
            return QueryResult(rows=cached, from_cache=True)

    # Cache miss: call API
    base_url = source.base_url if source else ""
    auth = source.auth if source else None

    pages = await call_api(endpoint, params, base_url=base_url, auth=auth)

    # Flatten all pages
    all_rows: list[dict] = []
    for page_data in pages:
        rows = flatten_response(page_data, endpoint.response_root, endpoint.columns)
        all_rows.extend(rows)

    # Write to cache
    if endpoint.id is not None:
        await write_cache(conn, endpoint, params, all_rows, ttl)

    return QueryResult(rows=all_rows, from_cache=False)
