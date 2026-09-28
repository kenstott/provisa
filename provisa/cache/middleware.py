# Copyright (c) 2026 Kenneth Stott
# Canary: 5d65e9dc-49ef-4456-b9b1-c3f176070cf7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cache check/store functions called from the query endpoint (REQ-077).

Not FastAPI middleware — these are pipeline functions that need query context.
"""

from __future__ import annotations

import logging

from provisa.cache.codec import decode_cache_payload, encode_cache_payload
from provisa.cache.store import CacheStore, CachedResult

log = logging.getLogger(__name__)

# Requirements: REQ-536


async def check_cache(  # REQ-544
    store: CacheStore, key: str, org_id: str | None = None
) -> CachedResult | None:
    """Check for a cached result. Returns CachedResult on HIT, None on MISS."""
    return await store.get(key, tenant_id=org_id)


async def store_result(  # REQ-544, REQ-1896
    store: CacheStore,
    key: str,
    result_data: dict,
    ttl: int,
    table_ids: set[int] | None = None,
    org_id: str | None = None,
    column_types: list[str] | None = None,
) -> None:
    """Store a query result in the cache.

    Args:
        store: The cache store.
        key: Cache key.
        result_data: Serialized query result (dict).
        ttl: Time-to-live in seconds.
        table_ids: Set of table IDs referenced by this query (for invalidation).
        org_id: Tenant/org identifier for key prefixing (REQ-595).
        column_types: The result's real column types (REQ-1896), stored alongside so any
            transport sharing this cache can rebuild its own native wire shape from a hit
            without a lossy round trip through JSON.
    """
    try:
        data = encode_cache_payload({"data": result_data, "column_types": column_types})
        await store.set(key, data, ttl, tenant_id=org_id, table_ids=table_ids)
    except Exception:
        log.warning("Failed to store result in cache", exc_info=True)


def decode_cached_result(cached: CachedResult) -> tuple[dict, list[str] | None]:  # REQ-1896
    """Decode a cache HIT written by :func:`store_result` back to ``(result_data, column_types)``."""
    payload = decode_cache_payload(cached.data)
    return payload.get("data", {}), payload.get("column_types")


def build_cache_headers(cached: CachedResult | None) -> dict[str, str]:  # REQ-536
    """Build X-Provisa-Cache response headers.

    Returns headers dict with HIT/MISS status and age on HIT.
    """
    if cached is not None:
        return {
            "X-Provisa-Cache": "HIT",
            "X-Provisa-Cache-Age": str(cached.age_seconds),
        }
    return {"X-Provisa-Cache": "MISS"}
