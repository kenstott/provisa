# Copyright (c) 2025 Kenneth Stott
# Canary: 28daafb0-328d-4d71-a425-672e1376ff33
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

import json
import logging

from provisa.cache.store import CacheStore, CachedResult

log = logging.getLogger(__name__)


async def check_cache(store: CacheStore, key: str) -> CachedResult | None:
    """Check for a cached result. Returns CachedResult on HIT, None on MISS."""
    return await store.get(key)


async def store_result(
    store: CacheStore,
    key: str,
    result_data: dict,
    ttl: int,
    table_ids: set[int] | None = None,
) -> None:
    """Store a query result in the cache.

    Args:
        store: The cache store.
        key: Cache key.
        result_data: Serialized query result (dict).
        ttl: Time-to-live in seconds.
        table_ids: Set of table IDs referenced by this query (for invalidation).
    """
    try:
        data = json.dumps(result_data, default=str).encode("utf-8")
        await store.set(key, data, ttl, table_ids=table_ids)
    except Exception:
        log.warning("Failed to store result in cache", exc_info=True)


def build_cache_headers(cached: CachedResult | None) -> dict[str, str]:
    """Build X-Provisa-Cache response headers.

    Returns headers dict with HIT/MISS status and age on HIT.
    """
    if cached is not None:
        return {
            "X-Provisa-Cache": "HIT",
            "X-Provisa-Cache-Age": str(cached.age_seconds),
        }
    return {"X-Provisa-Cache": "MISS"}
