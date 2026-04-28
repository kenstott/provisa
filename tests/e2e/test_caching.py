# Copyright (c) 2026 Kenneth Stott
# Canary: 91ebd250-4fab-40c3-9579-eb9b05962e96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for query caching — headers, role partitioning, invalidation.

Tests the cache middleware pipeline functions (check_cache, store_result,
build_cache_headers) and cache key role-partitioning.
"""

from __future__ import annotations

import json

import pytest

from provisa.cache.key import cache_key
from provisa.cache.middleware import build_cache_headers, check_cache, store_result
from provisa.cache.store import CachedResult, NoopCacheStore, RedisCacheStore

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


class FakeStore:
    """In-memory cache store for testing without Redis."""

    def __init__(self):
        self._data: dict[str, tuple[bytes, dict]] = {}

    async def get(self, key: str) -> CachedResult | None:
        if key not in self._data:
            return None
        data, meta = self._data[key]
        return CachedResult(data=data, cached_at=meta["cached_at"], ttl=meta["ttl"])

    async def set(self, key: str, data: bytes, ttl: int, table_ids=None) -> None:
        import time
        self._data[key] = (data, {"cached_at": time.time(), "ttl": ttl})
        if table_ids:
            for tid in table_ids:
                tkey = f"_table:{tid}"
                if tkey not in self._data:
                    self._data[tkey] = (set(), {})
                self._data[tkey][0].add(key)

    async def invalidate_by_table(self, table_id: int) -> int:
        tkey = f"_table:{table_id}"
        if tkey not in self._data:
            return 0
        keys = self._data.pop(tkey)[0]
        count = 0
        for k in keys:
            if k in self._data:
                del self._data[k]
                count += 1
        return count

    async def invalidate_by_pattern(self, pattern: str) -> int:
        return 0

    async def close(self) -> None:
        pass


class TestCacheHeaders:
    def test_miss_header(self):
        headers = build_cache_headers(None)
        assert headers["X-Provisa-Cache"] == "MISS"
        assert "X-Provisa-Cache-Age" not in headers

    def test_hit_header(self):
        import time
        cached = CachedResult(data=b"data", cached_at=time.time() - 5, ttl=60)
        headers = build_cache_headers(cached)
        assert headers["X-Provisa-Cache"] == "HIT"
        assert int(headers["X-Provisa-Cache-Age"]) >= 5


class TestRolePartitionedCache:
    async def test_same_query_different_roles_different_keys(self):
        store = FakeStore()
        sql = "SELECT * FROM orders"

        key_analyst = cache_key(sql, [], "analyst", {})
        key_admin = cache_key(sql, [], "admin", {})
        assert key_analyst != key_admin

        await store_result(store, key_analyst, {"rows": [1]}, ttl=60)
        await store_result(store, key_admin, {"rows": [1, 2, 3]}, ttl=60)

        analyst_hit = await check_cache(store, key_analyst)
        admin_hit = await check_cache(store, key_admin)
        assert analyst_hit is not None
        assert admin_hit is not None
        assert json.loads(analyst_hit.data) == {"rows": [1]}
        assert json.loads(admin_hit.data) == {"rows": [1, 2, 3]}

    async def test_same_role_same_key_hits(self):
        store = FakeStore()
        sql = "SELECT * FROM orders"
        key = cache_key(sql, [], "analyst", {})

        miss = await check_cache(store, key)
        assert miss is None

        await store_result(store, key, {"rows": [1]}, ttl=60)

        hit = await check_cache(store, key)
        assert hit is not None
        assert hit.data == json.dumps({"rows": [1]}).encode("utf-8")


class TestMutationInvalidatesCache:
    async def test_mutation_invalidates_affected_table(self):
        store = FakeStore()
        key = cache_key("SELECT * FROM orders", [], "analyst", {})

        await store_result(store, key, {"rows": [1]}, ttl=60, table_ids={42})

        hit = await check_cache(store, key)
        assert hit is not None

        count = await store.invalidate_by_table(42)
        assert count == 1

        miss = await check_cache(store, key)
        assert miss is None

    async def test_mutation_does_not_invalidate_unrelated_table(self):
        store = FakeStore()
        key = cache_key("SELECT * FROM orders", [], "analyst", {})

        await store_result(store, key, {"rows": [1]}, ttl=60, table_ids={42})

        count = await store.invalidate_by_table(99)
        assert count == 0

        hit = await check_cache(store, key)
        assert hit is not None
