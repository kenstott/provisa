# Copyright (c) 2025 Kenneth Stott
# Canary: 292243fd-6594-4d00-9bdc-01bdcc3e6711
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for RedisCacheStore (REQ-077).

Requires a running Redis instance (Docker).
"""

from __future__ import annotations

import asyncio
import os

import pytest

from provisa.cache.store import CachedResult, RedisCacheStore

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/1")


@pytest.fixture
async def store():
    s = RedisCacheStore(REDIS_URL)
    await s._connect()
    # Clean test keys before each test
    async for key in s._redis.scan_iter(match="provisa:*"):
        await s._redis.delete(key)
    yield s
    # Clean up after
    async for key in s._redis.scan_iter(match="provisa:*"):
        await s._redis.delete(key)
    await s.close()


class TestSetGetRoundTrip:
    async def test_set_and_get(self, store):
        await store.set("test-key-1", b'{"rows": [1,2,3]}', ttl=60)
        result = await store.get("test-key-1")
        assert result is not None
        assert isinstance(result, CachedResult)
        assert result.data == b'{"rows": [1,2,3]}'
        assert result.ttl == 60

    async def test_get_miss(self, store):
        result = await store.get("nonexistent-key")
        assert result is None


class TestTTLExpiration:
    async def test_expires_after_ttl(self, store):
        await store.set("ttl-key", b"data", ttl=1)
        result = await store.get("ttl-key")
        assert result is not None
        await asyncio.sleep(1.5)
        result = await store.get("ttl-key")
        assert result is None


class TestInvalidateByTable:
    async def test_invalidate_by_table_id(self, store):
        await store.set("q1", b"data1", ttl=60, table_ids={10})
        await store.set("q2", b"data2", ttl=60, table_ids={10, 20})
        await store.set("q3", b"data3", ttl=60, table_ids={20})

        count = await store.invalidate_by_table(10)
        assert count == 2

        assert await store.get("q1") is None
        assert await store.get("q2") is None
        assert await store.get("q3") is not None

    async def test_invalidate_by_table_no_match(self, store):
        await store.set("q1", b"data", ttl=60, table_ids={10})
        count = await store.invalidate_by_table(999)
        assert count == 0
        assert await store.get("q1") is not None


class TestInvalidateByPattern:
    async def test_invalidate_by_pattern(self, store):
        await store.set("pat-a-1", b"d1", ttl=60)
        await store.set("pat-a-2", b"d2", ttl=60)
        await store.set("pat-b-1", b"d3", ttl=60)

        count = await store.invalidate_by_pattern("pat-a-*")
        assert count >= 2

        assert await store.get("pat-a-1") is None
        assert await store.get("pat-a-2") is None
        assert await store.get("pat-b-1") is not None

    async def test_invalidate_by_pattern_no_match(self, store):
        count = await store.invalidate_by_pattern("zzz-no-match-*")
        assert count == 0
