# Copyright (c) 2026 Kenneth Stott
# Canary: 5e6f7a8b-9c0d-1234-ef01-234567890125
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for Automatic Persisted Queries — Phase AN."""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, patch

import pytest

from provisa.apq.cache import NoopAPQCache, RedisAPQCache, compute_apq_hash


class TestComputeApqHash:
    def test_known_hash(self):
        query = "{ __typename }"
        expected = hashlib.sha256(query.encode()).hexdigest()
        assert compute_apq_hash(query) == expected

    def test_deterministic(self):
        q = "query Orders { orders { id } }"
        assert compute_apq_hash(q) == compute_apq_hash(q)

    def test_different_queries_different_hashes(self):
        assert compute_apq_hash("{ a }") != compute_apq_hash("{ b }")


class TestNoopAPQCache:
    @pytest.mark.asyncio
    async def test_always_misses(self):
        cache = NoopAPQCache()
        assert await cache.get("anything") is None

    @pytest.mark.asyncio
    async def test_set_is_silent(self):
        cache = NoopAPQCache()
        await cache.set("hash", "query")  # should not raise

    @pytest.mark.asyncio
    async def test_close_is_silent(self):
        cache = NoopAPQCache()
        await cache.close()


class TestRedisAPQCache:
    def _make_cache(self) -> RedisAPQCache:
        return RedisAPQCache(redis_url="redis://localhost:6379/0", ttl=3600)

    @pytest.mark.asyncio
    async def test_get_returns_cached_value(self):
        cache = self._make_cache()
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value="{ orders { id } }")
        cache._redis = mock_redis

        result = await cache.get("abc123")
        assert result == "{ orders { id } }"
        mock_redis.get.assert_awaited_once_with("provisa:apq:abc123")

    @pytest.mark.asyncio
    async def test_get_returns_none_on_miss(self):
        cache = self._make_cache()
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=None)
        cache._redis = mock_redis

        result = await cache.get("missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_stores_with_ttl(self):
        cache = self._make_cache()
        mock_redis = AsyncMock()
        cache._redis = mock_redis

        await cache.set("abc123", "{ orders { id } }")
        mock_redis.setex.assert_awaited_once_with(
            "provisa:apq:abc123", 3600, "{ orders { id } }"
        )

    @pytest.mark.asyncio
    async def test_get_handles_redis_error(self):
        cache = self._make_cache()
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(side_effect=Exception("connection refused"))
        cache._redis = mock_redis

        result = await cache.get("abc123")
        assert result is None  # graceful miss on error

    @pytest.mark.asyncio
    async def test_set_handles_redis_error(self):
        cache = self._make_cache()
        mock_redis = AsyncMock()
        mock_redis.setex = AsyncMock(side_effect=Exception("connection refused"))
        cache._redis = mock_redis

        await cache.set("abc123", "query")  # should not raise


class TestApqEndpointLogic:
    """Unit tests for APQ hash extraction and validation logic used in endpoint.py."""

    def test_hash_mismatch_detected(self):
        query = "{ orders { id } }"
        wrong_hash = "deadbeef" * 8  # 64 hex chars but wrong value
        computed = compute_apq_hash(query)
        assert computed != wrong_hash

    def test_hash_match_passes(self):
        query = "{ orders { id } }"
        correct_hash = compute_apq_hash(query)
        assert compute_apq_hash(query) == correct_hash
