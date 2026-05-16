# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f01234567891
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for RedisAPQCache tenant isolation."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from provisa.apq.cache import NoopAPQCache, RedisAPQCache


class TestRedisAPQCacheBuildKey:
    def _make_cache(self) -> RedisAPQCache:
        return RedisAPQCache(redis_url="redis://localhost:6379/0", ttl=3600)

    def test_build_key_no_tenant(self):
        cache = self._make_cache()
        assert cache._build_key("abc123", None) == "provisa:apq:abc123"

    def test_build_key_with_tenant(self):
        cache = self._make_cache()
        assert cache._build_key("abc123", "acme") == "provisa:apq:acme:abc123"


class TestRedisAPQCacheGet:
    def _make_cache(self) -> RedisAPQCache:
        return RedisAPQCache(redis_url="redis://localhost:6379/0", ttl=3600)

    @pytest.mark.asyncio
    async def test_get_no_tenant_uses_base_key(self):
        cache = self._make_cache()
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value="{ orders { id } }")
        cache._redis = mock_redis

        result = await cache.get("abc123", tenant_id=None)
        assert result == "{ orders { id } }"
        mock_redis.get.assert_awaited_once_with("provisa:apq:abc123")

    @pytest.mark.asyncio
    async def test_get_with_tenant_uses_tenant_key(self):
        cache = self._make_cache()
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value="{ orders { id } }")
        cache._redis = mock_redis

        result = await cache.get("abc123", tenant_id="acme")
        assert result == "{ orders { id } }"
        mock_redis.get.assert_awaited_once_with("provisa:apq:acme:abc123")

    @pytest.mark.asyncio
    async def test_different_tenants_use_different_keys(self):
        cache = self._make_cache()
        assert cache._build_key("h", "tenant-a") != cache._build_key("h", "tenant-b")
        assert cache._build_key("h", None) != cache._build_key("h", "tenant-a")


class TestRedisAPQCacheSet:
    def _make_cache(self) -> RedisAPQCache:
        return RedisAPQCache(redis_url="redis://localhost:6379/0", ttl=3600)

    @pytest.mark.asyncio
    async def test_set_no_tenant_uses_base_key(self):
        cache = self._make_cache()
        mock_redis = AsyncMock()
        cache._redis = mock_redis

        await cache.set("abc123", "{ orders { id } }", tenant_id=None)
        mock_redis.setex.assert_awaited_once_with("provisa:apq:abc123", 3600, "{ orders { id } }")

    @pytest.mark.asyncio
    async def test_set_with_tenant_uses_tenant_key(self):
        cache = self._make_cache()
        mock_redis = AsyncMock()
        cache._redis = mock_redis

        await cache.set("abc123", "{ orders { id } }", tenant_id="acme")
        mock_redis.setex.assert_awaited_once_with(
            "provisa:apq:acme:abc123", 3600, "{ orders { id } }"
        )


class TestRedisAPQCacheTlsEnforcement:
    def test_no_tls_env_allows_plain_redis(self):
        with patch.dict("os.environ", {}, clear=False):
            cache = RedisAPQCache(redis_url="redis://localhost:6379/0")
            assert cache._redis_url == "redis://localhost:6379/0"

    def test_require_tls_env_rejects_plain_redis(self):
        with patch.dict("os.environ", {"PROVISA_REQUIRE_REDIS_TLS": "true"}):
            with pytest.raises(RuntimeError, match="rediss://"):
                RedisAPQCache(redis_url="redis://localhost:6379/0")

    def test_require_tls_env_allows_rediss(self):
        with patch.dict("os.environ", {"PROVISA_REQUIRE_REDIS_TLS": "true"}):
            cache = RedisAPQCache(redis_url="rediss://localhost:6380/0")
            assert cache._redis_url.startswith("rediss://")


class TestNoopAPQCacheTenantSignature:
    @pytest.mark.asyncio
    async def test_get_accepts_tenant_id(self):
        cache = NoopAPQCache()
        assert await cache.get("h", tenant_id="t1") is None

    @pytest.mark.asyncio
    async def test_set_accepts_tenant_id(self):
        cache = NoopAPQCache()
        await cache.set("h", "q", tenant_id="t1")
