# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for RedisCacheStore tenant isolation."""

from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.cache.store import CachedResult, NoopCacheStore, RedisCacheStore


class TestRedisCacheStorePrefixedKey:
    def _make_store(self) -> RedisCacheStore:
        return RedisCacheStore(redis_url="redis://localhost:6379/0")

    def test_prefixed_key_no_tenant(self):
        store = self._make_store()
        assert store._prefixed_key("abc", None) == "provisa:cache:abc"

    def test_prefixed_key_with_tenant(self):
        store = self._make_store()
        assert store._prefixed_key("abc", "tenant-1") == "provisa:cache:tenant-1:abc"

    def test_prefixed_table_key_no_tenant(self):
        store = self._make_store()
        assert store._prefixed_table_key(42, None) == "provisa:table:42"

    def test_prefixed_table_key_with_tenant(self):
        store = self._make_store()
        assert store._prefixed_table_key(42, "tenant-1") == "provisa:table:tenant-1:42"


class TestRedisCacheStoreGet:
    def _make_store(self) -> RedisCacheStore:
        store = RedisCacheStore(redis_url="redis://localhost:6379/0")
        return store

    @pytest.mark.asyncio
    async def test_get_no_tenant_uses_base_prefix(self):
        store = self._make_store()
        mock_pipe = MagicMock()
        meta = json.dumps({"cached_at": time.time(), "ttl": 60}).encode()
        mock_pipe.execute = AsyncMock(return_value=[b"data", meta])
        mock_redis = MagicMock()
        mock_redis.pipeline = MagicMock(return_value=mock_pipe)
        store._redis = mock_redis

        result = await store.get("mykey", tenant_id=None)
        assert isinstance(result, CachedResult)
        assert result.data == b"data"
        mock_pipe.get.assert_any_call("provisa:cache:mykey")
        mock_pipe.get.assert_any_call("provisa:cache:mykey:meta")

    @pytest.mark.asyncio
    async def test_get_with_tenant_uses_tenant_prefix(self):
        store = self._make_store()
        mock_pipe = MagicMock()
        meta = json.dumps({"cached_at": time.time(), "ttl": 60}).encode()
        mock_pipe.execute = AsyncMock(return_value=[b"data", meta])
        mock_redis = MagicMock()
        mock_redis.pipeline = MagicMock(return_value=mock_pipe)
        store._redis = mock_redis

        result = await store.get("mykey", tenant_id="acme")
        assert isinstance(result, CachedResult)
        assert result.data == b"data"
        mock_pipe.get.assert_any_call("provisa:cache:acme:mykey")
        mock_pipe.get.assert_any_call("provisa:cache:acme:mykey:meta")

    @pytest.mark.asyncio
    async def test_get_miss_returns_none(self):
        store = self._make_store()
        mock_pipe = MagicMock()
        mock_pipe.execute = AsyncMock(return_value=[None, None])
        mock_redis = MagicMock()
        mock_redis.pipeline = MagicMock(return_value=mock_pipe)
        store._redis = mock_redis

        result = await store.get("missing", tenant_id=None)
        assert result is None


class TestRedisCacheStoreSet:
    def _make_store(self) -> RedisCacheStore:
        return RedisCacheStore(redis_url="redis://localhost:6379/0")

    @pytest.mark.asyncio
    async def test_set_no_tenant_uses_base_prefix(self):
        store = self._make_store()
        mock_pipe = MagicMock()
        mock_pipe.execute = AsyncMock(return_value=[])
        mock_redis = MagicMock()
        mock_redis.pipeline = MagicMock(return_value=mock_pipe)
        store._redis = mock_redis

        await store.set("k", b"val", 60, tenant_id=None)
        mock_pipe.set.assert_any_call("provisa:cache:k", b"val", ex=60)
        assert mock_pipe.set.call_count >= 1

    @pytest.mark.asyncio
    async def test_set_with_tenant_uses_tenant_prefix(self):
        store = self._make_store()
        mock_pipe = MagicMock()
        mock_pipe.execute = AsyncMock(return_value=[])
        mock_redis = MagicMock()
        mock_redis.pipeline = MagicMock(return_value=mock_pipe)
        store._redis = mock_redis

        await store.set("k", b"val", 60, tenant_id="acme")
        mock_pipe.set.assert_any_call("provisa:cache:acme:k", b"val", ex=60)
        assert mock_pipe.set.call_count >= 1


class TestRedisCacheStoreInvalidateByPattern:
    def _make_store(self) -> RedisCacheStore:
        return RedisCacheStore(redis_url="redis://localhost:6379/0")

    @pytest.mark.asyncio
    async def test_invalidate_no_tenant_scans_base_prefix(self):
        store = self._make_store()
        scanned_patterns = []

        async def fake_scan_iter(match):
            scanned_patterns.append(match)
            for _ in range(0):  # pragma: no branch — empty range makes this an async generator
                yield

        mock_redis = MagicMock()
        mock_redis.scan_iter = fake_scan_iter
        store._redis = mock_redis

        await store.invalidate_by_pattern("user:*", tenant_id=None)
        assert scanned_patterns == ["provisa:cache:user:*"]

    @pytest.mark.asyncio
    async def test_invalidate_with_tenant_scans_tenant_prefix(self):
        store = self._make_store()
        scanned_patterns = []

        async def fake_scan_iter(match):
            scanned_patterns.append(match)
            for _ in range(0):  # pragma: no branch — empty range makes this an async generator
                yield

        mock_redis = MagicMock()
        mock_redis.scan_iter = fake_scan_iter
        store._redis = mock_redis

        await store.invalidate_by_pattern("user:*", tenant_id="acme")
        assert scanned_patterns == ["provisa:cache:acme:user:*"]


class TestRedisCacheStoreTlsEnforcement:
    def test_no_tls_env_allows_plain_redis(self):
        with patch.dict("os.environ", {}, clear=False):
            store = RedisCacheStore(redis_url="redis://localhost:6379/0")
            assert store._redis_url == "redis://localhost:6379/0"

    def test_require_tls_env_rejects_plain_redis(self):
        with patch.dict("os.environ", {"PROVISA_REQUIRE_REDIS_TLS": "true"}):
            with pytest.raises(RuntimeError, match="rediss://") as exc_info:
                RedisCacheStore(redis_url="redis://localhost:6379/0")
        assert isinstance(exc_info.value, RuntimeError)
        assert "rediss://" in str(exc_info.value)

    def test_require_tls_env_allows_rediss(self):
        with patch.dict("os.environ", {"PROVISA_REQUIRE_REDIS_TLS": "true"}):
            store = RedisCacheStore(redis_url="rediss://localhost:6380/0")
            assert store._redis_url.startswith("rediss://")


class TestNoopCacheStoreTenantSignature:
    @pytest.mark.asyncio
    async def test_get_accepts_tenant_id(self):
        store = NoopCacheStore()
        assert await store.get("k", tenant_id="t1") is None

    @pytest.mark.asyncio
    async def test_set_accepts_tenant_id(self):
        store = NoopCacheStore()
        result = await store.set("k", b"v", 60, tenant_id="t1")
        assert result is None

    @pytest.mark.asyncio
    async def test_invalidate_by_pattern_accepts_tenant_id(self):
        store = NoopCacheStore()
        assert await store.invalidate_by_pattern("*", tenant_id="t1") == 0

    @pytest.mark.asyncio
    async def test_invalidate_by_table_accepts_tenant_id(self):
        store = NoopCacheStore()
        assert await store.invalidate_by_table(1, tenant_id="t1") == 0
