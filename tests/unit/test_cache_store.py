# Copyright (c) 2026 Kenneth Stott
# Canary: 55cb787e-7ab8-4377-9750-37a03321a276
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
        meta = json.dumps({"cached_at": time.time(), "ttl": 60}).encode()
        mock_redis = MagicMock()
        mock_redis.mget = AsyncMock(return_value=[b"data", meta])
        store._redis = mock_redis

        result = await store.get("mykey", tenant_id=None)
        assert isinstance(result, CachedResult)
        assert result.data == b"data"
        mock_redis.mget.assert_awaited_once_with("provisa:cache:mykey", "provisa:cache:mykey:meta")

    @pytest.mark.asyncio
    async def test_get_with_tenant_uses_tenant_prefix(self):
        store = self._make_store()
        meta = json.dumps({"cached_at": time.time(), "ttl": 60}).encode()
        mock_redis = MagicMock()
        mock_redis.mget = AsyncMock(return_value=[b"data", meta])
        store._redis = mock_redis

        result = await store.get("mykey", tenant_id="acme")
        assert isinstance(result, CachedResult)
        assert result.data == b"data"
        mock_redis.mget.assert_awaited_once_with(
            "provisa:cache:acme:mykey", "provisa:cache:acme:mykey:meta"
        )

    @pytest.mark.asyncio
    async def test_get_miss_returns_none(self):
        store = self._make_store()
        mock_redis = MagicMock()
        mock_redis.mget = AsyncMock(return_value=[None, None])
        store._redis = mock_redis

        result = await store.get("missing", tenant_id=None)
        assert result is None


class TestRedisCacheStoreGetCommands:
    """A response-cache hit is one Redis command (an MGET of the entry and its meta); a miss is
    the same one command; what each returns is unchanged."""

    @staticmethod
    def _store() -> RedisCacheStore:
        from provisa.core.redis_factory import make_redis

        store = RedisCacheStore(redis_url=None)
        store._redis = make_redis(None, decode_responses=False)
        return store

    @staticmethod
    def _record(store: RedisCacheStore) -> list[tuple]:
        """Every command ``store`` sends from now on; a pipeline fails the test."""
        assert store._redis is not None
        client = store._redis.client()
        sent: list[tuple] = []
        execute_command = client.execute_command

        def recording(*args, **kwargs):
            sent.append(args)
            return execute_command(*args, **kwargs)

        def no_pipeline(*_args, **_kwargs):
            raise AssertionError("a cache read sends no pipeline")

        client.execute_command = recording
        client.pipeline = no_pipeline
        return sent

    @pytest.mark.asyncio
    async def test_a_hit_is_one_command_and_returns_the_entry(self):
        store = self._store()
        await store.set("hitkey", b"rows", ttl=60)
        sent = self._record(store)

        result = await store.get("hitkey", tenant_id="acme")
        assert result is None  # another tenant's key: a miss
        result = await store.get("hitkey", tenant_id=None)
        assert isinstance(result, CachedResult)
        assert result.data == b"rows" and result.ttl == 60
        assert sent == [
            ("MGET", "provisa:cache:acme:hitkey", "provisa:cache:acme:hitkey:meta"),
            ("MGET", "provisa:cache:hitkey", "provisa:cache:hitkey:meta"),
        ]

    @pytest.mark.asyncio
    async def test_a_miss_is_one_command_and_returns_none(self):
        store = self._store()
        sent = self._record(store)
        assert await store.get("never-set", tenant_id=None) is None
        assert sent == [("MGET", "provisa:cache:never-set", "provisa:cache:never-set:meta")]


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


class TestRedisCacheStoreTableEntryCounts:
    def _make_store(self) -> RedisCacheStore:
        return RedisCacheStore(redis_url="redis://localhost:6379/0")

    @pytest.mark.asyncio
    async def test_counts_are_one_places_summed_over_its_models_by_table_id(self):
        """REQ-595: the index is read for one org's one environment — the scan pattern names
        it — and its counts are summed over the models that place has held."""
        store = self._make_store()

        async def fake_scan_iter(match):
            assert match == "provisa:table:acme:m*"
            for k in (
                b"provisa:table:acme:m7:1",
                b"provisa:table:acme:m8:1",
                b"provisa:table:acme:m8:2",
            ):
                yield k

        mock_pipe = MagicMock()
        mock_pipe.execute = AsyncMock(return_value=[3, 2, 5])
        mock_redis = MagicMock()
        mock_redis.scan_iter = fake_scan_iter
        mock_redis.pipeline = MagicMock(return_value=mock_pipe)
        store._redis = mock_redis

        counts = await store.table_entry_counts("acme")
        assert counts == {1: 5, 2: 5}

    @pytest.mark.asyncio
    async def test_empty_when_no_index_keys(self):
        store = self._make_store()

        async def fake_scan_iter(match):
            assert match == "provisa:table:acme:m*"
            for _ in range(0):  # pragma: no branch — empty async generator
                yield

        mock_redis = MagicMock()
        mock_redis.scan_iter = fake_scan_iter
        store._redis = mock_redis

        assert await store.table_entry_counts("acme") == {}

    @pytest.mark.asyncio
    async def test_noop_store_returns_empty(self):
        assert await NoopCacheStore().table_entry_counts("acme") == {}


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
