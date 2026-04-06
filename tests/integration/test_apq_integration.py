# Copyright (c) 2026 Kenneth Stott
# Canary: c3d4e5f6-a7b8-9012-cdef-012345678902
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Automatic Persisted Queries (APQ) — Phase AN.

Tests are split into two tiers:

  1. Logic-only — exercises compute_apq_hash, NoopAPQCache, and the
     RedisAPQCache interface contract.  No Redis connection required.

  2. Redis-backed — exercises RedisAPQCache against a live Redis instance.
     These tests FAIL when Redis is not reachable.

The cache key format tested here matches:
    ``provisa:apq:<sha256_hex>``
as documented in provisa/apq/cache.py.
"""

from __future__ import annotations

import hashlib
import os

import pytest

from provisa.apq.cache import (
    APQCache,
    NoopAPQCache,
    RedisAPQCache,
    compute_apq_hash,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

_TEST_QUERY = "{ orders { id amount region } }"
_TEST_HASH = compute_apq_hash(_TEST_QUERY)


# ---------------------------------------------------------------------------
# compute_apq_hash tests (no infrastructure required)
# ---------------------------------------------------------------------------


class TestComputeApqHash:
    """Verify the hash function matches the Apollo APQ spec (SHA-256 hex)."""

    async def test_hash_is_sha256_hex(self):
        query = "{ orders { id } }"
        expected = hashlib.sha256(query.encode()).hexdigest()
        assert compute_apq_hash(query) == expected

    async def test_hash_length_is_64_chars(self):
        assert len(compute_apq_hash(_TEST_QUERY)) == 64

    async def test_hash_is_lowercase_hex(self):
        h = compute_apq_hash(_TEST_QUERY)
        assert h == h.lower()
        assert all(c in "0123456789abcdef" for c in h)

    async def test_different_queries_produce_different_hashes(self):
        h1 = compute_apq_hash("{ orders { id } }")
        h2 = compute_apq_hash("{ customers { id } }")
        assert h1 != h2

    async def test_identical_queries_produce_identical_hashes(self):
        query = "{ orders { id amount } }"
        assert compute_apq_hash(query) == compute_apq_hash(query)

    async def test_empty_string_hashes_deterministically(self):
        h = compute_apq_hash("")
        expected = hashlib.sha256(b"").hexdigest()
        assert h == expected


# ---------------------------------------------------------------------------
# APQ cache key format test (no infrastructure required)
# ---------------------------------------------------------------------------


class TestApqCacheKeyFormat:
    """Verify the Redis key format follows the documented convention."""

    async def test_apq_cache_key_format(self):
        """Redis key follows the provisa:apq:<sha256_hex> convention."""
        prefix = RedisAPQCache.PREFIX
        assert prefix == "provisa:apq:"

        h = compute_apq_hash(_TEST_QUERY)
        full_key = prefix + h
        assert full_key.startswith("provisa:apq:")
        assert full_key.endswith(h)
        assert len(full_key) == len("provisa:apq:") + 64


# ---------------------------------------------------------------------------
# NoopAPQCache tests (no infrastructure required)
# ---------------------------------------------------------------------------


class TestNoopApqCache:
    """Verify the no-op cache always misses and never raises."""

    async def test_noop_get_returns_none(self):
        cache = NoopAPQCache()
        result = await cache.get(_TEST_HASH)
        assert result is None

    async def test_noop_set_does_nothing(self):
        cache = NoopAPQCache()
        await cache.set(_TEST_HASH, _TEST_QUERY)  # should not raise

    async def test_noop_close_does_nothing(self):
        cache = NoopAPQCache()
        await cache.close()  # should not raise

    async def test_noop_get_after_set_still_misses(self):
        """NoopAPQCache simulates the PersistedQueryNotFound scenario."""
        cache = NoopAPQCache()
        await cache.set(_TEST_HASH, _TEST_QUERY)
        result = await cache.get(_TEST_HASH)
        assert result is None

    async def test_noop_is_subclass_of_apqcache(self):
        assert issubclass(NoopAPQCache, APQCache)

    async def test_noop_apq_unknown_hash_returns_not_found(self):
        """Sending only a hash to a NoopAPQCache returns None (PersistedQueryNotFound)."""
        cache = NoopAPQCache()
        unknown_hash = "a" * 64
        result = await cache.get(unknown_hash)
        assert result is None


# ---------------------------------------------------------------------------
# APQ governance / production mode simulation (no Redis required)
# ---------------------------------------------------------------------------


class TestApqGovernanceSimulation:
    """Simulate governance rules applied on top of the cache layer."""

    async def _build_cache_with_allowlist(self, allowed: set[str]) -> APQCache:
        """Wrap a NoopAPQCache to only permit allow-listed hashes."""

        class GovernedCache(APQCache):
            def __init__(self, inner: APQCache, allow: set[str]) -> None:
                self._inner = inner
                self._allow = allow

            async def get(self, sha256_hash: str) -> str | None:
                if sha256_hash not in self._allow:
                    return None
                return await self._inner.get(sha256_hash)

            async def set(self, sha256_hash: str, query: str) -> None:
                if sha256_hash not in self._allow:
                    raise PermissionError(
                        f"Hash {sha256_hash!r} not in approved allowlist"
                    )
                await self._inner.set(sha256_hash, query)

            async def close(self) -> None:
                await self._inner.close()

        return GovernedCache(NoopAPQCache(), allowed)

    async def test_apq_governance_rejects_unregistered(self):
        """Unregistered hash in governed mode raises PermissionError."""
        cache = await self._build_cache_with_allowlist(set())
        unregistered_hash = compute_apq_hash("{ unregistered { query } }")
        with pytest.raises(PermissionError):
            await cache.set(unregistered_hash, "{ unregistered { query } }")

    async def test_apq_governance_allows_registered(self):
        """Registered hash in governed mode does not raise."""
        registered_query = "{ orders { id } }"
        registered_hash = compute_apq_hash(registered_query)
        cache = await self._build_cache_with_allowlist({registered_hash})
        # set should not raise for the allowlisted hash
        await cache.set(registered_hash, registered_query)

    async def test_apq_governance_get_returns_none_for_unregistered(self):
        """get() returns None for an unregistered hash even when hash is valid."""
        cache = await self._build_cache_with_allowlist(set())
        result = await cache.get("b" * 64)
        assert result is None


# ---------------------------------------------------------------------------
# RedisAPQCache integration tests (require live Redis)
# ---------------------------------------------------------------------------


class TestRedisApqCache:
    """Live Redis tests — require a reachable Redis instance."""

    async def _make_cache(self, ttl: int = 60) -> RedisAPQCache:
        cache = RedisAPQCache(_REDIS_URL, ttl=ttl)
        return cache

    async def test_apq_store_on_full_query(self):
        """Sending hash + full query body stores it in Redis."""
        cache = await self._make_cache()
        try:
            await cache.set(_TEST_HASH, _TEST_QUERY)
            stored = await cache.get(_TEST_HASH)
            assert stored == _TEST_QUERY
        finally:
            await cache.close()

    async def test_apq_unknown_hash_returns_not_found(self):
        """Sending only an unknown hash returns None (PersistedQueryNotFound)."""
        cache = await self._make_cache()
        try:
            unknown = "f" * 64
            result = await cache.get(unknown)
            assert result is None
        finally:
            await cache.close()

    async def test_apq_retrieve_on_second_request(self):
        """Second request with hash only retrieves the previously stored query."""
        cache = await self._make_cache()
        try:
            # First: store
            await cache.set(_TEST_HASH, _TEST_QUERY)
            # Second: retrieve
            result = await cache.get(_TEST_HASH)
            assert result == _TEST_QUERY
        finally:
            await cache.close()

    async def test_apq_overwrite_existing_hash(self):
        """Re-storing the same hash with different content updates the cache."""
        cache = await self._make_cache()
        query_v2 = "{ orders { id amount region customer_id } }"
        try:
            await cache.set(_TEST_HASH, _TEST_QUERY)
            await cache.set(_TEST_HASH, query_v2)
            result = await cache.get(_TEST_HASH)
            assert result == query_v2
        finally:
            await cache.close()

    async def test_apq_cache_key_format_in_redis(self):
        """The actual Redis key matches the provisa:apq:<hash> format."""
        import redis.asyncio as aioredis

        cache = await self._make_cache(ttl=30)
        try:
            await cache.set(_TEST_HASH, _TEST_QUERY)
            raw = aioredis.from_url(_REDIS_URL, decode_responses=True)
            expected_key = f"provisa:apq:{_TEST_HASH}"
            val = await raw.get(expected_key)
            assert val == _TEST_QUERY
            await raw.close()
        finally:
            await cache.close()

    async def test_apq_ttl_applied(self):
        """TTL is applied — key exists immediately after set (TTL > 0)."""
        import redis.asyncio as aioredis

        cache = await self._make_cache(ttl=120)
        try:
            await cache.set(_TEST_HASH, _TEST_QUERY)
            raw = aioredis.from_url(_REDIS_URL, decode_responses=True)
            ttl = await raw.ttl(f"provisa:apq:{_TEST_HASH}")
            assert ttl > 0
            await raw.close()
        finally:
            await cache.close()

    async def test_redis_close_cleans_connection(self):
        """close() sets the internal _redis attribute to None."""
        cache = await self._make_cache()
        await cache._connect()
        assert cache._redis is not None
        await cache.close()
        assert cache._redis is None
