# Copyright (c) 2026 Kenneth Stott
# Canary: 47406462-b4e7-441c-9401-6d1bb00ea5ff
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cache store implementations (REQ-077).

RedisCacheStore: production Redis-backed store.
NoopCacheStore: used when Redis is not configured (caching disabled).
"""

# Requirements: REQ-173, REQ-230, REQ-231, REQ-302, REQ-303

from __future__ import annotations

import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

from provisa.otel_compat import get_tracer as _get_tracer

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)


@dataclass(frozen=True)
class CachedResult:
    """A cached query result with metadata."""

    data: bytes
    cached_at: float  # unix timestamp
    ttl: int  # seconds

    @property
    def age_seconds(self) -> int:
        return int(time.time() - self.cached_at)


class CacheStore(ABC):
    """Abstract cache store interface."""

    @abstractmethod
    async def get(self, key: str, tenant_id: str | None = None) -> CachedResult | None:
        """Get a cached result by key. Returns None on miss."""

    @abstractmethod
    async def set(
        self,
        key: str,
        data: bytes,
        ttl: int,
        tenant_id: str | None = None,
        table_ids: set[int] | None = None,
    ) -> None:
        """Store a result with TTL in seconds."""

    @abstractmethod
    async def invalidate_by_pattern(self, pattern: str, tenant_id: str | None = None) -> int:
        """Invalidate cache entries matching a key pattern. Returns count deleted."""

    @abstractmethod
    async def invalidate_by_table(self, table_id: int, tenant_id: str | None = None) -> int:
        """Invalidate cache entries for queries touching a table. Returns count deleted."""

    @abstractmethod
    async def close(self) -> None:
        """Close the store connection."""


class NoopCacheStore(CacheStore):
    """No-op store when caching is disabled. Always misses."""

    async def get(self, key: str, tenant_id: str | None = None) -> CachedResult | None:
        return None

    async def set(
        self,
        key: str,
        data: bytes,
        ttl: int,
        tenant_id: str | None = None,
        table_ids: set[int] | None = None,
    ) -> None:
        pass

    async def invalidate_by_pattern(self, pattern: str, tenant_id: str | None = None) -> int:
        return 0

    async def invalidate_by_table(self, table_id: int, tenant_id: str | None = None) -> int:
        return 0

    async def close(self) -> None:
        pass


class RedisCacheStore(CacheStore):  # REQ-230, REQ-231
    """Redis-backed cache store.

    Key format: provisa:cache:<sha256_key>
    Multi-tenant key format: provisa:cache:<tenant_id>:<sha256_key>
    Table index: provisa:table:<table_id> → set of cache keys
    Each cached entry stores: data (bytes), cached_at (float), ttl (int).

    TLS: pass a ``rediss://`` URL — redis.asyncio handles TLS automatically.
    Set ``PROVISA_REQUIRE_REDIS_TLS=true`` to reject non-TLS URLs at startup.
    """

    PREFIX = "provisa:cache:"
    TABLE_PREFIX = "provisa:table:"

    def __init__(self, redis_url: str):
        if os.environ.get("PROVISA_REQUIRE_REDIS_TLS", "").lower() == "true":
            if not redis_url.startswith("rediss://"):
                raise RuntimeError(
                    "PROVISA_REQUIRE_REDIS_TLS is set but REDIS_URL does not use rediss://"
                )
        self._redis_url = redis_url
        self._redis = None

    def _prefixed_key(self, key: str, tenant_id: str | None) -> str:
        if tenant_id is not None:
            return self.PREFIX + tenant_id + ":" + key
        return self.PREFIX + key

    def _prefixed_table_key(self, table_id: int, tenant_id: str | None) -> str:
        if tenant_id is not None:
            return self.TABLE_PREFIX + tenant_id + ":" + str(table_id)
        return self.TABLE_PREFIX + str(table_id)

    async def _connect(self):
        if self._redis is None:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(
                self._redis_url,
                decode_responses=False,
            )

    async def get(self, key: str, tenant_id: str | None = None) -> CachedResult | None:
        with _tracer.start_as_current_span("cache.get") as span:
            span.set_attribute("cache.key", key)
            try:
                await self._connect()
                assert self._redis is not None
                rkey = self._prefixed_key(key, tenant_id)
                pipe = self._redis.pipeline()
                pipe.get(rkey)
                pipe.get(rkey + ":meta")
                data, meta = await pipe.execute()
                if data is None or meta is None:
                    span.set_attribute("cache.hit", False)
                    return None
                import json

                meta_dict = json.loads(meta)
                span.set_attribute("cache.hit", True)
                return CachedResult(
                    data=data,
                    cached_at=meta_dict["cached_at"],
                    ttl=meta_dict["ttl"],
                )
            except Exception:
                log.warning("Redis get failed, treating as cache miss", exc_info=True)
                span.set_attribute("cache.hit", False)
                return None

    async def set(
        self,
        key: str,
        data: bytes,
        ttl: int,
        tenant_id: str | None = None,
        table_ids: set[int] | None = None,
    ) -> None:
        with _tracer.start_as_current_span("cache.set") as span:
            span.set_attribute("cache.key", key)
            span.set_attribute("cache.ttl", ttl)
            span.set_attribute("cache.size_bytes", len(data))
            try:
                await self._connect()
                assert self._redis is not None
                import json

                rkey = self._prefixed_key(key, tenant_id)
                meta = json.dumps({"cached_at": time.time(), "ttl": ttl}).encode()
                pipe = self._redis.pipeline()
                pipe.setex(rkey, ttl, data)
                pipe.setex(rkey + ":meta", ttl, meta)
                if table_ids:
                    for tid in table_ids:
                        tkey = self._prefixed_table_key(tid, tenant_id)
                        pipe.sadd(tkey, key)
                        pipe.expire(tkey, ttl + 60)  # slightly longer than cache TTL
                await pipe.execute()
            except Exception:
                log.warning("Redis set failed, query result not cached", exc_info=True)

    async def invalidate_by_pattern(self, pattern: str, tenant_id: str | None = None) -> int:
        try:
            await self._connect()
            assert self._redis is not None
            prefix = self._prefixed_key("", tenant_id)
            keys = []
            async for key in self._redis.scan_iter(match=prefix + pattern):
                keys.append(key)
            if keys:
                return await self._redis.delete(*keys)
            return 0
        except Exception:
            log.warning("Redis invalidate_by_pattern failed", exc_info=True)
            return 0

    async def invalidate_by_table(
        self, table_id: int, tenant_id: str | None = None
    ) -> int:  # REQ-173, REQ-231
        try:
            await self._connect()
            assert self._redis is not None
            tkey = self._prefixed_table_key(table_id, tenant_id)
            cache_keys = await self._redis.smembers(tkey)
            if not cache_keys:
                return 0
            prefix = self._prefixed_key("", tenant_id)
            pipe = self._redis.pipeline()
            for ck in cache_keys:
                ck_str = ck.decode() if isinstance(ck, bytes) else ck
                pipe.delete(prefix + ck_str)
                pipe.delete(prefix + ck_str + ":meta")
            pipe.delete(tkey)
            await pipe.execute()
            return len(cache_keys)
        except Exception:
            log.warning("Redis invalidate_by_table failed", exc_info=True)
            return 0

    async def close(self) -> None:
        if self._redis:
            await self._redis.close()
            self._redis = None
