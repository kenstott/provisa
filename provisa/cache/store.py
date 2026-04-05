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

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

log = logging.getLogger(__name__)


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
    async def get(self, key: str) -> CachedResult | None:
        """Get a cached result by key. Returns None on miss."""

    @abstractmethod
    async def set(self, key: str, data: bytes, ttl: int) -> None:
        """Store a result with TTL in seconds."""

    @abstractmethod
    async def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching a key pattern. Returns count deleted."""

    @abstractmethod
    async def invalidate_by_table(self, table_id: int) -> int:
        """Invalidate cache entries for queries touching a table. Returns count deleted."""

    @abstractmethod
    async def close(self) -> None:
        """Close the store connection."""


class NoopCacheStore(CacheStore):
    """No-op store when caching is disabled. Always misses."""

    async def get(self, key: str) -> CachedResult | None:
        return None

    async def set(self, key: str, data: bytes, ttl: int) -> None:
        pass

    async def invalidate_by_pattern(self, pattern: str) -> int:
        return 0

    async def invalidate_by_table(self, table_id: int) -> int:
        return 0

    async def close(self) -> None:
        pass


class RedisCacheStore(CacheStore):
    """Redis-backed cache store.

    Key format: provisa:cache:<sha256_key>
    Table index: provisa:table:<table_id> → set of cache keys
    Each cached entry stores: data (bytes), cached_at (float), ttl (int).
    """

    PREFIX = "provisa:cache:"
    TABLE_PREFIX = "provisa:table:"

    def __init__(self, redis_url: str):
        self._redis_url = redis_url
        self._redis = None

    async def _connect(self):
        if self._redis is None:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(
                self._redis_url, decode_responses=False,
            )

    async def get(self, key: str) -> CachedResult | None:
        try:
            await self._connect()
            rkey = self.PREFIX + key
            pipe = self._redis.pipeline()
            pipe.get(rkey)
            pipe.get(rkey + ":meta")
            data, meta = await pipe.execute()
            if data is None or meta is None:
                return None
            import json
            meta_dict = json.loads(meta)
            return CachedResult(
                data=data,
                cached_at=meta_dict["cached_at"],
                ttl=meta_dict["ttl"],
            )
        except Exception:
            log.warning("Redis get failed, treating as cache miss", exc_info=True)
            return None

    async def set(self, key: str, data: bytes, ttl: int, table_ids: set[int] | None = None) -> None:
        try:
            await self._connect()
            import json
            rkey = self.PREFIX + key
            meta = json.dumps({"cached_at": time.time(), "ttl": ttl}).encode()
            pipe = self._redis.pipeline()
            pipe.setex(rkey, ttl, data)
            pipe.setex(rkey + ":meta", ttl, meta)
            # Track which tables this cache entry covers (for invalidation)
            if table_ids:
                for tid in table_ids:
                    tkey = self.TABLE_PREFIX + str(tid)
                    pipe.sadd(tkey, key)
                    pipe.expire(tkey, ttl + 60)  # slightly longer than cache TTL
            await pipe.execute()
        except Exception:
            log.warning("Redis set failed, query result not cached", exc_info=True)

    async def invalidate_by_pattern(self, pattern: str) -> int:
        try:
            await self._connect()
            keys = []
            async for key in self._redis.scan_iter(match=self.PREFIX + pattern):
                keys.append(key)
            if keys:
                return await self._redis.delete(*keys)
            return 0
        except Exception:
            log.warning("Redis invalidate_by_pattern failed", exc_info=True)
            return 0

    async def invalidate_by_table(self, table_id: int) -> int:
        try:
            await self._connect()
            tkey = self.TABLE_PREFIX + str(table_id)
            cache_keys = await self._redis.smembers(tkey)
            if not cache_keys:
                return 0
            # Delete each cached result
            pipe = self._redis.pipeline()
            for ck in cache_keys:
                ck_str = ck.decode() if isinstance(ck, bytes) else ck
                pipe.delete(self.PREFIX + ck_str)
                pipe.delete(self.PREFIX + ck_str + ":meta")
            pipe.delete(tkey)
            results = await pipe.execute()
            return len(cache_keys)
        except Exception:
            log.warning("Redis invalidate_by_table failed", exc_info=True)
            return 0

    async def close(self) -> None:
        if self._redis:
            await self._redis.close()
            self._redis = None
