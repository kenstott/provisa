# Copyright (c) 2026 Kenneth Stott
# Canary: 3c4d5e6f-7a8b-9012-cdef-012345678903
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""APQ cache store (Phase AN).

Stores the mapping: SHA-256 hash → GraphQL query string.
Redis-backed in production; no-op fallback when Redis is not configured.

Key format: ``provisa:apq:<sha256_hex>``
TTL: 24 hours (configurable via ``PROVISA_APQ_TTL`` env var, default 86400s).
"""

from __future__ import annotations

import hashlib
import logging
import os
from abc import ABC, abstractmethod

log = logging.getLogger(__name__)

_DEFAULT_TTL = int(os.environ.get("PROVISA_APQ_TTL", "86400"))


def compute_apq_hash(query: str) -> str:
    """Return the SHA-256 hex digest of *query* (Apollo APQ format)."""
    return hashlib.sha256(query.encode()).hexdigest()


class APQCache(ABC):
    """Abstract APQ cache interface."""

    @abstractmethod
    async def get(self, sha256_hash: str) -> str | None:
        """Return the cached query string for *sha256_hash*, or None on miss."""

    @abstractmethod
    async def set(self, sha256_hash: str, query: str) -> None:
        """Store *query* under *sha256_hash*."""

    @abstractmethod
    async def close(self) -> None:
        """Release resources."""


class NoopAPQCache(APQCache):
    """No-op APQ cache — always misses. Used when Redis is not configured."""

    async def get(self, sha256_hash: str) -> str | None:
        return None

    async def set(self, sha256_hash: str, query: str) -> None:
        pass

    async def close(self) -> None:
        pass


class RedisAPQCache(APQCache):
    """Redis-backed APQ cache.

    Args:
        redis_url: Redis connection URL (e.g. ``redis://localhost:6379/0``).
        ttl: Cache TTL in seconds (default: ``PROVISA_APQ_TTL`` env var or 86400).
    """

    PREFIX = "provisa:apq:"

    def __init__(self, redis_url: str, ttl: int = _DEFAULT_TTL) -> None:
        self._redis_url = redis_url
        self._ttl = ttl
        self._redis = None

    async def _connect(self):
        if self._redis is None:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(self._redis_url, decode_responses=True)

    async def get(self, sha256_hash: str) -> str | None:
        try:
            await self._connect()
            return await self._redis.get(self.PREFIX + sha256_hash)
        except Exception:
            log.warning("APQ Redis get failed", exc_info=True)
            return None

    async def set(self, sha256_hash: str, query: str) -> None:
        try:
            await self._connect()
            await self._redis.setex(self.PREFIX + sha256_hash, self._ttl, query)
        except Exception:
            log.warning("APQ Redis set failed", exc_info=True)

    async def close(self) -> None:
        if self._redis:
            await self._redis.close()
            self._redis = None
