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

Key format (single-tenant): ``provisa:apq:<sha256_hex>``
Key format (multi-tenant):  ``provisa:apq:<tenant_id>:<sha256_hex>``
TTL: 24 hours (configurable via ``PROVISA_APQ_TTL`` env var, default 86400s).

TLS: pass a ``rediss://`` URL — redis.asyncio handles TLS automatically.
Set ``PROVISA_REQUIRE_REDIS_TLS=true`` to reject non-TLS URLs at startup.
"""

# Requirements: REQ-288, REQ-289, REQ-290, REQ-291

from __future__ import annotations

import hashlib
import logging
import os
from abc import ABC, abstractmethod

log = logging.getLogger(__name__)

_DEFAULT_TTL = int(os.environ.get("PROVISA_APQ_TTL", "86400"))


def compute_apq_hash(query: str) -> str:  # REQ-288, REQ-291
    """Return the SHA-256 hex digest of *query* (Apollo APQ format)."""
    return hashlib.sha256(query.encode()).hexdigest()


class APQCache(ABC):  # REQ-288, REQ-289, REQ-290, REQ-291
    """Abstract APQ cache interface."""

    @abstractmethod
    async def get(self, sha256_hash: str, tenant_id: str | None = None) -> str | None:
        """Return the cached query string for *sha256_hash*, or None on miss."""

    @abstractmethod
    async def set(self, sha256_hash: str, query: str, tenant_id: str | None = None) -> None:
        """Store *query* under *sha256_hash*."""

    @abstractmethod
    async def close(self) -> None:
        """Release resources."""


class NoopAPQCache(APQCache):  # REQ-288, REQ-289
    """No-op APQ cache — always misses. Used when Redis is not configured."""

    async def get(self, sha256_hash: str, tenant_id: str | None = None) -> str | None:  # pyright: ignore[reportUnusedParameter]
        return None

    async def set(self, sha256_hash: str, query: str, tenant_id: str | None = None) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    async def close(self) -> None:
        pass


class RedisAPQCache(APQCache):  # REQ-288, REQ-289, REQ-290, REQ-291
    """Redis-backed APQ cache.

    Args:
        redis_url: Redis connection URL (e.g. ``redis://localhost:6379/0``).
        ttl: Cache TTL in seconds (default: ``PROVISA_APQ_TTL`` env var or 86400).
    """

    PREFIX = "provisa:apq:"

    def __init__(self, redis_url: str | None = None, ttl: int = _DEFAULT_TTL) -> None:  # REQ-829
        if redis_url and os.environ.get("PROVISA_REQUIRE_REDIS_TLS", "").lower() == "true":
            if not redis_url.startswith("rediss://"):
                raise RuntimeError(
                    "PROVISA_REQUIRE_REDIS_TLS is set but REDIS_URL does not use rediss://"
                )
        self._redis_url = redis_url
        self._ttl = ttl
        self._redis = None

    def _build_key(self, sha256_hash: str, tenant_id: str | None) -> str:
        if tenant_id is not None:
            return self.PREFIX + tenant_id + ":" + sha256_hash
        return self.PREFIX + sha256_hash

    async def _connect(self):
        if self._redis is None:
            from provisa.core.redis_factory import make_redis  # REQ-829

            self._redis = make_redis(self._redis_url, decode_responses=True)

    async def get(self, sha256_hash: str, tenant_id: str | None = None) -> str | None:
        try:
            await self._connect()
            assert self._redis is not None
            result = await self._redis.get(self._build_key(sha256_hash, tenant_id))
            return result.decode() if isinstance(result, bytes) else result
        except Exception:
            log.warning("APQ Redis get failed", exc_info=True)
            return None

    async def set(self, sha256_hash: str, query: str, tenant_id: str | None = None) -> None:
        try:
            await self._connect()
            assert self._redis is not None
            await self._redis.set(self._build_key(sha256_hash, tenant_id), query, ex=self._ttl)
        except Exception:
            log.warning("APQ Redis set failed", exc_info=True)

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None
