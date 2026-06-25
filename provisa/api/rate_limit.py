# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Redis-backed rate limiting (REQ-369–371).

Two primitives, both backed by Redis so state is shared across stateless Provisa
instances (REQ-371) — nothing is held in-process:

- ``allow(key, limit, window_s)`` — sliding-window request counter (req/sec, NL req/min).
  Returns ``(allowed, retry_after_seconds)``.
- ``acquire(key, limit)`` / ``release(key)`` — concurrency gauge for long-lived streams
  (SSE subscriptions, Arrow Flight streams).

When no Redis URL is configured the limiter degrades to a no-op (everything allowed):
distributed rate limiting is not possible without shared state, and in-process counters
would be wrong across instances.
"""

from __future__ import annotations

# Requirements: REQ-369, REQ-370, REQ-371

import time
from typing import Any, Protocol, cast


class _RedisClient(Protocol):
    """The async Redis commands used by the limiter (redis.asyncio compatible)."""

    async def zremrangebyscore(self, key: str, min: float, max: float) -> int: ...
    async def zcard(self, key: str) -> int: ...
    async def zrange(self, key: str, start: int, end: int, *, withscores: bool = ...) -> list: ...
    async def zadd(self, key: str, mapping: dict[str, float]) -> int: ...
    async def expire(self, key: str, seconds: int) -> bool: ...
    async def incr(self, key: str) -> int: ...
    async def decr(self, key: str) -> int: ...
    async def set(self, key: str, value: Any) -> Any: ...


class RateLimiter(Protocol):
    async def allow(self, key: str, limit: int, window_s: float) -> tuple[bool, float]: ...
    async def acquire(self, key: str, limit: int) -> bool: ...
    async def release(self, key: str) -> None: ...


class NoopRateLimiter:  # REQ-371
    """Allows everything — used when Redis is not configured."""

    async def allow(self, key: str, limit: int, window_s: float) -> tuple[bool, float]:
        return True, 0.0

    async def acquire(self, key: str, limit: int) -> bool:
        return True

    async def release(self, key: str) -> None:
        return None


class RedisRateLimiter:  # REQ-369, REQ-371
    """Sliding-window + concurrency limiter over Redis.

    The redis client is injected so the implementation is testable without a server.
    """

    def __init__(self, redis: _RedisClient, *, now=time.time) -> None:
        self._redis = redis
        self._now = now

    async def allow(self, key: str, limit: int, window_s: float) -> tuple[bool, float]:
        """Sliding-window counter keyed on a Redis sorted set of request timestamps."""
        if limit is None or limit <= 0:
            return True, 0.0
        now = self._now()
        cutoff = now - window_s
        r = self._redis
        # Drop entries outside the window, count the rest.
        await r.zremrangebyscore(key, 0, cutoff)
        count = await r.zcard(key)
        if count >= limit:
            # Retry after the oldest in-window entry expires.
            oldest = await r.zrange(key, 0, 0, withscores=True)
            retry_after = window_s
            if oldest:
                _member, score = oldest[0]
                retry_after = max(0.0, (score + window_s) - now)
            return False, retry_after
        # Admit: record this request. Member is unique per call.
        await r.zadd(key, {f"{now}:{count}": now})
        await r.expire(key, int(window_s) + 1)
        return True, 0.0

    async def acquire(self, key: str, limit: int) -> bool:
        """Increment a concurrency gauge; reject (and roll back) if over the limit."""
        if limit is None or limit <= 0:
            return True
        r = self._redis
        current = await r.incr(key)
        # Safety TTL so a missed release cannot wedge the gauge permanently.
        await r.expire(key, 3600)
        if current > limit:
            await r.decr(key)
            return False
        return True

    async def release(self, key: str) -> None:
        r = self._redis
        current = await r.decr(key)
        if current < 0:
            # Never let the gauge go negative.
            await r.set(key, 0)


def build_rate_limiter(redis_url: str | None) -> RateLimiter:  # REQ-369, REQ-371
    """Construct a RedisRateLimiter, or a no-op limiter when Redis is unavailable."""
    if not redis_url:
        return NoopRateLimiter()
    try:
        import redis.asyncio as aioredis

        client = aioredis.from_url(redis_url, decode_responses=True)
        return RedisRateLimiter(cast(_RedisClient, client))
    except Exception:
        return NoopRateLimiter()
