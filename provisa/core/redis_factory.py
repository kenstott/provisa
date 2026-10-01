# Copyright (c) 2026 Kenneth Stott
# Canary: 3d21f8de-2ee3-40be-b61b-7c1a538e1ddf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Single Redis client factory with an embedded fakeredis fallback (REQ-829).

Every Redis connection in Provisa routes through :func:`make_redis`. When a URL
is configured it returns a shared sync ``redis.Redis`` client; otherwise it returns
an embedded ``fakeredis.FakeRedis`` backed by one process-wide
``fakeredis.FakeServer``. All fake clients — which differ only in
``decode_responses`` — share that single server, so they see the same in-memory
store. This lets a developer run the full backend (result cache, APQ cache, hot
tables, rate limiter, invalidation index) with zero Redis and zero Docker while
exercising the identical code paths production runs against a real Redis, rather
than the previous silent no-op fallbacks.

Tenant isolation for the embedded medium is enforced in the app layer via the
existing key namespacing (``provisa:cache:<tenant_id>:...`` etc.), not by any
store-native RLS; the single shared FakeServer is acceptable because desktop is
single-tenant.
"""

from __future__ import annotations

import threading
from collections.abc import AsyncIterator
from typing import Any

_fake_server: Any = None
_fake_lock = threading.Lock()


def _get_fake_server() -> Any:
    """Return the process-wide FakeServer, creating it on first use."""
    global _fake_server
    if _fake_server is None:
        with _fake_lock:
            if _fake_server is None:
                import fakeredis

                _fake_server = fakeredis.FakeServer()
    return _fake_server


class _AsyncPipeline:
    """Awaitable ``execute`` over a sync pipeline; command methods queue synchronously as before."""

    def __init__(self, pipe: Any) -> None:
        self._pipe = pipe

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pipe, name)

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def execute(self) -> list[Any]:
        return self._pipe.execute()


class SharedRedis:
    """One shared, thread-safe synchronous Redis client per worker, behind the async call surface.

    REQ-1882 (amended 2026-09-29): every request runs on its own thread, so a single sync
    ``redis.Redis`` (whose ``ConnectionPool`` is thread-safe) is shared by all request threads; a
    borrower waits for a free pooled connection. Each command is exposed as an awaitable purely so
    the rate limiter, result cache, APQ cache, hot tables and NL job store keep their ``await r.x()``
    call sites — the command itself runs synchronously on the calling request thread."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def client(self) -> Any:
        """The shared underlying sync client."""
        return self._client

    @property
    def embedded(self) -> bool:
        """True when this is the in-process embedded Redis (REQ-829), not a Redis server."""
        return type(self._client).__module__.startswith("fakeredis")

    def pipeline(self, *args: Any, **kwargs: Any) -> _AsyncPipeline:
        return _AsyncPipeline(self._client.pipeline(*args, **kwargs))

    # Async only to keep the ``async for`` call-site contract; each SCAN step is synchronous.
    async def scan_iter(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        for key in self._client.scan_iter(*args, **kwargs):
            yield key

    # Async only to keep the awaitable call-site contract; closes the shared pool synchronously.
    async def aclose(self) -> None:
        self._client.close()

    def __getattr__(self, name: str) -> Any:
        command = getattr(self._client, name)
        if not callable(command):
            return command

        # Async only to keep the awaitable call-site contract; the command runs synchronously here.
        async def _call(*args: Any, **kwargs: Any) -> Any:
            return command(*args, **kwargs)

        return _call


# Bounded wait for a pooled Redis connection when every one is checked out (REQ-1882: the
# (max+1)th request waits, it never fails with a pool-exhausted error).
_POOL_MAX_CONNECTIONS = 50
_POOL_WAIT_S = 20


def _deadline_bounded_pool_class() -> type:
    import redis
    from redis.exceptions import ConnectionError as RedisConnectionError

    from provisa.core import request_deadline

    class _DeadlineBoundedPool(redis.BlockingConnectionPool):
        """``BlockingConnectionPool`` whose wait is bounded by the request's remaining budget.

        redis-py reads the wait from the shared ``self.timeout``, which cannot be set per caller
        without racing other threads; a bounded semaphore sized to ``max_connections`` gates
        checkout instead, waiting ``min(_POOL_WAIT_S, remaining budget)``. Once a slot is held a
        pooled connection (or a placeholder to create one) is guaranteed, so the inner get does not
        wait."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._slots = threading.BoundedSemaphore(self.max_connections)

        def get_connection(self, *args: Any, **kwargs: Any) -> Any:
            budget = request_deadline.remaining()
            wait = _POOL_WAIT_S if budget is None else min(_POOL_WAIT_S, budget)
            if not self._slots.acquire(timeout=wait):
                raise RedisConnectionError(
                    f"no Redis connection freed within {wait:.1f}s "
                    f"(all {self.max_connections} checked out)"
                )
            try:
                return super().get_connection(*args, **kwargs)
            except BaseException:
                self._slots.release()
                raise

        def release(self, connection: Any) -> None:
            try:
                super().release(connection)
            finally:
                self._slots.release()

    return _DeadlineBoundedPool


def watch_error() -> type[Exception]:
    """The client's optimistic-transaction conflict (``redis.WatchError``), for a caller running a
    WATCH/MULTI transaction on the shared client without importing the driver itself."""
    from redis.exceptions import WatchError

    return WatchError


def make_redis(url: str | None, *, decode_responses: bool) -> SharedRedis:
    """Return the shared Redis client (sync, thread-safe, awaitable call surface).

    Args:
        url: Redis connection URL. When falsy, an embedded fakeredis client
            backed by the shared process-wide FakeServer is returned.
        decode_responses: Whether string responses are decoded to ``str``.
    """
    if url:
        import redis

        pool = _deadline_bounded_pool_class().from_url(
            url,
            decode_responses=decode_responses,
            max_connections=_POOL_MAX_CONNECTIONS,
            timeout=_POOL_WAIT_S,
        )
        return SharedRedis(redis.Redis(connection_pool=pool))

    import fakeredis

    return SharedRedis(
        fakeredis.FakeRedis(server=_get_fake_server(), decode_responses=decode_responses)
    )
