# Copyright (c) 2026 Kenneth Stott
# Canary: 4c6e8a0b-2d4f-4618-9c1a-3e5b7d9f0a2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-829: pluggable Redis medium with an embedded fakeredis fallback.

No REDIS_URL → an embedded fakeredis client backed by one shared FakeServer, with
the full command surface the app uses (get/set, sorted sets, sets, incr). A real
URL returns a redis.asyncio client. No network — the fake path needs no infra.
"""

from __future__ import annotations

import pytest

from provisa.core.redis_factory import make_redis


def test_no_url_returns_fake_client():
    r = make_redis(None, decode_responses=True)
    assert type(r.client()).__name__ in ("FakeRedis", "FakeAsyncRedis")


def test_empty_url_returns_fake_client():
    assert type(make_redis("", decode_responses=False).client()).__name__ in (
        "FakeRedis",
        "FakeAsyncRedis",
    )


def test_real_url_returns_asyncio_redis_client():
    # No connection is made at construction — just verify the real client type.
    r = make_redis("redis://localhost:6379/0", decode_responses=True).client()
    assert type(r).__name__ not in ("FakeRedis", "FakeAsyncRedis")
    assert r.__class__.__module__.startswith("redis")


@pytest.mark.asyncio
async def test_fake_supports_full_command_surface():
    r = make_redis(None, decode_responses=True)
    # get/set
    await r.set("k", "v")
    assert await r.get("k") == "v"
    # sorted sets (sliding-window rate limiter)
    await r.zadd("z", {"a": 1.0, "b": 2.0})
    assert await r.zcard("z") == 2
    await r.zremrangebyscore("z", 0, 1)
    assert await r.zcard("z") == 1
    # sets (invalidation index)
    await r.sadd("s", "m1", "m2")
    assert await r.smembers("s") == {"m1", "m2"}
    # counters (concurrency gauge)
    assert await r.incr("c") == 1
    assert await r.decr("c") == 0


@pytest.mark.asyncio
async def test_fake_clients_share_one_server():
    # Clients that differ in decode_responses must see the same in-memory store.
    a = make_redis(None, decode_responses=True)
    b = make_redis(None, decode_responses=False)
    await a.set("shared", "yes")
    assert await b.get("shared") == b"yes"


@pytest.mark.asyncio
async def test_fake_supports_pipeline():
    r = make_redis(None, decode_responses=True)
    pipe = r.pipeline()
    pipe.set("p", "1")
    pipe.get("p")
    results = await pipe.execute()
    assert results[-1] == "1"


def test_request_threads_share_one_client():
    """REQ-1882 (amended 2026-09-29): Redis is a shared resource — two concurrently-live request
    threads (each on its own connection loop) use the SAME underlying client and store."""
    import threading

    from provisa.core.connection_loop import connection_loop, run_on_connection_loop

    r = make_redis(None, decode_responses=True)
    both_live = threading.Barrier(2)
    clients: list[object] = []

    async def _use() -> object:
        await r.set("shared-client", "shared")
        assert await r.get("shared-client") == "shared"
        return r.client()

    def _worker() -> None:
        with connection_loop():
            both_live.wait(timeout=10)
            clients.append(run_on_connection_loop(_use()))

    threads = [threading.Thread(target=_worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(clients) == 2
    assert clients[0] is clients[1]


def test_real_redis_pool_waits_when_exhausted(monkeypatch):
    """The (max+1)th borrower of the real-Redis pool waits for a free connection, then succeeds."""
    import threading

    import redis.connection

    import provisa.core.redis_factory as rf

    # No server: checkout exercises only the pool's slot accounting, never the socket.
    monkeypatch.setattr(redis.connection.AbstractConnection, "connect", lambda self: None)
    monkeypatch.setattr(redis.connection.AbstractConnection, "can_read", lambda self, *a: False)
    monkeypatch.setattr(rf, "_POOL_MAX_CONNECTIONS", 3)

    r = make_redis("redis://localhost:6379/0", decode_responses=True)
    pool = r.client().connection_pool
    assert pool.max_connections == 3
    held = [pool.get_connection() for _ in range(pool.max_connections)]

    got: list[object] = []
    t = threading.Thread(target=lambda: got.append(pool.get_connection()))
    t.start()
    t.join(0.3)
    assert t.is_alive() and got == []  # waiting, not failed
    pool.release(held.pop())
    t.join(5)
    assert not t.is_alive() and len(got) == 1
    for c in held + got:
        pool.release(c)


def test_real_redis_pool_wait_is_bounded_by_the_request_budget(monkeypatch, deadline_clock):
    """REQ-1882: an exhausted pool's wait ends at the request's remaining budget, not the fixed cap."""
    import time

    import pytest
    import redis.connection
    from redis.exceptions import ConnectionError as RedisConnectionError

    import provisa.core.redis_factory as rf
    from provisa.core import request_deadline

    monkeypatch.setattr(redis.connection.AbstractConnection, "connect", lambda self: None)
    monkeypatch.setattr(redis.connection.AbstractConnection, "can_read", lambda self, *a: False)
    monkeypatch.setattr(rf, "_POOL_MAX_CONNECTIONS", 1)

    pool = make_redis("redis://localhost:6379/0", decode_responses=True).client().connection_pool
    held = pool.get_connection()
    t0 = time.monotonic()
    with request_deadline.within(0.3), pytest.raises(RedisConnectionError, match="no Redis"):
        pool.get_connection()
    assert time.monotonic() - t0 < 2.0  # the fixed 20s cap did not apply
    pool.release(held)
    pool.release(pool.get_connection())  # slot accounting intact after the timeout
