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
    assert type(r).__name__ in ("FakeRedis", "FakeAsyncRedis")


def test_empty_url_returns_fake_client():
    assert type(make_redis("", decode_responses=False)).__name__ in ("FakeRedis", "FakeAsyncRedis")


def test_real_url_returns_asyncio_redis_client():
    # No connection is made at construction — just verify the real client type.
    r = make_redis("redis://localhost:6379/0", decode_responses=True)
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
