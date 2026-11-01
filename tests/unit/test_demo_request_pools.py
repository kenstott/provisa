# Copyright (c) 2026 Kenneth Stott
# Canary: 8c1f6a3e-5d27-4b90-a1e4-3f7e2b90d614
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Opening demos takes no thread a visitor's query or a status probe needs (REQ-2006, REQ-1882).

A worker has four data request threads. On 2026-11-01 four opens held all four for minutes, each
tour warm-up queued behind them for the same four, and a status probe, a /sandboxes/limits and a
visitor's query were each refused server.busy after 60 s."""

# Requirements: REQ-2006, REQ-1882

from __future__ import annotations

import asyncio
import threading

import pytest

from provisa.core.request_thread import (
    WARMING,
    RequestThreadMiddleware,
    RequestThreadPool,
)


def _pools() -> dict[str, RequestThreadPool]:
    return {
        name: RequestThreadPool(max_threads=threads, max_streams=2, wait_s=1.0)
        for name, threads in (("data", 2), ("control", 1), ("open", 1), ("warm", 1))
    }


def _middleware(pools: dict[str, RequestThreadPool]) -> RequestThreadMiddleware:
    async def _app(scope, receive, send):  # pragma: no cover - only its pool is asked
        raise AssertionError("not called")

    return RequestThreadMiddleware(
        _app,
        pool=pools["data"],
        control_pool=pools["control"],
        open_pool=pools["open"],
        warm_pool=pools["warm"],
    )


@pytest.mark.parametrize(
    ("method", "path", "pool"),
    [
        ("GET", "/public/demos/weblogs/status", "control"),
        ("GET", "/sandboxes/limits", "control"),
        ("POST", "/sandboxes/closing", "control"),
        ("POST", "/public/demos/weblogs", "open"),
        ("POST", "/sandboxes/weblogs", "open"),
        ("POST", "/data/sql", "data"),
        ("GET", "/data/rest/openapi.json", "data"),
    ],
)
def test_each_demo_request_has_the_pool_it_cannot_starve(method, path, pool):
    pools = _pools()
    chosen = _middleware(pools)._pool_for({"method": method, "path": path})
    assert chosen is pools[pool]


def test_a_tour_warm_up_runs_on_its_own_pool():
    pools = _pools()
    token = WARMING.set(True)
    try:
        chosen = _middleware(pools)._pool_for({"method": "POST", "path": "/data/sql"})
    finally:
        WARMING.reset(token)
    assert chosen is pools["warm"]


def test_opens_and_warm_ups_filling_their_pools_leave_a_query_its_thread():
    """Through the middleware, as the tier serves them: while every open and warm-up thread is held,
    a visitor's query and a status probe are served at once."""
    pools = _pools()
    release = threading.Event()
    started = {"open": threading.Event(), "warm": threading.Event()}

    async def _app(scope, receive, send):
        which = scope["path"].rsplit("/", 1)[-1]
        if which in started:
            started[which].set()
            await asyncio.to_thread(release.wait, 5)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    middleware = RequestThreadMiddleware(
        _app,
        pool=pools["data"],
        control_pool=pools["control"],
        open_pool=pools["open"],
        warm_pool=pools["warm"],
    )

    async def _request(method: str, path: str) -> int:
        sent: list[dict] = []

        async def _receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def _send(message):
            sent.append(message)

        scope = {"type": "http", "method": method, "path": path, "headers": []}
        await middleware(scope, _receive, _send)
        return sent[0]["status"]

    async def _warmed() -> int:
        WARMING.set(True)
        return await _request("POST", "/data/warm")

    async def _run() -> list[int]:
        held = [
            asyncio.create_task(_request("POST", "/public/demos/open")),
            asyncio.create_task(_warmed()),
        ]
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, started["open"].wait, 5)
        await loop.run_in_executor(None, started["warm"].wait, 5)
        served = await asyncio.wait_for(
            asyncio.gather(
                _request("POST", "/data/sql"),
                _request("GET", "/public/demos/weblogs/status"),
            ),
            timeout=0.9,  # under the pools' 1 s wait: served, not refused
        )
        release.set()
        return [*served, *await asyncio.gather(*held)]

    assert asyncio.run(_run()) == [200, 200, 200, 200]
