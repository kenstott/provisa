# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a1c2e-9b4d-4e7a-8c1f-5d2b7e9a0c14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882 (amended 2026-09-29): every HTTP and WebSocket request runs on its own request thread,
whatever its path; only lifespan (not a request) stays on the front loop."""

# Requirements: REQ-1882

from __future__ import annotations

import threading

import pytest
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient

from provisa.core.request_thread import RequestThreadMiddleware


def _app(seen: dict[str, tuple[int, str]]) -> FastAPI:
    app = FastAPI()

    @app.get("/admin/anything")
    async def _admin() -> dict[str, bool]:
        seen["admin"] = (threading.get_ident(), threading.current_thread().name)
        return {"ok": True}

    @app.get("/health")
    async def _health() -> dict[str, bool]:
        seen["health"] = (threading.get_ident(), threading.current_thread().name)
        return {"ok": True}

    @app.websocket("/ws")
    async def _ws(ws: WebSocket) -> None:
        await ws.accept()
        seen["ws"] = (threading.get_ident(), threading.current_thread().name)
        await ws.send_text("hi")
        await ws.close()

    app.add_middleware(RequestThreadMiddleware)
    return app


@pytest.fixture
def client_and_front():
    seen: dict[str, tuple[int, str]] = {}
    front: dict[str, int] = {}
    app = _app(seen)

    with TestClient(app) as client:
        # TestClient runs the ASGI app on its portal thread; capture that thread's ident.
        front["ident"] = client.portal.call(lambda: threading.get_ident())  # type: ignore[union-attr]
        yield client, seen, front["ident"]


def test_http_requests_on_every_path_run_on_a_request_thread(client_and_front) -> None:
    client, seen, front_ident = client_and_front
    assert client.get("/admin/anything").status_code == 200
    assert client.get("/health").status_code == 200
    for key in ("admin", "health"):
        ident, name = seen[key]
        assert ident != front_ident
        assert name == "provisa-request"


def test_websocket_runs_on_a_request_thread(client_and_front) -> None:
    client, seen, front_ident = client_and_front
    with client.websocket_connect("/ws") as ws:
        assert ws.receive_text() == "hi"
    ident, name = seen["ws"]
    assert ident != front_ident
    assert name == "provisa-request"
