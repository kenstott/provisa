# Copyright (c) 2026 Kenneth Stott
# Canary: e1c94a37-5d08-4b62-8f7e-2a6b0d3c9f15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each worker serves HTTP on its own SO_REUSEPORT socket on the public port (REQ-1900).

uvicorn's workers share one accept socket and connections pile onto a few of them. A socket per
worker lets the kernel spread connections, as it already does for pgwire, gRPC, Bolt and MCP."""

# Requirements: REQ-1900

from __future__ import annotations

import asyncio
import http.client
import json
import threading
import time

import pytest

from provisa.api import http_listener
from provisa.api.http_listener import WorkerHttpListener, configured_address
from tests.port_lease import lease_port


def _app(name: str, delay: float = 0.0):
    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        await asyncio.sleep(delay)
        body = json.dumps({"server": name, "client": scope["client"][0]}).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", str(len(body)).encode())],
            }
        )
        await send({"type": "http.response.body", "body": body})

    return app


class _Worker:
    """A worker process's event loop, on a thread, with its HTTP listener started on it."""

    def __init__(self, name: str, port: int, delay: float = 0.0) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self._thread.start()
        self.listener = self._call(self._start(name, port, delay))

    async def _start(self, name, port, delay):
        listener = WorkerHttpListener(_app(name, delay), "127.0.0.1", port)
        listener.start()
        return listener

    def _call(self, coro, timeout: float = 30.0):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def stop(self) -> None:
        self._call(self.listener.stop())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=10)


def _get(port: int, headers: dict | None = None) -> dict:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        conn.request("GET", "/", headers=headers or {})
        resp = conn.getresponse()
        assert resp.status == 200
        return json.loads(resp.read())
    finally:
        conn.close()


@pytest.fixture
def workers():
    started: list[_Worker] = []

    def _start(name: str, port: int, delay: float = 0.0) -> _Worker:
        w = _Worker(name, port, delay)
        started.append(w)
        return w

    yield _start
    for w in started:
        if w._thread.is_alive():
            w.stop()


def test_a_worker_serves_the_app_on_its_own_listener(workers):
    port = lease_port()
    workers("w1", port)
    assert _get(port)["server"] == "w1"


def test_two_workers_listen_on_the_same_public_port(workers):
    port = lease_port()
    workers("w1", port)
    workers("w2", port)  # no "address already in use"
    served = {_get(port)["server"] for _ in range(20)}
    # Which listener the kernel hands a connection to is the kernel's choice (Linux spreads
    # them; darwin gives them all to one), but every request is answered by one of the two.
    assert served and served <= {"w1", "w2"}


def test_proxy_headers_are_honoured_from_a_trusted_proxy(workers):
    port = lease_port()
    workers("w1", port)
    assert _get(port, {"X-Forwarded-For": "203.0.113.9"})["client"] == "203.0.113.9"


def test_stopping_lets_an_in_flight_request_finish_and_then_refuses_new_ones(workers):
    port = lease_port()
    worker = workers("w1", port, delay=0.5)
    answers: list[dict] = []
    in_flight = threading.Thread(target=lambda: answers.append(_get(port)))
    in_flight.start()
    time.sleep(0.15)  # the request is being served
    worker.stop()
    in_flight.join(timeout=10)
    assert answers and answers[0]["server"] == "w1"
    with pytest.raises(OSError):
        _get(port)


def test_the_launcher_names_the_public_address(monkeypatch):
    monkeypatch.delenv("PROVISA_HTTP_LISTEN", raising=False)
    assert configured_address() is None  # uvicorn's own socket is the public one
    monkeypatch.setenv("PROVISA_HTTP_LISTEN", "0.0.0.0:8001")
    assert configured_address() == ("0.0.0.0", 8001)


def test_the_listener_does_not_run_the_apps_lifespan_a_second_time():
    port = lease_port()
    listener = WorkerHttpListener(_app("w1"), "127.0.0.1", port)
    try:
        assert listener._server.config.lifespan == "off"
        assert listener._server.config.proxy_headers is True
    finally:
        listener._sock.close()


def test_the_module_needs_only_uvicorn():
    """It is loaded by path, without the application, to measure the connection spread in a
    Linux container (tests/integration/http_listener_spread_check.py)."""
    import ast
    import inspect

    imported = {
        (n.module if isinstance(n, ast.ImportFrom) else a.name).split(".")[0]
        for n in ast.walk(ast.parse(inspect.getsource(http_listener)))
        if isinstance(n, ast.Import | ast.ImportFrom)
        for a in n.names
    }
    assert "provisa" not in imported
