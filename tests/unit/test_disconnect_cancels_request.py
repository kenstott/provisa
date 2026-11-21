# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2f8a1c-4e93-4b07-9c5e-0a7b3d1f6e28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A client that disconnects from a running data request ends it on the engine (REQ-1905, REQ-1882).

The request runs on its own thread; the accepting loop hears the client leave (the server's
``http.disconnect``) and ends the request's deadline, which cancels the statement in flight
through the cancel its engine registered -- the same cancel the deadline's expiry fires -- once.
A request that answers first is left alone, and a cancelled request ends quietly."""

# Requirements: REQ-1905, REQ-1882

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import MutableMapping
from typing import Any

import pytest

from provisa.api.request_timeout import serve_within_deadline
from provisa.core import request_deadline
from provisa.core.request_thread import RequestThreadMiddleware, client_watch

# The statement a test runs: this long unless it is cancelled.
_STATEMENT_S = 10.0


class _Statement:
    """A blocking statement on the request thread, with the cancel its driver would register."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self._cancelled = threading.Event()
        self.cancels = 0
        self.deadline: request_deadline.Deadline | None = None

    def cancel(self) -> None:
        self.cancels += 1
        self._cancelled.set()

    def run(self, seconds: float) -> None:
        with request_deadline.cancel_on_deadline(self.cancel):
            self.started.set()
            if self._cancelled.wait(seconds):
                raise RuntimeError("canceling statement due to user request")


def _data_route(statement: _Statement, *, seconds: float = _STATEMENT_S) -> Any:
    """A data route behind the transport middleware: the request's deadline is bound and handed
    to the client watch (as provisa.api.app does), and the route runs one blocking statement."""

    async def route(scope: Any, receive: Any, send: Any) -> None:
        assert (await receive())["type"] == "http.request"
        try:
            statement.run(seconds)
            status, body = 200, b"rows"
        except TimeoutError:
            # A receive after the work (is_disconnected) is given the message the watch heard.
            assert (await receive())["type"] == "http.disconnect"
            status, body = 504, b"timeout"
        await send({"type": "http.response.start", "status": status, "headers": []})
        await send({"type": "http.response.body", "body": body})

    async def transport(scope: Any, receive: Any, send: Any) -> None:
        with request_deadline.within(30) as deadline:
            statement.deadline = deadline
            watch = client_watch(scope)
            assert watch is not None
            watch.watch(deadline)
            await serve_within_deadline(route, scope, receive, send, deadline)

    return transport


class _Client:
    """The server side of one connection: the body, then http.disconnect when the client leaves
    or once the response is complete (as uvicorn answers a receive after the body)."""

    def __init__(self) -> None:
        self.left = asyncio.Event()
        self.sent: list[dict[str, Any]] = []
        self._body_sent = False

    async def receive(self) -> MutableMapping[str, Any]:
        if not self._body_sent:
            self._body_sent = True
            return {"type": "http.request", "body": b"{}", "more_body": False}
        await self.left.wait()
        return {"type": "http.disconnect"}

    async def send(self, message: MutableMapping[str, Any]) -> None:
        self.sent.append(dict(message))
        if message["type"] == "http.response.body" and not message.get("more_body"):
            self.left.set()  # response complete: the server answers http.disconnect from now


_SCOPE = {"type": "http", "method": "POST", "path": "/data/sql", "headers": []}


async def _until(event: threading.Event, timeout: float = 10.0) -> None:
    started = time.monotonic()
    while not event.is_set():
        assert time.monotonic() - started < timeout, "the statement never started"
        await asyncio.sleep(0.01)


def test_a_client_that_disconnects_mid_statement_cancels_it_once():
    statement = _Statement()
    client = _Client()
    app = RequestThreadMiddleware(_data_route(statement))

    async def go() -> float:
        request = asyncio.ensure_future(app(dict(_SCOPE), client.receive, client.send))
        await _until(statement.started)
        left_at = time.monotonic()
        client.left.set()  # the client closes the connection (the UI's Stop aborts the fetch)
        await asyncio.wait_for(request, 10)
        return time.monotonic() - left_at

    took = asyncio.run(go())

    assert statement.cancels == 1
    assert took < _STATEMENT_S / 2, f"the statement ran on for {took:.1f}s after the client left"
    assert statement.deadline is not None and statement.deadline.client_left


def test_a_request_that_finishes_normally_fires_no_cancel():
    statement = _Statement()
    client = _Client()
    app = RequestThreadMiddleware(_data_route(statement, seconds=0.0))

    async def go() -> None:
        await asyncio.wait_for(app(dict(_SCOPE), client.receive, client.send), 10)
        # The server's http.disconnect after the complete response is not the client leaving.
        await asyncio.sleep(0.1)

    asyncio.run(go())

    assert statement.cancels == 0
    assert statement.deadline is not None and not statement.deadline.client_left
    assert [m.get("status") for m in client.sent if m["type"] == "http.response.start"] == [200]


def test_a_disconnect_reported_twice_ends_the_deadline_once():
    cancels: list[int] = []
    deadline = request_deadline.Deadline(30)
    watch = request_deadline.ClientWatch()
    watch.watch(deadline)
    # The statement comes back refused: the request it belonged to has ended.
    with (
        pytest.raises(TimeoutError, match="the client disconnected"),
        deadline._registered(lambda: cancels.append(1)),  # noqa: SLF001
    ):
        assert watch.gone() is True
        assert watch.gone() is False
    deadline.stop()
    assert cancels == [1]
    assert deadline.client_left


def test_a_client_that_left_before_the_deadline_was_bound_ends_it_when_it_is():
    watch = request_deadline.ClientWatch()
    assert watch.gone() is False  # nothing running yet to end
    deadline = request_deadline.Deadline(30)
    watch.watch(deadline)
    assert deadline.fired and deadline.client_left
    deadline.stop()


def test_a_finished_request_is_not_ended_by_a_late_disconnect():
    deadline = request_deadline.Deadline(30)
    watch = request_deadline.ClientWatch()
    watch.watch(deadline)
    deadline.stop()
    assert watch.gone() is False
    assert not deadline.client_left


def test_a_request_the_client_left_ends_without_a_warning(caplog):
    """The route fails with the cancelled statement after the client left: the boundary does not
    report it as a failure (the timeout path logs a warning with the traceback)."""

    async def route(scope: Any, receive: Any, send: Any) -> None:
        raise RuntimeError("canceling statement due to user request")

    async def go() -> list[dict[str, Any]]:
        sent: list[dict[str, Any]] = []

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        with request_deadline.within(30) as deadline:
            watch = request_deadline.ClientWatch()
            watch.watch(deadline)
            watch.gone()
            await serve_within_deadline(route, dict(_SCOPE), None, send, deadline)
        return sent

    with caplog.at_level(logging.INFO, logger="provisa.api.request_timeout"):
        sent = asyncio.run(go())

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("the client disconnected" in r.getMessage() for r in caplog.records)
    assert sent[0]["type"] == "http.response.start"


def test_sparql_over_http_is_a_transport_with_a_deadline(monkeypatch):
    from provisa.core.limits import REQUEST_TRANSPORTS, http_transport_for_path

    monkeypatch.setattr("provisa.core.limits.request_timeout_for", lambda transport: 30.0)
    monkeypatch.setattr(
        "provisa.core.limits.request_timeout_setting",
        lambda transport: f"limits.request_timeouts.{transport}",
    )
    monkeypatch.setattr("provisa.core.process_mode.refuse_data_request", lambda transport: None)

    transport = http_transport_for_path("/data/sparql")
    assert transport == "sparql_http" and transport in REQUEST_TRANSPORTS
    with request_deadline.request(transport) as deadline:
        assert deadline.transport == "sparql_http"
        assert request_deadline.current() is deadline


@pytest.mark.parametrize(
    "path", ["/data/graphql", "/data/sql", "/data/cypher", "/data/sparql", "/data/rest/a/b"]
)
def test_every_http_data_route_is_a_transport(path):
    from provisa.core.limits import http_transport_for_path

    assert http_transport_for_path(path) is not None
