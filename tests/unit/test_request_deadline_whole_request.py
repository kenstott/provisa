# Copyright (c) 2026 Kenneth Stott
# Canary: 9c3a7f52-6e1d-4b80-a4f9-2d5c8e7b1a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One deadline per request, on every transport, watched by one thread (REQ-1905).

A transport binds its request's deadline at its own boundary, around the whole request. Its
expiry names the transport and the setting. It is noticed where a request can be ended between
two pieces of its own work: a blocking driver call (cancelled, or refused on its way out), a
stream between batches, and the transport's boundary before it answers."""

# Requirements: REQ-1905, REQ-1882

from __future__ import annotations

import asyncio
import json
import threading
import time

import pytest

from provisa.api.request_timeout import serve_within_deadline
from provisa.core import request_deadline
from provisa.core.request_deadline import RequestTimedOut
from provisa.executor.result import StreamingQueryResult


@pytest.fixture(autouse=True)
def _timeouts(monkeypatch):
    """Every transport times out at 0.2 s here, from its own setting."""
    monkeypatch.setattr("provisa.core.limits.request_timeout_for", lambda transport: 0.2)
    monkeypatch.setattr(
        "provisa.core.limits.request_timeout_setting",
        lambda transport: f"limits.request_timeouts.{transport}",
    )


def _names(exc: BaseException, transport: str) -> None:
    assert isinstance(exc, RequestTimedOut)
    assert exc.transport == transport
    assert exc.setting == f"limits.request_timeouts.{transport}"
    assert transport in str(exc) and exc.setting in str(exc) and "0.2s" in str(exc)


# --- the deadline itself -----------------------------------------------------------------------


def test_a_requests_deadline_names_its_transport_and_setting():
    with request_deadline.request("rest") as deadline:
        assert request_deadline.current() is deadline
        request_deadline.check()  # in time: nothing
        time.sleep(0.25)
        with pytest.raises(RequestTimedOut) as raised:
            request_deadline.check()
        _names(raised.value, "rest")
    assert request_deadline.current() is None


def test_a_blocking_call_that_returns_after_the_deadline_ends_the_request():
    """Its driver's cancel reached nothing (rows already received, being converted): the call
    comes back, and the request is ended there instead of going on to shape and send them."""
    cancelled = []
    with request_deadline.request("sql_http"):
        with pytest.raises(RequestTimedOut) as raised:
            with request_deadline.cancel_on_deadline(lambda: cancelled.append(True)):
                time.sleep(0.3)  # returns normally, after the deadline
        _names(raised.value, "sql_http")
    assert cancelled == [True]  # the watchdog did call the statement's cancel at expiry


def test_a_blocking_call_within_the_deadline_is_untouched():
    with request_deadline.request("sql_http"):
        with request_deadline.cancel_on_deadline(lambda: None):
            pass


def test_a_failure_after_the_deadline_is_reported_as_the_timeout():
    with pytest.raises(RequestTimedOut) as raised:
        with request_deadline.request("bolt"):
            time.sleep(0.25)
            raise ValueError("what the request happened to fail with")
    _names(raised.value, "bolt")
    assert isinstance(raised.value.__cause__, ValueError)


def test_a_failure_before_the_deadline_is_itself():
    with pytest.raises(ValueError):
        with request_deadline.request("bolt"):
            raise ValueError("an ordinary failure")


def test_a_tighter_deadline_already_bound_is_kept():
    with request_deadline.within(0.05) as outer:
        with request_deadline.request("grpc") as inner:
            assert inner is outer


def test_a_deadline_held_across_messages_is_bound_per_message_and_stopped_by_its_owner():
    deadline = request_deadline.open_request("pgwire")
    assert request_deadline.current() is None
    with request_deadline.bound(deadline):
        assert request_deadline.current() is deadline
    time.sleep(0.25)
    with request_deadline.bound(deadline), pytest.raises(RequestTimedOut) as raised:
        request_deadline.check()
    _names(raised.value, "pgwire")
    deadline.stop()


# --- one watchdog thread -----------------------------------------------------------------------


def _watchdogs() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "provisa-deadline-watchdog"]


def test_two_hundred_requests_are_watched_by_one_thread():
    with request_deadline.request("graphql"):
        pass
    before = threading.active_count()
    held = [request_deadline.open_request("graphql") for _ in range(200)]
    assert threading.active_count() == before
    assert len(_watchdogs()) == 1
    for deadline in held:
        deadline.stop()


def test_ended_requests_do_not_stay_watched_until_their_timeout(monkeypatch):
    """A long timeout (Flight ships 3600 s) must not keep every request it timed in memory for
    that long: ended deadlines are dropped in bulk."""
    monkeypatch.setattr("provisa.core.limits.request_timeout_for", lambda transport: 3600.0)
    watchdog = request_deadline._watchdog  # noqa: SLF001
    baseline = watchdog.watched()
    for _ in range(5000):
        request_deadline.open_request("flight").stop()
    assert watchdog.watched() - baseline < 1000


def test_the_watchdog_cancels_the_statement_in_flight_at_expiry():
    released = threading.Event()
    started = time.monotonic()
    with request_deadline.request("mcp"):
        with pytest.raises(RequestTimedOut) as raised:
            with request_deadline.cancel_on_deadline(released.set):
                assert released.wait(5.0), "the watchdog never cancelled the statement"
                raise ConnectionError("the driver's error for a cancelled statement")
    assert time.monotonic() - started < 1.5
    _names(raised.value, "mcp")


def test_a_request_that_has_ended_is_not_acted_on_at_its_expiry():
    cancelled = []
    deadline = request_deadline.open_request("rest")
    with request_deadline.bound(deadline):
        with request_deadline.cancel_on_deadline(lambda: cancelled.append(True)):
            pass
    deadline.stop()
    time.sleep(0.4)
    assert cancelled == []


def test_work_that_outlives_the_request_does_not_inherit_its_deadline():
    """Detached background work runs in a copy of its caller's context. The request's deadline
    is the request's: work still running (or only starting) after the timeout has passed is not
    refused its statements because of it."""
    from provisa.core.connection_loop import spawn_background

    seen: list[object] = []

    async def _later() -> None:
        time.sleep(0.3)  # the request that started it has timed out by now
        seen.append(request_deadline.current())
        with request_deadline.cancel_on_deadline(lambda: None):
            seen.append("statement ran")

    with request_deadline.request("sql_http") as deadline:
        done = spawn_background(_later(), name="outlives-the-request")
        assert request_deadline.current() is deadline
    done.result(timeout=10)
    assert seen == [None, "statement ran"]


# --- a stream, between batches -----------------------------------------------------------------


def test_a_stream_ends_with_the_timeout_at_the_batch_after_the_deadline_and_frees_its_source():
    released = []

    def _batches():
        yield [(1,)]
        time.sleep(0.3)  # the deadline passes while this batch is being produced
        yield [(2,)]
        yield [(3,)]

    stream = StreamingQueryResult(_batches(), ["id"], on_release=lambda: released.append(True))
    seen = []
    with request_deadline.request("grpc"):
        with pytest.raises(RequestTimedOut) as raised:
            for batch in stream.batches():
                seen.append(batch)
    _names(raised.value, "grpc")
    assert seen == [[(1,)]]
    assert released == [True]


def test_a_stream_outside_a_request_is_not_checked():
    stream = StreamingQueryResult(iter([[(1,)], [(2,)]]), ["id"])
    assert list(stream.batches()) == [[(1,)], [(2,)]]


# --- the HTTP boundary -------------------------------------------------------------------------


class _Client:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def send(self, message: dict) -> None:
        self.messages.append(message)

    @property
    def status(self) -> int:
        return self.messages[0]["status"]

    @property
    def body(self) -> bytes:
        return b"".join(m.get("body", b"") for m in self.messages[1:])


async def _receive() -> dict:
    return {"type": "http.request", "body": b"", "more_body": False}


def _serve(app, transport: str = "graphql") -> _Client:
    client = _Client()

    async def _run() -> None:
        with request_deadline.request(transport) as deadline:
            scope = {"type": "http", "method": "POST", "path": "/data/graphql"}
            await serve_within_deadline(app, scope, _receive, client.send, deadline)

    asyncio.run(_run())
    return client


async def _respond(send, status: int, body: bytes, *, chunks: int = 1) -> None:
    await send({"type": "http.response.start", "status": status, "headers": []})
    for i in range(chunks):
        await send({"type": "http.response.body", "body": body, "more_body": i < chunks - 1})


def _is_the_timeout(client: _Client, transport: str) -> None:
    assert client.status == 504
    answer = json.loads(client.body)
    assert answer["code"] == "data.query_timeout"
    assert answer["params"] == {
        "timeout_s": "0.2",
        "transport": transport,
        "setting": f"limits.request_timeouts.{transport}",
    }
    assert transport in answer["detail"] and answer["params"]["setting"] in answer["detail"]


def test_a_response_inside_the_deadline_is_sent_as_it_is():
    async def app(scope, receive, send):
        await _respond(send, 200, b'{"data":1}')

    client = _serve(app)
    assert client.status == 200 and client.body == b'{"data":1}'


def test_a_result_that_is_ready_after_the_deadline_is_not_sent():
    async def app(scope, receive, send):
        time.sleep(0.25)  # shaping and encoding that outran the request timeout
        await _respond(send, 200, b'{"data":"six million rows"}')

    client = _serve(app)
    _is_the_timeout(client, "graphql")
    assert b"six million rows" not in client.body
    assert len(client.messages) == 2  # one start, one body: nothing of the route's own


def test_an_error_the_route_shaped_from_the_expiry_is_answered_as_the_timeout():
    async def app(scope, receive, send):
        time.sleep(0.25)
        await _respond(send, 500, b'{"error":"Execution failed: request exceeded its budget"}')

    _is_the_timeout(_serve(app, "cypher_http"), "cypher_http")


def test_a_failure_after_the_deadline_with_nothing_sent_is_answered_as_the_timeout():
    async def app(scope, receive, send):
        time.sleep(0.25)
        raise RuntimeError("what the route failed with")

    _is_the_timeout(_serve(app, "rest"), "rest")


def test_a_failure_inside_the_deadline_is_not_touched():
    async def app(scope, receive, send):
        raise RuntimeError("an ordinary failure")

    with pytest.raises(RuntimeError, match="an ordinary failure"):
        _serve(app)


def test_a_response_still_streaming_when_the_deadline_passes_ends_there():
    sent = []

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        for i in range(10):
            await send({"type": "http.response.body", "body": b"chunk", "more_body": True})
            sent.append(i)
            time.sleep(0.1)

    with pytest.raises(RequestTimedOut):
        _serve(app, "jsonapi")
    assert 1 <= len(sent) < 5
