# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8b2d61-9c3e-4a57-b0d8-2e6f1a9c5d73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""HTTP request threads are reused, and a request is handed over once each way (REQ-1882, amended
2026-10-01).

A request still runs start-to-finish on its own thread — one that serves nothing else while it
does. What changed is how it gets there:

- the thread comes from a pool (a thread serves ONE request, then the next) instead of being
  created per request;
- a small request body is read by the accepting loop before the hand-off, and a complete response
  is handed back once — instead of one relay to the accepting loop per ASGI message;
- a streamed response, a large or chunked body, a ``receive()`` after the body (a disconnect
  watch) and a WebSocket keep the per-message relay.
"""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from provisa.core import request_thread
from provisa.core.request_thread import RequestThreadMiddleware, RequestThreadPool


class _Front:
    """The accepting loop on its own thread, with a scripted ASGI receive and a recorded send."""

    def __init__(self, body: list[bytes] | None = None, headers: list | None = None) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.ident = asyncio.run_coroutine_threadsafe(self._ident(), self.loop).result()
        self._body = list(body if body is not None else [b""])
        self.headers = headers
        self.receives: list[int] = []  # thread ident of each receive() call
        self.sent: list[dict] = []
        self.send_threads: list[int] = []
        self.disconnect = asyncio.run_coroutine_threadsafe(self._event(), self.loop).result()

    async def _ident(self) -> int:
        return threading.get_ident()

    async def _event(self) -> asyncio.Event:
        return asyncio.Event()

    async def receive(self) -> dict:
        self.receives.append(threading.get_ident())
        if self._body:
            chunk = self._body.pop(0)
            return {"type": "http.request", "body": chunk, "more_body": bool(self._body)}
        await self.disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(self, message: dict) -> None:
        self.send_threads.append(threading.get_ident())
        self.sent.append(message)

    def scope(self, kind: str = "http") -> dict:
        headers = self.headers
        if headers is None:
            headers = [(b"content-length", str(sum(len(b) for b in self._body)).encode())]
        return {"type": kind, "path": "/data/graphql", "method": "POST", "headers": headers}

    def call(self, middleware, kind: str = "http", timeout: float = 10.0):
        fut = asyncio.run_coroutine_threadsafe(
            middleware(self.scope(kind), self.receive, self.send), self.loop
        )
        return fut.result(timeout)

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)


@pytest.fixture
def front():
    fronts: list[_Front] = []

    def make(**kw) -> _Front:
        f = _Front(**kw)
        fronts.append(f)
        return f

    yield make
    for f in fronts:
        f.close()


@pytest.fixture
def relays(monkeypatch) -> list[str]:
    """Every cross-thread relay to the accepting loop the request thread makes, by coroutine."""
    made: list[str] = []
    real = asyncio.run_coroutine_threadsafe

    def counting(coro, loop):
        name = getattr(coro, "__qualname__", "").rsplit(".", 1)[-1]
        if name in ("_flush", "_front_send", "_front_receive"):  # the middleware's own relays
            made.append(name)
        return real(coro, loop)

    monkeypatch.setattr(request_thread.asyncio, "run_coroutine_threadsafe", counting)
    return made


_START = {"type": "http.response.start", "status": 200, "headers": []}


async def _echo(scope, receive, send):
    chunks = []
    while True:
        message = await receive()
        chunks.append(message.get("body", b""))
        if not message.get("more_body"):
            break
    await send(dict(_START))
    await send({"type": "http.response.body", "body": b"".join(chunks)})


def _recording(seen: list, inner=_echo):
    async def app(scope, receive, send):
        seen.append((threading.get_ident(), threading.current_thread().name))
        await inner(scope, receive, send)
        seen.append((threading.get_ident(), threading.current_thread().name))

    return app


def test_a_request_runs_start_to_finish_on_one_request_thread(front):
    seen: list = []
    f = front(body=[b'{"q":1}'])
    f.call(RequestThreadMiddleware(_recording(seen), pool=RequestThreadPool()))
    (before, name), (after, _) = seen
    assert before == after != f.ident
    assert name == "provisa-request"
    assert f.sent == [_START, {"type": "http.response.body", "body": b'{"q":1}'}]


def test_the_next_request_reuses_the_thread(front):
    seen: list = []
    middleware = RequestThreadMiddleware(_recording(seen), pool=RequestThreadPool())
    for _ in range(3):
        front(body=[b"x"]).call(middleware)
    assert len({ident for ident, _ in seen}) == 1


def test_two_requests_at_once_are_on_two_threads_never_one(front):
    inside = threading.Barrier(2)
    seen: list = []

    async def meet(scope, receive, send):
        inside.wait(5)  # both requests are inside the app at the same moment
        await _echo(scope, receive, send)

    middleware = RequestThreadMiddleware(_recording(seen, meet), pool=RequestThreadPool())
    fronts = [front(body=[b"a"]), front(body=[b"b"])]
    callers = [threading.Thread(target=f.call, args=(middleware,)) for f in fronts]
    for c in callers:
        c.start()
    for c in callers:
        c.join(10)
    assert len({ident for ident, _ in seen}) == 2


def test_a_small_request_is_handed_over_once_each_way(front, relays):
    f = front(body=[b'{"query":"{ orders { id } }"}'])
    f.call(RequestThreadMiddleware(_echo, pool=RequestThreadPool()))
    assert f.receives == [f.ident]  # the body was read by the accepting loop, before the hand-off
    assert relays == ["_flush"]  # ONE relay back: the complete response
    assert set(f.send_threads) == {f.ident}
    assert [m["type"] for m in f.sent] == ["http.response.start", "http.response.body"]


def test_a_streamed_response_is_relayed_chunk_by_chunk_with_flow_control(front, relays):
    order: list[str] = []

    async def stream(scope, receive, send):
        await receive()
        await send(dict(_START))
        for n in range(3):
            order.append(f"produce {n}")
            await send({"type": "http.response.body", "body": str(n).encode(), "more_body": True})
            order.append(f"sent {n}")
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    f = front(body=[b""])
    f.call(RequestThreadMiddleware(stream, pool=RequestThreadPool()))
    assert [m.get("body") for m in f.sent[1:]] == [b"0", b"1", b"2", b""]
    # Each chunk was accepted by the server before the next was produced.
    assert order == ["produce 0", "sent 0", "produce 1", "sent 1", "produce 2", "sent 2"]
    assert len(relays) == 4  # start+first chunk together, then one per message


def test_a_large_or_chunked_body_is_relayed_per_message(front, relays):
    big = [b"x" * 600_000, b"y" * 600_000]  # past the pre-read bound
    f = front(body=list(big))
    f.call(RequestThreadMiddleware(_echo, pool=RequestThreadPool()))
    assert f.sent[1]["body"] == b"".join(big)
    assert relays.count("_front_receive") == 2

    f = front(body=[b"a", b"b"], headers=[(b"transfer-encoding", b"chunked")])
    relays.clear()
    f.call(RequestThreadMiddleware(_echo, pool=RequestThreadPool()))
    assert f.sent[1]["body"] == b"ab"
    assert relays.count("_front_receive") == 2


def test_a_receive_after_the_body_still_reaches_the_accepting_loop(front, relays):
    """``request.is_disconnected()`` and SSE disconnect watches read past the body."""
    got: list[str] = []

    async def watch(scope, receive, send):
        await receive()
        got.append((await receive())["type"])
        await send(dict(_START))
        await send({"type": "http.response.body", "body": b""})

    f = front(body=[b"q"])
    f.loop.call_soon_threadsafe(f.disconnect.set)
    f.call(RequestThreadMiddleware(watch, pool=RequestThreadPool()))
    assert got == ["http.disconnect"]
    assert relays.count("_front_receive") == 1


def test_a_websocket_keeps_the_per_message_relay(front, relays):
    async def ws(scope, receive, send):
        await send({"type": "websocket.accept"})
        await send({"type": "websocket.send", "text": "hi"})
        await send({"type": "websocket.close"})

    f = front()
    f.call(RequestThreadMiddleware(ws, pool=RequestThreadPool()), kind="websocket")
    assert [m["type"] for m in f.sent] == ["websocket.accept", "websocket.send", "websocket.close"]
    assert relays == ["_front_send"] * 3


def test_an_exception_reaches_the_caller_and_the_thread_serves_the_next_request(front):
    seen: list = []

    async def boom(scope, receive, send):
        seen.append(threading.get_ident())
        raise ValueError("no")

    pool = RequestThreadPool()
    with pytest.raises(ValueError, match="no"):
        front().call(RequestThreadMiddleware(boom, pool=pool))
    front(body=[b"x"]).call(RequestThreadMiddleware(_recording(seen), pool=pool))
    assert seen[0] == seen[1][0]


def test_the_pool_is_bounded_and_the_request_beyond_it_waits_for_a_thread(front):
    release = threading.Event()
    seen: list = []

    async def hold(scope, receive, send):
        seen.append(threading.get_ident())
        release.wait(10)
        await _echo(scope, receive, send)

    pool = RequestThreadPool(max_threads=2, wait_s=10.0)
    middleware = RequestThreadMiddleware(hold, pool=pool)
    fronts = [front(body=[b"x"]) for _ in range(3)]
    callers = [threading.Thread(target=f.call, args=(middleware,)) for f in fronts]
    for c in callers:
        c.start()
    deadline = time.monotonic() + 5
    while len(seen) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.2)
    assert len(seen) == 2  # the third request is waiting for a thread, not sharing one
    release.set()
    for c in callers:
        c.join(10)
    assert len(seen) == 3 and len(set(seen)) == 2  # it ran on a thread a finished request freed
    assert all(len(f.sent) == 2 for f in fronts)


def test_a_request_that_waits_past_its_budget_for_a_thread_fails_loudly(front):
    release = threading.Event()

    async def hold(scope, receive, send):
        release.wait(10)
        await _echo(scope, receive, send)

    pool = RequestThreadPool(max_threads=1, wait_s=0.2)
    middleware = RequestThreadMiddleware(hold, pool=pool)
    first = front(body=[b"x"])
    holder = threading.Thread(target=first.call, args=(middleware,))
    holder.start()
    time.sleep(0.1)
    try:
        refused = front(body=[b"y"])
        refused.call(middleware)
        assert refused.sent[0]["status"] == 503
        assert b"no request thread became free within 0.2s" in refused.sent[1]["body"]
    finally:
        release.set()
        holder.join(10)


def test_a_reused_thread_carries_nothing_from_the_request_before(front):
    """Request B on the thread request A used sees none of A's role, org, audit identity, trace
    detail or deadline."""
    from provisa import otel_compat
    from provisa.audit.context import current_audit_identity, set_audit_identity, AuditIdentity
    from provisa.core import request_deadline
    from provisa.core.request_context import (
        current_acting_role,
        current_org,
        set_current_org,
    )

    seen: dict[str, tuple] = {}

    def _state():
        return (
            threading.get_ident(),
            current_org.get(),
            current_acting_role.get(),
            current_audit_identity(),
            otel_compat._request_detail.get(),
            request_deadline.current(),
        )

    async def request_a(scope, receive, send):
        set_current_org("acme")
        current_acting_role.set("steward")
        set_audit_identity(AuditIdentity("alice", "http"))
        otel_compat.set_trace_detail("debug")
        with request_deadline.within(30.0):
            seen["a"] = _state()
            await _echo(scope, receive, send)

    async def request_b(scope, receive, send):
        seen["b"] = _state()
        await _echo(scope, receive, send)

    pool = RequestThreadPool()
    front(body=[b"a"]).call(RequestThreadMiddleware(request_a, pool=pool))
    front(body=[b"b"]).call(RequestThreadMiddleware(request_b, pool=pool))
    assert seen["a"][0] == seen["b"][0]  # the same thread
    assert seen["a"][1:4] == ("acme", "steward", AuditIdentity("alice", "http"))
    assert seen["a"][4] == "debug" and seen["a"][5] is not None
    assert seen["b"][1:] == (None, None, None, None, None)


def test_run_on_request_thread_uses_the_pool(front):
    """MCP's entry: the coroutine runs on a pooled request thread, in the caller's context."""
    import contextvars

    marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="unset")
    out: list = []

    async def body():
        out.append((threading.current_thread().name, marker.get()))
        return 7

    async def caller():
        marker.set("mcp-call")
        return await request_thread.run_on_request_thread(lambda: body())

    f = front()
    assert asyncio.run_coroutine_threadsafe(caller(), f.loop).result(10) == 7
    assert out == [("provisa-request", "mcp-call")]


# -- the bound: 4 request threads per worker, and what it must not starve (REQ-1882/REQ-1913) ----


def test_the_default_bound_is_four_request_threads_per_worker(monkeypatch):
    for name in (
        "PROVISA_REQUEST_THREADS",
        "PROVISA_STREAM_THREADS",
        "PROVISA_CONTROL_REQUEST_THREADS",
    ):
        monkeypatch.delenv(name, raising=False)
    limits = request_thread.default_limits()
    assert limits == {"request_threads": 4, "stream_threads": 256, "control_request_threads": 4}
    monkeypatch.setenv("PROVISA_REQUEST_THREADS", "9")
    assert request_thread.default_limits()["request_threads"] == 9


def _held_stream(release: threading.Event, started: threading.Event):
    async def stream(scope, receive, send):
        await receive()
        await send(dict(_START))
        await send({"type": "http.response.body", "body": b"first", "more_body": True})
        started.set()
        release.wait(10)  # an SSE subscription: open for as long as its client listens
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    return stream


def test_a_stream_does_not_hold_a_request_slot_for_its_lifetime(front):
    """With ONE request slot and a subscription open on it, the next request is still served:
    a response that starts streaming leaves the request bound and is counted as a stream."""
    release, started = threading.Event(), threading.Event()
    pool = RequestThreadPool(max_threads=1, max_streams=8, wait_s=2.0)
    streaming = front(body=[b""])
    holder = threading.Thread(
        target=streaming.call,
        args=(RequestThreadMiddleware(_held_stream(release, started), pool=pool),),
    )
    holder.start()
    assert started.wait(5)
    try:
        seen: list = []
        f = front(body=[b"x"])
        f.call(RequestThreadMiddleware(_recording(seen), pool=pool))
        assert f.sent[1]["body"] == b"x"  # served while the stream is still open
        assert pool.counts() == {"request_threads": 1, "streams": 1}
    finally:
        release.set()
        holder.join(10)
    time.sleep(0.2)
    assert pool.counts() == {"request_threads": 1, "streams": 0}  # the bound holds afterwards


def test_streams_have_their_own_bound_and_the_one_past_it_is_refused(front):
    release, started = threading.Event(), threading.Event()
    pool = RequestThreadPool(max_threads=4, max_streams=1, wait_s=2.0)
    app = RequestThreadMiddleware(_held_stream(release, started), pool=pool)
    first = front(body=[b""])
    holder = threading.Thread(target=first.call, args=(app,))
    holder.start()
    assert started.wait(5)
    try:
        second = front(body=[b""])
        second.call(app)
        assert second.sent[0]["status"] == 503
        assert b"PROVISA_STREAM_THREADS" in second.sent[1]["body"]
    finally:
        release.set()
        holder.join(10)


def test_a_websocket_is_a_stream_not_a_request_slot(front):
    release, opened = threading.Event(), threading.Event()

    async def ws(scope, receive, send):
        await send({"type": "websocket.accept"})
        opened.set()
        release.wait(10)
        await send({"type": "websocket.close"})

    pool = RequestThreadPool(max_threads=1, max_streams=1, wait_s=2.0)
    socket = front()
    holder = threading.Thread(
        target=socket.call,
        args=(RequestThreadMiddleware(ws, pool=pool),),
        kwargs={"kind": "websocket"},
    )
    holder.start()
    assert opened.wait(5)
    try:
        f = front(body=[b"x"])
        f.call(RequestThreadMiddleware(_echo, pool=pool))
        assert f.sent[1]["body"] == b"x"  # the request slot was never taken by the socket
        refused = front()
        refused.call(RequestThreadMiddleware(ws, pool=pool), kind="websocket")
        assert refused.sent == [{"type": "websocket.close", "code": 1013}]  # past the stream bound
    finally:
        release.set()
        holder.join(10)


def test_health_and_admin_stay_reachable_when_every_request_thread_is_busy(front):
    release = threading.Event()
    seen: dict[str, str] = {}

    async def app(scope, receive, send):
        if scope["path"].startswith("/data/"):
            release.wait(10)
        seen[scope["path"]] = threading.current_thread().name
        await _echo(scope, receive, send)

    middleware = RequestThreadMiddleware(
        app,
        pool=RequestThreadPool(max_threads=1, wait_s=0.3),
        control_pool=RequestThreadPool(max_threads=2, wait_s=0.3),
    )
    busy = front(body=[b"x"])
    holder = threading.Thread(target=busy.call, args=(middleware,))
    holder.start()
    time.sleep(0.1)
    try:
        for path in ("/health", "/ready", "/live", "/admin/graphql", "/admin/settings"):
            f = front(body=[b""])
            scope = f.scope()
            scope["path"] = path
            asyncio.run_coroutine_threadsafe(middleware(scope, f.receive, f.send), f.loop).result(5)
            assert f.sent[0]["status"] == 200, path
            assert seen[path] == "provisa-request"  # still on a request thread of its own
        refused = front(body=[b"y"])  # a data request, meanwhile, waits its budget and is refused
        refused.call(middleware)
        assert refused.sent[0]["status"] == 503
    finally:
        release.set()
        holder.join(10)


def test_the_bound_can_be_changed_while_running(front):
    release = threading.Event()
    seen: list = []

    async def hold(scope, receive, send):
        seen.append(threading.get_ident())
        release.wait(10)
        await _echo(scope, receive, send)

    pool = RequestThreadPool(max_threads=1, wait_s=10.0)
    middleware = RequestThreadMiddleware(hold, pool=pool)
    fronts = [front(body=[b"x"]) for _ in range(2)]
    callers = [threading.Thread(target=f.call, args=(middleware,)) for f in fronts]
    for c in callers:
        c.start()
    time.sleep(0.3)
    assert len(seen) == 1  # the second request is waiting at a bound of 1
    pool.resize(max_threads=2)
    deadline = time.monotonic() + 5
    while len(seen) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(seen) == 2 and seen[0] != seen[1]  # raising the bound served it at once
    pool.resize(max_threads=1)
    release.set()
    for c in callers:
        c.join(10)
    time.sleep(0.2)
    assert pool.counts()["request_threads"] == 1  # lowering it retired the extra thread


def test_the_wait_budget_is_asked_for_at_wait_time(front):
    """The seam for a per-transport request timeout: the budget is a function, read when a request
    has to wait, not a constant fixed at start."""
    asked: list[int] = []
    release = threading.Event()

    def budget() -> float:
        asked.append(1)
        return 0.2

    async def hold(scope, receive, send):
        release.wait(10)
        await _echo(scope, receive, send)

    pool = RequestThreadPool(max_threads=1, wait_s=budget)
    middleware = RequestThreadMiddleware(hold, pool=pool)
    holder = threading.Thread(target=front(body=[b"x"]).call, args=(middleware,))
    holder.start()
    time.sleep(0.1)
    try:
        assert asked == []  # nobody has waited yet
        refused = front(body=[b"y"])
        refused.call(middleware)
        assert refused.sent[0]["status"] == 503 and asked == [1]
    finally:
        release.set()
        holder.join(10)
