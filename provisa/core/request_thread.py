# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0f5c1e-3a47-4d0e-9a58-8f1b2d6c4e71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Request threads for the loop-fronted transports: HTTP/GraphQL and MCP (REQ-1882, amended 2026-10-01).

pgwire, Bolt, Arrow Flight and gRPC are served by thread-per-connection/RPC servers, so each request
already has its own OS thread. HTTP (uvicorn) and MCP (its own uvicorn) accept on an event loop.
Here that front loop only accepts, parses and hands off: the request itself — middleware, routing,
auth, governance, cache, execution, audit, response streaming — runs on its own request thread, on
a :class:`~provisa.core.connection_loop.ConnectionLoop` checked out for the request and driven by
that thread (``loop.run_until_complete``). Two requests therefore govern and execute in parallel on
two threads instead of interleaving on the front loop.

A request thread serves ONE request, start to finish, and nothing else while it does. When the
request has ended the thread goes back to a pool (:class:`RequestThreadPool`) and serves a later
request — as gRPC's pool threads and Flight's handler threads already do — so a request no longer
pays for creating a thread. Nothing of a request stays on the thread: it runs in its own
``contextvars`` context, on a loop checked out for it and checked back in.

The hand-off is one message each way for the common request. The ASGI ``receive``/``send``
callables belong to the front loop (the server's socket transport lives there), and every use of
them from the request thread is a cross-thread relay, so:

- a small request body is read by the front loop BEFORE the hand-off and travels with it;
- a complete response (start + its one body message) is handed back in one relay.

The per-message relay remains where the exchange is a conversation rather than one message: a
streamed response (chunk by chunk, with the server's flow control), a large or chunked request
body, a ``receive()`` after the body (a disconnect watch), and a WebSocket.
"""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import json
import logging
import queue
import threading
from collections.abc import Awaitable, Callable, Coroutine, MutableMapping
from typing import Any, TypeVar

from provisa.core import request_deadline
from provisa.core.connection_loop import connection_loop

log = logging.getLogger(__name__)

T = TypeVar("T")

# An idle request thread exits after this long with nothing to serve.
_IDLE_TTL_S = 60.0
# A request body up to this size (declared by Content-Length) is read by the accepting loop and
# handed over with the request; a larger or chunked one is relayed message by message.
_PREREAD_MAX_BYTES = 1 << 20
# Paths served from the control pool, so a worker whose request threads are all busy still answers
# its health probes and its administrators.
_CONTROL_PATHS = ("/health", "/live", "/ready")
_CONTROL_PREFIXES = ("/admin/", "/auth/")
_WS_TRY_AGAIN_LATER = 1013


_LIMITS = ("request_threads", "stream_threads", "control_request_threads")


def default_limits() -> dict[str, int]:
    """The per-worker bounds, from the environment (the operator settings REQ-1913 exposes):

    ``request_threads`` (``PROVISA_REQUEST_THREADS``, default 4): request threads serving data
    requests. One in-flight request already occupies a worker's core, so more threads do not
    raise throughput — they only let latency grow past the peak; beyond the bound a request
    waits its turn.
    ``stream_threads`` (``PROVISA_STREAM_THREADS``, default 256): responses being streamed and
    WebSockets open at once. A stream is a thread parked on its client, not a core, and it lives
    for minutes or hours — counted under the request bound, four subscriptions would stop the
    worker serving anything else.
    ``control_request_threads`` (``PROVISA_CONTROL_REQUEST_THREADS``, default 4): threads for
    health probes and the admin/auth API, kept apart so data load cannot make a worker
    unreachable to its orchestrator or its administrators.
    """
    # REQ-1913: three operator settings, declared (with these defaults) in
    # provisa/core/settings_catalog.py and resolved stored value, then environment, then default.
    from provisa.core import settings_registry

    return {name: settings_registry.value(f"concurrency.{name}") for name in _LIMITS}


def _request_budget() -> float:
    """How long a request may wait for a thread: the server's request budget. THE SEAM for the
    per-transport request timeout — when that resolver exists, this is where it is asked."""
    # REQ-1905: the DEFAULT request timeout. A request waits for a thread before it is routed,
    # so its transport — and that transport's own timeout — is not yet known here.
    from provisa.core import settings_registry

    return float(settings_registry.value("limits.request_timeout"))


def _serve(make_coro: Callable[[], Coroutine[Any, Any, T]], ctx: contextvars.Context) -> T:
    with connection_loop() as cl:
        return cl.run(make_coro(), context=ctx)


class RequestThreadsExhausted(TimeoutError):
    """Every request thread stayed busy for a waiting request's whole budget."""


class StreamsExhausted(RuntimeError):
    """The worker already has as many open streams as it allows."""


class _RequestThread:
    """One reusable request thread. It takes a request, runs it to completion, and only then
    takes another."""

    def __init__(self, pool: "RequestThreadPool") -> None:
        self._pool = pool
        self._jobs: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self.streaming = False  # its current request left the request bound for the stream bound
        # A request thread by design (REQ-1882): the request it is given runs entirely here.
        self._thread = threading.Thread(target=self._main, name="provisa-request", daemon=True)
        self._thread.start()

    def give(
        self, make_coro: Any, ctx: contextvars.Context, fut: concurrent.futures.Future
    ) -> None:
        self._jobs.put((make_coro, ctx, fut))

    def _main(self) -> None:
        _current_thread.thread = self
        shield = request_deadline.shielded()
        while True:
            try:
                job = self._jobs.get(timeout=_IDLE_TTL_S)
            except queue.Empty:
                if self._pool.retire(self):
                    return
                continue  # it was handed a request as it timed out: the job is on its way
            make_coro, ctx, fut = job
            if fut.set_running_or_notify_cancel():
                try:
                    fut.set_result(_serve(make_coro, ctx))
                except BaseException as exc:  # handed to the awaiting caller, which re-raises it
                    fut.set_exception(exc)
                finally:
                    # REQ-1905: the request is over; nothing of its deadline follows this thread
                    # to the next one (see request_deadline._ThreadShield.quiesce).
                    with shield.lock:
                        shield.settle()
                        shield.quiesce()
            if not self._pool.release(self):
                return  # the pool has no place for it any more


# The request thread the current code is running on (None off a request thread).
_current_thread = threading.local()


class RequestThreadPool:
    """The request threads of one worker process.

    A thread is taken for one request and returned when that request has ended; two requests are
    never on one thread. The pool grows on demand up to ``max_threads``. A request arriving with
    every thread busy waits — on its accepting loop, in arrival order — for at most the request
    budget, and is then refused (``RequestThreadsExhausted``) rather than run anywhere else.

    A STREAM is counted apart. A request whose response starts streaming, and a WebSocket, leave
    the request bound for ``max_streams``: the thread stays with its stream, but the slot it held
    is free for the next request. A stream past ``max_streams`` is refused at once
    (``StreamsExhausted``) — a stream slot does not free on a request's timescale, so waiting for
    one would only hold the client.
    """

    def __init__(
        self,
        *,
        max_threads: int | None = None,
        max_streams: int | None = None,
        wait_s: float | Callable[[], float] = _request_budget,
    ) -> None:
        limits = default_limits()
        self._max = limits["request_threads"] if max_threads is None else max_threads
        self._max_streams = limits["stream_threads"] if max_streams is None else max_streams
        self._wait = wait_s
        self._lock = threading.Lock()
        self._idle: list[_RequestThread] = []
        self._threads = 0  # threads under the request bound: serving a request, or idle
        self._streams = 0  # threads serving a stream
        # Requests waiting for a thread: (their accepting loop, a future on that loop).
        self._waiting: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[_RequestThread]]] = []

    def counts(self) -> dict[str, int]:
        with self._lock:
            return {"request_threads": self._threads, "streams": self._streams}

    def resize(self, *, max_threads: int | None = None, max_streams: int | None = None) -> None:
        """Change the bounds while running (the operator setting). A raised request bound serves
        waiting requests at once; a lowered one takes effect as requests end."""
        with self._lock:
            if max_streams is not None:
                self._max_streams = max_streams
            if max_threads is not None:
                self._max = max_threads
        self._serve_waiters()

    async def acquire(self) -> _RequestThread:
        """A thread for one request. Called on an accepting loop."""
        waiter: asyncio.Future[_RequestThread] | None = None
        with self._lock:
            if self._idle:
                return self._idle.pop()
            if self._threads < self._max:
                self._threads += 1
            else:
                loop = asyncio.get_running_loop()
                waiter = loop.create_future()
                self._waiting.append((loop, waiter))
        if waiter is None:
            return _RequestThread(self)
        budget = self._wait() if callable(self._wait) else self._wait
        try:
            # Async on the accepting loop by necessity: it keeps serving other connections while
            # this request waits its turn for a thread.
            return await asyncio.wait_for(waiter, budget)
        except TimeoutError:
            raise RequestThreadsExhausted(
                f"no request thread became free within {budget:g}s: all {self._max} request "
                "threads of this worker are serving requests (PROVISA_REQUEST_THREADS)"
            ) from None

    def acquire_stream(self) -> _RequestThread:
        """A thread for a WebSocket: a stream from its first message, never a request slot."""
        with self._lock:
            self._reserve_stream()
        thread = _RequestThread(self)
        thread.streaming = True
        return thread

    def _reserve_stream(self) -> None:
        if self._streams >= self._max_streams:
            raise StreamsExhausted(
                f"this worker already has {self._max_streams} open streams "
                "(PROVISA_STREAM_THREADS): subscriptions, streamed responses and WebSockets"
            )
        self._streams += 1

    def now_streaming(self) -> None:
        """The request on the calling thread has started to stream its response: it leaves the
        request bound, and the slot it held serves the next request."""
        thread: _RequestThread | None = getattr(_current_thread, "thread", None)
        if thread is None or thread.streaming or thread._pool is not self:  # noqa: SLF001
            return
        with self._lock:
            self._reserve_stream()
            thread.streaming = True
            self._threads -= 1
        self._serve_waiters()

    def _serve_waiters(self) -> None:
        """Give waiting requests the capacity that has just become free, each a new thread."""
        while True:
            with self._lock:
                if not self._waiting or self._threads >= self._max:
                    return
                loop, waiter = self._waiting.pop(0)
                self._threads += 1
            # A waiter that had already given up leaves the new thread parked as an idle one.
            self._hand(loop, waiter, _RequestThread(self), fresh=True)

    def _hand(self, loop: Any, waiter: Any, thread: _RequestThread, *, fresh: bool = False) -> bool:
        """Give ``thread`` to a waiting request on its own loop. False when it had gone."""
        handed: concurrent.futures.Future[bool] = concurrent.futures.Future()

        def _give() -> None:
            if waiter.done():  # it gave up waiting (its budget ran out)
                handed.set_result(False)
            else:
                waiter.set_result(thread)
                handed.set_result(True)

        try:
            loop.call_soon_threadsafe(_give)
        except RuntimeError:  # that accepting loop has closed: its waiter is gone with it
            taken = False
        else:
            taken = handed.result()
        if not taken and fresh:
            # Nobody took the new thread: run it once on nothing, so it parks itself as idle.
            thread.give(_noop, contextvars.Context(), concurrent.futures.Future())
        return taken

    def release(self, thread: _RequestThread) -> bool:
        """The thread's request has ended: give it the longest-waiting request, else park it.
        False when the pool has no place for it (the bound was lowered, or it served a stream and
        the request bound is full): the thread exits."""
        with self._lock:
            if thread.streaming:
                thread.streaming = False
                self._streams -= 1
                if self._threads >= self._max:
                    return False
                self._threads += 1
            elif self._threads > self._max:
                self._threads -= 1
                return False
        while True:
            with self._lock:
                if not self._waiting:
                    self._idle.append(thread)
                    return True
                loop, waiter = self._waiting.pop(0)
            if self._hand(loop, waiter, thread):
                return True

    def retire(self, thread: _RequestThread) -> bool:
        """An idle thread asks to exit. False when it has just been taken for a request."""
        with self._lock:
            if thread not in self._idle:
                return False
            self._idle.remove(thread)
            self._threads -= 1
            return True


async def _noop() -> None:
    return None


_pool = RequestThreadPool()
# Health probes and the admin/auth API: their own few threads, never the data pool's.
_control_pool = RequestThreadPool(max_threads=default_limits()["control_request_threads"])


def configure(
    *,
    request_threads: int | None = None,
    stream_threads: int | None = None,
    control_request_threads: int | None = None,
) -> None:
    """Apply the operator's bounds to this worker's pools, live (no restart)."""
    _pool.resize(max_threads=request_threads, max_streams=stream_threads)
    if control_request_threads is not None:
        _control_pool.resize(max_threads=control_request_threads)


def _register_live_bounds() -> None:
    """REQ-1913: every worker applies a stored change of its bounds to its own pools."""
    from provisa.core import settings_registry

    settings_registry.on_change(
        tuple(f"concurrency.{name}" for name in _LIMITS),
        lambda request, stream, control: configure(
            request_threads=request, stream_threads=stream, control_request_threads=control
        ),
    )


_register_live_bounds()


async def run_on_request_thread(
    make_coro: Callable[[], Coroutine[Any, Any, T]],
    *,
    context: contextvars.Context | None = None,
    pool: RequestThreadPool | None = None,
    stream: bool = False,
) -> T:
    """Run ``make_coro()`` to completion on a request thread's own connection loop.

    Awaited on a front loop (ASGI / MCP). The coroutine is created and run on the request thread, in
    ``context`` (default: a copy of the caller's, so request-scoped ContextVars the front side bound
    — MCP's role/org/identity — are visible to it). ``stream``: the request is a stream from its
    first message (a WebSocket) and is counted under the stream bound, never the request bound."""
    ctx = context if context is not None else contextvars.copy_context()
    chosen = pool or _pool
    thread = chosen.acquire_stream() if stream else await chosen.acquire()
    fut: concurrent.futures.Future[T] = concurrent.futures.Future()
    # REQ-1882 sanctioned hop: the accepting loop hands the request to its own thread, where it
    # runs start to finish.
    thread.give(make_coro, ctx, fut)
    # Async on the front loop by necessity: it must keep accepting other requests while this one
    # runs on its thread.
    return await asyncio.wrap_future(fut)


Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


def _small_body(scope: Scope) -> bool:
    """Whether the request body may be read whole by the accepting loop before the hand-off: its
    length is declared and small, or it has none (no Content-Length and not chunked)."""
    length: bytes | None = None
    for name, value in scope.get("headers") or ():
        if name == b"transfer-encoding":
            return False
        if name == b"content-length":
            length = value
    return length is None or int(length) <= _PREREAD_MAX_BYTES


class RequestThreadMiddleware:
    """Pure ASGI: run the downstream app for every HTTP and WebSocket request on a request thread.

    Everything below this middleware runs on the request thread's loop; ``receive``/``send`` belong
    to the front loop, where the server's transport lives. Only ``lifespan`` (server
    startup/shutdown, not a request) stays on the front loop."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        pool: RequestThreadPool | None = None,
        control_pool: RequestThreadPool | None = None,
    ) -> None:
        self._app = app
        self._pool = pool
        self._control_pool = control_pool

    def _pool_for(self, scope: Scope) -> RequestThreadPool:
        """Health probes and the admin/auth API run on the control pool's threads, so a worker
        whose request threads are all serving data requests is still reachable to its
        orchestrator and its administrators. Everything else is a data request."""
        path = scope.get("path", "")
        if path in _CONTROL_PATHS or path.startswith(_CONTROL_PREFIXES):
            return self._control_pool or _control_pool
        return self._pool or _pool

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self._app(scope, receive, send)
            return
        front = asyncio.get_running_loop()
        pool = self._pool_for(scope)

        # Async on the request thread's loop by necessity: the ASGI receive/send channels belong to
        # the front loop's transport; the request loop awaits their completion there.
        async def _front_receive() -> Message:
            return await receive()

        async def _front_send(message: Message) -> None:
            await send(message)

        async def _flush(messages: list[Message]) -> None:
            for message in messages:
                await send(message)

        async def relayed_receive() -> Message:
            return await asyncio.wrap_future(
                asyncio.run_coroutine_threadsafe(_front_receive(), front)
            )

        if scope["type"] == "websocket":
            # REQ-1882 sanctioned hop, per message: a WebSocket is a conversation — each frame is
            # received and sent as it happens, through the front loop's transport.
            async def thread_send(message: Message) -> None:
                await asyncio.wrap_future(
                    asyncio.run_coroutine_threadsafe(_front_send(message), front)
                )

            async def _request() -> None:
                await self._app(scope, relayed_receive, thread_send)

            try:
                # A WebSocket lives as long as its client stays: a stream, not a request slot.
                await run_on_request_thread(_request, pool=pool, stream=True)
            except StreamsExhausted as exc:
                log.error("websocket %s refused: %s", scope.get("path"), exc)
                await send({"type": "websocket.close", "code": _WS_TRY_AGAIN_LATER})
            return

        # The request body, read here on the accepting loop when it is small, so it travels with
        # the hand-off instead of costing a relay back.
        first: list[Message] = []
        if _small_body(scope):
            while True:
                message = await receive()
                first.append(message)
                if message["type"] != "http.request" or not message.get("more_body"):
                    break

        async def thread_receive() -> Message:
            if first:
                if len(first) == 1 or first[0]["type"] != "http.request":
                    return first.pop(0)
                body = b"".join(m.get("body", b"") for m in first if m["type"] == "http.request")
                tail = [m for m in first if m["type"] != "http.request"]
                first[:] = tail
                return {"type": "http.request", "body": body, "more_body": False}
            # REQ-1882 sanctioned hop, per message: a body too large to pre-read (or chunked)
            # arrives as the client sends it, and a receive() after the body is a disconnect watch
            # (``request.is_disconnected()``, an SSE stream) — both are answered by the front
            # loop's transport when it has something to say.
            return await relayed_receive()

        held: list[Message] = []  # the response so far, not yet handed to the front loop
        streaming = False
        refused: list[StreamsExhausted] = []  # the stream bound refused this response
        flushed: list[concurrent.futures.Future[None]] = []

        async def thread_send(message: Message) -> None:
            nonlocal streaming
            if refused:
                return  # nothing of a refused stream reaches the client; it is answered 503
            held.append(message)
            if message["type"] == "http.response.start":
                return  # travels with the first body message
            if not streaming and message.get("more_body"):
                # The response is a stream (an SSE subscription, a large result): from here it
                # is counted under the stream bound and gives its request slot back, so it does
                # not hold one of the worker's few request threads for its lifetime.
                try:
                    pool.now_streaming()
                except StreamsExhausted as exc:
                    held.clear()
                    refused.append(exc)
                    raise
            batch = held[:]
            held.clear()
            if streaming or message.get("more_body"):
                streaming = True
                # REQ-1882 sanctioned hop, per message: a streamed response (SSE, a large result)
                # is sent chunk by chunk and WAITS for the server to take each one — its flow
                # control is what keeps a slow client from being buffered without bound.
                await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(_flush(batch), front))
                return
            # REQ-1882 sanctioned hop, once: the complete response goes to the front loop's
            # transport in one relay. The request thread does not wait for the bytes to leave —
            # nothing is left for it to pace.
            flushed.append(asyncio.run_coroutine_threadsafe(_flush(batch), front))

        async def _request() -> None:
            await self._app(scope, thread_receive, thread_send)

        busy: Exception | None = None
        try:
            try:
                await run_on_request_thread(_request, pool=pool)
            except RequestThreadsExhausted as exc:
                busy = exc  # the request never started
            except Exception:
                if not refused:
                    raise
            if refused:
                busy = refused[0]  # its stream was refused before anything was sent
            if busy is not None:
                # Answered here, on the accepting loop, saying which bound was reached.
                log.error("%s %s refused: %s", scope.get("method"), scope.get("path"), busy)
                body = json.dumps({"error": {"code": "server.busy", "message": str(busy)}}).encode()
                await send(
                    {
                        "type": "http.response.start",
                        "status": 503,
                        "headers": [
                            (b"content-type", b"application/json"),
                            (b"content-length", str(len(body)).encode()),
                        ],
                    }
                )
                await send({"type": "http.response.body", "body": body})
        finally:
            # The response hand-off ran on this loop; surface a failed send (client gone) here,
            # and never return to the server with it still in flight.
            for sent in flushed:
                await asyncio.wrap_future(sent)
