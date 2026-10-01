# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0f5c1e-3a47-4d0e-9a58-8f1b2d6c4e71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Request threads for the loop-fronted transports: HTTP/GraphQL and MCP (REQ-1882, amended 2026-09-29).

pgwire, Bolt, Arrow Flight and gRPC are served by thread-per-connection/RPC servers, so each request
already has its own OS thread. HTTP (uvicorn) and MCP (its own uvicorn) accept on an event loop.
Here that front loop only accepts, parses and hands off: the request itself — middleware, routing,
auth, governance, cache, execution, audit, response streaming — runs on its own request thread
(one per request, as pgwire's socketserver and Bolt give each connection one), on a :class:`~provisa.core.connection_loop.ConnectionLoop` checked out for
the request and driven by that thread (``loop.run_until_complete``). Two requests therefore govern
and execute in parallel on two threads instead of interleaving on the front loop.

Only the ASGI ``receive``/``send`` callables stay on the front loop (the server's socket transport
lives there); the request thread reaches them through ``run_coroutine_threadsafe`` and awaits the
result on its own loop, so a streamed response is still sent chunk by chunk with the server's own
flow control.
"""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import threading
from collections.abc import Awaitable, Callable, Coroutine, MutableMapping
from typing import Any, TypeVar

from provisa.core.connection_loop import connection_loop

T = TypeVar("T")


def _serve(make_coro: Callable[[], Coroutine[Any, Any, T]], ctx: contextvars.Context) -> T:
    with connection_loop() as cl:
        return cl.run(make_coro(), context=ctx)


async def run_on_request_thread(
    make_coro: Callable[[], Coroutine[Any, Any, T]],
    *,
    context: contextvars.Context | None = None,
) -> T:
    """Run ``make_coro()`` to completion on a request thread's own connection loop.

    Awaited on a front loop (ASGI / MCP). The coroutine is created and run on the request thread, in
    ``context`` (default: a copy of the caller's, so request-scoped ContextVars the front side bound
    — MCP's role/org/identity — are visible to it)."""
    ctx = context if context is not None else contextvars.copy_context()
    fut: concurrent.futures.Future[T] = concurrent.futures.Future()

    def _target() -> None:
        if not fut.set_running_or_notify_cancel():
            return  # the front-loop caller went away before the thread started
        try:
            fut.set_result(_serve(make_coro, ctx))
        except BaseException as exc:  # handed to the awaiting front-loop caller, which re-raises it
            fut.set_exception(exc)

    threading.Thread(target=_target, name="provisa-request", daemon=True).start()
    # Async on the front loop by necessity: it must keep accepting other requests while this one
    # runs on its thread.
    return await asyncio.wrap_future(fut)


Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


class RequestThreadMiddleware:
    """Pure ASGI: run the downstream app for every HTTP and WebSocket request on a request thread.

    Everything below this middleware runs on the request thread's loop; ``receive``/``send`` are
    forwarded to the front loop, where the server's transport lives. Only ``lifespan`` (server
    startup/shutdown, not a request) stays on the front loop."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self._app(scope, receive, send)
            return
        front = asyncio.get_running_loop()

        # Async on the request thread's loop by necessity: the ASGI receive/send channels belong to
        # the front loop's transport; the request loop awaits their completion there.
        async def _front_receive() -> Message:
            return await receive()

        async def _front_send(message: Message) -> None:
            await send(message)

        async def thread_receive() -> Message:
            return await asyncio.wrap_future(
                asyncio.run_coroutine_threadsafe(_front_receive(), front)
            )

        async def thread_send(message: Message) -> None:
            await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(_front_send(message), front))

        async def _request() -> None:
            await self._app(scope, thread_receive, thread_send)

        await run_on_request_thread(_request)
