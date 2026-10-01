# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2e41a7-6c3b-4f15-8e0a-2b7c9f4d1a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One gRPC RPC, run entirely on its handler thread (REQ-1882, amended 2026-09-29).

The server is the synchronous ``grpc.server`` with a thread pool: each RPC is served on its own
pool thread. :func:`rpc_scope` checks a :class:`~provisa.core.connection_loop.ConnectionLoop` out
for the RPC and pairs it with a fresh ``contextvars.Context``; every coroutine the RPC runs — the
interceptor's credential check, org resolution, governance, execution, audit — runs on that loop,
on that thread, IN that context. ContextVars one step sets (the interceptor's principal, a handler's
org binding) are therefore seen by every later step of the same RPC, including each message of a
streamed response, and by no other RPC.

The scope is re-entrant on its thread: the interceptor opens it, the servicer handler it wraps
joins it.
"""

# Requirements: REQ-1882

from __future__ import annotations

import contextvars
import threading
from collections.abc import AsyncGenerator, Coroutine, Generator
from contextlib import AbstractContextManager, contextmanager
from typing import Any, TypeVar

from provisa.core.connection_loop import ConnectionLoop, connection_loop
from provisa.otel_compat import current_trace_context, get_tracer, request_span

_tracer = get_tracer(__name__)

T = TypeVar("T")

_local = threading.local()


class RpcScope:
    """The loop and context one RPC runs on."""

    def __init__(self, cl: ConnectionLoop) -> None:
        self._cl = cl
        self._ctx = contextvars.Context()

    def run(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run ``coro`` to completion on the RPC's loop, on this thread, in the RPC's context."""
        return self._cl.run(coro, context=self._ctx)

    @contextmanager
    def entered(self, block: AbstractContextManager[Any]) -> Generator[None]:
        """Hold ``block`` open for the body, entered and exited in the RPC's context rather than
        the handler thread's — what a ``with`` written here cannot do by itself, since every
        coroutine of the RPC runs in ``self._ctx``."""
        self._ctx.run(block.__enter__)
        try:
            yield
        finally:
            self._ctx.run(block.__exit__, None, None, None)

    def iterate(self, agen: AsyncGenerator[T, None]) -> Generator[T, None, None]:
        """Drive a response-streaming handler one message at a time on the RPC's loop."""

        async def _step() -> tuple[bool, Any]:
            try:
                return True, await agen.__anext__()
            except StopAsyncIteration:
                return False, None

        try:
            while True:
                more, item = self.run(_step())
                if not more:
                    return
                yield item
        finally:
            self.run(agen.aclose())


@contextmanager
def rpc_scope() -> Generator[RpcScope]:
    """The RPC's scope on this handler thread — opened here, or joined when already open."""
    current: RpcScope | None = getattr(_local, "scope", None)
    if current is not None:
        yield current
        return
    with connection_loop() as cl:
        scope = RpcScope(cl)
        _local.scope = scope
        # REQ-1910: the RPC's request span, opened INSIDE the RPC's own context — the one every
        # coroutine of the RPC runs in — so auth, governance, execution and audit all report into
        # it. The handler thread's context is passed as the parent: an instrumentor's gRPC server
        # span opened out there becomes the request span instead of a second one.
        span = request_span(_tracer, "grpc.rpc", transport="grpc", parent=current_trace_context())
        try:
            with scope.entered(span):
                yield scope
        finally:
            _local.scope = None
