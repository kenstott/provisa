# Copyright (c) 2026 Kenneth Stott
# Canary: 9e3b7c52-4d1a-4f86-b2e7-0c6a5d8f1b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""HTTP request entry for the debug-trace scope (REQ-1910).

The pipeline resolves a request's trace detail when it reaches the pipeline — for an HTTP request
that is after the framework has read the body, and the ASGI ``receive`` span is opened by that
read. Resolved only there, a request an open debug window covers lost its receive span: it was
sampled as a normal-detail child before anything had bound the detail.

So an HTTP request's WINDOW is resolved in middleware, at the first point where its org and role
are both bound: the org-routing middleware, around its dispatch. A small body has already been
read by then, on the accepting loop (REQ-1882), with no span opened: the read's start, end and
size travel with the hand-off, and the receive span is emitted here, with those times, when the
resolved detail is debug (REQ-1910, amended 2026-10-05).
The request's own hint (``@debugTrace``, a ``-- @provisa trace=debug`` comment, the
``X-Provisa-Trace`` header) is still resolved — and, for a role the operator has not permitted,
rejected (REQ-030) — where it is read, by the endpoint and the pipeline: most hints travel in the
body, and a rejection raised here would bypass the application's error handlers.
"""

# Requirements: REQ-1910

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any


@asynccontextmanager
async def http_trace_scope(state: Any, scope: dict) -> AsyncGenerator[None]:
    """Serve the enclosed dispatch under this request's trace detail: bound on entry from the
    operator's debug-trace windows on its org and role, unbound on exit — the scope is the block,
    so the detail cannot outlive the request it was resolved for. A request for which
    authentication resolved no role (health, docs, an unauthenticated path) is not decided here."""
    from provisa.otel_compat import RECEIVE_RECORD, emit_recorded_receive

    request_state = scope.get("state") or {}
    # REQ-1910 (amended 2026-10-05): a body the accepting loop read before this point carries its
    # read's timing; its receive span is emitted here, once the request's detail is known.
    received = request_state.pop(RECEIVE_RECORD, None)
    role_id = request_state.get("role")
    if role_id is None:
        if received is not None:
            emit_recorded_receive(received, scope["type"])
        yield
        return
    from provisa.otel_compat import clear_trace_detail
    from provisa.pgwire._pipeline import resolve_trace_scope

    await resolve_trace_scope(state, role_id, hint=False)
    try:
        if received is not None:
            emit_recorded_receive(received, scope["type"])
        yield
    finally:
        clear_trace_detail()
