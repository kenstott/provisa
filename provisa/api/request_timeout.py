# Copyright (c) 2026 Kenneth Stott
# Canary: 4b9d1e7a-2c63-4f08-8a5d-e31f7c0b96d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An HTTP data request is answered inside its request timeout, or with the timeout (REQ-1905).

The request's one deadline is bound around the whole ASGI call for its transport (GraphQL, REST,
JSON:API, SQL over HTTP, Cypher over HTTP). This is that transport's boundary: the last place a
response passes before it leaves. A response that reaches it after the deadline has passed is
not sent — the client is told the request timed out, naming the transport and the setting,
whatever the route had produced (a result, or an error of its own shaped from the same expiry).
A response already streaming when the deadline passes ends there."""

# Requirements: REQ-1905

from __future__ import annotations

import json
import logging
from typing import Any

from provisa.api.errors import timeout_error
from provisa.core.request_deadline import Deadline

log = logging.getLogger(__name__)


async def _send_timeout(send: Any, deadline: Deadline) -> None:
    error = timeout_error(deadline.expired_error())
    body = json.dumps({"detail": error.detail, "code": error.code, "params": error.params}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": error.status_code,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def serve_within_deadline(
    app: Any, scope: Any, receive: Any, send: Any, deadline: Deadline
) -> None:
    """Run ``app`` for one HTTP request whose deadline is ``deadline`` (already bound)."""
    started = False  # the route's own response has begun to leave
    streaming = False  # ... and is being sent in more than one piece
    replaced = False  # the route's response was withheld and the timeout sent in its place

    async def checked_send(message: Any) -> None:
        nonlocal started, streaming, replaced
        if replaced:
            return  # the rest of a response that was not sent
        if message["type"] == "http.response.start":
            if deadline.fired:
                replaced = True
                await _send_timeout(send, deadline)
                return
            started = True
        elif message["type"] == "http.response.body":
            if streaming and deadline.fired:
                raise deadline.expired_error()
            streaming = streaming or bool(message.get("more_body"))
        await send(message)

    try:
        await app(scope, receive, checked_send)
    except Exception:
        if started or replaced or not deadline.fired:
            raise
        # The request failed after its deadline had passed and nothing has been sent: what it
        # failed with is reported here, and the client is answered with the timeout.
        log.warning(
            "%s %s failed after its request deadline passed; answering the timeout",
            scope.get("method"),
            scope.get("path"),
            exc_info=True,
        )
        await _send_timeout(send, deadline)
