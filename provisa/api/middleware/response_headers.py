# Copyright (c) 2026 Kenneth Stott
# Canary: 5e9a3c17-2b6d-4f48-a1c5-7d0e8b4f6a92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Response headers every HTTP response may carry, added without touching the body.

- ``X-Schema-Version`` on ``/admin/graphql`` responses (the admin UI's stale-schema check).
- ``X-Provisa-License-Notice`` on any response while the post-trial nag is active (REQ-1137): an
  out-of-band header — never the response body or a schema-typed field, never a gate.
- ``X-Provisa-Warnings`` on any response whose request raised statement warnings (REQ-1350):
  ASCII-escaped JSON ``[{code, params, message}]``, so every HTTP surface carries them.
- A client that disconnected before any response started is answered 499.

Plain ASGI, not ``@app.middleware("http")`` (Starlette's ``BaseHTTPMiddleware``): that class runs
the app in a child task, relays the response through anyio streams and awaits ``receive()`` itself
to watch for a disconnect — on every request, the data path included, where the request runs on
its own thread and each of those is another relay to the accepting loop (REQ-1882). This wraps
``send`` only and reads nothing.
"""

# Requirements: REQ-1137, REQ-1350, REQ-1882

from __future__ import annotations

from typing import Any

from starlette.requests import ClientDisconnect

_ADMIN_GRAPHQL = "/admin/graphql"
_CLIENT_CLOSED_REQUEST = 499


class ResponseHeadersMiddleware:
    def __init__(self, app: Any, *, state: Any) -> None:
        self.app = app
        self._state = state

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        from provisa.licensing import emit as _lic_emit

        extra: list[tuple[bytes, bytes]] = []
        if scope["path"].startswith(_ADMIN_GRAPHQL):
            extra.append((b"x-schema-version", str(self._state.schema_version).encode()))
        if _lic_emit.should_nag():
            st = _lic_emit.current_state()
            if st is not None:
                # ASCII, strictly: a header value outside it fails the response it rides on, and
                # the notice must never fail a request (REQ-1137). nag_message is ASCII by
                # contract (tests/unit/test_licensing.py holds it to that).
                extra.append(
                    (b"x-provisa-license-notice", st.nag_text.replace("\n", " ").encode("ascii"))
                )
        started = False
        from provisa.core.statement_warnings import collecting, header_value

        async def send_with_headers(message: Any) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
                headers = [*extra]
                if warnings:
                    # REQ-1350: what this request's answers say about themselves, on every HTTP
                    # surface; ASCII-escaped JSON, since a header value outside ASCII fails the
                    # response it rides on.
                    headers.append((b"x-provisa-warnings", header_value(warnings).encode("ascii")))
                if headers:
                    message = {**message, "headers": [*message.get("headers", ()), *headers]}
            await send(message)

        # One collector for the request (REQ-1350): a statement it runs, on whatever thread, adds
        # to it (the request's thread runs in a copy of this context).
        with collecting() as warnings:
            await self._call(scope, receive, send, send_with_headers, lambda: started)

    async def _call(
        self, scope: Any, receive: Any, send: Any, send_with_headers: Any, started: Any
    ) -> None:
        try:
            await self.app(scope, receive, send_with_headers)
        except ClientDisconnect:
            if started():
                raise
            await send(
                {"type": "http.response.start", "status": _CLIENT_CLOSED_REQUEST, "headers": []}
            )
            await send({"type": "http.response.body", "body": b""})
