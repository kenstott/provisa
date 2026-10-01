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
- A client that disconnected before any response started is answered 499.

Plain ASGI, not ``@app.middleware("http")`` (Starlette's ``BaseHTTPMiddleware``): that class runs
the app in a child task, relays the response through anyio streams and awaits ``receive()`` itself
to watch for a disconnect — on every request, the data path included, where the request runs on
its own thread and each of those is another relay to the accepting loop (REQ-1882). This wraps
``send`` only and reads nothing.
"""

# Requirements: REQ-1137, REQ-1882

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
                extra.append(
                    (b"x-provisa-license-notice", st.nag_text.replace("\n", " ").encode("latin-1"))
                )
        started = False

        async def send_with_headers(message: Any) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
                if extra:
                    message = {**message, "headers": [*message.get("headers", ()), *extra]}
            await send(message)

        try:
            await self.app(scope, receive, send_with_headers)
        except ClientDisconnect:
            if started:
                raise
            await send(
                {"type": "http.response.start", "status": _CLIENT_CLOSED_REQUEST, "headers": []}
            )
            await send({"type": "http.response.body", "body": b""})
