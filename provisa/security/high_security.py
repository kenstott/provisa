# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8c1b6a-9e2d-4a7c-b0f5-6d3e8a1c4b90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""High-security mode gating (REQ-693).

When ``security.mode=high`` in provisa.yaml, the Provisa backend must never handle
plaintext data. This module enforces the request-transport half of that posture
(the pgwire-server-off half lives in the startup server-start gate):

  * REST and GraphQL *data* endpoints (``/data/sql``, ``/data/graphql``,
    ``/data/rest``, ``/data/jsonapi``) return 403 unless the caller presents the
    ``X-Provisa-KMS-Key`` header — the proof that it is a JDBC/Python client
    configured for client-side decryption. A browser or plaintext REST/GraphQL
    consumer carries no KMS key, so it is refused.
  * Schema-metadata endpoints (SDL / introspection / schema-version / domains) stay
    reachable so a client can discover which fields are ``@encrypted`` before it
    connects — they expose no row data.

The gate reads ``state.security_high`` (set from config at startup). It never
silently degrades: an unconfigured state is treated as standard mode, and a data
request in high mode without a KMS key is refused, not passed through.
"""

from __future__ import annotations

from typing import Any

from starlette.datastructures import Headers
from starlette.responses import JSONResponse

KMS_KEY_HEADER = "x-provisa-kms-key"

# Data endpoints that return row data — gated in high mode.
_GATED_PREFIXES = ("/data/sql", "/data/graphql", "/data/rest", "/data/jsonapi")
# Schema-metadata endpoints — always reachable (no row data).
_METADATA_PREFIXES = (
    "/data/sdl",
    "/data/introspection",
    "/data/schema-version",
    "/data/domains",
)


def is_high_security(state: Any) -> bool:
    """Whether high-security mode is active for this process."""
    return bool(getattr(state, "security_high", False))


def pgwire_start_allowed(state: Any, pgwire_port: int) -> bool:
    """Whether the pgwire server may start (REQ-693).

    A configured port is refused in high-security mode: the pgwire transport has no
    per-connection client-side-decrypt handshake, so it cannot honour the
    backend-never-sees-plaintext guarantee.
    """
    return bool(pgwire_port) and not is_high_security(state)


def _is_gated_data_path(path: str) -> bool:
    if any(path.startswith(p) for p in _METADATA_PREFIXES):
        return False
    return any(path.startswith(p) for p in _GATED_PREFIXES)


def high_security_reject(path: str, headers: Any) -> JSONResponse | None:
    """Return a 403 response if this request must be refused in high mode, else None.

    ``headers`` is any mapping with a case-insensitive ``.get``.
    """
    if not _is_gated_data_path(path):
        return None
    kms_key = headers.get(KMS_KEY_HEADER)  # type: ignore[attr-defined]
    if kms_key:
        return None
    return JSONResponse(
        status_code=403,
        content={
            "detail": (
                "high-security mode: REST/GraphQL data endpoints require a client-side "
                "decryption key. Connect with a KMS-configured JDBC/Python client "
                "(kms_provider + kms_key_arn). (REQ-693)"
            )
        },
    )


# Plain ASGI middleware, not starlette.middleware.base.BaseHTTPMiddleware: that class relays the
# inner app's response body through a background task + anyio memory stream, which fails to signal
# completion to the client for unbounded StreamingResponse bodies (SSE subscriptions, REQ-219) even
# after the inner generator has fully finished — the connection hangs open. A pure ASGI middleware
# calls the inner app's `send` directly, so no such relay exists.
class HighSecurityMiddleware:  # REQ-693
    """Refuse plaintext data requests when high-security mode is active."""

    def __init__(self, app: Any, state: Any) -> None:
        self.app = app
        self._state = state

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        if is_high_security(self._state):
            refusal = high_security_reject(scope["path"], Headers(scope=scope))
            if refusal is not None:
                await refusal(scope, receive, send)
                return
        await self.app(scope, receive, send)
