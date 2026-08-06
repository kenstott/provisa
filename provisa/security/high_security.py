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

# The whole /data tree is gated in high mode, and the exemptions below are enumerated rather than
# the gated paths. An allow-list of row-returning prefixes silently un-gates every route added
# after it was written — /data/ingest, /data/subscribe and the /data/grpc* proxies were all left
# open that way — so the default is refusal and a new endpoint has to argue its way out.
_GATED_ROOT = "/data/"
# Schema-metadata endpoints — always reachable, because a client has to read the schema (and the
# @encrypted markings on it) before it can connect at all. None of these return row data.
_METADATA_PREFIXES = (
    "/data/sdl",
    "/data/introspection",
    "/data/schema-version",
    "/data/domains",
    "/data/proto",  # protobuf descriptors for a role's schema
    "/data/compile",  # returns the generated SQL for a query, never its results
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


def bolt_start_allowed(state: Any, bolt_port: int) -> bool:
    """Whether the Bolt server may start (REQ-693).

    Bolt gets the pgwire treatment for the same reason: the protocol's HELLO/LOGON exchange
    negotiates a credential, not a decryption context, and a Cypher result is plaintext rows on
    the wire. There is no per-connection proof a client can present, so the port stays closed.
    """
    return bool(bolt_port) and not is_high_security(state)


def mcp_start_allowed(state: Any, mcp_port: int) -> bool:
    """Whether the MCP server may start (REQ-693).

    An MCP tool call hands query results to a model as text. Nothing in that path can decrypt
    client-side, so high-security mode does not serve it at all.
    """
    return bool(mcp_port) and not is_high_security(state)


def kms_key_in_pairs(pairs: Any) -> str | None:
    """The KMS key from gRPC-style ``(name, value)`` metadata pairs, if present.

    gRPC and Arrow Flight carry call metadata as a sequence of pairs rather than a mapping, and
    values arrive as ``bytes`` on some transports.
    """
    for key, value in pairs or ():
        if str(key).lower() != KMS_KEY_HEADER:
            continue
        return value.decode() if isinstance(value, bytes) else str(value)
    return None


def high_security_wire_reject(state: Any, kms_key: str | None) -> str | None:
    """The refusal reason for a data call carrying ``kms_key``, or None to let it through (REQ-693).

    gRPC and Arrow Flight are the transports encrypting clients actually use, so high-security
    mode keeps them serving and requires the same proof the HTTP data endpoints require: a
    client-side decryption key on the call. Closing these ports instead would leave a
    high-security deployment with no wire protocol at all.
    """
    if not is_high_security(state):
        return None
    if kms_key:
        return None
    return (
        "high-security mode: data calls require a client-side decryption key. Connect with a "
        "KMS-configured client (kms_provider + kms_key_arn) so the "
        f"{KMS_KEY_HEADER} call header is sent. (REQ-693)"
    )


def _is_gated_data_path(path: str) -> bool:
    if any(path.startswith(p) for p in _METADATA_PREFIXES):
        return False
    return path.startswith(_GATED_ROOT)


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
