# Copyright (c) 2026 Kenneth Stott
# Canary: 608b834f-87e8-4b32-8fc1-742ab7cde5d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FastAPI middleware for bearer token authentication."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from provisa.auth.models import AuthIdentity, AuthProvider
from provisa.auth.role_mapping import resolve_role

_SKIP_PATHS = {"/health", "/docs", "/openapi.json", "/auth/login"}


class AuthMiddleware(BaseHTTPMiddleware):
    """Extract and validate Bearer tokens, resolve identity to role."""

    def __init__(
        self,
        app,
        provider: AuthProvider | None = None,
        mapping_rules: list[dict] | None = None,
        default_role: str = "analyst",
    ) -> None:
        super().__init__(app)
        self._provider = provider
        self._mapping_rules = mapping_rules or []
        self._default_role = default_role

    async def dispatch(self, request: Request, call_next):
        if request.url.path in _SKIP_PATHS:
            return await call_next(request)

        # No auth configured — backward compat: admin identity
        if self._provider is None:
            request.state.identity = AuthIdentity(
                user_id="anonymous",
                email=None,
                display_name="Anonymous",
                roles=["admin"],
                raw_claims={},
            )
            request.state.role = "admin"
            return await call_next(request)

        auth_header = request.headers.get("authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid Authorization header"},
            )

        token = auth_header[7:]
        try:
            identity = await self._provider.validate_token(token)
        except Exception:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
            )

        role = resolve_role(identity, self._mapping_rules, self._default_role)
        request.state.identity = identity
        request.state.role = role
        return await call_next(request)
