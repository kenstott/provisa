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

import asyncio
import base64

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from provisa.auth.models import AuthIdentity, AuthProvider, RoleAssignment
from provisa.auth.role_mapping import resolve_assignments, resolve_role
from provisa.auth.superuser import check_superuser

# Requirements: REQ-120, REQ-125, REQ-273

_SKIP_PATHS = {"/health", "/docs", "/openapi.json", "/auth/login"}


class AuthMiddleware(BaseHTTPMiddleware):  # REQ-120, REQ-125, REQ-273
    """Extract and validate Bearer tokens, resolve identity to role."""

    def __init__(
        self,
        app,
        provider: AuthProvider | None = None,
        mapping_rules: list[dict] | None = None,
        default_role: str = "analyst",
        db_pool=None,
        assignments_source: str = "claims",
        default_assignments: list[dict] | None = None,
        multitenancy: bool = False,
        default_org_id: str = "root",
        superuser: dict | None = None,
    ) -> None:
        super().__init__(app)
        self._provider = provider
        self._mapping_rules = mapping_rules or []
        self._default_role = default_role
        self._db_pool = db_pool
        self._assignments_source = assignments_source
        self._default_assignments = default_assignments or []
        self._multitenancy = multitenancy
        self._default_org_id = default_org_id
        self._superuser = superuser

    async def dispatch(self, request: Request, call_next):
        if request.url.path in _SKIP_PATHS:
            return await call_next(request)

        # No auth configured — backward compat: admin identity. REQ-273 caveat: when the
        # server is unsecured, a client-supplied role IS honored (there is no auth to validate
        # against), so X-Provisa-Role is taken at face value here; it defaults to admin.
        if self._provider is None:
            unsecured_role = request.headers.get("x-provisa-role") or "admin"
            request.state.identity = AuthIdentity(
                user_id="anonymous",
                email=None,
                display_name="Anonymous",
                roles=[unsecured_role],
                raw_claims={},
            )
            request.state.role = unsecured_role
            request.state.assignments = [RoleAssignment(role_id=unsecured_role, domain_id="*")]
            request.state.active_org_id = self._default_org_id
            return await call_next(request)

        # REQ-125: superuser bootstrap — works regardless of the configured provider.
        # The superuser presents HTTP Basic credentials; on match, short-circuit to an
        # admin identity (admin role grants all capabilities downstream). Checked before
        # provider validation so it functions even when an IdP (bearer) is configured.
        if self._superuser:
            auth_header = request.headers.get("authorization")
            if auth_header and auth_header.startswith("Basic "):
                try:
                    decoded = base64.b64decode(auth_header[len("Basic ") :]).decode("utf-8")
                    su_username, su_password = decoded.split(":", 1)
                except Exception:
                    su_username = su_password = None
                if su_username is not None and su_password is not None:
                    su_identity = check_superuser(su_username, su_password, self._superuser)
                    if su_identity is not None:
                        request.state.identity = su_identity
                        request.state.role = "admin"
                        request.state.assignments = [RoleAssignment(role_id="admin", domain_id="*")]
                        request.state.active_org_id = self._default_org_id
                        return await call_next(request)

        scheme = getattr(self._provider, "auth_scheme", "bearer")
        if scheme == "basic":
            expected_prefix = "Basic "
        else:
            expected_prefix = "Bearer "

        auth_header = request.headers.get("authorization")
        if not auth_header or not auth_header.startswith(expected_prefix):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid Authorization header"},
            )

        token = auth_header[len(expected_prefix) :]
        try:
            identity = await self._provider.validate_token(token)
        except Exception:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
            )

        if self._assignments_source == "provisa" and self._db_pool is not None:
            async with self._db_pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT role_id, domain_id FROM user_role_assignments WHERE user_id = $1",
                    identity.user_id,
                )
            if rows:
                assignments = [
                    RoleAssignment(role_id=r["role_id"], domain_id=r["domain_id"]) for r in rows
                ]
            elif self._default_assignments:
                assignments = [
                    RoleAssignment(role_id=a["role_id"], domain_id=a.get("domain_id", "*"))
                    for a in self._default_assignments
                ]
            else:
                assignments = []
        else:
            assignments = resolve_assignments(identity)

        role = resolve_role(identity, self._mapping_rules, self._default_role)

        # REQ-273: a client may request a specific role via X-Provisa-Role, but the server
        # honors it only when the authenticated user is actually assigned that role — a bare
        # client-supplied role is never trusted. With a single assignment the default stands.
        requested_role = request.headers.get("x-provisa-role")
        if requested_role:
            assigned_role_ids = {a.role_id for a in assignments}
            if requested_role in assigned_role_ids:
                role = requested_role
            else:
                return JSONResponse(
                    status_code=403,
                    content={"detail": f"Role {requested_role!r} is not assigned to this user"},
                )

        # Fire-and-forget upsert of user_profiles
        if self._db_pool is not None:
            _db_pool = self._db_pool

            async def _upsert():
                try:
                    async with _db_pool.acquire() as conn:
                        await conn.execute(
                            """INSERT INTO user_profiles (user_id, email, display_name, provider, last_seen)
                               VALUES ($1, $2, $3, $4, NOW())
                               ON CONFLICT (user_id) DO UPDATE
                               SET email = EXCLUDED.email, display_name = EXCLUDED.display_name,
                                   provider = EXCLUDED.provider, last_seen = NOW()""",
                            identity.user_id,
                            identity.email,
                            identity.display_name,
                            getattr(self._provider, "provider_name", "unknown"),
                        )
                except Exception:
                    pass

            asyncio.ensure_future(_upsert())

        # Resolve active org
        if not self._multitenancy:
            active_org_id = self._default_org_id
        else:
            if identity.active_org_id:
                active_org_id = identity.active_org_id
            else:
                header_org = request.headers.get("x-org-id")
                if header_org:
                    active_org_id = header_org
                elif self._db_pool is not None:
                    async with self._db_pool.acquire() as conn:
                        org_rows = await conn.fetch(
                            "SELECT org_id FROM user_org_memberships WHERE user_id = $1",
                            identity.user_id,
                        )
                    if len(org_rows) == 1:
                        active_org_id = org_rows[0]["org_id"]
                    else:
                        return JSONResponse(
                            status_code=401,
                            content={"detail": "Org selection required"},
                        )
                else:
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Org selection required"},
                    )

        request.state.identity = identity
        request.state.role = role
        request.state.assignments = assignments
        request.state.active_org_id = active_org_id
        return await call_next(request)
