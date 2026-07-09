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
import binascii

import jwt

from sqlalchemy import func, select
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from provisa.auth.models import AuthIdentity, AuthProvider, RoleAssignment
from provisa.auth.role_mapping import resolve_assignments, resolve_role
from provisa.auth.superuser import check_superuser
from provisa.core.schema_admin import user_org_memberships, user_profiles
from provisa.core.schema_org import user_role_assignments

# Requirements: REQ-120, REQ-125, REQ-273

# Liveness/readiness probes (/live, /ready) return a static status with no data and must be
# reachable by unauthenticated orchestrators (k8s, load balancers) — same as /health.
_SKIP_PATHS = {
    "/health",
    "/live",
    "/ready",
    "/docs",
    "/openapi.json",
    "/auth/login",
    "/setup/status",
}


class AuthMiddleware(BaseHTTPMiddleware):  # REQ-120, REQ-125, REQ-273
    """Extract and validate Bearer tokens, resolve identity to role."""

    def __init__(
        self,
        app,
        provider: AuthProvider | None = None,
        mapping_rules: list[dict] | None = None,
        default_role: str = "analyst",
        db_pool=None,
        admin_pool=None,
        assignments_source: str = "claims",
        default_assignments: list[dict] | None = None,
        multitenancy: bool = False,
        default_org_id: str = "root",
        superuser: dict | None = None,
        config_resolver=None,
    ) -> None:
        super().__init__(app)
        self._provider = provider
        self._mapping_rules = mapping_rules or []
        self._default_role = default_role
        # Tenant control plane: user_role_assignments. Platform control plane
        # (admin_pool): user_profiles, user_org_memberships.
        self._db_pool = db_pool
        self._admin_pool = admin_pool
        self._assignments_source = assignments_source
        self._default_assignments = default_assignments or []
        self._multitenancy = multitenancy
        self._default_org_id = default_org_id
        self._superuser = superuser
        # Lazy wiring: when the middleware is installed at create_app (before the lifespan has loaded
        # auth_config and the control-plane pools), config_resolver returns the settings from live
        # ``state`` on the first request. None → settings above are already final (test/eager path).
        self._config_resolver = config_resolver
        self._resolved = config_resolver is None
        self._resolve_lock = asyncio.Lock()

    async def _ensure_resolved(self) -> None:
        if self._resolved:
            return
        async with self._resolve_lock:
            if self._resolved:
                return
            resolver = self._config_resolver
            assert resolver is not None  # _resolved is False only when a resolver was set
            s = resolver()
            self._provider = s["provider"]
            self._mapping_rules = s.get("mapping_rules") or []
            self._default_role = s["default_role"]
            self._db_pool = s["db_pool"]
            self._admin_pool = s["admin_pool"]
            self._assignments_source = s["assignments_source"]
            self._default_assignments = s.get("default_assignments") or []
            self._multitenancy = s["multitenancy"]
            self._default_org_id = s["default_org_id"]
            self._superuser = s["superuser"]
            self._resolved = True

    async def dispatch(self, request: Request, call_next):  # REQ-486
        if request.url.path in _SKIP_PATHS:
            return await call_next(request)

        await self._ensure_resolved()

        # No auth configured — backward compat: admin identity. REQ-273 caveat: when the
        # server is unsecured, a client-supplied role IS honored (there is no auth to validate
        # against), so X-Provisa-Role is taken at face value here; it defaults to admin.
        # With no identity provider, the username IS the role (there is nothing else to name the
        # caller by).
        if self._provider is None:
            unsecured_role = request.headers.get("x-provisa-role") or "admin"
            request.state.identity = AuthIdentity(
                user_id=unsecured_role,
                email=None,
                display_name=unsecured_role,
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
                except (ValueError, binascii.Error):
                    # Malformed Basic header — reject rather than fall through silently.
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Malformed Basic Authorization header"},
                    )
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
        except (ValueError, jwt.PyJWTError):
            # Only genuine token-validation failures map to 401; infra/unexpected
            # errors (DB down, JWKS fetch failure, misconfig) must propagate.
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
            )

        if self._assignments_source == "provisa" and self._db_pool is not None:
            async with self._db_pool.acquire() as conn:
                result = await conn.execute_core(
                    select(
                        user_role_assignments.c.role_id,
                        user_role_assignments.c.domain_id,
                    ).where(user_role_assignments.c.user_id == identity.user_id)
                )
                rows = [dict(r._mapping) for r in result.fetchall()]
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

        # Fire-and-forget upsert of user_profiles (platform control plane)
        if self._admin_pool is not None:
            _admin_pool = self._admin_pool

            # provider_name is part of the AuthProvider contract; a missing one is a
            # wiring bug, not something to mask with an "unknown" audit record.
            provider_name = self._provider.provider_name

            async def _upsert():
                async with _admin_pool.acquire() as conn:
                    await conn.upsert(
                        user_profiles,
                        {
                            "user_id": identity.user_id,
                            "email": identity.email,
                            "display_name": identity.display_name,
                            "provider": provider_name,
                            "last_seen": func.now(),
                        },
                        index_elements=["user_id"],
                        update_columns=["email", "display_name", "provider", "last_seen"],
                    )

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
                elif self._admin_pool is not None:
                    async with self._admin_pool.acquire() as conn:
                        result = await conn.execute_core(
                            select(user_org_memberships.c.org_id).where(
                                user_org_memberships.c.user_id == identity.user_id
                            )
                        )
                        org_rows = [dict(r._mapping) for r in result.fetchall()]
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
