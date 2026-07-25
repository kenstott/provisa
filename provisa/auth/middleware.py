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
from provisa.core.schema_admin import (
    superadmin_bootstrap,
    user_org_memberships,
    user_profiles,
)
from provisa.core.schema_org import user_role_assignments

# Requirements: REQ-120, REQ-125, REQ-273, REQ-1267

# Liveness/readiness probes (/live, /ready) return a static status with no data and must be
# reachable by unauthenticated orchestrators (k8s, load balancers) — same as /health.
_SKIP_PATHS = {
    "/health",
    "/live",
    "/ready",
    "/data/openapi/docs",
    "/data/openapi/redoc",
    "/data/openapi/openapi.json",
    "/auth/login",
    # REQ-1267: the login page fetches this BEFORE the user has a token, to decide which
    # sign-in UI to render (firebase Google button vs. basic form). It only reveals the
    # configured provider name — public info — so it must bypass the bearer gate.
    "/auth/provider-type",
    "/setup/status",
}


def _assignments_to_claims(assignments: list[RoleAssignment]) -> list[str]:
    """Render resolved (role_id, domain_id) pairs into identity.roles claim strings.

    Enforcement (_resolved_capabilities, /auth/me, resolve_assignments) reads
    identity.roles, so whichever source produced the assignments — bootstrap grant,
    DB user_role_assignments (provisa mode), or IdP claims — must be mirrored back
    into identity.roles for the capability layer to see it. '*' domain collapses to a
    bare role_id, matching resolve_assignments' inverse parse (role_id[:domain_id]).
    """
    return [
        a.role_id if a.domain_id == "*" else f"{a.role_id}:{a.domain_id}" for a in assignments
    ]


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
        bootstrap_superadmin: bool = False,
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
        self._bootstrap_superadmin = bootstrap_superadmin
        # Lazy wiring: when the middleware is installed at create_app (before the lifespan has loaded
        # auth_config and the control-plane pools), config_resolver returns the settings from live
        # ``state`` on the first request. None → settings above are already final (test/eager path).
        self._config_resolver = config_resolver
        self._resolved = config_resolver is None
        # Generation of auth_config this middleware last resolved against. -1 = never resolved; the
        # resolver path re-resolves whenever state.auth_reconfig_generation advances (runtime auth
        # (re)configure — setup wizard / PROVISA_IDP boot deferral), so a server that starts unsecured
        # and later becomes firebase enforces without a process restart (REQ-1267).
        self._resolved_generation = -1
        self._resolve_lock = asyncio.Lock()

    def _current_generation(self) -> int:
        from provisa.api.app import state

        return getattr(state, "auth_reconfig_generation", 0)

    async def _ensure_resolved(self) -> None:
        resolver = self._config_resolver
        if resolver is None:
            return  # eager/test path — settings were passed in and are final
        gen = self._current_generation()
        if self._resolved and gen == self._resolved_generation:
            return
        async with self._resolve_lock:
            gen = self._current_generation()
            if self._resolved and gen == self._resolved_generation:
                return
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
            self._bootstrap_superadmin = s["bootstrap_superadmin"]
            self._resolved = True
            self._resolved_generation = gen

    async def _upsert_profile(self, identity: AuthIdentity) -> None:
        """Record last-seen identity in user_profiles (platform control plane).

        provider_name is part of the AuthProvider contract; a missing one is a
        wiring bug, not something to mask with an "unknown" audit record."""
        assert self._admin_pool is not None
        assert self._provider is not None
        provider_name = self._provider.provider_name
        async with self._admin_pool.acquire() as conn:
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
                    su_assignments = [RoleAssignment(role_id="admin", domain_id="*")]
                    su_identity.roles = _assignments_to_claims(su_assignments)
                    request.state.identity = su_identity
                    request.state.role = "admin"
                    request.state.assignments = su_assignments
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

        # REQ-1266: single-administrator bootstrap (limited IdP mode). The first
        # authenticated user atomically claims the sole super-admin slot and is granted
        # admin; every subsequent, unclaimed user is denied (single-org, no second admin).
        # Runs after token validation (the identity is proven) and before assignment
        # resolution (it short-circuits, like the superuser path). Requires the platform
        # control plane for the singleton lock.
        if self._bootstrap_superadmin and self._admin_pool is not None:
            async with self._admin_pool.acquire() as conn:
                # First writer wins: upsert with DO NOTHING on the fixed id=1 row, then read
                # back the winning user_id. Concurrent first-logins race on the INSERT; only
                # one lands, and this returns whoever claimed the slot.
                claimed_user_id = await conn.upsert_returning(
                    superadmin_bootstrap,
                    {"id": 1, "user_id": identity.user_id},
                    index_elements=["id"],
                    update_columns=[],
                    returning="user_id",
                )
            if claimed_user_id != identity.user_id:
                # Single-tenant bootstrap: exactly one administrator, so every later identity is
                # denied. Multitenant bootstrap: the first user is the platform superadmin, but later
                # identities are NOT rejected — they authenticate with no default grant and join an
                # org by redeeming an invite (/auth/redeem-invite). So fall through to normal
                # assignment resolution instead of 403 when multitenancy is on.
                if not self._multitenancy:
                    return JSONResponse(
                        status_code=403,
                        content={
                            "detail": "Registration closed: this deployment permits a single administrator"
                        },
                    )
                # else: not the superadmin claimant — continue to DB-assignment resolution below.
            else:
                await self._upsert_profile(identity)
                boot_assignments = [RoleAssignment(role_id="admin", domain_id="*")]
                identity.roles = _assignments_to_claims(boot_assignments)
                request.state.identity = identity
                request.state.role = "admin"
                request.state.assignments = boot_assignments
                request.state.active_org_id = self._default_org_id
                return await call_next(request)

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
            asyncio.ensure_future(self._upsert_profile(identity))

        # Resolve active org
        if not self._multitenancy:
            active_org_id = self._default_org_id
        else:
            # Platform admins (global admin/superadmin) may act in any org — org CRUD runs on the
            # platform plane, not a tenant's active-org schema. Everyone else is confined to an org
            # they belong to: a client-supplied X-Org-Id (or token active_org claim) naming a
            # non-member org is rejected, not silently honored, or it becomes a cross-tenant escape.
            is_platform_admin = bool({a.role_id for a in assignments} & {"admin", "superadmin"})
            member_org_ids: list[str] = []
            if self._admin_pool is not None:
                async with self._admin_pool.acquire() as conn:
                    result = await conn.execute_core(
                        select(user_org_memberships.c.org_id).where(
                            user_org_memberships.c.user_id == identity.user_id
                        )
                    )
                    member_org_ids = [dict(r._mapping)["org_id"] for r in result.fetchall()]

            # Platform-plane auth endpoints must work for a just-authenticated user who has no
            # membership yet: /auth/redeem-invite CREATES the first membership, /auth/me reports zero
            # orgs, /setup + /admin/orgs run superadmin onboarding. They touch no tenant data, so an
            # unresolved (None) active org is correct there rather than the tenant-path 401.
            platform_plane = request.url.path.startswith(("/auth/", "/setup", "/admin/orgs"))
            requested_org = identity.active_org_id or request.headers.get("x-org-id")
            if requested_org is not None:
                if is_platform_admin or requested_org in member_org_ids:
                    active_org_id = requested_org
                else:
                    return JSONResponse(
                        status_code=403,
                        content={"detail": f"Not a member of org {requested_org!r}"},
                    )
            elif len(member_org_ids) == 1:
                active_org_id = member_org_ids[0]
            elif platform_plane:
                active_org_id = None
            else:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Org selection required"},
                )

        # Canonicalize identity.roles from the resolved assignments so the capability layer
        # (_resolved_capabilities/require_capability), /auth/me, and every downstream reader
        # see the same roles regardless of source. In "provisa" mode this is what surfaces
        # DB user_role_assignments to enforcement — a bearer token (e.g. Firebase) carries no
        # roles claim, so without this a DB-granted org role would never be enforced.
        identity.roles = _assignments_to_claims(assignments)

        request.state.identity = identity
        request.state.role = role
        request.state.assignments = assignments
        request.state.active_org_id = active_org_id
        return await call_next(request)
