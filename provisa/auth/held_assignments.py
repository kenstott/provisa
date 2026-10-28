# Copyright (c) 2026 Kenneth Stott
# Canary: 74812f93-a8f7-4eea-8f0e-f734b71ba5d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A principal's role assignments, read one way for every transport's sign-in (REQ-120, REQ-551).

``auth.assignments_source`` says where they are: ``claims``, the roles the sign-in itself carries;
or ``provisa``, the assignments the control plane holds for the user in the org it signs in to --
granted in the admin UI, an invitation or a configuration load -- or the configured
``default_assignments`` when it holds none. Then the rules HTTP applies apply on every
transport (:func:`settle`): the deployment's bootstrap administrator holds platform_admin besides
its seated roles (REQ-1297), and in a tenant org a role carrying a platform right is not held
(REQ-1297, REQ-1337). HTTP, Bolt, pgwire and every bearer sign-in (Flight, gRPC, Airport) read
them here, so a principal holds the same roles on each.
"""

# Requirements: REQ-120, REQ-551

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from provisa.auth.models import AuthIdentity, RoleAssignment
from provisa.core.assignments_source import CLAIMS, PROVISA


async def read_assignments(
    db_pool: Any, identity: AuthIdentity, default_assignments: list[dict]
) -> list[RoleAssignment]:
    """The user's assignments in force in the control plane of the org bound now (the
    ``current_org`` ContextVar selects its ``org_<id>`` schema; unset, the default org's), or
    ``default_assignments`` when it holds none."""
    from sqlalchemy import select

    from provisa.auth.role_grants import in_force as grant_in_force
    from provisa.core.schema_org import user_role_assignments

    async with db_pool.acquire() as conn:
        result = await conn.execute_core(
            select(user_role_assignments.c.role_id, user_role_assignments.c.domain_id).where(
                user_role_assignments.c.user_id == identity.user_id,
                # REQ-2077: an expired grant is not held from the instant it expires.
                grant_in_force(),
            )
        )
        rows = [dict(r._mapping) for r in result.fetchall()]
    if rows:
        return [RoleAssignment(role_id=r["role_id"], domain_id=r["domain_id"]) for r in rows]
    return [
        RoleAssignment(role_id=a["role_id"], domain_id=a.get("domain_id", "*"))
        for a in default_assignments
    ]


def dedup(assignments: list[RoleAssignment]) -> list[RoleAssignment]:
    """The same (role, domain) pair once, in first-seen order."""
    seen: set[tuple[str, str]] = set()
    out: list[RoleAssignment] = []
    for a in assignments:
        if (a.role_id, a.domain_id) not in seen:
            seen.add((a.role_id, a.domain_id))
            out.append(a)
    return out


def _roles_now() -> dict[str, dict]:
    """The roles registry of the runtime bound now: where a role id turns into its rights."""
    from provisa.api.app import state

    return getattr(state, "roles", {})


async def org_roles(
    org_id: str, roles_now: Callable[[], dict[str, dict]] = _roles_now
) -> dict[str, dict]:  # REQ-1337
    """The roles registry of ``org_id``'s own runtime (prod), built if it is not yet, read by
    ``roles_now`` with the org bound."""
    from provisa.api.app import ensure_serving_runtime
    from provisa.core.request_context import reset_current_org, set_current_org

    await ensure_serving_runtime(org_id, None)
    token = set_current_org(org_id)
    try:
        return roles_now()
    finally:
        reset_current_org(token)


async def is_bootstrap_claimant(state: Any, user_id: str) -> bool:  # REQ-1266, REQ-1290
    """Whether ``user_id`` holds the deployment's single bootstrap administrator slot."""
    from sqlalchemy import select

    from provisa.core.schema_admin import superadmin_bootstrap

    auth = state.auth_config or {}
    admin_pool = getattr(state, "admin_db", None)
    if not auth.get("bootstrap_superadmin") or admin_pool is None:
        return False
    async with admin_pool.acquire() as conn:
        result = await conn.execute_core(
            select(superadmin_bootstrap.c.user_id).where(superadmin_bootstrap.c.id == 1)
        )
        row = result.fetchone()
    return row is not None and row[0] == user_id


def _root_org_id(state: Any) -> str:  # REQ-1286
    """The deployment's own org: ``config.default_org_id`` when set, else the control plane's
    ``org_id`` (as auth.wiring resolves it for the HTTP middleware)."""
    config = getattr(state, "config", None)
    explicit = getattr(config, "default_org_id", None) if config is not None else None
    return explicit or state.org_id


async def settle(
    assignments: list[RoleAssignment],
    *,
    claimant: bool,
    org_id: str | None,
    root_org_id: str | None,
    tenant_roles: dict[str, dict] | None = None,
    roles_now: Callable[[], dict[str, dict]] = _roles_now,
) -> list[RoleAssignment]:
    """``assignments`` as a principal holds them in ``org_id``: the bootstrap ``claimant`` holds
    platform_admin besides its seated roles unless it already holds a control-plane role (the
    right decides, not the name; REQ-1297, REQ-1337), and in a tenant org a role carrying a
    platform right is not held, judged by that org's own role definitions (``tenant_roles``, read
    when not given). ``root_org_id`` is the deployment's own org on a multi-tenant deployment,
    None on a single-tenant one, which has no tenant org. ``roles_now``: the roles registry the
    claimant's roles are judged by."""
    from provisa.security.rights import (
        PLATFORM_ADMIN_ROLE,
        carries_platform_right,
        is_control_plane_role,
        is_tenant_org,
    )

    if claimant and not any(is_control_plane_role(a.role_id, roles_now()) for a in assignments):
        assignments = dedup([*assignments, RoleAssignment(PLATFORM_ADMIN_ROLE, "*")])
    if root_org_id is not None and is_tenant_org(org_id, root_org_id):
        assert org_id is not None  # a tenant org is a bound one
        roles = tenant_roles if tenant_roles is not None else await org_roles(org_id, roles_now)
        assignments = [a for a in assignments if not carries_platform_right(a.role_id, roles)]
    return assignments


async def held_assignments(
    state: Any, identity: AuthIdentity, org_id: str | None
) -> list[RoleAssignment]:
    """``identity``'s assignments, from the source the deployment names, read in ``org_id`` (the
    org its sign-in is bound to; None, the default org's)."""
    from provisa.auth.role_mapping import resolve_assignments

    auth = state.auth_config or {}
    if auth.get("assignments_source", CLAIMS) != PROVISA:
        read = resolve_assignments(identity)
    else:
        from provisa.api.org_runtime import ActiveOrgPool
        from provisa.core.request_context import reset_current_org, set_current_org

        token = set_current_org(org_id) if org_id is not None else None
        try:
            read = await read_assignments(
                ActiveOrgPool(), identity, auth.get("default_assignments") or []
            )
        finally:
            if token is not None:
                reset_current_org(token)
    return await settle(
        read,
        claimant=await is_bootstrap_claimant(state, identity.user_id),
        org_id=org_id,
        # A single-tenant deployment has no tenant org (as the HTTP middleware reads it).
        root_org_id=_root_org_id(state) if getattr(state, "multitenancy", False) else None,
    )
