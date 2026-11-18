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

A personal access token becomes roles here too (REQ-1263): the assignments read for one are its
OWNER'S, and :func:`settle` keeps of them only the roles the token's scopes name. So a token acts
only as a scope its owner still holds, on every transport, and a scope the owner has lost is dead.
:func:`sign_in_roles` answers what a sign-in acts as and may name from them.
"""

# Requirements: REQ-120, REQ-551, REQ-1263

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from provisa.auth.models import AuthIdentity, RoleAssignment
from provisa.core.assignments_source import CLAIMS, PROVISA


class NoRoleHeld(PermissionError):  # REQ-120, REQ-802
    """A sign-in whose identity holds no role in the org it signs in to, where Provisa holds the
    assignments: it acts as none, and is refused by name rather than acting as the deployment's
    default role, which nobody granted it."""

    code = "auth.no_role_held"

    def __init__(self, user_id: str) -> None:
        self.params = {"user": user_id}
        super().__init__(f"{user_id!r} holds no role in this organization")


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


def token_scopes(identity: Any) -> list[str] | None:  # REQ-1263
    """The role names a personal access token may act as, in the order the token lists them;
    None for any other credential."""
    if getattr(identity, "credential", None) != "pat":
        return None
    return list(identity.raw_claims["scopes"])


def live_scopes(identity: Any, held: Iterable[str]) -> list[str] | None:  # REQ-1263
    """The scopes of a personal access token that are among ``held`` (its owner's roles, read
    now), in the token's order: the roles it may act as, the first when it names none. None for
    any other credential."""
    scopes = token_scopes(identity)
    if scopes is None:
        return None
    held = set(held)
    return [s for s in scopes if s in held]


async def mirrored_claims(db_pool: Any, user_id: str) -> list[RoleAssignment]:  # REQ-1263
    """The role claims ``user_id``'s identity provider sent at their last sign-in, as the org bound
    now mirrors them (``user_idp_roles``; provisa.entitlements.roles.mirror_idp_roles). Where
    roles come from the sign-in's claims, a personal access token carries no sign-in of its own:
    these are what its owner holds."""
    from sqlalchemy import select

    from provisa.core.schema_org import user_idp_roles

    async with db_pool.acquire() as conn:
        rows = await conn.execute_core(
            select(user_idp_roles.c.role_id, user_idp_roles.c.domain_id)
            .where(user_idp_roles.c.user_id == user_id)
            .order_by(user_idp_roles.c.role_id, user_idp_roles.c.domain_id)
        )
        return [RoleAssignment(role_id=r.role_id, domain_id=r.domain_id) for r in rows.fetchall()]


async def read_held(
    db_pool: Any, identity: AuthIdentity, source: str, default_assignments: list[dict]
) -> list[RoleAssignment]:
    """What ``identity``'s principal holds in the org bound now, before :func:`settle`: from
    ``source`` (``auth.assignments_source``), the control plane's assignments or the sign-in's
    role claims. A personal access token's are its owner's: the same assignments, or the claims
    the owner's last sign-in carried (:func:`mirrored_claims`)."""
    from provisa.auth.role_mapping import resolve_assignments

    if source == PROVISA:
        return await read_assignments(db_pool, identity, default_assignments)
    if token_scopes(identity) is None:
        return resolve_assignments(identity)
    return await mirrored_claims(db_pool, identity.user_id)


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
    identity: Any,
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
    claimant's roles are judged by.

    REQ-1263: ``identity`` is the sign-in the assignments were read for. A personal access token
    keeps, of what its owner holds, only the roles its scopes name -- the one place a token's
    identity becomes roles, for every transport."""
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
    scopes = token_scopes(identity)
    if scopes is not None:
        assignments = [a for a in assignments if a.role_id in scopes]
    return assignments


async def held_assignments(
    state: Any, identity: AuthIdentity, org_id: str | None
) -> list[RoleAssignment]:
    """``identity``'s assignments, from the source the deployment names, read in ``org_id`` (the
    org its sign-in is bound to; None, the default org's)."""
    from provisa.api.org_runtime import ActiveOrgPool
    from provisa.core.request_context import reset_current_org, set_current_org

    auth = state.auth_config or {}
    token = set_current_org(org_id) if org_id is not None else None
    try:
        read = await read_held(
            ActiveOrgPool(),
            identity,
            auth.get("assignments_source", CLAIMS),
            auth.get("default_assignments") or [],
        )
    finally:
        if token is not None:
            reset_current_org(token)
    return await settle(
        read,
        identity=identity,
        claimant=await is_bootstrap_claimant(state, identity.user_id),
        org_id=org_id,
        # A single-tenant deployment has no tenant org (as the HTTP middleware reads it).
        root_org_id=_root_org_id(state) if getattr(state, "multitenancy", False) else None,
    )


async def sign_in_roles(
    state: Any, identity: AuthIdentity, org_id: str | None, *, naming: bool
) -> tuple[str, list[str]]:  # REQ-273, REQ-1263, REQ-120
    """What a sign-in acts as on a transport that is not the HTTP API: the role it acts as when
    it names none, and the roles it may name (that role first). ``naming``: whether the sign-in
    names a role, or may later (a Bolt session picks a database, an MCP call names a role); one
    that does not, where roles are the sign-in's claims, is answered from its claims alone, with
    nothing read.

    The role an identity's claims map to (``role_mapping``, else ``default_role``) is what it acts
    as, beside the roles it holds. Where Provisa holds the assignments they are read on every
    sign-in, and a mapped role the identity does not hold gives way to one it holds, as on the HTTP
    API: a sign-in acts as a role it holds, on every transport (REQ-120, amended 2026-11-01;
    REQ-802). One that holds none is refused by name (:class:`NoRoleHeld`).

    A personal access token acts only as its scopes its owner still holds (REQ-1263): the first of
    them when it names none, never the mapped role. With none held it is refused."""
    from provisa.auth.role_mapping import resolve_role

    auth = state.auth_config
    default_role = auth.get("default_role")
    if not default_role:
        # No admin default: an identity matching no mapping rule is refused, not escalated.
        raise PermissionError("identity matched no role and no default_role is configured")
    mapped = resolve_role(identity, auth.get("role_mapping", []), default_role)
    provisa_held = auth.get("assignments_source", CLAIMS) == PROVISA
    # Where Provisa holds the assignments the mapped role says nothing about what this identity
    # holds, so they are read on every sign-in, naming a role or not.
    if token_scopes(identity) is None and not naming and not provisa_held:
        return mapped, [mapped]
    held = list(dict.fromkeys(a.role_id for a in await held_assignments(state, identity, org_id)))
    live = live_scopes(identity, held)
    if live is not None:
        if not live:
            from provisa.security.role_probe import NOT_PERMITTED

            raise PermissionError(NOT_PERMITTED)
        return live[0], live
    if provisa_held:
        if not held:
            raise NoRoleHeld(identity.user_id)
        if mapped not in held:
            mapped = held[0]
    return mapped, [mapped, *[r for r in held if r != mapped]]


async def may_act_cross_org(state: Any, identity: AuthIdentity) -> bool:  # REQ-1337, REQ-802
    """Whether ``identity`` holds the cross_org right, judged from what its principal holds on the
    platform plane (the deployment's own org) -- its assignments where Provisa holds them, a
    token's scopes its owner still holds -- as the HTTP API judges it, never from the role claims
    a credential carries. Read before any org is bound to the sign-in."""
    from provisa.core.request_context import current_org
    from provisa.security.rights import can_act_cross_org, capabilities_for_claims

    unbound = current_org.set(None)
    try:
        held = await held_assignments(state, identity, None)
    finally:
        current_org.reset(unbound)
    return can_act_cross_org(
        capabilities_for_claims({a.role_id for a in held}, state.platform_roles)
    )
