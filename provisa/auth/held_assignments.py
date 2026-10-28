# Copyright (c) 2026 Kenneth Stott
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
``default_assignments`` when it holds none. HTTP, Bolt, pgwire and every bearer sign-in (Flight,
gRPC, Airport) read them here, so a principal holds the same roles on each.
"""

# Requirements: REQ-120, REQ-551

from __future__ import annotations

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


async def held_assignments(
    state: Any, identity: AuthIdentity, org_id: str | None
) -> list[RoleAssignment]:
    """``identity``'s assignments, from the source the deployment names, read in ``org_id`` (the
    org its sign-in is bound to; None, the default org's)."""
    from provisa.auth.role_mapping import resolve_assignments

    auth = state.auth_config or {}
    source = auth.get("assignments_source", CLAIMS)
    if source != PROVISA:
        return resolve_assignments(identity)
    from provisa.api.org_runtime import ActiveOrgPool
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id) if org_id is not None else None
    try:
        return await read_assignments(
            ActiveOrgPool(), identity, auth.get("default_assignments") or []
        )
    finally:
        if token is not None:
            reset_current_org(token)
