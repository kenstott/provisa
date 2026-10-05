# Copyright (c) 2026 Kenneth Stott
# Canary: 9de76f14-e675-473d-9e5b-d3c74e7168d5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Role repository — CRUD for roles, via SQLAlchemy Core (dialect-portable)."""

# Requirements: REQ-042, REQ-059, REQ-060, REQ-215

from provisa.core import model_change
from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select

from provisa.core.models import Role
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.repositories.origin import SEED, take_over
from provisa.core.repositories.origin import require as require_origin
from provisa.core.schema_org import roles
from provisa.security.rights import ORG_ADMIN_ROLE, PLATFORM_ADMIN_ROLE

if TYPE_CHECKING:
    from provisa.core.database import Connection


class RoleDeleteRefused(Exception):
    """A role that may not be deleted, and why. ``reason`` is ``"system"`` (a role the
    deployment defines) or ``"dependents"`` (other objects refer to it; ``dependents`` lists
    them: whoever holds it, the roles that inherit from it, every grant that names it)."""

    def __init__(
        self, role_id: str, reason: str, dependents: "list[Dependent] | None" = None
    ) -> None:
        self.role_id = role_id
        self.reason = reason
        self.dependents = dependents or []
        if reason == "dependents":
            named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in self.dependents)
            message = f"Role {role_id!r} is still referred to by: {named}"
        else:
            message = f"Role {role_id!r} is a system role and cannot be deleted"
        super().__init__(message)


async def upsert(  # REQ-042, REQ-059, REQ-060, REQ-1174, REQ-1919
    conn: "Connection", role: Role, *, org_id: str | None, origin: str
) -> None:
    """Create the role, or replace its definition.

    ``origin`` says where the role comes from (``repositories.origin``): written when the role
    is CREATED and left alone after, except that a config load takes over a role made through
    the admin. ``org_id`` is the org an administrator created it in (tenancy); it is recorded at
    creation and never changed, and is None for a role no administrator of an org created.
    """
    model_change.name("upsert", "role", role.id)  # REQ-1524
    require_origin(origin)
    if role.id in (PLATFORM_ADMIN_ROLE, ORG_ADMIN_ROLE):
        # REQ-1349: org_admin is refused on the same terms as platform_admin below. The shipped
        # install config redefined it WITHOUT `org_settings`/`observability`, and config load runs
        # after apply_tenancy_role_grants and overwrites `capabilities` wholesale — so an org
        # administrator lost every admin right the seed had just granted and the Admin tab vanished.
        # The two admin roles' definitions are the seed's, not a config file's.
        #
        # REQ-1297: platform_admin's definition belongs to schema.sql alone — it is the control-plane
        # role and holds no standing data capabilities. Config files and the roles admin surface used
        # to be able to redefine it, and the shipped install config did exactly that: it re-granted
        # source_registration/table_registration/query_development/approve_view and
        # domain_access ['*'] over the seeded row in every org schema the config loaded into. Refusing
        # the write here is what makes "platform_admin has no rights to tenant org data" hold for a
        # deployment that loads a config, not just a bare one.
        return
    await require_residency_grant_saved(conn, role.id, role.capabilities, role.residency_values)
    await conn.upsert(
        roles,
        {
            "id": role.id,
            "capabilities": role.capabilities,  # JSON column — list passes through
            "domain_access": role.domain_access,
            "residency_values": role.residency_values,  # REQ-1921
            # REQ-1174: per-role rate + query-complexity limits; None = unlimited (column NULL).
            "rate_limit": role.rate_limit.model_dump() if role.rate_limit is not None else None,
            "parent_role_id": role.parent_role_id,  # REQ-1677
            "org_id": org_id,
            "origin": origin,
        },
        index_elements=["id"],
        update_columns=[
            "capabilities",
            "domain_access",
            "residency_values",
            "rate_limit",
            "parent_role_id",
        ],
    )
    await take_over(
        conn, roles, (roles.c.id == role.id,), kind="role", ident=role.id, origin=origin
    )


async def require_residency_grant_saved(
    conn: "Connection", role_id: str, capabilities: list[str], values: list[str]
) -> None:
    """The save refuses what the load refuses (REQ-1921, ``regions.require_residency_grant``):
    data_residency only where the platform declares regions, naming only the org's regions and
    "no region"."""
    from provisa.core import process_region
    from provisa.core.regions import DEFAULT_REGION, require_residency_grant
    from provisa.core.repositories.region import list_regions

    selected = (
        None
        if process_region.region() == DEFAULT_REGION
        else [r.id for r in await list_regions(conn)]
    )
    require_residency_grant(role_id, capabilities, values, selected)


async def get(conn: "Connection", role_id: str) -> dict | None:  # REQ-042, REQ-215
    result = await conn.execute_core(select(roles).where(roles.c.id == role_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-042, REQ-059
    result = await conn.execute_core(select(roles).order_by(roles.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def delete(conn: "Connection", role_id: str) -> bool:  # REQ-042, REQ-1677, REQ-1918
    """Delete one role: THE delete, for every surface. False when there is no such role.

    Refused (:class:`RoleDeleteRefused`) for a role the deployment seeds — its origin is
    ``"seed"`` — and while anything depends on it (REQ-1918): a user
    who holds it, a role that inherits from it, a grant or ownership that names it. Its row
    filters go with it. One transaction; no database cascade is relied on.
    """
    model_change.name("delete", "role", role_id)  # REQ-1524
    ref = ObjectRef("role", role_id)
    async with conn.transaction():
        row = await get(conn, role_id)
        if row is None:
            return False
        if row["origin"] == SEED:
            raise RoleDeleteRefused(role_id, "system")
        blocking = await guard(conn, ref)
        if blocking:
            raise RoleDeleteRefused(role_id, "dependents", blocking)
        await discard(conn, role_id)
    return True


async def discard(conn: "Connection", role_id: str) -> None:
    """Remove a role's parts and its row WITHOUT asking the guard: for a caller that has
    already established it may go — :func:`delete`, and the config loader once its own check of
    everything the file dropped has passed."""
    await remove_parts(conn, ObjectRef("role", role_id))
    await conn.execute_core(_delete(roles).where(roles.c.id == role_id))
