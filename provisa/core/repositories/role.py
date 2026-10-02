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

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select

from provisa.core.models import Role
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
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


async def upsert(  # REQ-042, REQ-059, REQ-060, REQ-1174
    conn: "Connection", role: Role, *, org_id: str | None
) -> None:
    """Create the role, or replace its definition.

    ``org_id`` is recorded when the role is CREATED and never changed after: the org an
    administrator created it in. ``None`` is what the seed writes and marks a role the deployment
    defines — the seeded roles, and a role declared in the deployment's config file — which no
    admin surface deletes (:func:`delete`).
    """
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
    await conn.upsert(
        roles,
        {
            "id": role.id,
            "capabilities": role.capabilities,  # JSON column — list passes through
            "domain_access": role.domain_access,
            # REQ-1174: per-role rate + query-complexity limits; None = unlimited (column NULL).
            "rate_limit": role.rate_limit.model_dump() if role.rate_limit is not None else None,
            "parent_role_id": role.parent_role_id,  # REQ-1677
            "org_id": org_id,
        },
        index_elements=["id"],
        update_columns=["capabilities", "domain_access", "rate_limit", "parent_role_id"],
    )


async def get(conn: "Connection", role_id: str) -> dict | None:  # REQ-042, REQ-215
    result = await conn.execute_core(select(roles).where(roles.c.id == role_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-042, REQ-059
    result = await conn.execute_core(select(roles).order_by(roles.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def delete(conn: "Connection", role_id: str) -> bool:  # REQ-042, REQ-1677, REQ-1918
    """Delete one role: THE delete, for every surface. False when there is no such role.

    Refused (:class:`RoleDeleteRefused`) for a role the deployment defines — its row carries no
    org, which is how the seed marks it — and while anything depends on it (REQ-1918): a user
    who holds it, a role that inherits from it, a grant or ownership that names it. Its row
    filters go with it. One transaction; no database cascade is relied on.
    """
    ref = ObjectRef("role", role_id)
    async with conn.transaction():
        row = await get(conn, role_id)
        if row is None:
            return False
        if row["org_id"] is None:
            raise RoleDeleteRefused(role_id, "system")
        blocking = await guard(conn, ref)
        if blocking:
            raise RoleDeleteRefused(role_id, "dependents", blocking)
        await remove_parts(conn, ref)
        await conn.execute_core(_delete(roles).where(roles.c.id == role_id))
    return True


async def delete_all_except(conn: "Connection", keep: list[str]) -> None:
    """Remove every role not named in ``keep``: the config loader's full replace, which empties
    the model of whatever the new config does not declare. Like the deletion of a whole org it
    is outside the one-object rule and does not ask the guard."""
    statement = _delete(roles)
    if keep:
        statement = statement.where(roles.c.id.not_in(keep))
    await conn.execute_core(statement)
