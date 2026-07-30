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
from provisa.core.schema_org import roles
from provisa.security.rights import ORG_ADMIN_ROLE, PLATFORM_ADMIN_ROLE

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(conn: "Connection", role: Role) -> None:  # REQ-042, REQ-059, REQ-060, REQ-1174
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
        },
        index_elements=["id"],
        update_columns=["capabilities", "domain_access", "rate_limit"],
    )


async def get(conn: "Connection", role_id: str) -> dict | None:  # REQ-042, REQ-215
    result = await conn.execute_core(select(roles).where(roles.c.id == role_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-042, REQ-059
    result = await conn.execute_core(select(roles).order_by(roles.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def delete(conn: "Connection", role_id: str) -> bool:  # REQ-042
    result = await conn.execute_core(_delete(roles).where(roles.c.id == role_id))
    return (result.rowcount or 0) > 0
