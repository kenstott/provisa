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

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(conn: "Connection", role: Role) -> None:  # REQ-042, REQ-059, REQ-060, REQ-1174
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
