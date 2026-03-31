# Copyright (c) 2025 Kenneth Stott
# Canary: 9de76f14-e675-473d-9e5b-d3c74e7168d5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Role repository — CRUD for roles in PG config DB."""

import asyncpg

from provisa.core.models import Role


async def upsert(conn: asyncpg.Connection, role: Role) -> None:
    await conn.execute(
        """
        INSERT INTO roles (id, capabilities, domain_access)
        VALUES ($1, $2, $3)
        ON CONFLICT (id) DO UPDATE SET
            capabilities = EXCLUDED.capabilities,
            domain_access = EXCLUDED.domain_access
        """,
        role.id,
        role.capabilities,
        role.domain_access,
    )


async def get(conn: asyncpg.Connection, role_id: str) -> dict | None:
    row = await conn.fetchrow("SELECT * FROM roles WHERE id = $1", role_id)
    return dict(row) if row else None


async def list_all(conn: asyncpg.Connection) -> list[dict]:
    rows = await conn.fetch("SELECT * FROM roles ORDER BY id")
    return [dict(r) for r in rows]


async def delete(conn: asyncpg.Connection, role_id: str) -> bool:
    result = await conn.execute("DELETE FROM roles WHERE id = $1", role_id)
    return result == "DELETE 1"
