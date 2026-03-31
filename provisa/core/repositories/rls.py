# Copyright (c) 2025 Kenneth Stott
# Canary: 167bd755-fcdb-478f-8ff9-c11e0cbb9669
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""RLS rule repository — CRUD for row-level security rules in PG config DB."""

import asyncpg

from provisa.core.models import RLSRule
from provisa.core.repositories import table as table_repo


async def upsert(conn: asyncpg.Connection, rule: RLSRule) -> None:
    """Upsert an RLS rule. Resolves table_id from table name."""
    tbl = await table_repo.find_by_table_name(conn, rule.table_id)
    if tbl is None:
        raise ValueError(f"Table not registered: {rule.table_id}")

    await conn.execute(
        """
        INSERT INTO rls_rules (table_id, role_id, filter_expr)
        VALUES ($1, $2, $3)
        ON CONFLICT (table_id, role_id) DO UPDATE SET
            filter_expr = EXCLUDED.filter_expr
        """,
        tbl["id"],
        rule.role_id,
        rule.filter,
    )


async def get_for_table_role(
    conn: asyncpg.Connection, table_id: int, role_id: str
) -> dict | None:
    row = await conn.fetchrow(
        "SELECT * FROM rls_rules WHERE table_id = $1 AND role_id = $2",
        table_id,
        role_id,
    )
    return dict(row) if row else None


async def list_for_role(conn: asyncpg.Connection, role_id: str) -> list[dict]:
    rows = await conn.fetch(
        "SELECT * FROM rls_rules WHERE role_id = $1 ORDER BY id", role_id
    )
    return [dict(r) for r in rows]


async def list_all(conn: asyncpg.Connection) -> list[dict]:
    rows = await conn.fetch("SELECT * FROM rls_rules ORDER BY id")
    return [dict(r) for r in rows]


async def delete(conn: asyncpg.Connection, table_id: int, role_id: str) -> bool:
    result = await conn.execute(
        "DELETE FROM rls_rules WHERE table_id = $1 AND role_id = $2",
        table_id,
        role_id,
    )
    return result == "DELETE 1"
