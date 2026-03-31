# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Domain repository — CRUD for domains in PG config DB."""

import asyncpg

from provisa.core.models import Domain


async def upsert(conn: asyncpg.Connection, domain: Domain) -> None:
    await conn.execute(
        """
        INSERT INTO domains (id, description)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET description = EXCLUDED.description
        """,
        domain.id,
        domain.description,
    )


async def get(conn: asyncpg.Connection, domain_id: str) -> dict | None:
    row = await conn.fetchrow("SELECT * FROM domains WHERE id = $1", domain_id)
    return dict(row) if row else None


async def list_all(conn: asyncpg.Connection) -> list[dict]:
    rows = await conn.fetch("SELECT * FROM domains ORDER BY id")
    return [dict(r) for r in rows]


async def delete(conn: asyncpg.Connection, domain_id: str) -> bool:
    result = await conn.execute("DELETE FROM domains WHERE id = $1", domain_id)
    return result == "DELETE 1"
