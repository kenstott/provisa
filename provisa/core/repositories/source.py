# Copyright (c) 2025 Kenneth Stott
# Canary: 4b6b9c56-68fd-47f8-be86-c55348492b7e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source repository — CRUD for data sources in PG config DB."""

import asyncpg

from provisa.core.models import Source


async def upsert(conn: asyncpg.Connection, source: Source) -> None:
    await conn.execute(
        """
        INSERT INTO sources (id, type, host, port, database, username, dialect)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET
            type = EXCLUDED.type,
            host = EXCLUDED.host,
            port = EXCLUDED.port,
            database = EXCLUDED.database,
            username = EXCLUDED.username,
            dialect = EXCLUDED.dialect
        """,
        source.id,
        source.type.value,
        source.host,
        source.port,
        source.database,
        source.username,
        source.dialect,
    )


async def get(conn: asyncpg.Connection, source_id: str) -> dict | None:
    row = await conn.fetchrow("SELECT * FROM sources WHERE id = $1", source_id)
    return dict(row) if row else None


async def list_all(conn: asyncpg.Connection) -> list[dict]:
    rows = await conn.fetch("SELECT * FROM sources ORDER BY id")
    return [dict(r) for r in rows]


async def delete(conn: asyncpg.Connection, source_id: str) -> bool:
    result = await conn.execute("DELETE FROM sources WHERE id = $1", source_id)
    return result == "DELETE 1"
