# Copyright (c) 2026 Kenneth Stott
# Canary: 4b6b9c56-68fd-47f8-be86-c55348492b7e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source repository — CRUD for data sources in PG config DB."""

# Requirements: REQ-012, REQ-013, REQ-014, REQ-250

import json as _json

import asyncpg

from provisa.core.models import Source


async def upsert(conn: asyncpg.Connection, source: Source) -> None:  # REQ-012, REQ-250
    await conn.execute(
        """
        INSERT INTO sources (id, type, host, port, database, username, dialect, path, description, mapping)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
        ON CONFLICT (id) DO UPDATE SET
            type = EXCLUDED.type,
            host = EXCLUDED.host,
            port = EXCLUDED.port,
            database = EXCLUDED.database,
            username = EXCLUDED.username,
            dialect = EXCLUDED.dialect,
            path = EXCLUDED.path,
            description = EXCLUDED.description,
            mapping = EXCLUDED.mapping
        """,
        source.id,
        source.type.value,
        source.host,
        source.port,
        source.database,
        source.username,
        source.dialect or "",
        source.path,
        source.description,
        _json.dumps(source.mapping or {}),
    )


async def get(conn: asyncpg.Connection, source_id: str) -> dict | None:  # REQ-012
    row = await conn.fetchrow("SELECT * FROM sources WHERE id = $1", source_id)
    return dict(row) if row else None


async def list_all(conn: asyncpg.Connection) -> list[dict]:  # REQ-012
    rows = await conn.fetch("SELECT * FROM sources ORDER BY id")
    return [dict(r) for r in rows]


async def delete(conn: asyncpg.Connection, source_id: str) -> bool:  # REQ-014
    result = await conn.execute("DELETE FROM sources WHERE id = $1", source_id)
    return result == "DELETE 1"


async def rename(conn: asyncpg.Connection, old_id: str, new_id: str) -> bool:  # REQ-012
    """Rename a source: copy to new_id, retarget registered_tables, delete old_id."""
    async with conn.transaction():
        row = await conn.fetchrow("SELECT * FROM sources WHERE id = $1", old_id)
        if row is None:
            return False
        await conn.execute(
            """
            INSERT INTO sources (id, type, host, port, database, username, dialect, path, description)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO NOTHING
            """,
            new_id,
            row["type"],
            row["host"],
            row["port"],
            row["database"],
            row["username"],
            row["dialect"],
            row["path"],
            row.get("description", ""),
        )
        await conn.execute(
            "UPDATE registered_tables SET source_id = $1 WHERE source_id = $2",
            new_id,
            old_id,
        )
        await conn.execute("DELETE FROM sources WHERE id = $1", old_id)
    return True
