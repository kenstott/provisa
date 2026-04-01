# Copyright (c) 2025 Kenneth Stott
# Canary: b8353796-185d-43e1-810b-0febb812669d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationship repository — CRUD for relationships in PG config DB."""

import asyncpg

from provisa.core.models import Relationship
from provisa.core.repositories import table as table_repo


async def upsert(conn: asyncpg.Connection, rel: Relationship) -> None:
    """Upsert a relationship. Resolves table names to registered_tables IDs."""
    source_tbl = await table_repo.find_by_table_name(conn, rel.source_table_id)
    if source_tbl is None:
        raise ValueError(f"Source table not registered: {rel.source_table_id}")
    target_tbl = await table_repo.find_by_table_name(conn, rel.target_table_id)
    if target_tbl is None:
        raise ValueError(f"Target table not registered: {rel.target_table_id}")

    await conn.execute(
        """
        INSERT INTO relationships (id, source_table_id, target_table_id,
                                   source_column, target_column, cardinality,
                                   materialize, refresh_interval)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO UPDATE SET
            source_table_id = EXCLUDED.source_table_id,
            target_table_id = EXCLUDED.target_table_id,
            source_column = EXCLUDED.source_column,
            target_column = EXCLUDED.target_column,
            cardinality = EXCLUDED.cardinality,
            materialize = EXCLUDED.materialize,
            refresh_interval = EXCLUDED.refresh_interval
        """,
        rel.id,
        source_tbl["id"],
        target_tbl["id"],
        rel.source_column,
        rel.target_column,
        rel.cardinality.value,
        rel.materialize,
        rel.refresh_interval,
    )


async def get(conn: asyncpg.Connection, rel_id: str) -> dict | None:
    row = await conn.fetchrow("SELECT * FROM relationships WHERE id = $1", rel_id)
    return dict(row) if row else None


async def list_all(conn: asyncpg.Connection) -> list[dict]:
    rows = await conn.fetch("SELECT * FROM relationships ORDER BY id")
    return [dict(r) for r in rows]


async def delete(conn: asyncpg.Connection, rel_id: str) -> bool:
    result = await conn.execute("DELETE FROM relationships WHERE id = $1", rel_id)
    return result == "DELETE 1"
