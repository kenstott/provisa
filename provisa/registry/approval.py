# Copyright (c) 2026 Kenneth Stott
# Canary: e74ae35f-29f0-4a3d-a5d2-00c097530ea5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Approval workflow — flag affected queries on registration/relationship changes (REQ-025, REQ-020).

Also triggers cache invalidation when registration model changes (REQ-079).
"""

from __future__ import annotations

import asyncpg

from provisa.cache.store import CacheStore


async def invalidate_cache_for_table(cache_store: CacheStore, table_id: int) -> None:
    """Invalidate cached query results that reference a table (REQ-079)."""
    await cache_store.invalidate_by_table(table_id)


async def flag_queries_for_table(conn: asyncpg.Connection, table_id: int) -> int:
    """Flag all approved queries that reference a table for re-review.

    Returns number of queries flagged.
    """
    result = await conn.execute(
        """
        UPDATE persisted_queries
        SET status = 'flagged', updated_at = NOW()
        WHERE status = 'approved' AND $1 = ANY(target_tables)
        """,
        table_id,
    )
    count = int(result.split()[-1]) if result else 0
    if count > 0:
        # Log the flagging
        rows = await conn.fetch(
            """
            SELECT id FROM persisted_queries
            WHERE status = 'flagged' AND $1 = ANY(target_tables)
            """,
            table_id,
        )
        for row in rows:
            await conn.execute(
                "INSERT INTO approval_log (query_id, action, actor_id, reason) "
                "VALUES ($1, 'flagged', 'system', $2)",
                row["id"],
                f"registration change on table_id={table_id}",
            )
    return count


async def flag_queries_for_relationship(
    conn: asyncpg.Connection,
    source_table_id: int,
    target_table_id: int,
) -> int:
    """Flag approved queries using either side of a changed relationship (REQ-020).

    Returns number of queries flagged.
    """
    result = await conn.execute(
        """
        UPDATE persisted_queries
        SET status = 'flagged', updated_at = NOW()
        WHERE status = 'approved'
          AND ($1 = ANY(target_tables) OR $2 = ANY(target_tables))
        """,
        source_table_id,
        target_table_id,
    )
    return int(result.split()[-1]) if result else 0
