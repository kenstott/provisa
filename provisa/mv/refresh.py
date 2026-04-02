# Copyright (c) 2025 Kenneth Stott
# Canary: 19fe4b3b-8f72-42d9-88b9-1989c69b7663
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Materialized view refresh engine (REQ-081, REQ-084).

Background asyncio task that refreshes stale MVs on schedule.
Uses Trino CTAS for initial creation, DELETE+INSERT for refresh.
"""

from __future__ import annotations

import asyncio
import logging
import time

import trino

from provisa.mv.models import MVDefinition
from provisa.mv.registry import MVRegistry

log = logging.getLogger(__name__)


def _build_refresh_sql(mv: MVDefinition, trino_conn=None) -> str:
    """Build the SELECT SQL for an MV refresh.

    For join-pattern MVs, builds a SELECT from the source tables with the join.
    Prefixes right-table columns to avoid duplicate column names.
    For custom SQL MVs, uses the provided SQL directly.
    """
    if mv.sql:
        return mv.sql

    if mv.join_pattern:
        jp = mv.join_pattern
        # Prefix all right-table columns as "right_table__col" to avoid
        # duplicate column names when both tables share column names like "id".
        right_cols = ""
        if trino_conn:
            try:
                cursor = trino_conn.cursor()
                cursor.execute(f'SHOW COLUMNS FROM "{jp.right_table}"')
                cols = [row[0] for row in cursor.fetchall()]
                right_cols = ", ".join(
                    f'"{jp.right_table}"."{c}" AS "{jp.right_table}__{c}"'
                    for c in cols
                )
            except Exception:
                log.warning(
                    "Could not introspect columns for %s, falling back",
                    jp.right_table,
                )

        if right_cols:
            select_clause = f'"{jp.left_table}".*, {right_cols}'
        else:
            # Fallback: just take left.* (loses right-side non-join columns)
            select_clause = f'"{jp.left_table}".*'

        return (
            f'SELECT {select_clause} FROM "{jp.left_table}" '
            f'{jp.join_type.upper()} JOIN "{jp.right_table}" '
            f'ON "{jp.left_table}"."{jp.left_column}" = '
            f'"{jp.right_table}"."{jp.right_column}"'
        )

    raise ValueError(f"MV {mv.id} has neither sql nor join_pattern defined")


def _target_ref(mv: MVDefinition) -> str:
    """Build the fully qualified target table reference."""
    return f'"{mv.target_catalog}"."{mv.target_schema}"."{mv.target_table}"'


async def refresh_mv(
    trino_conn: trino.dbapi.Connection,
    mv: MVDefinition,
    registry: MVRegistry,
) -> None:
    """Refresh a single MV via Trino.

    First refresh: CREATE TABLE AS SELECT.
    Subsequent: DELETE FROM target; INSERT INTO target SELECT.
    """
    registry.mark_refreshing(mv.id)
    target = _target_ref(mv)
    select_sql = _build_refresh_sql(mv, trino_conn)

    start = time.time()
    try:
        cursor = trino_conn.cursor()

        # Check if target table exists
        try:
            cursor.execute(f"SELECT 1 FROM {target} LIMIT 0")
            cursor.fetchall()
            table_exists = True
        except trino.exceptions.TrinoUserError:
            table_exists = False

        if table_exists:
            cursor.execute(f"DELETE FROM {target}")
            cursor.fetchall()
            cursor.execute(f"INSERT INTO {target} {select_sql}")
            cursor.fetchall()
        else:
            cursor.execute(f"CREATE TABLE {target} AS {select_sql}")
            cursor.fetchall()

        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {target}")
        row_count = cursor.fetchone()[0]

        duration = time.time() - start
        registry.mark_refreshed(mv.id, row_count)
        log.info(
            "Refreshed MV %s: %d rows in %.1fs", mv.id, row_count, duration,
        )
    except Exception as e:
        registry.mark_refresh_failed(mv.id, str(e))
        log.exception("Failed to refresh MV %s", mv.id)


async def refresh_loop(
    trino_conn: trino.dbapi.Connection,
    registry: MVRegistry,
    check_interval: int = 30,
) -> None:
    """Background loop that checks for and refreshes due MVs.

    Args:
        trino_conn: Trino connection for executing refresh queries.
        registry: MV registry to check for due MVs.
        check_interval: Seconds between checks for due MVs.
    """
    while True:
        try:
            due = registry.get_due_for_refresh()
            for mv in due:
                await refresh_mv(trino_conn, mv, registry)
        except Exception:
            log.exception("Error in MV refresh loop")
        await asyncio.sleep(check_interval)
