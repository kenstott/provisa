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

from provisa.mv.models import MVDefinition, MVStatus
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


def _probe_source_count(trino_conn: trino.dbapi.Connection, mv: MVDefinition) -> int:
    """Run a COUNT(*) probe against the MV's source query to estimate result size."""
    select_sql = _build_refresh_sql(mv, trino_conn)
    cursor = trino_conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM ({select_sql}) _probe")
    return cursor.fetchone()[0]


async def refresh_mv(
    trino_conn: trino.dbapi.Connection,
    mv: MVDefinition,
    registry: MVRegistry,
) -> None:
    """Refresh a single MV via Trino.

    First refresh: CREATE TABLE AS SELECT.
    Subsequent: DELETE FROM target; INSERT INTO target SELECT.
    Skips materialization if source row count exceeds max_rows.
    """
    registry.mark_refreshing(mv.id)
    target = _target_ref(mv)

    start = time.time()
    try:
        # Size guard: probe source count before materializing
        source_count = _probe_source_count(trino_conn, mv)
        if source_count > mv.max_rows:
            log.warning(
                "MV %s source has %d rows (max_rows=%d) — skipping materialization",
                mv.id, source_count, mv.max_rows,
            )
            mv.status = MVStatus.SKIPPED_SIZE
            mv.last_error = (
                f"Source row count {source_count} exceeds max_rows {mv.max_rows}"
            )
            return

        select_sql = _build_refresh_sql(mv, trino_conn)
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


def reclaim_removed_mvs(
    trino_conn: trino.dbapi.Connection,
    registry: MVRegistry,
    config_mv_ids: set[str],
) -> list[str]:
    """Drop backing tables for MVs removed from config.

    Compares registry against current config MV IDs. MVs in the registry
    but not in config are removed and their backing tables dropped.

    Returns list of reclaimed MV IDs.
    """
    registry_ids = {mv.id for mv in registry.all()}
    removed_ids = registry_ids - config_mv_ids
    reclaimed = []
    for mv_id in removed_ids:
        mv = registry.get(mv_id)
        if mv is None:
            continue
        target = _target_ref(mv)
        try:
            cursor = trino_conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {target}")
            cursor.fetchall()
            log.info("Reclaimed removed MV %s — dropped %s", mv_id, target)
        except Exception:
            log.exception("Failed to drop table for removed MV %s", mv_id)
        reclaimed.append(mv_id)
    # Remove from registry
    for mv_id in reclaimed:
        registry.unregister(mv_id)
    return reclaimed


def detect_orphans(
    trino_conn: trino.dbapi.Connection,
    registry: MVRegistry,
    schema_name: str,
    catalog: str = "postgresql",
) -> list[str]:
    """Detect orphan tables in the MV cache schema not tracked by the registry.

    Returns list of orphan table names.
    """
    cursor = trino_conn.cursor()
    cursor.execute(f'SHOW TABLES FROM "{catalog}"."{schema_name}"')
    actual_tables = {row[0] for row in cursor.fetchall()}

    known_tables = {mv.target_table for mv in registry.all()}
    orphans = actual_tables - known_tables
    if orphans:
        log.warning(
            "Detected %d orphan tables in %s.%s: %s",
            len(orphans), catalog, schema_name, orphans,
        )
    return sorted(orphans)


def drop_expired_orphans(
    trino_conn: trino.dbapi.Connection,
    orphan_tracker: dict[str, float],
    orphan_tables: list[str],
    grace_period: int,
    schema_name: str,
    catalog: str = "postgresql",
) -> list[str]:
    """Drop orphan tables that have exceeded the grace period.

    Args:
        trino_conn: Trino connection.
        orphan_tracker: Dict mapping orphan table name to first-seen timestamp.
        orphan_tables: Current list of orphan table names.
        grace_period: Seconds to wait before dropping.
        schema_name: MV cache schema name.
        catalog: Trino catalog.

    Returns list of dropped table names.
    """
    now = time.time()
    dropped = []

    # Track newly discovered orphans
    for table in orphan_tables:
        if table not in orphan_tracker:
            orphan_tracker[table] = now

    # Remove tables no longer orphaned
    for table in list(orphan_tracker):
        if table not in orphan_tables:
            del orphan_tracker[table]

    # Drop orphans past grace period
    for table, first_seen in list(orphan_tracker.items()):
        if (now - first_seen) >= grace_period:
            target = f'"{catalog}"."{schema_name}"."{table}"'
            try:
                cursor = trino_conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {target}")
                cursor.fetchall()
                log.info("Dropped expired orphan table %s", target)
                dropped.append(table)
            except Exception:
                log.exception("Failed to drop orphan table %s", target)
            del orphan_tracker[table]

    return dropped


async def refresh_loop(
    trino_conn: trino.dbapi.Connection,
    registry: MVRegistry,
    check_interval: int = 30,
    config_mv_ids: set[str] | None = None,
) -> None:
    """Background loop that checks for and refreshes due MVs.

    Also runs storage reclamation and orphan detection each cycle.

    Args:
        trino_conn: Trino connection for executing refresh queries.
        registry: MV registry to check for due MVs.
        check_interval: Seconds between checks for due MVs.
        config_mv_ids: Set of MV IDs from current config (for reclamation).
    """
    orphan_tracker: dict[str, float] = {}

    while True:
        try:
            # Reclaim removed MVs if config IDs provided
            if config_mv_ids is not None:
                reclaim_removed_mvs(trino_conn, registry, config_mv_ids)

            # Orphan detection across all enabled MVs
            schemas_seen: set[tuple[str, str]] = set()
            for mv in registry.all():
                schemas_seen.add((mv.target_catalog, mv.target_schema))
            for catalog, schema in schemas_seen:
                orphans = detect_orphans(trino_conn, registry, schema, catalog)
                # Use shortest grace period from any registered MV
                all_mvs = registry.all()
                grace = min(
                    (m.orphan_grace_period for m in all_mvs),
                    default=86400,
                )
                drop_expired_orphans(
                    trino_conn, orphan_tracker, orphans, grace, schema, catalog,
                )

            due = registry.get_due_for_refresh()
            for mv in due:
                await refresh_mv(trino_conn, mv, registry)
        except Exception:
            log.exception("Error in MV refresh loop")
        await asyncio.sleep(check_interval)
