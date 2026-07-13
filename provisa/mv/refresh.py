# Copyright (c) 2026 Kenneth Stott
# Canary: 19fe4b3b-8f72-42d9-88b9-1989c69b7663
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
# complexity-gate: allow-ble=2 reason="engine-agnostic table-exists probe (SELECT 1 fails => absent) cannot name the engine-specific exception without re-coupling; plus the grandfathered refresh_mv outer catch"

"""Materialized view refresh engine (REQ-081, REQ-084).

Background asyncio task that refreshes stale MVs on schedule.
Uses the engine CTAS for initial creation, DELETE+INSERT for refresh.
"""

# Requirements: REQ-135, REQ-158, REQ-160, REQ-199, REQ-234, REQ-235

from __future__ import annotations

import asyncio
import logging
import time

from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.otel_compat import get_tracer as _get_tracer

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)


def _emit_column_lineage_span(
    mv: MVDefinition, select_sql: str, refresh_epoch: str, input_signals=None
) -> None:  # REQ-862
    """Emit a span capturing this refresh's column-level lineage and version stamps.

    Carries, store-independently (works for Iceberg or RDB targets): per-output-column
    derivation from the view SQL, the MV definition-version (content hash), the resolved
    input-version + its fidelity kind, and the refresh trace_id. Best-effort telemetry:
    a SQL the lineage resolver cannot parse is logged and skipped, never failing the
    refresh.
    """
    from sqlglot.errors import SqlglotError

    from provisa.lineage import (
        lineage_span_attributes,
        resolve_column_lineage,
        resolve_input_version,
    )

    try:
        derivations = resolve_column_lineage(select_sql, dialect="postgres")
    except SqlglotError as exc:
        log.warning("MV %s: column lineage unresolved (%s)", mv.id, exc)
        derivations = []
    input_version = resolve_input_version(input_signals or [], refresh_epoch)
    with _tracer.start_as_current_span("mv.refresh.column_lineage") as span:
        _get_ctx = getattr(span, "get_span_context", None)  # _NoopSpan lacks it
        ctx = _get_ctx() if _get_ctx is not None else None
        trace_id = format(ctx.trace_id, "032x") if ctx is not None else ""
        span.set_attribute("mv.id", mv.id)
        span.set_attribute("mv.target_table", mv.target_table or "")
        span.set_attribute("lineage.definition_version", _mv_definition_version(mv))
        span.set_attribute("lineage.input_version", input_version.value)
        span.set_attribute("lineage.input_version_kind", input_version.kind)
        span.set_attribute("lineage.trace_id", trace_id)
        for key, value in lineage_span_attributes(derivations).items():
            span.set_attribute(key, value)


def _mv_definition_version(mv: MVDefinition) -> str:  # REQ-862
    from provisa.lineage import mv_definition_version

    return mv_definition_version(
        sql=mv.sql,
        join_pattern=mv.join_pattern,
        source_tables=mv.source_tables,
        serves_aggregates=mv.serves_aggregates,
        aggregate_columns=mv.aggregate_columns,
    )


async def _build_refresh_sql(mv: MVDefinition, engine=None) -> str:
    """Build the SELECT SQL for an MV refresh.

    For join-pattern MVs, builds a SELECT from the source tables with the join.
    Prefixes right-table columns to avoid duplicate column names.
    For custom SQL MVs, uses the provided SQL directly. That SQL is already a
    catalog-qualified physical plan — the semantic→physical rewrite happens once at
    schema-rebuild time (app_rebuild._compile_view_sqls), the same point the query
    path compiles view SQL, so the engine never sees an unresolved semantic schema.
    """
    if mv.sql:
        return mv.sql

    if mv.join_pattern:
        jp = mv.join_pattern
        # Prefix all right-table columns as "right_table__col" to avoid
        # duplicate column names when both tables share column names like "id".
        if engine is None:
            raise ValueError(f"MV {mv.id}: engine required to introspect right-table columns")
        try:
            rows = (await engine.execute_engine(f'SHOW COLUMNS FROM "{jp.right_table}"')).rows
        except Exception as exc:
            # Falling back to left.* silently drops right-table columns — fail loud.
            raise RuntimeError(
                f"MV {mv.id}: could not introspect columns for {jp.right_table!r}: {exc}"
            ) from exc
        cols = [row[0] for row in rows]
        right_cols = ", ".join(f'"{jp.right_table}"."{c}" AS "{jp.right_table}__{c}"' for c in cols)
        select_clause = f'"{jp.left_table}".*, {right_cols}'

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


async def _probe_source_count(engine, mv: MVDefinition) -> int:  # REQ-235
    """Run a COUNT(*) probe against the MV's source query to estimate result size."""
    select_sql = await _build_refresh_sql(mv, engine)
    res = await engine.execute_engine(f"SELECT COUNT(*) FROM ({select_sql}) _probe")
    return res.rows[0][0]


async def refresh_mv(  # REQ-135, REQ-160, REQ-235
    engine,
    mv: MVDefinition,
    registry: MVRegistry,
) -> None:
    """Refresh a single MV through the engine terminal.

    First refresh: CREATE TABLE AS SELECT.
    Subsequent: DELETE FROM target; INSERT INTO target SELECT.
    Skips materialization if source row count exceeds max_rows.
    """
    registry.mark_refreshing(mv.id)
    target = _target_ref(mv)

    start = time.time()
    try:
        from provisa.mv.input_signals import gather_input_signals, input_token  # noqa: PLC0415

        input_signals = await gather_input_signals(engine, mv.source_tables)  # REQ-862
        # REQ-881: probe-freshness gate — skip the expensive rebuild when every source reports
        # an unchanged input token. Runs before the size-count probe so even that is skipped.
        if mv.freshness_mode in ("probe", "ttl_probe"):
            token = input_token(input_signals, mv.source_tables)
            if token is not None and token == mv.last_input_token:
                registry.mark_unchanged(mv.id)
                log.info("MV %s: sources unchanged (probe) — skipped rebuild", mv.id)
                return

        # Size guard: probe source count before materializing
        source_count = await _probe_source_count(engine, mv)
        if source_count > mv.max_rows:
            log.warning(
                "MV %s source has %d rows (max_rows=%d) — skipping materialization",
                mv.id,
                source_count,
                mv.max_rows,
            )
            mv.status = MVStatus.SKIPPED_SIZE
            mv.last_error = f"Source row count {source_count} exceeds max_rows {mv.max_rows}"
            return

        select_sql = await _build_refresh_sql(mv, engine)
        _emit_column_lineage_span(mv, select_sql, str(start), input_signals)  # REQ-862

        # Ensure the target schema exists before the CTAS. The store's MV-cache schema is created on
        # demand (it need not pre-exist — e.g. a fresh deployment where no source has landed yet). The
        # catalog-qualified form is portable across the engines that materialize (DuckDB/Trino/
        # Postgres/Databricks/BigQuery all accept CREATE SCHEMA IF NOT EXISTS "catalog"."schema").
        await engine.execute_engine(
            f'CREATE SCHEMA IF NOT EXISTS "{mv.target_catalog}"."{mv.target_schema}"'
        )

        # Check if target table exists — probe through the engine (empty rows on absence).
        try:
            await engine.execute_engine(f"SELECT 1 FROM {target} LIMIT 0")
            table_exists = True
        except Exception:
            table_exists = False

        if table_exists:
            await engine.execute_engine(f"DELETE FROM {target}")
            await engine.execute_engine(f"INSERT INTO {target} {select_sql}")
        else:
            await engine.execute_engine(f"CREATE TABLE {target} AS {select_sql}")

        # Get row count
        row_count = (await engine.execute_engine(f"SELECT COUNT(*) FROM {target}")).rows[0][0]

        duration = time.time() - start
        registry.mark_refreshed(mv.id, row_count)
        mv.last_input_token = input_token(input_signals, mv.source_tables)  # REQ-881
        log.info(
            "Refreshed MV %s: %d rows in %.1fs",
            mv.id,
            row_count,
            duration,
        )
    except Exception as e:
        registry.mark_refresh_failed(mv.id, str(e))
        log.exception("Failed to refresh MV %s", mv.id)


async def reclaim_removed_mvs(  # REQ-234
    engine,
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
            await engine.execute_engine(f"DROP TABLE IF EXISTS {target}")
            log.info("Reclaimed removed MV %s — dropped %s", mv_id, target)
        except Exception:
            log.exception("Failed to drop table for removed MV %s", mv_id)
        reclaimed.append(mv_id)
    # Remove from registry
    for mv_id in reclaimed:
        registry.unregister(mv_id)
    return reclaimed


async def detect_orphans(  # REQ-234
    engine,
    registry: MVRegistry,
    schema_name: str,
    catalog: str = "postgresql",
) -> list[str]:
    """Detect orphan tables in the MV cache schema not tracked by the registry.

    Returns list of orphan table names.
    """
    rows = (await engine.execute_engine(f'SHOW TABLES FROM "{catalog}"."{schema_name}"')).rows
    actual_tables = {row[0] for row in rows}

    known_tables = {mv.target_table for mv in registry.all()}
    orphans = actual_tables - known_tables
    if orphans:
        log.warning(
            "Detected %d orphan tables in %s.%s: %s",
            len(orphans),
            catalog,
            schema_name,
            orphans,
        )
    return sorted(orphans)


async def drop_expired_orphans(  # REQ-234
    engine,
    orphan_tracker: dict[str, float],
    orphan_tables: list[str],
    grace_period: int,
    schema_name: str,
    catalog: str = "postgresql",
) -> list[str]:
    """Drop orphan tables that have exceeded the grace period.

    Args:
        engine: EngineRuntime terminal.
        orphan_tracker: Dict mapping orphan table name to first-seen timestamp.
        orphan_tables: Current list of orphan table names.
        grace_period: Seconds to wait before dropping.
        schema_name: MV cache schema name.
        catalog: the engine catalog.

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
                await engine.execute_engine(f"DROP TABLE IF EXISTS {target}")
                log.info("Dropped expired orphan table %s", target)
                dropped.append(table)
            except Exception:
                log.exception("Failed to drop orphan table %s", target)
            del orphan_tracker[table]

    return dropped


async def reclamation_loop(  # REQ-234
    engine,
    registry: MVRegistry,
    check_interval: int = 30,
    config_mv_ids: set[str] | None = None,
) -> None:
    """Background STORAGE-RECLAMATION loop (REQ-234): drop MV tables removed from config and
    reap orphaned MV tables past their grace period. It NO LONGER refreshes MVs — the event loop
    (provisa/events) is the sole MV compute path (REQ-966: event-driven recompute-to-current), and
    each MV's periodic cadence is its event-loop poll job (poll_seconds = refresh_interval), so a
    periodic refresh here would double-compute the same target table. Reclamation is separate GC and
    is not expressible as an MV, so it stays a dedicated loop.

    Args:
        engine: EngineRuntime terminal for executing reclamation DDL.
        registry: MV registry (source of enabled MVs + target schemas).
        check_interval: Seconds between reclamation sweeps.
        config_mv_ids: Set of MV IDs from current config (for removed-MV reclamation).
    """
    orphan_tracker: dict[str, float] = {}

    while True:
        try:
            # Reclaim removed MVs if config IDs provided
            if config_mv_ids is not None:
                await reclaim_removed_mvs(engine, registry, config_mv_ids)

            # Orphan detection across all enabled MVs
            schemas_seen: set[tuple[str, str]] = set()
            for mv in registry.all():
                schemas_seen.add((mv.target_catalog, mv.target_schema))
            for catalog, schema in schemas_seen:
                orphans = await detect_orphans(engine, registry, schema, catalog)
                # Use shortest grace period from any registered MV
                all_mvs = registry.all()
                grace = min(
                    (m.orphan_grace_period for m in all_mvs),
                    default=86400,
                )
                await drop_expired_orphans(
                    engine,
                    orphan_tracker,
                    orphans,
                    grace,
                    schema,
                    catalog,
                )
        except Exception:
            log.exception("Error in MV reclamation loop")
        await asyncio.sleep(check_interval)
