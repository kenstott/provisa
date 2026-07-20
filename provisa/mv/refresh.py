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
# complexity-gate: allow-ble=3 reason="engine-agnostic table-exists probe (SELECT 1 fails => absent) cannot name the engine-specific exception without re-coupling; the grandfathered refresh_mv outer catch; and the REQ-877 best-effort row-delta capture catch (mandated best-effort — must never fail the refresh)"

"""Materialized view refresh engine (REQ-081, REQ-084).

Background asyncio task that refreshes stale MVs on schedule.
Uses the engine CTAS for initial creation, DELETE+INSERT for refresh.
"""

# Requirements: REQ-135, REQ-158, REQ-160, REQ-199, REQ-234, REQ-235

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

from provisa.mv.bitemporal import append_sql, create_sql, system_columns_ddl
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.otel_compat import get_tracer as _get_tracer

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)


def _now_ts_literal() -> str:
    """One system-time stamp for a refresh, as an engine-agnostic SQL literal. Refreshes are
    serialized per MV and spaced by refresh_interval, so successive stamps strictly increase —
    which is what the append-only reconstruction relies on to order versions (REQ-1159)."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    return f"TIMESTAMP '{ts}'"


async def _refresh_bitemporal(
    engine,
    mv: MVDefinition,
    target: str,
    select_sql: str,
    table_exists: bool,
    existing_cols: list[str],
) -> None:
    """Advance a bitemporal MV by APPENDING this refresh (REQ-1159): first materialization creates
    the log; subsequent refreshes append a full snapshot or an engine-computed delta. No UPDATE/
    DELETE of history ever runs. A view-definition column change is the one exception — the append
    log's business shape no longer matches, so the log is rebuilt (history reset) and surfaced."""
    spec = mv.bitemporal
    assert spec is not None
    now_ts = _now_ts_literal()
    if not table_exists:
        await engine.execute_engine(create_sql(target, select_sql, spec, now_ts))
        return

    sys_names = {c for c, _ in system_columns_ddl(spec)}
    existing_business = [c for c in existing_cols if c not in sys_names]
    new_cols = list(
        (await engine.execute_engine(f"SELECT * FROM ({select_sql}) _shape LIMIT 0")).column_names
    )
    if new_cols != existing_business:
        log.info(
            "MV %s: bitemporal target %s business shape drifted (%d→%d cols) — rebuilding "
            "(history reset)",
            mv.id,
            target,
            len(existing_business),
            len(new_cols),
        )
        await engine.execute_engine(f"DROP TABLE {target}")
        await engine.execute_engine(create_sql(target, select_sql, spec, now_ts))
        return

    for stmt in append_sql(target, select_sql, spec, new_cols, now_ts, engine.dialect):
        await engine.execute_engine(stmt)


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


async def _read_target_rows(engine, target: str) -> list[dict]:  # REQ-877
    """Read the full target row set as column-keyed dicts — the snapshot the row-level delta diff
    (REQ-877) is computed on. Only called when an MV opts into row-delta capture."""
    res = await engine.execute_engine(f"SELECT * FROM {target}")
    return [dict(zip(res.column_names, row, strict=True)) for row in res.rows]


def _captures_deltas(mv: MVDefinition, store) -> bool:  # REQ-877
    """The MV opted into row-delta capture AND a store holds the ledger (its home)."""
    return mv.capture_row_deltas and store is not None


async def _snapshot_prev_rows(  # REQ-877
    engine, mv: MVDefinition, store, target: str, *, table_exists: bool
) -> list[dict]:
    """Prior landed rows for the delta diff, read BEFORE any mutation. Empty unless this MV captures
    deltas and the target already exists (a first refresh has an empty prior set ⇒ all inserts)."""
    if _captures_deltas(mv, store) and table_exists:
        return await _read_target_rows(engine, target)
    return []


async def _post_refresh_delta_capture(  # REQ-877
    engine, mv: MVDefinition, store, prev_rows: list[dict], target: str
) -> None:
    """Best-effort row-level delta capture OFF the refresh critical path: diff the prior and freshly
    landed row sets into the append-only ledger. Runs AFTER the refresh is committed and marked
    fresh, so a slow or failed capture never delays or fails the refresh (REQ-877's mandate).
    Documented blind catch — justified by REQ-877's best-effort rule."""
    if not _captures_deltas(mv, store):
        return
    from provisa.mv.delta import capture_row_deltas  # noqa: PLC0415

    try:
        curr_rows = await _read_target_rows(engine, target)
        await capture_row_deltas(
            store, mv, prev_rows, curr_rows, definition_version=_mv_definition_version(mv)
        )
    except Exception:  # noqa: BLE001 — REQ-877: best-effort delta capture never fails refresh
        log.exception("MV %s: row-level delta capture failed (refresh unaffected)", mv.id)


async def _probe_source_count(engine, mv: MVDefinition) -> int:  # REQ-235
    """Run a COUNT(*) probe against the MV's source query to estimate result size."""
    select_sql = await _build_refresh_sql(mv, engine)
    res = await engine.execute_engine(f"SELECT COUNT(*) FROM ({select_sql}) _probe")
    return res.rows[0][0]


async def refresh_mv(  # REQ-135, REQ-160, REQ-235, REQ-879
    engine,
    mv: MVDefinition,
    registry: MVRegistry,
    store=None,
    writer: str | None = None,
) -> None:
    """Refresh a single MV through the engine terminal.

    First refresh: CREATE TABLE AS SELECT.
    Subsequent: DELETE FROM target; INSERT INTO target SELECT.
    Skips materialization if source row count exceeds max_rows.

    REQ-879: when ``store`` (the shared control-plane catalog) is provided and the MV is on the
    ``shared`` consistency tier, the refresh is driven off an ATOMIC CLAIM on the shared
    ``materialized_views`` row — exactly one fleet instance refreshes a given MV at a time. A
    second concurrent instance sees the live lease, its claim returns 0 rows, and it skips. The
    result is finalized with a FENCED COMMIT (only while this instance still owns a live lease);
    a lost lease discards the result rather than clobbering a newer refresh. When ``store`` is
    None or the MV is ``distributed``, refresh is per-instance (the distributed tier)."""
    from provisa.mv.input_signals import gather_input_signals, input_token  # noqa: PLC0415

    coordinated = store is not None and mv.consistency == "shared"
    if coordinated and writer is None:
        from provisa.mv.coordination import INSTANCE_WRITER  # noqa: PLC0415

        writer = INSTANCE_WRITER

    target = _target_ref(mv)
    start = time.time()

    # Input signals are gathered up front: the input token is both the REQ-881 probe key and
    # the REQ-879 claim dedup key (the REQ-862 stamp of the source state being materialized).
    input_signals = await gather_input_signals(engine, mv.source_tables)  # REQ-862
    target_token = input_token(input_signals, mv.source_tables)

    if coordinated:
        assert store is not None and writer is not None  # coordinated ⇒ both set (see above)
        from provisa.mv.coordination import claim_refresh, ensure_mv_row  # noqa: PLC0415

        # The in-memory registry never writes the control-plane catalog row; seed it here so
        # the atomic claim below has a row to elect on (else 0 rows → permanent STALE).
        await ensure_mv_row(store, mv)
        claimed = await claim_refresh(store, mv.id, writer, target_token)
        if not claimed:
            # 0 rows: another fleet instance owns this refresh, or the store already holds this
            # exact input version. Skip — never race a second writer onto the same relation.
            log.info(
                "MV %s: refresh claim not won (owned by another instance or already current) — skip",
                mv.id,
            )
            return

    registry.mark_refreshing(mv.id)

    try:
        # REQ-881: probe-freshness gate — skip the expensive rebuild when every source reports
        # an unchanged input token. Runs before the size-count probe so even that is skipped.
        if mv.freshness_mode in ("probe", "ttl_probe"):
            if target_token is not None and target_token == mv.last_input_token:
                if coordinated:
                    assert store is not None and writer is not None
                    # Advance the shared version + release the lease without a rebuild.
                    from provisa.mv.coordination import commit_refresh  # noqa: PLC0415

                    await commit_refresh(
                        store,
                        mv.id,
                        writer,
                        row_count=mv.row_count if mv.row_count is not None else 0,
                        input_version=target_token,
                        definition_version=_mv_definition_version(mv),
                        snapshot_id=None,
                    )
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
            if coordinated:
                assert store is not None and writer is not None
                from provisa.mv.coordination import release_refresh  # noqa: PLC0415

                await release_refresh(store, mv.id, writer, None)
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
        # SELECT * (not SELECT 1) so column_names carries the existing target shape.
        try:
            existing_cols = (
                await engine.execute_engine(f"SELECT * FROM {target} LIMIT 0")
            ).column_names
            table_exists = True
        except Exception:
            existing_cols = []
            table_exists = False

        # REQ-877: snapshot the prior landed rows BEFORE any mutation, so the post-refresh diff sees
        # the true previous state (empty unless this MV captures deltas and the target exists).
        prev_rows = await _snapshot_prev_rows(engine, mv, store, target, table_exists=table_exists)

        if mv.bitemporal is not None:
            # REQ-1159: append-only bitemporal maintenance — never DELETE/UPDATE the history.
            await _refresh_bitemporal(engine, mv, target, select_sql, table_exists, existing_cols)
        else:
            # DELETE+INSERT only reconciles rows, not shape. If the view SQL was edited so its
            # column set no longer matches the existing target (count or names), INSERT would
            # mismatch — "table T has N columns but M values were supplied". Rebuild instead.
            if table_exists:
                new_cols = (
                    await engine.execute_engine(f"SELECT * FROM ({select_sql}) _shape LIMIT 0")
                ).column_names
                if new_cols != existing_cols:
                    log.info(
                        "MV %s: target %s shape drifted (%d→%d cols) — rebuilding",
                        mv.id,
                        target,
                        len(existing_cols),
                        len(new_cols),
                    )
                    await engine.execute_engine(f"DROP TABLE {target}")
                    table_exists = False

            if table_exists:
                await engine.execute_engine(f"DELETE FROM {target}")
                await engine.execute_engine(f"INSERT INTO {target} {select_sql}")
            else:
                await engine.execute_engine(f"CREATE TABLE {target} AS {select_sql}")

        # Get row count
        row_count = (await engine.execute_engine(f"SELECT COUNT(*) FROM {target}")).rows[0][0]

        duration = time.time() - start
        if coordinated:
            assert store is not None and writer is not None
            # FENCED COMMIT: finalize only while this instance still owns a live lease. A lost
            # lease (slow / crashed-then-revived) discards the result — never clobber a newer refresh.
            from provisa.mv.coordination import commit_refresh  # noqa: PLC0415

            committed = await commit_refresh(
                store,
                mv.id,
                writer,
                row_count=row_count,
                input_version=target_token,
                definition_version=_mv_definition_version(mv),
                snapshot_id=None,
            )
            if not committed:
                log.warning("MV %s: lease lost during refresh — result discarded (fencing)", mv.id)
                return
        registry.mark_refreshed(mv.id, row_count)
        mv.last_input_token = target_token  # REQ-881
        log.info(
            "Refreshed MV %s: %d rows in %.1fs",
            mv.id,
            row_count,
            duration,
        )
        await _post_refresh_delta_capture(engine, mv, store, prev_rows, target)  # REQ-877
    except Exception as e:
        if coordinated:
            assert store is not None and writer is not None
            from provisa.mv.coordination import release_refresh  # noqa: PLC0415

            await release_refresh(store, mv.id, writer, str(e))
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
