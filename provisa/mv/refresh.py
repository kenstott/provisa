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

# Requirements: REQ-135, REQ-158, REQ-160, REQ-199, REQ-234, REQ-235, REQ-2120

from __future__ import annotations

from provisa.compiler.sql_literals import sql_literal

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any
from datetime import date, datetime, timezone

from provisa.federation.execution_auth import SystemAuth, mint_system_token
from provisa.mv.bitemporal import append_sql, create_sql, system_columns_ddl, system_ts_literal
from provisa.mv.bitemporal_inherit import InheritedBuild, inherited_build
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.observability.ops_schema import MV_REFRESH_LINEAGE_SPAN
from provisa.otel_compat import detached_trace_context
from provisa.otel_compat import get_tracer as _get_tracer

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)


def _now_ts_literal() -> str:
    """One system-time stamp for a refresh, as an engine-agnostic SQL literal. Refreshes are
    serialized per MV and spaced by refresh_interval, so successive stamps strictly increase —
    which is what the append-only reconstruction relies on to order versions (REQ-1162)."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    return f"TIMESTAMP '{ts}'"


# -- REQ-1901: MV writes into an embedded DuckDB-file store go through its broker ----------------
# The broker stages the engine-computed fresh rows as ``_mv_fresh`` and runs these statements on its
# own store connection; they are the same CTAS / DELETE+INSERT / bitemporal-append statements the
# engine path runs, with the SELECT replaced by the staged rows.
_FRESH = "SELECT * FROM _mv_fresh"


def _mv_store_broker(engine):
    """The store broker an MV refresh writes through, or None when the engine writes the store as
    SQL. An engine terminal without the seam (a test fake, a non-native engine) has no broker."""
    return engine.mv_store_broker() if hasattr(engine, "mv_store_broker") else None


def _require_broker_target(mv: MVDefinition) -> None:
    from provisa.federation.materialize_broker import _MAT_STORE_ALIAS  # noqa: PLC0415

    if mv.target_catalog != _MAT_STORE_ALIAS:
        raise RuntimeError(
            f"MV {mv.id}: target catalog {mv.target_catalog!r} is not the broker-backed store "
            f"{_MAT_STORE_ALIAS!r}"
        )


async def _in_executor(fn):
    """Run a blocking broker/engine call off a plain event loop (on a request/background thread's
    connection loop the executor runs it inline on that thread)."""
    return await asyncio.get_running_loop().run_in_executor(None, fn)


def _create_schema_sql(engine, catalog: str, schema: str) -> str:
    """The statement that creates the store schema ``schema`` if it is absent. An engine whose SQL
    names no catalog (REQ-1730: PostgreSQL, ``catalog_qualified=False``) materializes into its own
    database and names the schema alone; every other engine qualifies it with the catalog."""
    if not engine.engine.catalog_qualified:
        return f'CREATE SCHEMA IF NOT EXISTS "{schema}"'
    return f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."{schema}"'


def _list_tables_sql(engine, catalog: str, schema: str) -> str:
    """The statement listing the tables of the store schema ``schema``, one name per row. An engine
    whose SQL names no catalog (REQ-1730) has no ``SHOW TABLES``: it reads its own
    ``information_schema``. Snowflake spells the listing ``SHOW TABLES IN SCHEMA``; Trino and
    the rest ``FROM``."""
    if not engine.engine.catalog_qualified:
        quoted = schema.replace("'", "''")
        return f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{quoted}'"
    scope = "IN SCHEMA" if getattr(engine, "dialect", "") == "snowflake" else "FROM"
    return f'SHOW TABLES {scope} "{catalog}"."{schema}"'


async def _store_statement(engine, sql: str, authorization: SystemAuth) -> list:
    """Run an MV-maintenance statement that acts on the store alone (DROP, SHOW TABLES): through the
    broker for an embedded DuckDB-file store the engine never ATTACHes (REQ-1901), else as engine
    SQL. Returns the statement's rows."""
    broker = _mv_store_broker(engine)
    if broker is not None:
        return await _in_executor(lambda: broker.execute(sql))
    return (await engine.execute_engine(sql, authorization=authorization)).rows


async def _target_columns(
    engine, mv: MVDefinition, target: str, authorization: SystemAuth
) -> list[str] | None:
    """The columns of ``mv``'s stored table, or None when it is not built yet: the broker's
    listing for an embedded DuckDB-file store (REQ-1901), else an engine probe (``SELECT *`` so
    the column names carry the existing shape; an engine-agnostic probe whose failure means
    absent -- it cannot name the engine-specific exception)."""
    broker = _mv_store_broker(engine)
    if broker is not None:
        return await _in_executor(lambda: broker.table_columns(mv.target_schema, mv.target_table))
    try:
        result = await engine.execute_engine(
            f"SELECT * FROM {target} LIMIT 0", authorization=authorization
        )
    except Exception:
        return None
    return list(result.column_names)


async def _inherited(
    engine, mv: MVDefinition, target: str, authorization: SystemAuth, full: bool
) -> InheritedBuild | None:
    """REQ-2109: this refresh of a view inheriting its versions (``mv.bitemporal_inherit``) or of
    a table's version log (``mv.bitemporal_capture``), or None for any other view."""
    if mv.bitemporal is None or (mv.bitemporal.inherited is None and not mv.bitemporal.captures):
        return None
    existing = await _target_columns(engine, mv, target, authorization)
    if mv.bitemporal.captures:
        from provisa.mv.bitemporal_capture import capture_build

        return await capture_build(engine, mv, authorization, existing, full=full)
    return await inherited_build(engine, mv, authorization, existing, full=full)


def _replace_plan(mv: MVDefinition, target: str):
    def plan(existing: list[str] | None, fresh_cols: list[str]) -> list[str]:
        if existing is None:
            return [f"CREATE TABLE {target} AS {_FRESH}"]
        if existing != fresh_cols:
            log.info(
                "MV %s: target %s shape drifted (%d→%d cols) — rebuilding",
                mv.id,
                target,
                len(existing),
                len(fresh_cols),
            )
            return [f"DROP TABLE {target}", f"CREATE TABLE {target} AS {_FRESH}"]
        return [f"DELETE FROM {target}", f"INSERT INTO {target} {_FRESH}"]

    return plan


def _bitemporal_plan(
    mv: MVDefinition, target: str, now_ts: str, inherited: InheritedBuild | None = None
):
    spec = mv.bitemporal
    assert spec is not None
    # The broker's store is DuckDB: the statements below run there.
    watermark = inherited.watermark_sql("duckdb") if inherited is not None else None
    scope = inherited.scope_sql(spec, "duckdb") if inherited is not None else None

    def plan(existing: list[str] | None, fresh_cols: list[str]) -> list[str]:
        if existing is None:
            return [create_sql(target, _FRESH, spec, now_ts, watermark)]
        sys_names = {c for c, _ in system_columns_ddl(spec)}
        business = [c for c in existing if c not in sys_names]
        if fresh_cols != business:
            log.info(
                "MV %s: bitemporal target %s business shape drifted (%d→%d cols) — rebuilding "
                "(history reset)",
                mv.id,
                target,
                len(business),
                len(fresh_cols),
            )
            if scope is not None:
                # inherited_build rebuilds every date when the shape changed; a scoped build here
                # would reset the log to the rebuilt dates alone.
                raise RuntimeError(f"MV {mv.id}: a scoped build cannot rebuild its log")
            return [f"DROP TABLE {target}", create_sql(target, _FRESH, spec, now_ts, watermark)]
        return list(
            append_sql(
                target,
                _FRESH,
                spec,
                fresh_cols,
                now_ts,
                "duckdb",
                watermark_sql=watermark,
                scope=scope,
            )
        )

    return plan


def _write_via_broker(
    engine, broker, mv: MVDefinition, select_sql: str, plan, authorization: SystemAuth
) -> int:
    """Stream the MV SELECT from the engine as Arrow batches into the broker's write; return the
    target's row count. The reader is consumed batch by batch inside the broker's lock hold. The
    stream carries the refresh's SystemAuth (REQ-1760), verified by the terminal."""
    import pyarrow as pa  # noqa: PLC0415

    schema, batches = engine.execute_engine_stream(select_sql, authorization=authorization)
    try:
        reader = pa.RecordBatchReader.from_batches(schema, batches)
        return broker.write_mv(
            mv.target_schema,
            mv.target_table,
            reader,
            lambda existing: plan(existing, list(schema.names)),
        )
    finally:
        batches.close()


async def _refresh_bitemporal(
    engine,
    mv: MVDefinition,
    target: str,
    select_sql: str,
    table_exists: bool,
    existing_cols: list[str],
    system_ts: str | None = None,
    authorization: SystemAuth | None = None,
    inherited: InheritedBuild | None = None,
) -> None:
    """Advance a bitemporal MV by APPENDING this refresh (REQ-1162): first materialization creates
    the log; subsequent refreshes append a full snapshot or an engine-computed delta. No UPDATE/
    DELETE of history ever runs. A view-definition column change is the one exception — the append
    log's business shape no longer matches, so the log is rebuilt (history reset) and surfaced.

    ``system_ts`` is the append stamp as a SQL literal. None = wall-clock (a live refresh). A CALENDAR
    boundary (``window.end`` via ``system_ts_literal``) makes the seal DETERMINISTIC and addressable
    as-of that window — the periodic-snapshot binding (REQ-1166/1167).

    ``inherited`` is an inherited view's build (REQ-2109): its watermark stamps the append, its
    rebuilt dates confine the tombstones."""
    spec = mv.bitemporal
    assert spec is not None
    now_ts = system_ts or _now_ts_literal()
    watermark = inherited.watermark_sql(engine.dialect) if inherited is not None else None
    scope = inherited.scope_sql(spec, engine.dialect) if inherited is not None else None
    if not table_exists:
        await engine.execute_engine(
            create_sql(target, select_sql, spec, now_ts, watermark), authorization=authorization
        )
        return

    sys_names = {c for c, _ in system_columns_ddl(spec)}
    existing_business = [c for c in existing_cols if c not in sys_names]
    new_cols = list(
        (
            await engine.execute_engine(
                f"SELECT * FROM ({select_sql}) _shape LIMIT 0", authorization=authorization
            )
        ).column_names
    )
    if new_cols != existing_business:
        if scope is not None:
            # inherited_build rebuilds every date when the shape changed; a scoped build here
            # would reset the log to the rebuilt dates alone.
            raise RuntimeError(f"MV {mv.id}: a scoped build cannot rebuild its log")
        log.info(
            "MV %s: bitemporal target %s business shape drifted (%d→%d cols) — rebuilding "
            "(history reset)",
            mv.id,
            target,
            len(existing_business),
            len(new_cols),
        )
        await engine.execute_engine(f"DROP TABLE {target}", authorization=authorization)
        await engine.execute_engine(
            create_sql(target, select_sql, spec, now_ts, watermark), authorization=authorization
        )
        return

    for stmt in append_sql(
        target,
        select_sql,
        spec,
        new_cols,
        now_ts,
        engine.dialect,
        watermark_sql=watermark,
        scope=scope,
    ):
        await engine.execute_engine(stmt, authorization=authorization)


async def apply_bitemporal_append(engine, mv: MVDefinition, *, system_ts: str | None = None) -> str:
    """Append one bitemporal refresh for ``mv`` and return the target ref (REQ-1162). The reusable
    entry point for BOTH refresh paths: the scheduled materializer (wall-clock stamp) and the
    event-loop periodic-snapshot generate (calendar ``window.end`` stamp via ``system_ts``). Ensures
    the target schema, probes the existing shape, and delegates to :func:`_refresh_bitemporal`.

    Mints one SystemAuth token for this entire append (REQ-1760) — the system's own identity,
    not a per-role governance claim, since a refresh always runs unrestricted by design (REQ-1756;
    governance is enforced on read of the MV, not on this landing write)."""
    target = _target_ref(mv)
    authorization = SystemAuth(mint_system_token(), reason=f"mv_bitemporal_append:{mv.id}")
    inherited = await _inherited(engine, mv, target, authorization, full=False)
    if inherited is not None and inherited.select_sql is None:
        return target  # REQ-2109: no input row changed since its watermark — nothing appended
    select_sql = (
        inherited.select_sql
        if inherited is not None and inherited.select_sql is not None
        else await _build_refresh_sql(mv, engine, authorization=authorization)
    )
    broker = _mv_store_broker(engine)
    if broker is not None:
        # REQ-1901: embedded DuckDB-file store — the broker writes the append (see refresh_mv).
        _require_broker_target(mv)
        plan = _bitemporal_plan(mv, target, system_ts or _now_ts_literal(), inherited)
        await _in_executor(
            lambda: _write_via_broker(engine, broker, mv, select_sql, plan, authorization)
        )
        return target
    await engine.execute_engine(
        _create_schema_sql(engine, mv.target_catalog, mv.target_schema),
        authorization=authorization,
    )
    existing = await _target_columns(engine, mv, target, authorization)
    await _refresh_bitemporal(
        engine,
        mv,
        target,
        select_sql,
        existing is not None,
        existing if existing is not None else [],
        system_ts=system_ts,
        authorization=authorization,
        inherited=inherited,
    )
    return target


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
    # The record roots a trace of its own: as a child of a request's span it was dropped in
    # normal detail (the trace-detail sampler) and was no `traces` row on any engine, which
    # keeps only records (REQ-1910). The org it is recorded for is the context's, not the span's.
    with detached_trace_context(), _tracer.start_as_current_span(MV_REFRESH_LINEAGE_SPAN) as span:
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
        # The derivations as one JSON value, the column ops.mv_refresh_lineage reads them from.
        span.set_attribute(
            "lineage.column_derivations",
            json.dumps(
                [
                    {"output": d.output, "sources": list(d.sources), "transform": d.transform}
                    for d in derivations
                ]
            ),
        )


def _mv_definition_version(mv: MVDefinition) -> str:  # REQ-862
    from provisa.mv.delta import definition_version_of

    return definition_version_of(mv)


async def _build_refresh_sql(
    mv: MVDefinition, engine=None, authorization: SystemAuth | None = None
) -> str:
    """Build the SELECT SQL for an MV refresh.

    A SQL-defined or join-pattern view is a view: its definition in the model's terms is built
    through the one pipeline (REQ-1921/1922, ``mv.governed_build``), so every table it reads is
    read as a direct statement reads it.
    """
    if mv.sql or mv.join_pattern:
        from provisa.mv.governed_build import view_build_sql

        return await view_build_sql(mv, engine)
    raise ValueError(f"MV {mv.id} has neither sql nor join_pattern defined")


async def join_pattern_sql(
    mv: MVDefinition,
    ref: "Callable[[str], Awaitable[str]]",
    columns_of: "Callable[[str], Awaitable[list[str]]]",
) -> str:
    """The SELECT of join-pattern ``mv``: its left table's columns, and each other table's
    prefixed ``"{table}__{col}"`` (the convention the MV rewriter rewrites refs to). ``ref(table)``
    names a table the SELECT reads (it is aliased to its registered name) and ``columns_of(table)``
    lists its columns — an engine's addresses and shapes for a physical build, the model's for a
    governed one (``mv.governed_build``)."""
    jp = mv.join_pattern
    assert jp is not None
    bound = dict(zip(mv.source_tables, mv.inputs, strict=True))

    for table in (jp.left_table, jp.right_table, *([jp.via_table] if jp.is_junction else [])):
        if table not in bound:
            raise RuntimeError(
                f"MV {mv.id}: its join pattern names {table!r}, which is not one of the "
                f"tables it was bound to ({', '.join(sorted(bound))})"
            )

    async def _from(table: str) -> str:
        return f'{await ref(table)} AS "{table}"'

    async def _prefixed(table: str) -> str:
        cols = await columns_of(table)
        return ", ".join(f'"{table}"."{c}" AS "{table}__{c}"' for c in cols)

    right_cols = await _prefixed(jp.right_table)

    if jp.is_junction:  # REQ-1586: left -> via -> right, one MV row per edge
        # JoinPattern.__post_init__ admits no junction without a via table and both via keys.
        assert jp.via_table is not None
        # The junction's own columns are the edge's attributes; they are materialized
        # under the same "{table}__{col}" convention the rewriter rewrites refs to.
        via_cols = await _prefixed(jp.via_table)
        join_kw = jp.join_type.upper()
        sql = (
            f'SELECT "{jp.left_table}".*, {via_cols}, {right_cols} '
            f"FROM {await _from(jp.left_table)} "
            f"{join_kw} JOIN {await _from(jp.via_table)} "
            f'ON "{jp.left_table}"."{jp.left_column}" = '
            f'"{jp.via_table}"."{jp.via_left_column}" '
            f"{join_kw} JOIN {await _from(jp.right_table)} "
            f'ON "{jp.via_table}"."{jp.via_right_column}" = '
            f'"{jp.right_table}"."{jp.right_column}"'
        )
        if jp.via_type_column:
            # __post_init__ declares the discriminator column and its value together.
            assert jp.via_type_value is not None
            literal = sql_literal(jp.via_type_value, "postgres")
            sql += f' WHERE "{jp.via_table}"."{jp.via_type_column}" = {literal}'
        return sql

    return (
        f'SELECT "{jp.left_table}".*, {right_cols} FROM {await _from(jp.left_table)} '
        f"{jp.join_type.upper()} JOIN {await _from(jp.right_table)} "
        f'ON "{jp.left_table}"."{jp.left_column}" = '
        f'"{jp.right_table}"."{jp.right_column}"'
    )


def _target_ref(mv: MVDefinition) -> str:
    """The view's stored table, as the engine names it."""
    return mv.stored_ref()


async def _read_target_rows(
    engine, target: str, authorization: SystemAuth | None = None
) -> list[dict]:  # REQ-877
    """Read the full target row set as column-keyed dicts — the snapshot the row-level delta diff
    (REQ-877) is computed on. Only called when an MV opts into row-delta capture."""
    res = await engine.execute_engine(f"SELECT * FROM {target}", authorization=authorization)
    return [dict(zip(res.column_names, row, strict=True)) for row in res.rows]


def _captures_deltas(mv: MVDefinition, ledger) -> bool:  # REQ-877
    """The MV opted into row-delta capture AND a state store holds the ledger (its home)."""
    return mv.capture_row_deltas and ledger is not None


async def _snapshot_prev_rows(  # REQ-877
    engine,
    mv: MVDefinition,
    ledger,
    target: str,
    *,
    table_exists: bool,
    authorization: SystemAuth | None = None,
) -> list[dict]:
    """Prior landed rows for the delta diff, read BEFORE any mutation. Empty unless this MV captures
    deltas and the target already exists (a first refresh has an empty prior set ⇒ all inserts)."""
    if _captures_deltas(mv, ledger) and table_exists:
        return await _read_target_rows(engine, target, authorization=authorization)
    return []


async def _post_refresh_delta_capture(  # REQ-877
    engine,
    mv: MVDefinition,
    ledger,
    prev_rows: list[dict],
    target: str,
    authorization: SystemAuth | None = None,
) -> None:
    """Best-effort row-level delta capture OFF the refresh critical path: diff the prior and freshly
    landed row sets into the append-only ledger. Runs AFTER the refresh is committed and marked
    fresh, so a slow or failed capture never delays or fails the refresh (REQ-877's mandate).
    Documented blind catch — justified by REQ-877's best-effort rule."""
    if not _captures_deltas(mv, ledger):
        return
    from provisa.mv.delta import capture_row_deltas  # noqa: PLC0415

    try:
        curr_rows = await _read_target_rows(engine, target, authorization=authorization)
        await capture_row_deltas(ledger, mv, prev_rows, curr_rows)
    except Exception:  # noqa: BLE001 — REQ-877: best-effort delta capture never fails refresh
        log.exception("MV %s: row-level delta capture failed (refresh unaffected)", mv.id)


async def _probe_source_count(
    engine, select_sql: str, authorization: SystemAuth | None = None
) -> int:  # REQ-235
    """Run a COUNT(*) probe over the build's own statement (REQ-2108: a DIRECT build's is its
    staged rows, so the source is read once) to estimate result size."""
    res = await engine.execute_engine(
        f"SELECT COUNT(*) FROM ({select_sql}) _probe", authorization=authorization
    )
    return res.rows[0][0]


async def _evaluate_preflight(engine, mv: MVDefinition, select_sql: str):
    """Evaluate the MV's preflight check before materializing (REQ-1165). Returns a
    :class:`~provisa.mv.preflight.Verdict`, or ``None`` when the MV declares no check.

    The gate's subject is the MV's INPUTS, streamed per input node (never the materialized output):
    a SQL-expressible check pushes down as an engine-side count probe over the named input node; a
    non-SQL check opens one lazy Arrow stream per input and short-circuits through the compiled
    hook. The two strategies must reach the same verdict for a dataset (REQ-964). The input nodes
    are the SQL-lineage inputs of the view as its definition names them — the names the check is
    written against. ``select_sql`` has had its replica-served inputs renamed to their replicas
    (REQ-1912), so it stands in only for a join-pattern view, which has no SQL of its own; each
    input is addressed where the engine reads it at the read (``preflight_eval``)."""
    source = getattr(mv, "preprocess", None)
    if source is None or not source.strip():
        return None

    from provisa.events.lineage import extract_inputs  # noqa: PLC0415
    from provisa.events.processor import NodeContext  # noqa: PLC0415
    from provisa.mv.preflight_eval import evaluate_streams  # noqa: PLC0415

    ctx = NodeContext(node=mv.id, kind="mv", claimed=[], prior_hash=None)
    inputs = sorted(extract_inputs(mv.sql or select_sql, "postgres"))
    return await evaluate_streams(engine, source, inputs, ctx)


class SealedBuildRefused(ValueError):
    """REQ-2120: a build sealed at a business day's close asked of a view that names no
    calendar -- the calendar is what says when that day closes."""

    def __init__(self, mv_id: str) -> None:
        self.mv_id = mv_id
        super().__init__(
            f"MV {mv_id!r} names no calendar: a build sealed at a business day's close needs one"
        )


class SealedBuildNotDone(ValueError):
    """REQ-2120: a build sealed at a business day's close that appended nothing -- another
    instance held the view's build, or nothing it reads changed since its last build -- so no
    version is recorded at that close."""

    REASONS = {
        "held": "another build of it holds its claim",
        "unchanged": "nothing it reads changed since its last build",
    }

    def __init__(self, mv_id: str, day: date, reason: str) -> None:
        self.mv_id, self.day, self.reason = mv_id, day, reason
        super().__init__(f"MV {mv_id!r} was not sealed at {day}'s close: {self.REASONS[reason]}")


async def _sealed_stamp(mv: MVDefinition, day: date, store) -> str:  # REQ-2120
    """The append stamp of ``mv``'s build sealed at business day ``day``'s close: the end of its
    calendar's window for that date (REQ-2111: the day's close in its zone), as the event loop's
    periodic seal stamps it (``events.handlers.make_mv_bitemporal_generate``). The calendar is
    its latest version in the model store ``store``."""
    from provisa.core.repositories import calendar as calendar_repo
    from provisa.events.calendars import calendar_of, window_for_date

    if mv.calendar is None:
        raise SealedBuildRefused(mv.id)
    if mv.grain is None:
        raise ValueError(f"MV {mv.id!r}: calendar declared without a grain")
    if store is None:
        raise ValueError(f"MV {mv.id!r}: a sealed build reads its calendar from the model store")
    async with store.acquire() as conn:
        row = await calendar_repo.get_latest(conn, mv.calendar)
    if row is None:
        raise ValueError(f"unknown calendar {mv.calendar!r}")
    sealed = window_for_date(calendar_of(row), mv.grain, day, business_day=mv.business_day_grain)
    if sealed is None:
        raise ValueError(
            f"MV {mv.id!r}: calendar {mv.calendar!r} has no {mv.grain} window on {day}"
        )
    return system_ts_literal(sealed.end)


async def refresh_mv(  # REQ-135, REQ-160, REQ-235, REQ-879, REQ-1760
    engine,
    mv: MVDefinition,
    registry: MVRegistry,
    store=None,
    writer: str | None = None,
    ledger=None,
    *,
    full: bool = False,
    window: date | None = None,
) -> None:
    """Refresh a single MV through the engine terminal.

    ``full`` (REQ-2109): a view inheriting its versions ignores its watermark and rebuilds every
    valid date, its tombstones unconfined -- what a row moved between dates or a source that
    hard-deletes needs (``mv.bitemporal_inherit``). Every other view's refresh is full already.

    ``window`` (REQ-2120): build the view sealed at that business day's close -- its append
    stamped by the end of its calendar's window for that date (:func:`_sealed_stamp`), not the
    wall clock. Refused, raised before anything is claimed or recorded, for a view that names
    no calendar (:class:`SealedBuildRefused`), an unknown calendar, or a date its calendar has
    no window on.

    First refresh: CREATE TABLE AS SELECT.
    Subsequent: DELETE FROM target; INSERT INTO target SELECT.
    Skips materialization if source row count exceeds max_rows.

    Mints one SystemAuth token for this entire refresh (REQ-1760) — proves the system's own
    identity is making these execute_engine calls, not a per-role governance claim: a refresh
    always runs unrestricted by design (REQ-1756), with governance enforced on read of the MV,
    not on this landing write.

    REQ-879, REQ-1922: when ``store`` (the org's MODEL store) and ``ledger`` (its region's STATE
    store) are provided and the MV is on the ``shared`` consistency tier, the refresh is driven
    off an ATOMIC CLAIM on the region's ``mv_build_state`` row — exactly one instance of the
    region's fleet refreshes a given MV at a time, and the row names the region that built it. A
    second concurrent instance sees the live lease, its claim returns 0 rows, and it skips. The
    result is finalized with a FENCED COMMIT (only while this instance still owns a live lease);
    a lost lease discards the result rather than clobbering a newer refresh. When ``store`` is
    None or the MV is ``distributed``, refresh is per-instance (the distributed tier).

    REQ-877, REQ-1922: ``ledger`` is the org region's STATE store, which holds the row-delta
    ledger; ``store`` is its MODEL store, which holds the view's catalog row."""
    from provisa.mv.input_signals import gather_input_signals, input_token  # noqa: PLC0415

    # REQ-2120: the seal of a build for a business day; None = the wall clock at each append.
    system_ts = await _sealed_stamp(mv, window, store) if window is not None else None

    # A view is built only from inputs the engine reads whole (provisa/mv/readable_inputs.py). A
    # view saved while its inputs were readable can stop being so (a table switched to row-level
    # replication): its refresh then fails here, before anything reaches the engine or the
    # refresh is claimed, with the reason recorded as the view's error — it is not built from
    # whatever rows requests left cached.
    from provisa.api.app import state  # noqa: PLC0415
    from provisa.mv.readable_inputs import (  # noqa: PLC0415
        ViewInputNotReadable,
        require_readable_inputs,
    )

    # REQ-1921: a view naming another region is built and refreshed only there — refused here
    # before anything is claimed or recorded in this region's state.
    from provisa.mv.governed_build import ViewNotBuildable, require_built_here  # noqa: PLC0415

    try:
        require_built_here(mv)
        await require_readable_inputs(mv, state)
    except (ViewNotBuildable, ViewInputNotReadable) as refused:
        registry.mark_refresh_failed(mv.id, str(refused))
        log.error("MV %s not refreshed: %s", mv.id, refused)
        return

    authorization = SystemAuth(mint_system_token(), reason=f"mv_refresh:{mv.id}")

    coordinated = store is not None and ledger is not None and mv.consistency == "shared"
    if coordinated and writer is None:
        from provisa.mv.coordination import INSTANCE_WRITER  # noqa: PLC0415

        writer = INSTANCE_WRITER

    target = _target_ref(mv)
    start = time.time()

    # Input signals are gathered up front: the input token is both the REQ-881 probe key and
    # the REQ-879 claim dedup key (the REQ-862 stamp of the source state being materialized).
    from provisa.mv.view_inputs import read_tables  # noqa: PLC0415

    inputs = read_tables(mv, state)
    input_signals = await gather_input_signals(engine, inputs)  # REQ-862
    target_token = input_token(input_signals, inputs)

    if coordinated:
        assert ledger is not None and writer is not None  # coordinated ⇒ both set (see above)
        from provisa.mv.coordination import claim_refresh, ensure_mv_row  # noqa: PLC0415

        # The in-memory registry never writes the control-plane catalog row; seed it here so
        # the atomic claim below has a row to elect on (else 0 rows → permanent STALE).
        assert store is not None  # coordinated ⇒ both stores
        await ensure_mv_row(store, ledger, mv)
        claimed = await claim_refresh(ledger, mv.id, writer, target_token)
        if not claimed:
            # 0 rows: another fleet instance owns this refresh, or the store already holds this
            # exact input version. Skip — never race a second writer onto the same relation.
            log.info(
                "MV %s: refresh claim not won (owned by another instance or already current) — skip",
                mv.id,
            )
            if window is not None:
                raise SealedBuildNotDone(mv.id, window, "held")
            return

    registry.mark_refreshing(mv.id)

    try:
        # REQ-881: probe-freshness gate — skip the expensive rebuild when every source reports
        # an unchanged input token. Runs before the size-count probe so even that is skipped.
        if mv.freshness_mode in ("probe", "ttl_probe"):
            if target_token is not None and target_token == mv.last_input_token:
                if coordinated:
                    assert ledger is not None and writer is not None
                    # Advance the shared version + release the lease without a rebuild.
                    from provisa.mv.coordination import commit_refresh  # noqa: PLC0415

                    await commit_refresh(
                        ledger,
                        mv.id,
                        writer,
                        row_count=mv.row_count if mv.row_count is not None else 0,
                        input_version=target_token,
                        definition_version=_mv_definition_version(mv),
                        snapshot_id=None,
                    )
                registry.mark_unchanged(mv.id)
                log.info("MV %s: sources unchanged (probe) — skipped rebuild", mv.id)
                if window is not None:
                    raise SealedBuildNotDone(mv.id, window, "unchanged")
                return

        # REQ-1047: storage guard — refuse to materialize for an org already at its tier's
        # platform-storage allowance. Sits beside the row guard below because the two bound
        # different quantities: max_rows bounds THIS MV, the quota bounds everything the org has
        # accumulated on the operator's disk. Both refuse rather than truncate, and neither
        # applies to an org materializing into a store it owns (REQ-1048).
        from provisa.core.request_context import current_org  # noqa: PLC0415
        from provisa.storage.quota import require_storage_headroom  # noqa: PLC0415

        org_id = current_org.get()
        if org_id is not None:
            from provisa.api.errors import ApiError  # noqa: PLC0415

            try:
                await require_storage_headroom(org_id, operation=f"MV {mv.id} refresh")
            except ApiError as quota_exc:
                # Held as MV state rather than propagated: a refresh runs on the scheduler, where
                # there is no request to answer with a 507. The message is the one the org sees on
                # the MV, and it names the same two exits the API rejection does.
                if coordinated:
                    assert ledger is not None and writer is not None
                    from provisa.mv.coordination import release_refresh  # noqa: PLC0415

                    await release_refresh(ledger, mv.id, writer, quota_exc.detail)
                mv.status = MVStatus.SKIPPED_QUOTA
                mv.last_error = str(quota_exc.detail)
                log.warning("MV %s: %s", mv.id, quota_exc.detail)
                return

        # REQ-2109: a view inheriting its versions rebuilds only the valid dates its input changed
        # since its watermark; with none changed nothing is appended.
        inherited = await _inherited(engine, mv, target, authorization, full)
        if inherited is not None and inherited.select_sql is None:
            if coordinated:
                assert ledger is not None and writer is not None
                from provisa.mv.coordination import commit_refresh  # noqa: PLC0415

                await commit_refresh(
                    ledger,
                    mv.id,
                    writer,
                    row_count=mv.row_count if mv.row_count is not None else 0,
                    input_version=target_token,
                    definition_version=_mv_definition_version(mv),
                    snapshot_id=None,
                )
            registry.mark_unchanged(mv.id)
            log.info("MV %s: no input row changed since its watermark — nothing appended", mv.id)
            if window is not None:
                raise SealedBuildNotDone(mv.id, window, "unchanged")
            return
        # The build's SELECT, built once: the size guard probes it and the build reads it (a
        # column transform's rows are computed for it, REQ-2131 -- once a refresh, not twice).
        select_sql = (
            inherited.select_sql
            if inherited is not None and inherited.select_sql is not None
            else await _build_refresh_sql(mv, engine, authorization=authorization)
        )
        # Size guard: probe source count before materializing
        source_count = await _probe_source_count(engine, select_sql, authorization=authorization)
        if source_count > mv.max_rows:
            log.warning(
                "MV %s source has %d rows (max_rows=%d) — skipping materialization",
                mv.id,
                source_count,
                mv.max_rows,
            )
            if coordinated:
                assert ledger is not None and writer is not None
                from provisa.mv.coordination import release_refresh  # noqa: PLC0415

                await release_refresh(ledger, mv.id, writer, None)
            mv.status = MVStatus.SKIPPED_SIZE
            mv.last_error = f"Source row count {source_count} exceeds max_rows {mv.max_rows}"
            return

        from provisa.mv.governed_build import lineage_sql  # noqa: PLC0415

        # REQ-862
        _emit_column_lineage_span(mv, lineage_sql(mv, select_sql), str(start), input_signals)

        # REQ-1165: preflight CHECK — gate the materialization before any write. A SQL-expressible
        # check pushes down as a count probe over the SELECT (engine-side, no rows to Python); a
        # non-SQL check streams the SELECT as Arrow batches and short-circuits. Anything but CONTINUE
        # skips the rebuild: ABORT is a fatal reject (STALE + error), QUARANTINE a non-fatal hold —
        # neither writes the target, mirroring the size-guard skip above.
        # NOTE: preflight_eval.evaluate_streams (mv/preflight_eval.py) is a separate module with
        # its own execute_engine call site, not yet migrated to REQ-1760 — out of scope here.
        verdict = await _evaluate_preflight(engine, mv, select_sql)
        if verdict is not None and not verdict.is_continue:
            if coordinated:
                assert ledger is not None and writer is not None
                from provisa.mv.coordination import release_refresh  # noqa: PLC0415

                await release_refresh(ledger, mv.id, writer, verdict.reason)
            detail = f"preflight {verdict.decision.value}" + (
                f": {verdict.reason}" if verdict.reason else ""
            )
            if verdict.is_abort:
                registry.mark_refresh_failed(mv.id, detail)  # STALE + last_error (fatal reject)
            else:
                mv.status = MVStatus.SKIPPED_PREFLIGHT  # non-fatal hold
                mv.last_error = detail
            log.warning("MV %s: %s — materialization skipped", mv.id, detail)
            return

        broker = _mv_store_broker(engine)
        if broker is not None:
            # REQ-1901: the store is an embedded DuckDB file the engine connection never ATTACHes —
            # the engine computes the fresh rows, the broker writes them into the store.
            _require_broker_target(mv)
            existing = await _target_columns(engine, mv, target, authorization)
            table_exists = existing is not None
            prev_rows = await _snapshot_prev_rows(
                engine, mv, ledger, target, table_exists=table_exists, authorization=authorization
            )
            plan = (
                _bitemporal_plan(mv, target, system_ts or _now_ts_literal(), inherited)
                if mv.bitemporal is not None
                else _replace_plan(mv, target)
            )
            row_count = await _in_executor(
                lambda: _write_via_broker(engine, broker, mv, select_sql, plan, authorization)
            )
            if mv.bitemporal is None and hasattr(engine, "reconcile_mv_metadata"):
                await engine.reconcile_mv_metadata(
                    schema=mv.target_schema,
                    table=mv.target_table,
                    pk_columns=list(getattr(mv, "primary_key", []) or []) or None,
                )
        else:
            # Ensure the target schema exists before the CTAS. The store's MV-cache schema is created on
            # demand (it need not pre-exist — e.g. a fresh deployment where no source has landed yet).
            await engine.execute_engine(
                _create_schema_sql(engine, mv.target_catalog, mv.target_schema),
                authorization=authorization,
            )

            # Check if target table exists, and its existing shape.
            probed = await _target_columns(engine, mv, target, authorization)
            table_exists = probed is not None
            existing_cols = probed if probed is not None else []

            # REQ-877: snapshot the prior landed rows BEFORE any mutation, so the post-refresh diff sees
            # the true previous state (empty unless this MV captures deltas and the target exists).
            prev_rows = await _snapshot_prev_rows(
                engine, mv, ledger, target, table_exists=table_exists, authorization=authorization
            )

            if mv.bitemporal is not None:
                # REQ-1162: append-only bitemporal maintenance — never DELETE/UPDATE the history.
                await _refresh_bitemporal(
                    engine,
                    mv,
                    target,
                    select_sql,
                    table_exists,
                    existing_cols,
                    system_ts=system_ts,
                    authorization=authorization,
                    inherited=inherited,
                )
            else:
                # DELETE+INSERT only reconciles rows, not shape. If the view SQL was edited so its
                # column set no longer matches the existing target (count or names), INSERT would
                # mismatch — "table T has N columns but M values were supplied". Rebuild instead.
                if table_exists:
                    new_cols = (
                        await engine.execute_engine(
                            f"SELECT * FROM ({select_sql}) _shape LIMIT 0",
                            authorization=authorization,
                        )
                    ).column_names
                    if new_cols != existing_cols:
                        log.info(
                            "MV %s: target %s shape drifted (%d→%d cols) — rebuilding",
                            mv.id,
                            target,
                            len(existing_cols),
                            len(new_cols),
                        )
                        await engine.execute_engine(
                            f"DROP TABLE {target}", authorization=authorization
                        )
                        table_exists = False

                if table_exists:
                    await engine.execute_engine(
                        f"DELETE FROM {target}", authorization=authorization
                    )
                    await engine.execute_engine(
                        f"INSERT INTO {target} {select_sql}", authorization=authorization
                    )
                else:
                    await engine.execute_engine(
                        f"CREATE TABLE {target} AS {select_sql}", authorization=authorization
                    )
                # REQ-1652/1654/1655: the MV's keys, descriptions and tags converge onto the store table
                # it was just created in (or refreshed into) -- on a store that can hold them.
                if hasattr(engine, "reconcile_mv_metadata"):
                    await engine.reconcile_mv_metadata(
                        schema=mv.target_schema,
                        table=mv.target_table,
                        pk_columns=list(getattr(mv, "primary_key", []) or []) or None,
                    )

            # Get row count
            row_count = (
                await engine.execute_engine(
                    f"SELECT COUNT(*) FROM {target}", authorization=authorization
                )
            ).rows[0][0]

        duration = time.time() - start
        if coordinated:
            assert ledger is not None and writer is not None
            # FENCED COMMIT: finalize only while this instance still owns a live lease. A lost
            # lease (slow / crashed-then-revived) discards the result — never clobber a newer refresh.
            from provisa.mv.coordination import commit_refresh  # noqa: PLC0415

            committed = await commit_refresh(
                ledger,
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
        await _post_refresh_delta_capture(
            engine, mv, ledger, prev_rows, target, authorization=authorization
        )  # REQ-877
    except SealedBuildNotDone:
        raise  # its claim is already released and the view is unchanged, not failed
    except Exception as e:
        if coordinated:
            assert ledger is not None and writer is not None
            from provisa.mv.coordination import release_refresh  # noqa: PLC0415

            await release_refresh(ledger, mv.id, writer, str(e))
        registry.mark_refresh_failed(mv.id, str(e))
        log.exception("Failed to refresh MV %s", mv.id)


def refresh_failure(mv: MVDefinition) -> str | None:
    """Why ``mv``'s refresh did not leave it built, as recorded on the view — or None when it
    did. :func:`refresh_mv` never raises; a caller that must answer for the refresh (the admin
    mutation) reads the outcome here."""
    return mv.last_error


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
            await _store_statement(
                engine,
                f"DROP TABLE IF EXISTS {target}",
                SystemAuth(mint_system_token(), reason=f"mv_reclaim:{mv_id}"),
            )
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
    catalog: str,
) -> list[str]:
    """Detect orphan tables in the MV cache schema not tracked by the registry.

    ``catalog`` is required: the store catalog is the ACTIVE engine's
    (``materialize_store_target``), and a ``postgresql`` default silently swept a catalog no
    Trino/DuckDB deployment has.

    Returns list of orphan table names.
    """
    broker = _mv_store_broker(engine)
    if broker is not None:
        # An embedded DuckDB-file store creates a view schema with the first view built into it
        # (write_mv). Until then the schema is not there, and ``SHOW TABLES FROM`` it is a
        # catalog error -- every sweep of a deployment whose views had not yet been built logged
        # "Error in MV reclamation loop". The store is asked what it holds: nothing, there.
        actual_tables = set(await _in_executor(lambda: broker.tables(schema_name)))
    else:
        rows = await _store_statement(
            engine,
            _list_tables_sql(engine, catalog, schema_name),
            SystemAuth(mint_system_token(), reason=f"mv_detect_orphans:{catalog}.{schema_name}"),
        )
        actual_tables = {row[0] for row in rows}

    # REQ-1921: a view naming another region is kept only there — a copy of it here (left when
    # its region changed) is an orphan of this region's store like a removed view's.
    from provisa.federation.replica_converge import builds_here

    from provisa.mv.governed_build import staged_table  # noqa: PLC0415

    # REQ-2108: a view's DIRECT build stages its rows beside it; the staged table is the view's.
    here = [mv for mv in registry.all() if builds_here(mv.region)]
    known_tables = {mv.target_table for mv in here} | {staged_table(mv) for mv in here}
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
    catalog: str,
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
                await _store_statement(
                    engine,
                    f"DROP TABLE IF EXISTS {target}",
                    SystemAuth(mint_system_token(), reason=f"mv_drop_expired_orphan:{target}"),
                )
                log.info("Dropped expired orphan table %s", target)
                dropped.append(table)
            except Exception:
                log.exception("Failed to drop orphan table %s", target)
            del orphan_tracker[table]

    return dropped


async def reclamation_loop(  # REQ-234
    registries: Callable[[], list[tuple[Any, MVRegistry]]],
    check_interval: int = 30,
    config_mv_ids: set[str] | None = None,
    should_run: Callable[[], bool] | None = None,
) -> None:
    """Background STORAGE-RECLAMATION loop (REQ-234): drop MV tables removed from config and
    reap orphaned MV tables past their grace period. It NO LONGER refreshes MVs — the event loop
    (provisa/events) is the sole MV compute path (REQ-966: event-driven recompute-to-current), and
    each MV's periodic cadence is its event-loop poll job (poll_seconds = refresh_interval), so a
    periodic refresh here would double-compute the same target table. Reclamation is separate GC and
    is not expressible as an MV, so it stays a dedicated loop.

    Args:
        registries: Asked at each sweep: every org environment's MV registry (REQ-1266: each
            org's views are its own), each with the EngineRuntime terminal its store is reached
            through for the reclamation DDL.
        check_interval: Seconds between reclamation sweeps.
        config_mv_ids: Set of MV IDs from current config (for removed-MV reclamation).
        should_run: Asked before each sweep (REQ-1900). The sweep drops tables in the SHARED
            materialization store, so of N worker processes one runs it: the server passes its
            scheduler holder's ``holds``. ``None`` sweeps every time.
    """
    orphan_tracker: dict[str, float] = {}

    while True:
        try:
            if should_run is not None and not should_run():
                await asyncio.sleep(check_interval)
                continue
            for engine, registry in registries():
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
