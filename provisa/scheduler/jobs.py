# Copyright (c) 2026 Kenneth Stott
# Canary: 2dadb1c4-7dac-45cd-bece-d8d8955e690d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Scheduled trigger execution via APScheduler (REQ-216).

Registers cron-based jobs from config. Supports webhook URLs and
internal function names. Reuses existing async patterns.
"""
# Requirements: REQ-216, REQ-177, REQ-302, REQ-303
# complexity-gate: allow-ble=2 reason="best-effort Iceberg maintenance DDL (partition-spec evolution, snapshot expiration) over a pluggable engine backend whose failure taxonomy is unbounded; each logs exc_info and continues so OTEL signal compaction is never aborted by a maintenance step"

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import httpx
import pyarrow as pa
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

if TYPE_CHECKING:
    from provisa.core.models import ScheduledTrigger

logger = logging.getLogger(__name__)


async def _execute_webhook(
    url: str, trigger_id: str, args: dict | None = None
) -> None:  # REQ-216, REQ-209
    """Fire a webhook for a scheduled trigger."""
    try:
        payload: dict = {"trigger_id": trigger_id}
        if args:
            payload.update(args)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            logger.info("Trigger %s: webhook %s returned %s", trigger_id, url, resp.status_code)
    except Exception:
        logger.exception("Trigger %s: webhook %s failed", trigger_id, url)


# The "admin" role is the platform's well-known system execution role for governed
# internal SQL (same default used by the Flight server, provisa/api/flight/server.py).
# Scheduled triggers carry no per-run identity, so scheduled SQL runs under it.
# REQ-1003: governed execution requires a role; this is the documented system role.
_SCHEDULER_ROLE = "admin"


async def _execute_sql(sql: str, trigger_id: str) -> None:  # REQ-1003, REQ-1004
    """Execute a scheduled SQL statement against the federated engine.

    Substitutes date/timestamp tokens with this run's execution time (REQ-1004),
    routes the statement through the shared governance pipeline (REQ-1003), and
    executes the resulting plan. Failures are logged and re-raised — never
    silently swallowed.
    """
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route
    from provisa.scheduler.templating import substitute_date_tokens

    run_at = datetime.now(timezone.utc)
    rendered = substitute_date_tokens(sql, run_at)
    try:
        plan = await _govern_and_route(rendered, _SCHEDULER_ROLE)
        result = await _execute_plan(plan)
    except Exception:
        logger.exception("Trigger %s: scheduled SQL failed: %s", trigger_id, rendered)
        raise
    logger.info("Trigger %s: scheduled SQL executed (%d rows)", trigger_id, len(result.rows))


async def compact_otel_signals() -> None:  # REQ-302, REQ-303
    """Compact today's OTEL Parquet from MinIO into Iceberg vithe engine.

    Runs every minute. Deletes existing rows for today's partition before
    reinserting so re-runs are idempotent. Set OTEL_COMPACT_DATE (YYYY-MM-DD)
    to backfill a specific date.
    """
    logger.warning("compact_otel: invoked")
    import asyncio
    import os

    from provisa.api.app import state

    override = os.environ.get("OTEL_COMPACT_DATE")
    if override:
        target = datetime.strptime(override, "%Y-%m-%d")
    else:
        target = datetime.now(timezone.utc).replace(tzinfo=None)

    s3_endpoint = os.environ.get("PROVISA_OTEL_S3_ENDPOINT") or state.otel_s3_endpoint
    logger.warning(
        "compact_otel: starting — s3=%s engine=%s",
        s3_endpoint,
        state.federation_engine is not None,
    )
    access_key = os.environ["PROVISA_OTEL_S3_ACCESS_KEY"]
    secret_key = os.environ["PROVISA_OTEL_S3_SECRET_KEY"]
    otel_bucket = os.environ.get("PROVISA_OTEL_BUCKET", "provisa-otel")
    file_chunk = getattr(state, "otel_compact_file_chunk", 50)
    engine = state.federation_engine

    loop = asyncio.get_event_loop()
    for signal in ("logs", "metrics", "traces"):
        if loop.is_closed() or not loop.is_running():
            logger.warning("compact_otel: event loop shutting down, skipping %s", signal)
            return
        try:
            await asyncio.to_thread(
                _compact_signal,
                signal,
                target,
                s3_endpoint,
                access_key,
                secret_key,
                otel_bucket,
                file_chunk,
                engine,
            )
        except asyncio.CancelledError:
            logger.warning("compact_otel: cancelled during shutdown, skipping %s", signal)
            return
        except RuntimeError as e:
            if "shutdown" in str(e).lower() or "closed" in str(e).lower():
                logger.warning("compact_otel: executor shutting down, skipping %s", signal)
                return
            raise


def _compact_signal(
    signal: str,
    target: datetime,
    s3_endpoint: str,
    access_key: str,
    secret_key: str,
    otel_bucket: str,
    file_chunk: int,
    engine,
) -> None:
    """Compact one OTel signal type. Runs entirely in a thread — no event loop blocking."""
    import io
    import os

    import boto3
    from botocore.config import Config as BotoConfig

    assert isinstance(target, datetime)
    year = target.strftime("%Y")
    month = target.strftime("%m")
    day = target.strftime("%d")
    date_glob = f"year={year}/month={month}/day={day}/"

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=BotoConfig(signature_version="s3v4"),
    )

    service = os.environ.get("OTEL_SERVICE_NAME", "provisa")
    prefix = f"{signal}/{service}/{date_glob}"
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=otel_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        logger.debug("compact_otel: no %s files for %s", signal, date_glob)
        return

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.exception("compact_otel: pyarrow not available")
        return

    total_rows = 0
    for chunk_start in range(0, len(keys), file_chunk):
        chunk_keys = keys[chunk_start : chunk_start + file_chunk]
        try:
            parts = []
            for key in chunk_keys:
                obj = s3.get_object(Bucket=otel_bucket, Key=key)
                parts.append(pq.read_table(io.BytesIO(obj["Body"].read())))
            combined = pa.concat_tables(parts, promote_options="default")
        except Exception:
            logger.exception(
                "compact_otel: failed reading %s parquet files (chunk %d)", signal, chunk_start
            )
            return

        try:
            _insert_otel_iceberg(engine, signal, combined, target)
            total_rows += len(combined)
            del_resp = s3.delete_objects(
                Bucket=otel_bucket,
                Delete={"Objects": [{"Key": k} for k in chunk_keys]},
            )
            for err in del_resp.get("Errors", []):
                logger.warning(
                    "compact_otel: s3 delete failed key=%s code=%s msg=%s",
                    err.get("Key"),
                    err.get("Code"),
                    err.get("Message"),
                )
        except Exception:
            logger.exception(
                "compact_otel: failed inserting %s into Iceberg (chunk %d)", signal, chunk_start
            )
            return

    logger.info("compact_otel: inserted %d %s rows for %s", total_rows, signal, date_glob)


_PA_TO_PHYSICAL: dict[object, str] = {
    pa.string(): "VARCHAR",
    pa.large_string(): "VARCHAR",
    pa.int32(): "INTEGER",
    pa.int64(): "BIGINT",
    pa.float32(): "REAL",
    pa.float64(): "DOUBLE",
    pa.bool_(): "BOOLEAN",
    pa.timestamp("ns"): "TIMESTAMP(6)",
    pa.timestamp("us"): "TIMESTAMP(6)",
    pa.timestamp("ms"): "TIMESTAMP(3)",
    pa.date32(): "DATE",
}

_PHYSICAL_TO_PA: dict[str, object] = {
    "varchar": pa.string(),
    "bigint": pa.int64(),
    "integer": pa.int32(),
    "double": pa.float64(),
    "real": pa.float32(),
    "boolean": pa.bool_(),
    "timestamp(6)": pa.timestamp("us"),
    "timestamp(3)": pa.timestamp("ms"),
    "date": pa.date32(),
}

_PHYSICAL_CAST: dict[str, str] = {
    "bigint": "BIGINT",
    "integer": "INTEGER",
    "double": "DOUBLE",
    "real": "REAL",
    "boolean": "BOOLEAN",
    "timestamp(6)": "TIMESTAMP(6)",
    "timestamp(3)": "TIMESTAMP(3)",
    "date": "DATE",
    "varchar": "VARCHAR",
}

_TRACE_EXTRA_COLS: list[str] = ["table_name", "domain_id", "role_id", "query_text"]
_ATTR_KEYS: dict[str, str] = {
    "table_name": "provisa.table",
    "domain_id": "provisa.domain",
    "role_id": "provisa.role",
    "query_text": "provisa.query_text",
}
_SPAN_ATTRS_MAX: int = 4096


def _build_iceberg_col_defs(signal: str, table: pa.Table) -> list[str]:
    """Build Iceberg column definition strings from a PyArrow table schema."""
    col_defs = []
    for name, typ in zip(table.schema.names, table.schema.types):
        if typ not in _PA_TO_PHYSICAL:
            raise ValueError(f"unmapped Arrow type for column {name!r}: {typ}")
        column_type = _PA_TO_PHYSICAL[typ]
        col_defs.append(f'"{name}" {column_type}')
    if signal == "traces":
        existing = {n.lower() for n in table.schema.names}
        for ec in _TRACE_EXTRA_COLS:
            if ec not in existing:
                col_defs.append(f'"{ec}" VARCHAR')
    col_defs.append('"_date" DATE')
    return col_defs


def _ensure_iceberg_table(
    engine,
    signal: str,
    col_defs: list[str],
    partition_cols: list[str],
) -> None:
    """CREATE TABLE IF NOT EXISTS and evolve partition spec for traces."""
    create_ddl = (
        f"CREATE TABLE IF NOT EXISTS otel.signals.{signal} "
        f"({', '.join(col_defs)}) "
        f"WITH (partitioning = ARRAY[{', '.join(partition_cols)}], format = 'PARQUET')"
    )
    engine.execute_engine_sync(create_ddl)
    if signal == "traces":
        try:
            engine.execute_engine_sync(
                f"ALTER TABLE otel.signals.{signal} "
                f"SET PROPERTIES partitioning = ARRAY[{', '.join(partition_cols)}]"
            )
        except Exception as exc:
            logger.warning("compact_otel: could not evolve partition spec for %s: %s", signal, exc)


def _cast_table_to_physical_schema(
    signal: str,
    table: pa.Table,
    engine_cols: dict[str, str],
) -> pa.Table:
    """Cast a PyArrow table's columns to match the actual the engine column types."""
    cast_fields = []
    for field in table.schema:
        column_type = engine_cols.get(field.name.lower())
        if column_type is None:
            pa_type = field.type
        elif column_type not in _PHYSICAL_TO_PA:
            raise ValueError(f"unknown engine column type for {field.name!r}: {column_type}")
        else:
            pa_type = _PHYSICAL_TO_PA[column_type]
        cast_fields.append(pa.field(field.name, pa_type))
    return table.cast(pa.schema(cast_fields), safe=False)


def _build_insert_columns(
    _signal: str,
    table: pa.Table,
    engine_cols: dict[str, str],
    extract_trace_attrs: bool,
    extra_cols: list[str],
) -> tuple[list[str], list[str], list[str]]:
    """Return (parquet_cols, col_names, placeholders) for INSERT statement building."""

    def _cast_ph(col_lower: str) -> str:
        if col_lower not in engine_cols:
            raise ValueError(f"no engine cast type for column {col_lower!r}")
        t = _PHYSICAL_CAST.get(engine_cols[col_lower], "VARCHAR")
        return f"CAST(? AS {t})"

    parquet_cols = [n for n in table.schema.names if n.lower() in engine_cols]
    col_names = [f'"{n}"' for n in parquet_cols] + ['"_date"']
    placeholders = [_cast_ph(n.lower()) for n in parquet_cols] + ["CAST(? AS DATE)"]
    if extract_trace_attrs:
        extra_insert = [
            ec for ec in extra_cols if ec not in table.schema.names and ec.lower() in engine_cols
        ]
        col_names += [f'"{ec}"' for ec in extra_insert]
        placeholders += [_cast_ph(ec.lower()) for ec in extra_insert]
    return parquet_cols, col_names, placeholders


def _build_row(
    row: dict,  # type: ignore[type-arg]
    parquet_cols: list[str],
    date_val: str,
    extract_trace_attrs: bool,
    table_has_table_name: bool,
    extra_cols: list[str],
    engine_cols: dict[str, str],
) -> tuple:  # type: ignore[type-arg]
    """Convert a single row dict to a tuple for the engine INSERT."""
    vals = dict(row)
    if "span_attributes" in vals and isinstance(vals["span_attributes"], str):
        vals["span_attributes"] = vals["span_attributes"][:_SPAN_ATTRS_MAX]
    base = tuple(vals.get(n) for n in parquet_cols) + (date_val,)
    if not extract_trace_attrs or table_has_table_name:
        return base
    attrs: dict = {}  # type: ignore[type-arg]
    raw = row.get("span_attributes")
    if raw:
        attrs = json.loads(raw)
    return base + tuple(attrs.get(_ATTR_KEYS[ec]) for ec in extra_cols if ec.lower() in engine_cols)


def _resolve_batch_size(signal: str) -> int:
    """Determine INSERT batch size from app state, bounded to safe limits."""
    from provisa.api.app import state as _state

    batch = min(max(_state.otel_compact_batch_size, 1), 10)
    if signal == "traces":
        batch = min(batch, 5)
    return batch


def _execute_batch_inserts(
    engine,
    signal: str,
    rows: list[tuple],  # type: ignore[type-arg]
    col_names: list[str],
    placeholders: list[str],
    batch_size: int,
) -> None:
    """Execute multi-row batch INSERTs into Iceberg through the engine terminal."""
    row_ph = f"({', '.join(placeholders)})"
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        multi_sql = (
            f"INSERT INTO otel.signals.{signal} ({', '.join(col_names)}) VALUES "
            + ", ".join([row_ph] * len(batch))
        )
        flat = [v for row in batch for v in row]
        engine.execute_engine_sync(multi_sql, flat)


def _expire_iceberg_snapshots(engine, signal: str) -> None:
    """Expire old Iceberg snapshots to prevent metadata bloat."""
    try:
        engine.execute_engine_sync(
            f"ALTER TABLE otel.signals.{signal} EXECUTE expire_snapshots"
            f"(retention_threshold => '7d')"
        )
    except Exception:
        logger.warning("compact_otel: expire_snapshots for %s failed", signal, exc_info=True)


def _insert_otel_iceberg(engine, signal: str, table: pa.Table, dt: datetime) -> None:
    """Create Iceberg table from schema and INSERT the rows (runs in thread, sync engine)."""
    engine.execute_engine_sync("CREATE SCHEMA IF NOT EXISTS otel.signals")

    col_defs = _build_iceberg_col_defs(signal, table)
    partition_cols = ["'_date'", "'table_name'"] if signal == "traces" else ["'_date'"]
    _ensure_iceberg_table(engine, signal, col_defs, partition_cols)

    # Read back actual the engine column types and cast PyArrow table to match exactly.
    _cols = engine.execute_engine_sync(f"SHOW COLUMNS FROM otel.signals.{signal}")
    engine_cols = {row[0].lower(): row[1].lower() for row in _cols.rows}
    table = _cast_table_to_physical_schema(signal, table, engine_cols)

    date_val = dt.strftime("%Y-%m-%d")

    # For traces: extract provisa-specific span attributes into dedicated columns.
    extract_trace_attrs = signal == "traces" and "span_attributes" in table.schema.names
    extra_cols: list[str] = _TRACE_EXTRA_COLS if extract_trace_attrs else []

    parquet_cols, col_names, placeholders = _build_insert_columns(
        signal, table, engine_cols, extract_trace_attrs, extra_cols
    )

    table_has_table_name = "table_name" in table.schema.names
    rows = [
        _build_row(
            r,
            parquet_cols,
            date_val,
            extract_trace_attrs,
            table_has_table_name,
            extra_cols,
            engine_cols,
        )
        for r in table.to_pylist()
    ]
    if not rows:
        return

    batch_size = _resolve_batch_size(signal)
    _execute_batch_inserts(engine, signal, rows, col_names, placeholders, batch_size)
    _expire_iceberg_snapshots(engine, signal)


async def watch_engine() -> None:
    """Engine-terminal liveness watchdog (scheduled job). Delegates to the bound engine's
    watchdog through the seam — the engine restarts its container and replaces a dead connection;
    native engines have no external process to watch (no-op)."""
    from provisa.api.app import state

    await state.federation_engine.watchdog()


def build_scheduler(
    triggers: list[ScheduledTrigger],
) -> AsyncIOScheduler | None:  # REQ-216, REQ-177
    """Build an APScheduler instance from config triggers.

    Returns None if no enabled triggers exist.
    """
    enabled = [t for t in triggers if t.enabled]
    if not enabled:
        return None

    scheduler = AsyncIOScheduler()

    for trigger in enabled:
        # Mutual exclusivity: exactly one action type per trigger (REQ-1003).
        _set = [n for n in ("url", "function", "sql") if getattr(trigger, n)]
        if len(_set) > 1:
            raise ValueError(
                f"Trigger {trigger.id}: url/function/sql are mutually exclusive, got {_set}"
            )
        cron = CronTrigger.from_crontab(trigger.cron)
        if trigger.url:
            scheduler.add_job(
                _execute_webhook,
                trigger=cron,
                args=[trigger.url, trigger.id, trigger.args or None],
                id=trigger.id,
                name=f"trigger:{trigger.id}",
                replace_existing=True,
            )
            logger.info("Scheduled trigger %s: %s -> %s", trigger.id, trigger.cron, trigger.url)
        elif trigger.sql:  # REQ-1003, REQ-1004
            scheduler.add_job(
                _execute_sql,
                trigger=cron,
                args=[trigger.sql, trigger.id],
                id=trigger.id,
                name=f"trigger:{trigger.id}",
                replace_existing=True,
            )
            logger.info("Scheduled trigger %s: %s -> SQL", trigger.id, trigger.cron)
        elif trigger.function:
            logger.warning(
                "Trigger %s: internal function %s not yet supported",
                trigger.id,
                trigger.function,
            )

    return scheduler
