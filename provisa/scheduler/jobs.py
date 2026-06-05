# Copyright (c) 2026 Kenneth Stott
# Canary: 2dadb1c4-7dac-45cd-bece-d8d8955e690d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Scheduled trigger execution via APScheduler (REQ-216).

Registers cron-based jobs from config. Supports webhook URLs and
internal function names. Reuses existing async patterns.
"""

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
    import trino.dbapi

    from provisa.core.models import ScheduledTrigger

    _TrinoCursor = trino.dbapi.Cursor

logger = logging.getLogger(__name__)


async def _execute_webhook(url: str, trigger_id: str) -> None:
    """Fire a webhook for a scheduled trigger."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json={"trigger_id": trigger_id})
            resp.raise_for_status()
            logger.info("Trigger %s: webhook %s returned %s", trigger_id, url, resp.status_code)
    except Exception:
        logger.exception("Trigger %s: webhook %s failed", trigger_id, url)


async def compact_otel_signals() -> None:
    """Compact today's OTEL Parquet from MinIO into Iceberg via Trino.

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

    s3_endpoint = os.environ.get("PROVISA_OTEL_S3_ENDPOINT") or getattr(
        state, "otel_s3_endpoint", "http://minio:9000"
    )
    logger.warning(
        "compact_otel: starting — s3=%s trino_conn=%s", s3_endpoint, state.trino_conn is not None
    )
    access_key = os.environ.get("PROVISA_OTEL_S3_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("PROVISA_OTEL_S3_SECRET_KEY", "minioadmin")
    otel_bucket = os.environ.get("PROVISA_OTEL_BUCKET", "provisa-otel")
    file_chunk = getattr(state, "otel_compact_file_chunk", 50)
    trino_conn = state.trino_conn

    for signal in ("logs", "metrics", "traces"):
        await asyncio.to_thread(
            _compact_signal,
            signal,
            target,
            s3_endpoint,
            access_key,
            secret_key,
            otel_bucket,
            file_chunk,
            trino_conn,
        )


def _compact_signal(
    signal: str,
    target: datetime,
    s3_endpoint: str,
    access_key: str,
    secret_key: str,
    otel_bucket: str,
    file_chunk: int,
    trino_conn: trino.dbapi.Connection | None,
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
    try:
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=otel_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])
    except Exception:
        logger.warning("compact_otel: cannot list s3://%s/%s", otel_bucket, prefix)
        return

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
            _insert_otel_iceberg(trino_conn, signal, combined, target)
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


_PA_TO_TRINO: dict[object, str] = {
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

_TRINO_TO_PA: dict[str, object] = {
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

_TRINO_CAST: dict[str, str] = {
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
        trino_type = _PA_TO_TRINO.get(typ, "VARCHAR")
        col_defs.append(f'"{name}" {trino_type}')
    if signal == "traces":
        existing = {n.lower() for n in table.schema.names}
        for ec in _TRACE_EXTRA_COLS:
            if ec not in existing:
                col_defs.append(f'"{ec}" VARCHAR')
    col_defs.append('"_date" DATE')
    return col_defs


def _ensure_iceberg_table(
    cursor: trino.dbapi.Cursor,
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
    cursor.execute(create_ddl)  # type: ignore[union-attr]
    if signal == "traces":
        try:
            cursor.execute(  # type: ignore[union-attr]
                f"ALTER TABLE otel.signals.{signal} "
                f"SET PROPERTIES partitioning = ARRAY[{', '.join(partition_cols)}]"
            )
        except Exception as exc:
            logger.warning("compact_otel: could not evolve partition spec for %s: %s", signal, exc)


def _cast_table_to_trino_schema(
    signal: str,
    table: pa.Table,
    trino_cols: dict[str, str],
) -> pa.Table:
    """Cast a PyArrow table's columns to match the actual Trino column types."""
    cast_fields = []
    for field in table.schema:
        trino_type = trino_cols.get(field.name.lower())
        pa_type = _TRINO_TO_PA.get(trino_type, pa.string()) if trino_type else field.type  # type: ignore[arg-type]
        cast_fields.append(pa.field(field.name, pa_type))
    try:
        return table.cast(pa.schema(cast_fields), safe=False)
    except Exception:
        logger.warning(
            "compact_otel: could not cast %s table to Trino schema, proceeding as-is", signal
        )
        return table


def _build_insert_columns(
    signal: str,
    table: pa.Table,
    trino_cols: dict[str, str],
    extract_trace_attrs: bool,
    extra_cols: list[str],
) -> tuple[list[str], list[str], list[str]]:
    """Return (parquet_cols, col_names, placeholders) for INSERT statement building."""

    def _cast_ph(col_lower: str) -> str:
        t = _TRINO_CAST.get(trino_cols.get(col_lower, "varchar"), "VARCHAR")
        return f"CAST(? AS {t})"

    parquet_cols = [n for n in table.schema.names if n.lower() in trino_cols]
    col_names = [f'"{n}"' for n in parquet_cols] + ['"_date"']
    placeholders = [_cast_ph(n.lower()) for n in parquet_cols] + ["CAST(? AS DATE)"]
    if extract_trace_attrs:
        extra_insert = [
            ec for ec in extra_cols if ec not in table.schema.names and ec.lower() in trino_cols
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
    trino_cols: dict[str, str],
) -> tuple:  # type: ignore[type-arg]
    """Convert a single row dict to a tuple for Trino INSERT."""
    vals = dict(row)
    if "span_attributes" in vals and isinstance(vals["span_attributes"], str):
        vals["span_attributes"] = vals["span_attributes"][:_SPAN_ATTRS_MAX]
    base = tuple(vals.get(n) for n in parquet_cols) + (date_val,)
    if not extract_trace_attrs or table_has_table_name:
        return base
    attrs: dict = {}  # type: ignore[type-arg]
    raw = row.get("span_attributes")
    if raw:
        try:
            attrs = json.loads(raw)
        except Exception:
            pass
    return base + tuple(
        attrs.get(_ATTR_KEYS[ec]) for ec in extra_cols if ec.lower() in trino_cols
    )


def _resolve_batch_size(signal: str) -> int:
    """Determine INSERT batch size from app state, bounded to safe limits."""
    try:
        from provisa.api.app import state as _state

        batch = min(max(_state.otel_compact_batch_size, 1), 10)
    except Exception:
        batch = 10
    if signal == "traces":
        batch = min(batch, 5)
    return batch


def _execute_batch_inserts(
    cursor: trino.dbapi.Cursor,
    signal: str,
    rows: list[tuple],  # type: ignore[type-arg]
    col_names: list[str],
    placeholders: list[str],
    batch_size: int,
) -> None:
    """Execute multi-row batch INSERTs into Iceberg."""
    row_ph = f"({', '.join(placeholders)})"
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        multi_sql = (
            f"INSERT INTO otel.signals.{signal} ({', '.join(col_names)}) VALUES "
            + ", ".join([row_ph] * len(batch))
        )
        flat = [v for row in batch for v in row]
        cursor.execute(multi_sql, flat)  # type: ignore[union-attr]


def _expire_iceberg_snapshots(cursor: trino.dbapi.Cursor, signal: str) -> None:
    """Expire old Iceberg snapshots to prevent metadata bloat."""
    try:
        cursor.execute(  # type: ignore[union-attr]
            f"ALTER TABLE otel.signals.{signal} EXECUTE expire_snapshots"
            f"(retention_threshold => '7d')"
        )
    except Exception:
        logger.warning("compact_otel: expire_snapshots for %s failed", signal, exc_info=True)


def _insert_otel_iceberg(
    conn: trino.dbapi.Connection | None, signal: str, table: pa.Table, dt: datetime
) -> None:
    """Create Iceberg table from schema and INSERT the rows (runs in thread)."""
    cursor = conn.cursor()  # type: ignore[union-attr]
    cursor.execute("CREATE SCHEMA IF NOT EXISTS otel.signals")

    col_defs = _build_iceberg_col_defs(signal, table)
    partition_cols = ["'_date'", "'table_name'"] if signal == "traces" else ["'_date'"]
    _ensure_iceberg_table(cursor, signal, col_defs, partition_cols)

    # Read back actual Trino column types and cast PyArrow table to match exactly.
    cursor.execute(f"SHOW COLUMNS FROM otel.signals.{signal}")
    trino_cols = {row[0].lower(): row[1].lower() for row in cursor.fetchall()}
    table = _cast_table_to_trino_schema(signal, table, trino_cols)

    date_val = dt.strftime("%Y-%m-%d")

    # For traces: extract provisa-specific span attributes into dedicated columns.
    extract_trace_attrs = signal == "traces" and "span_attributes" in table.schema.names
    extra_cols: list[str] = _TRACE_EXTRA_COLS if extract_trace_attrs else []

    parquet_cols, col_names, placeholders = _build_insert_columns(
        signal, table, trino_cols, extract_trace_attrs, extra_cols
    )

    table_has_table_name = "table_name" in table.schema.names
    rows = [
        _build_row(r, parquet_cols, date_val, extract_trace_attrs, table_has_table_name, extra_cols, trino_cols)
        for r in table.to_pylist()
    ]
    if not rows:
        return

    batch_size = _resolve_batch_size(signal)
    _execute_batch_inserts(cursor, signal, rows, col_names, placeholders, batch_size)
    _expire_iceberg_snapshots(cursor, signal)


async def watch_trino() -> None:
    """Restart the Trino Docker container if it is not responding."""
    import asyncio
    import trino
    from provisa.api.app import state

    if state.trino_conn is None:
        return

    try:
        await asyncio.to_thread(_trino_ping, state.trino_conn)
        return
    except Exception:
        pass

    logger.warning("watch_trino: Trino unresponsive — attempting restart")
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "start",
            "provisa-trino-1",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error("watch_trino: docker start failed: %s", stderr.decode().strip())
            return
        logger.info("watch_trino: provisa-trino-1 started, waiting for healthy state")
    except Exception:
        logger.exception("watch_trino: docker start provisa-trino-1 failed")
        return

    # Wait up to 120 s for Trino to accept connections, then replace the dead conn.
    deadline = asyncio.get_event_loop().time() + 120
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(5)
        try:
            new_conn = await asyncio.to_thread(
                lambda: trino.dbapi.connect(**state.trino_conn_kwargs)
            )
            await asyncio.to_thread(_trino_ping, new_conn)
            old_conn = state.trino_conn
            state.trino_conn = new_conn
            try:
                old_conn.close()
            except Exception:
                pass
            logger.info("watch_trino: Trino reconnected successfully")
            return
        except Exception:
            pass

    logger.error("watch_trino: Trino did not become healthy within 120 s")


def _trino_ping(conn: trino.dbapi.Connection) -> None:
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()


def build_scheduler(triggers: list[ScheduledTrigger]) -> AsyncIOScheduler | None:
    """Build an APScheduler instance from config triggers.

    Returns None if no enabled triggers exist.
    """
    enabled = [t for t in triggers if t.enabled]
    if not enabled:
        return None

    scheduler = AsyncIOScheduler()

    for trigger in enabled:
        cron = CronTrigger.from_crontab(trigger.cron)
        if trigger.url:
            scheduler.add_job(
                _execute_webhook,
                trigger=cron,
                args=[trigger.url, trigger.id],
                id=trigger.id,
                name=f"trigger:{trigger.id}",
                replace_existing=True,
            )
            logger.info("Scheduled trigger %s: %s -> %s", trigger.id, trigger.cron, trigger.url)
        elif trigger.function:
            logger.warning(
                "Trigger %s: internal function %s not yet supported",
                trigger.id,
                trigger.function,
            )

    return scheduler
