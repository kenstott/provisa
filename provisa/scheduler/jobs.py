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
from typing import TYPE_CHECKING

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

if TYPE_CHECKING:
    from provisa.core.models import ScheduledTrigger

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


def _parse_compiled_sql(raw: str) -> list[str]:
    """Return SQL statements from a compiled_sql value (plain string or JSON array)."""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [s for s in parsed if isinstance(s, str)]
    except (json.JSONDecodeError, TypeError):
        pass
    return [raw]


async def run_scheduled_query(
    stable_id: str,
    output_type: str,
    output_format: str | None,
    destination: str | None,
) -> None:
    """Execute an approved persisted query and dispatch output on schedule.

    output_type: 'redirect' (upload to S3), 'webhook' (POST results), 'kafka' (publish to topic)
    output_format: parquet | csv | json | ndjson | arrow (for redirect only)
    destination: S3 key prefix, webhook URL, or Kafka topic name
    """
    from provisa.api.app import state
    from provisa.registry.store import get_by_stable_id

    logger.info("Scheduled query %s: starting (output_type=%s)", stable_id, output_type)

    async with state.pg_pool.acquire() as conn:
        row = await get_by_stable_id(conn, stable_id)
    if not row:
        logger.error("Scheduled query %s: not found in registry", stable_id)
        return

    sql_list = _parse_compiled_sql(row["compiled_sql"])
    role_id = row.get("developer_id") or "admin"

    from provisa.transpiler.router import Route, decide_route
    from provisa.transpiler.transpile import transpile, transpile_to_trino
    from provisa.executor.direct import execute_direct
    from provisa.executor.trino import execute_trino

    routing_hint = row.get("routing_hint")
    decision = decide_route(
        sources=set(),
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        steward_hint=routing_hint,
    )

    results_list = []
    for sql_item in sql_list:
        try:
            if decision.route == Route.TRINO:
                results_list.append(await execute_trino(transpile_to_trino(sql_item), []))
            else:
                dialect = decision.dialect or "postgres"
                results_list.append(await execute_direct(
                    state.source_pools,
                    decision.source_id or next(iter(state.source_pools), "pg"),
                    transpile(sql_item, dialect),
                    [],
                ))
        except Exception:
            logger.exception("Scheduled query %s: execution failed", stable_id)
            return

    if not results_list:
        return

    rows_as_dicts = [
        dict(zip(r.column_names, row))
        for r in results_list
        for row in r.rows
    ]

    if output_type == "redirect":
        from provisa.executor.redirect import RedirectConfig, upload_result
        from provisa.compiler.sql_gen import ColumnRef
        cfg = RedirectConfig.from_env()
        fmt = output_format or cfg.default_format
        for i, r in enumerate(results_list):
            suffix = f"_{i}" if len(results_list) > 1 else ""
            columns = [ColumnRef(field_name=c, column=c) for c in r.column_names]
            key = f"{destination or stable_id}/{stable_id}{suffix}.{fmt}"
            try:
                url, _ = await upload_result(r, columns, fmt, key, cfg)
                logger.info("Scheduled query %s: uploaded to %s", stable_id, url)
            except Exception:
                logger.exception("Scheduled query %s: redirect upload failed", stable_id)

    elif output_type == "webhook":
        if not destination:
            logger.error("Scheduled query %s: webhook output_type but no destination URL", stable_id)
            return
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    destination,
                    json={"stable_id": stable_id, "rows": rows_as_dicts},
                )
                resp.raise_for_status()
            logger.info("Scheduled query %s: webhook %s returned %s", stable_id, destination, resp.status_code)
        except Exception:
            logger.exception("Scheduled query %s: webhook delivery failed", stable_id)

    elif output_type == "kafka":
        topic = destination or row.get("sink_topic")
        if not topic:
            logger.error("Scheduled query %s: kafka output_type but no topic", stable_id)
            return
        try:
            from provisa.kafka.producer import publish_rows
            await publish_rows(topic, rows_as_dicts, key_column=row.get("sink_key_column"))
            logger.info("Scheduled query %s: published %d rows to %s", stable_id, len(rows_as_dicts), topic)
        except Exception:
            logger.exception("Scheduled query %s: kafka publish failed", stable_id)

    else:
        logger.warning("Scheduled query %s: unknown output_type %r", stable_id, output_type)


async def compact_otel_signals() -> None:
    """Compact today's OTEL Parquet from MinIO into Iceberg via Trino.

    Runs every minute. Deletes existing rows for today's partition before
    reinserting so re-runs are idempotent. Set OTEL_COMPACT_DATE (YYYY-MM-DD)
    to backfill a specific date.
    """
    import asyncio
    import os
    from datetime import datetime

    from provisa.api.app import state

    override = os.environ.get("OTEL_COMPACT_DATE")
    if override:
        target = datetime.strptime(override, "%Y-%m-%d")
    else:
        target = datetime.utcnow()

    s3_endpoint = os.environ.get("PROVISA_OTEL_S3_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("PROVISA_OTEL_S3_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("PROVISA_OTEL_S3_SECRET_KEY", "minioadmin")
    otel_bucket = os.environ.get("PROVISA_OTEL_BUCKET", "provisa-otel")
    file_chunk = getattr(state, "otel_compact_file_chunk", 50)
    trino_conn = state.trino_conn

    for signal in ("logs", "metrics", "traces"):
        await asyncio.to_thread(
            _compact_signal, signal, target, s3_endpoint, access_key, secret_key,
            otel_bucket, file_chunk, trino_conn,
        )


def _compact_signal(
    signal: str,
    target: object,
    s3_endpoint: str,
    access_key: str,
    secret_key: str,
    otel_bucket: str,
    file_chunk: int,
    trino_conn: object,
) -> None:
    """Compact one OTel signal type. Runs entirely in a thread — no event loop blocking."""
    import io
    from datetime import datetime

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

    prefix = f"{signal}/"
    try:
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=otel_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet") and date_glob in obj["Key"]:
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
            logger.exception("compact_otel: failed reading %s parquet files (chunk %d)", signal, chunk_start)
            return

        try:
            _insert_otel_iceberg(trino_conn, signal, combined, target, delete_first=(chunk_start == 0))
            total_rows += len(combined)
            del_resp = s3.delete_objects(
                Bucket=otel_bucket,
                Delete={"Objects": [{"Key": k} for k in chunk_keys]},
            )
            for err in del_resp.get("Errors", []):
                logger.warning(
                    "compact_otel: s3 delete failed key=%s code=%s msg=%s",
                    err.get("Key"), err.get("Code"), err.get("Message"),
                )
        except Exception:
            logger.exception("compact_otel: failed inserting %s into Iceberg (chunk %d)", signal, chunk_start)
            return

    logger.info("compact_otel: inserted %d %s rows for %s", total_rows, signal, date_glob)


def _insert_otel_iceberg(conn: object, signal: str, table: object, dt: object, *, delete_first: bool = True) -> None:
    """Create Iceberg table from schema and INSERT the rows (runs in thread)."""
    import pyarrow as pa

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

    cursor = conn.cursor()  # type: ignore[union-attr]
    cursor.execute("CREATE SCHEMA IF NOT EXISTS otel.signals")

    col_defs = []
    for name, typ in zip(table.schema.names, table.schema.types):
        trino_type = _PA_TO_TRINO.get(typ, "VARCHAR")
        col_defs.append(f'"{name}" {trino_type}')
    if signal == "traces":
        existing = {n.lower() for n in table.schema.names}
        for ec in ["table_name", "domain_id", "role_id", "query_text"]:
            if ec not in existing:
                col_defs.append(f'"{ec}" VARCHAR')
    col_defs.append('"_date" DATE')

    partition_cols = ["'_date'", "'table_name'"] if signal == "traces" else ["'_date'"]
    create_ddl = (
        f"CREATE TABLE IF NOT EXISTS otel.signals.{signal} "
        f"({', '.join(col_defs)}) "
        f"WITH (partitioning = ARRAY[{', '.join(partition_cols)}], format = 'PARQUET')"
    )
    cursor.execute(create_ddl)
    if signal == "traces":
        try:
            cursor.execute(
                f"ALTER TABLE otel.signals.{signal} "
                f"SET PROPERTIES partitioning = ARRAY[{', '.join(partition_cols)}]"
            )
        except Exception as exc:
            logger.warning("compact_otel: could not evolve partition spec for %s: %s", signal, exc)

    # Read back actual Trino column types and cast PyArrow table to match exactly.
    # This guarantees to_pylist() returns Python types that the trino client maps correctly.
    cursor.execute(f"SHOW COLUMNS FROM otel.signals.{signal}")
    trino_cols = {row[0].lower(): row[1].lower() for row in cursor.fetchall()}
    cast_fields = []
    for field in table.schema:
        trino_type = trino_cols.get(field.name.lower())
        pa_type = _TRINO_TO_PA.get(trino_type, pa.string()) if trino_type else field.type  # type: ignore[arg-type]
        cast_fields.append(pa.field(field.name, pa_type))
    try:
        table = table.cast(pa.schema(cast_fields), safe=False)
    except Exception:
        logger.warning("compact_otel: could not cast %s table to Trino schema, proceeding as-is", signal)

    date_val = dt.strftime("%Y-%m-%d")

    if delete_first:
        try:
            cursor.execute(
                f"DELETE FROM otel.signals.{signal} WHERE _date = DATE '{date_val}'"
            )
        except Exception:
            pass

    # For traces: extract provisa-specific span attributes into dedicated columns.
    # otlp2parquet stores attributes as JSON in span_attributes (not "attributes").
    extract_trace_attrs = signal == "traces" and "span_attributes" in table.schema.names
    extra_cols: list[str] = []
    if extract_trace_attrs:
        extra_cols = ["table_name", "domain_id", "role_id", "query_text"]

    _TRINO_CAST = {
        "bigint": "BIGINT", "integer": "INTEGER", "double": "DOUBLE", "real": "REAL",
        "boolean": "BOOLEAN", "timestamp(6)": "TIMESTAMP(6)", "timestamp(3)": "TIMESTAMP(3)",
        "date": "DATE", "varchar": "VARCHAR",
    }

    def _cast_ph(col_lower: str) -> str:
        t = _TRINO_CAST.get(trino_cols.get(col_lower, "varchar"), "VARCHAR")
        return f"CAST(? AS {t})"

    parquet_cols = [n for n in table.schema.names if n.lower() in trino_cols]
    col_names = [f'"{n}"' for n in parquet_cols] + ['"_date"']
    placeholders = [_cast_ph(n.lower()) for n in parquet_cols] + ["CAST(? AS DATE)"]
    if extract_trace_attrs:
        extra_insert = [ec for ec in extra_cols if ec not in table.schema.names and ec.lower() in trino_cols]
        col_names += [f'"{ec}"' for ec in extra_insert]
        placeholders += [_cast_ph(ec.lower()) for ec in extra_insert]
    insert_sql = f"INSERT INTO otel.signals.{signal} ({', '.join(col_names)}) VALUES ({', '.join(placeholders)})"

    def _row(row: dict) -> tuple:  # type: ignore[type-arg]
        base = tuple(row.get(n) for n in parquet_cols) + (date_val,)
        if not extract_trace_attrs or "table_name" in table.schema.names:
            return base
        attrs: dict = {}
        raw = row.get("span_attributes")
        if raw:
            try:
                attrs = json.loads(raw)
            except Exception:
                pass
        _attr_keys = {
            "table_name": "provisa.table",
            "domain_id": "provisa.domain",
            "role_id": "provisa.role",
            "query_text": "provisa.query_text",
        }
        return base + tuple(attrs.get(_attr_keys[ec]) for ec in extra_cols if ec.lower() in trino_cols)

    rows = [_row(r) for r in table.to_pylist()]
    if not rows:
        return

    # Batch inserts: build multi-row VALUES per execute call (avoids per-row round trips).
    try:
        from provisa.api.app import state as _state
        _BATCH = max(_state.otel_compact_batch_size, 100)
    except Exception:
        _BATCH = 100
    row_ph = f"({', '.join(placeholders)})"
    for i in range(0, len(rows), _BATCH):
        batch = rows[i : i + _BATCH]
        multi_sql = (
            f"INSERT INTO otel.signals.{signal} ({', '.join(col_names)}) VALUES "
            + ", ".join([row_ph] * len(batch))
        )
        flat = [v for row in batch for v in row]
        cursor.execute(multi_sql, flat)

    # Expire all but the current snapshot to prevent metadata bloat → Trino OOM.
    # retention_threshold must be a TIMESTAMP; CURRENT_TIMESTAMP expires everything
    # older than "now", leaving only the snapshot just written.
    try:
        cursor.execute(
            f"ALTER TABLE otel.signals.{signal} EXECUTE expire_snapshots"
            f"(retention_threshold => CURRENT_TIMESTAMP)"
        )
    except Exception:
        logger.warning("compact_otel: expire_snapshots for %s failed", signal, exc_info=True)


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
            "docker", "start", "provisa-trino-1",
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


def _trino_ping(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[union-attr]
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
                trigger.id, trigger.function,
            )

    return scheduler
