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

import asyncio
import json
import logging
import math
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import httpx
import pyarrow as pa
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from provisa.core.trino_system_catalogs import OTEL_CATALOG

if TYPE_CHECKING:
    from provisa.core.models import ScheduledTrigger

logger = logging.getLogger(__name__)

# REQ-1428: concurrent object-store fetches per compaction chunk. Sized to the object store's
# round-trip latency rather than to CPU — the work is entirely network wait.
_OTEL_FETCH_WORKERS = 16


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
    import asyncio
    import os

    from provisa.api.app import state

    # None = compact every date present, oldest first. A backfill pins one date.
    override = os.environ.get("OTEL_COMPACT_DATE")
    target = datetime.strptime(override, "%Y-%m-%d") if override else None

    # OTEL→Iceberg compaction targets an S3/MinIO object store. The native (no-Docker) tier has no
    # S3, so its credentials are unset — there is nothing to compact. Skip cleanly instead of
    # crashing the scheduled job every minute with a KeyError (capability gate, not a masked error).
    access_key = os.environ.get("PROVISA_OTEL_S3_ACCESS_KEY")
    secret_key = os.environ.get("PROVISA_OTEL_S3_SECRET_KEY")
    if not access_key or not secret_key:
        logger.debug(
            "compact_otel: no S3 credentials configured — skipping (native tier has no object store)."
        )
        return

    s3_endpoint = os.environ.get("PROVISA_OTEL_S3_ENDPOINT") or state.otel_s3_endpoint
    otel_bucket = os.environ.get("PROVISA_OTEL_BUCKET", "provisa-otel")
    file_chunk = state.otel_compact_file_chunk
    max_files = state.otel_compact_max_files_per_run
    engine = state.federation_engine

    loop = asyncio.get_event_loop()
    if loop.is_closed() or not loop.is_running():
        logger.warning("compact_otel: event loop shutting down, skipping this run")
        return

    async def _one(signal: str) -> None:
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
                max_files,
                engine,
            )
        except asyncio.CancelledError:
            logger.warning("compact_otel: cancelled during shutdown, skipping %s", signal)
        except RuntimeError as e:
            if "shutdown" in str(e).lower() or "closed" in str(e).lower():
                logger.warning("compact_otel: executor shutting down, skipping %s", signal)
                return
            raise

    # Concurrent, not a fixed sequence (REQ-1428): each signal owns its own file budget, but a
    # sequential run spent minutes on `logs` and `metrics` and was killed — by a restart or the
    # node's idle-stop — before `traces` ever got a turn. Traces stopped landing in Iceberg
    # entirely, so the queries report read an empty table while 15k trace files sat in the object
    # store. Running the three together means no signal's backlog can starve another's.
    await asyncio.gather(*(_one(s) for s in ("logs", "metrics", "traces")))


_DATE_SEG_RE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/")


def _key_date(key: str) -> datetime | None:
    """The partition date encoded in an otlp2parquet key, or None if the layout doesn't carry one."""
    m = _DATE_SEG_RE.search(key)
    if not m:
        return None
    return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _compact_signal(
    signal: str,
    target: datetime | None,
    s3_endpoint: str,
    access_key: str,
    secret_key: str,
    otel_bucket: str,
    file_chunk: int,
    max_files: int,
    engine,
) -> None:
    """Compact one OTel signal type. Runs entirely in a thread — no event loop blocking.

    ``target`` pins compaction to a single date (the OTEL_COMPACT_DATE backfill); None — the
    scheduled case — compacts EVERY date present, oldest first. Restricting the listing to today
    was an unbounded leak: a file whose date segment had rolled over was never listed again, so it
    was never inserted and never deleted. 70586 such files (15 GiB under `traces/trino/`) had
    accumulated on the SaaS node and filled the coordinator's disk. Rows carry their own key's
    date into the `_date` partition, so draining a backlog cannot misfile yesterday's spans
    under today.
    """
    import io

    import boto3
    from botocore.config import Config as BotoConfig

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        # The object store's own region — R2 requires "auto", MinIO ignores it. Hardcoding
        # us-east-1 here contradicted otel_object_store(), which reads PROVISA_OTEL_S3_REGION.
        region_name=os.environ.get("PROVISA_OTEL_S3_REGION", "us-east-1"),
        # REQ-1428: the pool has to hold every concurrent fetch. botocore defaults to 10, below the
        # worker count, so the surplus workers' connections were built and discarded on each chunk
        # ("Connection pool is full, discarding connection") — paying a TLS handshake per file for
        # the round trips the concurrency exists to amortise.
        config=BotoConfig(signature_version="s3v4", max_pool_connections=_OTEL_FETCH_WORKERS),
    )

    # The service segment is not always the first one under the signal. otlp2parquet writes traces
    # and logs as {signal}/{service}/{date}, but metrics as {signal}/{instrument}/{service}/{date}
    # — one level deeper, because a metric's parquet schema depends on its instrument type
    # (histogram, sum, ...). Listing the signal root and matching the date segment anywhere in the
    # key covers both layouts without hard-coding either one's depth.
    #
    # Every service that reports to the collector is compacted, not just this process. The federated
    # engine emits its own spans under `traces/trino/`, and a filter on OTEL_SERVICE_NAME left those
    # files unread and undeleted forever — 26521 of them on the SaaS node — while the traces report
    # showed nothing of what the engine actually did.
    paginator = s3.get_paginator("list_objects_v2")
    by_date: dict[datetime, list[str]] = {}
    for page in paginator.paginate(Bucket=otel_bucket, Prefix=f"{signal}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            key_date = _key_date(key)
            if key_date is None:
                continue
            if target is not None and key_date != target.replace(
                hour=0, minute=0, second=0, microsecond=0
            ):
                continue
            by_date.setdefault(key_date, []).append(key)

    # Each signal gets a bounded share of one run. The signals are compacted in a fixed order, so an
    # unbounded backlog in an earlier one starves every later one: metrics held the whole run and
    # traces — last in the list — never got a turn, which is why the traces report stayed empty
    # while metrics filled. Remaining files are picked up by the next tick. Oldest date first, so a
    # backlog drains from the tail instead of being permanently outrun by the current partition.
    total_available = sum(len(v) for v in by_date.values())
    budget = max_files
    batches: list[tuple[datetime, list[str]]] = []
    for key_date in sorted(by_date):
        if budget <= 0:
            break
        take = by_date[key_date][:budget]
        budget -= len(take)
        batches.append((key_date, take))

    if total_available > max_files:
        logger.info(
            "compact_otel: %s backlog %d files across %d dates, taking %d this run",
            signal,
            total_available,
            len(by_date),
            max_files,
        )

    if not batches:
        logger.debug("compact_otel: no %s files to compact", signal)
        return

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.exception("compact_otel: pyarrow not available")
        return

    total_rows = 0
    for key_date, keys in batches:
        for chunk_start in range(0, len(keys), file_chunk):
            chunk_keys = keys[chunk_start : chunk_start + file_chunk]
            try:
                # REQ-1428: the chunk's objects are fetched concurrently. Read one at a time, a
                # chunk cost one object-store round trip per file — roughly 50 files a minute
                # against R2 — so the per-minute run never came close to its 500-file budget and
                # compaction was slower than ingest. The backlog therefore grew regardless of how
                # the signals were scheduled, and the traces the queries report reads never
                # arrived. Order is preserved so a chunk's rows keep their key order.
                def _read(key: str):
                    obj = s3.get_object(Bucket=otel_bucket, Key=key)
                    return pq.read_table(io.BytesIO(obj["Body"].read()))

                with ThreadPoolExecutor(max_workers=_OTEL_FETCH_WORKERS) as pool:
                    parts = list(pool.map(_read, chunk_keys))
                combined = pa.concat_tables(parts, promote_options="default")
            except Exception:
                logger.exception(
                    "compact_otel: failed reading %s parquet files (chunk %d)", signal, chunk_start
                )
                return

            try:
                _insert_otel_iceberg(engine, signal, combined, key_date)
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

    logger.info(
        "compact_otel: inserted %d %s rows across %d date(s)", total_rows, signal, len(batches)
    )


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

# Trino's query.max-length defaults to 1,000,000 characters. Chunking the generated INSERT well
# under that keeps one statement — and therefore one Iceberg commit — per chunk.
_MAX_INSERT_SQL_CHARS: int = 400_000


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


def _resolve_batch_size(_signal: str) -> int:
    """Rows per INSERT statement, from `observability.compact_batch_size`.

    The clamp that used to hold this at ten rows — five for traces — was a workaround for the Trino
    client's parameterised-statement header limit. _execute_batch_inserts inlines its literals now,
    so the configured size is what runs, and each statement is one Iceberg commit instead of one
    commit per five spans.
    """
    from provisa.api.app import state as _state

    return max(_state.otel_compact_batch_size, 1)


def _sql_literal(value) -> str:
    """Render one INSERT value as a Trino SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "nan()"
        if math.isinf(value):
            return "infinity()" if value > 0 else "-infinity()"
        return repr(value)
    if isinstance(value, (bytes, bytearray)):
        return f"X'{value.hex()}'"
    # Trino string literals have no escape sequences — a doubled quote is the entire rule.
    return "'" + str(value).replace("'", "''") + "'"


def _execute_batch_inserts(
    engine,
    signal: str,
    rows: list[tuple],  # type: ignore[type-arg]
    col_names: list[str],
    placeholders: list[str],
    batch_size: int,
) -> None:
    """Execute multi-row INSERTs into Iceberg through the engine terminal.

    Values are inlined rather than parameterised. The Trino client carries a parameterised
    statement in an HTTP header, and that header cap is what held the batch at five rows — one
    Iceberg commit per five spans, which is how the node accumulated 67 GiB of metadata for 30 MiB
    of trace data. Inlining takes the header out of the path, so each chunk commits exactly once.
    """
    prefix = f"INSERT INTO otel.signals.{signal} ({', '.join(col_names)}) VALUES "
    values: list[str] = []
    pending = len(prefix)
    for row in rows:
        rendered = (
            "("
            + ", ".join(ph.replace("?", _sql_literal(v)) for ph, v in zip(placeholders, row))
            + ")"
        )
        over_chars = pending + len(rendered) > _MAX_INSERT_SQL_CHARS
        if values and (over_chars or len(values) >= batch_size):
            engine.execute_engine_sync(prefix + ", ".join(values))
            values = []
            pending = len(prefix)
        values.append(rendered)
        pending += len(rendered) + 2
    if values:
        engine.execute_engine_sync(prefix + ", ".join(values))


async def reclaim_otel_storage() -> None:  # REQ-302, REQ-303
    """Reclaim the OTel Iceberg tables' object-store footprint (scheduled).

    Expiring snapshots alone does NOT free space — it only unlinks them. The files an expired
    snapshot referenced become unreferenced, and ONLY ``remove_orphan_files`` deletes those.
    Nothing ran it, so the SaaS node reached 57 GiB of metadata behind 93 MiB of data and filled
    the coordinator's disk; a manual pass returned 59 GiB. Both procedures run here, at the
    configured ``ops_snapshot_retention_hours``.

    Trino floors both retentions at 7 days (``iceberg.expire-snapshots.min-retention``,
    ``iceberg.remove-orphan-files.min-retention``) and REJECTS a shorter threshold outright, so a
    configured retention below that is not merely ignored — the procedure errors. The matching
    catalog session properties are set for this statement so the configured value is the one that
    applies.
    """
    from provisa.api.app import state

    retention_hours = state.otel_snapshot_retention_hours
    if retention_hours is None:
        logger.debug("reclaim_otel: no ops_snapshot_retention_hours configured — skipping")
        return
    engine = state.federation_engine
    if engine is None:
        return

    threshold = f"{retention_hours}h"
    for signal in ("logs", "metrics", "traces"):
        for proc, hint in (
            ("expire_snapshots", "expire_snapshots_min_retention"),
            ("remove_orphan_files", "remove_orphan_files_min_retention"),
        ):
            try:
                await asyncio.to_thread(
                    engine.execute_engine_sync,
                    f"ALTER TABLE otel.signals.{signal} EXECUTE {proc}"
                    f"(retention_threshold => '{threshold}')",
                    session_hints={f"{OTEL_CATALOG}.{hint}": threshold},
                )
            except Exception:
                logger.warning("reclaim_otel: %s on %s failed", proc, signal, exc_info=True)


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
