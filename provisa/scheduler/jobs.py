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
import functools
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from provisa.federation.execution_auth import system_auth

import pyarrow as pa

from provisa.compiler.sql_literals import sql_literal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from provisa.core.trino_system_catalogs import OTEL_CATALOG
from provisa.observability.ops_schema import CATALOG_DRIVER_SCOPES, REQUEST_SPAN_ATTR

if TYPE_CHECKING:
    from provisa.scheduler.holder import Holders
    from provisa.core.models import ScheduledTrigger

logger = logging.getLogger(__name__)

# REQ-1428: concurrent object-store fetches per compaction chunk. Sized to the object store's
# round-trip latency rather than to CPU — the work is entirely network wait.
_OTEL_FETCH_WORKERS = 16


# The "org_admin" role is the platform's well-known system execution role for governed
# internal SQL (same default used by the Flight server, provisa/api/flight/server.py).
# Scheduled triggers carry no per-run identity, so scheduled SQL runs under it.
# REQ-1003: governed execution requires a role; this is the documented system role.
def _in_deployment_org(job: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    """Run a scheduled job as the deployment org's work (REQ-1266): a job fires with nothing bound,
    and these are the deployment's own -- its config triggers, its shared engine's upkeep, its
    platform registry's sweeps -- so each binds the deployment org by name for its run."""

    @functools.wraps(job)
    async def _bound(*args: Any, **kwargs: Any) -> None:
        from provisa.api.app import state
        from provisa.core.request_context import reset_current_org, set_current_org

        token = set_current_org(state.org_id)
        try:
            await job(*args, **kwargs)
        finally:
            reset_current_org(token)

    return _bound


async def _execute_sql(  # REQ-1003, REQ-1004
    sql: str, trigger_id: str, role: str, org_id: str
) -> None:
    """Run a scheduled SQL trigger: one insert, update or delete of registered tables, its date
    tokens rendered as values for this run (scheduler/trigger_sql.py), through the shared
    pipeline and its write admission as the trigger's role -- in ``org_id``, the org whose model
    holds the trigger, bound for the run (a scheduled job fires with nothing bound). Failures are
    logged and re-raised — never silently swallowed.
    """
    import sqlglot

    from provisa.audit.context import audit_identity_scope
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route
    from provisa.scheduler.trigger_sql import (
        TriggerSqlRefused,
        checked_trigger_sql,
        command_call,
    )

    rendered = checked_trigger_sql(sql, trigger_id, datetime.now(timezone.utc))
    call = command_call(sqlglot.parse_one(rendered, read="postgres"))
    token = set_current_org(org_id)
    try:
        # REQ-2001: every run is audited, as the trigger's principal on the scheduler's surface.
        with audit_identity_scope(f"scheduler:{trigger_id}", "scheduler"):
            from provisa.api.app import state
            from provisa.auth.derived_roles import service_acting_role

            # The node's own work: the trigger's role with the node's region derived role.
            role = service_acting_role(state, role)
            if call is not None:
                from provisa.pgwire.function_call import maybe_invoke_registered_function

                # One registered command, through the command executor as the trigger's role.
                result = await maybe_invoke_registered_function(rendered, role, state)
                if result is None:
                    raise TriggerSqlRefused(
                        f"trigger {trigger_id!r}: {call.name!r} is no command role {role!r} "
                        "may call"
                    )
            else:
                plan = await _govern_and_route(rendered, role)
                result = await _execute_plan(plan)
    except Exception:
        logger.exception("Trigger %s: scheduled SQL failed: %s", trigger_id, rendered)
        raise
    finally:
        reset_current_org(token)
    logger.info("Trigger %s: scheduled SQL executed (%d rows)", trigger_id, len(result.rows))


@_in_deployment_org
async def compact_otel_signals() -> None:  # REQ-302, REQ-303
    """Compact today's OTEL Parquet from MinIO into Iceberg vithe engine.

    Runs every minute. Deletes existing rows for today's partition before
    reinserting so re-runs are idempotent. Set OTEL_COMPACT_DATE (YYYY-MM-DD)
    to backfill a specific date.
    """
    import asyncio
    import os

    from provisa.api.app import state

    # The Parquet is compacted into the Iceberg ``otel.signals`` tables, which only an engine that
    # declares the ``otel`` catalog has (Trino) (REQ-302, REQ-303). A native engine that lands its
    # own traces (REQ-862, observability.native_traces) has each day's small batch files merged
    # into files of up to 64 MB instead; one that lands none has nothing to compact.
    if state.shared_federation_engine is None:
        return
    if not state.shared_federation_engine.has_otel_catalog:
        if state.shared_federation_engine.landed_signals:
            from provisa.observability import native_traces

            merged = await asyncio.to_thread(native_traces.compact, native_traces.traces_dir())
            logger.debug("compact_otel: merged %d native trace files", merged)
        return

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

    s3_endpoint = state.otel_s3_endpoint  # REQ-1913: the resolved setting `otel.s3_endpoint`
    otel_bucket = os.environ.get("PROVISA_OTEL_BUCKET", "provisa-otel")
    file_chunk = state.otel_compact_file_chunk
    max_files = state.otel_compact_max_files_per_run
    engine = state.shared_federation_engine

    loop = asyncio.get_event_loop()
    if loop.is_closed() or not loop.is_running():
        logger.warning("compact_otel: event loop shutting down, skipping this run")
        return

    # The inserts below run through the shared terminal, which on the hosted control plane is a
    # connection to a shard that idles to zero. Two failures follow if this is skipped: the terminal
    # dials the pod IP the coordinator was released from and every chunk dies on a connect timeout,
    # and a resumed coordinator has none of the catalogs `otel.signals` is written through. This
    # attaches to the live one when there is one — and when there is not, the run is skipped rather
    # than allowed to wake the shard: compaction ticks every minute, so waking here would keep a pod
    # up forever and idle-to-zero would save nothing (REQ-1448, REQ-1464). Nothing is lost by
    # waiting; the parquet stays in the object store and the next tick with a live shard takes it.
    from provisa.federation.engine_wake import attach_if_serving

    if not await attach_if_serving(state):
        logger.debug(
            "compact_otel: engine shard is not serving — leaving this run for a later tick"
        )
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
    #
    # return_exceptions=True, not the default: gather() with the default returns as soon as ANY
    # task raises, without waiting for its siblings — one signal failing fast (a bad row, a network
    # blip) could starve a slower sibling exactly the way REQ-1428 exists to prevent, if that
    # sibling's asyncio.to_thread() call hadn't yet gotten a worker thread. Every signal must
    # genuinely run to completion (success or failure) before this raises anything.
    results = await asyncio.gather(
        *(_one(s) for s in ("logs", "metrics", "traces")), return_exceptions=True
    )
    for result in results:
        if isinstance(result, BaseException):
            raise result


_DATE_SEG_RE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/")


def _key_date(key: str) -> datetime | None:
    """The partition date encoded in an otlp2parquet key, or None if the layout doesn't carry one."""
    m = _DATE_SEG_RE.search(key)
    if not m:
        return None
    return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _drop_foreign_rows(signal: str, table):
    """REQ-1428/REQ-1425: apply the collector's parquet-lane predicates to the bucket backlog.

    The collector drops both of these span classes before they are written now, but the bucket
    still holds the objects written before those filters existed, and compaction commits every one
    of their rows to Iceberg:

    * asyncpg statement spans (BEGIN, COMMIT, every SELECT the API makes against its metadata
      store) — 21 000 files in which they were 95% of the rows.
    * spans from any service other than Provisa. The Trino coordinator's javaagent emits a span
      per stage, task, split and internal HTTP call; on the cloud node they reached 3.57M of the
      3.58M rows in ``otel.signals.traces``, against 59 ``provisa.query`` spans. The queries
      report reads that table through a ``span_name LIKE 'provisa.query%'`` view, so a page of 51
      rows had to scan all 3.57M and took 2m34s — past the 60s request timeout, which is the
      Internal Server Error the report showed.

    Compaction walks oldest-date-first, so all of those rows had to be committed before the job
    reached the spans the ops reports read. Applying the same predicates on rows already in memory
    costs one filter each; the objects are deleted from the bucket either way.
    """
    if signal != "traces":
        return table
    import pyarrow as pa
    import pyarrow.compute as pc

    # ``call_function`` rather than ``pc.not_equal``/``pc.equal``: the comparison kernels are
    # generated onto the module at import time from the C++ function registry, so they exist at
    # runtime but are not members any reader -- human or checker -- can find in the source.
    if "scope_name" in table.column_names:
        for scope in CATALOG_DRIVER_SCOPES:
            keep = pc.call_function(
                "not_equal", [table.column("scope_name"), pa.scalar(scope, type=pa.string())]
            )
            table = table.filter(pc.fill_null(keep, True))
    if "service_name" in table.column_names:
        service = os.environ.get("PROVISA_OTEL_SERVICE_NAME", "provisa")
        keep = pc.call_function(
            "equal", [table.column("service_name"), pa.scalar(service, type=pa.string())]
        )
        table = table.filter(pc.fill_null(keep, False))
    if "parent_span_id" in table.column_names and "span_attributes" in table.column_names:
        # REQ-1910: `traces` holds one row per request (and per piece of background work). A span
        # with a parent is detail under one — unless it is itself a request span whose caller
        # sent a traceparent, which its provisa.transport attribute marks. The Iceberg lane keeps
        # no detail rows: a debug request's child spans reach only the built-in store's
        # trace_details table (otlp2sql) and whatever external OTLP endpoint is configured.
        parent = table.column("parent_span_id")
        is_root = pc.fill_null(pc.call_function("equal", [parent, pa.scalar("")]), True)
        is_request = pc.fill_null(
            pc.call_function(
                "match_substring",
                [table.column("span_attributes")],
                pc.MatchSubstringOptions(f'"{REQUEST_SPAN_ATTR}"'),
            ),
            False,
        )
        table = table.filter(pc.call_function("or", [is_root, is_request]))
    return table


# REQ-1435: OTLP instant columns that otlp2parquet writes as an integer rather than an arrow
# timestamp, so the Iceberg column came out BIGINT and every ops report rendered the instant as a
# long digit string — undatable by a reader and unsortable as time by the grid.
#
# The writer does NOT use one unit for all of them, and the epoch unit is not recoverable from the
# parquet type (they are all plain int64). Each is listed with the unit that signal's writer
# actually emits, verified against live objects in the bucket:
#
#   traces.end_timestamp        1786588661161      epoch milliseconds
#   metrics.start_timestamp     1786588918330      epoch milliseconds
#   logs.observed_timestamp     1786589996931031   epoch microseconds
#
# `timestamp` is written as timestamp[us] in all three signals and needs no reinterpretation.
# An unlisted integer instant column is a writer schema change, not something to guess at: the
# conversion raises so the new column is mapped deliberately instead of landing in 1970.
_EPOCH_INT_INSTANTS: dict[str, dict[str, str]] = {
    "traces": {"end_timestamp": "ms"},
    "metrics": {"start_timestamp": "ms"},
    "logs": {"observed_timestamp": "us"},
}

_INSTANT_COL_NAMES = frozenset(
    {"timestamp", "end_timestamp", "start_timestamp", "observed_timestamp"}
)


def _instants_from_epoch_ints(signal: str, table):
    """Reinterpret this signal's integer epoch columns as real timestamps.

    Cast in two steps on purpose: int64 -> timestamp(<the unit it was written in>) reinterprets the
    value, and only then is it rescaled to the microseconds every store in the lane keeps (Iceberg
    TIMESTAMP(6)). Casting straight to timestamp('us') reads a millisecond count as microseconds
    and puts every span in 1970.

    Columns already carrying an arrow timestamp are left alone.
    """
    import pyarrow as pa

    units = _EPOCH_INT_INSTANTS[signal]
    for name in table.column_names:
        if name.lower() not in _INSTANT_COL_NAMES:
            continue
        col = table.column(name)
        if not pa.types.is_integer(col.type):
            continue
        unit = units.get(name.lower())
        if unit is None:
            raise ValueError(
                f"compact_otel: {signal}.{name} arrived as an integer with no epoch unit mapped; "
                "add it to _EPOCH_INT_INSTANTS with the unit otlp2parquet writes"
            )
        idx = table.column_names.index(name)
        # safe=False is the rescale, not a shortcut: a sub-microsecond value cannot be represented
        # at TIMESTAMP(6), and a checked cast refuses it outright. Truncating those digits is the
        # intended narrowing — that is the precision the lane's stores keep.
        converted = col.cast(pa.timestamp(unit)).cast(pa.timestamp("us"), safe=False)
        table = table.set_column(idx, pa.field(name, pa.timestamp("us")), converted)
    return table


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

    # A table registered with its metadata unreadable is said (an ERROR naming it) and left as it
    # is: this signal is not compacted this run, and its files stay where they are for a run after
    # the table is recovered. The other signals carry on (ops_trino.metadata_missing).
    from provisa.observability.ops_trino import metadata_missing

    if metadata_missing(
        lambda sql: engine.execute_engine_sync(sql, authorization=system_auth("scheduled job")),
        signal,
    ):
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
                combined = _drop_foreign_rows(signal, combined)
                combined = _instants_from_epoch_ints(signal, combined)
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

_TRACE_EXTRA_COLS: list[str] = ["table_name", "domain_id", "role_id", "query_text", "tenant_id"]
_ATTR_KEYS: dict[str, str] = {
    "table_name": "provisa.table",
    "domain_id": "provisa.domain",
    "role_id": "provisa.role",
    "query_text": "provisa.query_text",
    # The org the span was started for (otel_setup.OrgSpanProcessor): what a read is scoped by.
    "tenant_id": "provisa.tenant",
}
# Each signal's attributes column, and the columns extracted from it at ingest. A metric point
# and a log record carry their org as a span does (otel_compat.record_query, otel_setup.OrgLogFilter).
_ATTR_COLUMNS: dict[str, str] = {
    "traces": "span_attributes",
    "metrics": "metric_attributes",
    "logs": "log_attributes",
}
_SIGNAL_EXTRA_COLS: dict[str, list[str]] = {
    "traces": _TRACE_EXTRA_COLS,
    "metrics": ["tenant_id"],
    "logs": ["tenant_id"],
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
    existing = {n.lower() for n in table.schema.names}
    for ec in _SIGNAL_EXTRA_COLS[signal]:
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
    engine.execute_engine_sync(create_ddl, authorization=system_auth("scheduled job"))
    if signal == "traces":
        try:
            engine.execute_engine_sync(
                f"ALTER TABLE otel.signals.{signal} "
                f"SET PROPERTIES partitioning = ARRAY[{', '.join(partition_cols)}]",
                authorization=system_auth("scheduled job"),
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
        # REQ-1435: an instant column narrowed back to an integer would be cast, not reinterpreted
        # — timestamp('us') -> int64 yields microseconds where the column's contract is an instant,
        # and the rows would land silently wrong. It means the table predates the instant columns
        # and has to be recreated, so say that instead of writing the bad value.
        if pa.types.is_timestamp(field.type) and not pa.types.is_timestamp(pa_type):
            raise ValueError(
                f"otel.signals.{signal}.{field.name} is {column_type} but the lane now writes a "
                f"timestamp; drop the table so it is recreated at the current schema"
            )
        cast_fields.append(pa.field(field.name, pa_type))
    return table.cast(pa.schema(cast_fields), safe=False)


def _build_insert_columns(
    _signal: str,
    table: pa.Table,
    engine_cols: dict[str, str],
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
    attr_col: str,
    extra_cols: list[str],
    engine_cols: dict[str, str],
) -> tuple:  # type: ignore[type-arg]
    """Convert a single row dict to a tuple for the engine INSERT: its own columns, then each
    extra column the object does not carry, read from its ``attr_col`` JSON (_build_insert_columns
    names the same extras)."""
    vals = dict(row)
    if "span_attributes" in vals and isinstance(vals["span_attributes"], str):
        vals["span_attributes"] = vals["span_attributes"][:_SPAN_ATTRS_MAX]
    base = tuple(vals.get(n) for n in parquet_cols) + (date_val,)
    extra = [ec for ec in extra_cols if ec not in row and ec.lower() in engine_cols]
    if not extra:
        return base
    raw = row.get(attr_col)
    attrs = json.loads(raw) if raw else {}
    return base + tuple(attrs.get(_ATTR_KEYS[ec]) for ec in extra)


def _resolve_batch_size(_signal: str) -> int:
    """Rows per INSERT statement, from `observability.compact_batch_size`.

    The clamp that used to hold this at ten rows — five for traces — was a workaround for the Trino
    client's parameterised-statement header limit. _execute_batch_inserts inlines its literals now,
    so the configured size is what runs, and each statement is one Iceberg commit instead of one
    commit per five spans.
    """
    from provisa.api.app import state as _state

    return max(_state.otel_compact_batch_size, 1)


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
            + ", ".join(
                ph.replace("?", sql_literal(v, engine.dialect)) for ph, v in zip(placeholders, row)
            )
            + ")"
        )
        over_chars = pending + len(rendered) > _MAX_INSERT_SQL_CHARS
        if values and (over_chars or len(values) >= batch_size):
            engine.execute_engine_sync(
                prefix + ", ".join(values), authorization=system_auth("scheduled job")
            )
            values = []
            pending = len(prefix)
        values.append(rendered)
        pending += len(rendered) + 2
    if values:
        engine.execute_engine_sync(
            prefix + ", ".join(values), authorization=system_auth("scheduled job")
        )


@_in_deployment_org
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
    engine = state.shared_federation_engine
    if engine is None:
        return
    # Snapshots and orphan files are the Iceberg ``otel`` catalog's, and ALTER TABLE ... EXECUTE is
    # Trino's procedure syntax: only an engine that declares that catalog has either. A native
    # engine's telemetry is the dedicated ops store, which keeps no snapshots, leaves no orphan
    # files and prunes its own rows (REQ-1910) — reclaim has nothing to do there (REQ-302, REQ-303).
    if not engine.has_otel_catalog:
        logger.debug("reclaim_otel: the engine has no otel Iceberg catalog — nothing to reclaim")
        return

    # Same seam as compaction: these ALTER TABLE ... EXECUTE statements go through the shared
    # terminal, so a released coordinator means a connect timeout per procedure and a resumed one
    # means the otel catalog is not there yet. Reclaim is maintenance and must not be what wakes the
    # shard (REQ-1448) — the orphan files keep until a tick that finds it already serving.
    from provisa.federation.engine_wake import attach_if_serving

    if not await attach_if_serving(state):
        logger.debug(
            "reclaim_otel: engine shard is not serving — leaving this run for a later tick"
        )
        return

    threshold = f"{retention_hours}h"
    from provisa.observability.ops_trino import metadata_missing

    for signal in ("logs", "metrics", "traces"):
        # A table whose metadata cannot be read is left as it is (said by metadata_missing): its
        # files may be the history its recovery restores, and orphan removal would delete them.
        if await asyncio.to_thread(
            metadata_missing, lambda sql: engine.execute_engine_sync(sql), signal
        ):
            continue
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
    engine.execute_engine_sync(
        "CREATE SCHEMA IF NOT EXISTS otel.signals", authorization=system_auth("scheduled job")
    )

    col_defs = _build_iceberg_col_defs(signal, table)
    partition_cols = ["'_date'", "'table_name'"] if signal == "traces" else ["'_date'"]
    _ensure_iceberg_table(engine, signal, col_defs, partition_cols)

    # Read back actual the engine column types and cast PyArrow table to match exactly.
    # ``otel.signals.*`` are Provisa's own telemetry tables in the engine's Iceberg catalog — the
    # store this job writes, read where it writes them. They are not tables of a registered
    # source and have no replica, so nothing here passes the address seam (REQ-1912).
    _cols = engine.execute_engine_sync(
        f"SHOW COLUMNS FROM otel.signals.{signal}", authorization=system_auth("scheduled job")
    )
    engine_cols = {row[0].lower(): row[1].lower() for row in _cols.rows}
    table = _cast_table_to_physical_schema(signal, table, engine_cols)

    date_val = dt.strftime("%Y-%m-%d")

    # Extract provisa-specific attributes (the org; for traces the request facts too) into
    # dedicated columns.
    attr_col = _ATTR_COLUMNS[signal]
    extra_cols = _SIGNAL_EXTRA_COLS[signal] if attr_col in table.schema.names else []

    parquet_cols, col_names, placeholders = _build_insert_columns(
        signal, table, engine_cols, extra_cols
    )

    rows = [
        _build_row(r, parquet_cols, date_val, attr_col, extra_cols, engine_cols)
        for r in table.to_pylist()
    ]
    if not rows:
        return

    batch_size = _resolve_batch_size(signal)
    _execute_batch_inserts(engine, signal, rows, col_names, placeholders, batch_size)


@_in_deployment_org
async def watch_engine() -> None:
    """Engine-terminal liveness watchdog (scheduled job). Delegates to the bound engine's
    watchdog through the seam — the engine restarts its container and replaces a dead connection;
    native engines have no external process to watch (no-op)."""
    from provisa.api.app import state
    from provisa.federation.engine_wake import attach_if_serving

    # A shard at zero replicas is not an unresponsive engine, and the watchdog cannot tell the two
    # apart: it pings, the ping times out against the released pod IP, and it tries to `docker start
    # provisa-trino-1` — a container that does not exist on a control plane whose engine runs on
    # GKE. Every tick of an idle shard therefore paid a connect timeout to reach a wrong action.
    # Resting is the designed state (REQ-1448); there is nothing here to watch until it is serving.
    if not await attach_if_serving(state):
        return

    await state.shared_federation_engine.watchdog()

    # Outside provisioning, a coordinator that restarted under the app -- the watchdog's restart above
    # or any other -- has no catalogs (catalog.store=memory). They are reissued here, within a minute,
    # rather than by whichever query reaches it first.
    from provisa.federation import k8s_provisioner
    from provisa.federation.engine_wake import reissue_after_coordinator_restart

    if not k8s_provisioner.provisioning_available():
        await reissue_after_coordinator_restart(state)


def unschedule_org(scheduler: "AsyncIOScheduler | None", org_id: str) -> int:  # REQ-2006
    """Remove every scheduled job of a deleted org -- each is named ``...:org_<id>``, an
    environment's replica build pass ``...:org_<id>:env_<name>`` -- and stop its replica build
    runners; how many jobs went. A job left behind fires against an org with no runtime, and fails
    every time it fires."""
    from provisa.federation.replica_builds import stop_replica_runner

    stop_replica_runner(org_id)
    if scheduler is None:
        return 0
    suffix = f":org_{org_id}"
    gone = [
        job.id
        for job in scheduler.get_jobs()
        if job.id.endswith(suffix) or f"{suffix}:env_" in job.id
    ]
    for job_id in gone:
        scheduler.remove_job(job_id)
    return len(gone)


async def reap_demo_sandboxes() -> None:  # REQ-2006
    """Delete the demo sandboxes no request has reached for ``sandbox.idle_ttl`` seconds. Run by
    the sandbox tier, the one process that opens their files."""
    from provisa.api.demo_sandbox import reap_expired

    reaped = await reap_expired()
    if reaped:
        logger.info("demo sandboxes reaped: %s", ", ".join(reaped))


async def stop_idle_sandbox_vm(since: datetime) -> None:  # REQ-2006
    """Stop the sandbox tier's own VM once no sandbox has been reached for
    ``PROVISA_SANDBOX_IDLE_STOP_SECONDS`` -- counted from the last request to any sandbox, or from
    ``since`` (when this process started) if none is open or none was reached since. Its
    sandboxes and templates are on its disk; the UI server starts it again when a request for the
    tier finds it down (provisa/sandbox_wake.py). Run by a sandbox tier on a VM of its own."""
    from provisa import sandbox_wake
    from provisa.api.demo_sandbox import last_active

    instance = sandbox_wake.SANDBOX_INSTANCE
    assert instance is not None  # scheduled only where the tier runs on a VM of its own
    idle = int(os.environ["PROVISA_SANDBOX_IDLE_STOP_SECONDS"])
    latest = await last_active()
    quiet_since = since if latest is None or latest < since else latest
    quiet = (datetime.now(timezone.utc) - quiet_since).total_seconds()
    if quiet < idle:
        return
    logger.warning("sandbox tier idle for %ds; stopping its VM %s", int(quiet), instance)
    await sandbox_wake.stop(instance)


@_in_deployment_org
async def reap_environments() -> None:  # REQ-1523
    """Delete the environments whose expiry has passed, with their schemas and their stores.

    A deployment with no admin control plane bound holds no ``environments`` rows at all — the table
    lives on the platform registry — so there is nothing expired for this to find and the job is not
    scheduled in the first place (see ``provisa.api.app_startup``). Both databases are read here
    rather than captured at registration because the pools are bound during the lifespan.
    """
    from provisa.api.app import state
    from provisa.core.env_reaper import reap_expired

    assert state.admin_db is not None  # the job is registered only when the admin plane is bound
    assert state.platform_model_db is not None

    reaped = await reap_expired(state.platform_model_db, state.admin_db, audit=_audit_reaped)
    if reaped:
        logger.info("reaped %d expired environment(s)", len(reaped))


async def _audit_reaped(org_id: str, name: str, outcome: dict) -> None:
    """Record the reap in the ORG's own trail, where the org can see what became of it (REQ-1488).

    The same trail and the same action shape the delete endpoint writes, because a reaped
    environment and a deleted one are the same act reached by two doors; the actor is the platform
    rather than a person, which is what the entry says.
    """
    from provisa.api.admin.orgs_router import _org_model_db
    from provisa.core.org_membership import record_admin_action

    await record_admin_action(
        await _org_model_db(org_id),
        action="environment.expired",
        actor_id="platform",
        subject_id=name,
        detail=outcome,
    )


def new_scheduler(holders: "Holders | None" = None) -> AsyncIOScheduler:
    """An AsyncIOScheduler whose wakeup chain never inherits a request's trace context.

    ``holders`` (REQ-1900, REQ-1922): the server's scheduler passes its ``Holders``, so the
    deployment's jobs run in one worker process only, and each region's jobs in one of its own.

    ``add_job`` on a running scheduler calls ``wakeup`` synchronously, and ``wakeup`` re-arms the
    timer with ``call_later`` -- which copies the CALLER's contextvars. A job registered from inside
    a request (the event-loop poll jobs a runtime build registers, REQ-1623) re-rooted the whole
    chain in that request's context, and every job the scheduler ran afterwards, including ones
    registered at startup, emitted its spans under that request's trace. Wrapping ``wakeup`` keeps
    the chain rootless whoever last called ``add_job``.
    """
    from provisa.otel_compat import detached_trace_context

    from provisa.scheduler.executor import background_scheduler

    # REQ-1882: jobs run on background worker threads, never on the process (front) loop.
    scheduler = background_scheduler(holders)
    inner = scheduler.wakeup

    def wakeup() -> None:
        with detached_trace_context():
            inner()

    scheduler.wakeup = wakeup  # type: ignore[method-assign]
    return scheduler


def trigger_job_id(trigger_id: str, org_id: str) -> str:
    """REQ-1003: the job of ``org_id``'s trigger ``trigger_id`` -- two orgs may share a trigger id."""
    return f"{trigger_id}:org_{org_id}"


def build_scheduler(
    triggers: list[ScheduledTrigger], org_id: str
) -> AsyncIOScheduler | None:  # REQ-216, REQ-177
    """Build an APScheduler instance from ``org_id``'s triggers; each job runs in that org.

    Returns None if no enabled triggers exist.
    """
    enabled = [t for t in triggers if t.enabled]
    if not enabled:
        return None

    scheduler = new_scheduler()
    _add_trigger_jobs(scheduler, enabled, org_id)
    return scheduler


def _add_trigger_jobs(scheduler: Any, triggers: list[ScheduledTrigger], org_id: str) -> None:
    """One job per scheduled trigger, each run through ``trigger_runs.run_trigger`` and recorded.
    A trigger on a table has no job: its table's change or event fires it (REQ-2054)."""
    from provisa.core.repositories.scheduled_trigger import _values

    for trigger in triggers:
        if trigger.cron is None:
            continue
        cron = CronTrigger.from_crontab(trigger.cron)
        job_id = trigger_job_id(trigger.id, org_id)
        scheduler.add_job(
            _run_scheduled,
            trigger=cron,
            args=[_values(trigger), org_id],
            id=job_id,
            name=f"trigger:{trigger.id}",
            replace_existing=True,
        )
        logger.info("Scheduled trigger %s: %s", job_id, trigger.cron)


async def _run_scheduled(trigger: dict, org_id: str) -> None:  # REQ-1003, REQ-2054
    """A scheduled trigger's run: given its declared arguments, recorded."""
    from provisa.scheduler.trigger_runs import run_trigger

    await run_trigger(trigger, org_id, given=dict(trigger["args"]), fired_by=None)


async def register_org_triggers(  # REQ-1003, REQ-1266
    scheduler: Any, org_id: str, env: str | None
) -> int:
    """Schedule ``org_id``'s prod triggers, as its model holds them, and unschedule the ones it no
    longer holds or has disabled. Only prod's are scheduled: an environment's triggers are
    definitions that run once promoted, so a runtime built for any other ``env`` schedules none.
    Returns how many are scheduled."""
    from provisa.api.app import state
    from provisa.core.environments import PROD

    if env is not None and env != PROD:
        return 0
    from provisa.core.models import ScheduledTrigger
    from provisa.core.repositories import scheduled_trigger as trigger_repo
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )

    org_token = set_current_org(org_id)
    env_token = set_current_env(None)
    try:
        assert state.model_db is not None, "the org's model store is bound with its runtime"
        async with state.model_db.acquire() as conn:
            rows = await trigger_repo.list_all(conn)
        # REQ-1934: a Data Profiler's schedule fires on this scheduler as a trigger's does, so its
        # jobs follow the org's model wherever the triggers' do.
        from provisa.profiler.source import register_org_profilers

        await register_org_profilers(scheduler, org_id)
        # REQ-1443: likewise each checker table's scans, every cache_ttl seconds.
        from provisa.dq.scan import register_org_checkers

        await register_org_checkers(scheduler, org_id)
        # REQ-506: and its service level, evaluated against the operator's limits.
        from provisa.audit.sla_monitor import register_org_sla

        register_org_sla(scheduler, org_id)
        # REQ-2077: and its role grants, swept of the expired and the unused.
        from provisa.auth.role_grants import register_org_role_grants

        register_org_role_grants(scheduler, org_id)
        # REQ-2091: and its sensitive columns' fakes and rules, checked for adequacy.
        from provisa.fakes.adequacy import register_org as register_org_fake_checks

        register_org_fake_checks(scheduler, org_id)
    finally:
        reset_current_env(env_token)
        reset_current_org(org_token)
    triggers = [
        ScheduledTrigger(
            id=r["id"],
            name=r["name"],
            cron=r["cron"],
            on_table=r["on_table"],  # REQ-2054
            on_event=r["on_event"],
            url=r["url"],
            webhook_name=r["webhook_name"],
            args=r["args"],
            sql=r["sql"],
            command=r["command"],
            role=r["role"],
            enabled=r["enabled"],
        )
        for r in rows
    ]
    enabled = [t for t in triggers if t.enabled and t.cron is not None]
    wanted = {trigger_job_id(t.id, org_id) for t in enabled}
    suffix = f":org_{org_id}"
    for job in scheduler.get_jobs():
        if job.name.startswith("trigger:") and job.id.endswith(suffix) and job.id not in wanted:
            scheduler.remove_job(job.id)
    _add_trigger_jobs(scheduler, enabled, org_id)
    return len(enabled)
