# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""otlp2sql — an OTLP/HTTP receiver that writes traces/metrics/logs straight to
a SQL database (any SQLAlchemy URL), matching the ops-domain schema.

It's the native/desktop counterpart to the ``otlp2parquet`` + the engine-compaction
pipeline: because attributes are extracted **inline at ingest** (see
``TRACE_ATTR_COLS``), rows land already-shaped like the compacted output, so
there is no separate compaction step. Point the app/the engine OTLP exporter here
(``OTEL_EXPORTER_OTLP_ENDPOINT``) and the ops domain at the same DB
(``ops_db_url``), and telemetry is queryable the instant it lands.

Run standalone:  python -m provisa.observability.otlp2sql   (uvicorn on :4318)
Or mount ``build_app()`` into an existing ASGI app.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone

import sqlalchemy as sa
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
    ExportLogsServiceResponse,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)

from provisa.observability.ops_schema import (
    TRACE_ATTR_COLS,
    ensure_tables,
    ops_db_url,
)

log = logging.getLogger(__name__)

_engine: sa.Engine | None = None
_tables: dict[str, sa.Table] = {}

# Batched ingest: accumulate rows and flush on size or interval, rather than one
# INSERT per OTLP request. This is what makes a single sink viable against any
# engine (DuckDB, ClickHouse, a warehouse) — cheap ingest, engine-appropriate.
_BATCH_MAX_ROWS = int(os.environ.get("OTLP2SQL_BATCH_MAX_ROWS", "1000"))
_BATCH_MAX_SECS = float(os.environ.get("OTLP2SQL_BATCH_MAX_SECS", "2"))
_buffer: dict[str, list[dict]] = defaultdict(list)
_buf_lock = asyncio.Lock()


# ── attribute / value decoding ────────────────────────────────────────────────
def _anyval(v):
    """Decode an OTLP AnyValue oneof into a Python scalar/container."""
    which = v.WhichOneof("value")
    if which is None:
        return None
    if which == "string_value":
        return v.string_value
    if which == "int_value":
        return v.int_value
    if which == "double_value":
        return v.double_value
    if which == "bool_value":
        return v.bool_value
    if which == "bytes_value":
        return v.bytes_value.hex()
    if which == "array_value":
        return [_anyval(x) for x in v.array_value.values]
    if which == "kvlist_value":
        return {kv.key: _anyval(kv.value) for kv in v.kvlist_value.values}
    return None


def _attrs(kvs) -> dict:
    return {kv.key: _anyval(kv.value) for kv in kvs}


def _ns_to_ms(ns: int) -> int | None:
    return ns // 1_000_000 if ns else None


def _date_of(ms: int | None) -> date | None:
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, timezone.utc).date()


# ── OTLP -> rows ──────────────────────────────────────────────────────────────
def _trace_rows(req: ExportTraceServiceRequest) -> list[dict]:
    rows: list[dict] = []
    for rs in req.resource_spans:
        res_attrs = _attrs(rs.resource.attributes)
        svc = res_attrs.get("service.name")
        ns = res_attrs.get("service.namespace")
        res_json = json.dumps(res_attrs)
        for ss in rs.scope_spans:
            scope = ss.scope.name
            for sp in ss.spans:
                a = _attrs(sp.attributes)
                ts = _ns_to_ms(sp.start_time_unix_nano)
                end = _ns_to_ms(sp.end_time_unix_nano)
                row = {
                    "trace_id": sp.trace_id.hex(),
                    "span_id": sp.span_id.hex(),
                    "parent_span_id": sp.parent_span_id.hex() or None,
                    "span_name": sp.name,
                    "span_kind": int(sp.kind),
                    "service_name": svc,
                    "service_namespace": ns,
                    "timestamp": ts,
                    "end_timestamp": end,
                    "duration": (end - ts) if (ts and end) else None,
                    "status_code": int(sp.status.code),
                    "status_message": sp.status.message or None,
                    "scope_name": scope,
                    "span_attributes": json.dumps(a),
                    "resource_attributes": res_json,
                    "table_name": None,
                    "domain_id": None,
                    "role_id": None,
                    "query_text": None,
                    "tenant_id": None,
                    "_date": _date_of(ts),
                }
                for attr_key, col in TRACE_ATTR_COLS.items():
                    if attr_key in a:
                        row[col] = a[attr_key]
                rows.append(row)
    return rows


def _metric_rows(req: ExportMetricsServiceRequest) -> list[dict]:
    rows: list[dict] = []
    for rm in req.resource_metrics:
        res_attrs = _attrs(rm.resource.attributes)
        svc = res_attrs.get("service.name")
        ns = res_attrs.get("service.namespace")
        res_json = json.dumps(res_attrs)
        for sm in rm.scope_metrics:
            scope = sm.scope.name
            for m in sm.metrics:
                kind = m.WhichOneof("data")
                points = []
                if kind == "gauge":
                    points = [(dp, "gauge") for dp in m.gauge.data_points]
                elif kind == "sum":
                    points = [(dp, "sum") for dp in m.sum.data_points]
                else:
                    continue  # histograms/summaries: not modelled as a scalar row
                for dp, mtype in points:
                    ts = _ns_to_ms(dp.time_unix_nano)
                    val = dp.as_double if dp.HasField("as_double") else float(dp.as_int)
                    rows.append(
                        {
                            "timestamp": ts,
                            "start_timestamp": _ns_to_ms(dp.start_time_unix_nano),
                            "metric_name": m.name,
                            "metric_description": m.description or None,
                            "metric_unit": m.unit or None,
                            "metric_type": mtype,
                            "service_name": svc,
                            "service_namespace": ns,
                            "scope_name": scope,
                            "metric_attributes": json.dumps(_attrs(dp.attributes)),
                            "resource_attributes": res_json,
                            "value": val,
                            "tenant_id": None,
                            "_date": _date_of(ts),
                        }
                    )
    return rows


def _log_rows(req: ExportLogsServiceRequest) -> list[dict]:
    rows: list[dict] = []
    for rl in req.resource_logs:
        res_attrs = _attrs(rl.resource.attributes)
        svc = res_attrs.get("service.name")
        ns = res_attrs.get("service.namespace")
        res_json = json.dumps(res_attrs)
        for sl in rl.scope_logs:
            scope = sl.scope.name
            for r in sl.log_records:
                ts = _ns_to_ms(r.time_unix_nano)
                rows.append(
                    {
                        "timestamp": ts,
                        "observed_timestamp": _ns_to_ms(r.observed_time_unix_nano),
                        "trace_id": r.trace_id.hex() or None,
                        "span_id": r.span_id.hex() or None,
                        "severity_number": int(r.severity_number),
                        "severity_text": r.severity_text or None,
                        "body": _anyval(r.body) if r.HasField("body") else None,
                        "service_name": svc,
                        "service_namespace": ns,
                        "scope_name": scope,
                        "log_attributes": json.dumps(_attrs(r.attributes)),
                        "resource_attributes": res_json,
                        "tenant_id": None,
                        "_date": _date_of(ts),
                    }
                )
    return rows


# ── ingest ────────────────────────────────────────────────────────────────────
def _insert(table_name: str, rows: list[dict]) -> None:
    if not rows or _engine is None:
        return
    tbl = _tables[table_name]
    with _engine.begin() as conn:
        conn.execute(sa.insert(tbl), rows)


async def _flush() -> None:
    """Drain the buffer and insert each table's rows in a threadpool (writes
    serialize through the one-connection pool). Best-effort — telemetry is
    lossy under error, never fatal."""
    async with _buf_lock:
        pending = {t: rows for t, rows in _buffer.items() if rows}
        _buffer.clear()
    for table_name, rows in pending.items():
        try:
            await run_in_threadpool(_insert, table_name, rows)
        except Exception:
            log.exception("otlp2sql: flush failed for %s (%d rows)", table_name, len(rows))


async def _flush_loop() -> None:
    try:
        while True:
            await asyncio.sleep(_BATCH_MAX_SECS)
            await _flush()
    except asyncio.CancelledError:
        pass


async def _enqueue(table_name: str, rows: list[dict]) -> None:
    if not rows:
        return
    async with _buf_lock:
        buf = _buffer[table_name]
        buf.extend(rows)
        over = len(buf) >= _BATCH_MAX_ROWS
    if over:  # size trigger — don't wait for the interval
        await _flush()


async def _handle(request: Request, decode, to_rows, table_name: str, resp_cls) -> Response:
    body = await request.body()
    if request.headers.get("content-encoding", "").lower() == "gzip":
        body = gzip.decompress(body)
    msg = decode()
    msg.ParseFromString(body)
    await _enqueue(table_name, to_rows(msg))
    return Response(resp_cls().SerializeToString(), media_type="application/x-protobuf")


async def traces(request: Request) -> Response:
    return await _handle(
        request, ExportTraceServiceRequest, _trace_rows, "traces", ExportTraceServiceResponse
    )


async def metrics(request: Request) -> Response:
    return await _handle(
        request, ExportMetricsServiceRequest, _metric_rows, "metrics", ExportMetricsServiceResponse
    )


async def logs(request: Request) -> Response:
    return await _handle(
        request, ExportLogsServiceRequest, _log_rows, "logs", ExportLogsServiceResponse
    )


async def health(_: Request) -> Response:
    return PlainTextResponse("ok")


def build_app(db_url: str | None = None) -> Starlette:
    """ASGI app; creates the ops tables on startup against ``db_url`` (or
    ``ops_db_url()``)."""

    @asynccontextmanager
    async def _lifespan(_app):
        global _engine, _tables, _BATCH_MAX_ROWS, _BATCH_MAX_SECS
        # Read batch config at startup (not import) so env set before build_app
        # takes effect.
        _BATCH_MAX_ROWS = int(os.environ.get("OTLP2SQL_BATCH_MAX_ROWS", "1000"))
        _BATCH_MAX_SECS = float(os.environ.get("OTLP2SQL_BATCH_MAX_SECS", "2"))
        url = db_url or ops_db_url()
        kwargs: dict = {"future": True}
        # DuckDB/SQLite are single-writer file stores with only a SYNC driver.
        # We never touch an async driver — inserts run in a threadpool (see
        # _insert). A one-connection pool serializes those writes so concurrent
        # threads can't corrupt the single writable handle.
        if url.startswith(("duckdb", "sqlite")):
            kwargs.update(poolclass=sa.pool.QueuePool, pool_size=1, max_overflow=0)
        else:
            kwargs["pool_pre_ping"] = True
        _engine = sa.create_engine(url, **kwargs)
        _tables = ensure_tables(_engine)
        log.info("otlp2sql: ready -> %s", _engine.url.render_as_string(hide_password=True))
        flusher = asyncio.create_task(_flush_loop())
        try:
            yield
        finally:
            flusher.cancel()
            await _flush()  # drain anything buffered before shutdown
            _engine.dispose()

    return Starlette(
        lifespan=_lifespan,
        routes=[
            Route("/v1/traces", traces, methods=["POST"]),
            Route("/v1/metrics", metrics, methods=["POST"]),
            Route("/v1/logs", logs, methods=["POST"]),
            Route("/health", health, methods=["GET"]),
        ],
    )


app = build_app()


def main() -> None:
    import os

    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("OTLP2SQL_PORT", "4318")))


if __name__ == "__main__":
    main()
