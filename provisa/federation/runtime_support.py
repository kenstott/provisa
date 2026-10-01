# Copyright (c) 2026 Kenneth Stott
# Canary: 61356214-20aa-4ad2-a541-783335fa5233
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared helpers for the federation runtimes (duckdb / clickhouse / pg / sqlalchemy).

These are behaviorless — the four ``*FederationRuntime`` classes share no state or
lifecycle (each owns a different connection object), so their common logic lives here
as free functions the concretes call, not in a base class."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from decimal import Decimal
from itertools import islice
from typing import Any, Callable

from provisa.executor.result import (
    QueryResult,
    ResultStream,
    StreamingQueryResult,
    StreamStats,
)

# Rows pulled per fetchmany when streaming a DBAPI cursor. Bounds the in-memory working set to one
# batch instead of the whole result (REQ-028). Shared by every row-cursor streaming consumer: gRPC's
# ENGINE-route row stream, pgwire/Flight SQL's DIRECT-route stream (execute_native_stream,
# _pg_passthrough_stream), pg_runtime's named-cursor stream, and sqlalchemy_runtime's yield_per stream
# — each batch crosses a thread-pool or run_coroutine_threadsafe hop, so a small batch means many hops
# for a large scan. Matches _ARROW_STREAM_BATCH_ROWS (REQ-1893): the memory-bounded integration suite
# (tests/integration/test_streaming_memory_bounded_e2e.py) measures a comparable 3-column row's total
# materialized footprint at ~215 bytes/row (~1 GiB / 5,000,000 rows), so a 65,536-row batch of plain
# Python tuples costs on the order of 10-20 MiB — trivial against that suite's 400 MiB RLIMIT_AS
# headroom and 500 MiB peak-RSS ceiling. A row-cursor batch (Python objects/protobuf messages) is not
# as cheap per row as Arrow's columnar buffers, but nothing here depends on it staying near 1000; the
# REQ-028 guarantee is "one batch, not the whole result", not a specific batch size.
_STREAM_BATCH_ROWS = 65_536

# Rows folded into one Arrow RecordBatch by the generic row→Arrow adapter (REQ-1219). Larger than
# the DBAPI fetch batch: an Arrow batch is columnar and cheap to hold, and fewer/larger batches cut
# per-batch transport overhead. Matches the native Arrow runtimes' record-batch size.
_ARROW_STREAM_BATCH_ROWS = 65_536


def result_from_dbapi(obj: Any) -> QueryResult:
    """Build a QueryResult from a DBAPI cursor or result object (anything exposing
    ``.description`` + ``.fetchall()``). A ``None`` description (non-SELECT) yields no
    columns and no rows. Used by the pg / sqlalchemy / duckdb runtimes; clickhouse
    delegates to its own ``_backend.query`` and does not use this.

    Fully materializes. Use :func:`stream_from_dbapi` where the cursor's fetch state
    outlives this call and the caller is on a worker thread — but NOT for drivers that
    close the cursor before the result is drained, nor across the async ``run_async``
    boundary (a blocking ``fetchmany`` must not be pulled on the event-loop thread)."""
    cols = [d[0] for d in obj.description] if obj.description else []
    rows = obj.fetchall() if obj.description else []
    return QueryResult(rows=rows, column_names=cols)


def stream_from_dbapi(
    obj: Any,
    *,
    on_close: Callable[[StreamStats], None] | None = None,
    type_names: Callable[[list[Any]], list[str]] | None = None,
) -> ResultStream:
    """Build a lazily-streamed result from a DBAPI cursor/result whose fetch state OUTLIVES
    this call. Rows are pulled in batches of ``_STREAM_BATCH_ROWS`` via ``fetchmany`` so a
    large result never fully materializes.

    Preconditions the caller MUST guarantee: the cursor stays open until the stream drains,
    and no other query runs on it meanwhile (hold a private cursor). ``on_close`` fires once
    at drain — the DuckDB terminal uses it to close the private cursor. Drivers that close
    the cursor in a ``finally`` or share one cursor across concurrent queries must use
    :func:`result_from_dbapi` instead. A ``None`` description (non-SELECT) yields an empty
    materialized result and fires ``on_close`` immediately.

    ``type_names`` maps the description's per-column type codes to declared type names, so the
    stream reports ``column_types`` even for zero rows (the pgwire Describe needs them without
    running the full statement, REQ-589)."""
    if not obj.description:
        if on_close is not None:
            on_close(StreamStats(done=True))
        return QueryResult(rows=[], column_names=[])
    cols = [d[0] for d in obj.description]
    types = type_names([d[1] for d in obj.description]) if type_names is not None else None

    def _batches() -> Iterator[list[tuple]]:
        while True:
            chunk = obj.fetchmany(_STREAM_BATCH_ROWS)
            if not chunk:
                return
            yield chunk

    return StreamingQueryResult(
        _batches(), column_names=cols, column_types=types, on_close=on_close
    )


def arrow_batches_from_rows(
    result: ResultStream,
    *,
    batch_rows: int = _ARROW_STREAM_BATCH_ROWS,
) -> tuple[Any, Iterator[Any]]:
    """Adapt a lazy row ``ResultStream`` into ``(pa.Schema, RecordBatch generator)`` — the generic
    Arrow-stream face for a ROWS-only engine (pg / sqlalchemy) that has no native Arrow reader
    (REQ-1219). NOT zero-copy — Python rows are packed into Arrow columns — but memory-bounded: only
    ``batch_rows`` rows are held at once, so Flight-SQL / airport stream from a row engine instead of
    materializing the whole result.

    The schema is LOCKED from the first batch (pyarrow type inference) and every later batch is cast
    to it, so all record batches share one schema. An incompatible later value (e.g. a column that was
    all-``NULL`` in the first batch but typed afterward) raises loudly rather than silently corrupting.
    A zero-row result yields a null-typed schema (column names only) and an empty generator."""
    import pyarrow as pa

    names = result.column_names

    def _conv(v: Any) -> Any:
        return float(v) if isinstance(v, Decimal) else v

    def _to_batch(rows: list[tuple], schema: Any | None) -> Any:
        cols = [[_conv(r[i]) for r in rows] for i in range(len(names))]
        if schema is None:
            return pa.RecordBatch.from_arrays([pa.array(c) for c in cols], names=names)
        arrays = [pa.array(c, type=schema.field(i).type) for i, c in enumerate(cols)]
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    row_iter = result.iter_rows()
    first_rows = list(islice(row_iter, batch_rows))
    if not first_rows:
        empty = pa.schema([pa.field(n, pa.null()) for n in names])
        return empty, iter(())
    first_batch = _to_batch(first_rows, None)
    schema = first_batch.schema

    def _gen() -> Iterator[Any]:
        yield first_batch
        while True:
            chunk = list(islice(row_iter, batch_rows))
            if not chunk:
                return
            yield _to_batch(chunk, schema)

    return schema, _gen()


def stream_rows_from_arrow(
    schema: Any,
    batches: Iterator[Any],
    *,
    on_close: Callable[[StreamStats], None] | None = None,
) -> ResultStream:
    """Adapt a lazy Arrow ``(schema, RecordBatch iterator)`` into a row-based ``StreamingQueryResult``
    — the inverse of :func:`arrow_batches_from_rows`. An engine whose only lazy primitive is Arrow
    (Snowflake, Databricks, BigQuery, ClickHouse, MSSQL) builds its streaming ROWS terminal
    (``run_sync``) on this, so the pgwire ENGINE route stays memory-bounded instead of materializing the
    whole result (REQ-1217, streaming-uniformity-gap Defect 3). Each Arrow batch is converted to row
    tuples on demand — peak memory is one batch — and draining the row stream drains (and closes) the
    underlying Arrow generator. Column names come from the Arrow schema; a zero-row result yields the
    columns and no rows. Column types are declared from the Arrow schema (``arrow_type_name``)."""
    names = list(schema.names)
    types = [arrow_type_name(f.type) for f in schema]

    def _batches() -> Iterator[list[tuple]]:
        for rb in batches:
            if not rb.num_rows:
                continue
            cols = [col.to_pylist() for col in rb.columns]
            yield list(zip(*cols, strict=True))

    return StreamingQueryResult(
        _batches(), column_names=names, column_types=types, on_close=on_close
    )


def arrow_type_name(t: Any) -> str:
    """A declared SQL type name for an Arrow type, for the pgwire RowDescription (REQ-589).

    Names map through ``provisa.pgwire.server._sql_type_to_bvtype``; the value each yields under
    ``to_pylist`` (int, float, Decimal, datetime, date, time, bool, bytes, str, list, dict) is what
    that wire type's encoder takes."""
    import pyarrow as pa

    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_integer(t):
        return "BIGINT"
    if pa.types.is_floating(t):
        return "DOUBLE"
    if pa.types.is_decimal(t):
        return "DECIMAL"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP"
    if pa.types.is_date(t):
        return "DATE"
    if pa.types.is_time(t):
        return "TIME"
    if pa.types.is_duration(t):
        return "INTERVAL"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        return "BLOB"
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        v = t.value_type
        if pa.types.is_integer(v):
            return "INTEGER[]"
        if pa.types.is_string(v) or pa.types.is_large_string(v):
            return "VARCHAR[]"
        return "JSON"
    if pa.types.is_struct(t) or pa.types.is_map(t):
        return "JSON"
    return "VARCHAR"


def columns_from_describe(rows: Any) -> dict[str, str]:
    """Map a DESCRIBE result's ``(name, type, ...)`` rows to ``{name: type_lower}``,
    the engine-introspection shape shared by the duckdb and clickhouse runtimes."""
    return {row[0]: str(row[1]).lower() for row in rows}


async def run_async(
    run_sync: Callable[[str, list | None], QueryResult],
    sql: str,
    params: list | None = None,
) -> QueryResult:
    """Run a runtime's synchronous ``run_sync`` on the default executor. The shared
    async wrapper for runtimes whose driver is blocking (pg / sqlalchemy / clickhouse)."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: run_sync(sql, params))


async def run_async_materialized(
    run_sync: Callable[[str, list | None], ResultStream],
    sql: str,
    params: list | None = None,
) -> QueryResult:
    """Async entry for a runtime whose ``run_sync`` STREAMS (Snowflake/Databricks/BigQuery/ClickHouse/
    MSSQL, built on ``stream_rows_from_arrow``). Drains the stream to a full ``QueryResult`` ON THE
    EXECUTOR THREAD — a lazy fetch pulled across the async boundary would block the event loop, so
    ``run`` materializes here exactly as the pg runtime's ``run`` does (REQ-1217, Defect 3)."""
    loop = asyncio.get_event_loop()

    def _drain() -> QueryResult:
        rs = run_sync(sql, params)
        return QueryResult(
            rows=list(rs.iter_rows()),
            column_names=rs.column_names,
            column_types=rs.column_types,
        )

    return await loop.run_in_executor(None, _drain)
