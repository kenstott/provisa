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

# Rows pulled per fetchmany when streaming a DBAPI cursor. Bounds the in-memory
# working set to one batch instead of the whole result (REQ-028).
_STREAM_BATCH_ROWS = 1000

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
) -> ResultStream:
    """Build a lazily-streamed result from a DBAPI cursor/result whose fetch state OUTLIVES
    this call. Rows are pulled in batches of ``_STREAM_BATCH_ROWS`` via ``fetchmany`` so a
    large result never fully materializes.

    Preconditions the caller MUST guarantee: the cursor stays open until the stream drains,
    and no other query runs on it meanwhile (hold a private cursor). ``on_close`` fires once
    at drain — the DuckDB terminal uses it to close the private cursor. Drivers that close
    the cursor in a ``finally`` or share one cursor across concurrent queries must use
    :func:`result_from_dbapi` instead. A ``None`` description (non-SELECT) yields an empty
    materialized result and fires ``on_close`` immediately."""
    if not obj.description:
        if on_close is not None:
            on_close(StreamStats(done=True))
        return QueryResult(rows=[], column_names=[])
    cols = [d[0] for d in obj.description]

    def _batches() -> Iterator[list[tuple]]:
        while True:
            chunk = obj.fetchmany(_STREAM_BATCH_ROWS)
            if not chunk:
                return
            yield chunk

    return StreamingQueryResult(_batches(), column_names=cols, on_close=on_close)


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
