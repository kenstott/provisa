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
import re
from collections.abc import Iterator
from decimal import Decimal
from itertools import islice
from typing import Any, Callable

from provisa.core.read_refusal import ReadRefused
from provisa.core import request_deadline
from provisa.core.session_time_zone import carrying_zone
from provisa.core.ir_arrow import column_values
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


def take_shielded(take: Callable[[], Any]) -> Any:
    """Take a resource (a cursor, a pooled connection) in a section the request's deadline does
    not interrupt (REQ-1905): a raise landing after the resource exists and before the caller
    holds it would leak it. Release it with :func:`release_shielded`."""
    shield = request_deadline.shielded()
    with shield.lock:
        shield.settle()
        return take()


def release_shielded(release: Callable[[], None]) -> None:
    """Give a resource back in a section the request's deadline does not interrupt (REQ-1905)."""
    shield = request_deadline.shielded()
    with shield.lock:
        shield.settle()
        release()


def open_cursor(conn: Any) -> Any:
    """A cursor on ``conn``, taken under the shield (:func:`take_shielded`)."""
    return take_shielded(conn.cursor)


def close_cursor(cur: Any) -> None:
    """Close ``cur`` under the shield (:func:`release_shielded`)."""
    release_shielded(cur.close)


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


_DECLARED_DECIMAL = re.compile(r"^\s*(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", re.I)
_UNSIZED_DECIMAL = re.compile(r"^\s*(?:decimal|numeric)\s*$", re.I)
_DECIMAL128_MAX_PRECISION = 38


def _exact_decimals(values: list) -> Any:
    """``values`` (Decimals and None) as the exact decimal they are, at the widest scale held."""
    import pyarrow as pa

    # A non-finite Decimal (NaN, Infinity) has no decimal128 form: pyarrow refuses it below.
    exponents = [
        e
        for e in (v.as_tuple().exponent for v in values if isinstance(v, Decimal))
        if isinstance(e, int)
    ]
    scale = max([0, *(-e for e in exponents)])
    return pa.array(values, type=pa.decimal128(_DECIMAL128_MAX_PRECISION, scale))


def _as_double(decimals: Any) -> Any:
    """An Arrow decimal column as the nearest double of each value, column-wise. Through text:
    Arrow's direct decimal-to-double cast is not correctly rounded (0.3 becomes
    0.30000000000000004); its text-to-double parse is."""
    import pyarrow as pa

    return decimals.cast(pa.string()).cast(pa.float64())


def _array_as(values: list, locked: Any) -> Any:
    """A later batch's column as the type its first batch locked: a decimal column that is
    floating point is converted as the first batch was; anything else is built as ``locked``,
    and values that are not that type raise."""
    import pyarrow as pa

    if pa.types.is_floating(locked) and any(isinstance(v, Decimal) for v in values):
        return _as_double(_exact_decimals(values)).cast(locked)
    return pa.array(values, type=locked)


def arrow_array_for_rows(values: list, declared: str | None) -> Any:  # REQ-1219, REQ-1897
    """One column of row values as an Arrow array: the one rows->Arrow typing Flight uses for a
    live row stream and for a cached ``rows`` entry alike, so a miss and its hit share a schema.

    A decimal column is exact where its size is known and floating point where it is not
    (REQ-1961, the rule for a registered numeric applied to a result's): a declared
    ``DECIMAL(p,s)``/``NUMERIC(p,s)`` is ``decimal128(p, s)``; a ``Decimal`` column the source
    reports no size for (Postgres reports plain ``numeric`` for any computed decimal) is
    ``float64``. Its type therefore never depends on how many digits the rows at hand happen to
    have — typing it by the first batch's widest scale failed the read on a later, wider value.
    A statement that wants such a column exact states its size: ``CAST(... AS numeric(p,s))``.
    Anything else is pyarrow's inference (an aware datetime keeps its zone)."""
    import pyarrow as pa

    m = _DECLARED_DECIMAL.match(declared) if declared else None
    if m is not None:
        return pa.array(values, type=pa.decimal128(int(m.group(1)), int(m.group(2))))
    if any(isinstance(v, Decimal) for v in values):
        return _as_double(_exact_decimals(values))
    if declared and _UNSIZED_DECIMAL.match(declared):
        return pa.array(values, type=pa.float64())
    return pa.array(values)


class ColumnTypeMismatch(ReadRefused):
    """A column's values cannot be read as the type the column is registered with (REQ-1961).

    The model says what a column is and a read returns it as that; values that are not that are
    never rounded, widened or passed through as something else. The statement is good and the
    server has not failed: the registration and the source disagree, and a registrar puts it
    right — by registering the column with the type its source returns (for a numeric, the
    precision and scale it really has), or by correcting the source. One of the refusals for a
    read the deployment cannot answer now (``core.read_refusal``), so every surface answers it
    in one place with this code and these parameters.
    """

    code = "query.column_type_mismatch"

    def __init__(self, column: str, registered: Any, found: Any) -> None:
        self.column = column
        self.params = {"column": column, "registered": str(registered), "found": str(found)}
        super().__init__(
            f"column {column!r} is registered as {registered} and its values arrived as "
            f"{found}, which cannot be read as that without changing them. Register the column "
            f"with the type its source returns, or correct the source."
        )


def typed_arrow_array(values: list, target: Any, column: str) -> Any:  # REQ-1961
    """One column of row values as an Arrow array of ``target``, the type the column is
    registered with — never a type read off the values. Where Arrow cannot build ``target``
    from the Python values directly (a ``Decimal`` read as floating point), the values are built
    as what they are and cast by Arrow, column-wise. Values that cannot be read as the
    registered type are an error naming the column (:class:`ColumnTypeMismatch`)."""
    import pyarrow as pa

    try:
        return pa.array(values, type=target)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
        decimals = any(isinstance(v, Decimal) for v in values)
        found = _exact_decimals(values) if decimals else pa.array(values)
    try:
        if decimals and pa.types.is_floating(target):
            return _as_double(found).cast(target)
        return found.cast(target)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as mismatch:
        raise ColumnTypeMismatch(column, target, found.type) from mismatch


def declared_arrow_stream(  # REQ-1961
    schema: Any, batches: Iterator[Any], shape: list[tuple[str, str]] | None
) -> tuple[Any, Iterator[Any]]:
    """An engine's own Arrow stream with each registered column as the model declares it.

    The engine's reader types its result by the engine's own catalog, which is the model's for
    most columns and not for all (an attached source's integer read 64-bit, a replica's decimal
    sized by the engine). A result column that is a registered column (:func:`read_targets`) and
    arrives as another type is cast to the registered one by Arrow, a column of a batch at a
    time, and the schema says so before the first batch — the same schema for no rows as for
    many. A cast that would change a value is refused naming the column
    (:class:`ColumnTypeMismatch`). A stream in which nothing differs is handed back untouched.
    """
    import pyarrow as pa

    targets = read_targets(list(schema.names), shape)
    differing = [
        i
        for i, target in enumerate(targets)
        if target is not None and schema.field(i).type != target
    ]
    if not differing:
        return schema, batches
    declared = pa.schema(
        [
            field.with_type(targets[i]) if i in differing else field
            for i, field in enumerate(schema)
        ],
        metadata=schema.metadata,
    )

    def _as_declared() -> Iterator[Any]:
        try:
            for batch in batches:
                columns = list(batch.columns)
                for i in differing:
                    try:
                        columns[i] = columns[i].cast(targets[i])
                    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as mismatch:
                        raise ColumnTypeMismatch(
                            schema.field(i).name, targets[i], schema.field(i).type
                        ) from mismatch
                yield pa.RecordBatch.from_arrays(columns, schema=declared)
        finally:
            close = getattr(batches, "close", None)
            if close is not None:
                close()

    return declared, _as_declared()


def read_targets(names: list[str], shape: list[tuple[str, str]] | None) -> list[Any]:  # REQ-1961
    """For each result column, the Arrow type it is read as, or None — the column keeps the type
    its values give it.

    A result column that IS a registered column (projected bare, aliased or through a star:
    ``compiler.result_shape.RegisteredType``) is read as that column's registered type, by the
    one rule (``core.catalog_types.column_arrow_type``): what the model declares for a column is
    what a read of it returns. None for everything else: a computed expression (the model
    declares columns, not expressions, and its engine sizes the result — a sum of a two-digit
    decimal is the engine's wider decimal, exact), a statement with no derivable shape, a result
    column the shape does not name, a registered type neither vocabulary names, and a JSON
    column (REQ-1965: what a JSON column reads as is its reader's to produce). One computed
    column IS typed: a numeric whose size the statement states with a CAST
    (``compiler.result_shape.StatedType``)."""
    import pyarrow as pa

    from provisa.compiler.result_shape import RegisteredType, StatedType
    from provisa.core.catalog_types import column_arrow_type, is_json_type
    from provisa.core.numeric_type import numeric_arrow_type, numeric_size

    by_name = dict(shape or [])
    targets: list[Any] = []
    for name in names:
        declared = by_name.get(name)
        if isinstance(declared, RegisteredType) and not is_json_type(declared):
            targets.append(column_arrow_type(declared))
        elif isinstance(declared, StatedType) and numeric_size(declared) is not None:
            # The statement states a numeric's size (CAST(... AS numeric(p,s))): read exact, at
            # that size, on every path — the way to keep a computed decimal exact.
            targets.append(numeric_arrow_type(declared))
        elif isinstance(declared, StatedType) and pa.types.is_floating(
            stated := column_arrow_type(declared)
        ):
            # The statement states floating point -- as the read of a numeric the model records
            # no size for does (CAST(... AS DOUBLE PRECISION)): it is that type with or without
            # rows. Untyped, a read of no rows answered it as Arrow null.
            targets.append(stated)
        else:
            targets.append(None)
    return targets


def arrow_batches_from_rows(
    result: ResultStream,
    *,
    batch_rows: int = _ARROW_STREAM_BATCH_ROWS,
    shape: list[tuple[str, str]] | None = None,
) -> tuple[Any, Iterator[Any]]:
    """Adapt a lazy row ``ResultStream`` into ``(pa.Schema, RecordBatch generator)`` — the generic
    Arrow-stream face for a ROWS-only engine (pg / sqlalchemy) that has no native Arrow reader
    (REQ-1219). NOT zero-copy — Python rows are packed into Arrow columns — but memory-bounded: only
    ``batch_rows`` rows are held at once, so Flight-SQL / airport stream from a row engine instead of
    materializing the whole result.

    ``shape`` is the governed statement's result columns and their registered types (the plan's
    ``result_shape``, REQ-1961). A column it types is read as its registered type — an integer
    column is 32 bits, a numeric is exact where the model records its size and floating point
    where it does not — whatever the driver reported and whatever the first rows held, and a
    result with no rows has the same schema as one with rows. Without a shape, and for a column
    the shape does not type, the type is read off the values as before:

    the schema is LOCKED from the first batch (pyarrow type inference) and every later batch is cast
    to it, so all record batches share one schema. An incompatible later value (e.g. a column that was
    all-``NULL`` in the first batch but typed afterward) raises loudly rather than silently corrupting.
    A zero-row result yields a null type for such a column."""
    import pyarrow as pa

    def _to_batch(rows: list[tuple], schema: Any | None) -> Any:
        cols = [[r[i] for r in rows] for i in range(len(names))]
        if schema is None:
            declared = result.column_types or [None] * len(names)
            arrays = [
                arrow_array_for_rows(c, declared[i])
                if targets[i] is None
                else typed_arrow_array(c, targets[i], names[i])
                for i, c in enumerate(cols)
            ]
            return pa.RecordBatch.from_arrays(arrays, names=names)
        arrays = [
            _array_as(c, schema.field(i).type)
            if targets[i] is None
            else typed_arrow_array(c, targets[i], names[i])
            for i, c in enumerate(cols)
        ]
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    row_iter = result.iter_rows()
    first_rows = list(islice(row_iter, batch_rows))
    # A lazy stream opens on its first read, which is when it learns its columns.
    names = result.column_names
    targets = read_targets(names, shape)
    if not first_rows:
        empty = pa.schema(
            [pa.field(n, pa.null() if t is None else t) for n, t in zip(names, targets)]
        )
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
    column_types: list[str] | None = None,
) -> ResultStream:
    """Adapt a lazy Arrow ``(schema, RecordBatch iterator)`` into a row-based ``StreamingQueryResult``
    — the inverse of :func:`arrow_batches_from_rows`. An engine whose only lazy primitive is Arrow
    (Snowflake, Databricks, BigQuery, ClickHouse, MSSQL) builds its streaming ROWS terminal
    (``run_sync``) on this, so the pgwire ENGINE route stays memory-bounded instead of materializing the
    whole result (REQ-1217, streaming-uniformity-gap Defect 3). Each Arrow batch is converted to row
    tuples on demand — peak memory is one batch — and draining the row stream drains (and closes) the
    underlying Arrow generator. Column names come from the Arrow schema; a zero-row result yields the
    columns and no rows. Column types are declared from the Arrow schema (``arrow_type_name``), unless
    the caller gives the engine's own (``column_types``)."""
    names = list(schema.names)
    types = column_types if column_types is not None else [arrow_type_name(f.type) for f in schema]

    def _batches() -> Iterator[list[tuple]]:
        for rb in batches:
            if not rb.num_rows:
                continue
            cols = [column_values(col) for col in rb.columns]
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

    if pa.types.is_union(t):
        # ClickHouse's value of a type that cannot be Nullable (an array) or NULL is its Variant
        # of that type and NULL: the type it holds.
        held = [t.field(i).type for i in range(t.num_fields)]
        values = [h for h in held if not pa.types.is_null(h)]
        if len(values) == 1:
            return arrow_type_name(values[0])
        return "VARCHAR"
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
    if _is_list(t):
        # PostgreSQL has one array type of an element, whatever its dimensions.
        v = t.value_type
        while _is_list(v):
            v = v.value_type
        if pa.types.is_integer(v):
            return "INTEGER[]"
        if pa.types.is_string(v) or pa.types.is_large_string(v):
            return "VARCHAR[]"
        if pa.types.is_float64(v):
            return "DOUBLE[]"
        if pa.types.is_float32(v):
            return "REAL[]"
        return "JSON"
    if pa.types.is_struct(t) or pa.types.is_map(t):
        return "JSON"
    return "VARCHAR"


def _is_list(t: Any) -> bool:
    import pyarrow as pa

    return pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t)


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
    return await loop.run_in_executor(None, carrying_zone(lambda: run_sync(sql, params)))


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

    return await loop.run_in_executor(None, carrying_zone(_drain))
