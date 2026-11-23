# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8e1f27-9a3d-4b50-8e6f-2d7a1c9b5e30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Raw-SQL response-cache entries (REQ-1897, REQ-1867): the entry format, and the write-through tee.

Every plan the one pipeline executes (pgwire, Flight SQL, /data/sql, gRPC, Bolt, REST, GraphQL)
reads and writes the one response cache under ``cache.key.raw_sql_cache_key``. An entry holds the
result, never a surface's own response shape, and is ONE Arrow IPC stream (zstd-compressed
buffers) of one of two explicitly tagged kinds (schema metadata ``provisa.cache.kind``):

* ``arrow`` — the result's record batches. A miss whose terminal had the engine's Arrow (the
  buffered chokepoint, pgwire's ENGINE sink, gRPC's stream, Flight SQL's Arrow terminal) keeps
  those batches as they are (REQ-1867, amended 2026-11-23: never pickled or per-value encoded
  Python rows); a miss whose terminal drained Python rows (a DIRECT stream, Flight's Cypher
  terminal) keeps them as the Arrow ``ir_arrow.row_batch`` holds them, which reads back the values
  it was given. The engine's declared ``column_types`` travel in the schema metadata. A hit is an
  ``ArrowResult`` over the stored batches: pgwire and gRPC hand them to their Arrow encoders and
  Flight SQL streams them, with no value converted;
* ``pg_datarows`` — pgwire's Postgres passthrough (REQ-1863): the source's raw, wire-framed
  DataRow messages (one binary column) with the column names/types it advertised and the client's
  result format codes, replayed byte-for-byte by the passthrough terminal (a hit skips decode
  exactly as the miss did). Its bytes are specific to those format codes, so it lives under its
  own key (``wire_formats``) and only the passthrough terminal reads it.

A decoded reader handed a ``pg_datarows`` entry — or any reader handed an unknown kind — raises;
there is no defaulted read.

The tee (``ResponseCacheTee``) forwards every batch the moment it arrives — no added latency to the
first byte — and keeps a copy only while the result is within the bound: the org's large-result
threshold (``RedirectConfig.threshold``, the same inline-vs-redirect line GraphQL's buffered
transports use). Past it the copy is dropped at once and nothing is written; a stream that fails,
is closed early, or is cut off by the request deadline writes nothing either. Only a stream
drained to its end within the bound is stored.
"""

# Requirements: REQ-1897, REQ-1867

from __future__ import annotations

import functools
import json
from collections.abc import Callable, Coroutine, Iterable, Iterator
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pyarrow as pa

from provisa.executor.result import (
    ArrowResult,
    ArrowStreamResult,
    DataRowResult,
    QueryResult,
    ResultStream,
    StreamingQueryResult,
    is_arrow,
    rewrapped,
)

KIND_ARROW = "arrow"
KIND_PG_DATAROWS = "pg_datarows"

# Where a miss's batches came from: the engine's Arrow, or Python rows built into Arrow.
ORIGIN_ARROW = "arrow"
ORIGIN_ROWS = "rows"

_KIND = b"provisa.cache.kind"
_ORIGIN = b"provisa.cache.origin"
_TYPES = b"provisa.cache.column_types"
_NAMES = b"provisa.cache.column_names"
_FORMATS = b"provisa.cache.wire_formats"
# Rows-origin only: the columns that held a Decimal, which Flight reads as it reads a live row
# stream's (runtime_support.arrow_array_for_rows) whatever form row_batch kept them in.
_DECIMALS = b"provisa.cache.decimal_columns"
_OURS = frozenset({_KIND, _ORIGIN, _TYPES, _NAMES, _FORMATS, _DECIMALS})

_DATAROW = "datarow"
_IPC_OPTIONS = pa.ipc.IpcWriteOptions(compression="zstd")


@dataclass(frozen=True)
class CacheEntry:
    """A decoded response-cache entry. ``table`` is the result (``arrow``) or the DataRow
    messages, one per row of its one binary column (``pg_datarows``); its schema carries the
    miss's own metadata, never the entry's."""

    kind: str
    table: pa.Table
    column_types: list[str] | None
    origin: str | None = None
    column_names: list[str] | None = None
    wire_formats: list[int] | None = None
    decimal_columns: frozenset[int] = frozenset()


def _encode(schema: pa.Schema, batches: Iterable[pa.RecordBatch], meta: dict[bytes, Any]) -> bytes:
    own = {k: v for k, v in (schema.metadata or {}).items() if k not in _OURS}
    tagged = schema.with_metadata({**own, **{k: json.dumps(v) for k, v in meta.items()}})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, tagged, options=_IPC_OPTIONS) as writer:
        for batch in batches:
            # The batch's schema metadata is the stream's: only its columns are written.
            writer.write_batch(pa.RecordBatch.from_arrays(batch.columns, schema=tagged))
    return sink.getvalue().to_pybytes()


def encode_arrow_entry(
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    column_types: list[str] | None,
    *,
    origin: str = ORIGIN_ARROW,
    decimal_columns: Iterable[int] = (),
) -> bytes:
    """An ``arrow`` entry: ``batches`` as they are, in one Arrow IPC stream."""
    return _encode(
        schema,
        batches,
        {
            _KIND: KIND_ARROW,
            _ORIGIN: origin,
            _TYPES: column_types,
            _DECIMALS: sorted(decimal_columns),
        },
    )


def encode_rows_entry(
    rows: list[tuple], column_names: list[str], column_types: list[str] | None
) -> bytes:
    """An ``arrow`` entry of a miss that drained Python rows: the rows as ``ir_arrow.row_batch``
    holds them (each column's values read back as they were given)."""
    from provisa.core.ir_arrow import row_batch

    batch = row_batch(rows, column_names, column_types)
    decimals = [
        i for i in range(len(column_names)) if any(isinstance(row[i], Decimal) for row in rows)
    ]
    return encode_arrow_entry(
        batch.schema, [batch], column_types, origin=ORIGIN_ROWS, decimal_columns=decimals
    )


def encode_datarows_entry(
    datarows: list[bytes],
    column_names: list[str],
    column_types: list[str] | None,
    wire_formats: list[int],
) -> bytes:
    """A ``pg_datarows`` entry: the raw DataRow messages, undecoded."""
    column = pa.array([bytes(m) for m in datarows], pa.binary())
    schema = pa.schema([pa.field(_DATAROW, pa.binary())])
    return _encode(
        schema,
        [pa.RecordBatch.from_arrays([column], schema=schema)],
        {
            _KIND: KIND_PG_DATAROWS,
            _TYPES: column_types,
            _NAMES: column_names,
            _FORMATS: wire_formats,
        },
    )


def decode_entry(data: bytes) -> CacheEntry:
    """An entry written by one of the ``encode_*`` functions. An entry with no kind raises."""
    table = pa.ipc.open_stream(data).read_all()
    meta = table.schema.metadata or {}
    if _KIND not in meta:
        raise ValueError("response-cache entry carries no kind")
    tag = {k: json.loads(meta[k]) for k in _OURS if k in meta}
    own = {k: v for k, v in meta.items() if k not in _OURS}
    table = table.replace_schema_metadata(own or None)
    return CacheEntry(
        kind=tag[_KIND],
        table=table,
        column_types=tag.get(_TYPES),
        origin=tag.get(_ORIGIN),
        column_names=tag.get(_NAMES),
        wire_formats=tag.get(_FORMATS),
        decimal_columns=frozenset(tag.get(_DECIMALS, ())),
    )


def entry_as_datarows(entry: CacheEntry, wire_formats: list[int]) -> QueryResult:
    """A ``pg_datarows`` entry as the passthrough result it replays: raw DataRow messages
    (``RawDataRowBytes``) that pgwire forwards without decoding, under the columns the miss
    advertised. Any other kind, or other format codes, raises."""
    from buenavista.core import RawDataRowBytes

    if entry.kind != KIND_PG_DATAROWS:
        raise ValueError(f"passthrough replay needs a pg_datarows entry, got {entry.kind!r}")
    if entry.wire_formats != wire_formats:
        raise ValueError(
            f"pg_datarows entry was written for format codes {entry.wire_formats!r}, "
            f"not {wire_formats!r}"
        )
    assert entry.column_names is not None  # encode_datarows_entry always writes them
    # A RawDataRowBytes stands in the row slot, as it does on the live passthrough stream
    # (FederationEngine._pg_passthrough_stream): buenavista's send_data_rows forwards it verbatim.
    datarows: list[Any] = [RawDataRowBytes(m) for m in entry.table.column(_DATAROW).to_pylist()]
    return DataRowResult(
        rows=datarows,
        column_names=entry.column_names,
        column_types=entry.column_types,
    )


def _decoded_only(kind: str) -> ValueError:
    if kind == KIND_PG_DATAROWS:
        return ValueError(
            "a pg_datarows entry holds pgwire passthrough wire bytes; only the passthrough "
            "terminal replays it"
        )
    return ValueError(f"raw-SQL response-cache entry has unknown kind {kind!r}")


def entry_as_result(entry: CacheEntry) -> ArrowResult:
    """An ``arrow`` entry as the result a live execution returns: Arrow, its stored batches handed
    to an Arrow encoder as they are (REQ-2194); rows are built only for a consumer of rows."""
    if entry.kind != KIND_ARROW:
        raise _decoded_only(entry.kind)
    from provisa.federation.runtime_support import arrow_type_name

    table = entry.table
    held_text = [i for i in entry.decimal_columns if pa.types.is_string(table.schema.field(i).type)]
    return ArrowResult(
        table,
        entry.column_types
        if entry.column_types is not None
        else [arrow_type_name(f.type) for f in table.schema],
        to_rows=functools.partial(_table_rows, decimal_text=held_text)
        if held_text
        else _table_rows,
    )


def entry_as_arrow(entry: CacheEntry, shape: list[tuple[str, str]] | None) -> pa.Table:
    """An ``arrow`` entry as the Arrow table Flight serves, typed as Flight's miss types it.
    ``shape`` is the governed statement's result shape, which types each registered column
    (REQ-1961): an engine-Arrow entry gets the cast Flight's ENGINE terminal applies
    (``declared_arrow_stream``, untouched where nothing differs); a rows entry gets Flight's
    rows->Arrow typing (``arrow_array_for_rows``/``typed_arrow_array``), column-wise."""
    if entry.kind != KIND_ARROW:
        raise _decoded_only(entry.kind)
    from provisa.federation.runtime_support import declared_arrow_stream, read_targets

    table = entry.table
    if entry.origin == ORIGIN_ARROW:
        schema, batches = declared_arrow_stream(table.schema, iter(table.to_batches()), shape)
        return pa.Table.from_batches(list(batches), schema=schema)
    names = list(table.column_names)
    declared = entry.column_types if entry.column_types is not None else [None] * len(names)
    targets = read_targets(names, shape)
    columns = [
        _flight_column(
            table.column(i).combine_chunks(),
            declared[i],
            targets[i],
            names[i],
            i in entry.decimal_columns,
        )
        for i in range(len(names))
    ]
    return pa.table(columns, names=names)


def _flight_column(
    column: pa.Array, declared: str | None, target: Any, name: str, held_decimals: bool
) -> pa.Array:
    """One rows-entry column as Flight's live row stream types it (runtime_support's
    ``typed_arrow_array`` for a registered column, else ``arrow_array_for_rows``), by Arrow
    casts over the stored column. A decimal becomes floating point through its text, as
    ``_as_double`` does (Arrow's direct cast is not correctly rounded)."""
    from provisa.federation.runtime_support import (
        _DECLARED_DECIMAL,
        _UNSIZED_DECIMAL,
        ColumnTypeMismatch,
    )

    def as_double(col: pa.Array) -> pa.Array:
        return col.cast(pa.string()).cast(pa.float64())

    def cast(col: pa.Array, to: Any) -> pa.Array:
        if col.type == to:
            return col
        try:
            if held_decimals and pa.types.is_floating(to):
                return as_double(col).cast(to)
            return col.cast(to)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as mismatch:
            raise ColumnTypeMismatch(name, to, col.type) from mismatch

    if target is not None:
        return cast(column, target)
    sized = _DECLARED_DECIMAL.match(declared) if declared else None
    if sized is not None:
        return cast(column, pa.decimal128(int(sized.group(1)), int(sized.group(2))))
    if held_decimals:
        return as_double(column)
    if declared and _UNSIZED_DECIMAL.match(declared):
        return cast(column, pa.float64())
    return column


def _table_rows(table: Any, decimal_text: Iterable[int] = ()) -> list[tuple]:
    """An Arrow entry's rows, each column's values as a live Arrow read gives them
    (ir_arrow.column_values, as stream_rows_from_arrow reads them): an interval as the IR
    interval, an infinite date or timestamp as the IR infinity -- so a hit answers as the miss
    did. ``decimal_text`` names the columns of a rows entry whose Decimals ``row_batch`` kept as
    numeric_out's text (mixed scales, NaN, the infinities): each is the Decimal it was."""
    from provisa.core.ir_arrow import column_values

    cols = [column_values(col) for col in table.columns]
    for i in decimal_text:
        cols[i] = [None if v is None else Decimal(v) for v in cols[i]]
    return list(zip(*cols, strict=True)) if cols else []


def _releaser(stream: ResultStream) -> Callable[[], None] | None:
    """Closing the teed stream closes the source (its server-side cursor / pooled connection) —
    a live stream releases on close exactly as it did untee'd; a materialized result holds none."""
    return stream.close if isinstance(stream, StreamingQueryResult) else None


@dataclass
class _Target:
    """Where and how long a completed tee stores its entry."""

    store: Any
    key: str
    org_id: str | None
    ttl: int
    table_ids: set[int]


class ResponseCacheTee:
    """Write-through capture of one streamed result (see the module docstring for the contract).

    ``run`` stores the entry from a synchronous terminal (it runs the store coroutine on the
    terminal's own loop, e.g. pgwire's ``cl.run``); a terminal already on its loop passes ``None``
    and awaits :meth:`commit` after the drain."""

    def __init__(
        self,
        target: _Target,
        bound: int,
        run: Callable[[Coroutine[Any, Any, Any]], Any] | None,
    ) -> None:
        self._target = target
        self.bound = bound
        self._run = run
        self._buffer: list[Any] | None = []
        self._buffered_rows = 0
        self.peak_buffered_rows = 0
        self._complete = False
        self._entry: bytes | None = None
        self._seen_rows = 0
        # REQ-1949: asked when the stream ends, with the rows it delivered: whether what was
        # captured is the statement's whole answer. A cut answer is not kept as the answer.
        self.whole: Callable[[int], bool] | None = None

    # -- capture -------------------------------------------------------------------------------

    def _keep(self, item: Any, n_rows: int) -> None:
        self._seen_rows += n_rows
        if self._buffer is None:
            return
        if self._buffered_rows + n_rows > self.bound:
            self._buffer = None  # over the bound: drop the copy, never write
            return
        self._buffer.append(item)
        self._buffered_rows += n_rows
        self.peak_buffered_rows = max(self.peak_buffered_rows, self._buffered_rows)

    def _finished(self, encode: Callable[[list[Any]], bytes]) -> None:
        if self.whole is not None and not self.whole(self._seen_rows):
            self._buffer = None
        if self._buffer is not None:
            self._entry = encode(self._buffer)
        self._buffer = None
        self._complete = True
        if self._run is not None:
            self._run(self.commit())

    def rows(self, stream: ResultStream) -> ResultStream:
        """``stream`` with every batch forwarded as it arrives and captured within the bound: an
        Arrow result's record batches as they are (REQ-1867: never turned into rows to be kept),
        a row result's rows built into Arrow once the stream has drained."""
        column_names = list(stream.column_names)
        column_types = stream.column_types
        if is_arrow(stream):
            schema = stream.schema if isinstance(stream, ArrowStreamResult) else stream.table.schema
            return rewrapped(
                stream,
                lambda batches: self._arrow(schema, batches, column_types),
                on_release=_releaser(stream),
            )

        def _batches(batches: Iterator[list[tuple]]) -> Iterator[list[tuple]]:
            for batch in batches:
                self._keep(batch, len(batch))
                yield batch
            self._finished(
                lambda kept: encode_rows_entry(
                    [r for b in kept for r in b], column_names, column_types
                )
            )

        return rewrapped(stream, _batches, on_release=_releaser(stream))

    def result(self, result: QueryResult) -> None:
        """Capture a materialized ``result`` whole (the buffered chokepoint): an Arrow result's
        table as it is, a row result's rows. Nothing is iterated that the result does not hold."""
        if isinstance(result, ArrowResult) and result.table is not None:
            table = result.table
            self._keep(table, table.num_rows)
            self._finished(
                lambda kept: encode_arrow_entry(
                    table.schema, table.to_batches(), result.column_types
                )
            )
            return
        rows = result.rows
        self._keep(rows, len(rows))
        self._finished(
            lambda kept: encode_rows_entry(rows, list(result.column_names), result.column_types)
        )

    def datarows(self, stream: ResultStream, wire_formats: list[int]) -> ResultStream:
        """A passthrough ``stream`` of raw DataRow messages forwarded as each batch arrives and
        captured (still undecoded) within the bound — the ``pg_datarows`` entry."""
        column_names = list(stream.column_names)
        column_types = stream.column_types

        def _batches(batches: Iterator[list[tuple]]) -> Iterator[list[tuple]]:
            for batch in batches:
                self._keep(batch, len(batch))
                yield batch
            self._finished(
                lambda kept: encode_datarows_entry(
                    [m for b in kept for m in b], column_names, column_types, wire_formats
                )
            )

        return rewrapped(stream, _batches, on_release=_releaser(stream))

    def arrow(self, schema: Any, batches: Iterable[Any]) -> Iterator[Any]:
        """``batches`` (Arrow record batches) forwarded as they arrive and captured within the
        bound (Flight SQL's Arrow terminal, typed as it serves them)."""
        return self._arrow(schema, batches, None)

    def _arrow(
        self, schema: Any, batches: Iterable[Any], column_types: list[str] | None
    ) -> Iterator[Any]:
        for batch in batches:
            self._keep(batch, batch.num_rows)
            yield batch
        self._finished(lambda kept: encode_arrow_entry(schema, kept, column_types))

    # -- store ---------------------------------------------------------------------------------

    @property
    def stored_entry(self) -> CacheEntry | None:
        """The entry a completed, in-bound drain produced (None otherwise)."""
        return decode_entry(self._entry) if self._complete and self._entry is not None else None

    async def commit(self) -> None:
        """Store the entry — only after the stream drained to its end within the bound."""
        if not self._complete or self._entry is None:
            return
        from provisa.cache.middleware import store_result

        t = self._target
        await store_result(
            t.store, t.key, self._entry, ttl=t.ttl, table_ids=t.table_ids, org_id=t.org_id
        )


def new_tee(
    store: Any,
    key: str,
    org_id: str | None,
    ttl: int,
    table_ids: set[int],
    bound: int,
    run: Callable[[Coroutine[Any, Any, Any]], Any] | None,
) -> ResponseCacheTee:
    return ResponseCacheTee(_Target(store, key, org_id, ttl, table_ids), bound, run)


__all__ = [
    "KIND_ARROW",
    "KIND_PG_DATAROWS",
    "ORIGIN_ARROW",
    "ORIGIN_ROWS",
    "CacheEntry",
    "ResponseCacheTee",
    "decode_entry",
    "encode_arrow_entry",
    "encode_datarows_entry",
    "encode_rows_entry",
    "entry_as_arrow",
    "entry_as_datarows",
    "entry_as_result",
    "new_tee",
]
