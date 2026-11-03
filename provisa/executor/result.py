# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1f2e94-3a7d-4c58-9e02-1d8a4f6c3b27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The engine-agnostic query result contract (REQ-028).

Every terminal — the ENGINE terminal (any federation engine) and the DIRECT native driver — returns
a result implementing :class:`ResultStream`. Two implementations exist:

* :class:`QueryResult` — a fully materialized result (``rows`` held in memory). Used by bounded
  metadata/catalog/config paths where buffering the whole set is intended.
* :class:`StreamingQueryResult` — a lazy result backed by an iterator of row batches, consumed
  once. Used by the row-producing terminals so large user result sets never materialize.

Both expose the same interface: ``column_names``, ``column_types``, ``iter_rows()``, ``batches()``,
and a ``rows`` accessor. Streaming consumers (pgwire DataRow, bolt RECORD, gRPC, ndjson/csv/arrow)
call ``batches()``/``iter_rows()``. Consumers that genuinely need the full set (GraphQL, JSON:API)
call ``rows`` to buffer explicitly — the buffering is visible in the call, never silent.

The type lives in its own neutral module so generic code never imports it from a specific engine's
executor.
"""

from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol, runtime_checkable

from provisa.core import request_deadline


@dataclass
class StreamStats:  # REQ-028
    """Cumulative counters for a result as it is consumed.

    Streaming stats are known end-of-stream, not up front: ``row_count`` tallies as batches flow and
    is final once ``done`` is set. ``byte_count`` stays ``None`` unless a layer that actually sees
    bytes fills it — Arrow batches (``RecordBatch.nbytes``) or the wire encoder — because a row of
    Python tuples has no meaningful byte size without serialization. No fabricated value is emitted.
    """

    row_count: int = 0
    byte_count: int | None = None
    done: bool = False


@runtime_checkable
class ResultStream(Protocol):  # REQ-028
    """The result contract shared by materialized and streaming results."""

    column_names: list[str]
    column_types: list[str] | None
    column_type_oids: list[int] | None
    stats: StreamStats

    def batches(self) -> Iterator[list[tuple]]:
        """Yield row batches. Streaming results yield once; materialized results yield their list."""
        ...

    def iter_rows(self) -> Iterator[tuple]:
        """Yield rows one at a time across all batches."""
        ...

    def arrow_batches(self) -> Iterator[Any]:
        """The result as Arrow record batches (REQ-2194): the engine's own, or a row result's
        rows built into them at its fetch boundary."""
        ...

    @property
    def rows(self) -> list[tuple]:
        """The full row list, materializing a streaming result if necessary."""
        ...


@dataclass
class QueryResult:  # REQ-028
    """A fully materialized query result.

    The row list is already in memory; ``batches()``/``iter_rows()`` iterate it without copying, and
    ``stats`` is complete on construction.
    """

    rows: list[tuple]
    column_names: list[str]
    column_types: list[str] | None = None
    # REQ-2190: the type oid PostgreSQL gave each column, where it ran the statement itself.
    column_type_oids: list[int] | None = None
    # REQ-1194/REQ-1195: when the ONE pipeline materialized the result to a sink instead of
    # returning rows, this carries the delivery handle ({sink, url|table_name, row_count, ...}) that
    # every transport reports in its own envelope. None for an inline row result.
    redirect: dict | None = field(default=None)
    # Native driver-reported DML row count (e.g. SQLAlchemy CursorResult.rowcount), for dialects
    # where a write has no RETURNING clause and ``rows`` is therefore empty. None when the driver
    # doesn't report one or the statement wasn't a DML write.
    rowcount: int | None = field(default=None)
    # REQ-536: the response-cache entry this result was served from (``cache.store.CachedResult``),
    # set by the pipeline on a HIT; None for a result that was executed. A surface reports
    # HIT/MISS and the entry's age from it (``cache.middleware.build_cache_headers``).
    cache_entry: Any = field(default=None)
    stats: StreamStats = field(init=False)

    def __post_init__(self) -> None:
        self.stats = StreamStats(row_count=len(self.rows), done=True)

    def iter_rows(self) -> Iterator[tuple]:
        yield from self.rows

    def batches(self, size: int | None = None) -> Iterator[list[tuple]]:
        if not self.rows:
            return
        if size is None:
            yield self.rows
            return
        for i in range(0, len(self.rows), size):
            yield self.rows[i : i + size]

    def materialize(self) -> QueryResult:
        return self

    def arrow_batches(self) -> Iterator[Any]:  # REQ-2194
        """The rows as Arrow record batches, built here from the rows (``ir_arrow.row_batch``):
        the fetch boundary of a row-returning engine (docs/arch/fetch-paths.md)."""
        from provisa.core.ir_arrow import row_batch

        for batch in self.batches(_ARROW_BATCH_ROWS):
            yield row_batch(batch, self.column_names, self.column_types)


#: Rows per record batch a row result is handed to an encoder in (DuckDB's Arrow reader's size).
_ARROW_BATCH_ROWS = 65_536


class DataRowResult(QueryResult):  # REQ-2194
    """A materialized result whose rows are a PostgreSQL source's own DataRow messages
    (``RawDataRowBytes``, a replayed passthrough): pgwire forwards them as they are."""

    def datarow_batches(self) -> Iterator[list[Any]]:
        return self.batches()


class StreamingQueryResult:  # REQ-028
    """A query result backed by a lazily-consumed iterator of row batches.

    The batches are pulled from the source (engine cursor, arrow record-batch reader, server-side
    cursor) on demand and drained exactly once. ``batches()``/``iter_rows()`` are the streaming
    path. ``rows`` (or ``materialize()``) buffers the whole result into memory — allowed, but the
    memory cost is explicit at the call site. Consuming the stream twice raises rather than silently
    returning an empty or partial result.

    ``stats.row_count`` tallies as batches are drained and is final once the source is exhausted; at
    that point ``stats.done`` is set and ``on_close(stats)`` — if given — is invoked exactly once.
    A terminal uses ``on_close`` to emit end-of-stream telemetry (e.g. set an OTel span attribute and
    end the span) since the total is unknown until the stream drains.
    """

    __slots__ = (
        "column_names",
        "column_types",
        "column_type_oids",
        "stats",
        "_batches",
        "_consumed",
        "_closed",
        "_materialized",
        "_on_close",
        "_on_release",
    )

    def __init__(
        self,
        batches: Iterable[list[tuple]],
        column_names: list[str],
        column_types: list[str] | None = None,
        *,
        on_close: Callable[[StreamStats], None] | None = None,
        on_release: Callable[[], None] | None = None,
        column_type_oids: list[int] | None = None,
    ) -> None:
        self.column_names = column_names
        self.column_types = column_types
        # REQ-2171: the type oid PostgreSQL gave each column, where it ran the statement itself.
        self.column_type_oids = column_type_oids
        self.stats = StreamStats()
        self._batches: Iterator[Any] | None = iter(batches)
        self._consumed = False
        self._closed = False
        self._materialized: list[tuple] | None = None
        self._on_close = on_close
        # Resource release (server-side cursor / pooled connection) that must run whether the stream
        # drains to exhaustion OR is closed early for its schema alone — the source is opened eagerly
        # at construction, so a never-iterated generator's finally can't be relied on to free it.
        self._on_release = on_release

    def batches(self) -> Generator[list[tuple], None, None]:
        if self._materialized is not None:
            if self._materialized:
                yield self._materialized
            return
        yield from self._drain()

    def _tally(self, batch: Any) -> None:
        self.stats.row_count += len(batch)

    def _drain(self) -> Generator[Any, None, None]:
        """The source's batches as it yields them, pulled once, tallied, and released at the end,
        when the consumer stops early, or when the source fails."""
        self._begin()
        source = self._batches
        self._batches = None
        assert source is not None  # guarded by _begin
        try:
            for batch in source:
                # REQ-1905: the one seam every stream on every transport is pulled through. A
                # request whose deadline has passed gets no further batch — its stream ends with
                # the timeout, whichever terminal feeds it and whichever transport drains it.
                request_deadline.check()
                self._tally(batch)
                yield batch
        except BaseException:
            # The request timed out — noticed here between batches, or raised into the pull of
            # one — the consumer stopped early and closed the stream's iterator (the buffered
            # threshold probe stops at threshold+1 rows), or the source failed as it produced a
            # batch (an engine error DuckDB meets past the first, duckdb_arrow.answered_batches).
            # The source (server-side cursor, pooled connection, the engine's catalogue read) is
            # released now, and the failure re-raised: nothing will drain this stream to the end.
            self._release_source(source)
            raise
        self._finish()

    def iter_rows(self) -> Iterator[tuple]:
        # Closing this iterator closes the batches under it, so a consumer that stops early
        # releases the source then, not whenever the abandoned generator is collected.
        batches = self.batches()
        try:
            for batch in batches:
                yield from batch
        finally:
            batches.close()

    @property
    def rows(self) -> list[tuple]:
        if self._materialized is None:
            # batches() tallies stats and fires _finish() on exhaustion.
            self._materialized = [row for batch in self.batches() for row in batch]
        return self._materialized

    def materialize(self) -> QueryResult:
        q = QueryResult(
            rows=self.rows, column_names=self.column_names, column_types=self.column_types
        )
        q.stats = self.stats
        return q

    def arrow_batches(self) -> Generator[Any, None, None]:  # REQ-2194
        """Each row batch as an Arrow record batch, built as it is pulled (``ir_arrow.row_batch``):
        the fetch boundary of a row-returning engine (docs/arch/fetch-paths.md)."""
        from provisa.core.ir_arrow import row_batch

        batches = self.batches()
        try:
            for batch in batches:
                if batch:
                    yield row_batch(batch, self.column_names, self.column_types)
        finally:
            batches.close()

    def _begin(self) -> None:
        if self._consumed:
            raise RuntimeError(
                "StreamingQueryResult already consumed; a streaming result yields once. "
                "Call .rows once up front if a consumer needs the full set or random access."
            )
        self._consumed = True

    def close(self) -> None:
        """Release the underlying source without draining the rows.

        A consumer that needs only the result schema (``column_names``/``column_types``, populated
        eagerly at open) closes the stream so the server-side cursor / pooled connection is freed —
        otherwise an un-iterated eager cursor leaks it. The generator's own ``finally`` cannot be
        relied on here: a generator suspended BEFORE its first yield takes no GeneratorExit on
        ``close()``, so its ``finally`` never runs. The explicit ``on_release`` hook frees the
        eagerly-opened resource regardless; idempotent with drain-to-exhaustion.
        """
        if self._closed:
            return
        source = self._batches
        self._batches = None
        self._consumed = True
        if source is not None:
            source_close = getattr(source, "close", None)
            if source_close is not None:
                source_close()
        self._finish()

    def _release_source(self, source: Iterator[Any]) -> None:
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            source_close = getattr(source, "close", None)
            if source_close is not None:
                source_close()
            self._finish()

    def _finish(self) -> None:
        # REQ-1905: giving the source back is a release section — the request's deadline does
        # not interrupt it (provisa.core.request_deadline.shielded).
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            self._finish_shielded()

    def _finish_shielded(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.stats.done = True
        if self._on_release is not None:
            self._on_release()
        if self._on_close is not None:
            self._on_close(self.stats)


class DataRowStreamResult(StreamingQueryResult):  # REQ-2194
    """A streaming result whose batches are a PostgreSQL source's own DataRow messages
    (``RawDataRowBytes``, the passthrough): pgwire forwards them as they are."""

    __slots__ = ()

    def datarow_batches(self) -> Generator[list[Any], None, None]:
        return self.batches()


class ArrowStreamResult(StreamingQueryResult):  # REQ-2194
    """A streaming result whose source is the engine's own Arrow record batches.

    ``arrow_batches()`` hands the batches on as the engine produced them, with no Python row in
    between: the face an encoder reads. ``batches()``/``iter_rows()``/``rows`` are the row face for
    the consumers that still take Python rows: each batch is turned into rows by ``to_rows`` only
    as that consumer pulls it, so the conversion happens at the consumer, never at the fetch. Either
    face drains the one stream, once; ``stats`` counts rows and the batches' bytes either way.
    """

    __slots__ = ("schema", "_to_rows")

    def __init__(
        self,
        schema: Any,
        batches: Iterable[Any],
        column_types: list[str] | None = None,
        *,
        to_rows: Callable[[Any], list[tuple]],
        on_close: Callable[[StreamStats], None] | None = None,
        on_release: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(
            batches,
            list(schema.names),
            column_types,
            on_close=on_close,
            on_release=on_release,
        )
        self.schema = schema
        self._to_rows = to_rows
        self.stats.byte_count = 0

    def _tally(self, batch: Any) -> None:
        self.stats.row_count += batch.num_rows
        self.stats.byte_count = (self.stats.byte_count or 0) + batch.nbytes

    def arrow_batches(self) -> Generator[Any, None, None]:
        """The engine's record batches, as it produced them."""
        if self._materialized is not None:
            raise RuntimeError("ArrowStreamResult was already read as rows")
        yield from self._drain()

    def batches(self) -> Generator[list[tuple], None, None]:
        if self._materialized is not None:
            if self._materialized:
                yield self._materialized
            return
        source = self._drain()
        try:
            for batch in source:
                if batch.num_rows:
                    yield self._to_rows(batch)
        finally:
            # A consumer that stops early, or a batch that fails to convert, releases the source.
            source.close()


#: What ArrowResult hands the dataclass __init__ for ``rows``: they are built from its table.
_FROM_TABLE: list[tuple] = []


class ArrowResult(QueryResult):  # REQ-2194
    """A materialized result held as the engine's Arrow table.

    ``table`` is what the engine returned; ``arrow_batches()`` hands it on with no Python row in
    between. ``rows`` is built from it by ``to_rows`` the first time a consumer reads it, so the
    conversion happens at the consumer, never at the fetch. A consumer that replaces ``rows``
    replaces the result: the table no longer answers for it and is dropped.
    """

    def __init__(
        self,
        table: Any,
        column_types: list[str] | None = None,
        *,
        to_rows: Callable[[Any], list[tuple]],
    ) -> None:
        self.table: Any = table
        self._to_rows = to_rows
        self._rows: list[tuple] | None = None
        super().__init__(
            rows=_FROM_TABLE, column_names=list(table.column_names), column_types=column_types
        )

    def __post_init__(self) -> None:
        self.stats = StreamStats(
            row_count=self.table.num_rows, byte_count=self.table.nbytes, done=True
        )

    @property
    def rows(self) -> list[tuple]:  # pyright: ignore[reportIncompatibleVariableOverride]
        if self._rows is None:
            self._rows = self._to_rows(self.table)
        return self._rows

    @rows.setter
    def rows(self, value: list[tuple]) -> None:  # pyright: ignore[reportIncompatibleVariableOverride]
        if value is _FROM_TABLE:  # the dataclass __init__: rows are built from the table when read
            return
        self._rows = value
        self.table = None

    def with_rows(self) -> ArrowResult:
        """This result with its rows built now, on the calling thread. An async terminal calls
        it on its worker thread, so a consumer awaiting the result does no per-row work on the
        event loop."""
        if self._rows is None:
            self._rows = self._to_rows(self.table)
        return self

    def __repr__(self) -> str:
        # The dataclass repr reads ``rows``, which would build every row of the table to log it.
        held = "rows replaced" if self.table is None else f"{self.table.num_rows} rows as Arrow"
        return f"ArrowResult(column_names={self.column_names!r}, {held})"

    def arrow_batches(self) -> Iterator[Any]:
        """The engine's record batches, as it produced them."""
        if self.table is None:
            raise RuntimeError(
                "ArrowResult rows were replaced; its Arrow table no longer holds them"
            )
        return iter(self.table.to_batches())


def rewrapped(  # REQ-2194
    stream: ResultStream,
    transform: Callable[[Iterator[Any]], Iterator[Any]],
    *,
    on_release: Callable[[], None] | None = None,
) -> StreamingQueryResult:
    """``stream`` as a stream of the same kind with its batches passed through ``transform`` (a
    permit guard, a response-cache tee): an Arrow result stays Arrow record batches, a
    passthrough's DataRow messages stay DataRow messages, rows stay rows. Wrapping never turns
    the engine's Arrow into Python rows."""
    if isinstance(stream, ArrowStreamResult) or (
        isinstance(stream, ArrowResult) and stream.table is not None
    ):
        schema = stream.schema if isinstance(stream, ArrowStreamResult) else stream.table.schema
        return ArrowStreamResult(
            schema,
            transform(stream.arrow_batches()),
            stream.column_types,
            to_rows=stream._to_rows,
            on_release=on_release,
        )
    kind = (
        DataRowStreamResult
        if isinstance(stream, (DataRowStreamResult, DataRowResult))
        else StreamingQueryResult
    )
    return kind(
        transform(stream.batches()),
        column_names=list(stream.column_names),
        column_types=stream.column_types,
        on_release=on_release,
        column_type_oids=stream.column_type_oids,
    )
