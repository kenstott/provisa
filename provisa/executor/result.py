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

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Callable, Protocol, runtime_checkable


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
    stats: StreamStats

    def batches(self) -> Iterator[list[tuple]]:
        """Yield row batches. Streaming results yield once; materialized results yield their list."""
        ...

    def iter_rows(self) -> Iterator[tuple]:
        """Yield rows one at a time across all batches."""
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
        "stats",
        "_batches",
        "_consumed",
        "_closed",
        "_materialized",
        "_on_close",
    )

    def __init__(
        self,
        batches: Iterable[list[tuple]],
        column_names: list[str],
        column_types: list[str] | None = None,
        *,
        on_close: Callable[[StreamStats], None] | None = None,
    ) -> None:
        self.column_names = column_names
        self.column_types = column_types
        self.stats = StreamStats()
        self._batches: Iterator[list[tuple]] | None = iter(batches)
        self._consumed = False
        self._closed = False
        self._materialized: list[tuple] | None = None
        self._on_close = on_close

    def batches(self) -> Iterator[list[tuple]]:
        if self._materialized is not None:
            if self._materialized:
                yield self._materialized
            return
        self._begin()
        source = self._batches
        self._batches = None
        assert source is not None  # guarded by _begin
        for batch in source:
            self.stats.row_count += len(batch)
            yield batch
        self._finish()

    def iter_rows(self) -> Iterator[tuple]:
        for batch in self.batches():
            yield from batch

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

    def _begin(self) -> None:
        if self._consumed:
            raise RuntimeError(
                "StreamingQueryResult already consumed; a streaming result yields once. "
                "Call .rows once up front if a consumer needs the full set or random access."
            )
        self._consumed = True

    def _finish(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.stats.done = True
        if self._on_close is not None:
            self._on_close(self.stats)
