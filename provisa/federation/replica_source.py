# Copyright (c) 2026 Kenneth Stott
# Canary: edbf44ff-8b74-4263-87ec-b3e898ca8910
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a source table's rows are read for replication: as a stream of Arrow record batches
(REQ-1915).

A replica is always built from a stream. Each kind of read here yields bounded Arrow record
batches and declares, in one place, the capability it is (``data_replicator.SourceRead``):

- :class:`EngineTableSource` — the engine reads the table and streams Arrow batches;
- :class:`DirectTableSource` — the source's own driver, through a server-side cursor;
- :class:`CursorSource` / :class:`BlockingCursorSource` — an adapter that yields bounded
  batches of rows off its client's own cursor;
- :class:`ArrowStreamSource` — a driver that yields Arrow batches itself;
- :class:`DocumentSource` — an adapter whose answer is one document, held whole and then cut
  into batches. It is declared as what it is: the memory it needs is the document's size.

Rows from a cursor or a document become batches through one helper, typed by the table's
declared columns.
"""

# Requirements: REQ-1915

from __future__ import annotations

import asyncio
from contextlib import aclosing
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterator
from typing import Any, Protocol

import pyarrow as pa

from provisa.core.ir_arrow import arrow_schema, rows_to_batch
from provisa.federation.data_replicator import BuildNote, SourceCaps, SourceRead

#: The default bound on one batch, in rows: the engines' own stream batch size.
BATCH_ROWS = 65_536


class ReplicaSource(Protocol):
    """One source table, readable as a stream."""

    caps: SourceCaps

    def batches(self, batch_rows: int) -> AsyncIterator[pa.RecordBatch]:
        """The table's rows as record batches of at most ``batch_rows`` rows."""
        ...


def _bounded(batch: pa.RecordBatch, batch_rows: int) -> list[pa.RecordBatch]:
    """``batch`` cut into slices of at most ``batch_rows`` rows (slices share its buffers)."""
    if batch.num_rows <= batch_rows:
        return [batch]
    return [batch.slice(start, batch_rows) for start in range(0, batch.num_rows, batch_rows)]


class EngineTableSource:
    """The engine reads the table at ``ref`` and streams it as Arrow record batches.

    ``in_place`` says the engine reads the source itself (a live attach), which is what lets the
    engine also run the copy as its own statement where the store allows it."""

    def __init__(self, engine_runtime: Any, ref: str, *, in_place: bool) -> None:
        self._engine = engine_runtime
        self._ref = ref
        reads = {SourceRead.ARROW_STREAM}
        if in_place:
            reads.add(SourceRead.ENGINE_REACHABLE)
        self.caps = SourceCaps(frozenset(reads))

    async def batches(self, batch_rows: int) -> AsyncIterator[pa.RecordBatch]:
        from provisa.federation.execution_auth import SystemAuth, mint_system_token

        sql = f"SELECT * FROM {self._ref}"
        # The system's own read of a source table for its replica: authorized as such, and
        # pinned to this one statement (REQ-1760).
        _schema, stream = self._engine.execute_engine_stream(
            sql,
            authorization=SystemAuth(
                mint_system_token(), reason=f"replica_build:{self._ref}", expected_sql=sql
            ),
        )
        try:
            for batch in stream:
                for part in _bounded(batch, batch_rows):
                    yield part
        finally:
            stream.close()


class DirectTableSource:
    """The source's own driver reads the table through a server-side cursor, a bounded fetch at
    a time. ``open_stream`` opens that cursor (``executor.direct.open_direct_stream`` bound to
    the source and its statement)."""

    caps = SourceCaps(frozenset({SourceRead.CURSOR}))

    def __init__(
        self, open_stream: Callable[[], Awaitable[Any]], columns: list[tuple[str, str]]
    ) -> None:
        self._open = open_stream
        self._columns = columns

    async def batches(self, batch_rows: int) -> AsyncIterator[pa.RecordBatch]:
        schema = arrow_schema(self._columns)
        stream = await self._open()
        try:
            names = list(stream.column_names)
            while True:
                chunk = await stream.fetch(batch_rows)
                if not chunk:
                    return
                rows = [dict(zip(names, row)) for row in chunk]
                yield rows_to_batch(rows, self._columns, schema)
        finally:
            await stream.close()


class CursorSource:
    """An adapter that yields the table's rows in bounded batches (``row_batches(batch_rows)``
    is an async iterator of lists of row dicts)."""

    caps = SourceCaps(frozenset({SourceRead.CURSOR}))

    def __init__(
        self,
        row_batches: Callable[[int], AsyncIterator[list[dict]]],
        columns: list[tuple[str, str]],
        *,
        notes: Callable[[], list[BuildNote]] | None = None,
    ) -> None:
        self._row_batches = row_batches
        self._columns = columns
        self._notes = notes

    def notes(self) -> list[BuildNote]:
        """What the adapter has to say of the read just made, once it is read to the end."""
        return self._notes() if self._notes is not None else []

    async def batches(self, batch_rows: int) -> AsyncIterator[pa.RecordBatch]:
        schema = arrow_schema(self._columns)
        async for rows in self._row_batches(batch_rows):
            for start in range(0, len(rows), batch_rows):
                yield rows_to_batch(rows[start : start + batch_rows], self._columns, schema)


async def _stepped(make: Callable[[], Iterator[Any]]) -> AsyncGenerator[Any, None]:
    """A blocking generator's items, each produced off the event loop, and the generator closed
    (its client and cursor released) however the iteration ends."""
    iterator = make()
    try:
        while True:
            item = await asyncio.to_thread(next, iterator, _DONE)
            if item is _DONE:
                return
            yield item
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            await asyncio.to_thread(close)


_DONE: Any = object()


class BlockingCursorSource:
    """A driver whose cursor is a blocking iterator of row batches: ``row_batches(batch_rows)``
    returns it. Each batch is fetched off the event loop; only one is held."""

    caps = SourceCaps(frozenset({SourceRead.CURSOR}))

    def __init__(
        self,
        row_batches: Callable[[int], Iterator[list[dict]]],
        columns: list[tuple[str, str]],
    ) -> None:
        self._row_batches = row_batches
        self._columns = columns

    async def batches(self, batch_rows: int) -> AsyncIterator[pa.RecordBatch]:
        schema = arrow_schema(self._columns)
        # aclosing: a reader that stops early closes the cursor now, not when it is collected.
        async with aclosing(_stepped(lambda: self._row_batches(batch_rows))) as stepped:
            async for rows in stepped:
                for start in range(0, len(rows), batch_rows):
                    yield rows_to_batch(rows[start : start + batch_rows], self._columns, schema)


class ArrowStreamSource:
    """A driver that yields Arrow record batches itself. ``open_stream()`` connects and returns
    the blocking iterator of batches and the call that closes the driver; batches pass through
    as they are, cut to the batch bound."""

    caps = SourceCaps(frozenset({SourceRead.ARROW_STREAM}))

    def __init__(
        self,
        open_stream: Callable[
            [],
            Awaitable[tuple[Callable[[], Iterator[pa.RecordBatch]], Callable[[], Awaitable[None]]]],
        ],
    ) -> None:
        self._open = open_stream

    async def batches(self, batch_rows: int) -> AsyncIterator[pa.RecordBatch]:
        record_batches, close = await self._open()
        try:
            async with aclosing(_stepped(record_batches)) as stepped:
                async for batch in stepped:
                    for part in _bounded(batch, batch_rows):
                        yield part
        finally:
            await close()


class DocumentSource:
    """An adapter whose answer is one document: ``load()`` returns every row at once. The rows
    are held whole, as the document is, and written in bounded batches."""

    caps = SourceCaps(frozenset({SourceRead.SINGLE_DOCUMENT}))

    def __init__(
        self, load: Callable[[], Awaitable[list[dict]]], columns: list[tuple[str, str]]
    ) -> None:
        self._load = load
        self._columns = columns

    async def batches(self, batch_rows: int) -> AsyncIterator[pa.RecordBatch]:
        schema = arrow_schema(self._columns)
        rows = await self._load()
        for start in range(0, len(rows), batch_rows):
            yield rows_to_batch(rows[start : start + batch_rows], self._columns, schema)
