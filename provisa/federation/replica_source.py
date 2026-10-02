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
- :class:`CursorSource` — an adapter that yields bounded batches of rows;
- :class:`DocumentSource` — an adapter whose answer is one document, held whole and then cut
  into batches. It is declared as what it is: the memory it needs is the document's size.

Rows from a cursor or a document become batches through one helper, typed by the table's
declared columns.
"""

# Requirements: REQ-1915

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Protocol

import pyarrow as pa

from provisa.core.ir_arrow import arrow_schema, rows_to_batch
from provisa.federation.data_replicator import SourceCaps, SourceRead

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
        _schema, stream = self._engine.execute_engine_stream(f"SELECT * FROM {self._ref}")
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
    ) -> None:
        self._row_batches = row_batches
        self._columns = columns

    async def batches(self, batch_rows: int) -> AsyncIterator[pa.RecordBatch]:
        schema = arrow_schema(self._columns)
        async for rows in self._row_batches(batch_rows):
            for start in range(0, len(rows), batch_rows):
                yield rows_to_batch(rows[start : start + batch_rows], self._columns, schema)


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
