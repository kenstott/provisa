# Copyright (c) 2026 Kenneth Stott
# Canary: bd260450-4eeb-4c52-9234-1cdd47df85cd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The data replicator: how a whole-table copy of a source table is made (REQ-1915).

Every whole-table copy of a source table into an engine's store is produced by this one
component. It is given what the three parties declare they can do:

- the SOURCE: be read by the engine in place, yield Arrow record batches, be read through a
  cursor, or return one document;
- the TARGET store: take a statement-level copy run by the engine, take a stream of batches
  through one held COPY, or take a batch through its bulk write; and swap a finished table in
  atomically;
- the ENGINE: reach this source, and run a statement-level copy;

and it answers with the method for that combination. The answer is a pure function of the
declarations (:func:`choose_method`), so the whole table of combinations is checked without
running a source, a store or an engine, and the same table says how a replica of a given source
on a given engine is built. A combination no method serves is refused, naming what is missing.

The engine performs the copy wherever it can reach the source: the copy is then a statement on
the engine and no row passes through a Provisa process. Otherwise Provisa extracts the source as
a stream of bounded Arrow record batches into the store's own bulk write. Either way the copy
fills a fresh table and swaps it in when complete, so a reader never sees an empty or
half-filled replica and a copy that dies leaves the previous replica intact.

What scales with what: for a streamed copy the rows pass through the node that runs the build,
so more building nodes are more capacity; for an engine statement the node only submits and
follows the statement, and the capacity is the engine's.

Row-level replication does not use this component: a table replicated row by row is filled by
the requests that ask for its rows.
"""

# Requirements: REQ-1915

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import pyarrow as pa


class SourceRead(str, Enum):
    """A way a source table's rows can be read."""

    ENGINE_REACHABLE = "engine_reachable"  # the engine reads the table in place
    ARROW_STREAM = "arrow_stream"  # the driver yields Arrow record batches
    CURSOR = "cursor"  # the driver yields bounded batches of rows from a cursor
    SINGLE_DOCUMENT = "single_document"  # the source answers with one document, held whole
    # one document, written to a file on this node as it arrives and parsed from it in batches
    SINGLE_DOCUMENT_SPOOLED = "single_document_spooled"


class TargetWrite(str, Enum):
    """A way a store takes a replica's rows."""

    STATEMENT_COPY = "statement_copy"  # the engine writes the table from its own SELECT
    COPY_STREAM = "copy_stream"  # one COPY held open for the build, written batch by batch
    BULK_BATCH = "bulk_batch"  # the store's bulk write, called once per batch


class TargetLoad(str, Enum):
    """How a store's write face takes a streamed build's rows: the one name the admin store
    page and the documentation state for a store."""

    BULK_STREAM = (
        "bulk_stream"  # the store's bulk path: COPY, Arrow ingest, direct path, staged files
    )
    ROW_COPY = "row_copy"  # rows bound as statement parameters; workable within limits, not for very large tables


class EngineRun(str, Enum):
    """A way an engine runs a copy itself."""

    STATEMENT = "statement"  # a statement the building process submits and waits on


class Method(str, Enum):
    """How one replica is built."""

    ENGINE_STATEMENT = "engine_statement"  # the engine copies; no row passes through Provisa
    STREAM_BATCHES = "stream_batches"  # Provisa streams bounded batches into the store
    # The store loads the origin itself (a SingleStore PIPELINE, REQ-990); chosen by the write
    # face (REQ-848 PIPELINE_LAND), not by choose_method.
    STORE_PIPELINE = "store_pipeline"


_STREAMED_READS = frozenset(
    {
        SourceRead.ARROW_STREAM,
        SourceRead.CURSOR,
        SourceRead.SINGLE_DOCUMENT,
        SourceRead.SINGLE_DOCUMENT_SPOOLED,
    }
)
#: Reads the admin page and the documentation mark as not optimal: the source has no cursor
#: and produces its whole answer for every build, in memory or in a file on the node.
NOT_OPTIMAL_READS = frozenset({SourceRead.SINGLE_DOCUMENT, SourceRead.SINGLE_DOCUMENT_SPOOLED})
_STREAMED_WRITES = frozenset({TargetWrite.COPY_STREAM, TargetWrite.BULK_BATCH})


@dataclass(frozen=True)
class SourceCaps:
    """What a source declares: the ways its tables can be read."""

    reads: frozenset[SourceRead]


@dataclass(frozen=True)
class TargetCaps:
    """What a store declares: the ways it takes rows, whether a finished build can replace the
    current replica in a single atomic step, and how its write face loads a streamed build."""

    writes: frozenset[TargetWrite]
    atomic_swap: bool
    load: TargetLoad


@dataclass(frozen=True)
class EngineCaps:
    """What an engine declares for one source: whether it reaches it, and how it runs a copy."""

    reaches_source: bool
    runs: frozenset[EngineRun]


class NoReplicationMethod(Exception):
    """No method builds a replica for this combination of source, store and engine."""

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        super().__init__("no replication method for this combination: " + "; ".join(missing))


def choose_method(source: SourceCaps, target: TargetCaps, engine: EngineCaps) -> Method:
    """The method that builds a replica from ``source`` into ``target`` on ``engine``.

    Pure: it reads only the three declarations. In order of preference:

    1. :attr:`Method.ENGINE_STATEMENT` when the source is read by the engine in place, the
       engine reaches it and runs statements, and the store takes a statement-level copy.
    2. :attr:`Method.STREAM_BATCHES` when the source can be read as a stream (Arrow batches, a
       cursor, or one document) and the store takes batches.

    Every method ends by swapping the finished table in, so a store that cannot do that
    atomically serves no method. Raises :class:`NoReplicationMethod` naming everything the
    combination lacks."""
    engine_copies = (
        SourceRead.ENGINE_REACHABLE in source.reads
        and engine.reaches_source
        and EngineRun.STATEMENT in engine.runs
        and TargetWrite.STATEMENT_COPY in target.writes
    )
    streams = bool(source.reads & _STREAMED_READS) and bool(target.writes & _STREAMED_WRITES)
    if target.atomic_swap:
        if engine_copies:
            return Method.ENGINE_STATEMENT
        if streams:
            return Method.STREAM_BATCHES

    missing: list[str] = []
    if not target.atomic_swap:
        missing.append("the store cannot swap a finished table in atomically")
    if not engine_copies and not streams:
        if not source.reads & _STREAMED_READS:
            missing.append(
                "the source offers no stream (Arrow batches, a cursor, or a single document)"
            )
        if not target.writes & _STREAMED_WRITES:
            missing.append("the store takes no batches (a held COPY or a bulk write)")
        if not (SourceRead.ENGINE_REACHABLE in source.reads and engine.reaches_source):
            missing.append("the engine does not reach the source")
        elif EngineRun.STATEMENT not in engine.runs:
            missing.append("the engine runs no statement-level copy")
        elif TargetWrite.STATEMENT_COPY not in target.writes:
            missing.append("the store takes no statement-level copy from the engine")
    raise NoReplicationMethod(missing)


# -- the job -----------------------------------------------------------------------------------

#: The most Arrow data a streamed build writes, and holds as row objects, at once: a batch of
#: wide rows is written in slices of this size. Measured: 65,536 rows of a 26-column table held
#: as row objects were about 200 MB; 8 MiB of Arrow data is a few tens of MB as row objects.
BATCH_BYTES = 8 * 1024 * 1024

#: Called by a job as it copies: the rows copied so far.
Progress = Callable[[int], Awaitable[None]]


@dataclass(frozen=True)
class BuildNote:
    """Something a completed build has to say about the copy it made, as a code the UI words
    in its own language and the particulars that go with it (a count, some ids). A build that
    read every row as it expected to has none."""

    code: str
    params: dict


@dataclass(frozen=True)
class BuildOutcome:
    """What a finished build reports. ``changed`` is False when the copy's content hash equals
    the previous build's: the build table was discarded and the replica left as it was."""

    rows_copied: int
    method: str
    content_hash: str | None = None
    changed: bool = True
    #: What the replica was built from and the columns it has, when the builder tracks them
    #: (``replica_converge.definition_hash``); recorded with the build's completion.
    definition_hash: str | None = None
    built_columns: list | None = None
    #: What the source had to say of the rows it read, when it had anything (BuildNote).
    note: BuildNote | None = None


class _Source(Protocol):
    """A source may also carry ``note()``: called once its batches are read to the end, it
    answers a :class:`BuildNote` for the read just made, or None."""

    caps: SourceCaps

    def batches(self, batch_rows: int) -> AsyncIterator["pa.RecordBatch"]: ...


class _Target(Protocol):
    caps: TargetCaps

    async def begin(self) -> None: ...

    async def write(self, batch: "pa.RecordBatch", rows: list[dict]) -> None: ...

    async def swap(self) -> None: ...

    async def abort(self) -> None: ...


class _Engine(Protocol):
    caps: EngineCaps

    async def copy(self, prior_hash: str | None) -> BuildOutcome:
        """Run the copy as the engine's own statement, swap included. ``prior_hash`` is the
        content hash of the replica's last build; a copy whose own hash equals it is discarded
        and reported unchanged."""
        ...

    async def after_swap(self) -> None:
        """What the engine must do once a new replica stands in the store."""
        ...


class ReplicaJob:
    """The build of one replica by the method its parties allow. Run it once."""

    def __init__(
        self,
        method: Method,
        source: _Source,
        target: _Target,
        engine: _Engine,
        *,
        batch_rows: int,
        batch_bytes: int = BATCH_BYTES,
        prior_hash: str | None,
        still_wanted: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.method = method
        self._source = source
        self._target = target
        self._engine = engine
        self._batch_rows = batch_rows
        self._batch_bytes = batch_bytes
        self._prior_hash = prior_hash
        self._still_wanted = still_wanted

    async def run(self, progress: Progress) -> BuildOutcome:
        if self.method is Method.ENGINE_STATEMENT:
            outcome = await self._engine.copy(self._prior_hash)
            if outcome.changed:
                await self._engine.after_swap()
            return outcome
        return await self._stream(progress)

    async def _stream(self, progress: Progress) -> BuildOutcome:
        """Stream the source into the build table a bounded batch at a time, then swap it in.
        Only one batch is held at any moment, and a batch is bounded in rows AND in bytes: a
        batch of wide rows is written in slices, so what the build holds as row objects does
        not grow with the row width; the content hash is accumulated as the batches
        pass, and a copy whose hash equals the previous build's is discarded unswapped."""
        from provisa.events.content_hash import RowSetHash

        digest = RowSetHash()
        copied = 0
        swapped = False
        try:
            # Inside the try: a target that fails while opening is aborted like any other.
            await self._target.begin()
            async for read in self._source.batches(self._batch_rows):
                for batch in _within_bytes(read, self._batch_bytes):
                    rows = batch.to_pylist()
                    digest.update(rows)
                    await self._target.write(batch, rows)
                    copied += len(rows)
                    await progress(copied)
            content_hash = digest.hexdigest()
            # What the source says of this read holds whether or not the copy is swapped in.
            said = getattr(self._source, "note", None)
            note = said() if said is not None else None
            if content_hash == self._prior_hash:
                return BuildOutcome(
                    rows_copied=copied,
                    method=self.method.value,
                    content_hash=content_hash,
                    changed=False,
                    note=note,
                )
            if self._still_wanted is not None:
                # Raises when the model stopped declaring the table while it was copied: the
                # build table is removed (the finally below) and nothing is swapped.
                await self._still_wanted()
            await self._target.swap()
            swapped = True
        finally:
            if not swapped:
                await self._target.abort()
        await self._engine.after_swap()
        return BuildOutcome(
            rows_copied=copied, method=self.method.value, content_hash=content_hash, note=note
        )


def _within_bytes(batch: "pa.RecordBatch", max_bytes: int) -> Iterator["pa.RecordBatch"]:
    """``batch`` whole when it is within ``max_bytes`` of Arrow data, else in equal slices that
    each are (by the batch's average row size). A slice is never empty: one row wider than the
    bound is written alone."""
    size = batch.nbytes
    if size <= max_bytes or batch.num_rows <= 1:
        yield batch
        return
    rows = max(1, batch.num_rows * max_bytes // size)
    for start in range(0, batch.num_rows, rows):
        yield batch.slice(start, rows)


def data_replicator(
    source: Any,
    target: Any,
    engine: Any,
    *,
    batch_rows: int,
    batch_bytes: int = BATCH_BYTES,
    prior_hash: str | None = None,
    still_wanted: Callable[[], Awaitable[None]] | None = None,
) -> ReplicaJob:
    """The job that builds one replica from ``source`` into ``target`` on ``engine``.

    Each party carries its declared capabilities (``caps``) and the operations the methods use:
    the source its ``batches``, the target its ``begin / write / swap / abort``, the engine its
    ``copy`` and ``after_swap``. The method is :func:`choose_method` of the three declarations;
    a combination no method serves raises :class:`NoReplicationMethod`. ``prior_hash`` is the
    content hash of the replica's last build, when it has one. ``still_wanted`` is awaited
    just before a streamed build swaps and raises when the table is no longer declared."""
    method = choose_method(source.caps, target.caps, engine.caps)
    return ReplicaJob(
        method,
        source,
        target,
        engine,
        batch_rows=batch_rows,
        batch_bytes=batch_bytes,
        prior_hash=prior_hash,
        still_wanted=still_wanted,
    )
