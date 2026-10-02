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

from dataclasses import dataclass
from enum import Enum


class SourceRead(str, Enum):
    """A way a source table's rows can be read."""

    ENGINE_REACHABLE = "engine_reachable"  # the engine reads the table in place
    ARROW_STREAM = "arrow_stream"  # the driver yields Arrow record batches
    CURSOR = "cursor"  # the driver yields bounded batches of rows from a cursor
    SINGLE_DOCUMENT = "single_document"  # the source answers with one document, held whole


class TargetWrite(str, Enum):
    """A way a store takes a replica's rows."""

    STATEMENT_COPY = "statement_copy"  # the engine writes the table from its own SELECT
    COPY_STREAM = "copy_stream"  # one COPY held open for the build, written batch by batch
    BULK_BATCH = "bulk_batch"  # the store's bulk write, called once per batch


class EngineRun(str, Enum):
    """A way an engine runs a copy itself."""

    STATEMENT = "statement"  # a statement the building process submits and waits on


class Method(str, Enum):
    """How one replica is built."""

    ENGINE_STATEMENT = "engine_statement"  # the engine copies; no row passes through Provisa
    STREAM_BATCHES = "stream_batches"  # Provisa streams bounded batches into the store


_STREAMED_READS = frozenset(
    {SourceRead.ARROW_STREAM, SourceRead.CURSOR, SourceRead.SINGLE_DOCUMENT}
)
_STREAMED_WRITES = frozenset({TargetWrite.COPY_STREAM, TargetWrite.BULK_BATCH})


@dataclass(frozen=True)
class SourceCaps:
    """What a source declares: the ways its tables can be read."""

    reads: frozenset[SourceRead]


@dataclass(frozen=True)
class TargetCaps:
    """What a store declares: the ways it takes rows, and whether a finished table can replace
    the current one in a single atomic step."""

    writes: frozenset[TargetWrite]
    atomic_swap: bool


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
