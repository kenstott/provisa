# Copyright (c) 2026 Kenneth Stott
# Canary: a73fcc4a-895b-480c-bb26-bd4123d9c382
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: several replicas built from ONE read of their source. Each table is streamed as
its batches arrive; nothing is swapped until the read has ended, so a read that fails leaves
every previous copy; after it, each table, whole and hashed, is swapped or not on its own."""

import pyarrow as pa
import pytest

from provisa.core.ir_arrow import arrow_schema, rows_to_batch
from provisa.events.content_hash import RowSetHash
from provisa.federation.data_replicator import (
    BuildNote,
    EngineCaps,
    EngineRun,
    GroupPart,
    NoReplicationMethod,
    ReplicaGroupJob,
    SourceCaps,
    SourceRead,
    TargetCaps,
    TargetLoad,
    TargetWrite,
)

A = [("id", "bigint"), ("name", "text")]
B = [("id", "bigint"), ("of", "bigint")]


def _a(n, start=0):
    return [{"id": i, "name": f"n{i}"} for i in range(start, start + n)]


def _b(n, start=0):
    return [{"id": i, "of": i // 2} for i in range(start, start + n)]


def _batch(rows, columns) -> pa.RecordBatch:
    return rows_to_batch(rows, columns, arrow_schema(columns))


class _Target:
    caps = TargetCaps(frozenset({TargetWrite.BULK_BATCH}), True, TargetLoad.BULK_STREAM)

    def __init__(self, *, fail_swap: bool = False):
        self.events: list[str] = []
        self.rows: list[dict] = []
        self._fail_swap = fail_swap

    async def begin(self):
        self.events.append("begin")

    async def write(self, batch, rows):
        assert batch.num_rows == len(rows)
        self.rows += rows

    async def swap(self):
        if self._fail_swap:
            raise RuntimeError("the store refused the swap")
        self.events.append("swap")

    async def abort(self):
        self.events.append("abort")


class _Engine:
    def __init__(self, caps=EngineCaps(False, frozenset())):
        self.caps = caps
        self.calls: list[str] = []

    async def copy(self, prior_hash):
        raise AssertionError("a group build never asks an engine to copy")

    async def after_swap(self):
        self.calls.append("after_swap")


class _Read:
    """One read giving two tables' batches interleaved, as a mailbox read gives them."""

    caps = SourceCaps(frozenset({SourceRead.CURSOR}))

    def __init__(self, pages, *, fail_after: int | None = None, note: BuildNote | None = None):
        self._pages = pages
        self._fail_after = fail_after
        self._note = note
        self.reads = 0

    def notes(self):
        return [] if self._note is None else [self._note]

    async def batches(self, batch_rows):
        self.reads += 1
        for n, (table, rows, columns) in enumerate(self._pages):
            if self._fail_after is not None and n >= self._fail_after:
                raise RuntimeError("the source stopped answering")
            yield table, _batch(rows, columns)


def _pages():
    return [("a", _a(3), A), ("b", _b(4), B), ("a", _a(2, 3), A), ("extra", _a(1), A)]


def _job(read, **parts):
    return ReplicaGroupJob(read, parts, batch_rows=1000)


def _hash(rows) -> str:
    digest = RowSetHash()
    digest.update(rows)
    return digest.hexdigest()


async def _noop(_table, _rows):
    return None


async def test_one_read_gives_each_table_exactly_its_rows_and_swaps_each():
    ta, tb, ea, eb, seen = _Target(), _Target(), _Engine(), _Engine(), []

    async def progress(table, rows):
        seen.append((table, rows))

    read = _Read(_pages(), note=BuildNote("x.note", {"count": 1}))
    results = await _job(read, a=GroupPart(ta, ea), b=GroupPart(tb, eb)).run(progress)

    assert read.reads == 1
    assert ta.rows == _a(5) and tb.rows == _b(4)  # the table nobody asked for is dropped
    assert ta.events == ["begin", "swap"] and tb.events == ["begin", "swap"]
    assert ea.calls == ["after_swap"] and eb.calls == ["after_swap"]
    assert (results["a"].rows_copied, results["b"].rows_copied) == (5, 4)
    assert results["a"].method == "stream_batches" and results["a"].changed
    assert results["a"].content_hash == _hash(_a(5)) != results["b"].content_hash
    assert (
        [n.code for n in results["a"].notes] == [n.code for n in results["b"].notes] == ["x.note"]
    )
    assert seen == [("a", 3), ("b", 4), ("a", 5)]


async def test_a_table_whose_content_is_unchanged_is_discarded_and_the_other_is_swapped():
    ta, tb = _Target(), _Target()
    results = await _job(
        _Read(_pages()),
        a=GroupPart(ta, _Engine(), prior_hash=_hash(_a(5))),
        b=GroupPart(tb, _Engine(), prior_hash="something else"),
    ).run(_noop)
    assert ta.events == ["begin", "abort"] and results["a"].changed is False
    assert tb.events == ["begin", "swap"] and results["b"].changed is True


async def test_a_read_that_fails_swaps_nothing_and_removes_every_build_table():
    ta, tb = _Target(), _Target()
    job = _job(
        _Read(_pages(), fail_after=2), a=GroupPart(ta, _Engine()), b=GroupPart(tb, _Engine())
    )
    with pytest.raises(RuntimeError, match="stopped answering"):
        await job.run(_noop)
    assert ta.events == ["begin", "abort"] and tb.events == ["begin", "abort"]


async def test_a_table_no_longer_declared_is_aborted_alone():
    ta, tb = _Target(), _Target()

    async def gone():
        raise LookupError("table a is no longer declared")

    results = await _job(
        _Read(_pages()), a=GroupPart(ta, _Engine(), still_wanted=gone), b=GroupPart(tb, _Engine())
    ).run(_noop)
    assert isinstance(results["a"], LookupError) and ta.events == ["begin", "abort"]
    assert tb.events == ["begin", "swap"] and results["b"].rows_copied == 4


async def test_a_swap_that_fails_fails_its_table_and_the_others_already_whole_are_swapped():
    ta, tb = _Target(fail_swap=True), _Target()
    results = await _job(
        _Read(_pages()), a=GroupPart(ta, _Engine()), b=GroupPart(tb, _Engine())
    ).run(_noop)
    assert isinstance(results["a"], RuntimeError) and ta.events == ["begin", "abort"]
    assert tb.events == ["begin", "swap"]


async def test_a_target_that_cannot_open_aborts_the_ones_already_open():
    class _Closed(_Target):
        async def begin(self):
            raise RuntimeError("no room")

    ta, tb = _Target(), _Closed()
    with pytest.raises(RuntimeError, match="no room"):
        await _job(_Read(_pages()), a=GroupPart(ta, _Engine()), b=GroupPart(tb, _Engine())).run(
            _noop
        )
    assert ta.events == ["begin", "abort"] and tb.events == []


async def test_a_table_its_engine_would_copy_itself_is_not_a_group_table():
    read = _Read(_pages())
    read.caps = SourceCaps(frozenset({SourceRead.CURSOR, SourceRead.ENGINE_REACHABLE}))
    engine = _Engine(EngineCaps(True, frozenset({EngineRun.STATEMENT})))
    target = _Target()
    target.caps = TargetCaps(
        frozenset({TargetWrite.BULK_BATCH, TargetWrite.STATEMENT_COPY}),
        True,
        TargetLoad.BULK_STREAM,
    )
    with pytest.raises(NoReplicationMethod, match="group build streams its tables; a would"):
        ReplicaGroupJob(read, {"a": GroupPart(target, engine)}, batch_rows=10)


async def test_a_group_needs_a_table():
    with pytest.raises(ValueError, match="at least one table"):
        ReplicaGroupJob(_Read([]), {}, batch_rows=10)


async def test_a_build_table_that_cannot_be_removed_does_not_hide_why_the_read_failed():
    class _Stuck(_Target):
        async def abort(self):
            self.events.append("abort")
            raise RuntimeError("the build table could not be removed")

    ta, tb = _Stuck(), _Target()
    job = _job(
        _Read(_pages(), fail_after=2), a=GroupPart(ta, _Engine()), b=GroupPart(tb, _Engine())
    )
    with pytest.raises(RuntimeError, match="stopped answering"):
        await job.run(_noop)
    assert ta.events == ["begin", "abort"] and tb.events == ["begin", "abort"]  # both tried
