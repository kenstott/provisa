# Copyright (c) 2026 Kenneth Stott
# Canary: 6372336e-1380-40f6-93e0-ef0065dca312
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a replica build streams bounded batches into a build table and swaps it in; the
memory it uses does not grow with the table."""

import tracemalloc

import pyarrow as pa
import pytest

from provisa.core.ir_arrow import arrow_schema, rows_to_batch
from provisa.events.content_hash import RowSetHash
from provisa.federation.data_replicator import (
    BuildOutcome,
    EngineCaps,
    EngineRun,
    Method,
    NoReplicationMethod,
    SourceCaps,
    SourceRead,
    TargetCaps,
    TargetLoad,
    TargetWrite,
    data_replicator,
)
from provisa.federation.replica_source import CursorSource, DocumentSource

COLUMNS = [("id", "bigint"), ("name", "text"), ("amount", "numeric")]


def _rows(n, start=0):
    return [{"id": i, "name": f"n{i}", "amount": f"{i}.50"} for i in range(start, start + n)]


class _Target:
    caps = TargetCaps(frozenset({TargetWrite.BULK_BATCH}), True, TargetLoad.BULK_STREAM)

    def __init__(self):
        self.events: list[str] = []
        self.batch_sizes: list[int] = []
        self.rows = 0

    async def begin(self):
        self.events.append("begin")

    async def write(self, batch, rows):
        assert batch.num_rows == len(rows)
        self.batch_sizes.append(len(rows))
        self.rows += len(rows)

    async def swap(self):
        self.events.append("swap")

    async def abort(self):
        self.events.append("abort")


class _Engine:
    def __init__(self, caps=EngineCaps(False, frozenset())):
        self.caps = caps
        self.calls: list[str] = []

    async def copy(self, prior_hash):
        self.calls.append("copy")
        changed = prior_hash != "pg:same"
        return BuildOutcome(42, "engine_statement", content_hash="pg:same", changed=changed)

    async def after_swap(self):
        self.calls.append("after_swap")


def _cursor(total, per_fetch):
    """A source that hands out ``total`` rows, ``per_fetch`` at a time, never holding more."""

    async def row_batches(_batch_rows):
        for start in range(0, total, per_fetch):
            yield _rows(min(per_fetch, total - start), start)

    return CursorSource(row_batches, COLUMNS)


async def _noop(_rows_copied):
    return None


async def test_a_streamed_build_writes_bounded_batches_then_swaps():
    target, engine, seen = _Target(), _Engine(), []

    async def progress(rows_copied):
        seen.append(rows_copied)

    job = data_replicator(_cursor(2_500, 700), target, engine, batch_rows=300)
    outcome = await job.run(progress)

    assert job.method is Method.STREAM_BATCHES
    assert max(target.batch_sizes) <= 300 and sum(target.batch_sizes) == 2_500
    assert target.events == ["begin", "swap"]
    assert engine.calls == ["after_swap"]
    assert (outcome.rows_copied, outcome.method, outcome.changed) == (2_500, "stream_batches", True)
    assert seen[-1] == 2_500 and seen == sorted(seen)


async def test_an_unchanged_copy_is_discarded_without_a_swap_or_an_engine_step():
    first = _Target()
    built = await data_replicator(_cursor(900, 300), first, _Engine(), batch_rows=300).run(_noop)

    async def reversed_batches(_batch_rows):  # the same rows, another order
        for start in (600, 300, 0):
            yield list(reversed(_rows(300, start)))

    target, engine = _Target(), _Engine()
    job = data_replicator(
        CursorSource(reversed_batches, COLUMNS),
        target,
        engine,
        batch_rows=300,
        prior_hash=built.content_hash,
    )
    outcome = await job.run(_noop)
    assert outcome.changed is False and outcome.content_hash == built.content_hash
    assert target.events == ["begin", "abort"] and engine.calls == []


async def test_a_source_that_fails_mid_copy_aborts_the_build_and_never_swaps():
    async def failing(_batch_rows):
        yield _rows(10)
        raise RuntimeError("source went away")

    target = _Target()
    job = data_replicator(CursorSource(failing, COLUMNS), target, _Engine(), batch_rows=5)
    with pytest.raises(RuntimeError, match="source went away"):
        await job.run(_noop)
    assert target.events == ["begin", "abort"]


async def test_a_target_that_fails_to_open_is_aborted_so_nothing_stays_held():
    class _WontOpen(_Target):
        async def begin(self):
            self.events.append("begin")
            raise RuntimeError("store refused")

    target = _WontOpen()
    with pytest.raises(RuntimeError, match="store refused"):
        await data_replicator(_cursor(10, 5), target, _Engine(), batch_rows=5).run(_noop)
    assert target.events == ["begin", "abort"]


async def test_where_the_engine_can_copy_no_row_passes_through_the_job():
    class _EngineReached(DocumentSource):
        caps = SourceCaps(frozenset({SourceRead.ENGINE_REACHABLE, SourceRead.ARROW_STREAM}))

    async def load():
        raise AssertionError("the engine copies; the source is not read here")

    target = _Target()
    target.caps = TargetCaps(
        frozenset({TargetWrite.STATEMENT_COPY, TargetWrite.BULK_BATCH}),
        True,
        TargetLoad.BULK_STREAM,
    )
    engine = _Engine(EngineCaps(True, frozenset({EngineRun.STATEMENT})))
    job = data_replicator(_EngineReached(load, COLUMNS), target, engine, batch_rows=100)
    outcome = await job.run(_noop)
    assert job.method is Method.ENGINE_STATEMENT
    assert (outcome.rows_copied, outcome.method) == (42, "engine_statement")
    assert engine.calls == ["copy", "after_swap"] and target.events == []

    # The engine's own hash gates the ripple: an unchanged copy runs no after-swap step.
    again = _Engine(EngineCaps(True, frozenset({EngineRun.STATEMENT})))
    unchanged = await data_replicator(
        _EngineReached(load, COLUMNS), target, again, batch_rows=100, prior_hash="pg:same"
    ).run(_noop)
    assert unchanged.changed is False and again.calls == ["copy"]


async def test_a_combination_no_method_serves_is_refused_before_anything_is_opened():
    target = _Target()
    target.caps = TargetCaps(frozenset(), True, TargetLoad.BULK_STREAM)
    with pytest.raises(NoReplicationMethod, match="the store takes no batches"):
        data_replicator(_cursor(1, 1), target, _Engine(), batch_rows=1)
    assert target.events == []


async def test_memory_does_not_grow_with_the_size_of_the_table():
    async def peak(total):
        tracemalloc.start()
        try:
            await data_replicator(
                _cursor(total, 2_000), _Target(), _Engine(), batch_rows=2_000
            ).run(_noop)
            return tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()

    small, large = await peak(20_000), await peak(200_000)
    assert large < small * 1.5, (small, large)  # ten times the rows, the same working memory


def test_the_streamed_content_hash_ignores_order_and_sees_every_change():
    rows = _rows(50)
    forward, backward, changed, fewer = RowSetHash(), RowSetHash(), RowSetHash(), RowSetHash()
    forward.update(rows[:20])
    forward.update(rows[20:])
    backward.update(list(reversed(rows)))
    changed.update(rows[:49] + [{**rows[49], "name": "other"}])
    fewer.update(rows[:49])
    assert forward.hexdigest() == backward.hexdigest()
    assert len({forward.hexdigest(), changed.hexdigest(), fewer.hexdigest()}) == 3
    assert RowSetHash().hexdigest() == "0:" + "0" * 64


def test_every_ir_type_has_an_arrow_type_and_booleans_and_intervals_travel():
    import datetime as dt

    from provisa.core.ir_arrow import arrow_type
    from provisa.core.ir_types import IR_TYPES

    for ir_type in IR_TYPES:
        assert isinstance(arrow_type(ir_type), pa.DataType), ir_type
    columns = [("open", "boolean"), ("wait", "interval")]
    batch = rows_to_batch(
        [{"open": True, "wait": dt.timedelta(days=1, seconds=5)}, {"open": None, "wait": "2 days"}],
        columns,
        arrow_schema(columns),
    )
    assert batch.to_pylist() == [
        {"open": True, "wait": "1 days 5 seconds 0 microseconds"},
        {"open": None, "wait": "2 days"},
    ]


def test_rows_become_a_batch_typed_by_the_declared_columns():
    columns = [("id", "bigint"), ("note", "text"), ("amount", "numeric"), ("doc", "json")]
    schema = arrow_schema(columns)
    batch = rows_to_batch(
        [{"id": 1, "amount": 12.5, "doc": {"a": 1}}, {"id": 2, "note": None}], columns, schema
    )
    assert batch.schema.types == [pa.int64(), pa.string(), pa.string(), pa.string()]
    assert batch.to_pylist() == [
        {"id": 1, "note": None, "amount": "12.5", "doc": '{"a": 1}'},
        {"id": 2, "note": None, "amount": None, "doc": None},
    ]


async def test_a_batch_of_wide_rows_is_written_in_slices_bounded_by_bytes():
    """Bounded in rows AND bytes: a batch within the row bound whose rows are wide is written
    in slices, so the row objects a build holds do not grow with the row width."""
    import pyarrow as pa

    from provisa.federation.data_replicator import _within_bytes

    wide = pa.RecordBatch.from_pylist([{"id": i, "blob": "x" * 1000} for i in range(1000)])
    slices = list(_within_bytes(wide, 100_000))
    assert len(slices) > 5 and all(s.nbytes <= 110_000 for s in slices)
    assert sum(s.num_rows for s in slices) == 1000
    assert [s.column(0)[0].as_py() for s in slices] == sorted(
        s.column(0)[0].as_py() for s in slices
    )
    assert list(_within_bytes(wide, wide.nbytes)) == [wide]  # within the bound: untouched
    one = wide.slice(0, 1)
    assert list(_within_bytes(one, 10)) == [one]  # a row wider than the bound is written alone

    target = _Target()
    job = data_replicator(
        _cursor(1_000, 1_000), target, _Engine(), batch_rows=1_000, batch_bytes=2_000
    )
    outcome = await job.run(_noop)
    assert outcome.rows_copied == 1_000
    assert len(target.batch_sizes) > 1 and max(target.batch_sizes) < 1_000
