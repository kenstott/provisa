# Copyright (c) 2026 Kenneth Stott
# Canary: 9a41d7c3-2e5f-4b80-a6d1-7c3e0f2b9d14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The Arrow results (REQ-2194): an engine hands its Arrow batches on as they are, and a consumer
that still takes Python rows has them built as it reads them, never at the fetch."""

from __future__ import annotations

import pyarrow as pa
import pytest

from provisa.executor.result import (
    ArrowResult,
    ArrowStreamResult,
    QueryResult,
    ResultStream,
    StreamingQueryResult,
)

_SCHEMA = pa.schema([("n", pa.int64()), ("s", pa.string())])


def _batch(start: int, count: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array(range(start, start + count)), pa.array([str(i) for i in range(count)])],
        schema=_SCHEMA,
    )


def _plain_rows(data) -> list[tuple]:
    return list(zip(*(c.to_pylist() for c in data.columns)))


class _CountingRows:
    """A ``to_rows`` that records each batch it was asked to convert."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, data) -> list[tuple]:
        self.calls += 1
        return _plain_rows(data)


def _source(batches: list[pa.RecordBatch], pulled: list[int], released: list[str]):
    try:
        for i, b in enumerate(batches):
            pulled.append(i)
            yield b
    finally:
        released.append("source")


# ── ArrowStreamResult ────────────────────────────────────────────────────────────


def test_a_stream_hands_on_the_engines_batches_without_building_rows():
    to_rows = _CountingRows()
    batches = [_batch(0, 2), _batch(2, 3)]
    res = ArrowStreamResult(_SCHEMA, iter(batches), ["BIGINT", "VARCHAR"], to_rows=to_rows)
    got = list(res.arrow_batches())
    assert got == batches
    assert all(a is b for a, b in zip(got, batches))  # the engine's own buffers
    assert to_rows.calls == 0
    assert res.stats.row_count == 5 and res.stats.done
    assert res.stats.byte_count == sum(b.nbytes for b in batches)


def test_a_stream_builds_rows_only_as_the_consumer_pulls_them():
    to_rows = _CountingRows()
    pulled: list[int] = []
    res = ArrowStreamResult(
        _SCHEMA, _source([_batch(0, 2), _batch(2, 1)], pulled, []), to_rows=to_rows
    )
    assert res.column_names == ["n", "s"]
    assert pulled == [] and to_rows.calls == 0
    it = res.batches()
    assert next(it) == [(0, "0"), (1, "1")]
    assert pulled == [0] and to_rows.calls == 1
    assert next(it) == [(2, "0")]
    assert to_rows.calls == 2


def test_a_stream_skips_empty_batches_on_its_row_face():
    res = ArrowStreamResult(_SCHEMA, iter([_batch(0, 0), _batch(0, 1)]), to_rows=_plain_rows)
    assert list(res.batches()) == [[(0, "0")]]


def test_a_streams_rows_are_its_row_face_materialized():
    res = ArrowStreamResult(_SCHEMA, iter([_batch(0, 2), _batch(2, 1)]), to_rows=_plain_rows)
    assert res.rows == [(0, "0"), (1, "1"), (2, "0")]
    assert res.rows == [(0, "0"), (1, "1"), (2, "0")]  # idempotent once materialized
    assert list(res.iter_rows()) == res.rows
    q = res.materialize()
    assert isinstance(q, QueryResult) and q.rows == res.rows


def test_a_stream_is_read_once_whichever_face_reads_it():
    res = ArrowStreamResult(_SCHEMA, iter([_batch(0, 1)]), to_rows=_plain_rows)
    list(res.arrow_batches())
    with pytest.raises(RuntimeError, match="already consumed"):
        list(res.batches())


def test_a_stream_read_as_rows_has_no_arrow_face_left():
    res = ArrowStreamResult(_SCHEMA, iter([_batch(0, 1)]), to_rows=_plain_rows)
    assert res.rows == [(0, "0")]
    with pytest.raises(RuntimeError, match="already read as rows"):
        list(res.arrow_batches())


@pytest.mark.parametrize("face", ["rows", "arrow"])
def test_a_stream_its_consumer_stops_early_releases_its_source(face):
    released: list[str] = []
    res = ArrowStreamResult(
        _SCHEMA,
        _source([_batch(i, 1) for i in range(5)], [], released),
        to_rows=_plain_rows,
        on_release=lambda: released.append("release"),
    )
    it = res.iter_rows() if face == "rows" else res.arrow_batches()
    next(it)
    it.close()
    assert released == ["source", "release"]
    assert res.stats.done


def test_a_batch_that_fails_to_convert_releases_its_source():
    released: list[str] = []

    def _broken(data) -> list[tuple]:
        raise ValueError("no row for this value")

    res = ArrowStreamResult(
        _SCHEMA,
        _source([_batch(0, 1), _batch(1, 1)], [], released),
        to_rows=_broken,
        on_release=lambda: released.append("release"),
    )
    with pytest.raises(ValueError, match="no row for this value"):
        list(res.batches())
    assert released == ["source", "release"]


def test_a_stream_is_a_streaming_result_stream():
    res = ArrowStreamResult(_SCHEMA, iter(()), to_rows=_plain_rows)
    assert isinstance(res, StreamingQueryResult)
    assert isinstance(res, ResultStream)


def test_a_streams_on_close_reports_its_rows_and_bytes():
    seen = []
    b = _batch(0, 3)
    res = ArrowStreamResult(_SCHEMA, iter([b]), to_rows=_plain_rows, on_close=seen.append)
    list(res.batches())
    [stats] = seen
    assert stats.row_count == 3 and stats.byte_count == b.nbytes and stats.done


# ── ArrowResult ────────────────────────────────────────────────────────────────


def test_a_materialized_result_keeps_the_engines_table_and_builds_rows_when_read():
    table = pa.Table.from_batches([_batch(0, 2), _batch(2, 1)])
    to_rows = _CountingRows()
    res = ArrowResult(table, ["BIGINT", "VARCHAR"], to_rows=to_rows)
    assert isinstance(res, QueryResult)
    assert res.column_names == ["n", "s"]
    assert res.column_types == ["BIGINT", "VARCHAR"]
    assert res.stats.row_count == 3 and res.stats.done
    assert res.stats.byte_count == table.nbytes
    assert to_rows.calls == 0
    assert pa.Table.from_batches(list(res.arrow_batches())) == table
    assert to_rows.calls == 0
    assert res.rows == [(0, "0"), (1, "1"), (2, "0")]
    assert list(res.iter_rows()) == res.rows
    assert to_rows.calls == 1  # built once, then held


def test_a_materialized_result_answers_more_than_once():
    res = ArrowResult(pa.Table.from_batches([_batch(0, 2)]), to_rows=_plain_rows)
    assert list(res.batches()) == [[(0, "0"), (1, "1")]]
    assert list(res.batches()) == [[(0, "0"), (1, "1")]]
    assert len(list(res.arrow_batches())) == 1
    assert len(list(res.arrow_batches())) == 1


def test_replacing_a_materialized_results_rows_drops_its_table():
    res = ArrowResult(pa.Table.from_batches([_batch(0, 2)]), to_rows=_plain_rows)
    res.rows = [(9, "x")]
    assert res.rows == [(9, "x")]
    with pytest.raises(RuntimeError, match="rows were replaced"):
        res.arrow_batches()


def test_an_empty_materialized_result_has_its_columns_and_no_batches_of_rows():
    res = ArrowResult(_SCHEMA.empty_table(), to_rows=_plain_rows)
    assert res.column_names == ["n", "s"]
    assert res.rows == []
    assert list(res.batches()) == []
