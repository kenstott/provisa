# Copyright (c) 2026 Kenneth Stott
# Canary: b5aefb44-636f-4c16-a3ac-1c884497384f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The streaming result contract (REQ-028): QueryResult (materialized) and StreamingQueryResult."""

from __future__ import annotations

import pytest

from provisa.executor.result import (
    QueryResult,
    ResultStream,
    StreamingQueryResult,
    StreamStats,
)


def _batch_source(calls: list[int]):
    """A generator that records how many batches were pulled, to prove laziness."""
    for i, chunk in enumerate([[(1,), (2,)], [(3,)], [(4,), (5,)]]):
        calls.append(i)
        yield chunk


# ── QueryResult (materialized) ──────────────────────────────────────────────────


def test_queryresult_iter_rows_and_default_batch():
    r = QueryResult(rows=[(1,), (2,), (3,)], column_names=["a"])
    assert list(r.iter_rows()) == [(1,), (2,), (3,)]
    assert list(r.batches()) == [[(1,), (2,), (3,)]]


def test_queryresult_chunked_batches():
    r = QueryResult(rows=[(1,), (2,), (3,), (4,), (5,)], column_names=["a"])
    assert list(r.batches(size=2)) == [[(1,), (2,)], [(3,), (4,)], [(5,)]]


def test_queryresult_empty_yields_no_batch():
    r = QueryResult(rows=[], column_names=["a"])
    assert list(r.batches()) == []
    assert list(r.iter_rows()) == []


def test_queryresult_materialize_is_self():
    r = QueryResult(rows=[(1,)], column_names=["a"])
    assert r.materialize() is r


def test_queryresult_satisfies_protocol():
    r = QueryResult(rows=[(1,)], column_names=["a"])
    assert isinstance(r, ResultStream)


# ── StreamingQueryResult (lazy) ─────────────────────────────────────────────────


def test_streaming_yields_batches_in_order():
    r = StreamingQueryResult(iter([[(1,), (2,)], [(3,)]]), column_names=["a"], column_types=["INT"])
    assert list(r.batches()) == [[(1,), (2,)], [(3,)]]
    assert r.column_names == ["a"]
    assert r.column_types == ["INT"]


def test_streaming_iter_rows_flattens():
    r = StreamingQueryResult(iter([[(1,), (2,)], [(3,)]]), column_names=["a"])
    assert list(r.iter_rows()) == [(1,), (2,), (3,)]


def test_streaming_is_lazy():
    """Batches are pulled from the source only as consumed — not eagerly at construction."""
    calls: list[int] = []
    r = StreamingQueryResult(_batch_source(calls), column_names=["a"])
    assert calls == []  # nothing pulled yet
    it = r.iter_rows()
    assert next(it) == (1,)
    assert calls == [0]  # only the first batch was pulled
    assert next(it) == (2,)
    assert calls == [0]  # still inside the first batch


def test_streaming_rows_materializes():
    r = StreamingQueryResult(iter([[(1,), (2,)], [(3,)]]), column_names=["a"])
    assert r.rows == [(1,), (2,), (3,)]


def test_streaming_rows_is_idempotent_after_materialize():
    r = StreamingQueryResult(iter([[(1,)], [(2,)]]), column_names=["a"])
    assert r.rows == [(1,), (2,)]
    assert r.rows == [(1,), (2,)]  # cached, does not re-consume
    assert list(r.batches()) == [[(1,), (2,)]]  # replays from the materialized cache


def test_streaming_double_batches_raises():
    r = StreamingQueryResult(iter([[(1,)]]), column_names=["a"])
    list(r.batches())
    with pytest.raises(RuntimeError, match="already consumed"):
        list(r.batches())


def test_streaming_rows_after_stream_raises():
    r = StreamingQueryResult(iter([[(1,)]]), column_names=["a"])
    list(r.batches())
    with pytest.raises(RuntimeError, match="already consumed"):
        _ = r.rows


def test_streaming_materialize_returns_queryresult():
    r = StreamingQueryResult(iter([[(1,)], [(2,)]]), column_names=["a"], column_types=["INT"])
    q = r.materialize()
    assert isinstance(q, QueryResult)
    assert q.rows == [(1,), (2,)]
    assert q.column_names == ["a"]
    assert q.column_types == ["INT"]


def test_streaming_empty_source():
    assert list(StreamingQueryResult(iter([]), column_names=["a"]).batches()) == []
    assert StreamingQueryResult(iter([]), column_names=["a"]).rows == []


def test_streaming_satisfies_protocol():
    r = StreamingQueryResult(iter([[(1,)]]), column_names=["a"])
    assert isinstance(r, ResultStream)


# ── stats ───────────────────────────────────────────────────────────────────────


def test_queryresult_stats_complete_on_construction():
    r = QueryResult(rows=[(1,), (2,), (3,)], column_names=["a"])
    assert r.stats == StreamStats(row_count=3, done=True)


def test_streaming_stats_tally_and_finalize():
    r = StreamingQueryResult(iter([[(1,), (2,)], [(3,)]]), column_names=["a"])
    it = r.batches()
    assert r.stats.row_count == 0 and not r.stats.done
    next(it)
    assert r.stats.row_count == 2 and not r.stats.done  # after first batch
    next(it)
    assert r.stats.row_count == 3
    with pytest.raises(StopIteration):
        next(it)
    assert r.stats.row_count == 3 and r.stats.done  # finalized on exhaustion


def test_streaming_on_close_fires_once_at_end():
    seen: list[StreamStats] = []
    r = StreamingQueryResult(
        iter([[(1,)], [(2,)]]), column_names=["a"], on_close=seen.append
    )
    assert seen == []  # not fired before drain
    r.rows  # drains
    assert len(seen) == 1
    assert seen[0].row_count == 2 and seen[0].done
    assert seen[0] is r.stats


def test_streaming_byte_count_defaults_none():
    r = StreamingQueryResult(iter([[(1,)]]), column_names=["a"])
    r.rows
    assert r.stats.byte_count is None


def test_materialize_preserves_stats():
    r = StreamingQueryResult(iter([[(1,)], [(2,)]]), column_names=["a"])
    q = r.materialize()
    assert q.stats is r.stats
    assert q.stats.row_count == 2 and q.stats.done
