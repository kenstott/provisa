# Copyright (c) 2026 Kenneth Stott
# Canary: 4f1bd89e-397e-4474-b1e3-625322d98889
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The DuckDB sync ENGINE terminal streams rows lazily over a private cursor (REQ-028)."""

from __future__ import annotations

import pytest

from provisa.executor.result import QueryResult, StreamingQueryResult
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.federation.runtime_support import (
    _ARROW_STREAM_BATCH_ROWS,
    _STREAM_BATCH_ROWS,
    stream_from_dbapi,
)


def test_stream_batch_rows_matches_arrow_batch_rows():
    # REQ-1893: row-cursor batches now match the Arrow-native batch size (65,536) — a 65x cut in
    # per-batch thread-pool/run_coroutine_threadsafe hops for gRPC's ENGINE-route stream and the
    # DIRECT-route stream shared by pgwire/Flight SQL, with no memory-bound guarantee depending on
    # the old 1000 value (see docs/arch/requirements.yaml REQ-1893).
    assert _STREAM_BATCH_ROWS == _ARROW_STREAM_BATCH_ROWS == 65_536


def test_stream_from_dbapi_calls_fetchmany_with_stream_batch_rows():
    class _FakeCursor:
        description = [("x",)]
        calls: list[int] = []

        def fetchmany(self, size):
            self.calls.append(size)
            return []

    cur = _FakeCursor()
    res = stream_from_dbapi(cur)
    list(res.batches())
    assert cur.calls == [_STREAM_BATCH_ROWS]


@pytest.fixture
def runtime():
    rt = DuckDBFederationRuntime()
    yield rt
    rt.close()


def test_run_sync_returns_streaming_result(runtime):
    res = runtime.run_sync("select * from range(3) t(x)")
    assert isinstance(res, StreamingQueryResult)
    assert res.column_names == ["x"]
    assert res.rows == [(0,), (1,), (2,)]


def test_run_sync_ddl_yields_empty(runtime):
    # DuckDB DDL reports a 'Count' column with zero rows → an empty stream that drains cleanly.
    res = runtime.run_sync("create table t(x int)")
    assert list(res.batches()) == []
    assert res.stats.done


def test_run_sync_streams_lazily_not_fully_materialized(runtime):
    # More rows than one fetch batch: draining the first batch must not pull them all.
    n = _STREAM_BATCH_ROWS * 2 + 5
    res = runtime.run_sync(f"select * from range({n}) t(x)")
    assert isinstance(res, StreamingQueryResult)
    it = res.batches()
    first = next(it)
    assert len(first) == _STREAM_BATCH_ROWS
    assert res.stats.row_count == _STREAM_BATCH_ROWS  # only the first batch tallied
    assert not res.stats.done
    total = len(first) + sum(len(b) for b in it)
    assert total == n
    assert res.stats.row_count == n and res.stats.done


def test_run_sync_closes_private_cursor_on_drain(runtime):
    # After the stream drains, its private cursor is closed; the shared connection stays usable.
    res = runtime.run_sync("select * from range(2) t(x)")
    assert res.rows == [(0,), (1,)]
    # A subsequent query on the same runtime still works (private cursors are independent).
    again = runtime.run_sync("select 42 as y")
    assert again.rows == [(42,)]


def test_stream_from_dbapi_fires_on_close_once_on_non_select():
    class _FakeCursor:
        description = None
        closed = 0

        def close(self):
            self.closed += 1

    cur = _FakeCursor()
    res = stream_from_dbapi(cur, on_close=lambda *_: cur.close())
    assert isinstance(res, QueryResult)
    assert res.rows == []
    assert cur.closed == 1


def test_run_sync_reads_an_interval_with_its_months(runtime):
    """DuckDB's own rows fold an interval's months into days (1 year 2 months reads as 420 days);
    a result with an interval column streams through Arrow, which keeps them."""
    from datetime import timedelta

    from dateutil.relativedelta import relativedelta

    res = runtime.run_sync(
        "select interval '1 year 2 months' as ym, interval '3 days 04:05:06' as ds, 1 as n"
    )
    assert res.column_names == ["ym", "ds", "n"]
    assert res.column_types == ["INTERVAL", "INTERVAL", "INTEGER"]
    assert res.rows == [(relativedelta(years=1, months=2), timedelta(days=3, seconds=14706), 1)]
    assert res.stats.done


def test_run_sync_reads_an_infinite_date_or_timestamp_as_infinite(runtime):
    """REQ-2167: DuckDB's own rows read infinity as the last date Python holds, a value a table may
    hold itself; a result with a date or timestamp column streams through Arrow, which keeps it."""
    from provisa.core.ir_types import infinity_of

    res = runtime.run_sync(
        "select 'infinity'::date as d, '-infinity'::timestamp as t, date '9999-12-31' as m"
    )
    [row] = res.rows
    assert [infinity_of(v) for v in row] == [1, -1, 0]
