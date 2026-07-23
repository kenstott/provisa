# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The DuckDB sync ENGINE terminal streams rows lazily over a private cursor (REQ-028)."""

from __future__ import annotations

import pytest

from provisa.executor.result import QueryResult, StreamingQueryResult
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.federation.runtime_support import _STREAM_BATCH_ROWS, stream_from_dbapi


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
