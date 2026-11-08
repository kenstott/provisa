# Copyright (c) 2026 Kenneth Stott
# Canary: 4f1bd89e-397e-4474-b1e3-625322d98889
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The DuckDB sync ENGINE terminal streams rows lazily over a private cursor (REQ-028)."""

from __future__ import annotations

import duckdb
import pytest

from provisa.core.pg_error import PostgresError
from provisa.executor.result import ArrowStreamResult, QueryResult, StreamingQueryResult
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


def test_run_sync_hands_on_duckdbs_arrow_batches(runtime):
    """REQ-2194: the sync terminal's result is DuckDB's Arrow reader, never its DB-API rows; an
    encoder reads the record batches as DuckDB produced them."""
    import pyarrow as pa

    from provisa.executor.result import ArrowStreamResult

    n = _ARROW_STREAM_BATCH_ROWS + 3
    res = runtime.run_sync(f"select x, x::varchar as s from range({n}) t(x)")
    assert isinstance(res, ArrowStreamResult)
    assert res.column_names == ["x", "s"]
    assert res.column_types == ["BIGINT", "VARCHAR"]
    batches = list(res.arrow_batches())
    assert all(isinstance(b, pa.RecordBatch) for b in batches)
    assert sum(b.num_rows for b in batches) == n
    assert res.stats.row_count == n and res.stats.done
    assert res.stats.byte_count == sum(b.nbytes for b in batches)


def test_run_sync_arrow_reader_keeps_a_zero_row_results_columns(runtime):
    res = runtime.run_sync("select 1::integer as a, 'x' as b where false")
    assert res.column_names == ["a", "b"]
    assert res.column_types == ["INTEGER", "VARCHAR"]
    assert res.rows == []


def test_run_sync_reads_values_duckdbs_rows_hold(runtime):
    """A row consumer is given the values DuckDB's own rows hold (duckdb_arrow.arrow_rows): a
    UUID is a uuid.UUID, a MAP a dict, a HUGEINT an int, a TIMETZ keeps its offset."""
    import uuid
    from datetime import time, timedelta, timezone

    res = runtime.run_sync(
        "select '7d9f1c2e-1111-2222-3333-444455556666'::uuid as u, map {'k': 1} as m, "
        "170141183460469231731687303715884105727::hugeint as h, '01:02:03+05'::timetz as t"
    )
    assert res.rows == [
        (
            uuid.UUID("7d9f1c2e-1111-2222-3333-444455556666"),
            {"k": 1},
            2**127 - 1,
            time(1, 2, 3, tzinfo=timezone(timedelta(hours=5))),
        )
    ]


def test_run_sync_keeps_duckdbs_default_export_for_the_flight_transport(runtime):
    """The lossless export is the row terminal's cursor's own: the Arrow transport's reads keep
    DuckDB's default export (a HUGEINT is a decimal128 there, not DuckDB's opaque bytes)."""
    import pyarrow as pa

    runtime.run_sync("select 1::hugeint as h").rows
    schema, batches = runtime.run_arrow_stream("select 1::hugeint as h")
    list(batches)
    assert schema.field("h").type == pa.decimal128(38, 0)


async def test_run_returns_the_engines_arrow_table(runtime):
    """REQ-2194: the async terminal's materialized result is DuckDB's Arrow table; rows are built
    from it only when a consumer reads them."""
    import pyarrow as pa

    from provisa.executor.result import ArrowResult

    res = await runtime.run("select x, x * 2 as y from range(4) t(x)")
    assert isinstance(res, ArrowResult)
    assert isinstance(res.table, pa.Table)
    assert res.column_names == ["x", "y"]
    assert res.column_types == ["BIGINT", "BIGINT"]
    assert res.stats.row_count == 4
    assert res.rows == [(0, 0), (1, 2), (2, 4), (3, 6)]


async def test_run_of_a_statement_that_returns_no_rows_has_none(runtime):
    # DuckDB reports a SET as one 'Success' column with no rows, as its cursor always did.
    res = await runtime.run("set threads = 2")
    assert res.rows == [] and res.column_names == ["Success"]


async def test_run_builds_its_rows_off_the_event_loop(runtime, monkeypatch):
    """Every request runs on its own thread, and its event loop does no per-row work: the async
    terminal's rows are built on the executor thread before the result is handed back, and a
    consumer reading them on the loop builds nothing."""
    import threading

    from provisa.executor.drivers import duckdb_arrow
    from provisa.federation import duckdb_runtime

    built_on: list[int] = []

    def recording(data):
        built_on.append(threading.get_ident())
        return duckdb_arrow.arrow_rows(data)

    monkeypatch.setattr(duckdb_runtime, "arrow_rows", recording)
    loop_thread = threading.get_ident()
    res = await runtime.run("select x, x::varchar as s from range(1000) t(x)")
    assert built_on and loop_thread not in built_on
    built = len(built_on)
    assert len(res.rows) == 1000  # read on the loop
    assert len(list(res.iter_rows())) == 1000
    assert len(built_on) == built  # nothing more built there
    assert loop_thread not in built_on


@pytest.fixture
def single_threaded_runtime(runtime):
    """The runtime with its engine on one thread, for the tests of an error met mid-stream.

    DuckDB 1.5.5/1.5.6 race when the error is met while a streamed batch is fetched with more than
    one engine thread: the task that hits the error interrupts its sibling pipelines, and about
    1.6% of the time under load the fetch reports that internal ``INTERRUPT Error: Interrupted!``
    instead of the error itself (no ``interrupt()`` call involved; 0/1200 with ``threads = 1``).
    These tests check how Provisa answers the error DuckDB reports, so they read on one thread
    until DuckDB fixes the race. The client-facing mis-answer is tracked in
    https://github.com/kenstott/provisa/issues/324 (upstream DuckDB issue: see there)."""
    runtime._con.execute("SET threads = 1")
    return runtime


#: The row the engine raises PostgreSQL's own error on (pg_error.raising): past what DuckDB
#: computes when the statement is run (about 128K rows), so it is met only as a batch is read.
_LATE = 400_000
_LATE_ERROR = (
    f"select case when x = {_LATE} then error('postgresql-error 22012: division by zero') "
    f"else x end as v from range({_LATE + 1}) t(x)"
)


@pytest.mark.usefixtures("single_threaded_runtime")
@pytest.mark.parametrize("face", ["rows", "arrow_batches"])
def test_run_sync_raises_postgresqls_error_met_in_a_later_batch(runtime, face):
    """DuckDB's reader meets the engine's error only as it produces the batch that holds it, as
    an OSError carrying its message: it is raised as PostgreSQL's own error, on either face."""
    res = runtime.run_sync(_LATE_ERROR)
    assert isinstance(res, ArrowStreamResult)
    read = res.batches() if face == "rows" else res.arrow_batches()
    first = next(read)
    assert len(first) == _ARROW_STREAM_BATCH_ROWS
    with pytest.raises(PostgresError) as raised:
        list(read)
    assert (raised.value.sqlstate, str(raised.value)) == ("22012", "division by zero")
    # The stream that failed is released (its cursor and the catalogue read it held); the runtime
    # answers the next statement.
    assert res.stats.done
    assert runtime.run_sync("select 42 as y").rows == [(42,)]


@pytest.mark.usefixtures("single_threaded_runtime")
def test_run_sync_raises_any_other_engine_error_as_duckdb_raises_it(runtime):
    """As the exception DuckDB raises for the same error met as the statement is run, so its
    SQLSTATE is known (pgwire.server._sqlstate_of)."""
    res = runtime.run_sync(_LATE_ERROR.replace("postgresql-error 22012: division by zero", "no"))
    read = res.batches()
    assert next(read)
    with pytest.raises(duckdb.InvalidInputException, match="Invalid Input Error: no$"):
        list(read)
