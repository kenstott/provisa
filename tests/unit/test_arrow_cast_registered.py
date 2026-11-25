# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2e7f41-6b3a-4c58-a1e0-7f5c2b8d3e94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DIRECT read from a driver that fetches Arrow reaches its consumer as the driver's own record
batches, each column cast to its registered type (REQ-2194, REQ-1961): the Flight DIRECT stream,
the airport scan and a replica's ``DirectTableSource`` never rebuild the batches from rows. A cast
Arrow refuses (a value the registered type cannot hold) fails naming the column, never truncated.
BigQuery's stream is one such driver: its job's row iterator yields record batches
(``to_arrow_iterable``)."""

# Requirements: REQ-2194, REQ-1961

from __future__ import annotations

import asyncio
import types
from datetime import time
from decimal import Decimal
from unittest.mock import AsyncMock

import pyarrow as pa
import pytest

from provisa.compiler.result_shape import RegisteredType
from provisa.core.connection_loop import connection_loop
from provisa.core.rpc_loop import run_rpc
from provisa.executor.drivers import duckdb_driver
from provisa.executor.drivers.base import DirectArrowStream, DirectResultStream
from provisa.executor.drivers.duckdb_driver import DuckDBDriver
from provisa.executor.result import ArrowStreamResult
from provisa.federation import runtime_support
from provisa.federation.runtime import EngineRuntime
from provisa.federation.runtime_support import ColumnTypeMismatch

# DuckDB answers i as BIGINT and d as DECIMAL(18,2); the model registers i as integer and d as
# double precision, so each must be cast to what the model declares.
_SQL = "SELECT range AS i, (range * 1.25)::DECIMAL(18,2) AS d FROM range(5)"
_SHAPE = [("i", RegisteredType("integer")), ("d", RegisteredType("double precision"))]
_REGISTERED = pa.schema([pa.field("i", pa.int32()), pa.field("d", pa.float64())])
# A value past a 32-bit integer: a registered integer cannot hold it.
_WIDE_SQL = "SELECT (range + 4294967296) AS i, 1.25::DECIMAL(18,2) AS d FROM range(2)"


@pytest.fixture
def driver():
    d = DuckDBDriver()
    asyncio.run(d.connect("", 0, ":memory:", "", ""))
    yield d
    asyncio.run(d.close())


class _Pools:
    """The one source's pool, as ``executor.direct.open_direct_stream`` reads it."""

    def __init__(self, driver: DuckDBDriver) -> None:
        self._driver = driver

    def has(self, _source_id: str) -> bool:
        return True

    def supports_stream(self, _source_id: str) -> bool:
        return True

    async def open_stream(self, _source_id: str, sql: str, params: list | None):
        return await self._driver.open_stream(sql, params)


@pytest.fixture
def no_rows(monkeypatch):
    """Every row builder of the three consumers fails the read: a batch that reaches a consumer
    through rows fails the test. Records each batch the DuckDB driver turns into rows."""
    built: list[int] = []
    real = duckdb_driver.arrow_rows

    def _recording(batch):
        built.append(batch.num_rows)
        return real(batch)

    def _refused(*_a, **_k):
        raise AssertionError("the driver's batches were rebuilt from rows")

    monkeypatch.setattr(duckdb_driver, "arrow_rows", _recording)
    monkeypatch.setattr(runtime_support, "arrow_batches_from_rows", _refused)
    monkeypatch.setattr("provisa.api.airport.query._typed_batches_from_rows", _refused)
    monkeypatch.setattr("provisa.federation.replica_source.rows_to_batch", _refused)
    return built


def _shape(shape):
    return types.SimpleNamespace(now=lambda: shape)


# -- the Flight DIRECT stream ---------------------------------------------------------------------


def _flight_plan(sql: str, shape):
    from provisa.transpiler.router import Route

    return types.SimpleNamespace(
        warnings=[],
        materialize=None,
        columns=None,  # as _Plan: no column transform (REQ-2131)
        result_shape=_shape(shape),
        route=Route.DIRECT,
        source_id="src",
        sql=sql,
        exec_params=[],
    )


def _flight_direct(monkeypatch, driver, sql: str, shape):
    """The Flight SQL DIRECT terminal over a DuckDB source: what it hands pyarrow to stream,
    which holds the RPC's loop until it is drained (run in ``run_rpc``, as the RPC is)."""
    from provisa.api.flight import server as flight_server
    from provisa.api.flight.server import ProvisaFlightServer
    from provisa.pgwire import _pipeline

    plan = _flight_plan(sql, shape)
    monkeypatch.setattr(_pipeline, "govern_batch_final_plan_with_fn", AsyncMock(return_value=plan))
    monkeypatch.setattr(_pipeline, "require_governed_plan", lambda _p: None)
    monkeypatch.setattr(_pipeline, "streams_from_terminal", lambda *_a, **_k: True)
    monkeypatch.setattr(_pipeline, "finalize_audit", AsyncMock(return_value=None))
    # The response cache keeps nothing here: the MISS is the live stream itself.
    monkeypatch.setattr(
        _pipeline,
        "serve_stream_through_cache",
        lambda _plan, _state, *, run, check_rows, passthrough, open_rows: open_rows(),
    )
    monkeypatch.setattr(flight_server, "_audited_arrow", lambda _plan, batches: batches)

    srv = ProvisaFlightServer.__new__(ProvisaFlightServer)
    srv._state = types.SimpleNamespace(
        federation_engine=EngineRuntime.__new__(EngineRuntime),
        source_pools=_Pools(driver),
    )  # pyright: ignore[reportAttributeAccessIssue]
    served: dict = {}

    def _capture(schema, batch_gen, _role_id, _warnings=()):
        served["schema"], served["batches"] = schema, batch_gen
        return None

    monkeypatch.setattr(srv, "_acquire_stream_slot", lambda: lambda: None)
    monkeypatch.setattr(srv, "_license_stream_gen", _capture)
    srv._do_get_sql_governed({"query": sql, "role": "analyst"})
    return served["schema"], served["batches"]


def test_flight_direct_streams_the_drivers_batches_cast_to_the_registered_types(
    monkeypatch, driver, no_rows
):
    schema, batches = run_rpc(lambda: _flight_direct(monkeypatch, driver, _SQL, _SHAPE))
    drained = list(batches)
    assert schema == _REGISTERED
    assert drained and all(b.schema == _REGISTERED for b in drained)
    table = pa.Table.from_batches(drained, schema=_REGISTERED)
    assert table.column("i").to_pylist() == [0, 1, 2, 3, 4]
    assert table.column("d").to_pylist()[1] == 1.25
    assert no_rows == []


def test_flight_direct_refuses_a_lossy_cast_naming_the_column(monkeypatch, driver, no_rows):
    _schema, batches = run_rpc(lambda: _flight_direct(monkeypatch, driver, _WIDE_SQL, _SHAPE))
    with pytest.raises(ColumnTypeMismatch) as refused:
        list(batches)
    assert refused.value.column == "i"
    assert no_rows == []


# -- the airport scan -----------------------------------------------------------------------------


def _airport_plan(sql: str, shape):
    from provisa.transpiler.router import Route

    return types.SimpleNamespace(
        route=Route.DIRECT,
        sql=sql,
        source_id="src",
        sources=frozenset({"src"}),
        route_reason=None,
        cache_as_of=None,
        table_ids=(),
        exec_params=[],
        pk_bounds=(),
        role_id="analyst",
        result_shape=_shape(shape),
        audit=None,
        audit_written=False,
        row_count=None,
        audit_deferred=None,
        row_limit=None,
        writes_tables=False,
        live_caps=(),
        live_caps_org=None,
        tier_caps=None,
        tier_plan=None,
        warnings=[],
        answered_in=None,
    )


def _airport_scan(monkeypatch, driver, sql: str, shape):
    """The airport's governed DIRECT scan over a DuckDB source."""
    from provisa.api.airport.query import governed_table_scan_stream

    plan = _airport_plan(sql, shape)
    monkeypatch.setattr("provisa.api.airport.query._plan_for_scan", lambda *_a: plan)
    state = types.SimpleNamespace(
        federation_engine=EngineRuntime.__new__(EngineRuntime), source_pools=_Pools(driver)
    )
    schema, batches, _warnings = governed_table_scan_stream(state, sql, "analyst")
    return schema, batches, state


def test_the_airport_scan_streams_the_drivers_batches_cast_to_the_registered_types(
    monkeypatch, driver, no_rows
):
    from provisa.api.airport.query import governed_table_scan_schema

    with connection_loop():
        schema, batches, state = _airport_scan(monkeypatch, driver, _SQL, _SHAPE)
        drained = list(batches)
        advertised = governed_table_scan_schema(state, _SQL, "analyst")
    assert schema == _REGISTERED and advertised == _REGISTERED
    assert drained and all(b.schema == _REGISTERED for b in drained)
    assert sum(b.num_rows for b in drained) == 5
    assert no_rows == []


def test_the_airport_scan_refuses_a_lossy_cast_naming_the_column(monkeypatch, driver, no_rows):
    with connection_loop():
        _schema, batches, _state = _airport_scan(monkeypatch, driver, _WIDE_SQL, _SHAPE)
        with pytest.raises(ColumnTypeMismatch) as refused:
            list(batches)
    assert refused.value.column == "i"
    assert no_rows == []


# -- a replica's DirectTableSource ----------------------------------------------------------------


async def _replica(driver, sql: str, columns, batch_rows: int = 2):
    from provisa.federation.replica_source import DirectTableSource

    source = DirectTableSource(lambda: driver.open_stream(sql), columns)
    return [batch async for batch in source.batches(batch_rows)]


def test_a_replica_reads_the_drivers_batches_cast_to_the_registered_types(driver, no_rows):
    columns = [("i", "integer"), ("d", "double")]
    batches = asyncio.run(_replica(driver, _SQL, columns))
    assert [b.num_rows for b in batches] == [2, 2, 1]
    assert all(b.schema == _REGISTERED for b in batches)
    assert pa.Table.from_batches(batches).column("i").to_pylist() == [0, 1, 2, 3, 4]
    assert no_rows == []


def test_a_replica_refuses_a_lossy_cast_naming_the_column(driver, no_rows):
    with pytest.raises(ColumnTypeMismatch) as refused:
        asyncio.run(_replica(driver, _WIDE_SQL, [("i", "integer"), ("d", "double")]))
    assert refused.value.column == "i"


def test_a_replica_column_held_as_text_is_the_text_the_row_read_writes(driver, monkeypatch):
    """An unsized numeric, a time and an interval are held by a replica as text: the Arrow read
    writes the same text of each value as the row read always has."""
    sql = (
        "SELECT range AS i, (range * 1.25)::DECIMAL(18,2) AS n, TIME '12:34:56' AS t, "
        "INTERVAL 3 DAY AS iv FROM range(3)"
    )
    columns = [("i", "integer"), ("n", "numeric"), ("t", "time"), ("iv", "interval")]

    class _RowFace(DirectResultStream):
        """The same driver stream read only through its row face: the row read."""

        def __init__(self, inner: DirectArrowStream) -> None:
            self._inner = inner
            self.column_names = inner.column_names
            self.column_types = inner.column_types

        async def fetch(self, size: int) -> list[tuple]:
            return await self._inner.fetch(size)

        async def close(self) -> None:
            await self._inner.close()

    async def _row_read():
        from provisa.federation.replica_source import DirectTableSource

        async def _open():
            return _RowFace(await driver.open_stream(sql))

        return [b async for b in DirectTableSource(_open, columns).batches(2)]

    by_rows = asyncio.run(_row_read())
    by_arrow = asyncio.run(_replica(driver, sql, columns))
    assert pa.Table.from_batches(by_arrow).equals(pa.Table.from_batches(by_rows))
    assert pa.Table.from_batches(by_arrow).column("n").to_pylist() == ["0.00", "1.25", "2.50"]
    assert time(12, 34, 56).isoformat() in pa.Table.from_batches(by_arrow).column("t").to_pylist()


def test_a_replica_column_the_driver_does_not_return_is_refused_by_name(driver):
    with pytest.raises(ValueError, match="missing_col"):
        asyncio.run(_replica(driver, _SQL, [("i", "integer"), ("missing_col", "text")]))


# -- BigQuery's stream ----------------------------------------------------------------------------


def _bigquery(fields, pages, pulled: list[int], cancelled: list[int]):
    from provisa.executor.drivers.bigquery import BigQueryDriver

    class _RowIterator:
        schema = fields

        def to_arrow_iterable(self):
            for n, page in enumerate(pages, start=1):
                pulled.append(n)
                yield page

        def __iter__(self):
            raise AssertionError("the job's rows were read one Python row at a time")

    job = types.SimpleNamespace(result=lambda: _RowIterator(), cancel=lambda: cancelled.append(1))
    driver = BigQueryDriver()
    driver._client = types.SimpleNamespace(query=lambda sql: job)
    return driver


def test_bigquery_streams_the_record_batches_of_its_jobs_row_iterator():
    from google.cloud.bigquery import SchemaField
    from google.cloud.bigquery._pandas_helpers import bq_to_arrow_schema

    fields = [SchemaField("id", "INTEGER"), SchemaField("amount", "NUMERIC")]
    schema = bq_to_arrow_schema(fields)
    pages = [
        pa.RecordBatch.from_arrays(
            [pa.array([i]), pa.array([Decimal(i)], type=schema.field("amount").type)],
            schema=schema,
        )
        for i in range(1, 4)
    ]
    pulled: list[int] = []
    cancelled: list[int] = []
    driver = _bigquery(fields, pages, pulled, cancelled)

    async def run():
        stream = await driver.open_stream("SELECT id, amount FROM t")
        first = await stream.fetch_arrow()
        await stream.close()
        return stream, first

    stream, first = asyncio.run(run())
    assert isinstance(stream, DirectArrowStream)
    assert stream.schema == schema and stream.column_names == ["id", "amount"]
    assert first is pages[0]
    assert pulled == [1]  # one page fetched, the rest never pulled
    assert cancelled == [1]


def test_bigquery_through_the_direct_terminal_is_arrow_and_reads_as_rows():
    from google.cloud.bigquery import SchemaField
    from google.cloud.bigquery._pandas_helpers import bq_to_arrow_schema

    fields = [SchemaField("id", "INTEGER")]
    schema = bq_to_arrow_schema(fields)
    pages = [pa.RecordBatch.from_arrays([pa.array([1, 2])], schema=schema)]
    driver = _bigquery(fields, pages, [], [])

    class _BQPools:
        async def open_stream(self, _source_id, sql, params):
            return await driver.open_stream(sql, params)

    loop = asyncio.new_event_loop()
    try:
        stream = EngineRuntime.__new__(EngineRuntime).execute_native_stream(
            _BQPools(), "bq", "SELECT id FROM t", [], run=loop.run_until_complete
        )
        assert isinstance(stream, ArrowStreamResult) and stream.schema == schema
        assert list(stream.iter_rows()) == [(1,), (2,)]
    finally:
        loop.close()


def test_a_bigquery_column_with_no_arrow_type_is_refused_by_name():
    from google.cloud.bigquery import SchemaField

    driver = _bigquery([SchemaField("span", "INTERVAL")], [], [], [])
    with pytest.raises(ValueError, match="span INTERVAL"):
        asyncio.run(driver.open_stream("SELECT span FROM t"))
