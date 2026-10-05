# Copyright (c) 2026 Kenneth Stott
# Canary: ebf5e893-d0f3-4f57-8c70-073b139ba996
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1190 streaming-uniformity-gap Defect 1, closed for the drivers that had no streaming read:
Databricks, Exasol, HiveServer2, Fabric/Synapse (one implementation in the single-statement
base), every SQLAlchemy-URL type (Trino, SAP HANA, Redshift...), Snowflake, BigQuery, ClickHouse
and DuckDB. Each reads a DIRECT result a batch at a time -- the server is asked for the next batch
only when it is fetched -- and a stream closed early has its statement cancelled. REQ-1915's
filtered row-level read depends on it."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

# -- a DB-API cursor and a pool that record what the stream did ----------------------------------


class _Cursor:
    def __init__(self, rows: list[tuple], columns=("id",)) -> None:
        self._rows = list(rows)
        self.description = [(c,) for c in columns]
        self.fetch_sizes: list[int] = []
        self.cancelled = 0
        self.closed = False

    def execute(self, sql, params=None) -> None:
        self.sql = sql

    def fetchmany(self, size: int) -> list[tuple]:
        self.fetch_sizes.append(size)
        out, self._rows = self._rows[:size], self._rows[size:]
        return out

    def cancel(self) -> None:
        self.cancelled += 1

    cancel_operation = cancel

    def close(self) -> None:
        self.closed = True


class _Pool:
    def __init__(self, conn) -> None:
        self.conn = conn
        self.returned = 0
        self.discarded = 0

    def getconn(self):
        return self.conn

    def putconn(self, conn) -> None:
        self.returned += 1

    def discard(self, conn) -> None:
        self.discarded += 1


async def _read_all(stream, size: int) -> list[tuple]:
    rows: list[tuple] = []
    while batch := await stream.fetch(size):
        rows += batch
    return rows


def _single_statement(driver_cls, cursor):
    driver = driver_cls.__new__(driver_cls)
    driver._pool = _Pool(SimpleNamespace(cursor=lambda: cursor))
    return driver


# -- the single-statement base: databricks, hive, fabric/synapse, exasol --------------------------


@pytest.mark.parametrize(
    "path",
    [
        "provisa.executor.drivers.databricks.DatabricksDriver",
        "provisa.executor.drivers.hive.HiveDriver",
        "provisa.executor.drivers.mssql_warehouse.MssqlWarehouseDriver",
    ],
)
def test_a_single_statement_driver_reads_a_batch_at_a_time_and_gives_the_connection_back(path):
    import importlib

    module, name = path.rsplit(".", 1)
    cursor = _Cursor([(1,), (2,), (3,)])
    driver = _single_statement(getattr(importlib.import_module(module), name), cursor)
    assert driver.supports_streaming

    async def run():
        stream = await driver.open_stream("SELECT id FROM t")
        return stream.column_names, await _read_all(stream, 2)

    columns, rows = asyncio.run(run())
    assert columns == ["id"] and rows == [(1,), (2,), (3,)]
    assert cursor.fetch_sizes == [2, 2, 2]  # one batch per fetch, never the whole result
    assert driver._pool.returned == 1 and driver._pool.discarded == 0 and cursor.cancelled == 0


@pytest.mark.parametrize(
    "path",
    [
        "provisa.executor.drivers.databricks.DatabricksDriver",
        "provisa.executor.drivers.hive.HiveDriver",
        "provisa.executor.drivers.mssql_warehouse.MssqlWarehouseDriver",
    ],
)
def test_a_single_statement_stream_closed_early_cancels_and_discards(path):
    import importlib

    module, name = path.rsplit(".", 1)
    cursor = _Cursor([(1,), (2,), (3,)])
    driver = _single_statement(getattr(importlib.import_module(module), name), cursor)

    async def run():
        stream = await driver.open_stream("SELECT id FROM t")
        await stream.fetch(1)
        await stream.close()

    asyncio.run(run())
    assert cursor.cancelled == 1  # the driver's own cancel (cancel / cancel_operation)
    assert driver._pool.discarded == 1 and driver._pool.returned == 0


def test_exasol_reads_its_statement_a_batch_at_a_time_and_aborts_one_closed_early():
    from provisa.executor.drivers.exasol import ExasolDriver

    class _Stmt:
        result_type = "resultSet"

        def __init__(self) -> None:
            self._rows = [(1,), (2,), (3,)]
            self.closed = False

        def column_names(self):
            return ["id"]

        def fetchmany(self, size):
            out, self._rows = self._rows[:size], self._rows[size:]
            return out

        def close(self):
            self.closed = True

    stmt = _Stmt()
    aborted: list[int] = []
    conn = SimpleNamespace(execute=lambda sql: stmt, abort_query=lambda: aborted.append(1))
    driver = ExasolDriver.__new__(ExasolDriver)
    pool = _Pool(conn)
    driver._pool = pool  # pyright: ignore[reportAttributeAccessIssue]  # a recording stand-in

    async def run():
        stream = await driver.open_stream("SELECT id FROM t")
        first = await stream.fetch(2)
        await stream.close()
        return stream.column_names, first

    columns, first = asyncio.run(run())
    assert columns == ["id"] and first == [(1,), (2,)]
    assert aborted == [1] and stmt.closed is False  # aborted on its connection, which is dropped
    assert pool.discarded == 1


# -- SQLAlchemy-URL types (trino, saphana, redshift, ...) ----------------------------------------


def test_a_sqlalchemy_source_streams_through_stream_results():
    from sqlalchemy import create_engine, text

    from provisa.executor.drivers.sqlalchemy_driver import SQLAlchemyDriver

    driver = SQLAlchemyDriver("sqlite")
    driver._engine = create_engine("sqlite://")
    with driver._engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (1), (2), (3)"))
    assert driver.supports_streaming

    async def run():
        stream = await driver.open_stream("SELECT id FROM t ORDER BY id")
        batches = []
        while batch := await stream.fetch(2):
            batches.append(batch)
        return stream.column_names, batches

    columns, batches = asyncio.run(run())
    assert columns == ["id"] and batches == [[(1,), (2,)], [(3,)]]


def test_a_sqlalchemy_stream_closed_early_invalidates_its_connection():
    from sqlalchemy import create_engine, event, text

    from provisa.executor.drivers.sqlalchemy_driver import SQLAlchemyDriver

    driver = SQLAlchemyDriver("sqlite")
    driver._engine = create_engine("sqlite://")
    with driver._engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (1), (2), (3)"))
    invalidated: list[int] = []
    event.listen(driver._engine, "invalidate", lambda *a: invalidated.append(1))

    async def run():
        stream = await driver.open_stream("SELECT id FROM t")
        await stream.fetch(1)
        await stream.close()
        await stream.close()  # idempotent

    asyncio.run(run())
    assert invalidated == [1]


# -- DuckDB ---------------------------------------------------------------------------------------


def test_duckdb_streams_a_batch_at_a_time_and_interrupts_one_closed_early():
    from provisa.executor.drivers.duckdb_driver import DuckDBDriver

    driver = DuckDBDriver()
    asyncio.run(driver.connect("", 0, ":memory:", "", ""))
    assert driver.supports_streaming

    async def run():
        stream = await driver.open_stream("SELECT range AS id FROM range(5)")
        batches = []
        while batch := await stream.fetch(2):
            batches.append(batch)
        early = await driver.open_stream("SELECT range AS id FROM range(1000)")
        first = await early.fetch(3)
        await early.close()
        return stream.column_names, batches, first

    columns, batches, first = asyncio.run(run())
    assert columns == ["id"] and [len(b) for b in batches] == [2, 2, 1]
    assert first == [(0,), (1,), (2,)]
    asyncio.run(driver.close())


# -- Snowflake and BigQuery (no pool: a cursor the driver keeps) ---------------------------------


def test_snowflake_streams_its_cursor_and_aborts_one_closed_early():
    from provisa.executor.drivers.snowflake import SnowflakeDriver

    aborted: list[str] = []

    class _SnowflakeCursor(_Cursor):
        sfqid = "q-1"

        def abort_query(self, qid: str) -> None:
            aborted.append(qid)

    cursor = _SnowflakeCursor([(1,), (2,), (3,)])
    driver = SnowflakeDriver()
    driver._conn = SimpleNamespace(cursor=lambda: cursor)
    assert driver.supports_streaming

    async def run():
        stream = await driver.open_stream("SELECT id FROM t")
        first = await stream.fetch(2)
        await stream.close()
        return first

    assert asyncio.run(run()) == [(1,), (2,)]
    assert aborted == ["q-1"] and cursor.closed


def test_bigquery_reads_its_job_a_page_at_a_time_and_cancels_one_closed_early():
    from provisa.executor.drivers.bigquery import BigQueryDriver

    pulled: list[int] = []

    def rows():
        for i in range(1, 4):
            pulled.append(i)
            yield SimpleNamespace(values=lambda i=i: (i,))

    class _Result:
        schema = [SimpleNamespace(name="id")]

        def __iter__(self):
            return rows()

    cancelled: list[int] = []
    job = SimpleNamespace(result=lambda: _Result(), cancel=lambda: cancelled.append(1))
    driver = BigQueryDriver()
    driver._client = SimpleNamespace(query=lambda sql: job)
    assert driver.supports_streaming

    async def run():
        stream = await driver.open_stream("SELECT id FROM t")
        first = await stream.fetch(2)
        await stream.close()
        return stream.column_names, first

    columns, first = asyncio.run(run())
    assert columns == ["id"] and first == [(1,), (2,)]
    assert pulled == [1, 2]  # the third row was never pulled
    assert cancelled == [1]


# -- ClickHouse -----------------------------------------------------------------------------------


def test_clickhouse_reads_row_blocks_as_asked_and_kills_one_closed_early(monkeypatch):
    from provisa.executor.drivers.clickhouse import ClickHouseDriver

    blocks_read: list[int] = []

    class _Context:
        source = SimpleNamespace(column_names=("id",))

        def __init__(self) -> None:
            self.exited = False

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.exited = True

        def __iter__(self):
            for n, block in enumerate([[(1,), (2,)], [(3,), (4,)], [(5,)]], start=1):
                blocks_read.append(n)
                yield block

    context = _Context()
    client = SimpleNamespace(query_row_block_stream=lambda sql, parameters, settings: context)
    driver = ClickHouseDriver()
    pool = _Pool(client)
    driver._pool = pool  # pyright: ignore[reportAttributeAccessIssue]  # a recording stand-in
    killed: list[str] = []
    monkeypatch.setattr(driver, "_kill_query", lambda query_id: killed.append(query_id))
    assert driver.supports_streaming

    async def run():
        stream = await driver.open_stream("SELECT id FROM t")
        first = await stream.fetch(3)
        await stream.close()
        return stream.column_names, first

    columns, first = asyncio.run(run())
    assert columns == ["id"] and first == [(1,), (2,), (3,)]
    assert blocks_read == [1, 2]  # the third block was never asked for
    assert len(killed) == 1 and context.exited is False  # killed; its client is discarded
    assert pool.discarded == 1
