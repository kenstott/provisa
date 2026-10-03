# Copyright (c) 2026 Kenneth Stott
# Canary: a562009e-ddfc-425b-bd0f-e62e300c06a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1905: a runtime's read takes its cursor and gives it back in sections the request's
deadline does not interrupt, and gives it back however the read ends — a statement the deadline
cuts short included. A timed-out request never leaves a cursor open on the runtime's connection.
"""

from __future__ import annotations

from typing import Any

import pytest

from provisa.core.request_deadline import DeadlinePassed


class _Cursor:
    def __init__(self, conn: "_Conn") -> None:
        self.conn = conn
        self.closed = False
        self.description = None

    def execute(self, *args: Any, **kwargs: Any) -> None:
        raise DeadlinePassed()

    def close(self) -> None:
        self.closed = True

    def cancel(self) -> None:  # what a deadline's cancel calls (pyodbc, databricks)
        pass

    def interrupt(self) -> None:  # DuckDB's
        pass


class _Conn:
    def __init__(self) -> None:
        self.cursors: list[_Cursor] = []

    def cursor(self, *args: Any, **kwargs: Any) -> _Cursor:
        cur = _Cursor(self)
        self.cursors.append(cur)
        return cur


def _snowflake() -> tuple[Any, _Conn]:
    from provisa.federation.snowflake_runtime import SnowflakeFederationRuntime

    rt = object.__new__(SnowflakeFederationRuntime)
    rt._conn = _Conn()
    return rt, rt._conn


def test_a_snowflake_stream_cut_by_the_deadline_closes_its_cursor():
    rt, conn = _snowflake()
    with pytest.raises(DeadlinePassed):
        rt.run_arrow_stream("SELECT 1")
    assert [c.closed for c in conn.cursors] == [True]


def test_a_databricks_stream_cut_by_the_deadline_closes_its_cursor():
    from provisa.federation.databricks_runtime import DatabricksFederationRuntime

    rt = object.__new__(DatabricksFederationRuntime)
    rt._conn = conn = _Conn()
    with pytest.raises(DeadlinePassed):
        rt.run_arrow_stream("SELECT 1")
    assert [c.closed for c in conn.cursors] == [True]


def test_a_sql_server_warehouse_stream_cut_by_the_deadline_closes_its_cursor():
    from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime

    rt = object.__new__(MssqlWarehouseRuntime)
    rt._conn = conn = _Conn()
    with pytest.raises(DeadlinePassed):
        rt.run_arrow_stream("SELECT 1")
    assert [c.closed for c in conn.cursors] == [True]


def _duckdb() -> tuple[Any, _Conn]:
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime, _CatalogGate

    rt = object.__new__(DuckDBFederationRuntime)
    rt._con = conn = _Conn()
    rt._catalog_gate = _CatalogGate()
    rt._refresh_store_relations = lambda sql: sql  # type: ignore[method-assign]
    rt._rewrite_clickhouse_relations = lambda sql, params, **kw: (sql, False)  # type: ignore[method-assign]
    return rt, conn


@pytest.mark.parametrize("read", ["run_sync", "run_arrow_stream"])
def test_a_duckdb_stream_cut_by_the_deadline_closes_its_cursor_and_frees_the_gate(read):
    rt, conn = _duckdb()
    with pytest.raises(DeadlinePassed):
        getattr(rt, read)("SELECT 1")
    assert [c.closed for c in conn.cursors] == [True]
    with rt._catalog_gate.write():  # no reader left holding the gate
        pass


def test_a_sqlalchemy_read_cut_by_the_deadline_gives_back_its_pool_connection():
    from provisa.federation import sqlalchemy_runtime
    from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime

    class _SAConn:
        closed = False
        # a psycopg2 connection, as far as the deadline's cancel is concerned
        connection = type("C", (), {"dbapi_connection": type("D", (), {"cancel": print})()})()

        def execution_options(self, **kw: Any) -> "_SAConn":
            return self

        def exec_driver_sql(self, *a: Any) -> None:
            raise DeadlinePassed()

        def close(self) -> None:
            self.closed = True

    taken: list[_SAConn] = []

    def connect() -> _SAConn:
        conn = _SAConn()
        taken.append(conn)
        return conn

    rt = object.__new__(SqlAlchemyFederationRuntime)
    rt._sa = type("E", (), {"connect": staticmethod(connect)})()
    rt._open_side_conn = lambda: None  # type: ignore[method-assign]
    original = sqlalchemy_runtime._driver
    sqlalchemy_runtime._driver = lambda dbapi: "psycopg2"  # type: ignore[assignment]
    try:
        with pytest.raises(DeadlinePassed):
            rt.run_sync("SELECT 1")
    finally:
        sqlalchemy_runtime._driver = original
    assert [c.closed for c in taken] == [True]


async def test_a_sqlalchemy_materialized_read_that_fails_closes_its_cursor():
    from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime

    conn = _Conn()
    conn.dbapi_connection = object()  # type: ignore[attr-defined]
    rt = object.__new__(SqlAlchemyFederationRuntime)
    rt._con = conn
    rt._open_side_conn = lambda: None  # type: ignore[method-assign]
    with pytest.raises(DeadlinePassed):
        await rt.run("SELECT 1")
    assert [c.closed for c in conn.cursors] == [True]
