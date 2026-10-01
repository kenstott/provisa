# Copyright (c) 2026 Kenneth Stott
# Canary: 4d8b1e62-7a3c-4f9e-b2d6-0c5e8a1f3b97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every federation engine runtime registers its driver's real cancel around a blocking query, so
the request deadline (provisa.core.request_deadline) stops it mid-call (REQ-1882, amended
2026-09-29). Fake driver objects only — no live engines."""

# Requirements: REQ-1882

from __future__ import annotations

import math
import threading
import time
from typing import Any

import pytest

from provisa.core import request_deadline

_BUDGET = 0.3
_BLOCK = 5.0


class _Blocker:
    """A blocking call that returns only when cancelled (or after _BLOCK seconds)."""

    def __init__(self) -> None:
        self.cancelled = threading.Event()

    def cancel(self, *_: Any) -> None:
        self.cancelled.set()

    def block(self) -> None:
        if self.cancelled.wait(_BLOCK):
            raise RuntimeError("query cancelled by driver")
        pytest.fail("the request deadline never cancelled the blocking call")


def _expect_deadline_cancel(fn, blocker: _Blocker) -> None:
    t0 = time.monotonic()
    with request_deadline.within(_BUDGET), pytest.raises(TimeoutError):
        fn()
    assert blocker.cancelled.is_set()
    assert time.monotonic() - t0 < _BLOCK / 2


# -- DuckDB ------------------------------------------------------------------------------------


def test_duckdb_run_arrow_interrupts_its_cursor(monkeypatch) -> None:
    from provisa.federation import duckdb_runtime as mod

    b = _Blocker()

    class _Cur:
        def interrupt(self) -> None:
            b.cancel()

        def execute(self, *_: Any) -> Any:
            b.block()

        def close(self) -> None:
            pass

    rt = mod.DuckDBFederationRuntime.__new__(mod.DuckDBFederationRuntime)
    rt._catalog_gate = mod._CatalogGate()  # type: ignore[attr-defined]
    rt._con = type("_Con", (), {"cursor": lambda self: _Cur()})()  # type: ignore[attr-defined]
    monkeypatch.setattr(rt, "_refresh_store_relations", lambda sql: None)
    # REQ-899: the statement names no ClickHouse relation.
    monkeypatch.setattr(rt, "_rewrite_clickhouse_relations", lambda sql, params, **_: (sql, False))
    _expect_deadline_cancel(lambda: rt.run_arrow("SELECT 1"), b)


# -- ClickHouse --------------------------------------------------------------------------------


def test_clickhouse_http_backend_kills_its_query_id() -> None:
    from provisa.federation.clickhouse_runtime import _ServerBackend

    b = _Blocker()
    seen: dict[str, str] = {}

    class _Client:
        def query(self, sql: str, settings: dict) -> Any:
            seen["qid"] = settings["query_id"]
            b.block()

    be = _ServerBackend.__new__(_ServerBackend)
    be._client = _Client()  # type: ignore[attr-defined]
    killed: list[str] = []

    def _kill(qid: str) -> None:
        killed.append(qid)
        b.cancel()

    be._kill = _kill  # type: ignore[method-assign]
    _expect_deadline_cancel(lambda: be.query("SELECT 1"), b)
    assert killed == [seen["qid"]]


def test_clickhouse_native_backend_kills_its_query_id() -> None:
    from provisa.federation.clickhouse_runtime import _NativeBackend

    b = _Blocker()
    seen: dict[str, str] = {}

    class _Client:
        def execute(self, sql: str, with_column_types: bool, query_id: str) -> Any:
            seen["qid"] = query_id
            b.block()

    be = _NativeBackend.__new__(_NativeBackend)
    be._client = _Client()  # type: ignore[attr-defined]
    killed: list[str] = []

    def _kill(qid: str) -> None:
        killed.append(qid)
        b.cancel()

    be._kill = _kill  # type: ignore[method-assign]
    _expect_deadline_cancel(lambda: be.query("SELECT 1"), b)
    assert killed == [seen["qid"]]


# -- BigQuery ----------------------------------------------------------------------------------


def test_bigquery_run_arrow_cancels_its_job() -> None:
    from provisa.federation.bigquery_runtime import BigQueryFederationRuntime

    b = _Blocker()

    class _Job:
        def cancel(self) -> None:
            b.cancel()

        def to_arrow(self) -> Any:
            b.block()

    rt = BigQueryFederationRuntime.__new__(BigQueryFederationRuntime)
    rt._client = type("_C", (), {"query": lambda self, sql: _Job()})()  # type: ignore[attr-defined]
    _expect_deadline_cancel(lambda: rt.run_arrow("SELECT 1"), b)


# -- Databricks --------------------------------------------------------------------------------


def test_databricks_run_arrow_cancels_its_cursor() -> None:
    from provisa.federation.databricks_runtime import DatabricksFederationRuntime

    b = _Blocker()

    class _Cur:
        def cancel(self) -> None:
            b.cancel()

        def execute(self, *_: Any) -> None:
            b.block()

        def close(self) -> None:
            pass

    rt = DatabricksFederationRuntime.__new__(DatabricksFederationRuntime)
    rt._conn = type("_Conn", (), {"cursor": lambda self: _Cur()})()  # type: ignore[attr-defined]
    _expect_deadline_cancel(lambda: rt.run_arrow("SELECT 1"), b)


# -- Snowflake ---------------------------------------------------------------------------------


def test_snowflake_execute_arms_the_connector_timeout_from_the_budget() -> None:
    from provisa.federation.snowflake_runtime import _execute_within_deadline

    calls: list[dict[str, Any]] = []

    class _Cur:
        def execute(self, sql: str, params: Any, **kw: Any) -> None:
            calls.append(kw)

    with request_deadline.within(10.0) as dl:
        _execute_within_deadline(_Cur(), "SELECT 1", None)
        left = dl.remaining()
    assert 1 <= calls[0]["timeout"] <= math.ceil(left) + 1

    _execute_within_deadline(_Cur(), "SELECT 1", None)  # outside a request: no timeout armed
    assert calls[1] == {}


def test_snowflake_execute_after_expiry_raises_without_running() -> None:
    from provisa.federation.snowflake_runtime import _execute_within_deadline

    class _Cur:
        def execute(self, *_: Any, **__: Any) -> None:
            pytest.fail("must not start a statement after the budget expired")

    with request_deadline.within(0.05), pytest.raises(TimeoutError):
        time.sleep(0.1)
        _execute_within_deadline(_Cur(), "SELECT 1", None)


# -- MSSQL warehouse (pyodbc) ------------------------------------------------------------------


def test_mssql_warehouse_stream_cancels_its_cursor() -> None:
    from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime

    b = _Blocker()

    class _Cur:
        description = None

        def cancel(self) -> None:
            b.cancel()

        def execute(self, *_: Any) -> None:
            b.block()

        def close(self) -> None:
            pass

    rt = MssqlWarehouseRuntime.__new__(MssqlWarehouseRuntime)
    rt._conn = type("_Conn", (), {"cursor": lambda self: _Cur()})()  # type: ignore[attr-defined]
    _expect_deadline_cancel(lambda: rt.run_arrow_stream("SELECT 1"), b)


# -- SQLAlchemy --------------------------------------------------------------------------------


def _fake_dbapi_conn(module: str, b: _Blocker) -> Any:
    cls = type("Connection", (), {"cancel": lambda self: b.cancel()})
    cls.__module__ = module
    return cls()


def test_sqlalchemy_run_cancels_via_the_psycopg2_connection() -> None:
    from provisa.core.connection_loop import connection_loop
    from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime

    b = _Blocker()

    class _Cur:
        description = None

        def execute(self, *_: Any) -> None:
            b.block()

        def close(self) -> None:
            pass

    proxied = type(
        "_Proxied",
        (),
        {"cursor": lambda self: _Cur(), "commit": lambda self: None},
    )()
    proxied.dbapi_connection = _fake_dbapi_conn("psycopg2.extensions", b)  # type: ignore[attr-defined]
    rt = SqlAlchemyFederationRuntime.__new__(SqlAlchemyFederationRuntime)
    rt._con = proxied  # type: ignore[attr-defined]

    # Production path: run()'s run_in_executor work runs inline on the request's connection loop,
    # so the request deadline bound by cl.run(timeout=...) is visible to it.
    t0 = time.monotonic()
    with connection_loop() as cl, pytest.raises(TimeoutError):
        cl.run(rt.run("SELECT 1"), timeout=_BUDGET)
    assert b.cancelled.is_set()
    assert time.monotonic() - t0 < _BLOCK / 2


def test_sqlalchemy_cancel_dispatch_per_driver() -> None:
    from provisa.federation.sqlalchemy_runtime import _dbapi_cancel

    def _no_side() -> Any:
        pytest.fail("only the MySQL drivers open a side connection")

    b = _Blocker()
    for module in ("psycopg2.extensions", "psycopg", "oracledb.connection"):
        assert _dbapi_cancel(_fake_dbapi_conn(module, b), None, _no_side) is not None
    cur = type("C", (), {"cancel": lambda self: None})()
    assert _dbapi_cancel(_fake_dbapi_conn("pyodbc", b), cur, _no_side) == cur.cancel
    with pytest.raises(RuntimeError, match="create it before execute"):
        _dbapi_cancel(_fake_dbapi_conn("pyodbc", b), None, _no_side)
    assert _dbapi_cancel(_fake_dbapi_conn("ibm_db_dbi", b), None, _no_side) is None


def test_sqlalchemy_exasol_aborts_via_its_pyexasol_connection() -> None:
    from provisa.federation.sqlalchemy_runtime import _dbapi_cancel

    b = _Blocker()

    class _PyExa:
        def abort_query(self) -> None:
            b.cancel()

    class Connection:
        connection = _PyExa()

    Connection.__module__ = "exasol.driver.websocket._connection"
    cancel = _dbapi_cancel(Connection(), None, lambda: pytest.fail("no side connection"))
    assert cancel is not None

    def _blocking() -> None:
        with request_deadline.cancel_on_deadline(cancel):
            b.block()

    _expect_deadline_cancel(_blocking, b)


def _fake_mysql_conn(module: str, thread_id: int, b: _Blocker) -> Any:
    class Connection:
        def thread_id(self) -> int:
            return thread_id

        def cursor(self) -> Any:
            class _Cur:
                description = None

                def execute(self, *_: Any) -> None:
                    b.block()

                def close(self) -> None:
                    pass

            return _Cur()

        def commit(self) -> None:
            pass

    Connection.__module__ = module
    return Connection()


@pytest.mark.parametrize("module", ["pymysql.connections", "MySQLdb.connections"])
def test_sqlalchemy_mysql_kills_the_query_thread_on_a_side_connection(module) -> None:
    from provisa.core.connection_loop import connection_loop
    from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime

    b = _Blocker()
    killed: list[str] = []
    side_closed: list[bool] = []

    class _Side:
        def cursor(self) -> Any:
            class _C:
                def execute(self, sql: str) -> None:
                    killed.append(sql)
                    b.cancel()

                def close(self) -> None:
                    pass

            return _C()

        def close(self) -> None:
            side_closed.append(True)

    dbapi = _fake_mysql_conn(module, 4242, b)
    proxied = type(
        "_Proxied",
        (),
        {"cursor": lambda self: dbapi.cursor(), "commit": lambda self: None},
    )()
    proxied.dbapi_connection = dbapi  # type: ignore[attr-defined]
    rt = SqlAlchemyFederationRuntime.__new__(SqlAlchemyFederationRuntime)
    rt._con = proxied  # type: ignore[attr-defined]
    rt._open_side_conn = lambda: _Side()  # type: ignore[method-assign]

    t0 = time.monotonic()
    with connection_loop() as cl, pytest.raises(TimeoutError):
        cl.run(rt.run("SELECT SLEEP(10)"), timeout=_BUDGET)
    assert killed == ["KILL QUERY 4242"]
    assert side_closed == [True]
    assert time.monotonic() - t0 < _BLOCK / 2


def test_sqlalchemy_pyodbc_stream_registers_cursor_cancel_before_execute() -> None:
    from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime

    b = _Blocker()

    class Cursor:
        description = None

        def cancel(self) -> None:
            b.cancel()

        def execute(self, *_: Any) -> None:
            b.block()

        def close(self) -> None:
            pass

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

    Connection.__module__ = "pyodbc"
    dbapi = Connection()
    closed: list[bool] = []

    class _SaConn:
        connection = type("_PC", (), {"dbapi_connection": dbapi})()

        def execution_options(self, **_: Any) -> "_SaConn":
            return self

        def exec_driver_sql(self, *_: Any) -> Any:
            pytest.fail("pyodbc must stream on its own cursor, not through exec_driver_sql")

        def close(self) -> None:
            closed.append(True)

    rt = SqlAlchemyFederationRuntime.__new__(SqlAlchemyFederationRuntime)
    rt._sa = type("_Eng", (), {"connect": lambda self: _SaConn()})()  # type: ignore[attr-defined]
    _expect_deadline_cancel(lambda: rt.run_sync("SELECT 1"), b)
    assert closed == [True]  # the connection is released when setup fails


# -- Trino (DBAPI and Flight SQL) --------------------------------------------------------------


def test_trino_execute_cancels_its_cursor() -> None:
    from provisa.executor.trino import execute_trino

    b = _Blocker()

    class _Cur:
        description = None

        def cancel(self) -> None:
            b.cancel()

        def execute(self, sql: str, *_: Any) -> None:
            if sql == "SELECT x FROM t":
                b.block()

        def fetchone(self) -> tuple:
            return (1,)

        def fetchall(self) -> list:
            return []

    conn = type("_Conn", (), {"cursor": lambda self: _Cur()})()
    execute_trino(conn, "SELECT warm")  # type: ignore[arg-type]  # first call pays module imports
    _expect_deadline_cancel(lambda: execute_trino(conn, "SELECT x FROM t"), b)  # type: ignore[arg-type]


def test_trino_flight_arrow_cancels_its_adbc_cursor() -> None:
    from provisa.executor.trino_flight import execute_trino_flight_arrow

    b = _Blocker()

    class _Cur:
        def adbc_cancel(self) -> None:
            b.cancel()

        def execute(self, *_: Any) -> None:
            b.block()

        def close(self) -> None:
            pass

    conn = type("_Conn", (), {"cursor": lambda self: _Cur()})()
    _expect_deadline_cancel(lambda: execute_trino_flight_arrow(conn, "SELECT 1"), b)
