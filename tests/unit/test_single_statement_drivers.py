# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8b1d57-3a9c-4f26-b7e0-9c2d5a1f8b63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Direct drivers whose client connection serves ONE statement at a time (REQ-1882).

databricks-sql-connector, pyodbc and impyla declare DB-API ``threadsafety = 1`` (threads may share
the module, not a connection); a pyexasol connection is one websocket carrying one request. Each of
these drivers held a single connection and ran every statement on it from a worker thread
(``asyncio.to_thread``), so two requests at once interleaved on one connection — the defect
confirmed on a real ClickHouse (``request N read []``). Every request runs on its own thread, so
the driver holds a bounded pool: one connection per statement, on the request's own thread, the
request deadline able to cancel it, a broken connection closed rather than pooled.

Unit level, against stand-ins for the client libraries: these are metered or heavyweight services
(Databricks, Fabric; Exasol and HiveServer2 containers)."""

# Requirements: REQ-1882, REQ-1905

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from provisa.core import request_deadline
from provisa.executor.drivers.databricks import DatabricksDriver
from provisa.executor.drivers.exasol import ExasolDriver
from provisa.executor.drivers.hive import HiveDriver
from provisa.executor.drivers.mssql_warehouse import MssqlWarehouseDriver


class _Library:
    """What the four client libraries have in common, as the drivers use them."""

    def __init__(self, broken: BaseException) -> None:
        self.broken = broken  # what the library raises when the connection is gone
        self.connections: list[_Connection] = []
        self.overlaps = 0
        self.threads: set[int] = set()
        self.block = False  # a statement runs until cancelled
        self.fail_with: BaseException | None = None
        self.cancelled = 0

    def connect(self, *args, **kwargs):
        conn = _Connection(self)
        self.connections.append(conn)
        return conn


class _Connection:
    def __init__(self, lib: _Library) -> None:
        self.lib = lib
        self.busy = threading.Lock()
        self.closed = False
        self._cancel = threading.Event()

    def _statement(self, sql: str):
        lib = self.lib
        lib.threads.add(threading.get_ident())
        if not self.busy.acquire(blocking=False):
            lib.overlaps += 1
            raise AssertionError("two statements on one connection at once")
        try:
            if lib.fail_with is not None:
                raise lib.fail_with
            if lib.block:
                if self._cancel.wait(2.0):
                    raise RuntimeError("statement cancelled")
            else:
                time.sleep(0.02)
            return [(sql,)]
        finally:
            self.busy.release()

    def _cancelled(self) -> None:
        self.lib.cancelled += 1
        self._cancel.set()

    # DB-API shape (databricks, pyodbc, impyla)
    def cursor(self):
        return _Cursor(self)

    # pyexasol shape
    def execute(self, sql: str):
        return _Statement(self._statement(sql))

    def abort_query(self) -> None:
        self._cancelled()

    def close(self) -> None:
        self.closed = True


class _Cursor:
    def __init__(self, conn: _Connection) -> None:
        self._conn = conn
        self.description = None
        self._rows: list[tuple] = []

    def execute(self, sql: str, params=None) -> None:
        self._rows = self._conn._statement(sql)
        self.description = [("statement",)]

    def fetchall(self):
        return self._rows

    def cancel(self) -> None:
        self._conn._cancelled()

    cancel_operation = cancel

    def close(self) -> None:
        pass


class _Statement:
    # pyexasol names a statement that returns rows by its result_type; these all do.
    result_type = "resultSet"

    def __init__(self, rows) -> None:
        self._rows = rows

    def column_names(self):
        return ["statement"]

    def fetchall(self):
        return self._rows


def _databricks():
    import databricks.sql.exc as exc

    lib = _Library(exc.RequestError("connection lost"))
    driver = DatabricksDriver()
    driver.configure({"http_path": "/sql/1.0/warehouses/x"})
    return lib, driver, patch("databricks.sql.connect", lib.connect)


def _mssql():
    import pyodbc

    lib = _Library(pyodbc.OperationalError("08S01", "connection lost"))
    driver = MssqlWarehouseDriver()
    driver._token = lambda: "token"  # type: ignore[method-assign]  # no Azure AD in a unit test
    return lib, driver, patch("pyodbc.connect", lib.connect)


def _hive():
    from thrift.transport.TTransport import TTransportException

    lib = _Library(TTransportException(message="connection lost"))
    return lib, HiveDriver(), patch("impala.dbapi.connect", lib.connect)


def _exasol():
    import pyexasol

    lib = _Library(pyexasol.ExaCommunicationError(None, "connection lost"))
    return lib, ExasolDriver(), patch("pyexasol.connect", lib.connect)


_DRIVERS = {"databricks": _databricks, "mssql_warehouse": _mssql, "hive": _hive, "exasol": _exasol}


@pytest.fixture(params=sorted(_DRIVERS))
def connected(request):
    lib, driver, library = _DRIVERS[request.param]()
    with library:
        asyncio.run(
            driver.connect(
                host="h", port=1, database="d", user="u", password="p", min_pool=1, max_pool=4
            )
        )
        yield lib, driver
        asyncio.run(driver.close())


def test_concurrent_requests_each_get_their_own_connection(connected):
    lib, driver = connected

    def _request(i: int):
        return asyncio.run(driver.execute(f"SELECT {i}")).rows

    with ThreadPoolExecutor(max_workers=8) as requests:
        answers = list(requests.map(_request, range(16)))
    assert answers == [[(f"SELECT {i}",)] for i in range(16)]  # each request read its own answer
    assert lib.overlaps == 0
    assert 1 < len(lib.connections) <= 4  # bounded by max_pool; the 5th request waited


def test_the_statement_runs_on_the_request_thread(connected):
    lib, driver = connected
    asyncio.run(driver.execute("SELECT 1"))
    assert lib.threads == {threading.get_ident()}


def test_the_request_deadline_cancels_the_statement(connected):
    lib, driver = connected
    lib.block = True

    async def _request():
        with request_deadline.within(0.2):
            return await driver.execute("SELECT pg_sleep(60)")

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        asyncio.run(_request())
    assert time.monotonic() - started < 1.5 and lib.cancelled == 1
    lib.block = False  # the connection it ran on is still good: reused
    assert asyncio.run(driver.execute("SELECT 2")).rows == [("SELECT 2",)]
    assert len(lib.connections) == 1


def test_a_broken_connection_is_closed_not_pooled(connected):
    lib, driver = connected
    lib.fail_with = lib.broken
    with pytest.raises(type(lib.broken)):
        asyncio.run(driver.execute("SELECT 1"))
    assert lib.connections[0].closed
    lib.fail_with = None
    assert asyncio.run(driver.execute("SELECT 2")).rows == [("SELECT 2",)]
    assert len(lib.connections) == 2 and not lib.connections[1].closed


def test_a_failed_statement_keeps_its_connection(connected):
    lib, driver = connected
    lib.fail_with = ValueError("syntax error")
    with pytest.raises(ValueError):
        asyncio.run(driver.execute("SELEC 1"))
    lib.fail_with = None
    asyncio.run(driver.execute("SELECT 2"))
    assert len(lib.connections) == 1 and not lib.connections[0].closed


def test_close_closes_every_pooled_connection(connected):
    lib, driver = connected
    asyncio.run(driver.execute("SELECT 1"))
    asyncio.run(driver.close())
    assert not driver.is_connected and all(c.closed for c in lib.connections)
