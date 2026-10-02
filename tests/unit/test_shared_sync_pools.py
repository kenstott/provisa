# Copyright (c) 2026 Kenneth Stott
# Canary: e94b2775-3c5c-4101-964c-9db37389433d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared, thread-safe source-driver pools (REQ-1882, amended 2026-09-29).

Every request runs on its own thread, so each source's pool is one shared resource per worker:
the (max+1)th concurrent borrower waits for a free connection instead of failing, and every
blocking statement is registered with the request deadline's cancel callback."""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from provisa.core import request_deadline
from provisa.core.sync_pool import BlockingPool, PoolTimeout


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn
        self.description: list[Any] | None = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def execute(self, sql: str, params: Any = None) -> None:
        self._conn.executed.append(sql)
        dl = request_deadline.current()
        # The cancel callback is registered for exactly the duration of the blocking call.
        self._conn.cancel_registered.append(dl is not None and bool(dl._cancels))
        self.description = [("id",)]

    def fetchall(self) -> list[tuple]:
        return [(1,)]

    def cancel(self) -> None:
        self._conn.cancelled += 1

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.cancel_registered: list[bool] = []
        self.cancelled = 0
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def cancel(self) -> None:
        self.cancelled += 1

    def thread_id(self) -> int:
        return 42

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


# --------------------------------------------------------------------------------------------- #
# BlockingPool — the pool every sync source driver uses
# --------------------------------------------------------------------------------------------- #


def _pool(maxsize: int, wait_s: float = 5.0) -> BlockingPool[_FakeConn]:
    return BlockingPool(
        _FakeConn, lambda c: c.close(), minsize=1, maxsize=maxsize, wait_s=wait_s, name="t"
    )


def test_extra_borrower_waits_then_succeeds() -> None:
    pool = _pool(2)
    held = [pool.getconn(), pool.getconn()]
    got: list[_FakeConn] = []
    t = threading.Thread(target=lambda: got.append(pool.getconn()))
    t.start()
    t.join(0.3)
    assert t.is_alive() and got == []  # waiting, not failed
    pool.putconn(held.pop())
    t.join(2)
    assert not t.is_alive() and len(got) == 1


def test_wait_is_bounded() -> None:
    pool = _pool(1, wait_s=0.2)
    pool.getconn()
    with pytest.raises(PoolTimeout, match="all 1 checked out"):
        pool.getconn()


def test_wait_is_bounded_by_request_deadline() -> None:
    pool = _pool(1, wait_s=30)
    pool.getconn()
    with request_deadline.within(0.2):
        with pytest.raises(PoolTimeout):
            pool.getconn()


def test_never_exceeds_maxsize_under_concurrency() -> None:
    pool = _pool(3)
    peak = 0
    live = 0
    lock = threading.Lock()

    def _work() -> None:
        nonlocal peak, live
        conn = pool.getconn()
        with lock:
            live += 1
            peak = max(peak, live)
        threading.Event().wait(0.02)
        with lock:
            live -= 1
        pool.putconn(conn)

    threads = [threading.Thread(target=_work) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert peak <= 3


def test_broken_connection_is_discarded_and_slot_freed() -> None:
    pool = _pool(1)
    with pytest.raises(RuntimeError):
        with pool.connection(is_broken=lambda e: True) as conn:
            raise RuntimeError("socket gone")
    assert conn.closed
    assert pool.getconn() is not conn  # a fresh connection fills the freed slot


# --------------------------------------------------------------------------------------------- #
# Drivers: one shared pool; each blocking execute registers the deadline's cancel
# --------------------------------------------------------------------------------------------- #


def _inject(driver: Any, conn: _FakeConn) -> None:
    driver._pool = BlockingPool(
        lambda: conn, lambda c: c.close(), minsize=1, maxsize=1, wait_s=5, name="t"
    )


def _run_within_deadline(coro: Any) -> Any:
    with request_deadline.within(30):
        return asyncio.run(coro)


def test_mysql_execute_registers_deadline_cancel() -> None:
    from provisa.executor.drivers.mysql import MySQLDriver

    drv = MySQLDriver()
    conn = _FakeConn()
    _inject(drv, conn)
    res = _run_within_deadline(drv.execute("SELECT id FROM t WHERE id = $1", [1]))
    assert res.rows == [(1,)]
    assert conn.executed == ["SELECT id FROM t WHERE id = %s"]
    assert conn.cancel_registered == [True]


def test_mysql_cancel_kills_the_running_query_from_a_second_connection() -> None:
    from provisa.executor.drivers.mysql import MySQLDriver

    killer = _FakeConn()
    drv = MySQLDriver()
    drv._connect = lambda: killer
    drv._kill_query(42)
    assert killer.executed == ["KILL QUERY 42"]
    assert killer.closed


def test_sqlserver_execute_registers_deadline_cancel() -> None:
    from provisa.executor.drivers.sqlserver import SQLServerDriver

    drv = SQLServerDriver()
    conn = _FakeConn()
    _inject(drv, conn)
    res = _run_within_deadline(drv.execute("SELECT id FROM t WHERE id = $1", [1]))
    assert res.rows == [(1,)]
    assert conn.executed == ["SELECT id FROM t WHERE id = ?"]
    assert conn.cancel_registered == [True]


def test_oracle_execute_registers_deadline_cancel() -> None:
    from provisa.executor.drivers.oracle import OracleDriver

    class _Desc:
        name = "ID"

    class _OraCursor(_FakeCursor):
        def execute(self, sql: str, params: Any = None) -> None:
            super().execute(sql, params)
            self.description = [_Desc()]

    class _OraConn(_FakeConn):
        def cursor(self) -> _FakeCursor:
            return _OraCursor(self)

    drv = OracleDriver()
    conn = _OraConn()
    _inject(drv, conn)
    res = _run_within_deadline(drv.execute("SELECT id FROM t WHERE id = $1", [1]))
    assert res.column_names == ["id"]
    assert conn.executed == ["SELECT id FROM t WHERE id = :1"]
    assert conn.cancel_registered == [True]


def test_postgresql_execute_registers_deadline_cancel() -> None:
    from provisa.executor.drivers import postgresql as pg

    class _Col:
        name = "id"
        type_code = 23

    class _PgCursor(_FakeCursor):
        def execute(self, sql: str, params: Any = None) -> None:
            super().execute(sql, params)
            self.description = [_Col()]
            self._left = [(1,)]

        def fetchmany(self, size: int) -> list[tuple]:
            chunk, self._left = self._left[:size], self._left[size:]
            return chunk

    class _PgConn(_FakeConn):
        def cursor(self, **_: Any) -> _FakeCursor:
            return _PgCursor(self)

    class _PgPool:
        """The psycopg_pool.ConnectionPool surface the driver borrows through."""

        def __init__(self, conn: _FakeConn) -> None:
            self._conn = conn
            self.timeouts: list[float | None] = []
            self.returned: list[object] = []

        def getconn(self, timeout: float | None = None):
            self.timeouts.append(timeout)
            return self._conn

        def putconn(self, conn) -> None:
            assert conn is self._conn
            self.returned.append(conn)

    drv = pg.PostgreSQLDriver()
    drv._typnames[23] = "int4"
    conn = _PgConn()
    pool = _PgPool(conn)
    drv._pool = pool  # type: ignore[assignment]  # the surface execute() uses
    res = _run_within_deadline(drv.execute("SELECT id FROM t WHERE n LIKE 'a%' AND id = $1", [1]))
    # The pool wait is bounded by the request's remaining budget (here <= 30s, under 10s default).
    assert pool.timeouts and pool.timeouts[0] is not None and pool.timeouts[0] <= 10.0
    assert pool.returned == [conn]  # and it went back to the pool
    assert res.rows == [(1,)]
    assert res.column_types == ["int4"]
    assert conn.executed == ["SELECT id FROM t WHERE n LIKE 'a%%' AND id = %(p1)s"]
    assert conn.cancel_registered == [True]


def test_request_threads_share_one_driver_pool() -> None:
    from provisa.executor.drivers.mysql import MySQLDriver

    drv = MySQLDriver()
    made: list[_FakeConn] = []

    def _create() -> _FakeConn:
        c = _FakeConn()
        made.append(c)
        return c

    drv._pool = BlockingPool(_create, lambda c: c.close(), minsize=1, maxsize=2, wait_s=5, name="t")
    barrier = threading.Barrier(4)

    def _req() -> None:
        barrier.wait(5)
        asyncio.run(drv.execute("SELECT 1"))

    threads = [threading.Thread(target=_req) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    # Four concurrent requests on four threads were served by at most maxsize=2 connections.
    assert 1 <= len(made) <= 2
    assert sum(len(c.executed) for c in made) == 4


def test_postgresql_ends_a_large_fetch_between_chunks_when_the_deadline_passes(monkeypatch) -> None:
    """Converting a large result is not one uninterruptible call: the driver fetches it in chunks
    and looks at the request's deadline between them (REQ-1905)."""
    import time as _time

    from provisa.executor.drivers import postgresql as pg

    monkeypatch.setattr(pg, "_FETCH_CHUNK_ROWS", 10)

    class _Col:
        name = "id"
        type_code = 23

    fetched: list[int] = []

    class _SlowCursor(_FakeCursor):
        def execute(self, sql: str, params: Any = None) -> None:
            super().execute(sql, params)
            self.description = [_Col()]

        def fetchmany(self, size: int) -> list[tuple]:
            fetched.append(size)
            _time.sleep(0.05)  # each chunk's conversion
            return [(n,) for n in range(size)]  # a result that never ends

    class _Conn(_FakeConn):
        def cursor(self, **_: Any) -> _FakeCursor:
            return _SlowCursor(self)

    class _Pool:
        def __init__(self, conn: _FakeConn) -> None:
            self._conn = conn
            self.returned = 0

        def getconn(self, timeout: float | None = None):
            return self._conn

        def putconn(self, conn) -> None:
            self.returned += 1

    drv = pg.PostgreSQLDriver()
    pool = _Pool(_Conn())
    drv._pool = pool  # type: ignore[assignment]

    async def _request():
        with request_deadline.within(0.2):
            return await drv.execute("SELECT id FROM t")

    started = _time.monotonic()
    with pytest.raises(TimeoutError):
        asyncio.run(_request())
    assert _time.monotonic() - started < 1.0
    assert 2 <= len(fetched) <= 8  # cut a chunk or so after 0.2 s, not after the whole result
    assert pool.returned == 1
