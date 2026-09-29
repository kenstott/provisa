# Copyright (c) 2026 Kenneth Stott
# Canary: 8b1f2b5a-6d7e-4c3a-9a2e-2f6d1c9b7a4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``PgFederationRuntime.run_sync`` borrows/returns pooled connections instead of opening a fresh
``psycopg2.connect`` per call (REQ-1895). ``run`` (the async variant) does the same, instead of
sharing ``self._con`` with ``attach_source`` (REQ-1906). Exercises the pool against a fake
psycopg2 connection — no live Postgres needed — so this runs in the unit tier, not integration."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from provisa.federation.pg_runtime import PgFederationRuntime, _POOL_MAXCONN, _POOL_MINCONN


class _FakeCursor:
    def __init__(self, con: "_FakeConnection", name: str | None = None) -> None:
        self.con = con
        self.name = name
        self.description: list[Any] | None = None
        self.itersize = 1
        self._rows: list[Any] = []
        self.closed = False

    def execute(self, sql: str, params: Any = None) -> None:
        self.con.executed.append(sql)
        if self.con.raise_on_execute:
            raise self.con.raise_on_execute
        self.description = [("id",)]
        self._rows = [(1,), (2,)]

    def fetchmany(self, n: int) -> list[Any]:
        rows, self._rows = self._rows, []
        return rows

    def fetchall(self) -> list[Any]:
        rows, self._rows = self._rows, []
        return rows

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.autocommit = False
        self.closed = False
        self.executed: list[str] = []
        self.raise_on_execute: Exception | None = None
        self.committed = False
        self.info = type("Info", (), {"transaction_status": 0})()

    def cursor(self, name: str | None = None) -> _FakeCursor:
        return _FakeCursor(self, name)

    def commit(self) -> None:
        self.committed = True

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_psycopg2(monkeypatch):
    made: list[_FakeConnection] = []

    def _connect(dsn: str, *_, **__) -> _FakeConnection:
        con = _FakeConnection(dsn)
        made.append(con)
        return con

    import provisa.federation.pg_runtime as pg_runtime_mod

    monkeypatch.setattr(pg_runtime_mod.psycopg2, "connect", _connect)
    monkeypatch.setattr(pg_runtime_mod.psycopg2.pool.AbstractConnectionPool, "_connect", None)
    # psycopg2.pool.AbstractConnectionPool._connect calls the real psycopg2.connect internally
    # (module-level, not through our patched attribute lookup on psycopg2), so patch it directly
    # at the pool implementation instead of relying on the psycopg2.connect monkeypatch above.

    def _pool_connect(self, key=None):
        conn = _connect(*self._args, **self._kwargs)
        if key is not None:
            self._used[key] = conn
            self._rused[id(conn)] = key
        else:
            self._pool.append(conn)
        return conn

    monkeypatch.setattr(
        pg_runtime_mod.psycopg2.pool.AbstractConnectionPool, "_connect", _pool_connect
    )
    return made


def _runtime(fake_psycopg2) -> PgFederationRuntime:
    return PgFederationRuntime(engine_dsn="postgresql://fake/db")


def test_pool_bounds_use_module_constants() -> None:
    assert _POOL_MINCONN == 1
    assert _POOL_MAXCONN == 10


def test_run_sync_reuses_pooled_connection_across_calls(fake_psycopg2) -> None:
    rt = _runtime(fake_psycopg2)
    # __init__ opens: 1 direct self._con + _POOL_MINCONN pooled connections.
    made_after_init = len(fake_psycopg2)

    res1 = rt.run_sync("SELECT id FROM t")
    _ = res1.rows  # drain to trigger on_close -> putconn
    res2 = rt.run_sync("SELECT id FROM t")
    _ = res2.rows

    # No NEW connections were opened for either run_sync call: the pool's already-created
    # connection was borrowed and returned both times.
    assert len(fake_psycopg2) == made_after_init


def test_run_sync_never_calls_psycopg2_connect_directly(fake_psycopg2, monkeypatch) -> None:
    import provisa.federation.pg_runtime as pg_runtime_mod

    rt = _runtime(fake_psycopg2)

    calls: list[str] = []
    real_connect = pg_runtime_mod.psycopg2.connect

    def _tracking_connect(dsn: str, *a, **kw):
        calls.append(dsn)
        return real_connect(dsn, *a, **kw)

    monkeypatch.setattr(pg_runtime_mod.psycopg2, "connect", _tracking_connect)

    res = rt.run_sync("SELECT id FROM t")
    _ = res.rows

    assert calls == []  # run_sync borrowed from the pool, never called psycopg2.connect itself


def test_run_sync_discards_connection_on_setup_failure(fake_psycopg2) -> None:
    rt = _runtime(fake_psycopg2)
    made_after_init = len(fake_psycopg2)

    # Force the borrowed connection's cursor.execute to raise.
    borrowed = rt._read_pool.getconn()
    borrowed.raise_on_execute = RuntimeError("boom")
    rt._read_pool.putconn(borrowed)

    with pytest.raises(RuntimeError, match="boom"):
        rt.run_sync("SELECT id FROM t")

    # The broken connection was discarded (closed), not returned for reuse.
    assert borrowed.closed

    # Pool replenishes with a fresh connection on the next successful borrow.
    res = rt.run_sync("SELECT id FROM t")
    _ = res.rows
    assert len(fake_psycopg2) == made_after_init + 1


def test_close_closes_read_pool(fake_psycopg2) -> None:
    rt = _runtime(fake_psycopg2)
    pooled = list(rt._read_pool._pool)  # type: ignore[attr-defined]
    assert pooled  # min=1 warm connection exists

    rt.close()

    assert all(c.closed for c in pooled)


def test_run_borrows_from_pool_not_self_con(fake_psycopg2) -> None:
    # REQ-1906: run() must never touch self._con -- attach_source's DDL/ANALYZE connection --
    # since a run() call that outlives its caller's timeout keeps executing on whatever
    # connection it borrowed, and self._con is shared with attach_source.
    rt = _runtime(fake_psycopg2)
    self_con: _FakeConnection = rt._con  # type: ignore[assignment]

    res = asyncio.run(rt.run("SELECT id FROM t"))

    assert res.rows == [(1,), (2,)]
    assert self_con.executed == []  # nothing ran on the shared attach connection


def test_run_reuses_pooled_connection_across_calls(fake_psycopg2) -> None:
    rt = _runtime(fake_psycopg2)
    made_after_init = len(fake_psycopg2)

    asyncio.run(rt.run("SELECT id FROM t"))
    asyncio.run(rt.run("SELECT id FROM t"))

    # No NEW connections were opened for either run() call: the pool's already-created
    # connection was borrowed and returned both times.
    assert len(fake_psycopg2) == made_after_init


def test_run_discards_connection_on_failure(fake_psycopg2) -> None:
    rt = _runtime(fake_psycopg2)
    made_after_init = len(fake_psycopg2)

    borrowed = rt._read_pool.getconn()
    borrowed.raise_on_execute = RuntimeError("boom")
    rt._read_pool.putconn(borrowed)

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(rt.run("SELECT id FROM t"))

    assert borrowed.closed  # the broken connection was discarded, not returned for reuse

    res = asyncio.run(rt.run("SELECT id FROM t"))
    assert res.rows == [(1,), (2,)]
    assert len(fake_psycopg2) == made_after_init + 1
