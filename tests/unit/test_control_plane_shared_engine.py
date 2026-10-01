# Copyright (c) 2026 Kenneth Stott
# Canary: 5a8e2c71-3f9d-4b6a-9e10-7c4d2b8f1a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The control-plane store is ONE shared, synchronous engine per worker (REQ-828 amended by
REQ-1882): request threads share its pool, the extra borrower waits (bounded by the request
budget), statements are cancelled at the request deadline, and LISTEN/NOTIFY is delivered by a
dedicated listener thread."""

# Requirements: REQ-828, REQ-1882

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from types import SimpleNamespace

import pytest

from provisa.core import request_deadline
from provisa.core.connection_loop import connection_loop
from provisa.core.database import Database, _PgListener, _translate, create_engine_from_url


def _file_db(tmp_path, *, pool_size: int = 1, max_overflow: int = 0) -> Database:
    engine = create_engine_from_url(
        f"sqlite:///{tmp_path / 'cp.db'}", pool_size=pool_size, max_overflow=max_overflow
    )
    return Database(engine, name="cp")


def _on_request_thread(fn):
    """Run ``fn(cl)`` on a fresh thread with its own connection loop; return its result."""
    out: dict = {}

    def _target() -> None:
        try:
            with connection_loop() as cl:
                out["value"] = fn(cl)
        except BaseException as exc:  # handed back to the test thread, which re-raises it
            out["error"] = exc

    t = threading.Thread(target=_target)
    t.start()
    t.join(30)
    assert not t.is_alive()
    if "error" in out:
        raise out["error"]
    return out["value"]


def test_request_threads_share_one_engine_and_pool(tmp_path) -> None:
    db = _file_db(tmp_path, pool_size=2)
    seen: list[tuple[int, int, int]] = []
    both_inside = threading.Barrier(2)

    async def _query() -> None:
        async with db.acquire() as conn:
            await conn.fetchval("SELECT 1")
            seen.append((threading.get_ident(), id(db.engine), id(db.engine.pool)))
            both_inside.wait(10)  # both requests hold a pooled connection at the same moment

    try:
        threads = [
            threading.Thread(target=lambda: _on_request_thread(lambda cl: cl.run(_query())))
            for _ in range(2)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        assert len({t for t, _, _ in seen}) == 2  # two concurrent request threads
        assert len({(e, p) for _, e, p in seen}) == 1  # one engine, one pool
        assert db.get_size() == 2  # both pooled connections came from that one pool
    finally:
        asyncio.run(db.close())


def test_the_extra_borrower_waits_then_succeeds(tmp_path) -> None:
    db = _file_db(tmp_path, pool_size=1)
    holding = threading.Event()
    release = threading.Event()

    async def _hold() -> None:
        async with db.acquire() as conn:
            await conn.fetchval("SELECT 1")
            holding.set()
            release.wait(10)

    async def _borrow() -> int:
        async with db.acquire() as conn:
            return await conn.fetchval("SELECT 7")

    holder = threading.Thread(target=lambda: _on_request_thread(lambda cl: cl.run(_hold())))
    holder.start()
    try:
        assert holding.wait(10)
        result: dict = {}
        borrower = threading.Thread(
            target=lambda: result.update(v=_on_request_thread(lambda cl: cl.run(_borrow())))
        )
        borrower.start()
        borrower.join(0.3)
        assert borrower.is_alive() and not result  # waiting for the one pooled connection
        release.set()
        borrower.join(10)
        assert result == {"v": 7}  # got it once freed — never "pool exhausted"
    finally:
        release.set()
        holder.join(10)
        asyncio.run(db.close())


def test_the_wait_is_bounded_by_the_request_budget(tmp_path) -> None:
    db = _file_db(tmp_path, pool_size=1)
    holding = threading.Event()
    release = threading.Event()

    async def _hold() -> None:
        async with db.acquire() as conn:
            await conn.fetchval("SELECT 1")
            holding.set()
            release.wait(10)

    async def _borrow() -> None:
        async with db.acquire():
            pytest.fail("acquired a connection the holder still has")

    holder = threading.Thread(target=lambda: _on_request_thread(lambda cl: cl.run(_hold())))
    holder.start()
    try:
        assert holding.wait(10)
        t0 = time.monotonic()
        with pytest.raises(TimeoutError):
            _on_request_thread(lambda cl: cl.run(_borrow(), timeout=0.3))
        assert time.monotonic() - t0 < 5.0  # the 30s pool cap did not apply
    finally:
        release.set()
        holder.join(10)
        asyncio.run(db.close())


def test_a_statement_is_cancelled_at_the_request_deadline(tmp_path) -> None:
    """A real blocking SQLite statement (no await while it runs) is interrupted by the deadline
    watchdog — an asyncio timer alone could never fire here."""
    db = _file_db(tmp_path)
    slow = (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 500000000) "
        "SELECT count(*) FROM c"
    )

    async def _request() -> None:
        async with db.acquire() as conn:
            await conn.fetchval(slow)

    t0 = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            _on_request_thread(lambda cl: cl.run(_request(), timeout=0.3))
        assert time.monotonic() - t0 < 5.0
    finally:
        asyncio.run(db.close())


def test_postgres_dict_and_list_params_are_sent_as_json() -> None:
    """asyncpg's jsonb codec JSON-encoded dict/list params; psycopg would dump a list as ARRAY.
    Every control-plane column such a value targets is jsonb, so they are wrapped as JSONB."""
    from psycopg.types.json import Jsonb

    _sql, params = _translate(
        "UPDATE t SET a = $1, b = $2, c = $3", ({"k": 1}, [1, 2], "x"), "postgresql"
    )
    assert isinstance(params["p1"], Jsonb) and params["p1"].obj == {"k": 1}
    assert isinstance(params["p2"], Jsonb) and params["p2"].obj == [1, 2]
    assert params["p3"] == "x"
    _sql, params = _translate("UPDATE t SET a = $1", ([1, 2],), "sqlite")
    assert params["p1"] == [1, 2]  # only PostgreSQL gets the jsonb semantics


# --------------------------------------------------------------------------- #
# LISTEN/NOTIFY via the listener thread
# --------------------------------------------------------------------------- #
class _FakePgConn:
    """A psycopg 3-shaped notify connection: select()-able on fileno(), and notifies(timeout=0)
    drains what arrived."""

    def __init__(self) -> None:
        self._r, self._w = os.pipe()
        self.autocommit = False
        self.executed: list[str] = []
        self._pending: list = []
        self._lock = threading.Lock()

    def fileno(self) -> int:
        return self._r

    def cursor(self):
        conn = self

        class _Cur:
            def execute(self, sql: str) -> None:
                conn.executed.append(sql)

            def close(self) -> None:
                pass

        return _Cur()

    def notify(self, channel: str, payload: str) -> None:
        with self._lock:
            self._pending.append(SimpleNamespace(channel=channel, pid=42, payload=payload))
        os.write(self._w, b"n")

    def notifies(self, *, timeout: float | None = None):
        assert timeout == 0  # the listener only drains what select() reported readable
        os.read(self._r, 4096)
        with self._lock:
            arrived, self._pending = self._pending, []
        yield from arrived

    def close(self) -> None:
        os.close(self._r)
        os.close(self._w)


def _fake_engine(conn: _FakePgConn):
    dialect = SimpleNamespace(
        create_connect_args=lambda url: ((), {}),
        loaded_dbapi=SimpleNamespace(connect=lambda *a, **k: conn),
    )
    return SimpleNamespace(dialect=dialect, url=None)


async def test_notify_is_delivered_on_the_registering_loop_by_the_listener_thread() -> None:
    conn = _FakePgConn()
    listener = _PgListener(_fake_engine(conn))  # type: ignore[arg-type]
    got: asyncio.Queue = asyncio.Queue()
    loop_thread = threading.get_ident()
    delivered_on: list[int] = []

    def _cb(owner, pid, channel, payload) -> None:
        delivered_on.append(threading.get_ident())
        got.put_nowait((owner, pid, channel, payload))

    owner = object()
    try:
        listener.subscribe("provisa_orders", _cb, owner, asyncio.get_running_loop())
        assert conn.executed == ['LISTEN "provisa_orders"']  # in effect before subscribe returns
        conn.notify("provisa_orders", '{"op":"INSERT"}')
        item = await asyncio.wait_for(got.get(), 5)
        assert item == (owner, 42, "provisa_orders", '{"op":"INSERT"}')
        assert delivered_on == [loop_thread]  # handed to the registering loop, not run off-thread

        listener.unsubscribe("provisa_orders", _cb)
        assert conn.executed[-1] == 'UNLISTEN "provisa_orders"'
        with pytest.raises(KeyError):
            listener.unsubscribe("provisa_orders", _cb)
    finally:
        listener.close()


def test_listen_is_refused_on_a_backend_without_notify(tmp_path) -> None:
    db = _file_db(tmp_path)

    async def _listen() -> None:
        await db.add_listener("ch", lambda *a: None)

    try:
        with pytest.raises(NotImplementedError, match="sqlite"):
            asyncio.run(_listen())
    finally:
        asyncio.run(db.close())


def test_deadline_bound_is_not_applied_outside_a_request(tmp_path) -> None:
    """Startup/background work runs with no request budget bound."""
    assert request_deadline.remaining() is None
    db = _file_db(tmp_path)

    async def _q() -> int:
        async with db.acquire() as conn:
            return await conn.fetchval("SELECT 3")

    try:
        assert asyncio.run(_q()) == 3
    finally:
        asyncio.run(db.close())


async def test_a_bound_method_listener_unsubscribes() -> None:
    """A bound-method callback is a new object on every attribute access (EventTriggerManager
    passes ``self._on_notify`` to add and to remove), so removal matches by equality."""
    conn = _FakePgConn()
    listener = _PgListener(_fake_engine(conn))  # type: ignore[arg-type]

    class _Mgr:
        def on_notify(self, owner, pid, channel, payload) -> None:
            pass

    mgr = _Mgr()
    try:
        listener.subscribe("ch", mgr.on_notify, object(), asyncio.get_running_loop())
        listener.unsubscribe("ch", mgr.on_notify)
        assert conn.executed == ['LISTEN "ch"', 'UNLISTEN "ch"']
    finally:
        listener.close()


def test_copy_fields_keep_null_distinct_from_empty_and_render_postgres_text() -> None:
    import datetime

    from psycopg import Binary
    from psycopg.types.json import Json, Jsonb

    from provisa.core.database import _copy_text

    assert _copy_text(True) == "t" and _copy_text(False) == "f"
    assert _copy_text(b"\x00\xff") == "\\x00ff"
    assert _copy_text(Binary(b"\x01")) == "\\x01"  # LargeBinary's DBAPI wrapper is unwrapped
    assert _copy_text(["a", 'b"c', None, ["x"]]) == '{"a","b\\"c",NULL,{"x"}}'
    assert _copy_text(datetime.date(2025, 1, 2)) == "2025-01-02"
    assert _copy_text({"k": 1}) == '{"k": 1}'
    # The psycopg dialect's JSON/JSONB bind processors wrap values; COPY gets the JSON text.
    assert _copy_text(Jsonb({"k": 1}, dumps=json.dumps)) == '{"k": 1}'
    assert _copy_text(Json([1, 2], dumps=json.dumps)) == "[1, 2]"
