# Copyright (c) 2026 Kenneth Stott
# Canary: c7681576-6346-413b-b2f2-635192cee98f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1190: a blocking DB-API driver's DIRECT read is handed back a bounded batch at a time,
and its pooled connection is given back or discarded exactly once, however the read ends."""

from __future__ import annotations

import pytest

from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.cursor_stream import PooledCursorStream, fetch_chunked


class _Cursor:
    def __init__(self, rows, *, description=(("id",),), fail_after=None):
        self._rows = list(rows)
        self.description = description
        self.fetches: list[int] = []
        self.closed = False
        self.cancelled = False
        self._fail_after = fail_after

    def execute(self, *_):
        pass

    def fetchmany(self, size):
        if self._fail_after is not None and len(self.fetches) >= self._fail_after:
            raise RuntimeError("connection lost")
        self.fetches.append(size)
        out, self._rows = self._rows[:size], self._rows[size:]
        return out

    def close(self):
        self.closed = True


class _Conn:
    def __init__(self, cursor):
        self.cursor_ = cursor
        self.closed = False
        self.finished = 0


def _pool(cursor):
    conns: list[_Conn] = []

    def create():
        conn = _Conn(cursor)
        conns.append(conn)
        return conn

    def close(conn):
        conn.closed = True

    return BlockingPool(create, close, minsize=0, maxsize=1, wait_s=1, name="t"), conns


def _stream(pool, cursor, **kw):
    return PooledCursorStream(
        pool,
        open_cursor=lambda conn: conn.cursor_,
        execute=lambda cur: cur.execute(),
        columns=lambda cur: [d[0] for d in cur.description],
        cancel=lambda conn, cur: setattr(cur, "cancelled", True),
        finish=lambda conn: setattr(conn, "finished", conn.finished + 1),
        **kw,
    )


async def test_a_read_to_its_end_comes_back_in_batches_and_returns_the_connection():
    cursor = _Cursor([(i,) for i in range(7)])
    pool, conns = _pool(cursor)
    stream = _stream(pool, cursor)
    assert stream.column_names == ["id"]
    batches = []
    while rows := await stream.fetch(3):
        batches.append(rows)
    assert batches == [[(0,), (1,), (2,)], [(3,), (4,), (5,)], [(6,)]]
    assert cursor.fetches == [3, 3, 3, 3] and cursor.closed and not cursor.cancelled
    assert conns[0].finished == 1 and not conns[0].closed
    assert pool.getconn() is conns[0]  # back in the pool, reusable
    await stream.close()  # already released: nothing more happens
    assert not conns[0].closed


async def test_a_stream_closed_early_cancels_its_statement_and_discards_the_connection():
    cursor = _Cursor([(i,) for i in range(100)])
    pool, conns = _pool(cursor)
    stream = _stream(pool, cursor)
    assert await stream.fetch(10) == [(i,) for i in range(10)]
    await stream.close()
    assert cursor.cancelled and conns[0].closed and conns[0].finished == 0
    assert pool.getconn() is not conns[0]  # its slot is free; a new connection is made


async def test_a_failed_fetch_discards_the_connection_and_raises():
    cursor = _Cursor([(i,) for i in range(100)], fail_after=1)
    pool, conns = _pool(cursor)
    stream = _stream(pool, cursor)
    await stream.fetch(10)
    with pytest.raises(RuntimeError, match="connection lost"):
        await stream.fetch(10)
    assert conns[0].closed
    assert await stream.fetch(10) == []


async def test_a_statement_with_no_result_releases_at_open():
    cursor = _Cursor([], description=None)
    pool, conns = _pool(cursor)
    stream = _stream(pool, cursor)
    assert stream.column_names == [] and await stream.fetch(10) == []
    assert conns[0].finished == 1 and not conns[0].closed


async def test_a_failure_at_open_frees_the_slot():
    cursor = _Cursor([(1,)])
    pool, conns = _pool(cursor)

    def boom(cur):
        raise RuntimeError("bad statement")

    with pytest.raises(RuntimeError, match="bad statement"):
        PooledCursorStream(
            pool,
            open_cursor=lambda conn: conn.cursor_,
            execute=boom,
            columns=lambda cur: [],
            cancel=lambda conn, cur: setattr(cur, "cancelled", True),
        )
    assert conns[0].closed
    assert pool.getconn() is not conns[0]


def test_a_materialized_read_is_fetched_in_chunks():
    from provisa.executor.drivers import cursor_stream

    cursor = _Cursor([(i,) for i in range(5)])
    old = cursor_stream.FETCH_CHUNK_ROWS
    cursor_stream.FETCH_CHUNK_ROWS = 2
    try:
        assert fetch_chunked(cursor) == [(i,) for i in range(5)]
    finally:
        cursor_stream.FETCH_CHUNK_ROWS = old
    assert cursor.fetches == [2, 2, 2, 2]


async def test_a_driver_that_buffers_by_a_cursor_setting_is_set_to_each_batch_asked_for():
    cursor = _Cursor([(i,) for i in range(5)])
    cursor.arraysize = 100
    pool, _conns = _pool(cursor)
    seen = []

    def per_fetch(cur, size):
        cur.arraysize = size
        seen.append(size)

    stream = _stream(pool, cursor, per_fetch=per_fetch)
    await stream.fetch(2)
    assert cursor.arraysize == 2 and seen == [2]
