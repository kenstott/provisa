# Copyright (c) 2026 Kenneth Stott
# Canary: 5c8e1a7d-3f2b-4e9c-8a6d-2b7f0e4c9d13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PostgreSQL DIRECT source driver (psycopg 3) against a real Postgres: server-side prepared
statements are reused per connection (never under PgBouncer mode), a full pool makes the next
borrower wait, the request deadline cancels a running statement, value types match what the asyncpg
driver returned, and a stream is fetched in bounded batches."""

# Requirements: REQ-052, REQ-053, REQ-883, REQ-1190, REQ-1882

from __future__ import annotations

import asyncio
import ipaddress
import os
import threading
import time
import uuid
from decimal import Decimal

import pytest

from provisa.core import request_deadline
from provisa.executor.drivers.postgresql import PostgreSQLDriver

pytestmark = [pytest.mark.integration]

_PG = {
    "host": os.environ.get("PG_HOST", "localhost"),
    "port": int(os.environ.get("PG_PORT", "5432")),
    "database": os.environ.get("PG_DATABASE", "provisa"),
    "user": os.environ.get("PG_USER", "provisa"),
    "password": os.environ.get("PG_PASSWORD", "provisa"),
}


def _driver(*, use_pgbouncer: bool = False, max_pool: int = 1) -> PostgreSQLDriver:
    drv = PostgreSQLDriver(use_pgbouncer=use_pgbouncer)
    asyncio.run(drv.connect(min_pool=1, max_pool=max_pool, **_PG))
    return drv


def _run(coro):
    return asyncio.run(coro)


_MARKED = "SELECT $1::int + 1 AS n /* provisa-prepared-probe */"
_COUNT = (
    "SELECT count(*) FROM pg_prepared_statements "
    "WHERE statement LIKE '%provisa-prepared-probe%' AND statement NOT LIKE '%pg_prepared%'"
)


def test_a_repeated_statement_is_prepared_once_and_reused():
    drv = _driver(max_pool=1)  # one connection, so every call lands on the same session
    try:
        for i in range(3):
            assert _run(drv.execute(_MARKED, [i])).rows == [(i + 1,)]
        assert _run(drv.execute(_COUNT)).rows == [(1,)]
    finally:
        _run(drv.close())


def test_pgbouncer_mode_never_prepares():
    drv = _driver(use_pgbouncer=True, max_pool=1)
    try:
        for i in range(3):
            assert _run(drv.execute(_MARKED, [i])).rows == [(i + 1,)]
        assert _run(drv.execute(_COUNT)).rows == [(0,)]
    finally:
        _run(drv.close())


def test_the_extra_borrower_waits_then_succeeds():
    drv = _driver(max_pool=1)
    try:
        held = drv._require_pool().getconn()
        got: list[object] = []
        t = threading.Thread(target=lambda: got.append(_run(drv.execute("SELECT 1")).rows))
        t.start()
        t.join(0.5)
        assert t.is_alive() and got == []  # waiting, not failed
        drv._require_pool().putconn(held)
        t.join(10)
        assert got == [[(1,)]]
    finally:
        _run(drv.close())


def test_the_request_deadline_cancels_a_running_statement():
    drv = _driver(max_pool=1)
    try:
        t0 = time.monotonic()
        with request_deadline.within(0.5), pytest.raises(TimeoutError, match="0.5s budget"):
            _run(drv.execute("SELECT pg_sleep(5)"))
        assert time.monotonic() - t0 < 3.0
        assert _run(drv.execute("SELECT 2")).rows == [(2,)]  # the connection is reusable
    finally:
        _run(drv.close())


def test_value_types_match_the_asyncpg_driver():
    drv = _driver()
    try:
        res = _run(
            drv.execute(
                "SELECT '{\"a\": 1}'::json AS j, '{\"a\": 1}'::jsonb AS jb, "
                "'5f1c2c9e-6d0a-4b8e-9c3e-2a4b6d8f0a1c'::uuid AS u, '10.0.0.1'::inet AS i, "
                "'\\x0102'::bytea AS b, 12.34::numeric(18,2) AS d, 7::int4 AS n, 'x'::text AS t"
            )
        )
        j, jb, u, i, b, d, n, t = res.rows[0]
        assert j == '{"a": 1}' and isinstance(jb, str)
        assert u == uuid.UUID("5f1c2c9e-6d0a-4b8e-9c3e-2a4b6d8f0a1c")
        assert isinstance(i, (ipaddress.IPv4Address, ipaddress.IPv4Interface))
        assert b == b"\x01\x02" and isinstance(b, bytes)
        assert d == Decimal("12.34")
        assert (n, t) == (7, "x")
        assert res.column_types == [
            "json",
            "jsonb",
            "uuid",
            "inet",
            "bytea",
            "numeric",
            "int4",
            "text",
        ]
    finally:
        _run(drv.close())


def test_a_stream_is_fetched_in_bounded_batches():
    drv = _driver(max_pool=1)
    try:

        async def _drain() -> list[int]:
            stream = await drv.open_stream("SELECT g FROM generate_series(1, $1) AS g", [2500])
            sizes = []
            while batch := await stream.fetch(700):
                sizes.append(len(batch))
            await stream.close()
            return sizes

        sizes = _run(_drain())
        assert sum(sizes) == 2500
        assert sizes[0] == PostgreSQLDriver._FIRST_BATCH_ROWS and max(sizes) <= 1000
        assert _run(drv.execute("SELECT 3")).rows == [(3,)]  # the stream returned its connection
    finally:
        _run(drv.close())
