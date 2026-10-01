# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8b2e61-9a3c-4d75-b1e8-6c0d5a7f3e29
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The DuckDB direct driver gives each in-flight request its own connection (REQ-1882).

A DuckDB connection is not shareable between threads (the module declares threadsafety 1): it
holds ONE current result, so a second ``execute`` on it replaces the first request's result
before that request has fetched it. The driver held one connection for every request thread.
Each request now borrows its own cursor — DuckDB's per-thread connection onto the same database —
from a bounded pool. Real DuckDB, in process."""

# Requirements: REQ-1882, REQ-027

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from provisa.core import request_deadline
from provisa.core.connection_loop import connection_loop
from provisa.executor.drivers.duckdb_driver import DuckDBDriver


def _driver(max_pool: int = 4) -> DuckDBDriver:
    driver = DuckDBDriver()
    asyncio.run(driver.connect("", 0, ":memory:", "", "", min_pool=1, max_pool=max_pool))
    asyncio.run(driver.execute("CREATE TABLE t AS SELECT range AS n FROM range(200000)"))
    return driver


def test_concurrent_requests_each_read_their_own_result():
    driver = _driver()
    n, rounds = 8, 25
    start = threading.Barrier(n)
    errors: list[BaseException] = []

    def _request(i: int) -> None:
        try:
            start.wait(timeout=30)
            with connection_loop() as cl:
                for _ in range(rounds):
                    result = cl.run(
                        driver.execute(
                            "SELECT $1 AS me, count(*) AS c FROM t WHERE n % 8 = $1", [i]
                        )
                    )
                    assert result.column_names == ["me", "c"]
                    assert result.rows == [(i, 25000)], f"request {i} read {result.rows}"
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_request, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    asyncio.run(driver.close())
    assert not errors, errors[:2]


def test_every_connection_sees_the_same_database():
    """Per-request connections are cursors onto ONE database — an in-memory database included."""
    driver = _driver(max_pool=3)
    seen: list = []

    def _request() -> None:
        with connection_loop() as cl:
            seen.append(cl.run(driver.execute("SELECT count(*) FROM t")).rows)

    threads = [threading.Thread(target=_request) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    asyncio.run(driver.close())
    assert seen == [[(200000,)]] * 6


def test_a_statement_is_interrupted_at_the_request_deadline():
    driver = _driver()
    slow = "SELECT count(*) FROM t a, t b, t c WHERE a.n + b.n + c.n = -1"
    started = time.monotonic()
    with request_deadline.within(0.5), pytest.raises(TimeoutError):
        asyncio.run(driver.execute(slow))
    assert time.monotonic() - started < 10, "the statement ran on past its deadline"
    # the connection that was interrupted is still usable, or was replaced
    assert asyncio.run(driver.execute("SELECT 1")).rows == [(1,)]
    asyncio.run(driver.close())
    assert not driver.is_connected
