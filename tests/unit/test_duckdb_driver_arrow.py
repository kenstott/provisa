# Copyright (c) 2026 Kenneth Stott
# Canary: c2e7a9d4-1b6f-4e38-8a05-9f3d7b1c6e20
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The DuckDB DIRECT driver reads through DuckDB's Arrow export, never its DB-API rows
(REQ-2194): a materialized read keeps the table, and both it and the stream hand a row consumer
the values DuckDB's own rows hold (duckdb_arrow.arrow_rows)."""

from __future__ import annotations

import asyncio
import uuid
from datetime import time, timedelta, timezone

import pyarrow as pa
import pytest
from dateutil.relativedelta import relativedelta

from provisa.core.ir_types import infinity_of
from provisa.executor.drivers.duckdb_driver import DuckDBDriver
from provisa.executor.result import ArrowResult

_VALUES_SQL = (
    "SELECT '7d9f1c2e-1111-2222-3333-444455556666'::UUID AS u, MAP {'k': 1} AS m, "
    "170141183460469231731687303715884105727::HUGEINT AS h, '01:02:03+05'::TIMETZ AS t, "
    "INTERVAL '1 year 2 months' AS i, 'infinity'::DATE AS d"
)


def _check_values(row: tuple) -> None:
    u, m, h, t, i, d = row
    assert u == uuid.UUID("7d9f1c2e-1111-2222-3333-444455556666")
    assert m == {"k": 1}
    assert h == 2**127 - 1
    assert t == time(1, 2, 3, tzinfo=timezone(timedelta(hours=5)))
    assert i == relativedelta(years=1, months=2)
    assert infinity_of(d) == 1


@pytest.fixture
def driver():
    d = DuckDBDriver()
    asyncio.run(d.connect("", 0, ":memory:", "", ""))
    yield d
    asyncio.run(d.close())


def test_a_read_is_duckdbs_arrow_table_with_its_rows(driver):
    res = asyncio.run(driver.execute("SELECT x, x * 2 AS y FROM range(3) t(x)"))
    assert isinstance(res, ArrowResult)
    assert isinstance(res.table, pa.Table)
    assert res.column_names == ["x", "y"]
    assert res.rows == [(0, 0), (1, 2), (2, 4)]


def test_a_read_holds_duckdbs_own_values(driver):
    [row] = asyncio.run(driver.execute(_VALUES_SQL)).rows
    _check_values(row)


def test_a_read_with_no_rows_keeps_its_columns(driver):
    res = asyncio.run(driver.execute("SELECT 1 AS a, 'x' AS b WHERE false"))
    assert res.column_names == ["a", "b"] and res.rows == []


def test_a_read_binds_its_values(driver):
    res = asyncio.run(driver.execute("SELECT $1::INTEGER + 1 AS n, $2 AS s", [41, "x"]))
    assert res.rows == [(42, "x")]


def test_a_stream_holds_duckdbs_own_values(driver):
    async def run():
        stream = await driver.open_stream(_VALUES_SQL)
        rows = await stream.fetch(10)
        assert await stream.fetch(10) == []
        return stream.column_names, rows

    columns, [row] = asyncio.run(run())
    assert columns == ["u", "m", "h", "t", "i", "d"]
    _check_values(row)


def test_a_stream_reads_past_one_arrow_batch(driver):
    from provisa.executor.drivers.cursor_stream import FETCH_CHUNK_ROWS

    n = FETCH_CHUNK_ROWS * 2 + 7

    async def run():
        stream = await driver.open_stream(f"SELECT range AS id FROM range({n})")
        sizes, last = [], None
        while batch := await stream.fetch(30_000):
            sizes.append(len(batch))
            last = batch[-1]
        return sizes, last

    sizes, last = asyncio.run(run())
    assert sum(sizes) == n and all(s == 30_000 for s in sizes[:-1])
    assert last == (n - 1,)
