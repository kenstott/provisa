# Copyright (c) 2026 Kenneth Stott
# Canary: 4f0a9c2e-7b1d-4e58-a3c6-91d2e8f05b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two forms on DuckDB at an integer's greatest value (REQ-2236, amended 2026-11-15, AN EDGE CASE
RAISES; IT DOES NOT REFUSE THE FORM): each answers or raises what PostgreSQL 17.11 does, at once.

- polygon(n, circle) built all n points before looking at n: polygon(2147483647, circle) ran
  for good and grew the server's memory until the Conformance (functions) DuckDB leg was ended
  (run 43776890390). PostgreSQL's circle_poly raises before it allocates.
- bit(bit, n, explicit) padded the value to n bits: bit('1', 2147483647, true) built a 2^31-bit
  value whose write lost the connection. PostgreSQL leaves the value as it is for a length past
  VARBITMAXLEN (2147483640).

Each statement runs under a 256MB memory limit and is interrupted after QUICK_S seconds, so a form
that still builds the value fails here rather than hanging the test run or taking the machine.

The answers were recorded on postgres:17.11 (geo_ops.c circle_poly, varbit.c bit)."""

# Requirements: REQ-2236

from __future__ import annotations

import threading
import time

import duckdb
import pytest

from provisa.core.pg_error import from_engine
from provisa.transpiler.transpile import transpile_to_duckdb

#: Seconds a statement here may take: PostgreSQL answers each in under a millisecond.
QUICK_S = 5.0
UNIT = "'<(0,0),1>'::circle"


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("SET memory_limit = '256MB'")
    return c


def _run(con: duckdb.DuckDBPyConnection, sql: str) -> object:
    started = time.monotonic()
    timer = threading.Timer(QUICK_S, con.interrupt)
    timer.start()
    try:
        row = con.execute(transpile_to_duckdb(sql)).fetchone()
        assert row is not None
        return row[0]
    finally:
        timer.cancel()
        assert time.monotonic() - started < QUICK_S, f"{sql} took over {QUICK_S}s"


def _raised(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[str, str]:
    with pytest.raises(duckdb.Error) as caught:
        _run(con, sql)
    error = from_engine(caught.value)
    assert error is not None, caught.value
    return error.sqlstate, str(error)


@pytest.mark.parametrize(
    "n, sqlstate, message",
    [
        (2147483647, "54000", "too many points requested"),
        (1000000000, "54000", "too many points requested"),
        (134217726, "54000", "too many points requested"),
        (134217725, "XX000", "invalid memory alloc request size 2147483640"),
        (67108862, "XX000", "invalid memory alloc request size 1073741832"),
    ],
)
def test_too_many_points_raise_as_postgresql_does(
    con: duckdb.DuckDBPyConnection, n: int, sqlstate: str, message: str
) -> None:
    got = _raised(con, f"SELECT pg_catalog.\"polygon\"('{n}'::integer, {UNIT})")
    assert got[0] == sqlstate
    assert message in got[1]


@pytest.mark.parametrize(
    "sql, sqlstate, message",
    [
        (
            "SELECT pg_catalog.\"polygon\"('2147483647'::integer, '<(0,0),0>'::circle)",
            "0A000",
            "cannot convert circle with radius zero to polygon",
        ),
        (
            f"SELECT pg_catalog.\"polygon\"('-2147483648'::integer, {UNIT})",
            "22023",
            "must request at least 2 points",
        ),
    ],
)
def test_postgresqls_earlier_checks_come_first(
    con: duckdb.DuckDBPyConnection, sql: str, sqlstate: str, message: str
) -> None:
    got = _raised(con, sql)
    assert got[0] == sqlstate
    assert message in got[1]


def test_a_few_points_are_still_answered(con: duckdb.DuckDBPyConnection) -> None:
    assert _run(con, "SELECT pg_catalog.\"polygon\"('2'::integer, '<(1,2),3>'::circle)") == (
        "((-2,2),(4,2.0000000000000004))"
    )


def test_the_connection_answers_after_too_many_points(con: duckdb.DuckDBPyConnection) -> None:
    _raised(con, f"SELECT pg_catalog.\"polygon\"('2147483647'::integer, {UNIT})")
    assert con.execute("SELECT 1").fetchone() == (1,)


@pytest.mark.parametrize(
    "value, length, explicit, want",
    [
        ("1", 2147483647, "true", "1"),
        ("1", 2147483647, "false", "1"),
        ("1", 2147483641, "false", "1"),
        ("0", 2147483647, "true", "0"),
        ("101", -2147483648, "true", "101"),
        ("101", 2, "true", "10"),
    ],
)
def test_a_length_past_varbitmaxlen_leaves_the_value(
    con: duckdb.DuckDBPyConnection, value: str, length: int, explicit: str, want: str
) -> None:
    sql = f"SELECT pg_catalog.\"bit\"('{value}'::bit, '{length}'::integer, {explicit}::boolean)"
    assert _run(con, sql) == want


def test_varbitmaxlen_itself_is_a_length(con: duckdb.DuckDBPyConnection) -> None:
    got = _raised(con, "SELECT pg_catalog.\"bit\"('1'::bit, '2147483640'::integer, false::boolean)")
    assert got[0] == "22026"
    assert "bit string length 1 does not match type bit(2147483640)" in got[1]


@pytest.mark.parametrize(
    "sql, want",
    [
        ("SELECT CAST(pg_catalog.date_mii('infinity'::date, 3) AS text)", "infinity"),
        ("SELECT CAST(pg_catalog.date_pli('infinity'::date, -3) AS text)", "infinity"),
        ("SELECT CAST(pg_catalog.date_mii('-infinity'::date, -3) AS text)", "-infinity"),
        ("SELECT CAST(pg_catalog.date_pli('2000-01-01'::date, 3) AS text)", "2000-01-04"),
    ],
)
def test_an_infinite_date_moves_nowhere(
    con: duckdb.DuckDBPyConnection, sql: str, want: str
) -> None:
    """date_pli and date_mii answer an infinite date as it is (date.c: "can't change infinity")."""
    assert _run(con, sql) == want
