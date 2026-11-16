# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e5c21-9d4a-4f6e-8a10-2c5d9e7f4b38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""factorial on DuckDB at its edges (REQ-2236, amended 2026-11-15, AN EDGE CASE RAISES): past
32177, whose factorial numeric cannot hold, PostgreSQL raises 22003 "value overflows numeric
format" at once. The UDF raises the same, never computing the product: factorial of the bigint
maximum held the DuckDB server's GIL for good (J1367, D0)."""

# Requirements: REQ-2236

from __future__ import annotations

import math
import sys

import duckdb
import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.duckdb_pg_scalars import numeric_factorial, register

BIGINT_MAX = 9223372036854775807


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    register(c)
    return c


def _raised(con: duckdb.DuckDBPyConnection, n: int) -> tuple[str, str]:
    with pytest.raises(duckdb.Error) as caught:
        con.execute(f"SELECT provisa_factorial({n}::BIGINT)").fetchall()
    error = from_engine(caught.value)
    assert error is not None, caught.value
    return error.sqlstate, str(error)


@pytest.mark.parametrize("n", [32178, 5000000000, BIGINT_MAX])
def test_past_numerics_reach_raises_postgresqls_overflow(
    con: duckdb.DuckDBPyConnection, n: int
) -> None:
    sqlstate, message = _raised(con, n)
    assert sqlstate == "22003"
    assert "value overflows numeric format" in message


def test_the_bigint_maximum_raises_without_computing() -> None:
    with pytest.raises(ValueError, match="22003: value overflows numeric format"):
        numeric_factorial(BIGINT_MAX)


def test_the_connection_answers_the_next_statement(con: duckdb.DuckDBPyConnection) -> None:
    _raised(con, BIGINT_MAX)
    assert con.execute("SELECT provisa_factorial(5::BIGINT)").fetchall() == [("120",)]


def test_negative_still_raises_postgresqls_error(con: duckdb.DuckDBPyConnection) -> None:
    sqlstate, message = _raised(con, -1)
    assert sqlstate == "22003"
    assert "factorial of a negative number is undefined" in message


@pytest.mark.parametrize("n", [1755, 3249, 32177])
def test_past_4300_digits_numerics_reach_is_answered(
    con: duckdb.DuckDBPyConnection, n: int
) -> None:
    """Between 1755 and 32177 PostgreSQL answers every digit; a Python int's text stops at 4300."""
    row = con.execute(f"SELECT provisa_factorial({n}::BIGINT)").fetchone()
    assert row is not None
    got, want = row[0], math.factorial(n)
    limit = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(0)
    try:
        assert got == str(want)
    finally:
        sys.set_int_max_str_digits(limit)
