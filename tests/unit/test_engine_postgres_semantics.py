# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8c2a71-6d4e-4b09-9e15-c0a7b2d84f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement the engine answers answers as PostgreSQL does (REQ-066, REQ-068, REQ-2167): a
statement that reads no source is the engine's, so what it says on DuckDB is what PostgreSQL
would -- a zero divisor is an error, a set-returning function aliased in FROM names its column,
and an infinite date or timestamp stays infinite on the wire."""

# Requirements: REQ-066, REQ-068, REQ-2167

from __future__ import annotations

import datetime
import struct

import duckdb
import pytest

from provisa.transpiler.transpile import transpile_to_duckdb


def _on_duckdb(pg_sql: str) -> list[tuple]:
    from provisa.executor.drivers.duckdb_driver import fetch_rows

    with duckdb.connect() as con:
        return fetch_rows(con.execute(transpile_to_duckdb(pg_sql)))


@pytest.mark.parametrize(
    "sql",
    ["SELECT 1/0", "SELECT 1.0/0", "SELECT 5 % 0", "SELECT x / y FROM (VALUES (4, 0)) t(x, y)"],
)
def test_a_zero_divisor_is_an_error(sql):
    with pytest.raises(duckdb.InvalidInputException, match="division by zero"):
        _on_duckdb(sql)


def test_a_nonzero_divisor_divides_as_before():
    assert _on_duckdb("SELECT x / y, x % y FROM (VALUES (9, 2)) t(x, y)") == [(4.5, 1)]


def test_the_error_is_reported_as_division_by_zero():
    from provisa.pgwire.server import _sqlstate_of

    with pytest.raises(duckdb.InvalidInputException) as raised:
        _on_duckdb("SELECT 1/0")
    assert _sqlstate_of(raised.value) == "22012"
    assert _sqlstate_of(duckdb.InvalidInputException("Invalid Input Error: no such thing")) == (
        "XX000"
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT i, 'row_' || i FROM generate_series(1, 3) AS i",
        "SELECT i, 'row_' || i FROM unnest(ARRAY[1, 2, 3]) AS i",
    ],
)
def test_a_set_returning_function_aliased_in_from_names_its_column(sql):
    assert _on_duckdb(sql) == [(1, "row_1"), (2, "row_2"), (3, "row_3")]


def test_an_infinite_date_or_timestamp_is_read_as_infinite():
    from provisa.core.ir_types import infinity_of

    [row] = _on_duckdb(
        "SELECT 'infinity'::date, '-infinity'::date, DATE '9999-12-31', "
        "'infinity'::timestamp, '-infinity'::timestamptz, TIMESTAMP '2026-10-27 09:30:00'"
    )
    assert [infinity_of(v) for v in row] == [1, -1, 0, 1, -1, 0]
    # Anyone else reads the nearest value Python holds, as before.
    assert row[0] == datetime.date.max and row[2] == datetime.date(9999, 12, 31)
    assert row[5] == datetime.datetime(2026, 10, 27, 9, 30)


def test_infinity_is_written_as_postgresql_writes_it():
    from buenavista.postgres import BVTYPE_TO_PGTYPE
    from buenavista.core import BVType

    from provisa.core.ir_types import infinite_date, infinite_timestamp

    def wire(kind, value):
        _oid, text, binary = BVTYPE_TO_PGTYPE[kind]
        return text(value), binary(value)

    assert wire(BVType.DATE, infinite_date(1)) == ("infinity", struct.pack("!i", 0x7FFFFFFF))
    assert wire(BVType.DATE, infinite_date(-1)) == ("-infinity", struct.pack("!i", -0x80000000))
    for kind, aware in ((BVType.TIMESTAMP, False), (BVType.TIMESTAMPTZ, True)):
        assert wire(kind, infinite_timestamp(1, aware=aware)) == (
            "infinity",
            struct.pack("!q", 0x7FFFFFFFFFFFFFFF),
        )
        assert wire(kind, infinite_timestamp(-1, aware=aware)) == (
            "-infinity",
            struct.pack("!q", -0x8000000000000000),
        )
