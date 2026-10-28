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
import re
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
    [
        "SELECT 1/0",
        "SELECT 1.0/0",
        "SELECT 5 % 0",
        "SELECT x / y FROM (VALUES (4, 0)) t(x, y)",
        "SELECT 8 / (x / y) FROM (VALUES (4, 0)) t(x, y)",
    ],
)
def test_a_zero_divisor_is_an_error(sql):
    with pytest.raises(duckdb.InvalidInputException, match="division by zero"):
        _on_duckdb(sql)


def test_a_nonzero_divisor_divides_as_postgres_does():
    # An integer by an integer is an integer, truncated toward zero; a non-integer divides exactly.
    assert _on_duckdb(
        "SELECT x / y, x % y, -x / y, 9.0 / y, CAST(x AS DOUBLE PRECISION) / y"
        " FROM (VALUES (9, 2)) t(x, y)"
    ) == [(4, 1, -4, 4.5, 4.5)]


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


def test_a_uuid_beside_a_date_is_read_as_the_uuid_it_is():
    """A result read through Arrow for its date (as an infinite one must be) reads a UUID as the
    uuid.UUID DuckDB's own rows give, not Arrow's text of it."""
    import uuid

    value = "5b927589-4fa7-49a1-9880-ef2d58461b1d"
    [row] = _on_duckdb(f"SELECT CAST('{value}' AS UUID), DATE '2026-01-02', CAST(NULL AS UUID)")
    assert row == (uuid.UUID(value), datetime.date(2026, 1, 2), None)
    assert _on_duckdb(f"SELECT CAST('{value}' AS UUID)") == [(uuid.UUID(value),)]


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


def test_a_function_named_in_pg_catalog_is_that_function():
    # psql, pgjdbc and the conformance probe name built-ins as pg_catalog."fn"(...): it is the
    # function, as PostgreSQL resolves it, not a column of a relation named pg_catalog.
    assert _on_duckdb("SELECT pg_catalog.abs(-1) AS v") == [(1,)]
    assert _on_duckdb("SELECT pg_catalog.\"upper\"('a') AS v") == [("A",)]
    assert _on_duckdb('SELECT pg_catalog."abs"(NULL::bigint) AS v') == [(None,)]
    # Named as PostgreSQL names it, though DuckDB spells it otherwise.
    assert _on_duckdb("SELECT pg_catalog.char_length('abc') AS v") == [(3,)]
    assert _on_duckdb("SELECT pg_catalog.lower(pg_catalog.btrim('  X ')) AS v") == [("x",)]


# PostgreSQL's own to_char examples (Table 9.31), and a date's elements strftime has none of.
_TO_CHAR = [
    ("TIMESTAMP '2026-10-06 17:39:18'", "Day, DD  HH12:MI:SS", "Tuesday  , 06  05:39:18"),
    ("TIMESTAMP '2026-10-06 17:39:18'", "FMDay, FMDD  HH12:MI:SS", "Tuesday, 6  05:39:18"),
    ("DATE '2026-10-06'", 'YYYY-MM-DD"T" Mon MON mon D WW Q', "2026-10-06T Oct OCT oct 3 40 4"),
    ("-0.1", "99.99", "  -.10"),
    ("-0.1", "FM9.99", "-.1"),
    ("-0.1", "FM90.99", "-0.1"),
    ("0.1", "0.9", " 0.1"),
    ("12", "9990999.9", "    0012.0"),
    ("12", "FM9990999.9", "0012."),
    ("485", "999", " 485"),
    ("-485", "999", "-485"),
    ("1485", "9,999", " 1,485"),
    ("148.5", "999.999", " 148.500"),
    ("148.5", "FM999.999", "148.5"),
    ("148.5", "FM999.990", "148.500"),
    ("0", "999", "   0"),
    ("1485", "999", " ###"),
    ("NULL::numeric", "999", None),
]


@pytest.mark.parametrize("value, fmt, written", _TO_CHAR)
def test_to_char_writes_as_postgresql_writes(value, fmt, written):
    assert _on_duckdb(f"SELECT to_char({value}, '{fmt}')") == [(written,)]
    assert _on_duckdb(f"SELECT pg_catalog.to_char({value}, '{fmt}')") == [(written,)]


@pytest.mark.parametrize(
    "call, element",
    [
        ("to_char(5, '999S')", "'S'"),
        ("to_char(now(), 'TMMonth')", "'TM'"),
        ("to_char(now(), 'DDth')", "'DDth'"),
        ("to_char(now(), 'J')", "'J'"),
        ("to_char(now(), x) FROM (VALUES ('YYYY')) t(x)", "not a literal"),
    ],
)
def test_a_to_char_element_not_carried_is_refused_by_name(call, element):
    from provisa.pgwire.server import _sqlstate_of

    with pytest.raises(NotImplementedError, match=re.escape(element)) as refused:
        transpile_to_duckdb(f"SELECT {call}")
    assert _sqlstate_of(refused.value) == "0A000"
