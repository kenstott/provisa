# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1d47-6c2e-4b95-9e07-d81c5a4b2f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Edge values through pgwire on DuckDB (REQ-2236, amended 2026-11-15, AN EDGE CASE RAISES; IT
DOES NOT REFUSE THE FORM): a call at an edge value is answered as PostgreSQL answers it, or raises
an error to the client, and the connection answers the next statement (J1367: D0, D9).

- A date PostgreSQL holds past year 9999 or BC is sent as PostgreSQL writes it, in text and binary:
  the encoder held a date to Python's years 1..9999, and its error dropped the connection.
- A value the encoder cannot write (a sum of reals past real's range, a sum of money past money's,
  a timestamp past year 9999) is the statement's error, with PostgreSQL's SQLSTATE where PostgreSQL
  raises one, never a dropped connection.
- factorial past 32177 raises PostgreSQL's 22003 at once instead of holding the server."""

# Requirements: REQ-2236, REQ-2194
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

import datetime
import struct
from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

_Y2K = datetime.date(2000, 1, 1).toordinal()
#: Days from 2000-01-01, PostgreSQL's binary date. 5874897-12-31 is Julian day 2147483493 (the
#: day before DATE_END_JULIAN), 2000-01-01 Julian day 2451545 (POSTGRES_EPOCH_JDATE); 1 BC is a
#: leap year of 366 days.
_DATE_BINARY = {
    "5874897-12-31": 2147483493 - 2451545,
    "10000-01-01": datetime.date(9999, 12, 31).toordinal() + 1 - _Y2K,
    "0001-01-01 BC": datetime.date(1, 1, 1).toordinal() - 366 - _Y2K,
}


def _run(conn: Any, protocol: str, sql: str, result_format: int = 0) -> Any:
    pgconn = conn.conn.pgconn
    if protocol == "extended":
        return pgconn.exec_params(sql.encode(), [], result_format=result_format)
    return pgconn.exec_(sql.encode())


def _said(res: Any) -> tuple[str, str]:
    return (
        res.error_field(pq.DiagnosticField.SQLSTATE).decode(),
        res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode(),
    )


def _goes_on(conn: Any, protocol: str) -> None:
    after = _run(conn, protocol, "SELECT 42 AS v")
    assert after.status == pq.ExecStatus.TUPLES_OK, (after.error_message or b"").decode()
    assert after.get_value(0, 0) == b"42"


@pytest.mark.parametrize("protocol", ["extended", "simple"])
@pytest.mark.parametrize(
    ("sql", "want"),
    [
        (
            "SELECT pg_catalog.date_larger('5874897-12-31'::date, '2024-06-15'::date) AS v",
            b"5874897-12-31",
        ),
        ("SELECT pg_catalog.date_mii('5874897-12-31'::date, 3) AS v", b"5874897-12-28"),
        (
            "SELECT max(c1) AS v FROM (VALUES ('5874897-12-31'::date), ('2024-06-15'::date)) "
            "AS t(c1)",
            b"5874897-12-31",
        ),
        ("SELECT '9999-12-31'::date + 1 AS v", b"10000-01-01"),
        ("SELECT '0001-01-01'::date - 1 AS v", b"0001-12-31 BC"),
    ],
    ids=["date_larger", "date_mii", "max", "past-9999", "bc"],
)
def test_a_date_postgresql_holds_is_sent_as_postgresql_writes_it(
    conn: Any, protocol: str, sql: str, want: bytes
) -> None:
    res = _run(conn, protocol, sql)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert res.get_value(0, 0) == want
    _goes_on(conn, protocol)


@pytest.mark.parametrize(
    ("literal", "days"),
    [
        ("'5874897-12-31'::date", _DATE_BINARY["5874897-12-31"]),
        ("'9999-12-31'::date + 1", _DATE_BINARY["10000-01-01"]),
        ("'0001-01-01'::date - 366", _DATE_BINARY["0001-01-01 BC"]),
    ],
    ids=["greatest", "past-9999", "bc"],
)
def test_a_date_postgresql_holds_is_sent_in_binary(conn: Any, literal: str, days: int) -> None:
    res = _run(conn, "extended", f"SELECT {literal} AS v", result_format=1)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert struct.unpack("!i", res.get_value(0, 0)) == (days,)
    _goes_on(conn, "extended")


@pytest.mark.parametrize("protocol", ["extended", "simple"])
@pytest.mark.parametrize(
    ("sql", "sqlstate"),
    [
        # PostgreSQL: 22003 "value out of range: overflow".
        (
            "SELECT sum(c1) AS v FROM (VALUES ('3.4028235e+38'::real), ('3.4028235e+38'::real)) "
            "AS t(c1)",
            "22003",
        ),
        # PostgreSQL: 22003 "money out of range".
        (
            "SELECT sum(c1) AS v FROM (VALUES ('-92233720368547758.08'::money), "
            "('-92233720368547758.08'::money)) AS t(c1)",
            "22003",
        ),
        # PostgreSQL answers 10000-01-01 00:00:00; pgwire sends a timestamp of years 1 to 9999.
        ("SELECT '9999-12-31 23:00:00'::timestamp + interval '2 hours' AS v", "22008"),
        # PostgreSQL: 22003 "value overflows numeric format", at once (D0).
        ("SELECT pg_catalog.factorial(9223372036854775807::bigint) AS v", "22003"),
    ],
    ids=["sum-real", "sum-money", "timestamp-past-9999", "factorial"],
)
def test_a_value_past_what_is_sent_is_the_statements_error_and_the_connection_goes_on(
    conn: Any, protocol: str, sql: str, sqlstate: str
) -> None:
    res = _run(conn, protocol, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR, res.status
    assert _said(res)[0] == sqlstate, (res.error_message or b"").decode()
    _goes_on(conn, protocol)


def test_the_messages_are_postgresqls(conn: Any) -> None:
    for sql, want in [
        (
            "SELECT sum(c1) AS v FROM (VALUES ('3.4028235e+38'::real), ('3.4028235e+38'::real)) "
            "AS t(c1)",
            ("22003", "value out of range: overflow"),
        ),
        (
            "SELECT sum(c1) AS v FROM (VALUES ('-92233720368547758.08'::money), "
            "('-92233720368547758.08'::money)) AS t(c1)",
            ("22003", "money out of range"),
        ),
        (
            "SELECT pg_catalog.factorial(9223372036854775807::bigint) AS v",
            ("22003", "value overflows numeric format"),
        ),
    ]:
        assert _said(_run(conn, "extended", sql)) == want, sql
