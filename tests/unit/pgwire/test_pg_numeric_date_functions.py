# Copyright (c) 2026 Kenneth Stott
# Canary: 0c3f6a92-81d4-4b7e-a5e2-96d1e07b4c3f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Numeric and math and the Date and time families of pg_catalog (REQ-2167, PG_CATALOG FUNCTIONS
ARE GAPS; REQ-2171, THE RULE FOR TYPES AND FUNCTIONS): each statement run as a client runs it, over
pgwire on the default engine, and answered as PostgreSQL 17.11 answered it."""

# Requirements: REQ-2167, REQ-2171
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

import math
import re
from typing import Any

import duckdb
import pytest
import sqlglot
from psycopg import pq

from provisa.federation.duckdb_pg_functions import register
from provisa.transpiler.function_macros import MACROS, FunctionRefused, Step, overload
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.unit.pgwire.pg17_numeric_date_answers import RECORDED
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

TEXT, TIMESTAMPTZ = 25, 1184
_IRRATIONAL = re.compile(
    r"\b(asind|acosd|atand|atan2d|cosd|cotd|erf|erfc|dexp|dlog1|dlog10|dpow)\("
)
_LAST_BITS = 1e-14


def _run(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)


def _rows(res: Any) -> list[Any]:
    return [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]


def _problem(conn: Any, sql: str) -> str | None:
    res = _run(conn, f"{sql} AS v")
    kind, first, second = RECORDED[sql]
    if kind == "err":
        if res.status != pq.ExecStatus.FATAL_ERROR:
            return f"answered {_rows(res)}"
        said = (
            res.error_field(pq.DiagnosticField.SQLSTATE).decode(),
            res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode(),
        )
        return None if said == (first, second) else str(said)
    if res.status != pq.ExecStatus.TUPLES_OK:
        return (res.error_message or b"").decode()[:200]
    if res.ftype(0) != first:
        return f"type {res.ftype(0)}, rows {_rows(res)}"
    rows = _rows(res)
    return None if rows == second or _same_but_for_libm(sql, rows, second) else str(rows)


def _same_but_for_libm(sql: str, rows: list[Any], want: list[Any]) -> bool:
    """A result of asin, atan, exp and their kin differs in its last bits between libms, PostgreSQL
    on one against itself on another; what is compared is everything above those bits. The answers
    PostgreSQL promises are exact on every libm are held to it below."""
    if not _IRRATIONAL.search(sql) or None in (*rows, *want) or len(rows) != len(want):
        return False
    return all(math.isclose(float(g), float(w), rel_tol=_LAST_BITS) for g, w in zip(rows, want))


# One test over all of them: conn builds a DuckDB runtime per test, and hundreds of those in one
# worker exhaust it.
def test_every_statement_answers_as_postgresql_17_answered(conn: Any):
    problems = {sql: problem for sql in RECORDED if (problem := _problem(conn, sql)) is not None}
    assert not problems


def test_the_recorded_statements_probe_every_overload_of_both_families():
    probed = [sql for sql in RECORDED if "NULL::" in sql and sql.startswith('SELECT pg_catalog."')]
    assert len({sql.split("(")[0] for sql in probed}) >= 119


def test_the_exact_angles_are_exact_on_any_libm(conn: Any):
    """float.c's reason for its degree functions: these answer exactly, whatever the libm."""
    exact = {
        "cosd(0)": "1", "cosd(60)": "0.5", "cosd(90)": "0", "cosd(120)": "-0.5", "cosd(180)": "-1",
        "cosd(270)": "0", "cosd(360)": "1", "cosd(-60)": "0.5", "cotd(45)": "1", "cotd(90)": "0",
        "cotd(135)": "-1", "cotd(225)": "1", "asind(0.5)": "30", "asind(1)": "90", "asind(-1)": "-90",
        "acosd(0.5)": "60", "acosd(1)": "0", "acosd(-1)": "180", "acosd(0)": "90", "atand(1)": "45",
        "atand(-1)": "-45", "atan2d(1, 1)": "45", "atan2d(1, 0)": "90",
    }  # fmt: skip
    answers = {call: _rows(_run(conn, f"SELECT {call} AS v")) for call in exact}
    assert answers == {call: [want] for call, want in exact.items()}


def test_the_clock_moves_within_a_statement(conn: Any):
    res = _run(
        conn,
        "SELECT clock_timestamp() >= statement_timestamp() AND clock_timestamp() >= now() AS v",
    )
    assert _rows(res) == ["t"]
    res = _run(conn, "SELECT count(DISTINCT clock_timestamp()) > 1 AS v FROM range(200000)")
    assert _rows(res) == ["t"]


@pytest.mark.parametrize(
    ("call", "oid"),
    [
        ("clock_timestamp()", TIMESTAMPTZ),
        ("statement_timestamp()", TIMESTAMPTZ),
        ('pg_catalog."clock_timestamp"()', TIMESTAMPTZ),
        ('pg_catalog."statement_timestamp"()', TIMESTAMPTZ),
        ("timeofday()", TEXT),
        ('pg_catalog."timeofday"()', TEXT),
    ],
)
def test_a_reading_of_the_clock_has_postgresqls_type(conn: Any, call: str, oid: int):
    res = _run(conn, f"SELECT {call} AS v")
    assert res.ftype(0) == oid
    assert _rows(res)[0] is not None


def test_timeofday_writes_what_postgresql_writes(conn: Any):
    written = _rows(_run(conn, "SELECT timeofday() AS v"))[0]
    assert re.fullmatch(
        r"[A-Z][a-z]{2} [A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2}\.\d{6} \d{4} UTC", written
    )


def test_random_normal_draws_from_the_distribution_it_is_asked_for(conn: Any):
    drawn = _rows(
        _run(
            conn,
            "SELECT abs(avg(x) - 50) < 0.1 AND abs(stddev_samp(x) - 2) < 0.1 AS v "
            "FROM (SELECT random_normal(50, 2) AS x FROM range(100000)) t",
        )
    )
    assert drawn == ["t"]
    assert _rows(_run(conn, "SELECT stddev_samp(random_normal()) > 0.9 AS v FROM range(1000)")) == [
        "t"
    ]


@pytest.mark.parametrize(
    ("sql", "row"),
    [
        ("int4(1.5::numeric)", "numeric_int4"),
        ("int4('1'::jsonb)", "jsonb_int4"),
        ("int4(B'1'::bit(3))", "bittoint4"),
        ("int8(B'1'::bit(3))", "bittoint8"),
        ("float8(1.5::numeric)", "numeric_float8"),
        ("timetz('2024-01-01'::timestamptz)", "timestamptz_timetz"),
        ("int4(x)", "int4"),
    ],
)
def test_an_overload_is_chosen_by_the_type_the_statement_says(sql: str, row: str):
    call = sqlglot.parse_one(sql, read="postgres")
    assert overload(call.name.lower(), list(call.expressions)) == row


@pytest.mark.parametrize(
    ("name", "arity"),
    [("int24pl", 2), ("float84mul", 2), ("int4", 1), ("cosd", 1), ("justify_days", 1)],
)
def test_a_row_answers_with_the_engines_own_sql_and_no_udf(name: str, arity: int):
    _, form = MACROS[(name, arity)].resolved("duckdb")
    assert form is not None
    assert "provisa_" not in form


def test_an_integer_comparison_is_any_dialects_and_arithmetic_stays_the_federation_engines():
    assert "=" in transpile("SELECT int24eq(a, b) FROM t", "trino")
    with pytest.raises(FunctionRefused, match=r"int24pl\(\) has no form on mysql"):
        transpile("SELECT int24pl(a, b) FROM t", "mysql")


def test_erf_is_clickhouses_own_function_first():
    assert MACROS[("erf", 1)].resolved("clickhouse") == (Step.OWN, "erf(x)")


def test_justify_stays_bigquerys_own_function():
    assert "JUSTIFY_DAYS" in transpile("SELECT justify_days(x) FROM t", "bigquery").upper()


@pytest.fixture
def new_york():
    """The engine in a zone other than UTC. What PostgreSQL 17.11 answered there is below."""
    connection = duckdb.connect()
    register(connection)
    connection.execute("SET TimeZone = 'America/New_York'")
    return connection


@pytest.mark.parametrize(
    ("call", "want"),
    [
        ("timetz('2024-02-29 12:30:01.5+02'::timestamptz)", "05:30:01.5-05"),
        ("pg_catalog.\"time\"('2024-02-29 12:30:01.5+02'::timestamptz)", "05:30:01.5"),
        ("time('2024-02-29 12:30:01.5+02'::timestamptz)", "05:30:01.5"),
        ("timestamptz('2024-02-29'::date)", "2024-02-29 00:00:00-05"),
        ("timestamptz('2024-07-01'::date, '12:00:00'::time)", "2024-07-01 12:00:00-04"),
        ("timestamptz('2024-02-29 12:30:01.5'::timestamp)", "2024-02-29 12:30:01.5-05"),
        ("datetimetz_pl('2024-02-29'::date, '12:30:01+02'::timetz)", "2024-02-29 05:30:01-05"),
    ],
)
def test_the_session_zone_reads_as_postgresql_reads_it(new_york, call: str, want: str):
    written = transpile_to_duckdb(f"SELECT {call} AS v")
    assert new_york.execute(f"SELECT v::VARCHAR FROM ({written})").fetchone()[0] == want
    transpiled = transpile(f"SELECT {call} AS v", "duckdb")
    assert new_york.execute(f"SELECT v::VARCHAR FROM ({transpiled})").fetchone()[0] == want


def test_a_time_takes_the_sessions_offset_today(new_york):
    offset = new_york.execute("SELECT date_part('timezone', now()) / 3600").fetchone()[0]
    written = transpile_to_duckdb("SELECT timetz('12:30:01.5'::time) AS v")
    assert new_york.execute(f"SELECT v::VARCHAR FROM ({written})").fetchone()[0] == (
        f"12:30:01.5-{-int(offset):02d}"
    )
