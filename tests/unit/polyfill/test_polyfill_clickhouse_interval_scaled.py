# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2f4e19-6a7d-4b30-9e85-1d0b7a3c5f62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An interval times or divided by a number on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A
STATED BOUND COMES BEFORE A REFUSAL): interval_mul, interval_div and mul_d_interval, and the * and
/ operators, held as Tuple(months, days, microseconds) and computed in Float64 as PostgreSQL 17's
C computes them in doubles. Each was refused by name on the base.

The answers are PostgreSQL's (18.4, a throwaway local cluster: ~/provisa-agent-logs/J1337-oracle;
the interval code is 17's): the function support run's value calls (its samples, row by row),
others, and its errors."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import pytest

from provisa.core.ir_types import interval_parts
from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.pgwire.cast_types import interval_of_text
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

PG = [
    (
        "SELECT pg_catalog.\"interval_mul\"('1 day 02:03:04'::interval, 0.5::double precision) AS v",
        "13:01:32",
    ),
    (
        "SELECT pg_catalog.\"interval_mul\"('3 mons'::interval, (-2.25)::double precision) AS v",
        "-6 mons -22 days -12:00:00",
    ),
    (
        "SELECT pg_catalog.\"interval_mul\"('-1 year 2 days'::interval, 100::double precision) AS v",
        "-100 years +200 days",
    ),
    (
        "SELECT pg_catalog.\"interval_div\"('1 day 02:03:04'::interval, 0.5::double precision) AS v",
        "2 days 04:06:08",
    ),
    (
        "SELECT pg_catalog.\"interval_div\"('3 mons'::interval, (-2.25)::double precision) AS v",
        "-1 mons -10 days",
    ),
    (
        "SELECT pg_catalog.\"interval_div\"('-1 year 2 days'::interval, 100::double precision) AS v",
        "-3 days -13:55:12",
    ),
    (
        "SELECT pg_catalog.\"mul_d_interval\"(0.5::double precision, '1 day 02:03:04'::interval) AS v",
        "13:01:32",
    ),
    (
        "SELECT pg_catalog.\"mul_d_interval\"((-2.25)::double precision, '3 mons'::interval) AS v",
        "-6 mons -22 days -12:00:00",
    ),
    (
        "SELECT pg_catalog.\"mul_d_interval\"(100::double precision, '-1 year 2 days'::interval) AS v",
        "-100 years +200 days",
    ),
    (
        "SELECT pg_catalog.\"interval_mul\"('1 mon -1 day 25:00'::interval, 0.1::double precision) AS v",
        "3 days 00:06:00",
    ),
    (
        "SELECT pg_catalog.\"interval_mul\"('1 year 2 mons 3 days 04:05:06.789012'::interval, (-0.333333)::double precision) AS v",
        "-4 mons -20 days -25:21:30.075702",
    ),
    (
        "SELECT pg_catalog.\"interval_div\"('7 mons'::interval, 3::double precision) AS v",
        "2 mons 10 days",
    ),
    ("SELECT pg_catalog.\"interval_mul\"('1 day 02:03'::interval, 0.5::real) AS v", "13:01:30"),
    (
        "SELECT pg_catalog.\"interval_mul\"('1 day 02:03'::interval, 3::integer) AS v",
        "3 days 06:09:00",
    ),
    ("SELECT pg_catalog.\"interval_mul\"('1 day 02:03'::interval, '0.25') AS v", "06:30:45"),
    (
        "SELECT pg_catalog.\"interval_mul\"('1 day 02:03:04'::interval, -1::double precision) AS v",
        "-1 days -02:03:04",
    ),
    ("SELECT '1 day 02:03:04'::interval * 0.5 AS v", "13:01:32"),
    ("SELECT 3 * '1 mon 1 day'::interval AS v", "3 mons 3 days"),
    ("SELECT '1 mon 1 day'::interval / 3 AS v", "10 days 08:00:00"),
    (
        "SELECT pg_catalog.\"interval_pl\"(pg_catalog.\"interval_mul\"('1 day 02:03'::interval, 0.5::double precision), '1 hour'::interval) AS v",
        "14:01:30",
    ),
    (
        "SELECT pg_catalog.\"interval_div\"('1 day'::interval, 'Infinity'::double precision) AS v",
        "00:00:00",
    ),
    (
        "SELECT pg_catalog.\"interval_mul\"('1 day'::interval, 'NaN'::double precision) AS v",
        ("error", "22008"),
    ),
    (
        "SELECT pg_catalog.\"interval_div\"('1 day'::interval, 0::double precision) AS v",
        ("error", "22012"),
    ),
    (
        "SELECT pg_catalog.\"interval_mul\"('2147483647 days'::interval, 2::double precision) AS v",
        ("error", "22008"),
    ),
    (
        "SELECT pg_catalog.\"interval_mul\"('00:00'::interval, 'Infinity'::double precision) AS v",
        ("error", "22008"),
    ),
    (
        "SELECT pg_catalog.\"interval_mul\"('1 day'::interval, 1e300::double precision) AS v",
        ("error", "22008"),
    ),
]
_ANSWERED = [(sql, want) for sql, want in PG if not isinstance(want, tuple)]
_RAISED = [(sql, want[1]) for sql, want in PG if isinstance(want, tuple)]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    return value


@pytest.mark.parametrize(("sql", "want"), _ANSWERED)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    got = _answer(runtime, sql)
    assert got == want
    assert interval_parts(interval_of_text(got)) is not None  # pgwire reads the text back


@pytest.mark.parametrize(("sql", "state"), _RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


def test_an_infinite_factor_s_infinite_answer_is_refused(runtime):
    """PostgreSQL 17 answers 'infinity', which no interval held here is."""
    with pytest.raises(Exception) as raised:
        list(
            runtime.run_sync(
                transpile(
                    "SELECT pg_catalog.\"interval_mul\"('1 day'::interval, "
                    "'Infinity'::double precision) AS v",
                    "clickhouse",
                )
            ).iter_rows()
        )
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == "0A000"
    assert "infinite interval" in str(error)


@pytest.mark.parametrize(
    "call",
    [
        'pg_catalog."interval_mul"(NULL::interval, NULL::double precision)',
        'pg_catalog."interval_div"(NULL::interval, NULL::double precision)',
        'pg_catalog."mul_d_interval"(NULL::double precision, NULL::interval)',
        "pg_catalog.\"interval_mul\"('1 day'::interval, NULL::double precision)",
        'pg_catalog."interval_div"(NULL::interval, 2::double precision)',
    ],
)
def test_a_null_argument_answers_null(runtime, call):
    assert _answer(runtime, f"SELECT {call} AS v") is None


def test_a_factor_a_row_computes_is_refused_by_name():
    sql = (
        "SELECT pg_catalog.\"interval_mul\"('1 day'::interval, c) AS v "
        "FROM (VALUES (0.5::double precision)) AS t(c)"
    )
    with pytest.raises(FunctionRefused, match="interval_mul"):
        transpile(sql, "clickhouse")


@pytest.mark.parametrize("dialect", ["duckdb", "postgres"])
def test_every_other_dialect_keeps_what_it_had(dialect):
    """No row here has a form outside ClickHouse: no tuple of fields is written elsewhere."""
    sql = "SELECT pg_catalog.\"interval_mul\"('1 day'::interval, 0.5::double precision) AS v"
    written = transpile(sql, dialect)
    assert "tupleElement" not in written and "provisa_iv" not in written
    if dialect == "postgres":
        assert 'pg_catalog."interval_mul"' in written
