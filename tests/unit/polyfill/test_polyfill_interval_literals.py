# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9a1c57-0b2d-4f86-a4c3-9d17e2b6f051
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A literal of interval on the engines that hold no interval of PostgreSQL's (REQ-2167; REQ-2236,
AN ANSWER WITHIN A STATED BOUND COMES BEFORE A REFUSAL):

- ``interval '1 day 02:03'`` is the whole interval its text is: sqlglot read it as 1 day (the
  clock dropped, 'ago' dropped) and wrote '3 mons' as a unit no engine has, on DuckDB, ClickHouse
  and Trino alike (a wrong answer, or an engine error). The parser reads it as PostgreSQL does;
- a literal no call reads, on ClickHouse, answers interval_out's text in the select list and is
  refused by name anywhere else, never ClickHouse's engine error on a type named INTERVAL; a table's
  interval column in an interval call is refused by name, naming the column;
- ``f(..)::text`` of an interval call, on ClickHouse, answers the call's text;
- on Trino a literal no call reads is Trino's own INTERVAL where one of its types holds the value
  exactly, and refused by name otherwise; on DuckDB a text DuckDB's own input misreads is sent as
  its fields' text, which it reads exactly.

The answers are PostgreSQL's (18.4, a throwaway local cluster: ~/provisa-agent-logs/J1337-oracle).
The DuckDB and ClickHouse answers run on the embedded engines; Trino's is its SQL text."""

# Requirements: REQ-2167, REQ-2236

from __future__ import annotations

import datetime
from typing import Any

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.core.ir_types import interval_parts
from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.pgwire.cast_types import interval_of_text
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

#: Each statement and PostgreSQL's text of its answer: a tuple for one of several rows, and
#: ("error", SQLSTATE) for one PostgreSQL refuses.
PG = [
    ("SELECT interval '1 day 02:03' AS v", "1 day 02:03:00"),
    ("SELECT '1 day 02:03'::interval AS v", "1 day 02:03:00"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '1 day 02:03' AS v", "2024-02-01 12:03:00"),
    ("SELECT v FROM (VALUES (interval '1 day 02:03')) AS t(v)", "1 day 02:03:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1 day 02:03')) AS t(v)",
        "2024-01-30 07:57:00",
    ),
    ("SELECT interval '1 day ago' AS v", "-1 days"),
    ("SELECT '1 day ago'::interval AS v", "-1 days"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '1 day ago' AS v", "2024-01-30 10:00:00"),
    ("SELECT v FROM (VALUES (interval '1 day ago')) AS t(v)", "-1 days"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1 day ago')) AS t(v)",
        "2024-02-01 10:00:00",
    ),
    ("SELECT interval '1 day 02:03:04' AS v", "1 day 02:03:04"),
    ("SELECT '1 day 02:03:04'::interval AS v", "1 day 02:03:04"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' + interval '1 day 02:03:04' AS v",
        "2024-02-01 12:03:04",
    ),
    ("SELECT v FROM (VALUES (interval '1 day 02:03:04')) AS t(v)", "1 day 02:03:04"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1 day 02:03:04')) AS t(v)",
        "2024-01-30 07:56:56",
    ),
    ("SELECT interval '2 hours 3 minutes' AS v", "02:03:00"),
    ("SELECT '2 hours 3 minutes'::interval AS v", "02:03:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' + interval '2 hours 3 minutes' AS v",
        "2024-01-31 12:03:00",
    ),
    ("SELECT v FROM (VALUES (interval '2 hours 3 minutes')) AS t(v)", "02:03:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '2 hours 3 minutes')) AS t(v)",
        "2024-01-31 07:57:00",
    ),
    ("SELECT interval '1 day' AS v", "1 day"),
    ("SELECT '1 day'::interval AS v", "1 day"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '1 day' AS v", "2024-02-01 10:00:00"),
    ("SELECT v FROM (VALUES (interval '1 day')) AS t(v)", "1 day"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1 day')) AS t(v)",
        "2024-01-30 10:00:00",
    ),
    ("SELECT interval '3 mons' AS v", "3 mons"),
    ("SELECT '3 mons'::interval AS v", "3 mons"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '3 mons' AS v", "2024-04-30 10:00:00"),
    ("SELECT v FROM (VALUES (interval '3 mons')) AS t(v)", "3 mons"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '3 mons')) AS t(v)",
        "2023-10-31 10:00:00",
    ),
    ("SELECT interval '-1 year 2 days' AS v", "-1 years +2 days"),
    ("SELECT '-1 year 2 days'::interval AS v", "-1 years +2 days"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' + interval '-1 year 2 days' AS v",
        "2023-02-02 10:00:00",
    ),
    ("SELECT v FROM (VALUES (interval '-1 year 2 days')) AS t(v)", "-1 years +2 days"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '-1 year 2 days')) AS t(v)",
        "2025-01-29 10:00:00",
    ),
    ("SELECT interval '1 day -02:03' AS v", "1 day -02:03:00"),
    ("SELECT '1 day -02:03'::interval AS v", "1 day -02:03:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' + interval '1 day -02:03' AS v",
        "2024-02-01 07:57:00",
    ),
    ("SELECT v FROM (VALUES (interval '1 day -02:03')) AS t(v)", "1 day -02:03:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1 day -02:03')) AS t(v)",
        "2024-01-30 12:03:00",
    ),
    ("SELECT interval '@ 1 day' AS v", "1 day"),
    ("SELECT '@ 1 day'::interval AS v", "1 day"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '@ 1 day' AS v", "2024-02-01 10:00:00"),
    ("SELECT v FROM (VALUES (interval '@ 1 day')) AS t(v)", "1 day"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '@ 1 day')) AS t(v)",
        "2024-01-30 10:00:00",
    ),
    ("SELECT interval '1.5 days' AS v", "1 day 12:00:00"),
    ("SELECT '1.5 days'::interval AS v", "1 day 12:00:00"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '1.5 days' AS v", "2024-02-01 22:00:00"),
    ("SELECT v FROM (VALUES (interval '1.5 days')) AS t(v)", "1 day 12:00:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1.5 days')) AS t(v)",
        "2024-01-29 22:00:00",
    ),
    ("SELECT interval '02:03' AS v", "02:03:00"),
    ("SELECT '02:03'::interval AS v", "02:03:00"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '02:03' AS v", "2024-01-31 12:03:00"),
    ("SELECT v FROM (VALUES (interval '02:03')) AS t(v)", "02:03:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '02:03')) AS t(v)",
        "2024-01-31 07:57:00",
    ),
    ("SELECT interval '1 mon 2 days 03:04:05.5' AS v", "1 mon 2 days 03:04:05.5"),
    ("SELECT '1 mon 2 days 03:04:05.5'::interval AS v", "1 mon 2 days 03:04:05.5"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' + interval '1 mon 2 days 03:04:05.5' AS v",
        "2024-03-02 13:04:05.5",
    ),
    (
        "SELECT v FROM (VALUES (interval '1 mon 2 days 03:04:05.5')) AS t(v)",
        "1 mon 2 days 03:04:05.5",
    ),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1 mon 2 days 03:04:05.5')) AS t(v)",
        "2023-12-29 06:55:54.5",
    ),
    ("SELECT interval '1 day 02:03 ago' AS v", "-1 days -02:03:00"),
    ("SELECT '1 day 02:03 ago'::interval AS v", "-1 days -02:03:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' + interval '1 day 02:03 ago' AS v",
        "2024-01-30 07:57:00",
    ),
    ("SELECT v FROM (VALUES (interval '1 day 02:03 ago')) AS t(v)", "-1 days -02:03:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1 day 02:03 ago')) AS t(v)",
        "2024-02-01 12:03:00",
    ),
    ("SELECT interval '-1 day' AS v", "-1 days"),
    ("SELECT '-1 day'::interval AS v", "-1 days"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '-1 day' AS v", "2024-01-30 10:00:00"),
    ("SELECT v FROM (VALUES (interval '-1 day')) AS t(v)", "-1 days"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '-1 day')) AS t(v)",
        "2024-02-01 10:00:00",
    ),
    ("SELECT interval '1 hour' AS v", "01:00:00"),
    ("SELECT '1 hour'::interval AS v", "01:00:00"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '1 hour' AS v", "2024-01-31 11:00:00"),
    ("SELECT v FROM (VALUES (interval '1 hour')) AS t(v)", "01:00:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1 hour')) AS t(v)",
        "2024-01-31 09:00:00",
    ),
    ("SELECT interval '5 m' AS v", "00:05:00"),
    ("SELECT '5 m'::interval AS v", "00:05:00"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '5 m' AS v", "2024-01-31 10:05:00"),
    ("SELECT v FROM (VALUES (interval '5 m')) AS t(v)", "00:05:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '5 m')) AS t(v)",
        "2024-01-31 09:55:00",
    ),
    ("SELECT interval '+3 hours' AS v", "03:00:00"),
    ("SELECT '+3 hours'::interval AS v", "03:00:00"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '+3 hours' AS v", "2024-01-31 13:00:00"),
    ("SELECT v FROM (VALUES (interval '+3 hours')) AS t(v)", "03:00:00"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '+3 hours')) AS t(v)",
        "2024-01-31 07:00:00",
    ),
    ("SELECT interval '1.0000005 sec' AS v", "00:00:01"),
    ("SELECT '1.0000005 sec'::interval AS v", "00:00:01"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' + interval '1.0000005 sec' AS v",
        "2024-01-31 10:00:01",
    ),
    ("SELECT v FROM (VALUES (interval '1.0000005 sec')) AS t(v)", "00:00:01"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1.0000005 sec')) AS t(v)",
        "2024-01-31 09:59:59",
    ),
    (
        "SELECT interval '-1 years -2 mons +3 days -04:05:06' AS v",
        "-1 years -2 mons +3 days -04:05:06",
    ),
    (
        "SELECT '-1 years -2 mons +3 days -04:05:06'::interval AS v",
        "-1 years -2 mons +3 days -04:05:06",
    ),
    (
        "SELECT timestamp '2024-01-31 10:00:00' + interval '-1 years -2 mons +3 days -04:05:06' AS v",
        "2022-12-03 05:54:54",
    ),
    (
        "SELECT v FROM (VALUES (interval '-1 years -2 mons +3 days -04:05:06')) AS t(v)",
        "-1 years -2 mons +3 days -04:05:06",
    ),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '-1 years -2 mons +3 days -04:05:06')) AS t(v)",
        "2025-03-28 14:05:06",
    ),
    ("SELECT interval '2 weeks' AS v", "14 days"),
    ("SELECT '2 weeks'::interval AS v", "14 days"),
    ("SELECT timestamp '2024-01-31 10:00:00' + interval '2 weeks' AS v", "2024-02-14 10:00:00"),
    ("SELECT v FROM (VALUES (interval '2 weeks')) AS t(v)", "14 days"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '2 weeks')) AS t(v)",
        "2024-01-17 10:00:00",
    ),
    ("SELECT interval '1 millisecond' AS v", "00:00:00.001"),
    ("SELECT '1 millisecond'::interval AS v", "00:00:00.001"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' + interval '1 millisecond' AS v",
        "2024-01-31 10:00:00.001",
    ),
    ("SELECT v FROM (VALUES (interval '1 millisecond')) AS t(v)", "00:00:00.001"),
    (
        "SELECT timestamp '2024-01-31 10:00:00' - v AS v FROM (VALUES (interval '1 millisecond')) AS t(v)",
        "2024-01-31 09:59:59.999",
    ),
    ("SELECT '1 day 1 day'::interval AS v", ("error", "22007")),
    ("SELECT 'P1D'::interval AS v", "1 day"),
    ("SELECT NULL::interval AS v", None),
    (
        "SELECT v FROM (VALUES ('1 day 02:03'::interval), (NULL::interval)) AS t(v)",
        ("1 day 02:03:00", None),
    ),
    ("SELECT '1 day'::interval::text AS v", "1 day"),
    ("SELECT NULL::interval::text AS v", None),
    (
        "SELECT pg_catalog.\"interval_pl\"('1 day'::interval, '1 hour'::interval)::text AS v",
        "1 day 01:00:00",
    ),
    ("SELECT pg_catalog.\"justify_days\"('35 days'::interval)::text AS v", "1 mon 5 days"),
    (
        "SELECT (pg_catalog.\"interval_um\"('1 day 02:03'::interval))::varchar AS v",
        "-1 days -02:03:00",
    ),
    (
        "SELECT pg_catalog.\"interval_pl\"(interval '1 day 02:03', interval '3 mons') AS v",
        "3 mons 1 day 02:03:00",
    ),
    ("SELECT pg_catalog.\"justify_hours\"(interval '1 day 25:00') AS v", "2 days 01:00:00"),
    ("SELECT interval '1 day 02:03' = '26 hours 3 minutes'::interval AS v", "true"),
]
_ANSWERED = [
    (sql, want) for sql, want in PG if not (isinstance(want, tuple) and want[0] == "error")
]
#: The statements the DuckDB and ClickHouse checks read: the keyword form and its uses (part a),
#: and a literal standing alone (part b).
_A_AND_B = [
    (sql, want)
    for sql, want in _ANSWERED
    if not any(m in sql for m in ("pg_catalog", "::text", "::varchar", "'P1D'", " = '26 hours"))
]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _stamp(value: datetime.datetime) -> str:
    text = value.strftime("%Y-%m-%d %H:%M:%S")
    return text + (f".{value.microsecond:06d}".rstrip("0") if value.microsecond else "")


def _ch_rows(runtime: ClickHouseFederationRuntime, sql: str) -> list[Any]:
    rows = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    return [_stamp(r[0]) if isinstance(r[0], datetime.datetime) else r[0] for r in rows]


def _wanted(want: Any) -> list[Any]:
    return list(want) if isinstance(want, tuple) else [want]


def _fields(text: str | None) -> tuple[int, int, int] | None:
    return None if text is None else interval_parts(interval_of_text(text))


def _duckdb_rows(sql: str, want: list[Any]) -> list[Any]:
    """DuckDB's answer: an interval as its (months, days, microseconds), a timestamp as text."""
    written = transpile(sql, "duckdb")
    con = duckdb.connect()
    if any(w is not None and not w[:4].isdigit() for w in want):
        written = (
            "SELECT CASE WHEN v IS NULL THEN NULL ELSE [datepart('year', v) * 12 + "
            "datepart('month', v), datepart('day', v), datepart('hour', v)::BIGINT * 3600000000 "
            "+ datepart('minute', v)::BIGINT * 60000000 + datepart('microseconds', v)] END "
            f"FROM ({written}) AS s"
        )
        return [None if r[0] is None else tuple(r[0]) for r in con.execute(written).fetchall()]
    return [_stamp(r[0]) if r[0] is not None else None for r in con.execute(written).fetchall()]


# --- (a) the INTERVAL keyword's literal: what the parser reads ------------------------------------


@pytest.mark.parametrize(
    ("text", "written"),
    [
        ("1 day", "INTERVAL '1' DAY"),
        ("-2 hours", "INTERVAL '-2' HOUR"),
        ("+3 hours", "INTERVAL '3' HOUR"),
        ("3 mons", "INTERVAL '3' MONTH"),
        ("1 year", "INTERVAL '1' YEAR"),
        ("5 m", "INTERVAL '5' MINUTE"),
        ("1 day 02:03", "CAST('0 months 1 days 7380000000 microseconds' AS INTERVAL)"),
        ("1 day ago", "CAST('0 months -1 days 0 microseconds' AS INTERVAL)"),
        ("2 weeks", "CAST('0 months 14 days 0 microseconds' AS INTERVAL)"),
        ("1.5 days", "CAST('0 months 1 days 43200000000 microseconds' AS INTERVAL)"),
        ("P1D", "CAST('P1D' AS INTERVAL)"),
    ],
)
def test_the_keyword_s_literal_is_read_whole(text, written):
    """sqlglot kept the first amount and its unit: '1 day 02:03' was 1 day. One whole amount of a
    unit every engine reads alike stays INTERVAL 'n' UNIT; any other is the cast of its fields'
    text, and a form the reader does not read the cast of its own text (as '..'::interval is)."""
    tree = sqlglot.parse_one(f"SELECT interval '{text}' AS v", read="postgres")
    assert tree.expressions[0].this.sql(dialect="trino") == written


def test_a_unit_written_after_the_text_is_sqlglot_s():
    tree = sqlglot.parse_one("SELECT interval '1' day AS v", read="postgres")
    assert isinstance(tree.expressions[0].this, exp.Interval)
    assert tree.expressions[0].this.sql(dialect="trino") == "INTERVAL '1' DAY"


@pytest.mark.parametrize(("sql", "want"), _A_AND_B)
def test_duckdb_answers_postgresql_s(sql, want):
    wanted = _wanted(want)
    got = _duckdb_rows(sql, wanted)
    expected = [w if w is None or w[:4].isdigit() else _fields(w) for w in wanted]
    assert got == expected


@pytest.mark.parametrize(("sql", "want"), _A_AND_B)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    got = _ch_rows(runtime, sql)
    assert got == _wanted(want)
    for text in got:
        if text is not None and not text[:4].isdigit():  # pgwire reads interval_out's text back
            assert _fields(text) is not None


_TS = "SELECT timestamp '2024-01-31 10:00:00'"
_TS_TRINO = "SELECT CAST('2024-01-31 10:00:00' AS TIMESTAMP)"


@pytest.mark.parametrize(
    ("sql", "written"),
    [
        (f"{_TS} + interval '1 day' AS v", f"{_TS_TRINO} + INTERVAL '1' DAY AS v"),
        (f"{_TS} + interval '1 day 02:03' AS v", f"{_TS_TRINO} + INTERVAL '93780' SECOND AS v"),
        (f"{_TS} - '3 mons'::interval AS v", f"{_TS_TRINO} - INTERVAL '3' MONTH AS v"),
        (f"{_TS} + '1 day ago'::interval AS v", f"{_TS_TRINO} + INTERVAL '-1' DAY AS v"),
        ("SELECT interval '3 mons' AS v", "SELECT INTERVAL '3' MONTH AS v"),
        ("SELECT NULL::interval AS v", "SELECT CAST(NULL AS INTERVAL DAY TO SECOND) AS v"),
    ],
)
def test_trino_writes_a_literal_a_timestamp_is_moved_by(sql, written):
    """Trino has no type named interval. A timestamp moved by a value of months alone, or of days
    and whole seconds, is moved by Trino's INTERVAL 'n' UNIT; the answer is the timestamp. On the
    base '1 day 02:03' was INTERVAL '1' DAY (a wrong answer) and '3 mons' INTERVAL '3' MONS."""
    assert transpile(sql, "trino") == written


@pytest.mark.parametrize(
    ("sql", "named"),
    [
        ("SELECT interval '1 day 02:03' AS v", "interval '1 day 02:03:00' anywhere but"),
        ("SELECT '1 day'::interval AS v", "interval '1 day' anywhere but"),
        ("SELECT date '2024-01-31' + interval '1 day 02:03' AS v", "anywhere but"),
        ("SELECT now() - interval '1 day 02:03' AS v", "anywhere but"),
        (f"{_TS} + interval '1 mon 1 day' AS v", "interval '1 mon 1 day', which no one"),
        (f"{_TS} + interval '1.5 sec' AS v", "interval '00:00:01.5', which no one"),
        ("SELECT 'P1D'::interval AS v", "interval input 'P1D'"),
    ],
)
def test_trino_refuses_by_name_any_other_literal(sql, named):
    """Trino writes a day and its time as one count ('25:00:00' is its 1 day 01:00:00) and a date
    plus an interval as a date: anywhere but a timestamp's shift, the literal is refused by name.
    On the base, each reached Trino as INTERVAL '1' DAY (a wrong answer) or as a cast to a type
    it has none of (an engine error)."""
    with pytest.raises(FunctionRefused, match="trino") as refused:
        transpile(sql, "trino")
    assert named in str(refused.value)


# --- (b) a literal no call reads, and a table's column, on ClickHouse ----------------------------


@pytest.mark.parametrize(
    ("sql", "named"),
    [
        ("SELECT '1 day'::interval IS NULL AS v", "an interval value outside an interval call"),
        (
            "SELECT interval '1 day 02:03' IS NULL AS v",
            "an interval value outside an interval call",
        ),
        ("SELECT CASE WHEN true THEN '1 day'::interval END AS v", "has no form on clickhouse"),
        ("SELECT 'P1D'::interval AS v", "interval input 'P1D'"),
        ("SELECT '1 day 1 day'::interval AS v", "interval input '1 day 1 day'"),
        (
            "SELECT v FROM (VALUES ('1 day'::interval)) AS t(v) WHERE v IS NULL",
            "an interval value outside an interval call",
        ),
    ],
)
def test_clickhouse_refuses_by_name_a_literal_no_call_reads(runtime, sql, named):
    """On the base each reached ClickHouse as CAST(.. AS Nullable(INTERVAL)): an engine error."""
    with pytest.raises(FunctionRefused, match="clickhouse") as refused:
        transpile(sql, "clickhouse")
    assert named in str(refused.value)
    assert getattr(refused.value, "sqlstate", "0A000") == "0A000"


@pytest.mark.parametrize(
    "call",
    [
        'pg_catalog."justify_days"(c_interval)',
        "pg_catalog.\"interval_pl\"(c_interval, '1 day'::interval)",
        'pg_catalog."interval_um"(c_interval)',
        "pg_catalog.\"timestamp_pl_interval\"('2024-01-01'::timestamp, c_interval)",
    ],
)
def test_a_table_s_interval_column_is_refused_by_name(call):
    """The conformance run's column call: the column's type no statement says, and ClickHouse
    holds no interval of it. On the base: CAST(c_interval AS Nullable(INTERVAL)), an engine error."""
    sql = f"SELECT {call} AS v FROM conformance.function_columns WHERE id = 1"
    with pytest.raises(FunctionRefused, match="a table's interval column") as refused:
        transpile(sql, "clickhouse")
    assert refused.value.sqlstate == "0A000"


def test_pgwire_reads_an_interval_served_as_text():
    """A literal's answer on ClickHouse is interval_out's text; pgwire's cast type of interval
    reads it back as the interval it is."""
    from buenavista.core import BVType

    from provisa.pgwire.cast_types import result_casts

    (interval,) = result_casts("SELECT '1 day 02:03'::interval AS v") or (None,)
    assert interval is not None and interval.oid == 1186
    assert BVType.TEXT in interval.served_as
    assert interval.of_text is not None
    assert interval_parts(interval.of_text("1 day 02:03:00")) == (0, 1, 7380000000)


# --- (c) f(..)::text of an interval call ---------------------------------------------------------

_TEXT = [(sql, want) for sql, want in _ANSWERED if "::text" in sql or "::varchar" in sql]


@pytest.mark.parametrize(("sql", "want"), _TEXT)
def test_the_text_of_an_interval_call_answers_postgresql_s(runtime, sql, want):
    """On the base the cast was wrapped in a test of the value's type (zoned_instants), which no
    interval row reads: it was refused by name."""
    assert _ch_rows(runtime, sql) == _wanted(want)


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        (s, w)
        for s, w in _ANSWERED
        if "interval '1 day 02:03', interval" in s
        or 'justify_hours"(interval' in s
        or "= '26 hours" in s
    ],
)
def test_a_call_of_keyword_literals_answers_postgresql_s(runtime, sql, want):
    got = _ch_rows(runtime, sql)
    assert [str(g).lower() if isinstance(g, bool) else g for g in got] == _wanted(want)


# --- DuckDB's own interval input, where it differs from PostgreSQL's ----------------------------


@pytest.mark.parametrize(
    ("text", "want"),
    [
        ("1.0000005 sec", (0, 0, 1000000)),
        ("0.0000015 sec", (0, 0, 1)),
        ("+3 hours", (0, 0, 10800000000)),
        ("1 day 02:03 ago", (0, -1, -7380000000)),
        ("-1 years -2 mons +3 days -04:05:06", (-14, 3, -14706000000)),
    ],
)
def test_duckdb_reads_a_cast_s_text_as_postgresql_does(text, want):
    """DuckDB rounds a fraction of a microsecond up, and fails on '+3 hours' and on a clock before
    'ago': the text is sent as its three fields'."""
    assert _duckdb_rows(f"SELECT '{text}'::interval AS v", ["x"]) == [want]


@pytest.mark.parametrize("text", ["1 day 1 day", "1 quarter", "1 hour 02:03"])
def test_duckdb_refuses_by_name_an_input_postgresql_refuses(text):
    """PostgreSQL refuses each (22007); DuckDB answered them (2 days, 3 mons, 03:03:00)."""
    with pytest.raises(FunctionRefused, match="duckdb"):
        transpile(f"SELECT '{text}'::interval AS v", "duckdb")


@pytest.mark.parametrize("text", ["0", "5"])
def test_duckdb_keeps_its_own_reading_of_a_form_not_read_here(text):
    """A bare number is seconds in both: DuckDB reads it as PostgreSQL does."""
    assert transpile(f"SELECT '{text}'::interval AS v", "duckdb") == (
        f"SELECT CAST('{text}' AS INTERVAL) AS v"
    )


def test_duckdb_keeps_a_text_it_reads_alike():
    assert transpile("SELECT '1 day 02:03:04'::interval AS v", "duckdb") == (
        "SELECT CAST('1 day 02:03:04' AS INTERVAL) AS v"
    )


def test_an_engine_error_is_never_the_answer(runtime):
    """Every statement here either answers or is refused by name on ClickHouse."""
    for sql, _ in PG:
        try:
            written = transpile(sql, "clickhouse")
        except FunctionRefused:
            continue
        try:
            list(runtime.run_sync(written).iter_rows())
        except Exception as error:  # a PostgreSQL error the form raises is one
            assert from_engine(error) is not None, (sql, str(error)[:200])
