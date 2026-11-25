# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1c6e-5d27-4b90-a4e3-9c1b7f2d6e08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Literals of a time, an integer and a numeric on ClickHouse, and a date moved by days (REQ-2236,
AN EDGE CASE RAISES; IT DOES NOT REFUSE THE FORM; REQ-2167, NEVER A WRONG ANSWER AND NEVER A RAW
ENGINE ERROR).

Each literal is read from its text in Python, as the date literal already was
(polyfill.clickhouse.dated), so a literal at or past its type's range answers exactly or raises
PostgreSQL's error:

1. a time of hours and minutes: '12:30'::time was toTime64('12:30', 6), which ClickHouse reads as
   00:12:30;
2. an integer past its type: 2147483648::integer wrapped to -2147483648, '2147483648'::integer was
   NULL, and 2.5::integer truncated to 2;
3. a numeric of more than ~15 digits, or one ClickHouse's Float64 holds inexactly: a number with a
   fraction went through a Float64 (0.1::numeric(38,30) was 0.1000000000000000090..., 1.005 was
   1.004), and a text past the scale was truncated;
4. a date moved by days (date_pli, date_mii, integer_pl_date and their operators): the old form
   moved the day in a Date (1970 to 2149), which wrapped any other day, and the operator wrapped
   Date32 at int4's extremes, a row that then failed to decode; integer_pl_date was refused by
   name.

Every answer and SQLSTATE here is PostgreSQL's (the embedded PostgreSQL 16.11 the unit tests boot;
time_in, int4in, numeric_in, int84, numeric_int4 and date_pli answer as postgres:17 does). A day
inside a date's range but outside ClickHouse's Date32 (1900-01-01 .. 2299-12-31) raises 0A000, the
row's stated bound."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import datetime
import re
from decimal import Decimal

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.polyfill import GRID, clocks
from provisa.transpiler.transpile import transpile

_US = 1_000_000


def _clock(hours: int, minutes: int, seconds: int = 0, micros: int = 0) -> int:
    """A time of day as its microseconds, as Provisa holds it on ClickHouse (polyfill.clocks:
    an Int64; 24:00:00 is PostgreSQL's last)."""
    return ((hours * 60 + minutes) * 60 + seconds) * _US + micros


#: 1. A text cast to a time, answered as its microseconds of the day: the select list shows it as
#: time_out's text, read back here as pgwire reads it. 24:00:00 is read through a cast to text: a
#: result column of it raises (test_a_result_column_of_the_last_time_raises).
TIMES = [
    ("SELECT '12:30'::time AS v", _clock(12, 30)),
    ("SELECT '1:2'::time AS v", _clock(1, 2)),
    ("SELECT '12:5'::time AS v", _clock(12, 5)),
    ("SELECT ' 12:30 '::time AS v", _clock(12, 30)),
    ("SELECT CAST('7:05' AS time without time zone) AS v", _clock(7, 5)),
    ("SELECT time '23:59' AS v", _clock(23, 59)),
    ("SELECT '12:30:60'::time AS v", _clock(12, 31)),
    ("SELECT '23:59:60'::time::text AS v", _clock(24, 0)),
    ("SELECT '24:00'::time::text AS v", _clock(24, 0)),
    ("SELECT '24:00:00.0000001'::time::text AS v", _clock(24, 0)),
    ("SELECT '12:30:45.1234567'::time AS v", _clock(12, 30, 45, 123457)),
    ("SELECT '12:30:45.9999995'::time AS v", _clock(12, 30, 46)),
    ("SELECT '12:30:59.9999999'::time AS v", _clock(12, 31)),
    ("SELECT '12:30:45.5'::time AS v", _clock(12, 30, 45, 500000)),
    ("SELECT '00:00'::time AS v", 0),
    ("SELECT '12:30'::text::time AS v", _clock(12, 30)),
]

#: 2. A literal cast to an integer.
INTEGERS = [
    ("SELECT (-2147483648)::integer AS v", -2147483648),
    ("SELECT 2147483647::integer AS v", 2147483647),
    ("SELECT (-9223372036854775808)::bigint AS v", -9223372036854775808),
    ("SELECT 9223372036854775807::bigint AS v", 9223372036854775807),
    ("SELECT (-32768)::smallint AS v", -32768),
    ("SELECT 32767::smallint AS v", 32767),
    ("SELECT ' 0x7FFFFFFF '::integer AS v", 2147483647),
    ("SELECT '-0x80000000'::integer AS v", -2147483648),
    ("SELECT '0x_ff'::integer AS v", 255),
    ("SELECT '1_000'::integer AS v", 1000),
    ("SELECT '0o17'::int2 AS v", 15),
    ("SELECT '0b101'::int8 AS v", 5),
    ("SELECT '+12'::int2 AS v", 12),
    ("SELECT '-9223372036854775808'::bigint AS v", -9223372036854775808),
    ("SELECT CAST('2147483647' AS integer) AS v", 2147483647),
    ("SELECT integer '42' AS v", 42),
    ("SELECT '5'::text::integer AS v", 5),
    ("SELECT 2.5::integer AS v", 3),
    ("SELECT (-2.5)::integer AS v", -3),
    ("SELECT -2.5::integer AS v", -3),
    ("SELECT 1e3::integer AS v", 1000),
    ("SELECT 2147483646.5::integer AS v", 2147483647),
    ("SELECT CAST(-2.5 AS smallint) AS v", -3),
    ("SELECT 0.4999::bigint AS v", 0),
]

#: 3. A literal cast to a numeric.
NUMERICS = [
    ("SELECT 0.1::numeric(38,30) AS v", Decimal("0.100000000000000000000000000000")),
    ("SELECT 0.0000001::numeric(38,30) AS v", Decimal("1E-7")),
    ("SELECT 1.005::numeric(10,2) AS v", Decimal("1.01")),
    ("SELECT 1.235::numeric(10,2) AS v", Decimal("1.24")),
    ("SELECT '1.235'::numeric(10,2) AS v", Decimal("1.24")),
    ("SELECT '-1.235'::numeric(10,2) AS v", Decimal("-1.24")),
    ("SELECT '-0.001'::numeric(10,2) AS v", Decimal("0.00")),
    ("SELECT '.5'::numeric(3,0) AS v", Decimal("1")),
    ("SELECT 0.5::numeric(1,0) AS v", Decimal("1")),
    ("SELECT '1_000.5'::numeric(10,2) AS v", Decimal("1000.50")),
    ("SELECT 1.5e-3::numeric(10,4) AS v", Decimal("0.0015")),
    ("SELECT ' 1.5 '::numeric AS v", Decimal("1.5")),
    ("SELECT '0x1F'::numeric AS v", Decimal("31")),
    ("SELECT 123456789012345678.123::numeric AS v", Decimal("123456789012345678.123")),
    (
        "SELECT 12345678901234567890.123456789012345678::numeric(38,18) AS v",
        Decimal("12345678901234567890.123456789012345678"),
    ),
    (
        "SELECT 99999999999999999999999999999999999999::numeric AS v",
        Decimal("99999999999999999999999999999999999999"),
    ),
    ("SELECT (-7.125)::numeric AS v", Decimal("-7.125")),
    ("SELECT CAST(9007199254740993 AS numeric(20,0)) AS v", Decimal("9007199254740993")),
]

#: 4. A date moved by a number of days.
DAYS = [
    ("SELECT '2024-01-01'::date + 3 AS v", datetime.date(2024, 1, 4)),
    ("SELECT 3 + '2024-01-01'::date AS v", datetime.date(2024, 1, 4)),
    ("SELECT '1960-01-01'::date - 3 AS v", datetime.date(1959, 12, 29)),
    ("SELECT '2024-01-01'::date + (-5)::smallint AS v", datetime.date(2023, 12, 27)),
    ("SELECT date '2024-02-28' + 1 AS v", datetime.date(2024, 2, 29)),
    ("SELECT pg_catalog.date_pli('1960-01-01'::date, 1) AS v", datetime.date(1960, 1, 2)),
    ("SELECT pg_catalog.date_pli('2299-12-30'::date, 1) AS v", datetime.date(2299, 12, 31)),
    ("SELECT pg_catalog.date_pli('2150-06-01'::date, 0) AS v", datetime.date(2150, 6, 1)),
    ("SELECT pg_catalog.date_mii('1900-01-02'::date, 1) AS v", datetime.date(1900, 1, 1)),
    ("SELECT pg_catalog.date_mii('2024-01-01'::date, -366) AS v", datetime.date(2025, 1, 1)),
    ('SELECT pg_catalog."date_pli"(NULL::date, 1) AS v', None),
    ("SELECT pg_catalog.date_pli('2024-01-01'::date, NULL::integer) AS v", None),
    ("SELECT pg_catalog.integer_pl_date(5, '2024-01-01'::date) AS v", datetime.date(2024, 1, 6)),
    (
        "SELECT pg_catalog.\"integer_pl_date\"(-1, '1960-03-01'::date) AS v",
        datetime.date(1960, 2, 29),
    ),
    ("SELECT pg_catalog.integer_pl_date(NULL::integer, '2024-01-01'::date) AS v", None),
    ("SELECT pg_catalog.integer_pl_date(1, NULL::date) AS v", None),
]

ANSWERED = TIMES + INTEGERS + NUMERICS + DAYS

RAISED = [
    # 1. a field of a time out of range
    ("SELECT '25:00'::time AS v", "22008"),
    ("SELECT '12:60'::time AS v", "22008"),
    ("SELECT '12:30:61'::time AS v", "22008"),
    ("SELECT '123:00'::time AS v", "22008"),
    ("SELECT '24:00:00.000001'::time AS v", "22008"),
    ("SELECT '23:59:60.5'::time AS v", "22008"),
    # 2. an integer past its type, or a text the integer input does not read
    ("SELECT 2147483648::integer AS v", "22003"),
    ("SELECT -2147483648::integer AS v", "22003"),
    ("SELECT (-2147483649)::integer AS v", "22003"),
    ("SELECT 9223372036854775808::bigint AS v", "22003"),
    ("SELECT 32768::smallint AS v", "22003"),
    ("SELECT (-32768.5)::smallint AS v", "22003"),
    ("SELECT 2147483647.5::integer AS v", "22003"),
    ("SELECT 9223372036854775807.5::bigint AS v", "22003"),
    ("SELECT '2147483648'::integer AS v", "22003"),
    ("SELECT CAST('9223372036854775808' AS bigint) AS v", "22003"),
    ("SELECT '0x80000000'::integer AS v", "22003"),
    ("SELECT '32768'::int2 AS v", "22003"),
    ("SELECT 'abc'::integer AS v", "22P02"),
    ("SELECT '2.5'::integer AS v", "22P02"),
    ("SELECT ''::integer AS v", "22P02"),
    ("SELECT '_1'::integer AS v", "22P02"),
    ("SELECT '1_'::integer AS v", "22P02"),
    ("SELECT '- 5'::integer AS v", "22P02"),
    # 3. a numeric past its precision, or a text the numeric input does not read
    ("SELECT 123.4::numeric(3,1) AS v", "22003"),
    ("SELECT 99.995::numeric(4,2) AS v", "22003"),
    ("SELECT 9.5::numeric(1,0) AS v", "22003"),
    ("SELECT '1e400'::numeric(10,2) AS v", "22003"),
    ("SELECT 'abc'::numeric AS v", "22P02"),
    ("SELECT '1.2.3'::numeric(10,2) AS v", "22P02"),
    # 4. a day past a date's range
    ("SELECT '2024-01-01'::date + 2147483647 AS v", "22008"),
    ("SELECT 2147483647 + '2024-01-01'::date AS v", "22008"),
    ("SELECT '2024-01-01'::date - 2147483647 AS v", "22008"),
    ("SELECT pg_catalog.date_pli('2024-01-01'::date, 2147483647) AS v", "22008"),
    ("SELECT pg_catalog.date_pli('2024-01-01'::date, -2147483648) AS v", "22008"),
    ("SELECT pg_catalog.date_mii('2024-01-01'::date, 2147483647) AS v", "22008"),
    ("SELECT pg_catalog.integer_pl_date(2147483647, '2024-01-01'::date) AS v", "22008"),
]

#: Inside a date's or a numeric's range, outside what ClickHouse holds exactly: the rows' bound.
BOUND = [
    "SELECT '2299-12-31'::date + 1 AS v",
    "SELECT '1900-01-01'::date - 1 AS v",
    "SELECT 1 + '2299-12-31'::date AS v",
    "SELECT pg_catalog.date_pli('2299-12-31'::date, 1) AS v",
    "SELECT pg_catalog.date_mii('1900-01-01'::date, 1) AS v",
    "SELECT pg_catalog.integer_pl_date(1, '2299-12-31'::date) AS v",
    "SELECT 1234567890123456789012345678901234567890::numeric AS v",
]

#: A text time_in reads that no row here reads: refused by name.
REFUSED = [
    "SELECT '12:30 PM'::time AS v",
    "SELECT 'allballs'::time AS v",
    "SELECT '12:30:45+02'::time AS v",
    "SELECT '2024-01-01 12:30'::time AS v",
]


def _rows(runtime: ClickHouseFederationRuntime, sql: str) -> list[object]:
    return [
        row[0]
        for row in runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows()
    ]


def _held(value: object) -> object:
    """A time as the select list shows it (time_out's text, polyfill.clocks), as its microseconds
    of the day; any other value as it is."""
    if isinstance(value, str) and (clock := _TIME_OUT.fullmatch(value)) is not None:
        hours, minutes, seconds = int(clock[1]), int(clock[2]), int(clock[3])
        return _clock(hours, minutes, seconds, int((clock[4] or "").ljust(6, "0")))
    return value


_TIME_OUT = re.compile(r"(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?")


def _sqlstate(runtime: ClickHouseFederationRuntime, sql: str) -> tuple[str | None, str]:
    """The SQLSTATE a statement raises, where it is transpiled or where it runs, and its text."""
    try:
        _rows(runtime, sql)
    except RowRefused as refused:
        return refused.sqlstate, str(refused)
    except Exception as raised:  # noqa: BLE001 -- the engine's error, read as PostgreSQL's
        error = from_engine(raised)
        return (None if error is None else error.sqlstate), str(raised)[:300]
    return None, "answered"


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    (got,) = _rows(runtime, sql)
    got = _held(got)
    assert got == want and type(got) is type(want), (want, got)


@pytest.mark.parametrize(
    "sql", ["SELECT '24:00'::time AS v", "SELECT '23:59:60'::time AS v", "SELECT time '24:00' AS v"]
)
def test_a_result_column_of_the_last_time_raises(sql):
    """24:00:00 is held (and compared) as PostgreSQL holds it, but pgwire reads no time of day past
    23:59:59.999999 back: a result column of it raises the row's stated 0A000 (REQ-2236, AN EDGE
    CASE RAISES), never 00:00:00."""
    with pytest.raises(RowRefused) as refused:
        transpile(sql, "clickhouse", generated=False)
    assert refused.value.sqlstate == "0A000"
    assert str(refused.value) == clocks.LAST_SHOWN.message


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    got, text = _sqlstate(runtime, sql)
    assert got == state, text


@pytest.mark.parametrize("sql", BOUND)
def test_a_value_outside_the_bound_raises_0a000(runtime, sql):
    got, text = _sqlstate(runtime, sql)
    assert got == "0A000", text
    assert "has no exact form on clickhouse" in text


@pytest.mark.parametrize("sql", REFUSED)
def test_a_time_text_no_row_reads_is_refused_by_name(sql):
    with pytest.raises(RowRefused, match="has no form on clickhouse") as refused:
        transpile(sql, "clickhouse", generated=False)
    assert refused.value.sqlstate == "0A000"


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("SELECT 2147483648::integer AS v", "integer out of range"),
        ("SELECT 9223372036854775808::bigint AS v", "bigint out of range"),
        ("SELECT 32768::smallint AS v", "smallint out of range"),
        (
            "SELECT '2147483648'::integer AS v",
            'value "2147483648" is out of range for type integer',
        ),
        ("SELECT 'abc'::integer AS v", 'invalid input syntax for type integer: "abc"'),
        ("SELECT 'abc'::numeric AS v", 'invalid input syntax for type numeric: "abc"'),
        ("SELECT 123.4::numeric(3,1) AS v", "numeric field overflow"),
        ("SELECT '25:00'::time AS v", 'date/time field value out of range: "25:00"'),
    ],
)
def test_a_literal_raises_postgresql_s_message(sql, message):
    with pytest.raises(RowRefused) as refused:
        transpile(sql, "clickhouse", generated=False)
    assert str(refused.value) == message


def test_a_day_past_the_range_raises_postgresql_s_message(runtime):
    _, text = _sqlstate(runtime, "SELECT '2024-01-01'::date + 2147483647 AS v")
    assert "date out of range" in text


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 5::integer AS v",
        "SELECT (-5)::bigint AS v",
        "SELECT '12:30:45'::time AS v",
        "SELECT CAST(x AS integer) AS v FROM t",
        "SELECT CAST(x AS numeric(10, 2)) AS v FROM t",
        "SELECT CAST(x AS time) AS v FROM t",
    ],
)
def test_a_literal_clickhouse_reads_exactly_and_a_column_are_written_as_before(sql):
    """No row rewrites a literal ClickHouse already reads as PostgreSQL does, nor a column's cast."""
    from provisa.transpiler.polyfill import clickhouse
    import sqlglot

    tree = sqlglot.parse_one(sql, read="postgres")
    for node in tree.walk():
        for read in (clickhouse.timed, clickhouse.integer_read, clickhouse.numeric_read):
            assert read(node, "clickhouse") is None, (read.__name__, node.sql("postgres"))


def test_a_date_column_moved_by_days_keeps_clickhouse_s_own():
    """A date whose type the statement does not say: ClickHouse's own arithmetic, as stated."""
    assert "throwIf" not in transpile("SELECT d + 1 AS v FROM t", "clickhouse", generated=False)


@pytest.mark.parametrize("name", ["time", "integer", "numeric input", "date + integer"])
def test_each_literal_row_is_the_grid_s_and_states_its_limitation(name):
    feature = next(f for f in GRID.features if f.name == name)
    assert feature.read.limitations["clickhouse"]
    assert name in {f.name for f in GRID.on("clickhouse")}


@pytest.mark.parametrize("name", ["date_pli", "date_mii", "integer_pl_date"])
def test_each_day_row_states_its_bound_on_clickhouse(name):
    feature = next(f for f in GRID.features if f.name == name)
    # Its bound; any other is a refusal of the translator's it states (polyfill.measured).
    (limitation,) = [lim for lim in feature.read.limitations["clickhouse"] if lim.refusal is None]
    assert "1900-01-01 .. 2299-12-31" in limitation.text and "22008" in limitation.text


@pytest.mark.parametrize("dialect", ["duckdb", "trino"])
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT '12:30'::time AS v",
        "SELECT 2147483648::integer AS v",
        "SELECT 0.1::numeric(38,30) AS v",
        "SELECT '2024-01-01'::date + 3 AS v",
    ],
)
def test_another_engine_is_unchanged(dialect, sql):
    if dialect == "trino" and "2147483648::integer" in sql:
        # Trino's own row reads the literal as int4in does (polyfill.trino_literals): 22003.
        with pytest.raises(RowRefused) as refused:
            transpile(sql, dialect, generated=False)
        assert refused.value.sqlstate == "22003"
        return
    written = transpile(sql, dialect, generated=False)
    assert "throwIf" not in written and "toTime64" not in written
