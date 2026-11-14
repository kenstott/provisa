# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7f9e14-8b3d-4a61-95e0-d4b8a1f6c037
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A timestamp with time zone on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND
COMES BEFORE A REFUSAL; maintainer 2026-11-13): the grid is read before a type ClickHouse holds
none of is refused, and a timestamptz, held as DateTime64(6, 'UTC'), answers its comparisons
exactly from 1900-01-01 to 2299-12-31 and raises outside.

The statements are the function support run's (43490334080): its NULL call and its values, under
its session (UTC). Each answer is PostgreSQL 17's."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import datetime
from typing import Any

import pytest
import sqlglot

from provisa.core.pg_error import PostgresError, from_engine
from provisa.core.session_time_zone import StatementZone, bind_zone, reset_zone
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.polyfill import GRID, instants, trino
from provisa.transpiler.transpile import transpile

_UTC = datetime.timezone.utc
_TZ = "timestamp with time zone"


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _one(runtime: ClickHouseFederationRuntime, sql: str) -> Any:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    return value


def _raised(runtime: ClickHouseFederationRuntime, sql: str) -> PostgresError:
    with pytest.raises(Exception) as raised:
        list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    error = from_engine(raised.value)
    assert error is not None, str(raised.value)[:300]
    return error


def _call(function: str, a: str, b: str, a_type: str = _TZ, b_type: str = _TZ) -> str:
    return f'SELECT pg_catalog."{function}"({a}::{a_type}, {b}::{b_type}) AS v'


# --- the gate: a row that answers is read before the refusal by type ---------------------------


def test_a_call_a_row_answers_is_read_before_its_type_is_refused():
    sql = _call("timestamptz_lt", "'2024-06-15 12:30:45+00'", "'1999-12-31 23:59:59+02'")
    written = transpile(sql, "clickhouse")
    assert "DateTime64(6, 'UTC')" in written and " < " in written


@pytest.mark.parametrize(
    ("sql", "function"),
    [
        ('SELECT pg_catalog."age"(NULL::timestamp with time zone) AS v', "age"),
        ('SELECT pg_catalog."timestamptz_mi"(NULL::timestamp with time zone, NULL::timestamp with time zone) AS v', "timestamptz_mi"),
        ('SELECT pg_catalog."date_trunc"(NULL::text, NULL::interval) AS v', "date_trunc"),
        ('SELECT pg_catalog."avg"(NULL::numeric) AS v', "avg"),
    ],
)  # fmt: skip
def test_a_call_no_row_answers_is_refused_as_it_was(sql, function):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    # A refusal by name, as before the grid was read first: not a row's (which carries 0A000).
    assert type(refused.value) is FunctionRefused
    assert str(refused.value) == f"{function}() has no form on clickhouse"


def test_a_literal_no_iso_form_writes_keeps_its_refusal_by_name():
    sql = _call("timestamptz_lt", "'infinity'", "'2024-06-15 12:30:45+00'")
    with pytest.raises(FunctionRefused, match=r"^timestamptz_lt\(\) has no form on clickhouse$"):
        transpile(sql, "clickhouse")


# --- the exact case ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        # The run's own statements, and PostgreSQL 17's answers to them.
        ('SELECT pg_catalog."timestamptz_lt"(NULL::timestamp with time zone, NULL::timestamp with time zone) AS v', None),
        (_call("timestamptz_lt", "'2024-06-15 12:30:45+00'", "'1999-12-31 23:59:59+02'"), 0),
        (_call("timestamptz_ge", "'2024-06-15 12:30:45+00'", "'1999-12-31 23:59:59+02'"), 1),
        (_call("timestamptz_eq", "'1999-12-31 23:59:59+02'", "'1999-12-31 21:59:59+00'"), 1),
        (_call("timestamptz_ne", "'2000-02-29 00:00:00-05'", "'2000-02-29 05:00:00+00'"), 0),
        (_call("timestamptz_cmp", "'1999-12-31 23:59:59+02'", "'2000-02-29 00:00:00-05'"), -1),
        (_call("timestamptz_cmp", "'2024-06-15 12:30:45+00'", "'1999-12-31 23:59:59+02'"), 1),
        (_call("timestamptz_cmp", "'2024-06-15 12:30:45+00'", "'2024-06-15 14:30:45+02'"), 0),
        ('SELECT pg_catalog."timestamptz_cmp"(NULL::timestamp with time zone, NULL::timestamp with time zone) AS v', None),
        (_call("timestamptz_larger", "'1999-12-31 23:59:59+02'", "'2000-02-29 00:00:00-05'"),
         datetime.datetime(2000, 2, 29, 5, 0, tzinfo=_UTC)),
        (_call("timestamptz_smaller", "'1999-12-31 23:59:59+02'", "'2000-02-29 00:00:00-05'"),
         datetime.datetime(1999, 12, 31, 21, 59, 59, tzinfo=_UTC)),
        ('SELECT pg_catalog."timestamptz_larger"(NULL::timestamp with time zone, \'2024-06-15 12:30:45+00\'::timestamp with time zone) AS v', None),
        # A date at its midnight, a timestamp as its wall time, in the session's zone (UTC).
        (_call("timestamptz_lt_date", "'2024-06-15 12:30:45+00'", "'1999-12-31'", _TZ, "date"), 0),
        (_call("timestamptz_eq_date", "'2000-02-29 00:00:00+00'", "'2000-02-29'", _TZ, "date"), 1),
        (_call("date_lt_timestamptz", "'1999-12-31'", "'1999-12-31 23:59:59+02'", "date", _TZ), 1),
        (_call("date_cmp_timestamptz", "'2000-02-29'", "'2000-02-29 00:00:00-05'", "date", _TZ), -1),
        (_call("timestamptz_gt_timestamp", "'1999-12-31 23:59:59+02'", "'1999-12-31 23:59:59'",
               _TZ, "timestamp without time zone"), 0),
        (_call("timestamp_eq_timestamptz", "'1999-12-31 21:59:59'", "'1999-12-31 23:59:59+02'",
               "timestamp without time zone", _TZ), 1),
        (_call("timestamp_cmp_timestamptz", "'2024-06-15 12:30:45'", "'2024-06-15 12:30:45.000001+00'",
               "timestamp without time zone", _TZ), -1),
    ],
)  # fmt: skip
def test_comparisons_of_an_instant_answer_postgresql_on_clickhouse(runtime, sql, answer):
    assert _one(runtime, sql) == answer


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "snowflake"])
def test_every_other_dialect_keeps_what_it_had(dialect):
    sql = _call("timestamptz_lt", "'2024-06-15 12:30:45+00'", "'1999-12-31 23:59:59+02'")
    call = sqlglot.parse_one(sql, read="postgres").selects[0].this.expression  # past pg_catalog
    assert len(instants.FEATURES) == 37 + 12
    for feature in instants.FEATURES:
        (rule,) = feature.read.rules
        # A row of no pattern (max, min, date: other rows read those names; date_part, extract: a
        # pattern of them is sqlglot's Extract) writes ClickHouse only.
        assert rule.forms[dialect] is trino.KEPT if rule.pattern else dialect not in rule.forms
    applied = GRID.read(call, dialect)
    assert applied is not None and applied.form is trino.KEPT


# --- the bound: at it, the answer; past it, the raise -------------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (_call("timestamptz_eq", "'1900-01-01 00:00:00+00'", "'1900-01-01 02:00:00+02'"), 1),
        (_call("timestamptz_lt", "'2299-12-31 23:59:59.999998+00'", "'2299-12-31 23:59:59.999999+00'"), 1),
        (_call("timestamptz_cmp", "'2299-12-31 23:59:59.999999+00'", "'1900-01-01 00:00:00+00'"), 1),
        (_call("date_le_timestamptz", "'1900-01-01'", "'1900-01-01 00:00:00+00'", "date", _TZ), 1),
        (_call("timestamptz_ge_date", "'2299-12-31 23:59:59.999999+00'", "'2299-12-31'", _TZ, "date"), 1),
    ],
)  # fmt: skip
def test_a_value_at_the_bound_answers(runtime, sql, answer):
    assert _one(runtime, sql) == answer


@pytest.mark.parametrize(
    ("function", "sql"),
    [
        ("timestamptz_lt", _call("timestamptz_lt", "'1899-12-31 23:59:59.999999+00'", "'2024-06-15 12:30:45+00'")),
        # An offset moves the instant past the bound: 01:00 at +02 is 23:00 of the day before in UTC.
        ("timestamptz_eq", _call("timestamptz_eq", "'1900-01-01 01:00:00+02'", "'2024-06-15 12:30:45+00'")),
        ("timestamptz_cmp", _call("timestamptz_cmp", "'2024-06-15 12:30:45+00'", "'2300-01-01 00:00:00+00'")),
        ("timestamptz_larger", _call("timestamptz_larger", "'2299-12-31 23:00:00-05'", "'2024-06-15 12:30:45+00'")),
        ("timestamp_ne_timestamptz", _call("timestamp_ne_timestamptz", "'2300-01-01 00:00:00'",
                                           "'2024-06-15 12:30:45+00'", "timestamp without time zone", _TZ)),
    ],
)  # fmt: skip
def test_a_value_past_the_bound_raises(runtime, function, sql):
    error = _raised(runtime, sql)
    assert (error.sqlstate, str(error)) == ("0A000", instants.out_of_bound(function))


def test_a_date_past_the_bound_is_refused_by_the_row_of_its_literal():
    # date_in's row reads a date literal first, and raises 0A000 for one ClickHouse holds none of.
    sql = _call("date_lt_timestamptz", "'1899-12-31'", "'2024-06-15 12:30:45+00'", "date", _TZ)
    with pytest.raises(RowRefused) as refused:
        transpile(sql, "clickhouse")
    assert refused.value.sqlstate == "0A000"
    assert str(refused.value).startswith('date_in() of "1899-12-31" has no form on clickhouse')


def test_a_null_beside_a_value_past_the_bound_is_null(runtime):
    sql = _call("timestamptz_lt", "'1850-01-01 00:00:00+00'", "NULL")
    assert _one(runtime, sql) is None


def test_the_stated_bound_is_the_one_enforced():
    feature = next(f for f in GRID.features if f.name == "timestamptz_lt")
    (stated,) = feature.read.limitations["clickhouse"]
    assert instants.BOUND in stated.text
    assert instants.FIRST == datetime.datetime(1900, 1, 1)
    assert instants.LAST == datetime.datetime(2299, 12, 31, 23, 59, 59, 999999)


# --- the session's zone ----------------------------------------------------------------------


@pytest.fixture()
def off_utc():
    token = bind_zone(StatementZone("America/New_York", name="America/New_York"))
    yield
    reset_zone(token)


@pytest.mark.parametrize(
    "sql",
    [
        _call("timestamptz_lt", "'2024-06-15 12:30:45'", "'2024-06-15 12:30:45+00'"),
        _call("date_lt_timestamptz", "'2024-06-15'", "NULL", "date", _TZ),
    ],
)
def test_off_utc_a_value_read_in_the_session_zone_is_refused_by_name(off_utc, sql):
    with pytest.raises(FunctionRefused):
        transpile(sql, "clickhouse")


# --- a function of one instant, and date_trunc of one --------------------------------------------

_SAMPLES = ("'2024-06-15 12:30:45+00'", "'1999-12-31 23:59:59+02'", "'2000-02-29 00:00:00-05'")


def _of(function: str, value: str) -> str:
    return f'SELECT pg_catalog."{function}"({value}::{_TZ}) AS v'


def _aggregate(function: str, *values: str) -> str:
    rows = ", ".join(f"({v}::{_TZ})" for v in values)
    return f'SELECT pg_catalog."{function}"(c1) AS v FROM (VALUES {rows}) AS t(c1)'


def _trunc(unit: str, value: str) -> str:
    return f"SELECT pg_catalog.\"date_trunc\"('{unit}'::text, {value}::{_TZ}) AS v"


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        # The run's statements (an aggregate reads its values from a VALUES list), PostgreSQL 17's answers.
        (_aggregate("max", *_SAMPLES), datetime.datetime(2024, 6, 15, 12, 30, 45, tzinfo=_UTC)),
        (_aggregate("min", *_SAMPLES), datetime.datetime(1999, 12, 31, 21, 59, 59, tzinfo=_UTC)),
        (_aggregate("max", "NULL", "'2000-02-29 00:00:00-05'"), datetime.datetime(2000, 2, 29, 5, tzinfo=_UTC)),
        (_aggregate("min", "NULL", "NULL"), None),
        (_of("isfinite", _SAMPLES[0]), True),
        (_of("isfinite", "NULL"), None),
        (_of("date", _SAMPLES[0]), datetime.date(2024, 6, 15)),
        (_of("date", _SAMPLES[1]), datetime.date(1999, 12, 31)),
        (_of("date", _SAMPLES[2]), datetime.date(2000, 2, 29)),
        (_of("date", "NULL"), None),
        (_trunc("day", _SAMPLES[1]), datetime.datetime(1999, 12, 31, tzinfo=_UTC)),
        (_trunc("hour", _SAMPLES[2]), datetime.datetime(2000, 2, 29, 5, tzinfo=_UTC)),
        (_trunc("month", _SAMPLES[0]), datetime.datetime(2024, 6, 1, tzinfo=_UTC)),
        ('SELECT pg_catalog."date_trunc"(NULL::text, NULL::timestamp with time zone) AS v', None),
    ],
)  # fmt: skip
def test_a_function_of_one_instant_answers_postgresql_on_clickhouse(runtime, sql, answer):
    assert _one(runtime, sql) == answer


@pytest.mark.parametrize(
    ("value", "wall"),
    [
        (_SAMPLES[1], datetime.datetime(1999, 12, 31, 21, 59, 59)),
        ("'2024-06-15 12:30:45.000001+00'", datetime.datetime(2024, 6, 15, 12, 30, 45, 1)),
    ],
)
def test_timestamp_of_an_instant_is_its_wall_time_in_utc(runtime, value, wall):
    # PostgreSQL's timestamp holds no zone: ClickHouse's DateTime64(6, 'UTC') holds the same wall time.
    assert _one(runtime, _of("timestamp", value)).replace(tzinfo=None) == wall


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (_of("date", "'2299-12-31 23:59:59.999999+00'"), datetime.date(2299, 12, 31)),
        # An offset moves the instant inside the bound: 23:00 at -02 is 01:00 of 1900-01-01 in UTC.
        (_of("date", "'1899-12-31 23:00:00-02'"), datetime.date(1900, 1, 1)),
        (_aggregate("min", "'1900-01-01 00:00:00+00'", "'2299-12-31 23:59:59.999999+00'"),
         datetime.datetime(1900, 1, 1, tzinfo=_UTC)),
        (_trunc("year", "'2299-12-31 23:59:59.999999+00'"), datetime.datetime(2299, 1, 1, tzinfo=_UTC)),
    ],
)  # fmt: skip
def test_a_function_of_one_instant_at_the_bound_answers(runtime, sql, answer):
    assert _one(runtime, sql) == answer


@pytest.mark.parametrize(
    ("function", "sql"),
    [
        ("date", _of("date", "'1900-01-01 01:00:00+02'")),
        ("timestamp", _of("timestamp", "'2300-01-01 00:00:00+00'")),
        # One row of a VALUES list past the bound raises the aggregate, whatever the others hold.
        ("max", _aggregate("max", "'2024-06-15 12:30:45+00'", "'2300-01-01 00:00:00+00'")),
        ("min", _aggregate("min", "NULL", "'1899-12-31 23:59:59.999999+00'")),
        ("date_trunc", _trunc("day", "'1899-12-31 12:00:00+00'")),
    ],
)  # fmt: skip
def test_a_function_of_one_instant_past_the_bound_raises(runtime, function, sql):
    error = _raised(runtime, sql)
    assert (error.sqlstate, str(error)) == ("0A000", instants.out_of_bound(function))


def test_isfinite_reads_only_that_a_value_is_there(runtime):
    # PostgreSQL: every instant it holds but infinity is finite; past the bound too.
    assert _one(runtime, _of("isfinite", "'1850-01-01 00:00:00+00'")) is True


@pytest.mark.parametrize(
    ("sql", "function"),
    [
        (_of("date", "'infinity'"), "date"),
        (_aggregate("max", "'2024-06-15 12:30:45+00'", "'infinity'"), "max"),
        # A row of another type: PostgreSQL's coercion of it to an instant is not read here.
        ('SELECT pg_catalog."max"(c1) AS v FROM (VALUES (\'2024-06-15 12:30:45+00\'::timestamp with time zone), '
         "('2024-06-15'::date)) AS t(c1)", "max"),
    ],
)  # fmt: skip
def test_a_function_of_one_instant_the_row_does_not_read_is_refused_by_name(sql, function):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    assert type(refused.value) is FunctionRefused
    assert str(refused.value) == f"{function}() has no form on clickhouse"


@pytest.mark.parametrize(
    "sql",
    [
        _of("date", _SAMPLES[0]),
        _of("timestamp", _SAMPLES[0]),
        _trunc("day", _SAMPLES[0]),
        _aggregate("max", *_SAMPLES),
        _of("isfinite", _SAMPLES[0]),
    ],
)
def test_off_utc_a_function_of_one_instant_read_in_the_session_zone_is_refused(off_utc, sql):
    with pytest.raises(FunctionRefused):
        transpile(sql, "clickhouse")


def test_each_function_of_one_instant_states_its_bound():
    for name in (
        "max(timestamptz)",
        "min(timestamptz)",
        "date(timestamptz)",
        "timestamp(timestamptz)",
        "date_trunc",
    ):
        feature = next(f for f in GRID.features if f.name == name)
        for stated in feature.read.limitations["clickhouse"]:  # date_trunc: and at a zone
            assert instants.BOUND in stated.text and "{" not in stated.text
    isfinite = next(f for f in GRID.features if f.name == "isfinite(timestamptz)")
    (stated,) = isfinite.read.limitations["clickhouse"]
    assert "{" not in stated.text and "infinity" in stated.text


# --- date_part, extract, overlaps, timezone, timestamptz(precision) of an instant -----------------


def _part(function: str, part: str, value: str) -> str:
    return f'SELECT pg_catalog."{function}"({part}::text, {value}::{_TZ}) AS v'


def _overlaps(s1: str, e1: str, s2: str, e2: str) -> str:
    return f'SELECT pg_catalog."overlaps"({s1}::{_TZ}, {e1}::{_TZ}, {s2}::{_TZ}, {e2}::{_TZ}) AS v'


def _at(zone: str, value: str) -> str:
    return f'SELECT pg_catalog."timezone"({zone}::text, {value}::{_TZ}) AS v'


def _precise(value: str, digits: str) -> str:
    return f'SELECT pg_catalog."timestamptz"({value}::{_TZ}, {digits}) AS v'


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (_part("date_part", "'year'", _SAMPLES[0]), 2024),
        (_part("date_part", "'month'", _SAMPLES[1]), 12),
        # 2000-02-29 00:00 at -05 is 05:00 of a Tuesday in UTC.
        (_part("date_part", "'dow'", _SAMPLES[2]), 2),
        (_part("date_part", "'hour'", _SAMPLES[1]), 21),
        (_part("date_part", "'doy'", _SAMPLES[2]), 60),
        (_part("extract", "'day'", _SAMPLES[1]), 31),
        (_part("extract", "'year'", "'2299-12-31 23:59:59.999999+00'"), 2299),
        (_part("date_part", "NULL", _SAMPLES[0]), None),
        (_part("date_part", "'year'", "NULL"), None),
        (_overlaps("'2000-01-01 00:00:00+00'", "'2000-01-10 00:00:00+00'",
                   "'2000-01-05 00:00:00+00'", "'2000-01-20 00:00:00+00'"), True),
        # A period's ends in either order; periods that only touch do not overlap.
        (_overlaps("'2000-01-10 00:00:00+00'", "'2000-01-01 00:00:00+00'",
                   "'2000-01-10 00:00:00+00'", "'2000-01-20 00:00:00+00'"), False),
        (_overlaps("'2000-01-01 00:00:00+00'", "'2000-01-10 00:00:00+00'",
                   "'2000-01-05 00:00:00+00'", "NULL"), True),
        (_overlaps("'2000-01-01 00:00:00+00'", "NULL",
                   "'2000-01-05 00:00:00+00'", "'2000-01-10 00:00:00+00'"), None),
        (_at("'Asia/Tokyo'", _SAMPLES[1]), datetime.datetime(2000, 1, 1, 6, 59, 59)),
        # June in New York: EDT, four hours west of UTC.
        (_at("'America/New_York'", _SAMPLES[0]), datetime.datetime(2024, 6, 15, 8, 30, 45)),
        (_at("'UTC'", _SAMPLES[2]), datetime.datetime(2000, 2, 29, 5)),
        (_at("NULL", _SAMPLES[0]), None),
        (_at("'Asia/Tokyo'", "NULL"), None),
        (f'SELECT pg_catalog."timezone"({_SAMPLES[1]}::{_TZ}) AS v', datetime.datetime(1999, 12, 31, 21, 59, 59)),
        (_precise("'2024-06-15 12:30:45.123456+00'", "2"), datetime.datetime(2024, 6, 15, 12, 30, 45, 120000)),
        # Half away from 2000-01-01: up after it, down before it.
        (_precise("'2024-06-15 12:30:45.125+00'", "2"), datetime.datetime(2024, 6, 15, 12, 30, 45, 130000)),
        (_precise("'1999-12-31 23:59:59.125+00'", "2"), datetime.datetime(1999, 12, 31, 23, 59, 59, 120000)),
        (_precise("'1999-12-31 23:59:59.5+00'", "0"), datetime.datetime(1999, 12, 31, 23, 59, 59)),
        (_precise("'2000-01-01 00:00:00.5+00'", "0"), datetime.datetime(2000, 1, 1, 0, 0, 1)),
        (_precise(_SAMPLES[1], "6"), datetime.datetime(1999, 12, 31, 21, 59, 59)),
        (_precise(_SAMPLES[1], "NULL"), None),
        (_precise("NULL", "3"), None),
    ],
)  # fmt: skip
def test_a_call_of_an_instant_and_its_other_arguments_answers_postgresql(runtime, sql, answer):
    value = _one(runtime, sql)
    if isinstance(value, datetime.datetime):
        value = value.replace(tzinfo=None)  # PostgreSQL's timestamp, or the UTC wall time
    assert value == answer


def test_timezone_of_a_column_of_values_reads_each_row(runtime):
    sql = (
        "SELECT pg_catalog.\"timezone\"('Asia/Tokyo'::text, c1) AS v FROM (VALUES "
        f"({_SAMPLES[0]}::{_TZ}), ({_SAMPLES[1]}::{_TZ}), (NULL::{_TZ})) AS t(c1)"
    )
    rows = [r[0] for r in runtime.run_sync(transpile(sql, "clickhouse")).iter_rows()]
    walls = [r and r.replace(tzinfo=None) for r in rows]
    # A VALUES list's rows reach ClickHouse as a UNION ALL: their order is not the statement's.
    assert sorted(walls, key=lambda w: (w is None, w or datetime.datetime.min)) == [
        datetime.datetime(2000, 1, 1, 6, 59, 59),
        datetime.datetime(2024, 6, 15, 21, 30, 45),
        None,
    ]


@pytest.mark.parametrize(
    ("function", "sql"),
    [
        ("overlaps", _overlaps("'1850-01-01 00:00:00+00'", "NULL", "'2000-01-05 00:00:00+00'", "NULL")),
        ("date_part", _part("date_part", "'year'", "'2300-01-01 00:00:00+00'")),
        # Inside the bound in UTC, past it at Tokyo's wall time.
        ("timezone", _at("'Asia/Tokyo'", "'2299-12-31 20:00:00+00'")),
        ("timezone", _at("'America/New_York'", "'1900-01-01 02:00:00+00'")),
        # Inside the bound, rounded past it.
        ("timestamptz", _precise("'2299-12-31 23:59:59.999999+00'", "0")),
        ("timestamptz", _precise("'1899-12-31 23:59:59+00'", "3")),
    ],
)  # fmt: skip
def test_a_call_of_an_instant_past_the_bound_raises(runtime, function, sql):
    error = _raised(runtime, sql)
    assert (error.sqlstate, str(error)) == ("0A000", instants.out_of_bound(function))


@pytest.mark.parametrize(
    ("sql", "function"),
    [
        (_part("date_part", "'second'", _SAMPLES[0]), "date_part"),
        (_part("extract", "'epoch'", _SAMPLES[0]), "extract"),
        (_at("'EST'", _SAMPLES[0]), "timezone"),
        (_at("'+05'", _SAMPLES[0]), "timezone"),
        (_precise(_SAMPLES[0], "7"), "timestamptz"),
        (_precise(_SAMPLES[0], "-1"), "timestamptz"),
        # An instant ClickHouse computes: its wall time at a zone is not read here first.
        ("SELECT pg_catalog.\"timezone\"('Asia/Tokyo'::text, pg_catalog.\"timestamptz_larger\"("
         f"{_SAMPLES[0]}::{_TZ}, {_SAMPLES[1]}::{_TZ})) AS v", "timezone"),
    ],
)  # fmt: skip
def test_a_call_of_an_instant_the_row_does_not_read_is_refused_by_name(sql, function):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    assert type(refused.value) is FunctionRefused
    assert str(refused.value) == f"{function}() has no form on clickhouse"


@pytest.mark.parametrize(
    "sql",
    [
        _part("date_part", "'year'", _SAMPLES[0]),
        _at("'Asia/Tokyo'", _SAMPLES[0]),
        _precise(_SAMPLES[0], "2"),
        _overlaps(*_SAMPLES, _SAMPLES[0]),
        f'SELECT pg_catalog."timezone"({_SAMPLES[0]}::{_TZ}) AS v',
    ],
)
def test_off_utc_a_call_of_an_instant_is_refused_by_name(off_utc, sql):
    with pytest.raises(FunctionRefused):
        transpile(sql, "clickhouse")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.\"date_part\"('hour'::text, '1999-12-31 23:59:59'::timestamp) AS v",
        "SELECT pg_catalog.\"overlaps\"('1999-12-31'::timestamp, '2000-01-02'::timestamp, "
        "'2000-01-01'::timestamp, '2000-01-03'::timestamp) AS v",
    ],
)
def test_a_timestamp_keeps_its_own_form(sql):
    # The rows read an instant; a timestamp's call is the old table's still.
    assert "DateTime64(6, 'UTC')" in transpile(sql, "clickhouse")


# --- the day of an instant a call computes (J1303's open item) ----------------------------------


def _pair(function: str, a: str, b: str) -> str:
    return f'pg_catalog."{function}"({a}::{_TZ}, {b}::{_TZ})'


#: Each spelling of the day of a value: pg_catalog's call (the grid's walk), the bare call and the
#: cast (each read in the session's zone, zoned_instants).
_DAYS = (
    lambda v: f'SELECT pg_catalog."date"({v}) AS v',
    lambda v: f"SELECT date({v}) AS v",
    lambda v: f"SELECT ({v})::date AS v",
)


@pytest.mark.parametrize("day", _DAYS)
@pytest.mark.parametrize(
    ("value", "answer"),
    [
        # ClickHouse's DATE() is toDate, a Date of 1970 to 2149: these days were answered as
        # 2129-11-19 and 2060-07-26 before. Its day is now Date32's, PostgreSQL 17's answer.
        (_pair("timestamptz_larger", "'1950-06-15 12:00:00+00'", "'1940-01-01 00:00:00+00'"),
         datetime.date(1950, 6, 15)),
        (_pair("timestamptz_smaller", "'2250-06-15 12:00:00+00'", "'2240-01-01 00:00:00+00'"),
         datetime.date(2240, 1, 1)),
        (_pair("timestamptz_larger", "'1900-01-01 00:00:00+00'", "'1900-01-01 00:00:00+00'"),
         datetime.date(1900, 1, 1)),
        (_pair("timestamptz_smaller", "'2299-12-31 23:59:59.999999+00'", "NULL"), None),
        (_pair("timestamptz_larger", "'2299-12-31 23:59:59.999999+00'", "'2000-01-01 00:00:00+00'"),
         datetime.date(2299, 12, 31)),
        # A date a call computes: was 2129-11-20.
        ("'1950-06-15'::date + 1", datetime.date(1950, 6, 16)),
    ],
)  # fmt: skip
def test_the_day_of_a_value_a_call_computes_answers_postgresql(runtime, day, value, answer):
    assert _one(runtime, day(value)) == answer


@pytest.mark.parametrize(
    "spelled",
    [
        *_DAYS,
        lambda v: f"SELECT ({v})::text AS v",
        lambda v: f"SELECT date_trunc('day', {v}) AS v",
    ],
)
@pytest.mark.parametrize(
    ("function", "value"),
    [
        # The inner call's own bound: answered as 2299-12-31 and 1900-01-01 before, a clamped day.
        ("timestamptz_larger", _pair("timestamptz_larger", "'2024-06-15 12:00:00+00'", "'2300-01-01 00:00:00+00'")),
        ("timestamptz_smaller", _pair("timestamptz_smaller", "'2024-06-15 12:00:00+00'", "'1800-01-01 00:00:00+00'")),
    ],
)  # fmt: skip
def test_the_day_of_a_call_past_the_bound_raises_the_call(runtime, spelled, function, value):
    error = _raised(runtime, spelled(value))
    assert (error.sqlstate, str(error)) == ("0A000", instants.out_of_bound(function))


# --- date_trunc at a zone, tstzrange's bounds, to_char of a NULL (J1305) -------------------------


def _trunc_at(unit: str, value: str, zone: str) -> str:
    return f"SELECT pg_catalog.\"date_trunc\"('{unit}'::text, {value}::{_TZ}, '{zone}'::text) AS v"


def _utc(*parts: int) -> datetime.datetime:
    return datetime.datetime(*parts, tzinfo=_UTC)


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ('SELECT pg_catalog."date_trunc"(NULL::text, NULL::timestamp with time zone, NULL::text) AS v', None),
        (_trunc_at("day", "NULL", "Asia/Tokyo"), None),
        # 20:30 UTC is 05:30 the next day in Tokyo: that day's midnight there.
        (_trunc_at("day", "'2024-06-15 20:30:45+00'", "Asia/Tokyo"), _utc(2024, 6, 15, 15)),
        (_trunc_at("month", "'2024-06-15 12:00:00+00'", "America/New_York"), _utc(2024, 6, 1, 4)),
        (_trunc_at("year", "'2024-06-15 12:00:00+00'", "Europe/London"), _utc(2024, 1, 1)),
        (_trunc_at("week", "'2024-06-15 12:00:00+00'", "UTC"), _utc(2024, 6, 10)),
        (_trunc_at("quarter", "'2024-06-15 12:00:00+00'", "Asia/Kolkata"), _utc(2024, 3, 31, 18, 30)),
        (_trunc_at("century", "'2024-06-15 12:00:00+00'", "UTC"), _utc(2001, 1, 1)),
        (_trunc_at("millennium", "'2024-06-15 12:00:00+00'", "UTC"), _utc(2001, 1, 1)),
        (_trunc_at("decade", "'2024-06-15 12:00:00+00'", "UTC"), _utc(2020, 1, 1)),
        (_trunc_at("minute", "'2024-06-15 12:34:56.789+00'", "Asia/Kathmandu"), _utc(2024, 6, 15, 12, 34)),
        (_trunc_at("milliseconds", "'2024-06-15 12:34:56.789123+00'", "UTC"), _utc(2024, 6, 15, 12, 34, 56, 789000)),
        # The second 01:30 of a fall-back (EST): an hour keeps the instant's own offset.
        (_trunc_at("hour", "'2024-11-03 06:30:00+00'", "America/New_York"), _utc(2024, 11, 3, 6)),
        # A day read back at the offset of its midnight (EDT), not the instant's (EST).
        (_trunc_at("day", "'2024-11-03 12:00:00+00'", "America/New_York"), _utc(2024, 11, 3, 4)),
        # A start the bound holds, of an instant whose day at the zone is past it.
        (_trunc_at("day", "'2299-12-31 20:00:00+00'", "Asia/Tokyo"), _utc(2299, 12, 31, 15)),
        # Refused by name before J1305: the start of a unit in the zone the call names.
        (_trunc_at("day", "'1999-12-31 23:59:59+02'", "Asia/Tokyo"), _utc(1999, 12, 31, 15)),
        # The call written as PostgreSQL's syntax reads it too.
        ("SELECT date_trunc('day', '2024-06-15 20:30:45+00'::timestamptz, 'Asia/Tokyo') AS v", _utc(2024, 6, 15, 15)),
    ],
)  # fmt: skip
def test_date_trunc_at_a_zone_answers_postgresql(runtime, sql, answer):
    assert _one(runtime, sql) == answer


def test_date_trunc_at_a_zone_past_the_bound_raises(runtime):
    error = _raised(runtime, _trunc_at("day", "'2300-01-01 12:00:00+00'", "UTC"))
    assert (error.sqlstate, str(error)) == ("0A000", instants.out_of_bound("date_trunc"))
    # Its start at the zone is before the bound: 1900-01-01 00:30 UTC is 1899-12-31 in New York.
    error = _raised(runtime, _trunc_at("day", "'1900-01-01 00:30:00+00'", "America/New_York"))
    assert (error.sqlstate, str(error)) == ("0A000", instants.out_of_bound("date_trunc"))


@pytest.mark.parametrize(
    "sql",
    [
        # A zone abbreviation, an instant a row computes, a unit PostgreSQL refuses.
        _trunc_at("day", "'2024-06-15 12:00:00+00'", "EST"),
        "SELECT pg_catalog.\"date_trunc\"('day'::text, now(), 'Asia/Tokyo'::text) AS v",
        _trunc_at("fortnight", "'2024-06-15 12:00:00+00'", "UTC"),
        # A timestamp PostgreSQL reads as an instant first: was answered ignoring the zone.
        "SELECT pg_catalog.\"date_trunc\"('day'::text, '2024-06-15 20:30:45'::timestamp, 'Asia/Tokyo'::text) AS v",
    ],
)  # fmt: skip
def test_date_trunc_at_a_zone_the_row_does_not_read_is_refused_by_name(sql):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    assert str(refused.value) == "date_trunc() has no form on clickhouse"


def test_off_utc_date_trunc_at_a_zone_is_refused_by_name(off_utc):
    with pytest.raises(FunctionRefused):
        transpile(_trunc_at("day", "'2024-06-15 12:00:00+00'", "Asia/Tokyo"), "clickhouse")


def _range(lower: str, upper: str, bounds: str) -> str:
    return f"SELECT pg_catalog.\"tstzrange\"({lower}::{_TZ}, {upper}::{_TZ}, '{bounds}'::text) AS v"


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        # The run's statement: refused before (cast() has no form on clickhouse).
        (_range("NULL", "NULL", "[]"), "(,)"),
        (_range("NULL", "'2024-01-01 00:00:00+00'", "[]"), '(,"2024-01-01 00:00:00+00"]'),
        ("SELECT pg_catalog.\"tstzrange\"(c, NULL::timestamp with time zone, '[)'::text) AS v "
         "FROM (VALUES ('2024-01-01 00:00:00+00'::timestamp with time zone)) AS t(c)", '["2024-01-01 00:00:00+00",)'),
    ],
)  # fmt: skip
def test_tstzrange_of_its_bounds_answers_postgresql(runtime, sql, answer):
    assert _one(runtime, sql) == answer


def test_tstzrange_of_a_row_past_the_bound_raises(runtime):
    sql = (
        "SELECT pg_catalog.\"tstzrange\"(c, d, '(]'::text) AS v FROM (VALUES "
        f"('2024-01-01 00:00:00+00'::{_TZ}, '2300-01-01 00:00:00+00'::{_TZ})) AS t(c, d)"
    )
    error = _raised(runtime, sql)
    assert (error.sqlstate, str(error)) == ("0A000", instants.out_of_bound("tstzrange"))


@pytest.mark.parametrize(
    "sql",
    [
        f'SELECT pg_catalog."to_char"(NULL::{_TZ}, NULL::text) AS v',
        f"SELECT pg_catalog.\"to_char\"('2024-01-01 00:00:00+00'::{_TZ}, NULL::text) AS v",
        f"SELECT pg_catalog.\"to_char\"(NULL::{_TZ}, 'YYYY'::text) AS v",
    ],
)
def test_to_char_of_a_null_is_null(runtime, sql):
    assert _one(runtime, sql) is None


def test_to_char_of_an_instant_keeps_its_refusal_by_name():
    # The template reaches the translator as strftime text: YYYY and a literal %Y are the same.
    sql = f"SELECT pg_catalog.\"to_char\"('2024-01-01 00:00:00+00'::{_TZ}, 'YYYY'::text) AS v"
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    assert str(refused.value) == "to_char() has no form on clickhouse"
