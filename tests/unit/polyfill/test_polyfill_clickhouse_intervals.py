# Copyright (c) 2026 Kenneth Stott
# Canary: 7ad0dc55-1fd4-4272-ab15-194b9bc85fa6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An interval on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND COMES BEFORE A
REFUSAL): held as Tuple(months, days, microseconds), each call of the first tranche answers
PostgreSQL's answer, a field that overflows raises 22008, a timestamp outside DateTime64's bound
raises 0A000, and a form no row reads keeps its refusal by name.

The answers are PostgreSQL's (18.4, a throwaway local cluster; the interval code is 17's): the
function support run's value calls (its samples), others of each call, and the input forms the
literal reader reads (~/provisa-agent-logs/J1310-oracle)."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import datetime
import decimal
from typing import Any

import pytest

from provisa.core.ir_types import interval_parts
from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.pgwire.cast_types import interval_of_text
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.pg_type_literals import interval_text as pg_interval_text
from provisa.transpiler.polyfill import GRID, intervals
from provisa.transpiler.transpile import transpile

SAMPLES = [
    (
        "SELECT pg_catalog.\"interval_eq\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "true",
    ),
    (
        "SELECT pg_catalog.\"interval_ne\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "false",
    ),
    (
        "SELECT pg_catalog.\"interval_lt\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "false",
    ),
    (
        "SELECT pg_catalog.\"interval_le\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "true",
    ),
    (
        "SELECT pg_catalog.\"interval_gt\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "false",
    ),
    (
        "SELECT pg_catalog.\"interval_ge\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "true",
    ),
    (
        "SELECT pg_catalog.\"interval_cmp\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"interval_larger\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "1 day 02:03:04",
    ),
    (
        "SELECT pg_catalog.\"interval_smaller\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "1 day 02:03:04",
    ),
    (
        "SELECT pg_catalog.\"interval_pl\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "2 days 04:06:08",
    ),
    (
        "SELECT pg_catalog.\"interval_mi\"('1 day 02:03:04'::interval, '1 day 02:03:04'::interval) AS v",
        "00:00:00",
    ),
    ("SELECT pg_catalog.\"interval_um\"('1 day 02:03:04'::interval) AS v", "-1 days -02:03:04"),
    ("SELECT pg_catalog.\"justify_days\"('1 day 02:03:04'::interval) AS v", "1 day 02:03:04"),
    ("SELECT pg_catalog.\"justify_hours\"('1 day 02:03:04'::interval) AS v", "1 day 02:03:04"),
    ("SELECT pg_catalog.\"justify_interval\"('1 day 02:03:04'::interval) AS v", "1 day 02:03:04"),
    ("SELECT pg_catalog.\"isfinite\"('1 day 02:03:04'::interval) AS v", "true"),
    ("SELECT pg_catalog.\"extract\"('year', '1 day 02:03:04'::interval) AS v", "0"),
    ("SELECT pg_catalog.\"date_part\"('year', '1 day 02:03:04'::interval) AS v", "0"),
    (
        "SELECT pg_catalog.\"timestamp_pl_interval\"('2024-06-15 12:30:45'::timestamp without time zone, '1 day 02:03:04'::interval) AS v",
        "2024-06-16 14:33:49",
    ),
    (
        "SELECT pg_catalog.\"timestamp_mi_interval\"('2024-06-15 12:30:45'::timestamp without time zone, '1 day 02:03:04'::interval) AS v",
        "2024-06-14 10:27:41",
    ),
    (
        "SELECT pg_catalog.\"interval_pl_timestamp\"('1 day 02:03:04'::interval, '2024-06-15 12:30:45'::timestamp without time zone) AS v",
        "2024-06-16 14:33:49",
    ),
    (
        "SELECT pg_catalog.\"date_pl_interval\"('2024-06-15'::date, '1 day 02:03:04'::interval) AS v",
        "2024-06-16 02:03:04",
    ),
    (
        "SELECT pg_catalog.\"date_mi_interval\"('2024-06-15'::date, '1 day 02:03:04'::interval) AS v",
        "2024-06-13 21:56:56",
    ),
    (
        "SELECT pg_catalog.\"interval_pl_date\"('1 day 02:03:04'::interval, '2024-06-15'::date) AS v",
        "2024-06-16 02:03:04",
    ),
    ("SELECT pg_catalog.\"interval_eq\"('3 mons'::interval, '3 mons'::interval) AS v", "true"),
    ("SELECT pg_catalog.\"interval_ne\"('3 mons'::interval, '3 mons'::interval) AS v", "false"),
    ("SELECT pg_catalog.\"interval_lt\"('3 mons'::interval, '3 mons'::interval) AS v", "false"),
    ("SELECT pg_catalog.\"interval_le\"('3 mons'::interval, '3 mons'::interval) AS v", "true"),
    ("SELECT pg_catalog.\"interval_gt\"('3 mons'::interval, '3 mons'::interval) AS v", "false"),
    ("SELECT pg_catalog.\"interval_ge\"('3 mons'::interval, '3 mons'::interval) AS v", "true"),
    ("SELECT pg_catalog.\"interval_cmp\"('3 mons'::interval, '3 mons'::interval) AS v", "0"),
    (
        "SELECT pg_catalog.\"interval_larger\"('3 mons'::interval, '3 mons'::interval) AS v",
        "3 mons",
    ),
    (
        "SELECT pg_catalog.\"interval_smaller\"('3 mons'::interval, '3 mons'::interval) AS v",
        "3 mons",
    ),
    ("SELECT pg_catalog.\"interval_pl\"('3 mons'::interval, '3 mons'::interval) AS v", "6 mons"),
    ("SELECT pg_catalog.\"interval_mi\"('3 mons'::interval, '3 mons'::interval) AS v", "00:00:00"),
    ("SELECT pg_catalog.\"interval_um\"('3 mons'::interval) AS v", "-3 mons"),
    ("SELECT pg_catalog.\"justify_days\"('3 mons'::interval) AS v", "3 mons"),
    ("SELECT pg_catalog.\"justify_hours\"('3 mons'::interval) AS v", "3 mons"),
    ("SELECT pg_catalog.\"justify_interval\"('3 mons'::interval) AS v", "3 mons"),
    ("SELECT pg_catalog.\"isfinite\"('3 mons'::interval) AS v", "true"),
    ("SELECT pg_catalog.\"extract\"('month', '3 mons'::interval) AS v", "3"),
    ("SELECT pg_catalog.\"date_part\"('month', '3 mons'::interval) AS v", "3"),
    (
        "SELECT pg_catalog.\"timestamp_pl_interval\"('1999-12-31 23:59:59'::timestamp without time zone, '3 mons'::interval) AS v",
        "2000-03-31 23:59:59",
    ),
    (
        "SELECT pg_catalog.\"timestamp_mi_interval\"('1999-12-31 23:59:59'::timestamp without time zone, '3 mons'::interval) AS v",
        "1999-09-30 23:59:59",
    ),
    (
        "SELECT pg_catalog.\"interval_pl_timestamp\"('3 mons'::interval, '1999-12-31 23:59:59'::timestamp without time zone) AS v",
        "2000-03-31 23:59:59",
    ),
    (
        "SELECT pg_catalog.\"date_pl_interval\"('1999-12-31'::date, '3 mons'::interval) AS v",
        "2000-03-31 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"date_mi_interval\"('1999-12-31'::date, '3 mons'::interval) AS v",
        "1999-09-30 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"interval_pl_date\"('3 mons'::interval, '1999-12-31'::date) AS v",
        "2000-03-31 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"interval_eq\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "true",
    ),
    (
        "SELECT pg_catalog.\"interval_ne\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "false",
    ),
    (
        "SELECT pg_catalog.\"interval_lt\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "false",
    ),
    (
        "SELECT pg_catalog.\"interval_le\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "true",
    ),
    (
        "SELECT pg_catalog.\"interval_gt\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "false",
    ),
    (
        "SELECT pg_catalog.\"interval_ge\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "true",
    ),
    (
        "SELECT pg_catalog.\"interval_cmp\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "0",
    ),
    (
        "SELECT pg_catalog.\"interval_larger\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "-1 years +2 days",
    ),
    (
        "SELECT pg_catalog.\"interval_smaller\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "-1 years +2 days",
    ),
    (
        "SELECT pg_catalog.\"interval_pl\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "-2 years +4 days",
    ),
    (
        "SELECT pg_catalog.\"interval_mi\"('-1 year 2 days'::interval, '-1 year 2 days'::interval) AS v",
        "00:00:00",
    ),
    ("SELECT pg_catalog.\"interval_um\"('-1 year 2 days'::interval) AS v", "1 year -2 days"),
    ("SELECT pg_catalog.\"justify_days\"('-1 year 2 days'::interval) AS v", "-11 mons -28 days"),
    ("SELECT pg_catalog.\"justify_hours\"('-1 year 2 days'::interval) AS v", "-1 years +2 days"),
    (
        "SELECT pg_catalog.\"justify_interval\"('-1 year 2 days'::interval) AS v",
        "-11 mons -28 days",
    ),
    ("SELECT pg_catalog.\"isfinite\"('-1 year 2 days'::interval) AS v", "true"),
    (
        "SELECT pg_catalog.\"extract\"('dow', '-1 year 2 days'::interval) AS v",
        ("error", "0A000", 'unit "dow" not supported for type interval'),
    ),
    (
        "SELECT pg_catalog.\"date_part\"('dow', '-1 year 2 days'::interval) AS v",
        ("error", "0A000", 'unit "dow" not supported for type interval'),
    ),
    (
        "SELECT pg_catalog.\"timestamp_pl_interval\"('2000-02-29 00:00:00'::timestamp without time zone, '-1 year 2 days'::interval) AS v",
        "1999-03-02 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"timestamp_mi_interval\"('2000-02-29 00:00:00'::timestamp without time zone, '-1 year 2 days'::interval) AS v",
        "2001-02-26 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"interval_pl_timestamp\"('-1 year 2 days'::interval, '2000-02-29 00:00:00'::timestamp without time zone) AS v",
        "1999-03-02 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"date_pl_interval\"('2000-02-29'::date, '-1 year 2 days'::interval) AS v",
        "1999-03-02 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"date_mi_interval\"('2000-02-29'::date, '-1 year 2 days'::interval) AS v",
        "2001-02-26 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"interval_pl_date\"('-1 year 2 days'::interval, '2000-02-29'::date) AS v",
        "1999-03-02 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"max\"(c1) AS v FROM (VALUES ('1 day 02:03:04'::interval), ('3 mons'::interval), ('-1 year 2 days'::interval)) AS t(c1)",
        "3 mons",
    ),
    (
        "SELECT pg_catalog.\"min\"(c1) AS v FROM (VALUES ('1 day 02:03:04'::interval), ('3 mons'::interval), ('-1 year 2 days'::interval)) AS t(c1)",
        "-1 years +2 days",
    ),
    (
        "SELECT pg_catalog.\"sum\"(c1) AS v FROM (VALUES ('1 day 02:03:04'::interval), ('3 mons'::interval), ('-1 year 2 days'::interval)) AS t(c1)",
        "-9 mons +3 days 02:03:04",
    ),
]
EXTRA = [
    ("SELECT pg_catalog.\"interval_lt\"('1 mon'::interval, '31 days'::interval) AS v", "true"),
    ("SELECT pg_catalog.\"interval_eq\"('1 mon'::interval, '30 days'::interval) AS v", "true"),
    (
        "SELECT pg_catalog.\"interval_cmp\"('-1 year 2 days'::interval, '3 mons'::interval) AS v",
        "-1",
    ),
    (
        "SELECT pg_catalog.\"interval_cmp\"('3 mons'::interval, '1 day 02:03:04'::interval) AS v",
        "1",
    ),
    (
        "SELECT pg_catalog.\"interval_larger\"('1 mon'::interval, '30 days'::interval) AS v",
        "30 days",
    ),
    (
        "SELECT pg_catalog.\"interval_smaller\"('-1 year 2 days'::interval, '3 mons'::interval) AS v",
        "-1 years +2 days",
    ),
    (
        "SELECT pg_catalog.\"interval_pl\"('-1 year 2 days'::interval, '3 mons'::interval) AS v",
        "-9 mons +2 days",
    ),
    (
        "SELECT pg_catalog.\"interval_mi\"('1 day 02:03:04'::interval, '3 mons'::interval) AS v",
        "-3 mons +1 day 02:03:04",
    ),
    (
        "SELECT pg_catalog.\"interval_um\"('-1 years -2 mons +3 days -04:05:06'::interval) AS v",
        "1 year 2 mons -3 days +04:05:06",
    ),
    ("SELECT pg_catalog.\"justify_days\"('-1 mon 35 days'::interval) AS v", "5 days"),
    ("SELECT pg_catalog.\"justify_hours\"('-1 day 25:00'::interval) AS v", "01:00:00"),
    (
        "SELECT pg_catalog.\"justify_interval\"('1 mon -1 day 25:00'::interval) AS v",
        "1 mon 01:00:00",
    ),
    (
        "SELECT pg_catalog.\"justify_interval\"('-1 mon 35 days -00:00:01'::interval) AS v",
        "4 days 23:59:59",
    ),
    ("SELECT pg_catalog.\"extract\"('epoch', '-1 year 2 days'::interval) AS v", "-31384800.000000"),
    ("SELECT pg_catalog.\"extract\"('second', '1 day 02:03:04.5'::interval) AS v", "4.500000"),
    ("SELECT pg_catalog.\"extract\"('milliseconds', '-00:00:01.25'::interval) AS v", "-1250.000"),
    ("SELECT pg_catalog.\"extract\"('quarter', '-4 mons'::interval) AS v", "-2"),
    ("SELECT pg_catalog.\"extract\"('week', '15 days'::interval) AS v", "2"),
    ("SELECT pg_catalog.\"extract\"('hour', '-1 day -25:00'::interval) AS v", "-25"),
    (
        "SELECT pg_catalog.\"date_part\"('epoch', '1 year 1 mon 1 day 00:00:01.5'::interval) AS v",
        "34236001.5",
    ),
    ("SELECT pg_catalog.\"date_part\"('second', '00:00:04.25'::interval) AS v", "4.25"),
    ("SELECT pg_catalog.\"extract\"('foo', '1 day'::interval) AS v", ("error", "22023")),
    ("SELECT pg_catalog.\"extract\"('julian', '1 day'::interval) AS v", ("error", "0A000")),
    (
        "SELECT pg_catalog.\"timestamp_pl_interval\"('2024-01-31 10:00:00'::timestamp, '1 mon'::interval) AS v",
        "2024-02-29 10:00:00",
    ),
    (
        "SELECT pg_catalog.\"timestamp_mi_interval\"('2024-03-31 00:00:00'::timestamp, '1 mon 1 day 01:00'::interval) AS v",
        "2024-02-27 23:00:00",
    ),
    (
        "SELECT pg_catalog.\"date_pl_interval\"('2000-02-29'::date, '1 year'::interval) AS v",
        "2001-02-28 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"date_mi_interval\"('2000-03-31'::date, '-11 mons'::interval) AS v",
        "2001-02-28 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"interval_pl_timestamp\"('13 mons -1 day'::interval, '1999-12-31 23:59:59'::timestamp) AS v",
        "2001-01-30 23:59:59",
    ),
]
READS = [
    ("1 day 02:03:04", "1 day 02:03:04"),
    ("3 mons", "3 mons"),
    ("-1 year 2 days", "-1 years +2 days"),
    ("1 hour", "01:00:00"),
    ("-05:30", "-05:30:00"),
    ("00:00", "00:00:00"),
    ("1 day", "1 day"),
    ("2 days", "2 days"),
    ("1 mon", "1 mon"),
    ("30 days", "30 days"),
    ("1 mon -1 day 25:00", "1 mon -1 days +25:00:00"),
    ("-1 day 25:00", "-1 days +25:00:00"),
    ("-1 mon 35 days", "-1 mons +35 days"),
    ("1 year 2 mons -3 days 04:05:06.7", "1 year 2 mons -3 days +04:05:06.7"),
    ("-1 years -2 mons +3 days -04:05:06", "-1 years -2 mons +3 days -04:05:06"),
    ("1day", "1 day"),
    ("-1year", "-1 years"),
    ("1.5 mons", "1 mon 15 days"),
    ("0.1 mons", "3 days"),
    ("1.1 years", "1 year 1 mon"),
    ("1.5 weeks", "10 days 12:00:00"),
    ("-1.5 days", "-1 days -12:00:00"),
    ("1.0000005 sec", "00:00:01"),
    ("0.0000005 sec", "00:00:00"),
    ("0.0000015 sec", "00:00:00.000001"),
    ("@ 1 day ago", "-1 days"),
    ("1 day 02:03 ago", "-1 days -02:03:00"),
    ("100:00:59", "100:00:59"),
    ("02:03:04.123456", "02:03:04.123456"),
    ("-02:03:04.000001", "-02:03:04.000001"),
    ("+3 hours", "03:00:00"),
    ("-3 hours 15 minutes", "-02:45:00"),
    ("1 millennium 2 centuries 3 decades 4 years", "1234 years"),
    ("5 mils", "5000 years"),
    ("2 decs", "20 years"),
    ("3 c", "300 years"),
    ("7 w", "49 days"),
    ("2 hrs 3 mins 4 secs", "02:03:04"),
    ("5 ms 6 us", "00:00:00.005006"),
    ("1.5 ms", "00:00:00.0015"),
    ("2.25 seconds", "00:00:02.25"),
    ("1.5 hours", "01:30:00"),
    ("0.75 minutes", "00:00:45"),
    ("1 microsecond", "00:00:00.000001"),
    ("4 milliseconds", "00:00:00.004"),
    ("10 microseconds", "00:00:00.00001"),
    ("999999 us", "00:00:00.999999"),
    ("1000000 us", "00:00:01"),
    ("178956970 years 7 mons", "178956970 years 7 mons"),
    ("-178956970 years -8 mons", "-178956970 years -8 mons"),
    ("2147483647 days", "2147483647 days"),
    ("-2147483648 days", "-2147483648 days"),
    ("2562047788 hours", "2562047788:00:00"),
    ("-2562047788 hours", "-2562047788:00:00"),
    ("35 days 1 second", "35 days 00:00:01"),
    ("-35 days -1 second", "-35 days -00:00:01"),
    ("13 mons", "1 year 1 mon"),
    ("-13 mons", "-1 years -1 mons"),
    ("-14 mons", "-1 years -2 mons"),
    ("-4 mons", "-4 mons"),
    ("11 mons", "11 mons"),
    ("12 mons", "1 year"),
    ("25 mons -1 day", "2 years 1 mon -1 days"),
    ("1 mon 01:00", "1 mon 01:00:00"),
    ("-1 mon 01:00", "-1 mons +01:00:00"),
    ("1 mon -01:00", "1 mon -01:00:00"),
    ("-29 days 23:59:59.999999", "-29 days +23:59:59.999999"),
    ("29 days 24:00", "29 days 24:00:00"),
    ("0.5 years", "6 mons"),
    ("1.25 decades", "12 years 6 mons"),
    ("0.3 years", "4 mons"),
    ("2.7 mons", "2 mons 21 days"),
    ("0.01 days", "00:14:24"),
]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _text(value: Any) -> str | None:
    """An answer as PostgreSQL's text of it, as pgwire writes the type the call returns."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime.datetime):
        text = value.strftime("%Y-%m-%d %H:%M:%S")
        return text + (f".{value.microsecond:06d}".rstrip("0") if value.microsecond else "")
    if isinstance(value, float):
        return repr(value).removesuffix(".0")
    if (
        isinstance(value, str)
        and value[:1] in "-0123456789"
        and value.replace("-", "").replace(".", "").isdigit()
    ):
        return str(decimal.Decimal(value))
    return str(value)


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> Any:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    return value


def _raised(runtime: ClickHouseFederationRuntime, sql: str) -> PostgresError:
    with pytest.raises(Exception) as raised:
        list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    error = from_engine(raised.value)
    assert error is not None, str(raised.value)[:300]
    return error


def _check(runtime: ClickHouseFederationRuntime, sql: str, want: Any) -> None:
    if isinstance(want, tuple):
        assert _raised(runtime, sql).sqlstate == want[1]
        return
    got = _answer(runtime, sql)
    if isinstance(got, str) and " " in got or got in ("00:00:00",):
        # an interval: interval_out's text, which pgwire reads back as the same interval
        assert pg_interval_text(*interval_parts(interval_of_text(got))) == got
    assert _text(got) == want


def _call(function: str, *args: str) -> str:
    return f'SELECT pg_catalog."{function}"({", ".join(args)}) AS v'


def _i(text: str) -> str:
    return f"'{text}'::interval"


# --- the run's statements -----------------------------------------------------------------------


@pytest.mark.parametrize(("sql", "want"), SAMPLES)
def test_the_run_s_value_call_answers_postgresql_s(runtime, sql, want):
    _check(runtime, sql, want)


@pytest.mark.parametrize(("sql", "want"), EXTRA)
def test_each_call_answers_postgresql_s(runtime, sql, want):
    _check(runtime, sql, want)


_NULLS = [
    *((f"interval_{name}", 2) for name in ("eq", "ne", "lt", "le", "gt", "ge", "cmp")),
    *((f"interval_{name}", 2) for name in ("larger", "smaller", "pl", "mi")),
    ("interval_um", 1),
    ("justify_days", 1),
    ("justify_hours", 1),
    ("justify_interval", 1),
    ("isfinite", 1),
]


@pytest.mark.parametrize(("function", "count"), _NULLS)
def test_the_run_s_null_call_answers_null(runtime, function, count):
    assert _answer(runtime, _call(function, *["NULL::interval"] * count)) is None


@pytest.mark.parametrize(
    "sql",
    [
        _call("extract", "NULL::text", "NULL::interval"),
        _call("date_part", "'dow'", "NULL::interval"),
        _call("extract", "'year'", "NULL::interval"),
        _call("timestamp_pl_interval", "NULL::timestamp without time zone", "NULL::interval"),
        _call("timestamp_mi_interval", "'2024-01-01 00:00:00'::timestamp", "NULL::interval"),
        _call("interval_pl_timestamp", "NULL::interval", "NULL::timestamp without time zone"),
        _call("date_pl_interval", "NULL::date", "NULL::interval"),
        _call("date_mi_interval", "NULL::date", "'1 day'::interval"),
        _call("interval_pl_date", "NULL::interval", "'2024-01-01'::date"),
        'SELECT pg_catalog."max"(c1) AS v FROM (VALUES (NULL::interval), (NULL::interval)) AS t(c1)',
        'SELECT pg_catalog."sum"(c1) AS v FROM (VALUES (NULL::interval)) AS t(c1)',
    ],
)
def test_a_null_argument_answers_null(runtime, sql):
    assert _answer(runtime, sql) is None


# --- the literal reader -------------------------------------------------------------------------


@pytest.mark.parametrize(("text", "want"), READS)
def test_the_reader_reads_postgresql_s_input(runtime, text, want):
    read = intervals.interval_read(text)
    assert read is not None
    assert intervals.interval_text(*read) == pg_interval_text(*read) == want
    assert _answer(runtime, _call("interval_pl", _i(text), _i("00:00"))) == want


@pytest.mark.parametrize(
    "text",
    [
        "P1Y2M3DT4H5M6S",
        "1-2",
        "1 2:03:04",
        "5",
        "infinity",
        "-infinity",
        "02:03.5",
        "1 quarter",
        "1 day 1 day",
        "1 hour 02:03",
        "",
        "02:60",
        "2147483648 days",
    ],
)
def test_a_form_the_reader_does_not_read_is_refused_by_name(text):
    assert intervals.interval_read(text) is None
    with pytest.raises(FunctionRefused, match="interval_pl"):
        transpile(_call("interval_pl", _i(text), _i("00:00")), "clickhouse")


# --- PostgreSQL's errors, and the bound ---------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        _call("interval_pl", _i("2147483647 mons"), _i("1 mon")),
        _call("interval_mi", _i("-2147483648 days"), _i("1 day")),
        _call("interval_um", _i("-178956970 years -8 mons")),
        _call("justify_days", "'2147483647 mons 30 days'::interval"),
        _call("justify_hours", "'2147483647 days 24:00'::interval"),
        _call("justify_interval", "'2147483647 mons 30 days'::interval"),
        _call(
            "timestamp_mi_interval",
            "'2000-01-01 00:00:00'::timestamp",
            _i("-178956970 years -8 mons"),
        ),
        "SELECT pg_catalog.\"sum\"(c1) AS v FROM (VALUES ('2147483647 days'::interval), "
        "('1 day'::interval)) AS t(c1)",
    ],
)
def test_a_field_that_overflows_raises_22008(runtime, sql):
    assert _raised(runtime, sql).sqlstate == "22008"


@pytest.mark.parametrize(
    "sql",
    [
        _call("timestamp_pl_interval", "'2299-12-31 00:00:00'::timestamp", _i("1 day")),
        _call("timestamp_mi_interval", "'1900-01-01 00:00:00'::timestamp", _i("00:00:00.000001")),
        _call("date_pl_interval", "'2024-06-15'::date", _i("300 years")),
        _call("interval_pl_date", _i("-200 years"), "'2024-06-15'::date"),
        # a step outside the bound: PostgreSQL answers 2299-12-16, its month step 2300-01-15
        _call("timestamp_pl_interval", "'2299-12-15 00:00:00'::timestamp", _i("1 mon -30 days")),
    ],
)
def test_a_timestamp_outside_the_bound_raises_0a000(runtime, sql):
    error = _raised(runtime, sql)
    assert error.sqlstate == "0A000"
    assert "1900-01-01 00:00:00 .. 2299-12-31 23:59:59.999999" in str(error)


def test_a_tie_of_intervals_written_apart_raises_0a000(runtime):
    values = "(VALUES ('1 mon'::interval), ('30 days'::interval)) AS t(c1)"
    for function in ("max", "min"):
        error = _raised(runtime, f'SELECT pg_catalog."{function}"(c1) AS v FROM {values}')
        assert error.sqlstate == "0A000"
    same = "(VALUES ('1 mon'::interval), ('1 mon'::interval), ('1 day'::interval)) AS t(c1)"
    assert _answer(runtime, f'SELECT pg_catalog."max"(c1) AS v FROM {same}') == "1 mon"


def test_a_sum_whose_running_total_may_overflow_raises_0a000(runtime):
    values = "(VALUES ('2147483647 days'::interval), ('1 day'::interval), ('-1 day'::interval))"
    error = _raised(runtime, f'SELECT pg_catalog."sum"(c1) AS v FROM {values} AS t(c1)')
    assert error.sqlstate == "0A000"


@pytest.mark.parametrize(
    ("unit", "state"), [("dow", "0A000"), ("julian", "0A000"), ("foo", "22023")]
)
def test_a_unit_an_interval_has_no_part_of_is_postgresql_s_error(runtime, unit, state):
    for function in ("extract", "date_part"):
        error = _raised(runtime, _call(function, f"'{unit}'", _i("1 day")))
        assert error.sqlstate == state
        assert f'unit "{unit}"' in str(error)


# --- nesting, operators, the select list ---------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        (
            _call(
                "interval_pl",
                _call("interval_um", _i("1 day 02:03"))[len("SELECT ") : -len(" AS v")],
                _i("3 mons"),
            ),
            "3 mons -1 days -02:03:00",
        ),
        (
            'SELECT pg_catalog."max"(pg_catalog."interval_pl"(c1, \'1 day\'::interval)) AS v '
            "FROM (VALUES ('1 mon'::interval), ('-2 days'::interval)) AS t(c1)",
            "1 mon 1 day",
        ),
        ("SELECT '1 day'::interval + '2 days 01:00'::interval AS v", "3 days 01:00:00"),
        ("SELECT '1 day'::interval - '2 days 01:00'::interval AS v", "-1 days -01:00:00"),
        ("SELECT -'1 day 01:00'::interval AS v", "-1 days -01:00:00"),
        ("SELECT '1 mon'::interval = '30 days'::interval AS v", True),
        ("SELECT '1 mon'::interval < '31 days'::interval AS v", True),
        (
            "SELECT '2024-01-31 10:00:00'::timestamp + '1 mon'::interval AS v",
            datetime.datetime(2024, 2, 29, 10, 0, tzinfo=datetime.timezone.utc),
        ),
        (
            'SELECT pg_catalog."isfinite"(pg_catalog."interval_pl"(\'1 year\'::interval, '
            "'1 day'::interval)) AS v",
            True,
        ),
    ],
)
def test_a_nested_call_and_an_operator_answer_postgresql_s(runtime, sql, want):
    assert _answer(runtime, sql) == want


def test_isfinite_of_an_instant_is_still_instants_row(runtime):
    assert _answer(runtime, _call("isfinite", "'2024-01-01 00:00:00+00'::timestamptz")) is True


@pytest.mark.parametrize("around", ["{} IS NULL"])
def test_an_interval_answer_read_by_no_row_here_is_refused_by_name(around):
    call = "pg_catalog.\"interval_pl\"('1 day'::interval, '1 hour'::interval)"
    with pytest.raises(FunctionRefused):
        transpile(f"SELECT {around.format(call)} AS v", "clickhouse")


def test_an_interval_operator_read_by_no_row_here_is_refused_by_name():
    """It names no function, so no older refusal reads it: on the base it reached ClickHouse as
    an INTERVAL type it has none of (an engine error)."""
    # * 1 is interval_mul's row since J1337: the outermost operator no row reads is the one named
    with pytest.raises(RowRefused, match="interval_mul"):
        transpile("SELECT ('1 day'::interval + '1 hour'::interval) * 1 IS NULL AS v", "clickhouse")


# --- the grid -----------------------------------------------------------------------------------


def test_every_row_states_its_bound_on_clickhouse():
    rows = [f for f in GRID.features if f.family == intervals.FAMILY]
    # the 27 calls, interval_mul, mul_d_interval and interval_div, and the literal (J1337)
    assert len(rows) == 31
    for feature in rows:
        (limitation,) = feature.read.limitations["clickhouse"]
        assert "Tuple(months Int32, days Int32, microseconds Int64)" in limitation.text


_KEPT = {
    "duckdb": "SELECT CAST(CAST('1 day' AS INTERVAL) AS INTERVAL) + "
    "CAST(CAST('1 hour' AS INTERVAL) AS INTERVAL) AS v",
    "postgres": "SELECT pg_catalog.\"interval_pl\"(CAST('1 day' AS INTERVAL), "
    "CAST('1 hour' AS INTERVAL)) AS v",
}


@pytest.mark.parametrize("dialect", sorted(_KEPT))
def test_every_other_dialect_keeps_what_it_had(dialect):
    """The base's own text of the call, on each dialect no row here has a form on."""
    assert transpile(_call("interval_pl", _i("1 day"), _i("1 hour")), dialect) == _KEPT[dialect]


def test_trino_keeps_its_refusal_by_name():
    with pytest.raises(FunctionRefused, match="trino"):
        transpile(_call("interval_pl", _i("1 day"), _i("1 hour")), "trino")
