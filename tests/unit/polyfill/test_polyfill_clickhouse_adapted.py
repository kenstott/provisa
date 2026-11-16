# Copyright (c) 2026 Kenneth Stott
# Canary: 6293f388-4b6e-43f6-978e-c319702536f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's forms adapted from DuckDB's (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND
COMES BEFORE A REFUSAL): overloads ClickHouse refused by name on the base, each answered on the
embedded engine and compared with PostgreSQL's answer on its edge values.

- a time of day and an interval: time_pl_interval, time_mi_interval, interval_pl_time,
  interval(time), time(interval), and overlaps with an end given as an interval (of timestamps,
  and of times of day). DuckDB's form adds an INTERVAL, which ClickHouse has none of; adapted,
  the interval is ClickHouse's held tuple (polyfill.intervals) and the time an Int64 of its
  microseconds of the day (polyfill.clocks: ClickHouse's server has no Time64), shown in the
  select list as time_out's text;
- int8inc_any and int8dec_any: DuckDB's form, its error() as ClickHouse's throwIf and its
  TRY_CAST as accurateCastOrNull (polyfill.ported).

The answers are PostgreSQL's (the embedded PostgreSQL 16.11 the unit tests boot; these functions
answer as postgres:17.11 does), each statement's text as the measure writes it or an edge value:
NULL, zero, negative, the type's extremes, a day's end, and the inputs PostgreSQL raises for."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import datetime

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.pgwire.cast_types import clock_of_text
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.transpile import transpile

ANSWERED = [
    ("SELECT '1 hour'::interval + '12:30:45'::time AS v", datetime.time(13, 30, 45)),
    ("SELECT '12:30:45'::time - '13 hours'::interval AS v", datetime.time(23, 30, 45)),
    ("SELECT '12:30:45'::time + '1 hour'::interval AS v", datetime.time(13, 30, 45)),
    ('SELECT pg_catalog."int8dec_any"((-7)::bigint, (-7)::integer) AS v', -8),
    ('SELECT pg_catalog."int8dec_any"(3::bigint, 3::integer) AS v', 2),
    ('SELECT pg_catalog."int8dec_any"(5000000000::bigint, 42::integer) AS v', 4999999999),
    ('SELECT pg_catalog."int8dec_any"(NULL::bigint, NULL::integer) AS v', None),
    ('SELECT pg_catalog."int8inc_any"((-7)::bigint, (-7)::integer) AS v', -6),
    ('SELECT pg_catalog."int8inc_any"(3::bigint, 3::integer) AS v', 4),
    ('SELECT pg_catalog."int8inc_any"(5000000000::bigint, 42::integer) AS v', 5000000001),
    ('SELECT pg_catalog."int8inc_any"(NULL::bigint, NULL::integer) AS v', None),
    (
        "SELECT pg_catalog.\"interval_pl_time\"('-1 year 2 days'::interval, '23:59:59.5'::time without time zone) AS v",
        datetime.time(23, 59, 59, 500000),
    ),
    (
        "SELECT pg_catalog.\"interval_pl_time\"('1 day 02:03:04'::interval, '12:30:45'::time without time zone) AS v",
        datetime.time(14, 33, 49),
    ),
    (
        "SELECT pg_catalog.\"interval_pl_time\"('3 mons'::interval, '00:00:00'::time without time zone) AS v",
        datetime.time(0, 0),
    ),
    (
        'SELECT pg_catalog."interval_pl_time"(NULL::interval, NULL::time without time zone) AS v',
        None,
    ),
    ("SELECT pg_catalog.\"interval\"('00:00:00'::time without time zone) AS v", "00:00:00"),
    ("SELECT pg_catalog.\"interval\"('00:00'::time) AS v", "00:00:00"),
    ("SELECT pg_catalog.\"interval\"('12:30:45.123456'::time) AS v", "12:30:45.123456"),
    ("SELECT pg_catalog.\"interval\"('12:30:45'::time without time zone) AS v", "12:30:45"),
    ("SELECT pg_catalog.\"interval\"('23:59:59.5'::time without time zone) AS v", "23:59:59.5"),
    ("SELECT pg_catalog.\"interval\"('23:59:59.999999'::time) AS v", "23:59:59.999999"),
    ("SELECT pg_catalog.\"interval\"('24:00:00'::time) AS v", "24:00:00"),
    ('SELECT pg_catalog."interval"(NULL::time without time zone) AS v', None),
    ('SELECT pg_catalog."interval"(NULL::time) AS v', None),
    (
        "SELECT pg_catalog.\"overlaps\"('00:00:00'::time without time zone, '00:00:00'::time without time zone, '00:00:00'::time without time zone, '3 mons'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('00:00:00'::time without time zone, '3 mons'::interval, '00:00:00'::time without time zone, '00:00:00'::time without time zone) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('00:00:00'::time without time zone, '3 mons'::interval, '00:00:00'::time without time zone, '3 mons'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('12:00'::time, '1 hour'::interval, '12:30'::time, '1 hour'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('12:00'::time, '1 hour'::interval, '12:30'::time, '13:30'::time) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('12:00'::time, '1 hour'::interval, '13:00'::time, '1 hour'::interval) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('12:00'::time, '1 hour'::interval, '14:30'::time, '13:30'::time) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('12:00'::time, '12:59:59.999999'::time, '13:00'::time, '1 hour'::interval) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('12:00'::time, '13:00:00.000001'::time, '13:00'::time, '1 hour'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('12:00'::time, NULL::interval, '11:00'::time, '2 hours'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('12:30:45'::time without time zone, '1 day 02:03:04'::interval, '12:30:45'::time without time zone, '1 day 02:03:04'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('12:30:45'::time without time zone, '1 day 02:03:04'::interval, '12:30:45'::time without time zone, '12:30:45'::time without time zone) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('12:30:45'::time without time zone, '12:30:45'::time without time zone, '12:30:45'::time without time zone, '1 day 02:03:04'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('1999-12-31 23:59:59'::timestamp without time zone, '1999-12-31 23:59:59'::timestamp without time zone, '1999-12-31 23:59:59'::timestamp without time zone, '3 mons'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('1999-12-31 23:59:59'::timestamp without time zone, '3 mons'::interval, '1999-12-31 23:59:59'::timestamp without time zone, '1999-12-31 23:59:59'::timestamp without time zone) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('1999-12-31 23:59:59'::timestamp without time zone, '3 mons'::interval, '1999-12-31 23:59:59'::timestamp without time zone, '3 mons'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2000-02-29 00:00:00'::timestamp without time zone, '-1 year 2 days'::interval, '2000-02-29 00:00:00'::timestamp without time zone, '-1 year 2 days'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2000-02-29 00:00:00'::timestamp without time zone, '-1 year 2 days'::interval, '2000-02-29 00:00:00'::timestamp without time zone, '2000-02-29 00:00:00'::timestamp without time zone) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2000-02-29 00:00:00'::timestamp without time zone, '2000-02-29 00:00:00'::timestamp without time zone, '2000-02-29 00:00:00'::timestamp without time zone, '-1 year 2 days'::interval) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '-1 day'::interval, '2023-12-31 12:00'::timestamp, '1 hour'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '1 day'::interval, '2024-01-01 12:00'::timestamp, '1 day'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '1 day'::interval, '2024-01-01 12:00'::timestamp, '2024-01-03'::timestamp) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '1 day'::interval, '2024-01-01 23:59:59.999999'::timestamp, '1 day'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '1 day'::interval, '2024-01-02'::timestamp, '1 day'::interval) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '1 day'::interval, '2024-01-03'::timestamp, '2024-01-02'::timestamp) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '1 day'::interval, '2024-01-03'::timestamp, NULL::timestamp) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '1 day'::interval, NULL::timestamp, '2024-01-01 06:00'::timestamp) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '2024-01-02'::timestamp, '2024-01-01 12:00'::timestamp, '1 day'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '2024-01-02'::timestamp, '2024-01-01 12:00'::timestamp, '2024-01-03'::timestamp) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, NULL::interval, '2023-12-31'::timestamp, '2 days'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, NULL::interval, '2024-01-01'::timestamp, '1 day'::interval) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, NULL::timestamp, '2024-01-01'::timestamp, '1 day'::interval) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-02'::timestamp, '2024-01-01'::timestamp, '2024-01-02'::timestamp, '-1 microsecond'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-02'::timestamp, NULL::interval, '2024-01-01'::timestamp, '2 days'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-05'::timestamp, NULL::interval, '2024-01-01'::timestamp, '2 days'::interval) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-31'::timestamp, '1 month'::interval, '2024-02-29'::timestamp, '1 day'::interval) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-01-31'::timestamp, '1 month'::interval, '2024-03-01'::timestamp, '1 day'::interval) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-06-15 12:30:45'::timestamp without time zone, '1 day 02:03:04'::interval, '2024-06-15 12:30:45'::timestamp without time zone, '1 day 02:03:04'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-06-15 12:30:45'::timestamp without time zone, '1 day 02:03:04'::interval, '2024-06-15 12:30:45'::timestamp without time zone, '2024-06-15 12:30:45'::timestamp without time zone) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('2024-06-15 12:30:45'::timestamp without time zone, '2024-06-15 12:30:45'::timestamp without time zone, '2024-06-15 12:30:45'::timestamp without time zone, '1 day 02:03:04'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('23:00'::time, '2 hours'::interval, '00:30'::time, '1 hour'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('23:00'::time, '2 hours'::interval, '23:30'::time, '1 minute'::interval) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('23:59:59.5'::time without time zone, '-1 year 2 days'::interval, '23:59:59.5'::time without time zone, '-1 year 2 days'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('23:59:59.5'::time without time zone, '-1 year 2 days'::interval, '23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone, '23:59:59.5'::time without time zone, '-1 year 2 days'::interval) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"('24:00:00'::time, '1 day'::interval, '00:00'::time, '1 microsecond'::interval) AS v",
        True,
    ),
    (
        'SELECT pg_catalog."overlaps"(NULL::time without time zone, NULL::interval, NULL::time without time zone, NULL::interval) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."overlaps"(NULL::time without time zone, NULL::interval, NULL::time without time zone, NULL::time without time zone) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."overlaps"(NULL::time without time zone, NULL::time without time zone, NULL::time without time zone, NULL::interval) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"(NULL::time, NULL::time, '13:00'::time, '1 hour'::interval) AS v",
        None,
    ),
    (
        'SELECT pg_catalog."overlaps"(NULL::timestamp without time zone, NULL::interval, NULL::timestamp without time zone, NULL::interval) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."overlaps"(NULL::timestamp without time zone, NULL::interval, NULL::timestamp without time zone, NULL::timestamp without time zone) AS v',
        None,
    ),
    (
        'SELECT pg_catalog."overlaps"(NULL::timestamp without time zone, NULL::timestamp without time zone, NULL::timestamp without time zone, NULL::interval) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"overlaps\"(NULL::timestamp, '1 day'::interval, '2024-01-01'::timestamp, '1 day'::interval) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"time_mi_interval\"('00:00:00'::time without time zone, '3 mons'::interval) AS v",
        datetime.time(0, 0),
    ),
    (
        "SELECT pg_catalog.\"time_mi_interval\"('12:30:45'::time without time zone, '1 day 02:03:04'::interval) AS v",
        datetime.time(10, 27, 41),
    ),
    (
        "SELECT pg_catalog.\"time_mi_interval\"('23:59:59.5'::time without time zone, '-1 year 2 days'::interval) AS v",
        datetime.time(23, 59, 59, 500000),
    ),
    (
        'SELECT pg_catalog."time_mi_interval"(NULL::time without time zone, NULL::interval) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"time_pl_interval\"('00:00:00'::time without time zone, '3 mons'::interval) AS v",
        datetime.time(0, 0),
    ),
    (
        "SELECT pg_catalog.\"time_pl_interval\"('12:30:45'::time without time zone, '1 day 02:03:04'::interval) AS v",
        datetime.time(14, 33, 49),
    ),
    (
        "SELECT pg_catalog.\"time_pl_interval\"('23:59:59.5'::time without time zone, '-1 year 2 days'::interval) AS v",
        datetime.time(23, 59, 59, 500000),
    ),
    (
        'SELECT pg_catalog."time_pl_interval"(NULL::time without time zone, NULL::interval) AS v',
        None,
    ),
    ("SELECT pg_catalog.\"time\"('-1 hour'::interval) AS v", datetime.time(23, 0)),
    ("SELECT pg_catalog.\"time\"('-1 year 2 days'::interval) AS v", datetime.time(0, 0)),
    (
        "SELECT pg_catalog.\"time\"('-49 hours -0.000001 seconds'::interval) AS v",
        datetime.time(22, 59, 59, 999999),
    ),
    ("SELECT pg_catalog.\"time\"('1 day 02:03:04'::interval) AS v", datetime.time(2, 3, 4)),
    ("SELECT pg_catalog.\"time\"('1 hour'::interval) AS v", datetime.time(1, 0)),
    (
        "SELECT pg_catalog.\"time\"('1 year 2 days 03:04:05.678'::interval) AS v",
        datetime.time(3, 4, 5, 678000),
    ),
    ("SELECT pg_catalog.\"time\"('24 hours'::interval) AS v", datetime.time(0, 0)),
    # time of a timestamp (polyfill.clocks), its wall time's time of day; was refused by name
    (
        "SELECT pg_catalog.\"time\"('2024-01-01 12:30:45'::timestamp) AS v",
        datetime.time(12, 30, 45),
    ),
    (
        "SELECT pg_catalog.\"time\"('1900-01-01 23:59:59.999999'::timestamp) AS v",
        datetime.time(23, 59, 59, 999999),
    ),
    ("SELECT pg_catalog.\"time\"('25 hours'::interval) AS v", datetime.time(1, 0)),
    (
        "SELECT pg_catalog.\"time\"('2562047788:00:54.775807'::interval) AS v",
        datetime.time(4, 0, 54, 775807),
    ),
    ("SELECT pg_catalog.\"time\"('3 mons'::interval) AS v", datetime.time(0, 0)),
    ('SELECT pg_catalog."time"(NULL::interval) AS v', None),
    (
        "SELECT pg_catalog.int8dec_any(-9223372036854775807::bigint, 1::integer) AS v",
        -9223372036854775808,
    ),
    ("SELECT pg_catalog.int8dec_any(0::bigint, 'a'::text) AS v", -1),
    ("SELECT pg_catalog.int8dec_any(3::bigint, 3::integer) AS v", 2),
    (
        "SELECT pg_catalog.int8dec_any(9223372036854775807::bigint, 1::integer) AS v",
        9223372036854775806,
    ),
    ("SELECT pg_catalog.int8dec_any(NULL::bigint, NULL::integer) AS v", None),
    (
        "SELECT pg_catalog.int8inc_any((-9223372036854775808)::bigint, 1::integer) AS v",
        -9223372036854775807,
    ),
    ("SELECT pg_catalog.int8inc_any(0::bigint, 1.5::numeric) AS v", 1),
    ("SELECT pg_catalog.int8inc_any(1::bigint, NULL::integer) AS v", None),
    ("SELECT pg_catalog.int8inc_any(3::bigint, 3::integer) AS v", 4),
    (
        "SELECT pg_catalog.int8inc_any(9223372036854775806::bigint, 'x'::text) AS v",
        9223372036854775807,
    ),
    ("SELECT pg_catalog.int8inc_any(NULL::bigint, 1::integer) AS v", None),
    (
        "SELECT pg_catalog.interval_pl_time('-25 hours 3 days'::interval, '01:00'::time) AS v",
        datetime.time(0, 0),
    ),
    (
        "SELECT pg_catalog.interval_pl_time('1 hour'::interval, '12:30:45'::time) AS v",
        datetime.time(13, 30, 45),
    ),
    ("SELECT pg_catalog.interval_pl_time(NULL::interval, '01:00'::time) AS v", None),
    (
        "SELECT pg_catalog.time_mi_interval('00:00'::time, '1 microsecond'::interval) AS v",
        datetime.time(23, 59, 59, 999999),
    ),
    (
        "SELECT pg_catalog.time_mi_interval('12:00'::time, '-2562047788:00:54.775807'::interval) AS v",
        datetime.time(7, 59, 5, 224191),
    ),
    (
        "SELECT pg_catalog.time_mi_interval('12:00'::time, '1 day 1 hour'::interval) AS v",
        datetime.time(11, 0),
    ),
    (
        "SELECT pg_catalog.time_mi_interval('12:30:45'::time, '1 hour'::interval) AS v",
        datetime.time(11, 30, 45),
    ),
    ("SELECT pg_catalog.time_mi_interval(NULL::time, NULL::interval) AS v", None),
    (
        "SELECT pg_catalog.time_pl_interval('00:00'::time, '-1 microsecond'::interval) AS v",
        datetime.time(23, 59, 59, 999999),
    ),
    (
        "SELECT pg_catalog.time_pl_interval('12:00'::time, '-2562047788 hours'::interval) AS v",
        datetime.time(8, 0),
    ),
    (
        "SELECT pg_catalog.time_pl_interval('12:00'::time, '-49 hours'::interval) AS v",
        datetime.time(11, 0),
    ),
    (
        "SELECT pg_catalog.time_pl_interval('12:00'::time, '2562047788 hours'::interval) AS v",
        datetime.time(7, 58, 10, 448384),
    ),
    (
        "SELECT pg_catalog.time_pl_interval('12:00'::time, '2562047788:00:54.775807'::interval) AS v",
        datetime.time(7, 59, 5, 224191),
    ),
    ("SELECT pg_catalog.time_pl_interval('12:00'::time, NULL::interval) AS v", None),
    (
        "SELECT pg_catalog.time_pl_interval('12:30:45'::time, '1 hour'::interval) AS v",
        datetime.time(13, 30, 45),
    ),
    (
        "SELECT pg_catalog.time_pl_interval('12:30'::time, '1 year 2 mons 3 days 04:05:06.7'::interval) AS v",
        datetime.time(16, 35, 6, 700000),
    ),
    (
        "SELECT pg_catalog.time_pl_interval('23:30:45.123456'::time, '1 hour'::interval) AS v",
        datetime.time(0, 30, 45, 123456),
    ),
    (
        "SELECT pg_catalog.time_pl_interval('24:00:00'::time, '0 seconds'::interval) AS v",
        datetime.time(0, 0),
    ),
    (
        "SELECT pg_catalog.time_pl_interval('24:00:00'::time, '1 second'::interval) AS v",
        datetime.time(0, 0, 1),
    ),
    ("SELECT pg_catalog.time_pl_interval(NULL::time, '1 hour'::interval) AS v", None),
]
RAISED = [
    ("SELECT pg_catalog.int8dec_any(-9223372036854775808::bigint, 1::integer) AS v", "22003"),
    ("SELECT pg_catalog.int8dec_any((-9223372036854775808)::bigint, 1::integer) AS v", "22003"),
    ("SELECT pg_catalog.int8inc_any(-9223372036854775808::bigint, 1::integer) AS v", "22003"),
    ("SELECT pg_catalog.int8inc_any(1::bigint, 2147483648::integer) AS v", "22003"),
    ("SELECT pg_catalog.int8inc_any(9223372036854775807::bigint, 1::integer) AS v", "22003"),
    ("SELECT pg_catalog.int8inc_any(9223372036854775808::bigint, 1::integer) AS v", "22003"),
    (
        "SELECT pg_catalog.int8inc_any(c, c) AS v FROM (VALUES (1::bigint), (9223372036854775807::bigint)) AS t(c) ORDER BY c",
        "22003",
    ),
]
#: Inputs no row here reads, each refused by name (a bound the grid states): a time of day in a
#: form time_in reads but this row does not, a time of a table's column, an interval of no
#: literal, and a timestamp the shift takes outside DateTime64's range.
REFUSED = [
    "SELECT pg_catalog.time_pl_interval('12:30:45 PM'::time, '1 hour'::interval) AS v",
    "SELECT pg_catalog.time_pl_interval('12:30:45.1234567'::time, '1 hour'::interval) AS v",
    "SELECT pg_catalog.time_pl_interval('25:00'::time, '1 hour'::interval) AS v",
    "SELECT pg_catalog.time_pl_interval(c, '1 hour'::interval) AS v FROM (VALUES ('12:00'::time)) "
    "AS t(c)",
    "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '0'::interval, "
    "'2024-01-01'::timestamp, '0'::interval) AS v",
    "SELECT pg_catalog.\"interval\"('1 day'::interval, 3) AS v",
]
OUTSIDE = (
    "SELECT pg_catalog.\"overlaps\"('1900-01-01'::timestamp, '-1 day'::interval, "
    "'1900-01-01'::timestamp, '1 day'::interval) AS v"
)


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _rows(runtime: ClickHouseFederationRuntime, sql: str) -> list[object]:
    return [row[0] for row in runtime.run_sync(transpile(sql, "clickhouse")).iter_rows()]


def _time(value: object) -> object:
    """A time as the select list shows it, time_out's text, read back as pgwire reads it."""
    return clock_of_text(value) if isinstance(value, str) else value


def _same(want: object, got: object) -> bool:
    """PostgreSQL's answer and the engine's: a boolean the engine holds as 0 or 1, a time as
    time_out's text, an interval as interval_out's text, any other value as it is."""
    if isinstance(want, datetime.time):
        got = _time(got)
    if want is None or got is None:
        return want is got
    if isinstance(want, bool):
        return got in (0, 1) and bool(got) is want
    return want == got


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    (got,) = _rows(runtime, sql)
    assert _same(want, got), (want, got)


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _rows(runtime, sql)
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


@pytest.mark.parametrize("sql", REFUSED)
def test_an_input_no_row_reads_is_refused_by_name(sql):
    with pytest.raises((FunctionRefused, RowRefused), match="has no form on clickhouse"):
        transpile(sql, "clickhouse")


def test_a_shift_outside_the_bound_raises_0a000(runtime):
    with pytest.raises(Exception) as raised:
        _rows(runtime, OUTSIDE)
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == "0A000", str(raised.value)[:300]
    assert "has no exact form on clickhouse" in str(error)


ADAPTED = {
    "time_pl_interval",
    "time_mi_interval",
    "interval_pl_time",
    '"interval"',
    '"time"',
    '"overlaps"',
    "int8inc_any",
    "int8dec_any",
    '"int8dec_any"',
    '"int8inc_any"',
    '"time_pl_interval"',
    '"time_mi_interval"',
    '"interval_pl_time"',
}


def test_every_adapted_overload_is_compared():
    called = {
        sql.split("pg_catalog.")[1].split("(")[0]
        for sql, _ in [*ANSWERED, *RAISED]
        if "pg_catalog." in sql
    }
    assert {name.strip('"') for name in called} == {name.strip('"') for name in ADAPTED}
    overlaps = {sql.count("::interval") for sql, _ in ANSWERED if '"overlaps"' in sql}
    # an end as an interval on one side, and on both (and four timestamps, the other row's)
    assert overlaps == {0, 1, 2}


@pytest.mark.parametrize(
    "feature",
    [
        "time_pl_interval(time, interval)",
        "time_mi_interval(time, interval)",
        "interval_pl_time(interval, time)",
        "interval(time)",
        "time(interval)",
    ],
)
def test_every_other_dialect_keeps_what_it_had(feature):
    (row,) = [f for f in GRID.features if f.name == feature]
    (rule,) = row.read.rules
    assert set(rule.forms) == {"clickhouse"}


def test_overlaps_of_four_timestamps_keeps_its_form():
    sql = (
        "SELECT pg_catalog.\"overlaps\"('2024-01-01'::timestamp, '2024-01-02'::timestamp, "
        "'2024-01-01 12:00'::timestamp, '2024-01-03'::timestamp) AS v"
    )
    assert "provisa_iv" not in transpile(sql, "clickhouse")
