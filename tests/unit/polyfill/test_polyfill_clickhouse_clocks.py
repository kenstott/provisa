# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4e2f71-6c38-4d5a-8e09-1f7a3c2d5b64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A time of day on ClickHouse, held as an Int64 of its microseconds (REQ-2236, amended
2026-11-15; job J1391; polyfill.clocks).

The ClickHouse server Provisa runs on in CI (24.3) has no Time64 and no toTime64; the embedded
chdb (26.x) the unit tests run has both, so a form that writes either answers here and fails on
the server. Every time statement is therefore written with neither, and each form is run on chdb
and compared with PostgreSQL's answer (the embedded PostgreSQL 16.11, TimeZone UTC; these
functions answer as postgres:17 does). tests/integration/test_clickhouse_time_on_server.py runs
the measure's statements on the 24.3 server itself."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import datetime
import struct
from decimal import Decimal

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.pgwire.cast_types import clock_of_text, result_casts
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.polyfill import GRID, clocks
from provisa.transpiler.transpile import transpile
from tests.integration.test_clickhouse_time_on_server import RECORDED

#: Each statement and PostgreSQL's answer as its text (a time as time_out writes it).
ANSWERED = [
    ("SELECT '12:30'::time - '10:00:15.5'::time AS v", "02:29:44.5"),
    ('SELECT pg_catalog."time_mi_time"(\'00:00\'::time, \'23:59:59.999999\'::time) AS v', "-23:59:59.999999"),
    ('SELECT pg_catalog."datetime_pl"(\'1900-01-01\'::date, \'00:00\'::time) AS v', "1900-01-01 00:00:00"),
    (
        'SELECT pg_catalog."datetime_pl"(\'2299-12-31\'::date, \'23:59:59.999999\'::time) AS v',
        "2299-12-31 23:59:59.999999",
    ),
    ('SELECT pg_catalog."timedate_pl"(\'12:30\'::time, \'1999-12-31\'::date) AS v', "1999-12-31 12:30:00"),
    ('SELECT pg_catalog."timestamp"(\'2024-02-29\'::date, \'12:30:45.5\'::time) AS v', "2024-02-29 12:30:45.5"),
    ("SELECT date '2024-06-15' + time '12:30' AS v", "2024-06-15 12:30:00"),
    ("SELECT time '12:30' + date '1900-01-01' AS v", "1900-01-01 12:30:00"),
    ('SELECT pg_catalog."time"(\'1900-01-01 00:00:00.000001\'::timestamp) AS v', "00:00:00.000001"),
    ("SELECT '1950-03-04 05:06:07.08'::timestamp::time AS v", "05:06:07.08"),
    ('SELECT pg_catalog."time"(\'12:30:45.123456\'::time, 3) AS v', "12:30:45.123"),
    ('SELECT pg_catalog."time"(\'12:30:45.5\'::time, 0) AS v', "12:30:46"),
    ('SELECT pg_catalog."time"(\'12:30:45.4\'::time, 0) AS v', "12:30:45"),
    ('SELECT pg_catalog."time"(\'12:30:45.123456\'::time, 6) AS v', "12:30:45.123456"),
    ('SELECT pg_catalog."extract"(\'microseconds\'::text, \'12:30:45.123456\'::time) AS v', "45123456"),
    ('SELECT pg_catalog."extract"(\'milliseconds\'::text, \'12:30:45.123456\'::time) AS v', "45123.456"),
    ('SELECT pg_catalog."extract"(\'epoch\'::text, \'12:30:45.123456\'::time) AS v', "45045.123456"),
    ('SELECT pg_catalog."extract"(\'minute\'::text, \'12:30:45\'::time) AS v', "30"),
    ('SELECT pg_catalog."date_part"(\'microseconds\'::text, \'12:30:45.123456\'::time) AS v', 45123456.0),
    ('SELECT pg_catalog."date_part"(\'milliseconds\'::text, \'12:30:45.123456\'::time) AS v', 45123.456),
    ('SELECT pg_catalog."date_part"(\'epoch\'::text, \'12:30:45.123456\'::time) AS v', 45045.123456),
    ('SELECT pg_catalog."date_part"(\'hour\'::text, \'24:00\'::time) AS v', 24.0),
    ('SELECT pg_catalog."overlaps"(\'10:00\'::time, \'12:00\'::time, \'11:00\'::time, \'13:00\'::time) AS v', True),
    ('SELECT pg_catalog."overlaps"(\'10:00\'::time, \'11:00\'::time, \'11:00\'::time, \'13:00\'::time) AS v', False),
    ("SELECT '24:00'::time > '23:59:59.999999'::time AS v", True),
    ('SELECT pg_catalog."time_cmp"(\'12:30\'::time, \'12:30:00.000001\'::time) AS v', -1),
    ('SELECT pg_catalog."time_larger"(\'12:30\'::time, \'12:30:00.000001\'::time) AS v', "12:30:00.000001"),
    ("SELECT CAST(c AS time) AS v FROM (VALUES ('12:30')) AS t(c)", "12:30:00"),
    ("SELECT CAST(c AS time) AS v FROM (VALUES ('2024-06-15 07:08:09.5')) AS t(c)", "07:08:09.5"),
    ("SELECT CAST(c AS time) AS v FROM (VALUES (NULL::text)) AS t(c)", None),
    ("SELECT CASE WHEN true THEN '12:30'::time ELSE '13:00'::time END AS v", "12:30:00"),
    ("SELECT '12:30:45.5'::time::text AS v", "12:30:45.5"),
    # Refused on the server before polyfill.clocks held a time as its microseconds (no toTime64).
    ("SELECT timestamp('2024-06-15'::date, '12:30:45'::time) AS v", "2024-06-15 12:30:45"),
    ('SELECT pg_catalog."datetime_pl"(\'2024-06-15\'::date, \'12:30:45\'::time) AS v', "2024-06-15 12:30:45"),
    ('SELECT pg_catalog."overlaps"(NULL::time, NULL::time, NULL::time, NULL::time) AS v', None),
]  # fmt: skip

#: PostgreSQL's error of a value a time's input does not read, read at run time (a table's column):
#: its SQLSTATE; the message names no value (the row's stated limitation).
RAISED = [
    ("SELECT CAST(c AS time) AS v FROM (VALUES ('noon')) AS t(c)", "22007"),
    ("SELECT CAST(c AS time) AS v FROM (VALUES ('12:61')) AS t(c)", "22008"),
]

#: An edge PostgreSQL answers that ClickHouse does not hold: the row's stated 0A000.
EDGES = [
    # 2300-01-01 00:00:00 in PostgreSQL; DateTime64(6, 'UTC') holds no day past 2299-12-31
    "SELECT '2299-12-31'::date + '24:00'::time AS v",
    "SELECT pg_catalog.\"timestamp\"('2299-12-31'::date, '24:00:00'::time) AS v",
]


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    (row,) = list(runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows())
    return row[0]


def _text(value: object) -> object:
    """The engine's value as PostgreSQL's text: a timestamp held as DateTime64(6, 'UTC') as its
    wall time, its fraction without trailing zeros; a numeric as its digits."""
    if isinstance(value, datetime.datetime):
        wall = value.replace(tzinfo=None).isoformat(" ")
        return wall.rstrip("0").rstrip(".") if "." in wall else wall
    if isinstance(value, Decimal):
        return str(value)
    return value


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    got = _text(_answer(runtime, sql))
    if isinstance(want, bool):
        assert got in (0, 1) and bool(got) is want, (want, got)
    elif isinstance(want, float):
        assert struct.pack("d", got) == struct.pack("d", want), (want, got)
    else:
        assert got == want and type(got) is type(want), (want, got)


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _answer(runtime, sql)
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


@pytest.mark.parametrize("sql", EDGES)
def test_an_edge_raises_the_rows_stated_0a000(runtime, sql):
    with pytest.raises(Exception) as raised:
        _answer(runtime, sql)
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == "0A000", str(raised.value)[:300]
    assert "has no exact form on clickhouse" in str(error)


def test_a_time_minus_a_time_no_row_reads_is_refused_by_name():
    """ClickHouse would answer the difference of two Int64s, which is no interval."""
    sql = (
        "SELECT CAST(c AS time) - CAST(d AS time) AS v FROM (VALUES ('12:30', '10:00')) AS t(c, d)"
    )
    with pytest.raises(RowRefused, match="time_mi_time"):
        transpile(sql, "clickhouse", generated=False)


@pytest.mark.parametrize("sql", sorted(RECORDED))
def test_no_time_statement_names_what_the_server_lacks(sql):
    """The server (24.3) has no Time64 and no toTime64: no statement may be written with either."""
    try:
        written = transpile(sql, "clickhouse", generated=False)
    except (FunctionRefused, RowRefused):
        return  # refused by name, or a stated edge: nothing reaches the server
    assert "toTime64" not in written
    assert "Time64" not in written.replace("DateTime64", "")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT '12:30:45.5'::time AS v",
        "SELECT pg_catalog.\"time_pl_interval\"('23:30'::time, '1 hour'::interval) AS v",
        "SELECT pg_catalog.\"time\"('2024-01-01 12:30:45'::timestamp) AS v",
        "SELECT pg_catalog.\"time\"('12:30:45.5'::time, 0) AS v",
    ],
)
def test_a_time_column_is_text_pgwire_reads_back_as_the_time(runtime, sql):
    """pgwire describes the column as time (result_casts) and reads the engine's text back
    (clock_of_text): the mechanism an interval's text answer uses."""
    (cast,) = result_casts(sql) or (None,)
    assert cast is not None and cast.oid == 1083
    got = _answer(runtime, sql)
    assert isinstance(got, str)
    assert isinstance(clock_of_text(got), datetime.time)


def test_a_held_time_compares_as_its_microseconds():
    written = transpile("SELECT '12:30'::time < '13:00'::time AS v", "clickhouse", generated=False)
    assert written == "SELECT toInt64(45000000000) < toInt64(46800000000) AS v"


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "postgres"])
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT '12:30'::time AS v",
        "SELECT date '2024-06-15' + time '12:30' AS v",
        "SELECT pg_catalog.\"time\"('12:30:45.5'::time, 0) AS v",
        "SELECT '12:30:45'::time::text AS v",
    ],
)
def test_another_engine_is_unchanged(dialect, sql):
    try:
        written = transpile(sql, dialect, generated=False)
    except FunctionRefused:
        return
    assert "provisa_clock" not in written and "toInt64" not in written


@pytest.mark.parametrize(
    "name",
    [
        "datetime_pl(date, time)",
        "timestamp(date, time)",
        "timedate_pl(time, date)",
        "time(timestamp)",
        "time(time, integer)",
        "extract(text, time)",
        "date_part(text, time)",
    ],
)
def test_each_clock_row_states_the_held_representation(name):
    feature = next(f for f in GRID.features if f.name == name)
    assert feature.family == clocks.FAMILY
    # Its bound; any other is a refusal of the translator's it states (polyfill.measured).
    (limitation,) = [lim for lim in feature.read.limitations["clickhouse"] if lim.refusal is None]
    assert "no Time64" in limitation.text and "microseconds of the day" in limitation.text
    assert set(feature.read.limitations) == {"clickhouse"}


def test_the_time_row_states_the_held_representation_and_its_edge():
    feature = next(f for f in GRID.features if f.name == "time")
    (limitation,) = feature.read.limitations["clickhouse"]
    assert "Int64 of its microseconds of the day" in limitation.text
    assert "24:00:00" in limitation.text and "raises" in limitation.text


def test_the_text_of_a_held_time_is_time_out_s():
    assert [clocks.text_of(m) for m in (0, 45_045_500_000, 86_399_999_999, clocks.DAY_US)] == [
        "00:00:00",
        "12:30:45.5",
        "23:59:59.999999",
        "24:00:00",
    ]
