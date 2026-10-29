# Copyright (c) 2026 Kenneth Stott
# Canary: e787f1b9-597e-47aa-ae29-1d3f60db233c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Temporal values as the openCypher TCK's Temporal9 states them, answered on the DuckDB engine
through the governed path: time.truncate() of a time or a date time keeps the value's offset, and
duration() of an ISO-8601 string the query computes reads a negative or fractional part
(REQ-2173, REQ-2167)."""

# Requirements: REQ-2173, REQ-2167

from __future__ import annotations

import time

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb

pytestmark = pytest.mark.unit

_EMPTY = CypherLabelMap(nodes={}, relationships={})


def _value(query: str):
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # as the engine runs (federation/duckdb_runtime.py)
    ((value,),) = con.execute(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall()
    return value


@pytest.mark.parametrize(
    ("query", "iso"),
    [
        (
            "RETURN time.truncate('hour', time({hour: 12, minute: 31, second: 14,"
            " timezone: '-01:00'}), {}) AS r",
            "12:00:00-01:00",
        ),
        (
            "RETURN time.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12,"
            " minute: 31, second: 14, timezone: '+01:00'}), {}) AS r",
            "00:00:00+01:00",
        ),
        (
            "RETURN time.truncate('minute', datetime({year: 1984, month: 10, day: 11, hour: 12,"
            " minute: 31, second: 14, timezone: '-01:00'}), {}) AS r",
            "12:31:00-01:00",
        ),
        (
            "RETURN time.truncate('millisecond', time({hour: 12, minute: 31, second: 14,"
            " nanosecond: 645876123, timezone: '+01:00'})) AS r",
            "12:31:14.645000+01:00",
        ),
    ],
)
def test_a_truncated_time_keeps_its_offset(query, iso):
    assert _value(query).isoformat() == iso


@pytest.mark.parametrize(
    ("components", "text"),
    [
        ("{seconds: 2, milliseconds: -1}", "PT1.999S"),
        ("{years: 12, months: 5, days: -14}", "P12Y5M-14D"),
    ],
)
def test_a_duration_read_back_from_its_text_is_itself(components, text):
    # Temporal6 [6]: an ISO-8601 duration the query computes, a part negative or fractional.
    sql_ast, _, _ = cypher_to_sql(
        parse_cypher(
            f"WITH duration({components}) AS d RETURN toString(d) AS ts, duration(toString(d)) = d AS b"
        ),
        _EMPTY,
        {},
    )
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    assert con.execute(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall() == [
        (text, True)
    ]


def test_a_duration_read_back_from_its_text_is_translated_small_and_fast():
    # Each computed value is computed once (temporal._let), not copied per use: the round trip
    # was 287 KB of SQL and nine seconds to translate when each part was copied in.
    query = (
        "WITH duration({years: 12, months: 5, days: -14, hours: 16, minutes: 12, seconds: 70,"
        " nanoseconds: 1}) AS d RETURN toString(d) AS ts, duration(toString(d)) = d AS b"
    )
    started = time.perf_counter()
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    sql = transpile_to_duckdb(sql_ast.sql(dialect="postgres"))
    assert time.perf_counter() - started < 2.0
    assert len(sql) < 20_000


@pytest.mark.parametrize(
    ("query", "refusal"),
    [
        # REQ-2173 (amended 2026-10-27): a value beyond the bounds is refused by name, never
        # answered wrongly (Temporal10 [9]/[10], Temporal2 [3] #8).
        ("RETURN date('-999999999-01-01') AS d", "beyond the dates Provisa holds"),
        (
            "RETURN localdatetime('+999999999-12-31T23:59:59') AS d",
            "beyond the dates Provisa holds",
        ),
        ("RETURN time('22+18:00') AS t", "offset beyond the times Provisa holds"),
        (
            "RETURN time({hour: 22, timezone: '-16:00'}) AS t",
            "offset beyond the times Provisa holds",
        ),
    ],
)
def test_a_temporal_value_beyond_the_bounds_is_refused_by_name(query, refusal):
    from provisa.gql.translator_types import CypherTranslateError

    with pytest.raises(CypherTranslateError, match=refusal):
        _value(query)


def test_a_time_within_the_offsets_is_read():
    assert _value("RETURN time('22+15:00') AS t").isoformat() == "22:00:00+15:00"


def test_a_date_time_from_the_epoch_is_that_instant():
    # Temporal1 [11]: datetime.fromepoch(seconds, nanoseconds), datetime.fromepochmillis(ms),
    # to the microsecond (REQ-2173).
    d1 = _value("RETURN datetime.fromepoch(416779, 999999999) AS d")
    d2 = _value("RETURN datetime.fromepochmillis(237821673987) AS d")
    assert d1.isoformat() == "1970-01-05T19:46:19.999999+00:00"
    assert d2.isoformat() == "1977-07-15T13:34:33.987000+00:00"
    assert _value("RETURN datetime.fromepochmillis(237821673987).year AS y") == 1977


@pytest.mark.parametrize(
    ("duration", "plus", "minus"),
    [
        # Temporal8 [1] #3: a fractional duration's time is 1.4 days; it moves a date one day.
        (
            "duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5,"
            " seconds: 70.5, nanoseconds: 3})",
            "1997-10-11",
            "1971-10-12",
        ),
        # Under a day of time moves a date not at all; whole days of it, toward zero either way.
        ("duration({hours: 23})", "1984-10-11", "1984-10-11"),
        ("duration({hours: 47})", "1984-10-12", "1984-10-10"),
        ("duration({hours: -47})", "1984-10-10", "1984-10-12"),
    ],
)
def test_a_date_moves_by_the_whole_days_of_a_durations_time(duration, plus, minus):
    x = "date({year: 1984, month: 10, day: 11})"
    assert _value(f"RETURN {x} + {duration} AS d").isoformat() == plus
    assert _value(f"RETURN {x} - {duration} AS d").isoformat() == minus


def test_a_session_time_with_its_offset_has_its_accessors(monkeypatch):
    # Temporal5 [4]: a time with its offset stored in the session (a zoned_time property) is a
    # time to its accessors, after a WITH as on the node (REQ-2176, REQ-2173).
    from tests.unit.ephemeral_harness import Session

    s = Session(monkeypatch, {})
    s.run(
        "CREATE (:Val {date: time({hour: 12, minute: 31, second: 14, nanosecond: 645876123,"
        " timezone: '+01:00'})})"
    )
    rows = s.run(
        "MATCH (v:Val) WITH v.date AS d RETURN d.hour, d.minute, d.second, d.microsecond,"
        " d.timezone, d.offset, d.offsetMinutes, d.offsetSeconds"
    )
    assert rows == [
        {
            "d.hour": 12,
            "d.minute": 31,
            "d.second": 14,
            "d.microsecond": 645876,
            "d.timezone": "+01:00",
            "d.offset": "+01:00",
            "d.offsetMinutes": 60,
            "d.offsetSeconds": 3600,
        }
    ]
    assert s.run("MATCH (v:Val) RETURN v.date.hour AS h") == [{"h": 12}]
