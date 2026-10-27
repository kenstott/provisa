# Copyright (c) 2026 Kenneth Stott
# Canary: 184b6706-1c25-4b84-bfb6-99e147eef301
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher durations (REQ-2172), each case an openCypher TCK scenario (Temporal1, 5, 8 and 10)
translated, run on DuckDB and answered as the Cypher surface answers it. Where a scenario's value
has nanoseconds, the case keeps it to microseconds, the engine's precision."""

# Requirements: REQ-2172

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.assembler import assemble_rows, to_serializable
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile

pytestmark = pytest.mark.unit

_EMPTY = CypherLabelMap(nodes={}, relationships={})


def _answer(query: str) -> list[dict]:
    sql_ast, _params, graph_vars = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, _EMPTY)
    duck = duckdb.connect()
    duck.execute("SET TimeZone = 'UTC'")
    result = duck.sql(transpile(sql_ast.sql(dialect="postgres"), "duckdb"))
    rows = [dict(zip(result.columns, row)) for row in result.fetchall()]
    return to_serializable(assemble_rows(rows, graph_vars))


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        # Temporal1 [13]: a duration from a map, its fractions carried down
        ("RETURN duration({days: 14, hours: 16, minutes: 12}) AS d", "P14DT16H12M"),
        ("RETURN duration({months: 5, days: 1.5}) AS d", "P5M1DT12H"),
        ("RETURN duration({months: 0.75}) AS d", "P22DT19H51M49.5S"),
        ("RETURN duration({weeks: 2.5}) AS d", "P17DT12H"),
        (
            "RETURN duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, "
            "seconds: 70}) AS d",
            "P12Y5M14DT16H13M10S",
        ),
        ("RETURN duration({days: 14, seconds: 70, milliseconds: 1}) AS d", "P14DT1M10.001S"),
        ("RETURN duration({minutes: 1.5, seconds: 1}) AS d", "PT1M31S"),
        # Temporal2: from ISO-8601 text
        ("RETURN duration('P14DT16H12M') AS d", "P14DT16H12M"),
        ("RETURN duration('P5M1.5D') AS d", "P5M1DT12H"),
        ("RETURN duration('PT0.75M') AS d", "PT45S"),
        # Temporal10: between two values, and in months, days and seconds
        ("RETURN duration.between(date('1984-10-11'), date('2015-06-24')) AS d", "P30Y8M13D"),
        (
            "RETURN duration.between(date('1984-10-11'), localdatetime('2016-07-21T21:45:22.142'))"
            " AS d",
            "P31Y9M10DT21H45M22.142S",
        ),
        ("RETURN duration.between(date('1984-10-11'), localtime('16:30')) AS d", "PT16H30M"),
        ("RETURN duration.between(localtime('14:30'), date('2015-06-24')) AS d", "PT-14H-30M"),
        ("RETURN duration.between(time('14:30'), time('16:30+0100')) AS d", "PT1H"),
        (
            "RETURN duration.between(localdatetime('2015-07-21T21:40:32.142'), "
            "date('2015-06-24')) AS d",
            "P-27DT-21H-40M-32.142S",
        ),
        ("RETURN duration.inMonths(date('1984-10-11'), date('2015-06-24')) AS d", "P30Y8M"),
        ("RETURN duration.inMonths(date('1984-10-11'), localtime('16:30')) AS d", "PT0S"),
        ("RETURN duration.inDays(date('1984-10-11'), date('2015-06-24')) AS d", "P11213D"),
        (
            "RETURN duration.inSeconds(localtime('12:34:54.7'), localtime('12:34:54.3')) AS d",
            "PT-0.4S",
        ),
        (
            "RETURN duration.inSeconds(localtime('12:44:54.7'), localtime('12:34:55.3')) AS d",
            "PT-9M-59.4S",
        ),
        # Temporal8 [6]: durations added and subtracted, part by part
        (
            "WITH duration({months: 1, days: -14, hours: 16, minutes: -12, seconds: 70}) AS a "
            "RETURN a + a AS d",
            "P2M-28DT31H38M20S",
        ),
        (
            "WITH duration({months: 1, days: -14, hours: 16, minutes: -12, seconds: 70}) AS a "
            "RETURN a - a AS d",
            "PT0S",
        ),
        # Temporal8 [7]: multiplied by a number
        (
            "WITH duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70})"
            " AS a RETURN a * 2 AS d",
            "P24Y10M28DT32H26M20S",
        ),
    ],
)
def test_a_duration_is_answered_in_iso_8601(query, expected):
    assert _answer(query) == [{"d": expected}]


def test_a_date_or_a_time_moves_by_a_duration():
    """Temporal8 [1], [2]: a date by the duration's months and days; a time of day by its
    time, round the clock."""
    dur = "duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70})"
    assert _answer(
        f"WITH date('1984-10-11') AS x, {dur} AS dur RETURN x + dur AS sum, x - dur AS diff"
    ) == [{"sum": "1997-03-25", "diff": "1972-04-27"}]
    assert _answer(
        f"WITH localtime('12:31:14') AS x, {dur} AS dur RETURN x + dur AS sum, x - dur AS diff"
    ) == [{"sum": "04:44:24", "diff": "20:18:04"}]


def test_a_durations_parts_are_read_as_properties():
    """Temporal5 [7]."""
    [row] = _answer(
        "WITH duration({years: 1, months: 4, days: 10, hours: 1, minutes: 1, seconds: 1, "
        "microseconds: 111111}) AS d "
        "RETURN d.years, d.quarters, d.months, d.weeks, d.days, d.hours, d.minutes, d.seconds, "
        "d.milliseconds, d.microseconds, d.nanoseconds, d.quartersOfYear, d.monthsOfQuarter, "
        "d.monthsOfYear, d.daysOfWeek, d.minutesOfHour, d.secondsOfMinute, "
        "d.millisecondsOfSecond, d.microsecondsOfSecond, d.nanosecondsOfSecond"
    )
    assert row == {
        "d.years": 1,
        "d.quarters": 5,
        "d.months": 16,
        "d.weeks": 1,
        "d.days": 10,
        "d.hours": 1,
        "d.minutes": 61,
        "d.seconds": 3661,
        "d.milliseconds": 3661111,
        "d.microseconds": 3661111111,
        "d.nanoseconds": 3661111111000,
        "d.quartersOfYear": 1,
        "d.monthsOfQuarter": 1,
        "d.monthsOfYear": 4,
        "d.daysOfWeek": 3,
        "d.minutesOfHour": 1,
        "d.secondsOfMinute": 1,
        "d.millisecondsOfSecond": 111,
        "d.microsecondsOfSecond": 111111,
        "d.nanosecondsOfSecond": 111111000,
    }


def test_durations_are_equal_part_by_part():
    """Temporal8: 40 hours is not a day and 16 hours, as an engine's interval would have it."""
    assert _answer(
        "WITH duration({days: 14, hours: 16}) AS x, duration({days: 13, hours: 40}) AS y "
        "RETURN x = y AS same, x = x AS self"
    ) == [{"same": False, "self": True}]


def test_a_duration_written_as_text_is_its_iso_8601():
    """Temporal6: toString of a duration."""
    for given, text in [
        ("{years: 12, months: 5, days: -14, hours: 16}", "P12Y5M-14DT16H"),
        ("{minutes: 12, seconds: -60}", "PT11M"),
        ("{seconds: -2, milliseconds: 1}", "PT-1.999S"),
        ("{days: 1, milliseconds: -1}", "P1DT-0.001S"),
        ("{seconds: -60, milliseconds: -1}", "PT-1M-0.001S"),
    ]:
        assert _answer(f"RETURN toString(duration({given})) AS t") == [{"t": text}], given


def test_a_null_duration_is_null():
    assert _answer("RETURN duration.between(null, null) AS t, duration(null) AS n") == [
        {"t": None, "n": None}
    ]


def test_a_duration_is_read_on_a_source_only_where_its_sql_holds_intervals():
    from provisa.transpiler.dialect_capabilities import DURATION, unsupported

    sql = "SELECT d + CAST('1 months 0 days 0 microseconds' AS INTERVAL) FROM t"
    assert unsupported(sql, "postgres") is None
    assert unsupported(sql, "duckdb") is None
    assert unsupported(sql, "mysql") == DURATION
