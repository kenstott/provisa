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
