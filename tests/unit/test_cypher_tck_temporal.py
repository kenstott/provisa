# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Temporal values as the openCypher TCK's Temporal9 states them, answered on the DuckDB engine
through the governed path: time.truncate() of a time or a date time keeps the value's offset
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
