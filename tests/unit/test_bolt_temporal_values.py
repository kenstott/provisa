# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bolt client's temporal and spatial values travel as values (REQ-2173): a Date, Time,
DateTime, Duration or Point parameter is that value in the statement, and comes back as the same
Bolt structure -- never as a map or as text. The JSON surfaces write the same values as text."""

# Requirements: REQ-2173, REQ-802

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import duckdb
import pytest

from provisa.bolt.packstream import pack
from provisa.cypher.parser import parse_cypher
from provisa.gql.assembler import assemble_rows, to_serializable
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.spatial import Point
from provisa.gql.temporal import Duration
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb

pytestmark = pytest.mark.unit

_EMPTY = CypherLabelMap(nodes={}, relationships={})

_VALUES = {
    "date": dt.date(2019, 6, 1),
    "time": dt.time(12, 31, 14, 645876, tzinfo=dt.timezone(dt.timedelta(hours=1))),
    "localtime": dt.time(12, 31, 14),
    "localdatetime": dt.datetime(2019, 6, 1, 18, 40, 32, 142000),
    "datetime": dt.datetime(
        2019, 6, 1, 18, 40, 32, 142000, tzinfo=dt.timezone(dt.timedelta(hours=2))
    ),
    "zoned": dt.datetime(2019, 6, 1, 18, 40, 32, tzinfo=ZoneInfo("Europe/Stockholm")),
    "duration": Duration(14, 3, 4_500_000),
    "point": Point(7203, (2.3, 4.5)),
    "place": Point(4979, (1.1, -2.0, 123456.789)),
}


def _answer(keep_values: bool, query: str | None = None) -> dict:
    from provisa.gql.temporal import bindable

    query = query or "RETURN " + ", ".join(f"${k} AS {k}" for k in _VALUES)
    sql_ast, ordered, graph_vars = cypher_to_sql(parse_cypher(query), _EMPTY, dict(_VALUES))
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, _EMPTY)
    duck = duckdb.connect()
    duck.execute("SET TimeZone = 'UTC'")
    values = [bindable(_VALUES[name]) for name in ordered]
    result = duck.execute(transpile_to_duckdb(sql_ast.sql(dialect="postgres")), values)
    columns = [d[0] for d in result.description]
    rows = [dict(zip(columns, r)) for r in result.fetchall()]
    [row] = [to_serializable(r, keep_values=keep_values) for r in assemble_rows(rows, graph_vars)]
    return row


def test_a_temporal_or_spatial_parameter_comes_back_as_the_same_value():
    assert _answer(keep_values=True) == _VALUES


def test_on_bolt_each_is_packed_as_its_structure():
    row = _answer(keep_values=True)
    tags = {k: pack(v)[1] for k, v in row.items()}
    assert tags == {
        "date": 0x44,
        "time": 0x54,
        "localtime": 0x74,
        "localdatetime": 0x64,
        "datetime": 0x49,
        "zoned": 0x69,
        "duration": 0x45,
        "point": 0x58,
        "place": 0x59,
    }


def test_on_json_each_is_written_as_cypher_writes_it():
    assert _answer(keep_values=False) == {
        "date": "2019-06-01",
        "time": "12:31:14.645876+01:00",
        "localtime": "12:31:14",
        "localdatetime": "2019-06-01T18:40:32.142",
        "datetime": "2019-06-01T18:40:32.142+02:00",
        "zoned": "2019-06-01T18:40:32+02:00[Europe/Stockholm]",
        "duration": "P1Y2M3DT4.5S",
        "point": {"srid": 7203, "x": 2.3, "y": 4.5},
        "place": {"srid": 4979, "longitude": 1.1, "latitude": -2.0, "height": 123456.789},
    }


def test_a_point_parameter_has_its_coordinates_and_reference_system():
    row = _answer(
        keep_values=True,
        query="WITH $place AS p, $point AS q RETURN [p.longitude, p.latitude, p.height] AS geo, "
        "[p.x, p.y, p.z] AS xyz, p.srid AS srid, p.crs AS crs, [q.x, q.y] AS flat, q.crs AS plane",
    )
    assert row == {
        "geo": [1.1, -2.0, 123456.789],
        "xyz": [1.1, -2.0, 123456.789],
        "srid": 4979,
        "crs": "wgs-84-3d",
        "flat": [2.3, 4.5],
        "plane": "cartesian",
    }


@pytest.mark.parametrize(
    ("written", "point"),
    [
        ("point({x: 3, y: 4})", Point(7203, (3.0, 4.0))),
        ("point({x: 3, y: 4, z: 5})", Point(9157, (3.0, 4.0, 5.0))),
        ("point({longitude: 3, latitude: 4})", Point(4326, (3.0, 4.0))),
        ("point({longitude: 3, latitude: 4, height: 5})", Point(4979, (3.0, 4.0, 5.0))),
    ],
)
def test_a_point_the_query_builds_is_a_point(written, point):
    assert _answer(keep_values=True, query=f"RETURN {written} AS p") == {"p": point}
