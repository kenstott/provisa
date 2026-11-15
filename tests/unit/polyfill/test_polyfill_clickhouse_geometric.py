# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2b6c14-9f3a-4d70-b1c5-3a7e9d0f2b68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A shape's measures on ClickHouse (REQ-2236; REQ-2167): each is the form pg_forms_geometric writes
of its geo_ops.c row, so its answer is PostgreSQL's. The samples are the function support run's;
each answer is PostgreSQL 17's. A float answer of many steps is checked against geo_ops.c's steps
in the same order, in Python's doubles."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import math
from typing import Any

import pytest
import sqlglot

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import GRID, geometric, trino
from provisa.transpiler.transpile import transpile


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _one(runtime: ClickHouseFederationRuntime, sql: str) -> Any:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    return value


def _call(function: str, *typed: tuple[str, str]) -> str:
    args = ", ".join(f"{value}::{kind}" for value, kind in typed)
    return f'SELECT pg_catalog."{function}"({args}) AS v'


_PI = 3.141592653589793


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (_call("area", ("'(3,4),(0,0)'", "box")), 12.0),
        (_call("area", ("'(5,2),(2,1)'", "box")), 3.0),
        (_call("area", ("'<(1,2),3>'", "circle")), 3.0 * 3.0 * _PI),
        (_call("area", ("'<(-1,-1),0.5>'", "circle")), 0.5 * 0.5 * _PI),
        (_call("area", ("'((0,0),(1,0),(1,1))'", "path")), 0.5),
        (_call("area", ("'[(0,0),(2,2)]'", "path")), None),
        (_call("area", ("'((1,1),(4,1),(4,5))'", "path")), 6.0),
        (_call("center", ("'<(-1,-1),0.5>'", "circle")), "(-1,-1)"),
        (_call("circle_center", ("'<(1,2),3>'", "circle")), "(1,2)"),
        (_call("diagonal", ("'(1,1),(-1,-1)'", "box")), "[(1,1),(-1,-1)]"),
        # A box's text in either order of its corners: high, then low.
        (_call("diagonal", ("'(0,0),(3,4)'", "box")), "[(3,4),(0,0)]"),
        (_call("height", ("'(3,4),(0,0)'", "box")), 4.0),
        (_call("width", ("'(5,2),(2,1)'", "box")), 3.0),
        (_call("radius", ("'<(-1,-1),0.5>'", "circle")), 0.5),
        (_call("isclosed", ("'((0,0),(1,0),(1,1))'", "path")), True),
        (_call("isclosed", ("'[(0,0),(2,2)]'", "path")), False),
        (_call("isopen", ("'[(0,0),(2,2)]'", "path")), True),
        (_call("ishorizontal", ("'(1,2)'", "point"), ("'(5,2.0000001)'", "point")), True),
        (_call("isvertical", ("'(1,2)'", "point"), ("'(0,0)'", "point")), False),
        (_call("ishorizontal", ("'{0,1,-2}'", "line")), True),
        (_call("line_vertical", ("'{1,0,0}'", "line")), True),
        (_call("isparallel", ("'{1,2,3}'", "line"), ("'{2,4,5}'", "line")), True),
        (_call("line_parallel", ("'{1,2,3}'", "line"), ("'{1,0,0}'", "line")), False),
        (_call("isperp", ("'{1,0,0}'", "line"), ("'{0,1,-2}'", "line")), True),
        (_call("line_perp", ("'{1,1,0}'", "line"), ("'{1,-1,3}'", "line")), True),
        (_call("line_perp", ("'{1,2,3}'", "line"), ("'{1,0,0}'", "line")), False),
        (_call("isvertical", ("'[(1,1),(1,5)]'", "lseg")), True),
        (_call("lseg_horizontal", ("'[(-1,0),(2,0)]'", "lseg")), True),
        (_call("isparallel", ("'[(0,0),(3,4)]'", "lseg"), ("'[(1,1),(4,5)]'", "lseg")), True),
        (_call("lseg_parallel", ("'[(1,1),(1,5)]'", "lseg"), ("'[(2,0),(2,9)]'", "lseg")), True),
        (_call("isperp", ("'[(1,1),(1,5)]'", "lseg"), ("'[(-1,0),(2,0)]'", "lseg")), True),
        (_call("lseg_perp", ("'[(0,0),(3,4)]'", "lseg"), ("'[(0,0),(4,-3)]'", "lseg")), True),
        (_call("lseg_perp", ("'[(0,0),(3,4)]'", "lseg"), ("'[(1,1),(1,5)]'", "lseg")), False),
        (_call("lseg_length", ("'[(0,0),(3,4)]'", "lseg")), 5.0),
        (_call("path_length", ("'((1,1),(4,1),(4,5))'", "path")), 12.0),
        (_call("path_length", ("'[(0,0),(2,2)]'", "path")), math.sqrt(8.0)),
        (_call("pt_contained_circle", ("'(1,2)'", "point"), ("'<(1,2),3>'", "circle")), True),
        (_call("pt_contained_circle", ("'(0,0)'", "point"), ("'<(-1,-1),0.5>'", "circle")), False),
        (_call("pt_contained_poly", ("'(1,2)'", "point"), ("'((0,0),(4,0),(4,4),(0,4))'", "polygon")), True),
        (_call("pt_contained_poly", ("'(0,0)'", "point"), ("'((1,1),(2,3),(3,1))'", "polygon")), False),
        (_call("area", ("NULL", "box")), None),
        (_call("pt_contained_poly", ("NULL", "point"), ("'((1,1),(2,3),(3,1))'", "polygon")), None),
    ],
)  # fmt: skip
def test_a_measure_answers_postgresql_on_clickhouse(runtime, sql, answer):
    assert _one(runtime, sql) == answer


# --- the same steps in the same order ---------------------------------------------------------


def _hypot(x: float, y: float) -> float:
    """pg_hypot."""
    x, y = abs(x), abs(y)
    if x < y:
        x, y = y, x
    if y == 0.0:
        return x
    yx = y / x
    return x * math.sqrt(1.0 + yx * yx)


_RING = [(0.1, 0.2), (3.7, 0.3), (4.9, 2.2), (1.3, 5.1), (-0.7, 1.9)]
_RING_TEXT = ",".join(f"({x},{y})" for x, y in _RING)


def _path_area(points: list[tuple[float, float]]) -> float:
    area = 0.0
    for i, (x, y) in enumerate(points):
        nx, ny = points[(i + 1) % len(points)]
        area = area + x * ny
        area = area - y * nx
    return abs(area) / 2.0


def _path_length(points: list[tuple[float, float]], closed: bool) -> float:
    result = 0.0
    for i in range(len(points)):
        if i == 0 and not closed:
            continue
        px, py = points[i - 1]  # the last point before the first, of a closed path
        result = result + _hypot(px - points[i][0], py - points[i][1])
    return result


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (_call("area", (f"'({_RING_TEXT})'", "path")), _path_area(_RING)),
        (_call("path_length", (f"'({_RING_TEXT})'", "path")), _path_length(_RING, True)),
        (_call("path_length", (f"'[{_RING_TEXT}]'", "path")), _path_length(_RING, False)),
        (_call("lseg_length", ("'[(0.1,0.2),(3.7,0.3)]'", "lseg")), _hypot(0.1 - 3.7, 0.2 - 0.3)),
        (_call("area", ("'<(0,0),0.3>'", "circle")), 0.3 * 0.3 * _PI),
    ],
)
def test_a_measure_of_many_steps_is_geo_ops_steps_in_order(runtime, sql, answer):
    assert _one(runtime, sql) == answer  # exact: the same doubles in the same order


def test_an_answer_that_is_not_finite_raises(runtime):
    sql = _call("area", ("'(1e308,1e308),(-1e308,-1e308)'", "box"))
    with pytest.raises(Exception) as raised:
        _one(runtime, sql)
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == "0A000"
    assert str(error) == "box_area does not read this value"


@pytest.mark.parametrize(
    ("sql", "function"),
    [
        # PostgreSQL resolves no one overload of area() or ishorizontal() of a value of no type.
        ("SELECT pg_catalog.\"area\"('(3,4),(0,0)') AS v", "area"),
        ("SELECT pg_catalog.\"ishorizontal\"('[(0,0),(1,0)]') AS v", "ishorizontal"),
        ('SELECT pg_catalog."center"(NULL) AS v', "center"),
    ],
)
def test_an_overload_the_statement_does_not_type_keeps_its_refusal(sql, function):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    assert str(refused.value) == f"{function}() has no form on clickhouse"


def test_a_name_of_one_overload_reads_a_value_of_no_type_as_it():
    # diagonal has the one overload, of a box: PostgreSQL reads an untyped literal as one.
    sql = "SELECT pg_catalog.\"diagonal\"('(0,0),(3,4)') AS v"
    assert "postgresql-error 0A000: box_diagonal" in transpile(sql, "clickhouse")


def test_the_33_measures_are_rows_and_every_other_dialect_keeps_what_it_had():
    calls = [overload for overloads in geometric._CALLS.values() for overload in overloads]
    assert len(calls) == 33
    call = sqlglot.parse_one(_call("area", ("'(3,4),(0,0)'", "box")), read="postgres")
    node = call.selects[0].this.expression  # past pg_catalog
    for dialect in ("duckdb", "trino", "snowflake"):
        applied = GRID.read(node, dialect)
        assert applied is not None and applied.form is trino.KEPT
    for feature in geometric.FEATURES:
        (stated,) = feature.read.limitations["clickhouse"]
        assert "exact" in stated.text and "0A000" in stated.text
