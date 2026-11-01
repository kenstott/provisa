# Copyright (c) 2026 Kenneth Stott
# Canary: d2c6f8a1-4e07-4b39-91d6-58a3e7b0c214
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The geometric forms on DuckDB against what PostgreSQL 17.11 answered (REQ-2171, AN ANSWER THAT
DIFFERS IS REFUSED). Each engine replays every statement in tests/integration/test_geometric_on_engines.py,
one test to a function; DuckDB binds a form in about 125ms, so this file replays a quarter of them,
each overload still reached."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
import sys
from math import isclose
from typing import Any

import duckdb
import pytest

from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.geometric_corpus import statements
from tests.unit.pgwire.geometric_replay import BY_FUNCTION, function_of, outcome
from tests.unit.pgwire.pg17_geometric_answers import RECORDED


#: glibc's cos and sin, PostgreSQL's in postgres:17.11, are correctly rounded for the angles of
#: polygon(circle); macOS's are a digit off for some. The exact replay is the integration file's,
#: and Linux here: this slack is a developer's Mac only.
_LIBM_SLACK = 0.0 if sys.platform == "linux" else 1e-15
_NUMBERS = re.compile(r"-?[0-9.]+(?:e[-+][0-9]+)?")


def _within_libm(sql: str, got: Any, want: str | None) -> bool:
    """A polygon of a circle, the one form through cos and sin, against PostgreSQL's text."""
    if "::circle" not in sql or not sql.startswith("SELECT polygon(") or not want:
        return False
    numbers = _NUMBERS.findall(str(got)), _NUMBERS.findall(want)
    return len(numbers[0]) == len(numbers[1]) and all(
        isclose(float(a), float(b), rel_tol=0.0, abs_tol=_LIBM_SLACK) for a, b in zip(*numbers)
    )


def test_the_corpus_is_what_was_recorded():
    assert list(RECORDED) == statements()


def test_each_function_is_replayed():
    assert sorted(BY_FUNCTION) == sorted({function_of(sql) for sql in statements()})
    assert sum(map(len, BY_FUNCTION.values())) == len(RECORDED)


def test_duckdb_answers_as_postgresql_17_answered():
    connection = duckdb.connect()
    run = lambda sql: connection.execute(sql).fetchone()[0]  # noqa: E731
    wrong = {}
    for sql in list(RECORDED)[::4]:
        said = outcome(sql, "duckdb", run)
        want = RECORDED[sql][1]
        if said != "ok" and not _within_libm(sql, run(transpile(sql, "duckdb")), want):
            wrong[sql] = said
    assert not wrong, f"{len(wrong)} of {len(RECORDED)} {list(wrong.items())[:10]}"


@pytest.mark.parametrize(
    "sql, want",
    [
        ("SELECT point_add('(1,2)'::point, '(0.1,0.2)'::point)", "(1.1,2.2)"),
        ("SELECT point(1.5::double precision, 2::double precision)", "(1.5,2)"),
        ("SELECT slope('(1,1)'::point, '(1,2)'::point)", float("inf")),
        ("SELECT slope('(1,2)'::point, '(1,2)'::point)", float("inf")),
        ("SELECT point_eq(NULL::point, '(1,1)'::point)", None),
        ("SELECT npoints('((0,0),(1,0),(1,1))'::polygon)", 3),
        ("SELECT line('(1,1)'::point, '(1,5)'::point)", "{-1,0,1}"),
        ("SELECT line('(1,1)'::point, '(4,1)'::point)", "{0,-1,1}"),
        ("SELECT line('(0,0)'::point, '(1,1)'::point)", "{1,-1,0}"),
        ("SELECT circle('(1,2)'::point, 3::double precision)", "<(1,2),3>"),
        ("SELECT polygon('(3,4),(0,0)'::box)", "((0,0),(0,4),(3,4),(3,0))"),
        ("SELECT lseg('(3,4),(0,0)'::box)", "[(3,4),(0,0)]"),
        ("SELECT path('((0,0),(1,0),(1,1))'::polygon)", "((0,0),(1,0),(1,1))"),
        ("SELECT popen('((0,0),(1,0))'::path)", "[(0,0),(1,0)]"),
        ("SELECT pclose('[(0,0),(1,0)]'::path)", "((0,0),(1,0))"),
        ("SELECT path_add('[(0,0)]'::path, '[(1,0)]'::path)", "[(0,0),(1,0)]"),
        ("SELECT path_add('((0,0),(1,0))'::path, '[(1,0)]'::path)", None),
        ("SELECT circle_add_pt('<(1,2),3>'::circle, '(1,1)'::point)", "<(2,3),3>"),
        ("SELECT poly_contain('((0,0),(4,0),(4,4),(0,4))'::polygon, NULL::polygon)", None),
    ],
)
def test_a_form_writes_postgresqls_text(sql: str, want: Any):
    assert duckdb.connect().execute(transpile(sql, "duckdb")).fetchone()[0] == want


SQUARE = "((0,0),(4,0),(4,4),(0,4))"
NOTCHED = "((0,4),(6,4),(3,2),(6,0),(0,0))"
ACROSS_THE_NOTCH = "((1,2.5),(5,2.5),(5,1.5),(1,1.5))"


@pytest.mark.parametrize(
    "outer, inner, want",
    [
        (SQUARE, "((1,1),(2,3),(3,1))", True),
        (SQUARE, SQUARE, True),
        (SQUARE, "((0,0),(2,0),(2,2),(0,2))", True),
        (SQUARE, "((2,0),(4,2),(2,4),(0,2))", True),
        (SQUARE, "((3,3),(5,3),(5,5),(3,5))", False),
        ("((1,1),(2,3),(3,1))", SQUARE, False),
        (NOTCHED, "((1,1),(1,3),(2,3),(2,1))", True),
        (NOTCHED, ACROSS_THE_NOTCH, False),
        ("((0,0),(4,0),(4,4),(2,2),(0,4))", "((1,1),(3,1),(3,3),(1,3))", False),
    ],
)
def test_a_polygon_contains_a_polygon(outer: str, inner: str, want: bool):
    connection = duckdb.connect()
    contain = f"SELECT poly_contain('{outer}'::polygon, '{inner}'::polygon)"
    contained = f"SELECT poly_contained('{inner}'::polygon, '{outer}'::polygon)"
    assert connection.execute(transpile(contain, "duckdb")).fetchone()[0] is want
    assert connection.execute(transpile(contained, "duckdb")).fetchone()[0] is want


@pytest.mark.parametrize(
    "sql, raised",
    [
        ("SELECT line('(1,2)'::point, '(1,2)'::point)", "22023: invalid line specification"),
        ("SELECT polygon('[(0,0),(2,2)]'::path)", "22023: open path"),
        ("SELECT polygon(1::integer, '<(1,2),3>'::circle)", "22023: must request at least 2"),
        ("SELECT polygon('<(1,1),0>'::circle)", "0A000: cannot convert circle with radius zero"),
        ("SELECT circle_div_pt('<(1,2),3>'::circle, '(0,0)'::point)", "22012: division by zero"),
        ("SELECT path_div_pt('[(1,2)]'::path, '(0,0)'::point)", "22012: division by zero"),
    ],
)
def test_postgresql_refuses_as_postgresql_refuses(sql: str, raised: str):
    with pytest.raises(duckdb.Error, match=raised):
        duckdb.connect().execute(transpile(sql, "duckdb"))


@pytest.mark.parametrize(
    "name, sql",
    [
        ("circle_add_pt", "circle_add_pt('<(0,0),inf>'::circle, '(1,1)'::point)"),
        ("circle_same", "circle_same('<(nan,0),1>'::circle, '<(0,0),1>'::circle)"),
        ("path_add_pt", "path_add_pt('[(0,nan)]'::path, '(1,1)'::point)"),
        ("lseg_eq", "lseg_eq('[(inf,0),(1,1)]'::lseg, '[(0,0),(1,1)]'::lseg)"),
        ("poly_npoints", "npoints('((-inf,0),(1,1))'::polygon)"),
        ("poly_contain", "poly_contain('((inf,0),(1,1))'::polygon, '((0,0),(1,1))'::polygon)"),
        ("poly_contained", "poly_contained('((0,0),(1,1))'::polygon, '((nan,0),(1,1))'::polygon)"),
    ],
)
def test_a_shape_not_finite_is_refused_by_name(name: str, sql: str):
    with pytest.raises(duckdb.Error, match=f"{name} does not read this value"):
        duckdb.connect().execute(transpile(f"SELECT {sql}", "duckdb"))


def test_a_shape_with_an_infinity_is_refused_by_name():
    with pytest.raises(duckdb.Error, match="point_add does not read this value"):
        duckdb.connect().execute(
            transpile("SELECT point_add('(inf,1)'::point, '(1,1)'::point)", "duckdb")
        )
