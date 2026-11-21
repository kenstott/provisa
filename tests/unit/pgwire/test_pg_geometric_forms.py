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
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler import pg_forms_geometric as geometric
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.geometric_corpus import statements
from tests.unit.pgwire.geometric_replay import (
    BY_FUNCTION,
    column_statement,
    function_of,
    outcome,
    same,
)
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


def _sample() -> list[str]:
    """Each function's first statement, and a share of the rest: DuckDB binds the longest forms
    in about 300ms, and this file keeps to a minute."""
    first = [sqls[0] for sqls in BY_FUNCTION.values()]
    return list(dict.fromkeys((*first, *list(RECORDED)[::24])))


def test_the_corpus_is_what_was_recorded():
    assert list(RECORDED) == statements()


def test_each_function_is_replayed():
    assert sorted(BY_FUNCTION) == sorted({function_of(sql) for sql in statements()})
    assert sum(map(len, BY_FUNCTION.values())) == len(RECORDED)


def test_duckdb_answers_as_postgresql_17_answered():
    connection = duckdb.connect()
    run = lambda sql: connection.execute(sql).fetchone()[0]  # noqa: E731
    wrong = {}
    for sql in _sample():
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


@pytest.mark.parametrize(
    "sql, want",
    [
        # Read from geometry.out and geo_ops.c: a line's three shapes, and what meets and does not.
        ("SELECT dist_pl('(1,0)'::point, '{1,-1,0}'::line)", 0.7071067811865476),
        ("SELECT dist_lp('{0,-1,2}'::line, '(5,0)'::point)", 2.0),
        ("SELECT close_pl('(1,0)'::point, '{0,-1,2}'::line)", "(1,2)"),
        ("SELECT dist_ps('(0,1)'::point, '[(0,0),(4,0)]'::lseg)", 1.0),
        ("SELECT close_ps('(9,1)'::point, '[(0,0),(4,0)]'::lseg)", "(4,0)"),
        ("SELECT dist_pb('(5,0)'::point, '(3,4),(0,0)'::box)", 2.0),
        ("SELECT dist_pb('(1,1)'::point, '(3,4),(0,0)'::box)", 0.0),
        ("SELECT close_pb('(5,1)'::point, '(3,4),(0,0)'::box)", "(3,1)"),
        ("SELECT line_eq('{1,-1,0}'::line, '{2,-2,0}'::line)", True),
        ("SELECT line_eq('{1,-1,0}'::line, '{1,-1,1}'::line)", False),
        ("SELECT line_eq('[(0,0),(1,1)]'::line, '{1,-1,0}'::line)", True),
        ("SELECT line_interpt('{1,-1,0}'::line, '{0,-1,2}'::line)", "(2,2)"),
        ("SELECT line_interpt('{1,-1,0}'::line, '{2,-2,0}'::line)", None),
        ("SELECT line_interpt('{1,-1,0}'::line, '{1,-1,-5}'::line)", None),
        ("SELECT line_intersect('{1,-1,0}'::line, '{1,-1,-5}'::line)", False),
        ("SELECT line_distance('{0,-1,0}'::line, '{0,-1,3}'::line)", 3.0),
        ("SELECT line_distance('{1,-1,0}'::line, '{0,-1,2}'::line)", 0.0),
        ("SELECT lseg_interpt('[(0,0),(2,2)]'::lseg, '[(0,2),(2,0)]'::lseg)", "(1,1)"),
        ("SELECT lseg_interpt('[(0,0),(1,1)]'::lseg, '[(2,2),(3,3)]'::lseg)", None),
        ("SELECT lseg_intersect('[(0,0),(2,2)]'::lseg, '[(0,2),(2,0)]'::lseg)", True),
        ("SELECT lseg_distance('[(0,0),(1,0)]'::lseg, '[(0,3),(1,3)]'::lseg)", 3.0),
        ("SELECT close_lseg('[(0,0),(1,0)]'::lseg, '[(0,3),(1,3)]'::lseg)", None),
        ("SELECT on_pl('(1,1)'::point, '{1,-1,0}'::line)", True),
        ("SELECT on_ps('(1,1)'::point, '[(0,0),(2,2)]'::lseg)", True),
        ("SELECT on_sl('[(0,0),(1,1)]'::lseg, '{1,-1,0}'::line)", True),
        ("SELECT on_sb('[(1,1),(2,2)]'::lseg, '(3,4),(0,0)'::box)", True),
        ("SELECT on_ppath('(1,1)'::point, '[(0,0),(2,2)]'::path)", True),
        ("SELECT on_ppath('(1,0.5)'::point, '((0,0),(4,0),(4,4))'::path)", True),
        ("SELECT path_contain_pt('[(0,0),(2,2)]'::path, '(3,3)'::point)", False),
        ("SELECT inter_sl('[(0,0),(2,2)]'::lseg, '{0,-1,1}'::line)", True),
        ("SELECT inter_sb('[(-1,1),(5,1)]'::lseg, '(3,4),(0,0)'::box)", True),
        ("SELECT inter_lb('{1,-1,0}'::line, '(10,1),(8,0)'::box)", False),
        ("SELECT dist_ppath('(0,0)'::point, '[(9,9)]'::path)", 0.0),
        ("SELECT dist_ppoly('(5,0)'::point, '((0,0),(4,0),(4,4),(0,4))'::polygon)", 1.0),
        ("SELECT dist_ppoly('(1,1)'::point, '((0,0),(4,0),(4,4),(0,4))'::polygon)", 0.0),
        ("SELECT dist_cpoly('<(5,0),0.5>'::circle, '((0,0),(4,0),(4,4))'::polygon)", 0.5),
        ("SELECT poly_contain_pt('((0,0),(4,0),(4,4),(0,4))'::polygon, '(4,2)'::point)", True),
        ("SELECT poly_same('((0,0),(1,0),(1,1))'::polygon, '((1,1),(0,0),(1,0))'::polygon)", True),
        ("SELECT poly_same('((0,0),(1,0),(1,1))'::polygon, '((1,0),(0,0),(1,1))'::polygon)", True),
        ("SELECT poly_same('((0,0),(1,0),(1,1))'::polygon, '((0,0),(1,0))'::polygon)", False),
        (
            "SELECT poly_overlap('((0,0),(4,0),(4,4))'::polygon, '((1,1),(2,1),(2,1.5))'::polygon)",
            True,
        ),
        (
            "SELECT poly_distance('((0,0),(1,0),(1,1))'::polygon, '((4,0),(5,0),(5,1))'::polygon)",
            3.0,
        ),
        ("SELECT path_distance('[(9,9)]'::path, '[(0,0),(1,1)]'::path)", None),
        ("SELECT path_distance('[(0,0),(1,0)]'::path, '[(0,2),(1,2)]'::path)", 2.0),
        ("SELECT path_inter('[(0,0),(2,2)]'::path, '[(0,2),(2,0)]'::path)", True),
        ("SELECT path_inter('[(0,0),(1,1)]'::path, '[(5,5),(6,6)]'::path)", False),
    ],
)
def test_the_forms_of_two_shapes(sql: str, want: Any):
    assert duckdb.connect().execute(transpile(sql, "duckdb")).fetchone()[0] == want


@pytest.mark.parametrize(
    "sql, want",
    [
        # numeric.out and arrays.out, and the float8 and numeric answers of one value that differ.
        ("SELECT width_bucket(5.35, 0.024, 10.06, 5)", 3),
        ("SELECT width_bucket(5.35::double precision, 0.024::double precision, 10::float8, 5)", 3),
        ("SELECT width_bucket(5.35::numeric, 0.024::numeric, 10.06::numeric, 5)", 3),
        ("SELECT width_bucket(0.3, 0, 0.9, 3)", 2),
        ("SELECT width_bucket(-1, 0, 10, 5)", 0),
        ("SELECT width_bucket(10, 0, 10, 5)", 6),
        ("SELECT width_bucket(11, 10, 0, 5)", 0),
        ("SELECT width_bucket(0, 10, 0, 5)", 6),
        ("SELECT width_bucket(0.99999999999::numeric, 0::numeric, 1::numeric, 1)", 1),
        ("SELECT width_bucket(NULL::numeric, 0::numeric, 1::numeric, 4)", None),
        ("SELECT width_bucket(5, ARRAY[1, 3, 4, 6])", 3),
        ("SELECT width_bucket(5, ARRAY[9, 1, 7, 2, 3])", 2),
        ("SELECT width_bucket(5, ARRAY[3])", 1),
        ("SELECT width_bucket(5, ARRAY[]::integer[])", 0),
        ("SELECT width_bucket('b'::text, ARRAY['a', 'c', 'e'])", 1),
        ("SELECT width_bucket(5, NULL::integer[])", None),
    ],
)
def test_width_bucket(sql: str, want: Any):
    assert duckdb.connect().execute(transpile(sql, "duckdb")).fetchone()[0] == want


@pytest.mark.parametrize(
    "sql, raised",
    [
        ("SELECT width_bucket(1::float8, 0::float8, 1::float8, 0)", "2201G: count must"),
        ("SELECT width_bucket(1::numeric, 0::numeric, 1::numeric, -1)", "2201G: count must"),
        ("SELECT width_bucket(1::float8, 2::float8, 2::float8, 4)", "2201G: lower bound"),
        ("SELECT width_bucket(1::numeric, 1::numeric, 1::numeric, 4)", "2201G: lower bound"),
        ("SELECT width_bucket(1::float8, 0::float8, 1::float8, 2147483647)", "22003: integer"),
        ("SELECT width_bucket(1::numeric, 0::numeric, 1::numeric, 2147483647)", "22003: integer"),
        ("SELECT width_bucket(5, ARRAY[3, 4, NULL])", "22004: thresholds array"),
        ("SELECT width_bucket(5, ARRAY[ARRAY[1, 2], ARRAY[3, 4]])", "2202E: thresholds must"),
        ("SELECT line_eq('{0,0,1}'::line, '{1,-1,0}'::line)", "22P02: invalid line"),
        ("SELECT line_eq('[(1,1),(1,1)]'::line, '{1,-1,0}'::line)", "22P02: invalid line"),
    ],
)
def test_postgresql_refuses_width_bucket_and_line_as_it_does(sql: str, raised: str):
    with pytest.raises(duckdb.Error, match=raised):
        duckdb.connect().execute(transpile(sql, "duckdb"))


@pytest.mark.parametrize(
    "name, sql",
    [
        ("width_bucket", "width_bucket('NaN'::float8, 0::float8, 1::float8, 4)"),
        ("width_bucket", "width_bucket(0::float8, 'Infinity'::float8, 1::float8, 4)"),
        ("width_bucket", "width_bucket('NaN'::numeric, 0::numeric, 1::numeric, 4)"),
        ("width_bucket", "width_bucket('NaN'::float8, ARRAY[1.5]::float8[])"),
        ("width_bucket", "width_bucket(1.5::float8, ARRAY['NaN'::float8])"),
        (
            "width_bucket",
            "width_bucket(1234567890123456789012.1234567::numeric, 0::numeric, 1::numeric, 4)",
        ),
        ("width_bucket", "width_bucket(0.9999999999999999999::numeric, 0::numeric, 3::numeric, 3)"),
        ("width_bucket", "width_bucket('B'::text, ARRAY['a', 'c'])"),
        ("dist_pl", "dist_pl('(nan,0)'::point, '{1,-1,0}'::line)"),
        ("line_eq", "line_eq('{inf,1,0}'::line, '{1,-1,0}'::line)"),
        ("close_ls", "close_ls('{1,-1,0}'::line, '[(inf,0),(1,1)]'::lseg)"),
        ("poly_same", "poly_same('((nan,0),(1,1))'::polygon, '((0,0),(1,1))'::polygon)"),
        ("path_inter", "path_inter('[(0,0),(inf,1)]'::path, '[(0,0),(1,1)]'::path)"),
    ],
)
def test_what_no_engine_answers_is_refused_by_name(name: str, sql: str):
    with pytest.raises(duckdb.Error, match=f"{name} does not read this value"):
        duckdb.connect().execute(transpile(f"SELECT {sql}", "duckdb"))


COLUMNS = (
    "SELECT i, a, b, s, x, p, q, c FROM (VALUES "
    "(1, '(1,0)', '{1,-1,0}', '[(0,0),(4,0)]', '(3,4),(0,0)', '[(0,0),(2,2)]', "
    "'((0,0),(4,0),(4,4))', '<(5,0),0.5>'), "
    "(2, '(9,1)', '{0,-1,2}', '[(0,3),(1,3)]', '(10,10),(8,8)', '((0,0),(1,0),(1,1))', "
    "'((1,1),(2,3),(3,1))', '<(0,0),1>'), "
    "(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) AS t(i, a, b, s, x, p, q, c)"
)
_TYPES_OF = {
    "a": "point",
    "b": "line",
    "s": "lseg",
    "x": "box",
    "p": "path",
    "q": "polygon",
    "c": "circle",
}


@pytest.mark.parametrize(
    "call, want",
    [
        ("dist_pl(a, b)", [0.7071067811865476, 1.0, None]),
        ("close_pl(a, b)", ["(0.5,0.5)", "(9,2)", None]),
        ("dist_ps(a, s)", [0.0, 8.246211251235321, None]),
        ("on_pl(a, b)", [False, False, None]),
        ("line_eq(b, b)", [True, True, None]),
        ("dist_pb(a, x)", [0.0, 7.0, None]),
        ("dist_sb(s, x)", [0.0, 8.602325267042627, None]),
        ("inter_sb(s, x)", [True, False, None]),
        ("dist_ppoly(a, q)", [0.0, 6.0, None]),
        ("dist_cpoly(c, q)", [0.5, 0.41421356237309515, None]),
        ("poly_same(q, q)", [True, True, None]),
        ("path_distance(p, p)", [0.0, 0.0, None]),
        ("on_ppath(a, p)", [False, False, None]),
    ],
)
def test_a_form_reads_a_column_as_it_reads_a_literal(call: str, want: list[Any]):
    """Each row its own answer, its NULL a NULL: a form that folded a literal would not."""
    typed = re.sub(r"\b([abspqxc])\b", lambda m: f"{m[1]}::{_TYPES_OF[m[1]]}", call)
    sql = f"SELECT {typed} FROM ({COLUMNS}) t ORDER BY i"
    got = [row[0] for row in duckdb.connect().execute(transpile(sql, "duckdb")).fetchall()]
    assert got == want, sql


def test_width_bucket_reads_the_type_of_a_column():
    """No statement says a column's type: the engine's decides float8's rounding or numeric's."""
    doubles = (
        "SELECT width_bucket(x, lo, hi, n) FROM (VALUES (5.35::float8, 0.024::float8, 10.06::float8"
        ", 5), (1.0::float8, 2.0::float8, 3.0::float8, 2)) AS t(x, lo, hi, n)"
    )
    decimals = (
        "SELECT width_bucket(x, lo, hi, n) FROM (VALUES (CAST(0.3 AS DECIMAL(10,3)), "
        "CAST(0 AS DECIMAL(10,3)), CAST(0.9 AS DECIMAL(10,3)), 3)) AS t(x, lo, hi, n)"
    )
    connection = duckdb.connect()
    assert connection.execute(transpile(doubles, "duckdb")).fetchall() == [(3,), (0,)]
    assert connection.execute(transpile(decimals, "duckdb")).fetchall() == [(2,)]


def test_a_recorded_function_reads_columns_on_each_row():
    statement = column_statement("point_add")
    assert statement is not None
    sql, wants = statement
    got = [r[0] for r in duckdb.connect().execute(transpile(sql, "duckdb")).fetchall()]
    assert list(map(same, got, wants)) == [True, True], (sql, got, wants)


_SHAPE = "SELECT box_add('(3,4),(0,0)'::box, '(1,2)'::point) AS v"
_ANALYZER_OFF = " SETTINGS allow_experimental_analyzer = 0, precise_float_parsing = 1"


def test_clickhouse_plans_a_shapes_form_with_its_old_analyzer():
    # And reads a shape's numbers as float8in does: the nearest double ('0.3' is not, otherwise).
    assert transpile(_SHAPE, "clickhouse").endswith(_ANALYZER_OFF)


@pytest.mark.parametrize(
    ("sql", "dialect"),
    [("SELECT 1 AS v", "clickhouse"), (_SHAPE, "trino"), (_SHAPE, "duckdb")],
)
def test_no_other_statement_or_engine_is_planned_otherwise(sql: str, dialect: str):
    assert "SETTINGS" not in transpile(sql, dialect)


def test_trino_writes_a_name_read_once_in_place():
    """Trino's planning grows with how deep a lambda is nested: a box's form keeps only the lambdas
    of the names it reads more than once."""
    written = transpile("SELECT box_center('(3,4),(0,0)'::box) AS v", "trino")
    assert written.count("TRANSFORM(ARRAY[") <= 3, written  # 15 when each name was a lambda


_CLOSE_SB = "SELECT close_sb('[(1,2),(3,4)]'::lseg, '(5,5),(0,0)'::box) AS v"
_DEFINED = re.compile(r"FUNCTION (pg_geo_\w+)\(")


def _lambda_depth(expression: str) -> int:
    """How many lambdas the deepest one of ``expression`` is inside, itself counted."""

    def held(node: exp.Expr | None) -> int:
        return 0 if node is None else isinstance(node, exp.Lambda) + held(node.parent)

    tree = sqlglot.parse_one(f"SELECT {expression}", read="trino")
    return max((held(lam) for lam in tree.find_all(exp.Lambda)), default=0)


def _routines(written: str) -> list[str]:
    head, _, _ = written.partition(" SELECT CASE WHEN ")
    return _DEFINED.findall(head)


def test_trino_defines_each_helper_once_as_a_routine_of_the_statement():
    """Trino's analysis of a lambda grows with how deep it is nested: close_sb took 136s as helpers
    inside helpers. Each is a routine the statement defines once, after the routines it calls."""
    written = transpile(_CLOSE_SB, "trino")
    assert written.startswith("WITH FUNCTION pg_geo_"), written[:200]
    defined = _routines(written)
    assert len(defined) == len(set(defined)), defined
    assert {"pg_geo_near_box_seg", "pg_geo_near_seg", "pg_geo_cut", "pg_geo_float_text"} <= set(
        defined
    )
    for at, name in enumerate(defined):
        definition, calls = geometric.ROUTINES[name]
        assert written.count(definition) == 1, name
        assert calls <= set(defined[:at]), (name, calls, defined[:at])


def test_no_routine_holds_a_lambda_more_than_six_deep():
    for name, (definition, _) in geometric.ROUTINES.items():
        assert _lambda_depth(definition.split(" RETURN ", 1)[1]) <= 6, name


def test_trino_defines_only_the_routines_a_statement_calls():
    written = transpile("SELECT box_center('(3,4),(0,0)'::box) AS v", "trino")
    assert _routines(written) == ["pg_geo_float_text"], written[:300]
    assert transpile("SELECT 1 AS v", "trino") == "SELECT 1 AS v"


@pytest.mark.parametrize("dialect", ["duckdb", "clickhouse"])
def test_no_other_engine_calls_a_routine(dialect: str):
    written = transpile(_CLOSE_SB, dialect)
    assert "pg_geo_" not in written.lower() and "FUNCTION" not in written


def _duckdb_lambda_depth(written: str) -> int:
    """How many lambdas the deepest one of a DuckDB statement is inside, itself counted."""

    def held(node: exp.Expr | None) -> int:
        return 0 if node is None else isinstance(node, exp.Lambda) + held(node.parent)

    tree = sqlglot.parse_one(written, read="duckdb")
    return max((held(lam) for lam in tree.find_all(exp.Lambda)), default=0)


def test_duckdb_writes_a_run_of_names_as_one_pipeline():
    """DuckDB 2.0 keeps each list lambda's body twice (the bound argument and its bind data's copy),
    so a body nested n lambdas deep is held 2^n times: point_add's form, ten deep as one name inside
    the next, bound in 884MB, poly_contain's in 3.7GB. A run of names is a pipeline of stages, each
    stage's lambda beside the next, not inside it."""
    written = transpile("SELECT point_add('(1,2)'::point, '(3,4)'::point) AS v", "duckdb")
    assert _duckdb_lambda_depth(written) <= 4, written[:300]


def test_no_duckdb_form_holds_a_lambda_more_than_ten_deep():
    """Thirteen deep before (poly_contain, close_lseg): the deepest, poly_distance, is ten now, and
    binds in 1.5GB where it took 4.4GB."""
    deep = {
        name: depth
        for name, sqls in BY_FUNCTION.items()
        if (depth := _duckdb_lambda_depth(transpile(sqls[0], "duckdb"))) > 10
    }
    assert not deep, deep


def test_a_pipeline_reads_a_column_named_as_a_name_of_the_form():
    """A name of the form is read from its stage; a column of the same name is the column's."""
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t AS SELECT '(1,2)' AS _a, '(3,4)' AS _b")
    written = transpile("SELECT point_add(t._a::point, t._b::point) AS v FROM t", "duckdb")
    assert connection.execute(written).fetchall() == [("(4,6)",)]
