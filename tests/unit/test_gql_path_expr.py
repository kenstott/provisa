# Copyright (c) 2026 Kenneth Stott
# Canary: a6c77ae8-3bab-457e-852e-d78b981e5e79
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Path expressions in the graph IR (REQ-2135): an edge, its inverse, a sequence, an alternative,
a repetition under WALK or REACH, and a negated set of edges, each lowered to SQL and run on
DuckDB over a small graph, so the pairs each one joins are checked as rows.

No front end builds these yet (SPARQL's will), so each test builds the IR: a one-relationship
Cypher pattern whose relationship is given the path expression.
"""

# Requirements: REQ-2135

import dataclasses

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.ir import (
    AltPath,
    EdgeStep,
    LabelAnd,
    LabelAny,
    LabelName,
    LabelOr,
    MatchStep,
    NegatedEdges,
    NodePattern,
    PathExpr,
    PathMode,
    RepeatPath,
    SeqPath,
)
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CUT_COLUMN, CypherTranslateError
from tests.unit.test_cypher_min_hops import _node, _rel


def _model() -> CypherLabelMap:
    """People who know people and work at companies; companies with a parent company."""
    return CypherLabelMap(
        nodes={"Person": _node("Person", "persons"), "Company": _node("Company", "companies")},
        relationships={
            "KNOWS": _rel("KNOWS", "Person", "Person", "knows_id"),
            "WORKS_AT": _rel("WORKS_AT", "Person", "Company", "company_id"),
            "PARENT": _rel("PARENT", "Company", "Company", "parent_id"),
        },
    )


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """p1 knows p2 knows p3 knows p1 (a cycle); p4 knows no one. p1 and p2 work at c1, p3 at
    c2; c1's parent is c2, c2's is c3."""
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS pg")
    c.execute("CREATE SCHEMA pg.public")
    c.execute(
        "CREATE TABLE pg.public.persons (id INTEGER, name VARCHAR, knows_id INTEGER, "
        "company_id INTEGER)"
    )
    c.execute(
        "INSERT INTO pg.public.persons VALUES "
        "(1, 'p1', 2, 1), (2, 'p2', 3, 1), (3, 'p3', 1, 2), (4, 'p4', NULL, NULL)"
    )
    c.execute("CREATE TABLE pg.public.companies (id INTEGER, name VARCHAR, parent_id INTEGER)")
    c.execute("INSERT INTO pg.public.companies VALUES (1, 'c1', 2), (2, 'c2', 3), (3, 'c3', NULL)")
    return c


def _sql(src: str, tgt: str, path: PathExpr, mode: PathMode = PathMode.WALK) -> str:
    ast = parse_cypher(f"MATCH (a:{src})-[:KNOWS]->(b:{tgt}) RETURN a.name AS a, b.name AS b")
    step = ast.pipeline[0]
    assert isinstance(step, MatchStep)
    pattern = step.matches[0].pattern
    pattern.rels[0] = dataclasses.replace(  # pyright: ignore[reportAttributeAccessIssue]
        pattern.rels[0],  # pyright: ignore[reportAttributeAccessIssue]
        types=[],
        path=path,
        mode=mode,
    )
    return cypher_to_sql(ast, _model(), {})[0].sql(dialect="duckdb")


def _rows(con, src: str, tgt: str, path: PathExpr, mode: PathMode = PathMode.WALK) -> list:
    cur = con.execute(_sql(src, tgt, path, mode))
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def _pairs(con, src: str, tgt: str, path: PathExpr, mode: PathMode = PathMode.WALK) -> list:
    return sorted((r["a"], r["b"]) for r in _rows(con, src, tgt, path, mode))


KNOWS = EdgeStep("KNOWS")


def test_an_edge(con):
    assert _pairs(con, "Person", "Person", KNOWS) == [("p1", "p2"), ("p2", "p3"), ("p3", "p1")]


def test_an_inverse_edge(con):
    assert _pairs(con, "Person", "Person", EdgeStep("KNOWS", inverse=True)) == [
        ("p1", "p3"),
        ("p2", "p1"),
        ("p3", "p2"),
    ]


def test_a_sequence(con):
    path = SeqPath((KNOWS, EdgeStep("WORKS_AT")))
    assert _pairs(con, "Person", "Company", path) == [("p1", "c1"), ("p2", "c2"), ("p3", "c1")]


def test_an_alternative(con):
    path = AltPath((KNOWS, EdgeStep("KNOWS", inverse=True)))
    assert _pairs(con, "Person", "Person", path) == [
        ("p1", "p2"),
        ("p1", "p3"),
        ("p2", "p1"),
        ("p2", "p3"),
        ("p3", "p1"),
        ("p3", "p2"),
    ]


def test_a_bounded_walk_keeps_a_row_per_walk(con):
    pairs = _pairs(con, "Person", "Person", RepeatPath(KNOWS, 1, 4))
    assert pairs.count(("p1", "p2")) == 2  # one hop, and four round the cycle
    assert len(pairs) == 12


def test_reach_keeps_each_pair_once_and_a_cycle_within_the_limit_is_not_cut(con):
    rows = _rows(con, "Person", "Person", RepeatPath(KNOWS, 1, None), PathMode.REACH)
    pairs = sorted((r["a"], r["b"]) for r in rows)
    assert pairs == sorted((f"p{i}", f"p{j}") for i in (1, 2, 3) for j in (1, 2, 3))
    assert {r[CUT_COLUMN] for r in rows} == {0}


def test_reach_past_the_limit_is_cut(con, monkeypatch):
    monkeypatch.setenv("PROVISA_GRAPH_DEPTH_LIMIT", "1")
    rows = _rows(con, "Company", "Company", RepeatPath(EdgeStep("PARENT"), 1, None), PathMode.REACH)
    assert sorted((r["a"], r["b"]) for r in rows) == [("c1", "c2"), ("c2", "c3")]
    assert {r[CUT_COLUMN] for r in rows} == {1}


def test_a_walk_round_a_cycle_is_cut_at_the_limit(con, monkeypatch):
    monkeypatch.setenv("PROVISA_GRAPH_DEPTH_LIMIT", "5")
    rows = _rows(con, "Person", "Person", RepeatPath(KNOWS, 1, None))
    assert {r[CUT_COLUMN] for r in rows} == {1}


def test_zero_or_more_pairs_each_node_with_itself(con):
    pairs = _pairs(
        con, "Company", "Company", RepeatPath(EdgeStep("PARENT"), 0, None), PathMode.REACH
    )
    assert pairs == [
        ("c1", "c1"),
        ("c1", "c2"),
        ("c1", "c3"),
        ("c2", "c2"),
        ("c2", "c3"),
        ("c3", "c3"),
    ]


def test_zero_or_one(con):
    pairs = _pairs(con, "Company", "Company", RepeatPath(EdgeStep("PARENT"), 0, 1), PathMode.REACH)
    assert pairs == [("c1", "c1"), ("c1", "c2"), ("c2", "c2"), ("c2", "c3"), ("c3", "c3")]


def test_a_repeated_sequence(con):
    path = RepeatPath(SeqPath((KNOWS, KNOWS)), 1, 1)
    assert _pairs(con, "Person", "Person", path) == [("p1", "p3"), ("p2", "p1"), ("p3", "p2")]


def test_a_minimum_above_one(con):
    pairs = _pairs(con, "Company", "Company", RepeatPath(EdgeStep("PARENT"), 2, 3), PathMode.REACH)
    assert pairs == [("c1", "c3")]


def test_a_negated_set_is_every_other_edge(con):
    pairs = _pairs(con, "Person", "Company", NegatedEdges(("KNOWS", "PARENT")))
    assert pairs == [("p1", "c1"), ("p2", "c1"), ("p3", "c2")]


@pytest.mark.parametrize("mode", [PathMode.TRAIL, PathMode.ACYCLIC, PathMode.SIMPLE])
def test_the_other_path_modes_are_refused_by_name(mode):
    with pytest.raises(CypherTranslateError, match=mode.value):
        _sql("Person", "Person", RepeatPath(KNOWS, 1, 3), mode)


def test_a_minimum_past_the_bound_is_refused():
    with pytest.raises(CypherTranslateError, match="at least 4"):
        _sql("Person", "Person", RepeatPath(KNOWS, 4, 3))


def test_an_unknown_edge_type_is_refused():
    with pytest.raises(CypherTranslateError, match="NOPE"):
        _sql("Person", "Person", EdgeStep("NOPE"))


def test_a_bounded_path_expression_carries_no_cut_column():
    assert CUT_COLUMN not in _sql("Person", "Person", RepeatPath(KNOWS, 1, 3))


def test_a_label_expression_round_trips_through_a_node_pattern():
    for expr in (
        LabelAny(),
        LabelName("Person"),
        LabelOr((LabelName("Person"), LabelName("Company"))),
        LabelAnd((LabelName("Person"), LabelName("Employee"))),
    ):
        assert NodePattern.with_label_expr("n", expr).label_expr == expr
