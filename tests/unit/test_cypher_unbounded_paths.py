# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e9a47-2d8b-4f63-a0e5-8b7d3c19f2e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An unbounded variable-length path compiles with the maximum depth (graph.depth_limit)
substituted (REQ-348, amended 2026-11-03). The walk keeps Cypher's trail rule -- no relationship
is taken twice in one path -- so on a finite graph it ends by itself, and the maximum is only a
safety limit: a walk that reaches it and could still go on fails by name, never a truncated
answer. Each engine's lowering applies the one maximum: Trino through its max_recursion_depth
session property, DuckDB and PostgreSQL through the visited-relationship check the recursion
carries."""

# Requirements: REQ-348

from collections import Counter

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from provisa.cypher.parser import parse_cypher
from provisa.gql.assembler import take_overrun
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CUT_COLUMN, PathDepthExceeded
from tests.unit.test_cypher_min_hops import _people, _sql

# p1 -> p2 -> p3 -> p1: one KNOWS each, a directed cycle of three.
_TRIANGLE = "(1, 'p1', 2), (2, 'p2', 3), (3, 'p3', 1)"
# p1 -> p2 and p2 -> p1: two relationships between the same two nodes.
_PAIR = "(1, 'p1', 2), (2, 'p2', 1)"
_CHAIN_OF_5 = "(1, 'p1', 2), (2, 'p2', 3), (3, 'p3', 4), (4, 'p4', 5), (5, 'p5', NULL)"


def _run(rows: str, cypher: str) -> list[dict]:
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute("CREATE TABLE pg.public.persons (id INTEGER, name VARCHAR, knows_id INTEGER)")
    con.execute(f"INSERT INTO pg.public.persons VALUES {rows}")
    cur = con.execute(_sql(cypher, _people()))
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def _pairs(rows: list[dict]) -> Counter:
    return Counter((r["a"], r["b"]) for r in take_overrun(rows))


@pytest.mark.parametrize("star", ["*", "*..", "*1.."])
def test_an_unbounded_pattern_parses_with_no_upper_bound(star):
    ast = parse_cypher(f"MATCH (a:Person)-[:KNOWS{star}]->(b:Person) RETURN a")
    rel = ast.match_clauses[0].pattern.rels[0]
    assert rel.variable_length and rel.max_hops is None


def test_an_unbounded_directed_walk_on_a_cycle_returns_the_trails_neo4j_returns():
    # Neo4j: from each node the walk goes round once -- 1, 2 and 3 hops, the third back to
    # itself -- and stops there, the next hop being a relationship the path already took.
    rows = _run(_TRIANGLE, "MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN a.name AS a, b.name AS b")
    assert _pairs(rows) == Counter(
        {(a, b): 1 for a in ("p1", "p2", "p3") for b in ("p1", "p2", "p3")}
    )


def test_an_unbounded_undirected_walk_takes_each_relationship_once_per_trail():
    # Neo4j: from p1, [r1] and [r2] each reach p2, [r1, r2] and [r2, r1] each come back to p1.
    rows = _run(_PAIR, "MATCH (a:Person)-[:KNOWS*]-(b:Person) RETURN a.name AS a, b.name AS b")
    assert _pairs(rows) == Counter(
        {("p1", "p2"): 2, ("p1", "p1"): 2, ("p2", "p1"): 2, ("p2", "p2"): 2}
    )


def test_a_bounded_directed_walk_repeats_no_relationship_either():
    # [*1..5] on the triangle: a fourth hop would take p1 -> p2 a second time.
    rows = _run(
        _TRIANGLE, "MATCH (a:Person)-[:KNOWS*1..5]->(b:Person) RETURN a.name AS a, b.name AS b"
    )
    assert sum(_pairs(rows).values()) == 9


def test_an_unbounded_shortest_path_takes_the_maximum():
    rows = _run(
        _TRIANGLE,
        "MATCH p = shortestPath((a:Person {name: 'p1'})-[:KNOWS*]->(b:Person {name: 'p3'})) "
        "RETURN a.name AS a, b.name AS b, length(p) AS n",
    )
    assert [(r["a"], r["b"], r["n"]) for r in take_overrun(rows)] == [("p1", "p3", 2)]


def test_a_cycle_within_the_maximum_is_answered_whole(monkeypatch):
    # The triangle's longest trail is 3 hops: a maximum of 3 is reached, never exceeded.
    monkeypatch.setenv("PROVISA_GRAPH_DEPTH_LIMIT", "3")
    rows = _run(_TRIANGLE, "MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN a.name AS a, b.name AS b")
    assert sum(_pairs(rows).values()) == 9


def test_a_walk_that_reaches_the_maximum_and_could_go_on_fails_by_name(monkeypatch):
    monkeypatch.setenv("PROVISA_GRAPH_DEPTH_LIMIT", "3")
    rows = _run(_CHAIN_OF_5, "MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN a.name AS a")
    with pytest.raises(
        PathDepthExceeded, match="variable-length path exceeded the maximum depth 3"
    ):
        take_overrun(rows)


def test_take_overrun_strips_the_column_of_a_walk_within_the_maximum():
    assert take_overrun([{"a": 1, CUT_COLUMN: 0}, {"a": 2}]) == [{"a": 1}, {"a": 2}]


def _recursive_steps(sql: str, dialect: str) -> list[exp.Expression]:
    tree = sqlglot.parse_one(sql, read=dialect)
    with_ = tree.find(exp.With)
    assert with_ is not None and with_.args.get("recursive")
    return [cte.this for cte in with_.expressions if isinstance(cte.this, exp.Union)]


@pytest.mark.parametrize("dialect", ["duckdb", "postgres"])
@pytest.mark.parametrize("arrow", ["->", "-"])
def test_the_engine_lowering_carries_the_visited_relationship_check(dialect, arrow):
    sql, _, _ = cypher_to_sql(
        parse_cypher(f"MATCH (a:Person)-[:KNOWS*]{arrow}(b:Person) RETURN a.name"), _people(), {}
    )
    lowered = sql.sql(dialect=dialect)
    (body,) = _recursive_steps(lowered, dialect)
    step = body.expression
    assert "trail" in body.this.named_selects
    # The step refuses a relationship already on the trail.
    assert step.find(exp.StrPosition) is not None
    assert any(
        isinstance(cond, exp.EQ) and isinstance(cond.this, exp.StrPosition)
        for cond in step.args["where"].find_all(exp.EQ)
    )


def test_the_trino_lowering_sets_max_recursion_depth(monkeypatch):
    from provisa.compiler.hints import recursion_session_props

    monkeypatch.setenv("PROVISA_GRAPH_DEPTH_LIMIT", "250")
    sql, _, _ = cypher_to_sql(
        parse_cypher("MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN a.name"), _people(), {}
    )
    tree = sqlglot.parse_one(sql.sql(dialect="postgres"), read="postgres")
    # The walk goes one hop past the maximum, to tell a walk that could go on.
    assert recursion_session_props(tree) == {"max_recursion_depth": "251"}


def test_a_statement_with_no_recursion_sets_no_recursion_depth():
    from provisa.compiler.hints import recursion_session_props

    tree = sqlglot.parse_one("SELECT 1", read="postgres")
    assert recursion_session_props(tree) == {}


def test_the_engine_session_hints_carry_the_recursion_depth(monkeypatch):
    from types import SimpleNamespace

    from provisa.pgwire._pipeline import _engine_session_hints

    monkeypatch.setenv("PROVISA_GRAPH_DEPTH_LIMIT", "40")
    sql, _, _ = cypher_to_sql(
        parse_cypher("MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN a.name"), _people(), {}
    )
    text = sql.sql(dialect="postgres")
    hints = _engine_session_hints(
        SimpleNamespace(), (), None, text, writes=False, tree=sqlglot.parse_one(text)
    )
    assert hints is not None and hints["max_recursion_depth"] == "41"
