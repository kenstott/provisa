# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A path with no upper bound walks to the operator's depth limit (graph.depth_limit), and a result
cut there says so as a statement warning (REQ-2135, REQ-1350). The recursive form runs on DuckDB,
so whether the walk was cut is read from rows, not only from SQL."""

# Requirements: REQ-2135, REQ-1350

import duckdb
import pytest

from provisa.core.statement_warnings import NoWarningChannel, collecting
from provisa.cypher.parser import parse_cypher
from provisa.gql.assembler import take_cut
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CUT_COLUMN, GraphVarKind
from tests.unit.test_cypher_min_hops import _orders, _people, _sql


@pytest.fixture
def limit3(monkeypatch):
    monkeypatch.setenv("PROVISA_GRAPH_DEPTH_LIMIT", "3")


def _persons(con: duckdb.DuckDBPyConnection, rows: str) -> None:
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute("CREATE TABLE pg.public.persons (id INTEGER, name VARCHAR, knows_id INTEGER)")
    con.execute(f"INSERT INTO pg.public.persons VALUES {rows}")


def _run(rows: str, cypher: str) -> list[dict]:
    con = duckdb.connect()
    _persons(con, rows)
    cur = con.execute(_sql(cypher, _people()))
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


_CHAIN_OF_5 = "(1, 'p1', 2), (2, 'p2', 3), (3, 'p3', 4), (4, 'p4', 5), (5, 'p5', NULL)"
_CHAIN_OF_3 = "(1, 'p1', 2), (2, 'p2', 3), (3, 'p3', NULL)"
_UNBOUNDED = "MATCH (a:Person)-[:KNOWS*1..]->(b:Person) RETURN a.name AS a, b.name AS b"


@pytest.mark.usefixtures("limit3")
def test_an_unbounded_path_walks_to_the_depth_limit_and_is_cut_there():
    rows = _run(_CHAIN_OF_5, _UNBOUNDED)
    assert {(r["a"], r["b"]) for r in rows} == {
        (f"p{i}", f"p{j}") for i in range(1, 6) for j in range(i + 1, min(i + 3, 5) + 1)
    }
    assert {r[CUT_COLUMN] for r in rows} == {1}


@pytest.mark.usefixtures("limit3")
def test_a_walk_that_ends_within_the_limit_is_not_cut():
    rows = _run(_CHAIN_OF_3, _UNBOUNDED)
    assert {(r["a"], r["b"]) for r in rows} == {("p1", "p2"), ("p1", "p3"), ("p2", "p3")}
    assert {r[CUT_COLUMN] for r in rows} == {0}


@pytest.mark.usefixtures("limit3")
def test_a_cycle_is_cut_at_the_limit():
    rows = _run("(1, 'p1', 2), (2, 'p2', 1)", _UNBOUNDED)
    assert {r[CUT_COLUMN] for r in rows} == {1}


def test_the_default_depth_limit_is_ten():
    assert "hops <= 10" in _sql(_UNBOUNDED, _people())


def test_a_bounded_path_carries_no_cut_column():
    sql, _, graph_vars = cypher_to_sql(
        parse_cypher("MATCH (a:Person)-[:KNOWS*1..4]->(b:Person) RETURN a.name"), _people(), {}
    )
    assert CUT_COLUMN not in sql.sql() and CUT_COLUMN not in graph_vars


@pytest.mark.usefixtures("limit3")
def test_the_cut_column_is_a_graph_var_of_its_own_kind():
    _, _, graph_vars = cypher_to_sql(parse_cypher(_UNBOUNDED), _people(), {})
    assert graph_vars[CUT_COLUMN] == GraphVarKind.CUT


def test_a_flat_path_is_cut_when_the_schema_has_a_longer_path(monkeypatch):
    monkeypatch.setenv("PROVISA_GRAPH_DEPTH_LIMIT", "1")
    cut = _sql("MATCH (o:Order)-[*1..]->(c:City) RETURN o.name", _orders())
    assert '"customer_id"' not in cut and f"THEN 1 ELSE 0 END AS {CUT_COLUMN}" in cut
    monkeypatch.setenv("PROVISA_GRAPH_DEPTH_LIMIT", "2")
    assert CUT_COLUMN not in _sql("MATCH (o:Order)-[*1..]->(c:City) RETURN o.name", _orders())


@pytest.mark.usefixtures("limit3")
def test_a_distinct_union_keeps_one_row_whatever_its_parts_cut():
    rows = _run(
        _CHAIN_OF_5,
        "MATCH (a:Person)-[:KNOWS*1..]->(b:Person) RETURN a.name AS x "
        "UNION MATCH (a:Person) RETURN a.name AS x",
    )
    assert sorted(r["x"] for r in rows) == ["p1", "p2", "p3", "p4", "p5"]
    assert {r[CUT_COLUMN] for r in rows} == {1}


@pytest.mark.usefixtures("limit3")
def test_take_cut_strips_the_column_and_warns_on_the_plan_and_the_request():
    plan_warnings: list = []
    with collecting() as found:
        rows = take_cut([{"a": 1, CUT_COLUMN: 1}, {"a": 2, CUT_COLUMN: 1}], plan_warnings)
    assert rows == [{"a": 1}, {"a": 2}]
    assert [w.code for w in plan_warnings] == ["statement.path_cut"]
    assert [w.code for w in found] == ["statement.path_cut"]
    assert plan_warnings[0].params == {"limit": 3}


def test_take_cut_says_nothing_of_an_uncut_result():
    plan_warnings: list = []
    rows = take_cut([{"a": 1, CUT_COLUMN: 0}, {"a": 2}], plan_warnings)
    assert rows == [{"a": 1}, {"a": 2}] and plan_warnings == []


def test_a_cut_with_no_channel_to_say_it_fails():
    with pytest.raises(NoWarningChannel):
        take_cut([{"a": 1, CUT_COLUMN: 1}], None)
