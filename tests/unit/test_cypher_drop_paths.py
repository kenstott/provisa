# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6a9c14-7e3b-4d08-b5c1-94d0e7a3f261
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
# ruff: noqa: F811 -- a fixture imported here is redefined by each test that takes it

"""What a drop on a Cypher frame writes (REQ-2135, provisa-ui's graph-drop.ts) runs, on DuckDB
against the rows it must answer: a class's schema path, and a node dropped on a node."""

# Requirements: REQ-2135, REQ-348

import dataclasses
import json

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap, JunctionMapping
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CypherTranslateError
from tests.unit.test_cypher_find_paths_by_mapping import _node, _rel, _rows, chain  # noqa: F401


def test_a_class_pill_two_hops_away_is_read_through_its_path(chain):
    assert sorted(
        _rows(
            chain,
            "MATCH (a:A) OPTIONAL MATCH (a)-[r1:T]-(b:B)-[r2:T]-(c:C) RETURN a.name, b.name, c.name",
        )
    ) == [("a1", "b10", "c100"), ("a2", "b11", "c101")]


@pytest.mark.parametrize("func", ["shortestPath", "allShortestPaths"])
def test_a_node_dropped_on_a_node_finds_the_path_between_them(chain, func):
    rows = _rows(
        chain,
        f"MATCH p = {func}((a:A)-[*..4]-(c:C)) WHERE a.id = 1 AND c.id = 100 "
        "RETURN a.name, c.name LIMIT 25",
    )
    assert rows == [("a1", "c100")]


@pytest.mark.parametrize("func", ["shortestPath", "allShortestPaths"])
def test_two_nodes_nothing_joins_answer_no_path(chain, func):
    rows = _rows(
        chain,
        f"MATCH p = {func}((a:A)-[*..4]-(c:C)) WHERE a.id = 1 AND c.id = 101 "
        "RETURN a.name, c.name LIMIT 25",
    )
    assert rows == []


def test_a_node_path_returns_the_path_itself(chain):
    rows = _rows(
        chain,
        "MATCH p = allShortestPaths((a:A)-[*..4]-(c:C)) WHERE a.id = 1 AND c.id = 100 "
        "RETURN p LIMIT 25",
    )
    assert len(rows) == 1
    assert [n["id"] for n in json.loads(rows[0][0])["nodes"]] == ["A|1", "B|10", "C|100"]


@pytest.fixture
def around():
    """A reaches C through B (real rows) and through P, a parameterized table: its read needs an
    argument, so P's rows are no rows a path can walk. C keeps a self-join through B's sibling so
    a walk between two nodes of one label takes the recursive form."""
    p = dataclasses.replace(_node("P", "p"), native_filter_columns={"_nf_breed": "varchar"})
    lm = CypherLabelMap(
        nodes={"A": _node("A", "a"), "B": _node("B", "b"), "C": _node("C", "c"), "P": p},
        relationships={
            "AB::A→B": dataclasses.replace(_rel("A", "B", "b_id", "id"), rel_type="AB"),
            "BC::B→C": dataclasses.replace(_rel("B", "C", "c_id", "id"), rel_type="BC"),
            "AP::A→P": dataclasses.replace(_rel("A", "P", "id", "a_id"), rel_type="AP"),
            "PC::P→C": dataclasses.replace(_rel("P", "C", "c_id", "id"), rel_type="PC"),
        },
    )
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS cat")
    con.execute("CREATE SCHEMA cat.public")
    con.execute("CREATE TABLE cat.public.a (id INTEGER, name VARCHAR, b_id INTEGER)")
    con.execute("CREATE TABLE cat.public.b (id INTEGER, name VARCHAR, c_id INTEGER)")
    con.execute("CREATE TABLE cat.public.c (id INTEGER, name VARCHAR)")
    con.execute("CREATE TABLE cat.public.p (id INTEGER, name VARCHAR, a_id INTEGER, c_id INTEGER)")
    con.execute("INSERT INTO cat.public.a VALUES (1, 'a1', 10), (2, 'a2', 11)")
    con.execute("INSERT INTO cat.public.b VALUES (10, 'b10', 100), (11, 'b11', 101)")
    con.execute("INSERT INTO cat.public.c VALUES (100, 'c100'), (101, 'c101')")
    # What the path must not read: a row that would join a2 to c100.
    con.execute("INSERT INTO cat.public.p VALUES (7, 'p7', 2, 100)")
    return lm, con


def _sql(chain, cypher: str) -> str:
    lm, _ = chain
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    return apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="postgres")


@pytest.mark.parametrize("func", ["shortestPath", "allShortestPaths"])
def test_a_path_function_goes_round_a_parameterized_table(around, func):
    query = f"MATCH p = {func}((a:A)-[*..4]-(c:C)) WHERE c.id = 100 RETURN a.name, c.name LIMIT 25"
    assert _rows(around, query) == [("a1", "c100")]
    assert '"p"' not in _sql(around, query)


def test_an_any_path_goes_round_a_parameterized_table(around):
    query = "MATCH (a:A)-[*1..4]-(c:C) WHERE c.id = 100 RETURN a.name, c.name"
    assert _rows(around, query) == [("a1", "c100")]
    assert '"p"' not in _sql(around, query)


@pytest.mark.parametrize(
    "query",
    [
        "MATCH p = allShortestPaths((x:A)-[*..4]-(y:A)) RETURN x.name, y.name",
        "MATCH (x:A)-[*1..4]-(y:A) RETURN x.name, y.name",
        "MATCH (x:A)-[*1..4]-(y) RETURN x.name",
        "MATCH (x:A)-[:AP*1..4]-(y:C) RETURN x.name",
    ],
    ids=["all-shortest-one-label", "any-one-label", "free-end", "named-type"],
)
def test_a_recursive_walk_never_reads_a_parameterized_table(around, query):
    assert '"p"' not in _sql(around, query)


def test_a_walk_with_only_a_parameterized_way_answers_no_path(around):
    assert _rows(around, "MATCH (x:A)-[:AP*1..4]-(y:C) RETURN x.name") == []


@pytest.mark.parametrize("func", ["shortestPath", "allShortestPaths", "allPaths"])
def test_a_path_ending_on_a_parameterized_table_is_refused_by_name(around, func):
    with pytest.raises(CypherTranslateError, match="a path cannot include .*P"):
        _sql(around, f"MATCH p = {func}((a:A)-[*..4]-(x:P)) RETURN a.name")


def test_a_path_starting_on_a_parameterized_table_is_refused_by_name(around):
    with pytest.raises(CypherTranslateError, match="a path cannot include .*P"):
        _sql(around, "MATCH (x:P)-[*1..4]-(c:C) RETURN c.name")


def test_a_single_hop_to_a_parameterized_table_stays_a_join_of_its_own(around):
    # Not a path search: the query names the hop, and gives the argument itself.
    assert '"p"' in _sql(around, "MATCH (a:A)-[:AP]-(x:P) WHERE x._nf_breed = 'x' RETURN a.name")


@pytest.fixture
def declared_twice():
    """SUBMITTED and SUBMITTED_BY: one join, declared from each end."""
    lm = CypherLabelMap(
        nodes={"U": _node("U", "u"), "I": _node("I", "i"), "T": _node("T", "t")},
        relationships={
            "SUBMITTED::U→I": dataclasses.replace(
                _rel("U", "I", "id", "user_id"), rel_type="SUBMITTED"
            ),
            "SUBMITTED_BY::I→U": dataclasses.replace(
                _rel("I", "U", "user_id", "id"), rel_type="SUBMITTED_BY"
            ),
            "ABOUT::I→T": dataclasses.replace(_rel("I", "T", "t_id", "id"), rel_type="ABOUT"),
        },
    )
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS cat")
    con.execute("CREATE SCHEMA cat.public")
    con.execute("CREATE TABLE cat.public.u (id INTEGER, name VARCHAR)")
    con.execute(
        "CREATE TABLE cat.public.i (id INTEGER, name VARCHAR, user_id INTEGER, t_id INTEGER)"
    )
    con.execute("CREATE TABLE cat.public.t (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO cat.public.u VALUES (1, 'u1')")
    con.execute("INSERT INTO cat.public.i VALUES (10, 'i10', 1, 100)")
    con.execute("INSERT INTO cat.public.t VALUES (100, 't100')")
    return lm, con


def test_inverse_declarations_are_one_join():
    forward = dataclasses.replace(_rel("U", "I", "id", "user_id"), rel_type="SUBMITTED")
    inverse = dataclasses.replace(_rel("I", "U", "user_id", "id"), rel_type="SUBMITTED_BY")
    other = dataclasses.replace(_rel("U", "I", "id", "reviewer_id"), rel_type="REVIEWED")
    assert forward.join == inverse.join
    assert forward.join != other.join


def test_a_junctions_types_are_each_a_join():
    via = JunctionMapping("c", "s", "edges", "Edges", ("a",), ("b",), "kind", "KIND_OF")
    one = dataclasses.replace(_rel("T", "T", "id", "id"), rel_type="KIND_OF", via=via)
    two = dataclasses.replace(
        one, rel_type="PART_OF", via=dataclasses.replace(via, type_value="PART_OF")
    )
    mirrored = dataclasses.replace(
        one,
        rel_type="HAS_KIND",
        via=dataclasses.replace(via, source_columns=("b",), target_columns=("a",)),
    )
    assert one.join != two.join
    assert one.join == mirrored.join


@pytest.mark.parametrize(
    "query",
    [
        "MATCH p = allShortestPaths((u:U)-[*..4]-(t:T)) RETURN u.name, t.name",
        "MATCH (u:U)-[*1..4]-(t:T) RETURN u.name, t.name",
        "MATCH (u:U)-[*1..4]-(v:U) RETURN u.name, v.name",
    ],
    ids=["all-shortest", "any-path", "one-label"],
)
def test_a_join_declared_twice_answers_once(declared_twice, query):
    rows = _rows(declared_twice, query)
    assert rows == list(dict.fromkeys(rows)), rows
