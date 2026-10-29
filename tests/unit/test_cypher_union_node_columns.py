# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3e9a41-5b2d-4f86-a0e7-19d4c8b6f235
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A node matched by no label is a union of every label's rows, and carries the columns each
later clause reads of it (REQ-2167, the openCypher TCK's WithOrderBy, ReturnOrderBy, WithWhere and
Comparison scenarios): an ORDER BY's properties beside the node returned whole, through a WITH as
that WITH's columns, two such nodes beside each other as their own columns, and its identity where
it is compared with another node. Each is answered on DuckDB, over a session's graph as over the
model's tables, and its SQL is one statement for Postgres and Trino alike."""

# Requirements: REQ-2167, REQ-2176

import sqlglot

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from tests.unit.ephemeral_harness import Session, model_node
from provisa.gql.label_map import CypherLabelMap

_MODEL = {
    "Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Cy')"),
    "Pet": ({"id": "integer", "name": "text", "legs": "integer"}, "(1, 'Bo', 4)"),
}


def _session(monkeypatch) -> Session:
    s = Session(monkeypatch, {})
    s.run("CREATE (:A {num: 9, name: 'a'}), (:B {num: 5, name: 'b'}), (:C {num: 30, name: 'c'})")
    return s


def test_a_return_ordered_by_a_property_returns_the_node_whole(monkeypatch):
    s = Session(monkeypatch, _MODEL)
    rows = s.run("MATCH (n) RETURN n ORDER BY n.name SKIP 1 LIMIT 2")
    assert [(r["n"]["label"], r["n"]["properties"]["name"]) for r in rows] == [
        ("Pet", "Bo"),
        ("Person", "Cy"),
    ]
    # Every property of its own type, not only the one ordered by.
    assert rows[0]["n"]["properties"]["legs"] == "4"


def test_a_with_ordered_by_a_property_carries_the_node(monkeypatch):
    s = _session(monkeypatch)
    rows = s.run("MATCH (a) WITH a ORDER BY a.num LIMIT 2 RETURN a")
    assert [r["a"]["label"] for r in rows] == ["B", "A"]


def test_a_node_goes_on_through_two_withs_beside_its_property(monkeypatch):
    s = _session(monkeypatch)
    rows = s.run(
        "MATCH (a) WITH a, a.num AS num WITH a, num ORDER BY num DESC LIMIT 2 RETURN a, num"
    )
    assert [(r["a"]["label"], r["num"]) for r in rows] == [("C", 30), ("A", 9)]


def test_two_nodes_a_with_carries_keep_their_own_columns(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:A), (:B)")
    same = s.run("MATCH (a), (b) WITH a, b WHERE a = b RETURN a, b")
    assert sorted((r["a"]["label"], r["b"]["label"]) for r in same) == [("A", "A"), ("B", "B")]
    other = s.run("MATCH (a), (b) WITH a, b WHERE a <> b RETURN a, b")
    assert sorted((r["a"]["label"], r["b"]["label"]) for r in other) == [("A", "B"), ("B", "A")]


def test_a_carried_node_is_compared_with_a_matched_one_by_identity(monkeypatch):
    s = _session(monkeypatch)
    assert s.run("MATCH (a) WITH a MATCH (b) WHERE a = b RETURN count(b) AS c") == [{"c": 3}]


def test_a_list_property_of_a_union_is_a_list(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:A {list: [2, -2], list2: [3, -2]}), (:B {list: [1, 2], list2: [2, -2]})")
    rows = s.run("MATCH (a) WITH a ORDER BY a.list2 + a.list DESC LIMIT 1 RETURN a")
    assert [r["a"]["label"] for r in rows] == ["A"]


def test_a_pattern_predicate_on_a_node_of_several_labels_holds_for_its_own_row(monkeypatch):
    # The pattern goes on from the outer row's node, tied by its identity, never matched afresh:
    # E has no REL1, so it is not answered.
    s = Session(monkeypatch, {})
    s.run("CREATE (:C)-[:REL1]->(:D), (:E)")
    rows = s.run("MATCH (n) WHERE (n)-[:REL1]->() RETURN n")
    assert [r["n"]["label"] for r in rows] == ["C"]


def test_a_pattern_predicate_on_a_carried_node_of_several_labels_holds_for_its_own_row(
    monkeypatch,
):
    s = Session(monkeypatch, {})
    s.run("CREATE (:C)-[:REL1]->(:D), (:E)")
    rows = s.run("MATCH (n) WITH n WHERE (n)-[:REL1]->() RETURN n")
    assert [r["n"]["label"] for r in rows] == ["C"]


def test_its_sql_is_one_statement_on_every_dialect():
    nodes = {
        "A": model_node("A", {"id": "integer", "list": "list<bigint>", "num": "integer"}),
        "B": model_node("B", {"id": "integer", "list": "list<bigint>", "num": "integer"}),
    }
    lm = CypherLabelMap(
        nodes=nodes, relationships={}, aliases={}, nodes_by_table={k: [k] for k in nodes}
    )
    cypher = (
        "MATCH (a), (b) WITH a, b, a.num AS num WHERE a = b "
        "WITH a, b, num ORDER BY a.list + b.list, num RETURN a, b"
    )
    sql, _, graph_vars = cypher_to_sql(parse_cypher(cypher, lm), lm, {})
    text = apply_graph_rewrites(sql, graph_vars, lm).sql(dialect="postgres")
    assert "LIST<" not in text
    for dialect in ("duckdb", "postgres", "trino"):
        assert sqlglot.transpile(text, read="postgres", write=dialect)
