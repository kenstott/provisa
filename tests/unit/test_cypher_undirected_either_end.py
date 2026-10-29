# Copyright (c) 2026 Kenneth Stott
# Canary: e7f63397-6c9b-401c-b27e-5424d2992b75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An undirected relationship between nodes the pattern names no type of (REQ-347): each node is
either end of each relationship, so `MATCH (n)-[:REL1]-()` reads REL1's sources and its targets
alike, as Neo4j does, over the model's tables as over a session's graph, and so does the pattern
predicate `WHERE (n)-[:REL1]-()`. Beside them, the reads of the openCypher TCK's Create5 [2] and
[4] and Temporal4's `MATCH (n) RETURN n.dates` over an empty model (REQ-2176), each answered on
DuckDB."""

# Requirements: REQ-347, REQ-2176

from __future__ import annotations

import pytest

from provisa.gql.label_map import RelationshipMapping
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

# REL1 runs from an A to a B; a2 has no B and b11 no A.
_MODEL = {
    "A": ({"id": "integer", "name": "text", "b_id": "integer"}, "(1, 'a1', 10), (2, 'a2', NULL)"),
    "B": ({"id": "integer", "name": "text"}, "(10, 'b10'), (11, 'b11')"),
}
_REL1 = RelationshipMapping("REL1", "A", "B", "b_id", "id", "rel1")


def _model(monkeypatch) -> Session:
    return Session(monkeypatch, _MODEL, relationships=[_REL1])


def _session(monkeypatch) -> Session:
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (:A {name: 'a1'})-[:REL1]->(:B {name: 'b10'}), (:A {name: 'a2'}), (:B {name: 'b11'})"
    )
    return s


def _names(rows: list[dict]) -> list[tuple]:
    return sorted(tuple(r.values()) for r in rows)


_GRAPHS = pytest.mark.parametrize("graph", [_model, _session], ids=["model", "session"])


@_GRAPHS
@pytest.mark.parametrize(
    "query",
    [
        "MATCH (n)-[:REL1]-() RETURN n.name",
        "MATCH ()-[:REL1]-(n) RETURN n.name",
        "MATCH (n) WHERE (n)-[:REL1]-() RETURN n.name",
        "MATCH (n) MATCH (n)-[:REL1]-() RETURN n.name",
    ],
)
def test_an_unnamed_node_is_either_end(monkeypatch, graph, query):
    assert _names(graph(monkeypatch).run(query)) == [("a1",), ("b10",)]


@_GRAPHS
def test_both_unnamed_ends_read_each_relationship_both_ways(monkeypatch, graph):
    rows = graph(monkeypatch).run("MATCH (n)-[r:REL1]-(m) RETURN n.name, type(r), m.name")
    assert _names(rows) == [("a1", "REL1", "b10"), ("b10", "REL1", "a1")]


@_GRAPHS
@pytest.mark.parametrize(
    ("query", "names"),
    [
        # A direction, or a label on the node, still reads one end.
        ("MATCH (n)-[:REL1]->() RETURN n.name", [("a1",)]),
        ("MATCH (n)<-[:REL1]-() RETURN n.name", [("b10",)]),
        ("MATCH (n:A)-[:REL1]-() RETURN n.name", [("a1",)]),
        ("MATCH (n:B)-[:REL1]-() RETURN n.name", [("b10",)]),
    ],
)
def test_a_direction_or_a_label_reads_one_end(monkeypatch, graph, query, names):
    assert _names(graph(monkeypatch).run(query)) == names


def test_a_models_property_keeps_its_type_from_either_end(monkeypatch):
    rows = _model(monkeypatch).run("MATCH (n)-[:REL1]-() RETURN n.id, n.name")
    assert _names(rows) == [(1, "a1"), (10, "b10")]


def test_create5_2_a_reverse_chain_reads_back(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:A)<-[:R]-(:B)<-[:R]-(:C)")
    rows = s.run("MATCH (a)<-[:R]-(b)<-[:R]-(c) RETURN a, b, c")
    assert [(r["a"]["label"], r["b"]["label"], r["c"]["label"]) for r in rows] == [("A", "B", "C")]


def test_create5_4_hops_of_three_types_read_back(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:R1]->()<-[:R2]-()-[:R3]->()")
    rows = s.run("MATCH ()-[r1:R1]->()<-[r2:R2]-()-[r3:R3]->() RETURN r1, r2, r3")
    assert [(r["r1"]["type"], r["r2"]["type"], r["r3"]["type"]) for r in rows] == [
        ("R1", "R2", "R3")
    ]


@pytest.mark.parametrize("prop", ["dates", "d"])
def test_a_property_of_no_node_over_an_empty_model_reads_no_rows(monkeypatch, prop):
    s = Session(monkeypatch, {})
    assert s.run(f"MATCH (n) RETURN n.{prop}") == []
    s.run("CREATE ({dates: [date('1910-05-06')]})")
    assert len(s.run(f"MATCH (n) RETURN n.{prop}")) == 1
