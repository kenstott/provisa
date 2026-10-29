# Copyright (c) 2026 Kenneth Stott
# Canary: 72d93003-102c-4481-ad60-04d875d65826
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Patterns read in a graph session (REQ-347, REQ-348, REQ-2176): openCypher TCK scenarios of
the Match*, MatchWhere*, Pattern*, Path*, Quantifier*, ExistentialSubquery* and
TriadicSelection* features, run on DuckDB through the session's own read path."""

# Requirements: REQ-347, REQ-348, REQ-2176

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def _sorted(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


# --- a session's own node carries only its own properties ----------------------------------------


def test_a_session_node_returned_by_its_label_carries_no_key_property(monkeypatch):
    # TCK MatchWhere5 [1]: a node the session created has the properties it was created with;
    # the built-in _id is a model node's key (REQ-2136), which a session node has not.
    s = Session(monkeypatch, {})
    s.run("CREATE (root:Root {name: 'x'})-[:T]->(:TextNode {var: 'text'})")
    rows = s.run("MATCH (:Root {name: 'x'})-->(i:TextNode) RETURN i")
    assert [r["i"]["properties"] for r in rows] == [{"var": "text"}]


# --- a walk read in a WITH ------------------------------------------------------------------------


def test_a_walk_read_by_a_with_is_defined_ahead_of_it(monkeypatch):
    # TCK Quantifier1 [8]: the walk's recursive CTE comes before the WITH stage that reads it.
    s = Session(monkeypatch, {})
    s.run("CREATE (:SNodes)-[:RA]->(:A {name: 'a'})-[:RB]->(:B {name: 'b'})")
    rows = s.run(
        "MATCH p = (:SNodes)-[*0..2]->(x) WITH tail(nodes(p)) AS nodes "
        "RETURN size(nodes) AS n, none(x IN nodes WHERE x.name = 'b') AS result"
    )
    assert _sorted(rows) == [(0, True), (1, True), (2, False)]


def test_a_walks_node_list_carried_by_a_with_is_its_nodes(monkeypatch):
    # TCK Quantifier1 [8]: tail(nodes(p)) carried by a WITH is a list of nodes, each property of
    # which a quantifier reads, and RETURN gives those nodes.
    s = Session(monkeypatch, {})
    s.run("CREATE (:SNodes)-[:RA]->(:A {name: 'a'})")
    rows = s.run(
        "MATCH p = (:SNodes)-[*0..1]->(x) WITH tail(nodes(p)) AS nodes "
        "RETURN nodes, any(x IN nodes WHERE x.name = 'a') AS result"
    )
    got = sorted(([n["properties"] for n in r["nodes"]], r["result"]) for r in rows)
    assert got == [([], False), ([{"name": "a"}], True)]


def test_a_quantifier_reads_a_property_of_each_relationship_of_a_walk(monkeypatch):
    # TCK Quantifier3 [9]
    s = Session(monkeypatch, {})
    s.run("CREATE (:S)-[:RA {name: 'a'}]->()-[:RB {name: 'b'}]->()")
    rows = s.run(
        "MATCH p = (:S)-[*1..2]->(x) "
        "RETURN length(p) AS l, all(r IN relationships(p) WHERE r.name = 'a') AS result"
    )
    assert _sorted(rows) == [(1, True), (2, False)]


# --- a node's labels, every one -----------------------------------------------------------------


def test_a_node_of_several_labels_is_matched_by_all_of_them_and_returned_with_each(monkeypatch):
    # TCK Match1 [3]
    s = Session(monkeypatch, {})
    s.run("CREATE (:A:B:C), (:A:B), (:A:C), (:B:C), (:A), (:B), (:C), ()")
    rows = s.run("MATCH (a:A:B) RETURN a")
    assert sorted(r["a"]["labels"] for r in rows) == [["A", "B"], ["A", "B", "C"]]


def test_an_optional_end_is_returned_with_every_label(monkeypatch):
    # TCK Match7 [23]
    s = Session(monkeypatch, {})
    s.run("CREATE (:X), (x:X), (y1:Y), (y2:Y:Z) CREATE (x)-[:REL]->(y1), (x)-[:REL]->(y2)")
    rows = s.run("MATCH (a:X) OPTIONAL MATCH (a)-->(b:Y) RETURN b")
    got = sorted(((r["b"] or {}).get("labels") for r in rows), key=repr)
    assert got == [None, ["Y", "Z"], ["Y"]]


def test_a_label_predicate_reads_the_nodes_labels(monkeypatch):
    # TCK MatchWhere1 [1]: a node a union of labels reads is tested by its own labels.
    s = Session(monkeypatch, {})
    s.run("CREATE (a:A {name: 'a'}), (b:B {name: 'b'}) CREATE (a)-[:ADMIN]->(b)")
    rows = s.run("MATCH (a)-[:ADMIN]-(b) WHERE a:A RETURN a.name, b.name")
    assert _sorted(rows) == [("a", "b")]


# --- one hop of a session's pattern, from its one relationships relation -------------------------


def test_a_hop_of_several_types(monkeypatch):
    # TCK Match2 [6]
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}), "
        "(a)-[:KNOWS]->(b), (a)-[:HATES]->(c), (a)-[:WONDERS]->(c)"
    )
    rows = s.run("MATCH (n)-[r:KNOWS|HATES]->(x) RETURN type(r) AS t, x.name")
    assert _sorted(rows) == [("HATES", "C"), ("KNOWS", "B")]


def test_a_hop_from_a_labelled_node_reaches_every_label_its_type_reaches(monkeypatch):
    # J198: an unlabelled end is every node the relationship reaches, not one label's.
    s = Session(monkeypatch, {})
    s.run("CREATE (a:A)-[:T]->(:B), (a)-[:T]->(:C), (a)-[:T]->()")
    assert s.run("MATCH (n:A)-[:T]->(x) RETURN count(*) AS c") == [{"c": 3}]


def test_a_hop_in_a_session_holding_no_relationship_matches_nothing(monkeypatch):
    # TCK Match2 [1]: a pattern no relationship can match answers no rows, never an error.
    s = Session(monkeypatch, {})
    s.run("CREATE (:A)")
    assert s.run("MATCH ()-[r]->() RETURN r") == []


def test_a_path_of_hops_between_unlabelled_nodes_holds_its_every_node(monkeypatch):
    # TCK Match6 [3]
    s = Session(monkeypatch, {})
    s.run("CREATE (:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})-[:KNOWS]->(:C {name: 'C'})")
    rows = s.run("MATCH p = (a {name: 'A'})-[rel1]->(b)-[rel2]->(c) RETURN p")
    [path] = [r["p"] for r in rows]
    assert [n["properties"]["name"] for n in path["nodes"]] == ["A", "B", "C"]
    assert [e["type"] for e in path["edges"]] == ["KNOWS", "KNOWS"]


def test_a_path_read_backward_starts_at_its_first_node(monkeypatch):
    # TCK Match6 [6]
    s = Session(monkeypatch, {})
    s.run("CREATE (:A)-[:T]->(:B)")
    [path] = [r["p"] for r in s.run("MATCH p = (b)<--(a) RETURN p")]
    assert [n["labels"] for n in path["nodes"]] == [["B"], ["A"]]


def test_a_pattern_comprehension_of_paths_returns_paths(monkeypatch):
    # TCK Pattern2 [1]
    s = Session(monkeypatch, {})
    s.run("CREATE (a:A), (b:B) CREATE (a)-[:T]->(b), (b)-[:T]->(:C)")
    rows = s.run("MATCH (n) RETURN [p = (n)-->() | p] AS list")
    got = sorted([[n["labels"] for n in p["nodes"]] for p in r["list"]] for r in rows)
    assert got == [[], [[["A"], ["B"]]], [[["B"], ["C"]]]]


def test_an_optional_matchs_where_conditions_a_hop_read_from_the_relationships(monkeypatch):
    # TCK MatchWhere6 [2]
    s = Session(monkeypatch, {})
    s.run("CREATE (s:Single), (a:A {num: 42}) CREATE (s)-[:REL]->(a)")
    assert s.run("MATCH (n:Single) OPTIONAL MATCH (n)-[r]-(m) WHERE m:NonExistent RETURN r") == [
        {"r": None}
    ]


def test_a_list_of_a_hops_elements_orders_by_kind(monkeypatch):
    # TCK ReturnOrderBy1 [11] over a hop of an unlabelled end.
    s = Session(monkeypatch, {})
    s.run("CREATE (:N)-[:REL]->()")
    rows = s.run("MATCH p = (n:N)-[r]->() UNWIND [p, 1.5, n, 'text', r] AS v RETURN v ORDER BY v")
    kinds = [
        "path"
        if isinstance(v, dict) and "edges" in v
        else "rel"
        if isinstance(v, dict) and "startNode" in v
        else "node"
        if isinstance(v, dict)
        else v
        for v in (r["v"] for r in rows)
    ]
    assert kinds == ["node", "rel", "path", "text", 1.5]


# --- J198, J199 on the model graph --------------------------------------------------------------

_T_REACHES_TWO = {
    "A": (
        {"id": "integer", "name": "text", "b_id": "integer", "c_id": "integer"},
        "(1, 'a1', 1, 1), (2, 'a2', 2, NULL)",
    ),
    "B": ({"id": "integer", "name": "text"}, "(1, 'b1'), (2, 'b2')"),
    "C": ({"id": "integer", "name": "text"}, "(1, 'c1')"),
}


def _two_targets(monkeypatch) -> Session:
    from provisa.gql.label_map import RelationshipMapping

    rels = [
        RelationshipMapping("T", "A", "B", "b_id", "id", "t_ab"),
        RelationshipMapping("T", "A", "C", "c_id", "id", "t_ac"),
    ]
    return Session(monkeypatch, _T_REACHES_TWO, relationships=rels)


def test_j198_an_unlabelled_target_is_every_label_its_type_reaches(monkeypatch):
    s = _two_targets(monkeypatch)
    assert s.run("MATCH (n:A)-[:T]->(x) RETURN count(*) AS c") == [{"c": 3}]
    rows = s.run("MATCH (x)<-[:T]-(n:A) RETURN n.name, x.name")
    assert _sorted(rows) == [("a1", "b1"), ("a1", "c1"), ("a2", "b2")]
    # A labelled target is that label's alone.
    assert _sorted(s.run("MATCH (n:A)-[:T]->(x:B) RETURN x.name")) == [("b1",), ("b2",)]


def test_j198_a_pattern_comprehension_reaches_every_label_its_type_reaches(monkeypatch):
    s = _two_targets(monkeypatch)
    rows = s.run("MATCH (n:A) RETURN n.name AS n, [(n)-[:T]->(x) | x.name] AS xs")
    assert sorted((r["n"], sorted(r["xs"])) for r in rows) == [
        ("a1", ["b1", "c1"]),
        ("a2", ["b2"]),
    ]


def test_j199_a_node_of_several_labels_carried_beside_a_value_keeps_its_properties(monkeypatch):
    s = _two_targets(monkeypatch)
    rows = s.run("MATCH (n) WITH n, 1 AS d RETURN n.name AS name, d")
    assert _sorted(rows) == [("a1", 1), ("a2", 1), ("b1", 1), ("b2", 1), ("c1", 1)]


def test_j199_in_a_session(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:X {name: 'x'}), ({name: 'y'})")
    rows = s.run("MATCH (n) WITH n, 1 AS d RETURN n.name AS name, d")
    assert _sorted(rows) == [("x", 1), ("y", 1)]


# --- a pattern of several hops, variable-length ones among them ---------------------------------


def test_zero_length_walks_in_the_middle_of_a_pattern(monkeypatch):
    # TCK Match4 [3]
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}), ({name: 'D'}), ({name: 'E'}) "
        "CREATE (a)-[:CONTAINS]->(b), (b)-[:FRIEND]->(c)"
    )
    rows = s.run(
        "MATCH (a {name: 'A'})-[:CONTAINS*0..1]->(b)-[:FRIEND*0..1]->(c) "
        "RETURN a.name, b.name, c.name"
    )
    assert _sorted(rows) == [("A", "A", "A"), ("A", "B", "B"), ("A", "B", "C")]


def test_a_pattern_of_walks_and_a_bound_relationship_takes_no_relationship_twice(monkeypatch):
    # TCK Match4 [7]: relationship uniqueness across the walks and the hop of one pattern.
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (n0:Node), (n1:Node), (n2:Node), (n3:Node), "
        "(n0)-[:EDGE]->(n1), (n1)-[:EDGE]->(n2), (n2)-[:EDGE]->(n3)"
    )
    rows = s.run(
        "MATCH ()-[r:EDGE]-() MATCH p = (n)-[*0..1]-()-[r]-()-[*0..1]-(m) RETURN count(p) AS c"
    )
    assert rows == [{"c": 32}]


def test_the_path_of_a_pattern_of_walks_holds_every_hop(monkeypatch):
    # TCK Match6 [17]
    s = Session(monkeypatch, {})
    s.run("CREATE (a:A {name: 'A'})-[:KNOWS]->(b:B {name: 'B'})-[:FRIEND]->(c:C {name: 'C'})")
    rows = s.run(
        "MATCH p = (a {name: 'A'})-[:KNOWS*0..1]->(b)-[:FRIEND*0..1]->(c) RETURN p, length(p) AS l"
    )
    got = sorted(([n["properties"]["name"] for n in r["p"]["nodes"]], r["l"]) for r in rows)
    assert got == [(["A"], 0), (["A", "B"], 1), (["A", "B", "C"], 2)]
