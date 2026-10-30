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

# A read whose nodes' labels a test checks is read as the client is answered (Session.answer).

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
    rows = s.answer("MATCH (a:A:B) RETURN a")
    assert sorted(r["a"]["labels"] for r in rows) == [["A", "B"], ["A", "B", "C"]]


def test_an_optional_end_is_returned_with_every_label(monkeypatch):
    # TCK Match7 [23]
    s = Session(monkeypatch, {})
    s.run("CREATE (:X), (x:X), (y1:Y), (y2:Y:Z) CREATE (x)-[:REL]->(y1), (x)-[:REL]->(y2)")
    rows = s.answer("MATCH (a:X) OPTIONAL MATCH (a)-->(b:Y) RETURN b")
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
    [path] = [r["p"] for r in s.answer("MATCH p = (b)<--(a) RETURN p")]
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


# --- an embedded statement's own CTEs (V006) -----------------------------------------------------


def test_a_cte_of_an_embedded_statement_names_a_relation_within_it_only():
    # TCK ExistentialSubquery2 [2], Pattern1 [10]: an EXISTS { } reading a WITH, or a walk, holds
    # its own WITH inside the outer statement; its CTE is a relation there, nowhere else.
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.compiler.cte_utils import names_a_cte_in_scope

    tree = sqlglot.parse_one(
        "SELECT * FROM t WHERE EXISTS (WITH _w0 AS (SELECT 1 AS x) SELECT x FROM _w0) "
        "AND EXISTS (SELECT 1 FROM _w0)",
        read="postgres",
    )
    inside, outside = [t for t in tree.find_all(exp.Table) if t.name == "_w0"]
    assert names_a_cte_in_scope(inside)
    assert not names_a_cte_in_scope(outside)
    assert not names_a_cte_in_scope(next(t for t in tree.find_all(exp.Table) if t.name == "t"))


# --- a pattern predicate introduces no variable ---------------------------------------------------


@pytest.mark.parametrize("pattern", ["()-[r]->()", "()<-[r]-()", "()-[r]-()", "(n)-[r]->(a)"])
def test_a_pattern_predicate_naming_a_new_variable_is_refused(monkeypatch, pattern):
    # TCK Pattern1 [10]: UndefinedVariable.
    from provisa.gql.translator_types import CypherTranslateError

    s = Session(monkeypatch, {})
    s.run("CREATE (:X)-[:T]->()")
    with pytest.raises(CypherTranslateError, match="introduces no variable"):
        s.run(f"MATCH (n) WHERE {pattern} RETURN n")


def test_a_pattern_predicate_of_bound_variables_is_read(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:X)-[:T]->(), ()")
    assert len(s.run("MATCH (n) WHERE (n)-->() RETURN n")) == 1
    assert len(s.run("MATCH (n)-[r]->(m) WHERE (n)-[r]->(m) RETURN n")) == 1


# --- OPTIONAL MATCH after a WITH; nested statements; comprehensions -------------------------------


def test_an_optional_match_from_carried_rows_keeps_each(monkeypatch):
    # TCK Match7 [10], Match7 [4], MatchWhere6 [5]
    s = Session(monkeypatch, {})
    assert s.run("OPTIONAL MATCH (a) WITH a OPTIONAL MATCH (a)-->(b) RETURN b") == [{"b": None}]
    s.run("CREATE (:A)-[:T]->(:B)")
    rows = s.answer(
        "MATCH (a1)-[r]->() WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2"
    )
    assert [(r["a1"]["labels"], r["r"]["type"], r["b2"]) for r in rows] == [(["A"], "T", None)]


def test_a_held_node_a_later_pattern_labels_has_that_label(monkeypatch):
    # TCK Match3 [25]
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:T]->()")
    assert s.run("MATCH (a1)-[r]->() WITH r, a1 MATCH (a1:X)-[r]->(b2) RETURN a1, r, b2") == []


def test_nested_existential_subqueries_keep_the_outer_node(monkeypatch):
    # TCK ExistentialSubquery3 [1]
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (a:A {prop: 1})-[:R]->(b:B {prop: 1}), (a)-[:R]->(:C {prop: 2}), "
        "(a)-[:R]->(:D {prop: 3})"
    )
    rows = s.answer(
        "MATCH (n) WHERE exists { MATCH (m) WHERE exists { (n)-[]->(m) WHERE n.prop = m.prop } "
        "RETURN true } RETURN n"
    )
    assert [r["n"]["labels"] for r in rows] == [["A"]]


def test_a_pattern_comprehension_of_each_node_of_a_path(monkeypatch):
    # TCK Pattern2 [7]
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (n1:X {n: 1}), (m1:Y), (i1:Y), (i2:Y) "
        "CREATE (n1)-[:T]->(m1), (m1)-[:T]->(i1), (m1)-[:T]->(i2)"
    )
    rows = s.run("MATCH p = (n:X)-->() RETURN [x IN nodes(p) | size([(x)-->(:Y) | 1])] AS list")
    assert rows == [{"list": [1, 2]}]


def test_a_pattern_comprehension_carried_by_a_with_is_its_paths(monkeypatch):
    # TCK Pattern2 [8]
    s = Session(monkeypatch, {})
    s.run("CREATE (a:A), (b:B) CREATE (a)-[:T]->(b), (b)-[:T]->(:C)")
    rows = s.run("MATCH (n)-->(b) WITH [p = (n)-->() | p] AS ps, count(b) AS c RETURN ps, c")
    got = sorted([[n["labels"] for n in p["nodes"]] for p in r["ps"]] for r in rows)
    assert got == [[[["A"], ["B"]]], [[["B"], ["C"]]]]


# --- a session hop's ends are the session's own and those its relationships end at ---------------


def test_a_session_hop_reads_no_model_label_its_relationships_do_not_end_at(monkeypatch):
    # TCK Create2 [14], Create6 [8]-[14]: the read after CREATE ()-[r:R]->() finds r again over
    # the session's relationships; a model label no session relationship ends at is no end of
    # the hop -- read on both ends, governance refuses it as two model tables joined by nothing
    # it knows (V002).
    s = Session(
        monkeypatch, {"Traces": ({"id": "bigint"}, "(1)"), "Lineage": ({"id": "bigint"}, "(2)")}
    )
    s.run("CREATE ()-[:R {num: 42}]->()")
    sql = s.sql("MATCH ()-[r:R]->() RETURN r.num AS num")
    assert '"traces"' not in sql and '"lineage"' not in sql
    assert s.run("MATCH ()-[r:R]->() RETURN r.num AS num") == [{"num": 42}]


def test_a_session_hop_reads_a_model_label_a_session_relationship_ends_at(monkeypatch):
    s = Session(
        monkeypatch, {"Traces": ({"id": "bigint"}, "(1)"), "Lineage": ({"id": "bigint"}, "(2)")}
    )
    s.run("MATCH (t:Traces) CREATE (t)-[:R {num: 7}]->()")
    sql = s.sql("MATCH (a)-[r:R]->() RETURN a.id AS id, r.num AS num")
    assert '"lineage"' not in sql
    assert s.run("MATCH (a)-[r:R]->() RETURN a.id AS id, r.num AS num") == [{"id": 1, "num": 7}]


def test_a_pattern_of_undirected_session_hops_reads_no_model_label(monkeypatch):
    # TCK WithWhere2 [1]: five undirected hops over the session's relationships; each end read as
    # a union of every model label the role reads made the read outrun its request timeout.
    s = Session(
        monkeypatch, {"Traces": ({"id": "bigint"}, "(1)"), "Lineage": ({"id": "bigint"}, "(2)")}
    )
    s.run(
        "CREATE (a:A), (b:B {id: 1}), (c:C {id: 2}), (d:D) "
        "CREATE (a)-[:T]->(b), (a)-[:T]->(c), (a)-[:T]->(d), (b)-[:T]->(c), (b)-[:T]->(d), "
        "(c)-[:T]->(d)"
    )
    query = (
        "MATCH (a)--(b)--(c)--(d)--(a), (b)--(d) WITH a, c, d WHERE a.id = 1 AND c.id = 2 RETURN d"
    )
    sql = s.sql(query)
    assert '"traces"' not in sql and '"lineage"' not in sql
    assert sorted(r["d"]["labels"] for r in s.answer(query)) == [["A"], ["D"]]


# --- types and labels told apart by case -----------------------------------------------------------


def test_relationship_types_differing_only_in_case_are_two_types(monkeypatch):
    # TCK Graph5 [2]: T2 and t2 are two types; an engine that folds names held them in one table
    # and named their CTEs alike (Duplicate CTE name "__session_rel__T2").
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:T2 {n: 1}]->(), ()-[:t2 {n: 2}]->()")
    rows = s.run("MATCH ()-[r]->() RETURN type(r) AS t, r.n AS n")
    assert _sorted(rows) == [("T2", 1), ("t2", 2)]


def test_a_relationships_label_is_its_type(monkeypatch):
    # TCK Graph5 [2]: r:T2 holds of a relationship of type T2 alone.
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:T1]->(), ()-[:T2]->(), ()-[:t2]->(), (:T2)-[:T3]->(), ()-[:T4]->(:T2)")
    rows = s.run("MATCH ()-[r]->() RETURN type(r) AS t, r:T2 AS result")
    assert _sorted(rows) == [
        ("T1", False),
        ("T2", True),
        ("T3", False),
        ("T4", False),
        ("t2", False),
    ]


# --- nodes carried in a list ----------------------------------------------------------------------


def test_a_with_aggregating_beside_two_nodes_groups_by_their_columns(monkeypatch):
    # TCK Match4 [4]: WITH a, b, collect(n) carries a and b as their own columns; it grouped by
    # a.*, b.* ("STAR expression is not supported here").
    s = Session(monkeypatch, {})
    s.run("CREATE (a {var: 'start'}), (b {var: 'end'}), ({num: 1}), ({num: 2})")
    rows = s.run(
        "MATCH (a {var: 'start'}), (b {var: 'end'}), (n) WHERE n.num IN [1, 2] "
        "WITH a, b, count(n) AS c RETURN a.var AS a, b.var AS b, c"
    )
    assert _sorted(rows) == [("start", "end", 2)]


def test_an_item_of_a_list_of_nodes_is_a_node(monkeypatch):
    # TCK Match4 [4]: [a] + collect(n) + [b] is a list of nodes, nodeList[i] one of them; each
    # came back as its JSON text, and a CREATE between two of them was refused.
    s = Session(monkeypatch, {})
    made = s.run(
        "CREATE (a {var: 'start'}), (b {var: 'end'}) WITH * UNWIND range(1, 3) AS i "
        "CREATE (n {var: i}) WITH a, b, [a] + collect(n) + [b] AS nodeList "
        "UNWIND range(0, size(nodeList) - 2, 1) AS i "
        "WITH nodeList[i] AS n1, nodeList[i+1] AS n2 CREATE (n1)-[:T]->(n2)"
    )
    assert made.counters == {"nodes": 5, "relationships": 4, "labels": 0}
    rows = s.answer("MATCH (n {var: 'start'})-[:T*..10]->(m {var: 'end'}) RETURN m")
    assert [r["m"]["properties"] for r in rows] == [{"var": "end"}]
    listed = s.answer("MATCH (a {var: 'start'}) WITH [a] AS l RETURN l[0] AS x, l")
    assert listed[0]["x"]["properties"] == {"var": "start"}
    assert [n["properties"] for n in listed[0]["l"]] == [{"var": "start"}]


def test_a_later_match_of_a_node_alone_is_the_node_already_matched(monkeypatch):
    # MATCH (a) ... MATCH (a) / MATCH p = (a): the second read the node's table again under the
    # same name ("Ambiguous reference to table a"), as the read after MERGE p = (a) did.
    s = Session(monkeypatch, {})
    s.run("CREATE ({num: 1}), (:L {num: 2})")
    assert s.run("MATCH (a {num: 1}) MATCH (a) RETURN a.num AS n") == [{"n": 1}]
    rows = s.answer("MATCH (a:L) MATCH p = (a) RETURN p")
    assert [n["properties"] for n in rows[0]["p"]["nodes"]] == [{"num": 2}]


# --- a relationship between labelled nodes matched again -----------------------------------------


def test_a_relationship_between_labelled_nodes_is_carried_by_a_with(monkeypatch):
    # WITH a, r of r:REL between labelled nodes selected a column r no table has.
    s = Session(monkeypatch, {})
    s.run("CREATE (:A)-[:REL {w: 1}]->(:B)")
    rows = s.run("MATCH (a:A)-[r:REL]->(b:B) WITH a AS x, r RETURN type(r) AS t, r.w AS w")
    assert rows == [{"t": "REL", "w": 1}]


def test_a_relationship_bound_between_labelled_nodes_is_matched_again(monkeypatch):
    # TCK With7 [1]: r bound between labelled nodes, carried through WITHs that rename its ends,
    # is matched again from one of them; it was refused.
    s = Session(monkeypatch, {})
    s.run("CREATE (:A)-[:REL]->(:B)")
    rows = s.answer(
        "MATCH (a:A)-[r:REL]->(b:B) WITH a AS b, b AS tmp, r AS r WITH b AS a, r LIMIT 1 "
        "MATCH (a)-[r]->(b) RETURN a, r, b"
    )
    assert [(r["a"]["labels"], r["r"]["type"], r["b"]["labels"]) for r in rows] == [
        (["A"], "REL", ["B"])
    ]


def test_a_relationship_bound_between_labelled_nodes_is_matched_again_in_its_stage(monkeypatch):
    # The same relationship matched again with no WITH between: each edge once, never another.
    s = Session(monkeypatch, {})
    s.run("CREATE (:A)-[:REL]->(:B), (:A)-[:REL]->(:B)")
    rows = s.run("MATCH (a:A)-[r:REL]->(b:B) MATCH (x)-[r]->(c) RETURN count(*) AS n")
    assert rows == [{"n": 2}]


def test_a_relationship_type_matches_its_own_case_alone(monkeypatch):
    # openCypher: types are case-sensitive; [r:T2] also matched t2 (aliases keyed by upper case).
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:T2 {n: 1}]->(), ()-[:t2 {n: 2}]->()")
    assert s.run("MATCH ()-[r:T2]->() RETURN r.n AS n") == [{"n": 1}]
    assert s.run("MATCH ()-[r:t2]->() RETURN r.n AS n") == [{"n": 2}]
    assert _sorted(s.run("MATCH ()-[r:T2|t2]->() RETURN r.n AS n")) == [(1,), (2,)]
    assert s.run("MATCH ()-[:t2*1..2]->(m) RETURN count(m) AS c") == [{"c": 1}]


def test_labels_differing_only_in_case_are_two_labels(monkeypatch):
    # openCypher: labels are case-sensitive; A and a folded together (canonical_label), and their
    # anchor tables were one name to DuckDB, the second CREATE's nodes replacing the first's.
    s = Session(monkeypatch, {})
    s.run("CREATE (:A {n: 1}), (:a {n: 2})")
    assert s.run("MATCH (x:A) RETURN x.n AS n") == [{"n": 1}]
    assert s.run("MATCH (x:a) RETURN x.n AS n") == [{"n": 2}]
    assert s.run("MATCH (x) WHERE x:A RETURN x.n AS n") == [{"n": 1}]
    assert s.run("MATCH (x:A:a) RETURN x.n AS n") == []


def test_a_model_label_is_found_by_its_declared_name_alone(monkeypatch):
    from provisa.gql.translator_types import CypherMistake

    s = Session(monkeypatch, {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann')")})
    assert s.run("MATCH (p:Person) RETURN p.name AS n") == [{"n": "Ann"}]
    with pytest.raises(
        CypherMistake, match="There is no label :person. Registered labels: :Person"
    ):
        s.run("MATCH (p:person) RETURN p.name AS n")
