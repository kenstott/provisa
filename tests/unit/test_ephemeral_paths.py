# Copyright (c) 2026 Kenneth Stott
# Canary: 9c41f7a2-6e0b-4d8f-b315-2a7e9d06c4b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A path function with an unlabelled end in a graph session (REQ-348): it walks the session's
one relationships table, as one between labelled ends does; on the model graph it is refused by
name. Several are openCypher TCK scenarios (Match4, Match6, Path2), run on DuckDB through the
session's own path; a scenario's unbounded ``[*]`` is written with an upper bound, which a
statement here must give (REQ-348)."""

# Requirements: REQ-348, REQ-751, REQ-753, REQ-767

from __future__ import annotations

import pytest

from provisa.cypher.parser import parse_cypher
from provisa.ephemeral import seam
from provisa.gql.label_map import RelationshipMapping
from provisa.gql.path_walk import UNLABELLED_ENDS
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CypherTranslateError
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_PEOPLE = {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Bo')")}


def _sorted(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


# --- openCypher TCK scenarios --------------------------------------------------------------------


def test_tck_match4_simple_variable_length_pattern(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}), (d {name: 'D'}) "
        "CREATE (a)-[:CONTAINS]->(b), (b)-[:CONTAINS]->(c), (c)-[:CONTAINS]->(d)"
    )
    rows = s.run("MATCH (a {name: 'A'})-[*..3]->(x) RETURN x.name")
    assert _sorted(rows) == [("B",), ("C",), ("D",)]


def test_tck_match4_variable_length_with_a_type_and_bounds(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (a {name: 'A'})-[:CONTAINS]->(b {name: 'B'})-[:CONTAINS]->(c {name: 'C'})"
        "-[:FRIEND]->(d {name: 'D'})"
    )
    assert _sorted(s.run("MATCH (a {name: 'A'})-[:CONTAINS*2..3]->(x) RETURN x.name")) == [("C",)]
    assert _sorted(s.run("MATCH (a {name: 'A'})-[:CONTAINS|FRIEND*2..3]->(x) RETURN x.name")) == [
        ("C",),
        ("D",),
    ]


def test_tck_path2_a_variable_length_path_of_length_zero(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a:A {name: 'a'})-[:REL]->(b:B {name: 'b'})")
    rows = s.run("MATCH p = (a)-[*0..1]->(b) RETURN a.name, b.name, length(p) AS l")
    assert _sorted(rows) == [("a", "a", 0), ("a", "b", 1), ("b", "b", 0)]


def test_tck_match6_a_named_variable_length_path(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (a:A {name: 'A'})-[:KNOWS {num: 1}]->(b:B {name: 'B'})-[:KNOWS]->(c:C {name: 'C'})"
    )
    rows = s.run("MATCH p = (n {name: 'A'})-[:KNOWS*1..2]->(x) RETURN p, length(p) AS l")
    # REQ-751: the path is every node the walk passed, not its two ends alone; each a session
    # node, answered by its id within the session (seam.answered, REQ-2176).
    nodes = sorted(([n["id"] for n in r["p"]["nodes"]], r["l"]) for r in rows)
    assert nodes == [(["e:1", "e:2"], 1), (["e:1", "e:2", "e:3"], 2)]
    assert [len(r["p"]["edges"]) for r in sorted(rows, key=lambda r: r["l"])] == [1, 2]


def test_tck_match4_an_undirected_walk_takes_no_relationship_twice(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a {n: 1})-[:R]->(b {n: 2})")
    assert _sorted(s.run("MATCH (a {n: 1})-[*1..3]-(x) RETURN x.n")) == [(2,)]


def test_tck_match4_a_self_loop_is_walked_once_either_way(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (x {k: 1})-[:LOOP]->(x)")
    assert _sorted(s.run("MATCH (n)-[*1..2]-(m) RETURN n.k, m.k")) == [(1, 1)]


# --- direction, bounds and shortest paths ---------------------------------------------------------


def test_a_walk_written_leftward_leaves_the_node_the_arrow_leaves(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE ({n: 'a'})-[:T]->({n: 'b'})-[:T]->({n: 'c'})")
    assert _sorted(s.run("MATCH (x)<-[*2]-(y) RETURN x.n, y.n")) == [("c", "a")]


def test_the_upper_bound_ends_the_walk(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE ({n: 1})-[:T]->({n: 2})-[:T]->({n: 3})-[:T]->({n: 4})")
    assert _sorted(s.run("MATCH (a {n: 1})-[*..2]->(x) RETURN x.n")) == [(2,), (3,)]


def test_shortest_path_between_unlabelled_ends(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (a {n: 'a'}), (b {n: 'b'}), (c {n: 'c'}), (d {n: 'd'}) "
        "CREATE (a)-[:T]->(b), (b)-[:T]->(c), (c)-[:T]->(d), (a)-[:T]->(d)"
    )
    rows = s.run("MATCH p = shortestPath((x {n: 'a'})-[*..5]->(y {n: 'd'})) RETURN length(p) AS l")
    assert rows == [{"l": 1}]


def test_all_shortest_paths_are_every_walk_of_the_fewest_hops(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (a {n: 'a'}), (b {n: 'b'}), (c {n: 'c'}), (e {n: 'e'}), (d {n: 'd'}) "
        "CREATE (a)-[:T]->(b), (b)-[:T]->(d), (a)-[:T]->(c), (c)-[:T]->(d), "
        "(a)-[:T]->(e), (e)-[:T]->(c)"
    )
    rows = s.run(
        "MATCH (x {n: 'a'}), (y {n: 'd'}) "
        "MATCH p = allShortestPaths((x)-[*..5]->(y)) RETURN length(p) AS l"
    )
    assert rows == [{"l": 2}, {"l": 2}]


def test_a_walk_reaches_a_model_node_by_a_session_relationship(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run(
        "MATCH (p:Person) WHERE p.name = 'Ann' "
        "CREATE (p)-[:KNOWS]->(:Robot {m: 'R2'})-[:KNOWS]->(:Robot {m: 'C3'})"
    )
    rows = s.run(
        "MATCH (a:Person {name: 'Ann'}), (c {m: 'C3'}) "
        "MATCH p = shortestPath((a)-[*..5]-(c)) RETURN length(p) AS l, p"
    )
    assert [r["l"] for r in rows] == [2]
    # A model node as the model answers it; a session node by its id within the session.
    assert [n["id"] for n in rows[0]["p"]["nodes"]] == ["Person|1", "e:1", "e:2"]


def test_a_walk_reads_the_relationships_table_once(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE ({n: 1})-[:T]->({n: 2})-[:U]->({n: 3})")
    sql = s.sql("MATCH p = shortestPath((a {n: 1})-[*..3]->(b)) RETURN length(p)")
    assert sql.count("FROM cypher__rels") == 1
    assert "cypher__rel__T" not in sql and "cypher__rel__U" not in sql


# --- refused --------------------------------------------------------------------------------------


def test_the_model_graph_refuses_a_path_with_both_ends_free_by_name(monkeypatch):
    # REQ-348 (ONE END BOUND OR BINDABLE): one labelled end is enough; neither is refused. The
    # model graph is a read outside any graph session (an empty session walks its own, REQ-2176).
    s = Session(monkeypatch, _PEOPLE)
    query = "MATCH p = shortestPath((a)-[*..3]->(b)) RETURN p"
    on_model = seam.label_map(s.model, None)
    assert on_model is s.model
    with pytest.raises(CypherTranslateError, match=UNLABELLED_ENDS):
        cypher_to_sql(parse_cypher(query, on_model), on_model, {})


def test_a_walk_the_models_relationships_could_take_is_refused_by_name(monkeypatch):
    knows = RelationshipMapping("KNOWS", "Person", "Person", "id", "id", "knows")
    s = Session(monkeypatch, _PEOPLE, relationships=[knows])
    s.run("CREATE (:Robot {m: 'R2'})-[:KNOWS]->(:Robot {m: 'C3'})")
    with pytest.raises(CypherTranslateError, match=UNLABELLED_ENDS):
        s.run("MATCH (a)-[:KNOWS*1..2]->(b) RETURN b.m")


def test_a_walk_from_a_labelled_model_end_takes_the_models_relationships(monkeypatch):
    # REQ-348 (ONE END BOUND OR BINDABLE): one labelled end places the walk on the model's
    # relationships, session or not; Ann knows Bo, Bo knows no one.
    people = {
        "Person": (
            {"id": "integer", "name": "text", "knows_id": "integer"},
            "(1, 'Ann', 2), (2, 'Bo', NULL)",
        )
    }
    knows = RelationshipMapping("KNOWS", "Person", "Person", "knows_id", "id", "knows")
    s = Session(monkeypatch, people, relationships=[knows])
    s.run("CREATE (:Robot {m: 'R2'})-[:KNOWS]->(:Robot {m: 'C3'})")
    rows = s.run("MATCH (a:Person {name: 'Ann'})-[:KNOWS*1..2]->(b) RETURN b.name")
    assert _sorted(rows) == [("Bo",)]
