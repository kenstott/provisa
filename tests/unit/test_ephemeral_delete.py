# Copyright (c) 2026 Kenneth Stott
# Canary: 8aebb559-ec79-45ff-8601-215e159e2337
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DELETE and DETACH DELETE over the session's graph (REQ-2176).

A session node is removed from the session, DETACH removing the relationships it has; DELETE
of one that still has relationships is refused, as Neo4j refuses it. A session relationship is
its own row: DELETE r removes the one matched, never a parallel one. A model node's row is the
governed path's: a plain DELETE of one is the governed delete, its session tags going with it,
and one with session relationships is refused whole, nothing written; a DETACH DELETE of one is
the governed DETACH DELETE of its row, its session tags and relationships going with it. Every
refusal is decided before any write."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest

from provisa.cypher.parser import CypherParseError
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_PEOPLE = {
    "Person": ({"id": "integer", "name": "text", "age": "integer"}, "(1, 'Ann', 30), (2, 'Bo', 40)")
}


def _values(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


def _people(s: Session) -> list[tuple]:
    return s.duck.execute("SELECT id, name FROM s.person ORDER BY id").fetchall()


def test_a_session_node_is_deleted(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:T {k: 1}), (:T {k: 2})")
    s.run("MATCH (n:T) WHERE n.k = 1 DELETE n")
    assert _values(s.run("MATCH (n:T) RETURN n.k")) == [(2,)]


def test_a_session_node_with_relationships_is_deleted_with_detach_only(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (a:T {k: 1})-[:R]->(b:T {k: 2})")
    with pytest.raises(CypherParseError, match="still has relationships"):
        s.run("MATCH (n:T) WHERE n.k = 1 DELETE n")
    assert len(s.run("MATCH (a:T)-[r:R]->(b:T) RETURN r")) == 1
    s.run("MATCH (n:T) WHERE n.k = 1 DETACH DELETE n")
    assert _values(s.run("MATCH (n:T) RETURN n.k")) == [(2,)]
    assert s.run("MATCH (a:T)-[r:R]->(b:T) RETURN r") == []


def test_delete_r_removes_only_the_relationship_matched(monkeypatch):
    # Two parallel R's between the same ends are two edges, each its own row.
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (a:A)-[:R {w: 1}]->(b:B), (a)-[:R {w: 2}]->(b)")
    s.run("MATCH (:A)-[r:R]->(:B) WHERE r.w = 1 DELETE r")
    assert _values(s.run("MATCH (:A)-[r:R]->(:B) RETURN r.w")) == [(2,)]


def test_parallel_session_relationships_are_two_edges(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (a:A)-[:R {w: 1}]->(b:B), (a)-[:R {w: 2}]->(b)")
    rows = s.run("MATCH (:A)-[r:R]->(:B) RETURN r")
    assert len({r["r"]["identity"] for r in rows}) == 2


def test_a_tagged_model_node_is_the_governed_delete_and_its_tags_go_with_it(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("MATCH (p:Person) WHERE p.name = 'Ann' SET p:Vip")
    s.run("MATCH (p:Person) WHERE p.name = 'Ann' DELETE p")
    assert _people(s) == [(2, "Bo")]
    assert s.run("MATCH (n:Vip) RETURN n.name") == []


def test_a_model_node_with_session_relationships_is_refused_whole(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("MATCH (p:Person) WHERE p.name = 'Ann' CREATE (p)-[:OWNS]->(:Robot {m: 'R2'})")
    with pytest.raises(CypherParseError, match="still has relationships"):
        s.run("MATCH (p:Person) WHERE p.name = 'Ann' DELETE p")
    assert _people(s) == [(1, "Ann"), (2, "Bo")]
    assert len(s.run("MATCH (:Person)-[o:OWNS]->(:Robot) RETURN o")) == 1


def test_detach_delete_of_a_model_node_takes_its_session_tags_and_relationships(monkeypatch):
    # REQ-2176: DETACH DELETE of a model node is governed when every part has an exact target;
    # Person has no relationship in the model, so its row is the one, and its session tags and
    # relationships go with it.
    s = Session(monkeypatch, _PEOPLE)
    s.run("MATCH (p:Person) WHERE p.name = 'Ann' SET p:Vip")
    s.run("MATCH (p:Person) WHERE p.name = 'Ann' CREATE (p)-[:OWNS]->(:Robot {m: 'R2'})")
    s.run("MATCH (p:Person) WHERE p.name = 'Ann' DETACH DELETE p")
    assert _people(s) == [(2, "Bo")]
    assert s.run("MATCH (n:Vip) RETURN n.name") == []
    assert s.run("MATCH (:Person)-[o:OWNS]->(:Robot) RETURN o") == []
    assert _values(s.run("MATCH (r:Robot) RETURN r.m")) == [("R2",)]


def test_a_refusal_writes_nothing_of_the_statement(monkeypatch):
    # The session node would go, but the model node beside it is refused: neither goes.
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:T {k: 1})")
    s.run("MATCH (p:Person) WHERE p.name = 'Ann' CREATE (p)-[:OWNS]->(:Robot)")
    with pytest.raises(CypherParseError):
        s.run("MATCH (t:T), (p:Person) WHERE p.name = 'Ann' DELETE t, p")
    assert _values(s.run("MATCH (n:T) RETURN n.k")) == [(1,)]
    assert _people(s) == [(1, "Ann"), (2, "Bo")]


def test_with_nothing_in_the_session_a_delete_is_the_governed_paths(monkeypatch):
    import asyncio

    from provisa.ephemeral import seam
    from provisa.ephemeral.delete import delete_of

    s = Session(monkeypatch, _PEOPLE)
    statement = "MATCH (p:Person) WHERE p.name = 'Bo' DELETE p"
    made = asyncio.run(seam.write(statement, {}, s.model, s.session, s.state, s.read, s.write))
    assert made is None
    assert delete_of("MATCH (n) RETURN n") is None


def test_detach_delete_of_a_path_removes_its_nodes_and_relationships(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (x:X), (n1), (n2) CREATE (x)-[:R]->(n1) CREATE (n1)-[:R]->(n2)")
    s.run("MATCH p = (:X)-->()-->() DETACH DELETE p")
    assert s.run("MATCH (n) RETURN n") == []


def test_a_node_from_a_list_is_deleted(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (u:User) CREATE (u)-[:FRIEND]->(:F {k: 1}) CREATE (u)-[:FRIEND]->(:F {k: 2})")
    s.run(
        "MATCH (:User)-[:FRIEND]->(n) WITH n ORDER BY n.k WITH collect(n) AS fs DETACH DELETE fs[$i]",
        i=1,
    )
    assert _values(s.run("MATCH (n:F) RETURN n.k")) == [(1,)]


def test_a_node_a_map_holds_is_deleted(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:User), (:User)")
    s.run("MATCH (u:User) WITH {key: u} AS nodes DELETE nodes.key")
    assert s.run("MATCH (n) RETURN n") == []


def _seam(s: Session, statement: str):
    import asyncio

    from provisa.ephemeral import seam

    return asyncio.run(seam.write(statement, {}, s.model, s.session, s.state, s.read, s.write))


def test_deleting_null_deletes_nothing_even_in_an_empty_session(monkeypatch):
    s = Session(monkeypatch, {})
    # A form the governed path takes is its, unchanged, while the session holds nothing.
    assert _seam(s, "OPTIONAL MATCH (n) DELETE n") is None
    # One it does not take (a read after the DELETE) is the session's: null deletes nothing.
    made = s.run("OPTIONAL MATCH (n) DETACH DELETE n RETURN n")
    assert (made.rows, made.counters) == ([{"n": None}], {"nodes": 0, "relationships": 0})
    s.run("CREATE (:T)")
    s.run("OPTIONAL MATCH (n:T) WHERE n.k = 1 DELETE n")
    assert len(s.run("MATCH (n:T) RETURN n")) == 1


def test_a_delete_followed_by_a_read_sees_its_rows(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:R]->()")
    made = s.run("MATCH (a)-[r]-(b) DELETE r, a, b RETURN count(*) AS c")
    assert made.rows == [{"c": 2}]
    assert s.run("MATCH (n) RETURN n") == []
    # The type written before is still one to read, holding nothing.
    s.run("CREATE (:A)")
    assert s.run("MATCH (a:A) RETURN a.k") == [{"a.k": None}]


@pytest.mark.parametrize("deleted", ["n:Person", "1 + 1", "'x'"])
def test_a_value_that_is_never_a_node_is_refused_before_anything_is_read(monkeypatch, deleted):
    s = Session(monkeypatch, {})
    with pytest.raises(CypherParseError, match="a DELETE removes a node, a relationship"):
        s.run(f"MATCH (n) DELETE {deleted}")


def test_a_value_read_that_is_no_node_is_refused_by_name(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:T {k: 1})")
    with pytest.raises(CypherParseError, match=r"DELETE n\.k: 1 is not a node"):
        s.run("MATCH (n:T) DELETE n.k")


def test_a_relationship_from_a_collected_list_is_deleted(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (u:User) CREATE (u)-[:FRIEND]->() CREATE (u)-[:FRIEND]->()")
    s.run("MATCH (:User)-[r:FRIEND]->() WITH collect(r) AS fs DETACH DELETE fs[$i]", i=1)
    assert len(s.run("MATCH (:User)-[r:FRIEND]->() RETURN r")) == 1
    assert len(s.run("MATCH (n) RETURN n")) == 3


def test_a_relationship_a_nested_map_holds_is_deleted(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a:User), (b:User) CREATE (a)-[:R]->(b) CREATE (b)-[:R]->(a)")
    s.run("MATCH (:User)-[r]->(:User) WITH {key: {key: collect(r)}} AS rels DELETE rels.key.key[0]")
    assert len(s.run("MATCH (:User)-[r:R]->(:User) RETURN r")) == 1


def test_paths_a_map_holds_are_deleted_with_their_nodes_and_relationships(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a:User), (b:User) CREATE (a)-[:R]->(b) CREATE (b)-[:R]->(a)")
    s.run("MATCH p = (:User)-[r]->(:User) WITH {key: collect(p)} AS pc DELETE pc.key[0], pc.key[1]")
    assert s.run("MATCH (n) RETURN n") == []


@pytest.mark.parametrize(
    "rest", ["RETURN n.num", "RETURN labels(n)", "WITH n RETURN n['num']", "RETURN keys(n)"]
)
def test_reading_what_a_statement_deleted_is_refused_and_nothing_is_written(monkeypatch, rest):
    s = Session(monkeypatch, {})
    s.run("CREATE ({num: 0})")
    with pytest.raises(CypherParseError, match="DeletedEntityAccess"):
        s.run(f"MATCH (n) DELETE n {rest}")
    assert len(s.run("MATCH (n) RETURN n")) == 1


def test_a_deleted_variable_null_before_the_delete_is_read_as_null(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:T)")
    made = s.run("OPTIONAL MATCH (n:T) WHERE n.k = 1 DELETE n RETURN n.k AS k")
    assert made.rows == [{"k": None}]


def test_the_type_of_a_deleted_relationship_is_read_after_the_delete(monkeypatch):
    # TCK Return2 [14]: a relationship's type is still there after the DELETE (its properties
    # are not, DeletedEntityAccess); it was read as null.
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:T]->()")
    made = s.run("MATCH ()-[r]->() DELETE r RETURN type(r)")
    assert made.rows == [{"type(r)": "T"}]
    assert s.run("MATCH ()-[r]->() RETURN count(r) AS n") == [{"n": 0}]
