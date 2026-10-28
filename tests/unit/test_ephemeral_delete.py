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
