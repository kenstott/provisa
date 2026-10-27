# Copyright (c) 2026 Kenneth Stott
# Canary: 173ea0f8-2236-491a-964c-4acec8d301b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The read overlay (REQ-2176): a label with session matches reads the session's part beside the
model's own rows; a relationship the session made joins its ends wherever they are."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_PEOPLE = {
    "Person": ({"id": "integer", "name": "text", "age": "integer"}, "(1, 'Ann', 30), (2, 'Bo', 40)")
}


def _values(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


def test_an_ephemeral_label_is_read_from_the_session(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:Temp {k: 1}), (:Temp {k: 2.5, w: 'x'})")
    assert _values(s.run("MATCH (n:Temp) RETURN n.k, n.w")) == [(1.0, None), (2.5, "x")]


def test_a_model_label_reads_its_rows_beside_the_sessions(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:Person:Temp {name: 'Cy', mood: 'ok'})")
    assert _values(s.run("MATCH (n:Person) RETURN n.name, n.age, n.mood")) == [
        ("Ann", 30, None),
        ("Bo", 40, None),
        ("Cy", None, "ok"),
    ]
    assert _values(s.run("MATCH (n:Temp) RETURN n.name")) == [("Cy",)]


def test_a_relationship_joins_a_model_node_to_a_session_node(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("MATCH (p:Person) WHERE p.name = 'Ann' CREATE (p)-[:OWNS {since: 1}]->(:Robot {m: 'R2'})")
    rows = s.run("MATCH (p:Person)-[o:OWNS]->(r:Robot) RETURN p.name, o.since, r.m")
    assert _values(rows) == [("Ann", 1, "R2")]


def test_relationships_between_session_nodes(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (a:City {n: 'X'}), (b:City {n: 'Y'}), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a)")
    rows = s.run("MATCH (a:City)-[:ROAD]->(b:City) RETURN a.n, b.n")
    assert _values(rows) == [("X", "Y"), ("Y", "X")]


def test_a_model_node_keeps_its_identity_in_an_answer(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    before = s.run("MATCH (n:Person) WHERE n.name = 'Ann' RETURN n")
    s.run("CREATE (:Person:Temp {name: 'Cy'})")
    after = s.run("MATCH (n:Person) WHERE n.name = 'Ann' RETURN n")
    assert after[0]["n"]["id"] == before[0]["n"]["id"]


def test_a_label_without_session_matches_compiles_as_it_does_without_the_session(monkeypatch):
    s = Session(monkeypatch, {**_PEOPLE, "Pet": ({"id": "integer", "name": "text"}, "(1, 'Rex')")})
    before = s.sql("MATCH (p:Pet) WHERE p.name = 'Rex' RETURN p.name")
    s.run("CREATE (:Person:Temp {name: 'Cy'})")
    assert s.sql("MATCH (p:Pet) WHERE p.name = 'Rex' RETURN p.name") == before


def test_a_match_of_any_node_returns_each_node_once(monkeypatch):
    # Ann is tagged by no session label; Cy carries Person and Temp; Dee carries two free labels;
    # the last node carries none. Each is one row (REQ-2176).
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:Person:Temp {name: 'Cy'}), (:Temp:Scratch {name: 'Dee'}), ({name: 'Eve'})")
    rows = s.run("MATCH (n) RETURN n.name")
    assert _values(rows) == [("Ann",), ("Bo",), ("Cy",), ("Dee",), ("Eve",)]


def test_a_node_of_two_model_tables_is_one_node_of_any_match(monkeypatch):
    s = Session(monkeypatch, {**_PEOPLE, "Pet": ({"id": "integer", "name": "text"}, "(1, 'Rex')")})
    s.run("CREATE (:Person:Pet {name: 'Zed'})")
    assert _values(s.run("MATCH (n) WHERE n.name = 'Zed' RETURN n.name")) == [("Zed",)]
    assert _values(s.run("MATCH (n:Pet) RETURN n.name")) == [("Rex",), ("Zed",)]


def test_a_match_of_any_node_keeps_a_model_nodes_identity(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    before = s.run("MATCH (n) WHERE n.name = 'Ann' RETURN id(n) AS i")
    s.run("CREATE (:Temp {name: 'Cy'})")
    assert s.run("MATCH (n) WHERE n.name = 'Ann' RETURN id(n) AS i") == before


def test_a_mixed_create_lands_its_governed_row_and_its_session_parts(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (c:Person {id: 42, name: 'Cy'}), (n:Note {text: 'x'}), (c)-[:ABOUT]->(n)")
    assert s.duck.execute("SELECT id, name FROM s.person WHERE id = 42").fetchall() == [(42, "Cy")]
    assert _values(s.run("MATCH (n:Note) RETURN n.text")) == [("x",)]
    rows = s.run("MATCH (p:Person)-[:ABOUT]->(n:Note) RETURN p.name, n.text")
    assert _values(rows) == [("Cy", "x")]
