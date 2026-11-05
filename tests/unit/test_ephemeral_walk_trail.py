# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7a9c51-8d2f-4b16-a0e4-6f1c2d9b7a38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A graph session's walk answers what it walked (REQ-348, REQ-751, REQ-2176): a variable-length
relationship variable is the list of the relationships the walk took, in order, and a path its
every node and relationship, read from the walk's trail. An empty session walks its (empty)
relationships as one holding some does. Each is an openCypher TCK scenario, as written, run on
DuckDB through the session's own path."""

# Requirements: REQ-348, REQ-751, REQ-2176

from __future__ import annotations

from typing import Any

import pytest

from provisa.gql.label_map import RelationshipMapping
from provisa.gql.path_walk import UNLABELLED_ENDS
from provisa.gql.translator_types import CypherTranslateError
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def _rel(r: dict[str, Any]) -> tuple:
    """A relationship as the TCK writes it: ``[:TYPE {props}]``."""
    return ("rel", r["type"], r["properties"])


def _node(n: dict[str, Any]) -> tuple:
    """A node as the TCK writes it: ``(:Label {props})``."""
    return ("node", n["label"], n["properties"])


def _path(p: dict[str, Any]) -> list[tuple]:
    """A path as the TCK writes it: its nodes and relationships in turn."""
    out = [_node(p["nodes"][0])]
    assert len(p["nodes"]) == len(p["edges"]) + 1, p
    for edge, node in zip(p["edges"], p["nodes"][1:], strict=True):
        out += [_rel(edge), _node(node)]
    return out


def _rels(value: list[dict[str, Any]] | None) -> list[tuple] | None:
    return None if value is None else [_rel(r) for r in value]


# --- Match9: a variable-length relationship variable is the list of relationships it took -------


def test_tck_match9_1_variable_length_relationship_variables_are_lists(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a), (b), (c) CREATE (a)-[:T]->(b)")
    rows = s.run("MATCH ()-[r*0..1]-() RETURN last(r) AS l")
    got = sorted((None if r["l"] is None else _rel(r["l"]) for r in rows), key=repr)
    assert got == sorted([("rel", "T", {}), ("rel", "T", {}), None, None, None], key=repr)


def test_tck_match9_2_relationships_as_a_list_directed_one_way(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a:A)-[:REL {num: 1}]->(b:B)-[:REL {num: 2}]->(e:End)")
    rows = s.run("MATCH (a)-[r:REL*2..2]->(b:End) RETURN r")
    assert [_rels(r["r"]) for r in rows] == [
        [("rel", "REL", {"num": 1}), ("rel", "REL", {"num": 2})]
    ]


def test_tck_match9_3_relationships_as_a_list_undirected_from_two_extremes(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a:End)-[:REL {num: 1}]->(b:B)-[:REL {num: 2}]->(c:End)")
    rows = s.run("MATCH (a)-[r:REL*2..2]-(b:End) RETURN r")
    assert sorted((_rels(r["r"]) for r in rows), key=repr) == sorted(
        [
            [("rel", "REL", {"num": 1}), ("rel", "REL", {"num": 2})],
            [("rel", "REL", {"num": 2}), ("rel", "REL", {"num": 1})],
        ],
        key=repr,
    )


def test_tck_match9_4_relationships_as_a_list_undirected_from_one_extreme(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (s:Start)-[:REL {num: 1}]->(b:B)-[:REL {num: 2}]->(c:C)")
    rows = s.run("MATCH (a:Start)-[r:REL*2..2]-(b) RETURN r")
    assert [_rels(r["r"]) for r in rows] == [
        [("rel", "REL", {"num": 1}), ("rel", "REL", {"num": 2})]
    ]


# --- Match4: relationship lists ------------------------------------------------------------------


def test_tck_match4_1_fixed_length_variable_length_pattern(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:T]->()")
    rows = s.run("MATCH (a)-[r*1..1]->(b) RETURN r")
    assert [_rels(r["r"]) for r in rows] == [[("rel", "T", {})]]


def test_tck_match4_6_variable_length_from_a_bound_node(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a:A), (b), (c) CREATE (a)-[:X]->(b), (b)-[:Y]->(c)")
    rows = s.run("MATCH (a:A) MATCH (a)-[r*2]->() RETURN r")
    assert [_rels(r["r"]) for r in rows] == [[("rel", "X", {}), ("rel", "Y", {})]]


# --- Path2: relationships(p) ---------------------------------------------------------------------


def test_tck_path2_1_relationships_of_a_path(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (s:Start)-[:REL {num: 1}]->(b:B)-[:REL {num: 2}]->(c:C)")
    rows = s.run("MATCH p = (a:Start)-[:REL*2..2]->(b) RETURN relationships(p)")
    assert [_rels(r["relationships(p)"]) for r in rows] == [
        [("rel", "REL", {"num": 1}), ("rel", "REL", {"num": 2})]
    ]


def test_tck_path2_2_relationships_of_a_path_starting_from_the_end(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a:A)-[:REL {num: 1}]->(b:B)-[:REL {num: 2}]->(e:End)")
    rows = s.run("MATCH p = (a)-[:REL*2..2]->(b:End) RETURN relationships(p)")
    assert [_rels(r["relationships(p)"]) for r in rows] == [
        [("rel", "REL", {"num": 1}), ("rel", "REL", {"num": 2})]
    ]


# --- Match6: a path is every node and relationship it walked ----------------------------


def test_tck_match6_16_return_a_var_length_path(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (a:A {name: 'A'})-[:KNOWS {num: 1}]->(b:B {name: 'B'})"
        "-[:KNOWS {num: 2}]->(c:C {name: 'C'})"
    )
    rows = s.run("MATCH p = (n {name: 'A'})-[:KNOWS*1..2]->(x) RETURN p")
    a, b, c = (
        ("node", "A", {"name": "A"}),
        ("node", "B", {"name": "B"}),
        ("node", "C", {"name": "C"}),
    )
    one, two = ("rel", "KNOWS", {"num": 1}), ("rel", "KNOWS", {"num": 2})
    assert sorted((_path(r["p"]) for r in rows), key=len) == [[a, one, b], [a, one, b, two, c]]


def test_tck_match6_19_variable_length_relationship_without_lower_bound(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}) "
        "CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)"
    )
    rows = s.run("MATCH p = ({name: 'A'})-[:KNOWS*..2]->() RETURN p")
    a, b, c = ("node", "", {"name": "A"}), ("node", "", {"name": "B"}), ("node", "", {"name": "C"})
    knows = ("rel", "KNOWS", {})
    assert sorted((_path(r["p"]) for r in rows), key=len) == [
        [a, knows, b],
        [a, knows, b, knows, c],
    ]


def test_a_zero_length_path_is_its_one_node(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:A {name: 'a'})")
    rows = s.run("MATCH p = (a:A)-[*0..1]->(b) RETURN p, relationships(p) AS rs")
    assert [(_path(r["p"]), r["rs"]) for r in rows] == [([("node", "A", {"name": "a"})], [])]


def test_a_path_through_a_model_node_names_it(monkeypatch):
    people = {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Bo')")}
    s = Session(monkeypatch, people)
    s.run(
        "MATCH (p:Person) WHERE p.name = 'Ann' "
        "CREATE (p)-[:KNOWS]->(:Robot {m: 'R2'})-[:KNOWS]->(:Robot {m: 'C3'})"
    )
    rows = s.run("MATCH p = (a:Person {name: 'Ann'})-[*2..2]->(c) RETURN p")
    assert [_path(r["p"]) for r in rows] == [
        [
            ("node", "Person", {"id": 1, "name": "Ann"}),
            ("rel", "KNOWS", {}),
            ("node", "Robot", {"m": "R2"}),
            ("rel", "KNOWS", {}),
            ("node", "Robot", {"m": "C3"}),
        ]
    ]


# --- an empty session ------------------------------------------------------------------------------


def test_an_empty_session_walks_its_relationships_and_finds_none(monkeypatch):
    # REQ-2176: a session holding nothing yet is still a graph session; its walk reads its (empty)
    # relationships, never the model graph's refusal of two free ends.
    s = Session(monkeypatch, {})
    assert s.run("MATCH (a)-[r*1..2]->(b) RETURN r") == []
    assert s.run("MATCH p = (a)-[*0..1]->(b) RETURN p") == []


def test_an_empty_session_still_refuses_a_walk_the_models_relationships_could_take(monkeypatch):
    people = {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann')")}
    knows = RelationshipMapping("KNOWS", "Person", "Person", "id", "id", "knows")
    s = Session(monkeypatch, people, relationships=[knows])
    with pytest.raises(CypherTranslateError, match=UNLABELLED_ENDS):
        s.run("MATCH (a)-[:KNOWS*1..2]->(b) RETURN b")
