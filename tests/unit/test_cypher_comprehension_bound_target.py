# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1f8e2a-7c3d-4e96-a0b4-2d6f91c8e357
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pattern comprehension whose target the statement already binds (REQ-347): in
``MATCH (a), (b) RETURN [(a)-->(b) | ...]`` the target is that b, not any node the relationship
reaches. The comprehension had read the target's table under b's own name, shadowing the bound
b, so every b the source reached was listed. Run on DuckDB, over a session graph and over model
tables joined by a foreign key."""

# Requirements: REQ-347, REQ-2167

from __future__ import annotations

import pytest

from provisa.gql.label_map import RelationshipMapping
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_GRAPH = (
    "CREATE (a:A {name: 'a'}), (b:B {name: 'b'}), (c:B {name: 'c'}), (d:B {name: 'd'}), "
    "(a)-[:T]->(b), (a)-[:T]->(c)"
)


def test_a_bound_target_is_that_node(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(_GRAPH)
    rows = s.run("MATCH (a:A), (b:B) RETURN b.name AS b, [(a)-[:T]->(b) | b.name] AS r ORDER BY b")
    assert rows == [{"b": "b", "r": ["b"]}, {"b": "c", "r": ["c"]}, {"b": "d", "r": []}]


def test_a_bound_unlabelled_target_is_that_node(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(_GRAPH)
    rows = s.run("MATCH (a:A), (b {name: 'c'}) RETURN [(a)-->(b) | b.name] AS r")
    assert rows == [{"r": ["c"]}]


def test_an_unbound_target_is_every_node_reached(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(_GRAPH)
    rows = s.run("MATCH (a:A) RETURN [(a)-[:T]->(x) | x.name] AS r")
    assert sorted(rows[0]["r"]) == ["b", "c"]


_STAFF = {
    "Person": (
        {"id": "integer", "name": "text", "boss": "integer"},
        "(1, 'Ann', NULL), (2, 'Bo', 1), (3, 'Cy', 1)",
    )
}
_BOSS = RelationshipMapping("REPORTS_TO", "Person", "Person", "boss", "id", "reports_to")


def test_a_bound_target_on_model_tables(monkeypatch):
    s = Session(monkeypatch, _STAFF, relationships=[_BOSS])
    rows = s.run(
        "MATCH (e:Person {name: 'Bo'}), (m:Person) "
        "RETURN m.name AS m, [(e)-[:REPORTS_TO]->(m) | m.name] AS r ORDER BY m"
    )
    assert rows == [
        {"m": "Ann", "r": ["Ann"]},
        {"m": "Bo", "r": []},
        {"m": "Cy", "r": []},
    ]
