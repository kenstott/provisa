# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4c1f27-3e9b-4d05-b6a2-71e0c5d9f384
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two forms of a variable-length relationship (REQ-348, REQ-751).

An inline property map on a variable-length relationship (``-[:T* {k: v}]->``) holds of every
relationship the walk takes: each hop of the walk is filtered by it, not the walk as a whole.

A relationship variable an earlier clause bound to a list of relationships (``WITH [r1, r2] AS
rs MATCH (a)-[rs*]->(b)``) is the walk itself: the pattern matches exactly those relationships,
in that order, from ``a`` to ``b``, and nothing when they do not chain that way."""

# Requirements: REQ-348, REQ-751

from __future__ import annotations

import pytest

from tests.unit.governed_session_harness import GovernedSession

pytestmark = pytest.mark.unit

_ARTISTS = (
    "CREATE (a:Artist:A), (b:Artist:B), (c:Artist:C) "
    "CREATE (a)-[:WORKED_WITH {year: 1987}]->(b), (b)-[:WORKED_WITH {year: 1988}]->(c)"
)

_CHAIN = "CREATE (a:A), (b:B), (c:C) CREATE (a)-[:Y]->(b), (b)-[:Y]->(c)"


def _labels(node: dict) -> set[str]:
    return set(node["labels"])


def test_a_walk_s_property_map_holds_of_every_hop(monkeypatch):
    """Match4 [5]: only B -> C has year 1988; A -> B -> C does not hold of its first hop."""
    s = GovernedSession(monkeypatch)
    s.run(_ARTISTS)
    rows = s.answer("MATCH (a:Artist)-[:WORKED_WITH* {year: 1988}]->(b:Artist) RETURN *")
    assert [(_labels(r["a"]), _labels(r["b"])) for r in rows] == [
        ({"Artist", "B"}, {"Artist", "C"})
    ], s.judged


def test_a_bounded_walk_s_property_map(monkeypatch):
    """The map with a bound: a walk is kept only when every relationship it takes holds it."""
    s = GovernedSession(monkeypatch)
    s.run(
        "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), (d:N {name: 'd'}) "
        "CREATE (a)-[:T {k: 1}]->(b), (b)-[:T {k: 1}]->(c), (c)-[:T {k: 2}]->(d)"
    )
    rows = s.run("MATCH (x:N)-[:T*1..3 {k: 1}]->(y:N) RETURN x.name AS x, y.name AS y")
    assert sorted((r["x"], r["y"]) for r in rows) == [("a", "b"), ("a", "c"), ("b", "c")], s.judged


def test_an_unlabelled_walk_s_property_map_and_its_relationships(monkeypatch):
    """A walk from an unlabelled node (the session's own walk): each relationship it took holds
    the map, a parameter's value."""
    s = GovernedSession(monkeypatch)
    s.run(
        "CREATE (a {name: 'a'}), (b {name: 'b'}), (c {name: 'c'}), (d {name: 'd'}) "
        "CREATE (a)-[:T {k: 1}]->(b), (b)-[:T {k: 1}]->(c), (c)-[:T {k: 2}]->(d)"
    )
    rows = s.run("MATCH (x)-[r:T* {k: $k}]->(y) RETURN x.name AS x, y.name AS y, size(r) AS n", k=1)
    assert sorted((r["x"], r["y"], r["n"]) for r in rows) == [
        ("a", "b", 1),
        ("a", "c", 2),
        ("b", "c", 1),
    ], s.judged


def test_a_walk_s_property_map_reading_a_variable_is_refused_by_name(monkeypatch):
    s = GovernedSession(monkeypatch)
    s.run("CREATE (a {k: 1})-[:T {k: 1}]->(b)")
    with pytest.raises(Exception, match="reads a variable"):
        s.run("MATCH (x) WITH x.k AS v MATCH (x)-[:T* {k: v}]->(y) RETURN y")


def test_a_walk_along_a_carried_list_of_relationships(monkeypatch):
    """Match4 [8]."""
    s = GovernedSession(monkeypatch)
    s.run(_CHAIN)
    rows = s.answer(
        "MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs LIMIT 1 "
        "MATCH (first)-[rs*]->(second) RETURN first, second"
    )
    assert [(_labels(r["first"]), _labels(r["second"])) for r in rows] == [({"A"}, {"C"})], s.judged


def test_a_walk_along_a_carried_list_between_bound_nodes(monkeypatch):
    """Match9 [6]."""
    s = GovernedSession(monkeypatch)
    s.run(_CHAIN)
    rows = s.answer(
        "MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs, a AS first, b AS second LIMIT 1 "
        "MATCH (first)-[rs*]->(second) RETURN first, second"
    )
    assert [(_labels(r["first"]), _labels(r["second"])) for r in rows] == [({"A"}, {"C"})], s.judged


def test_a_carried_list_walked_the_wrong_way_matches_nothing(monkeypatch):
    """Match9 [7]."""
    s = GovernedSession(monkeypatch)
    s.run(_CHAIN)
    rows = s.answer(
        "MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs, a AS second, b AS first LIMIT 1 "
        "MATCH (first)-[rs*]->(second) RETURN first, second"
    )
    assert rows == [], s.judged


def test_a_carried_list_between_labelled_ends(monkeypatch):
    """Labelled ends take the same walk: the list's relationships, in order."""
    s = GovernedSession(monkeypatch)
    s.run(_CHAIN)
    rows = s.answer(
        "MATCH (:A)-[r1]->(:B)-[r2]->(:C) WITH [r1, r2] AS rs "
        "MATCH (first:A)-[rs*]->(second:C) RETURN first, second, rs"
    )
    assert len(rows) == 1, s.judged
    assert (_labels(rows[0]["first"]), _labels(rows[0]["second"])) == ({"A"}, {"C"})
    assert [rel["type"] for rel in rows[0]["rs"]] == ["Y", "Y"]


def test_a_carried_list_out_of_order_matches_nothing(monkeypatch):
    """The list [r2, r1] is no walk: r1 does not start at C, where r2 ends."""
    s = GovernedSession(monkeypatch)
    s.run(_CHAIN)
    rows = s.run(
        "MATCH ()-[r1]->()-[r2]->() WITH [r2, r1] AS rs MATCH (first)-[rs*]->(second) "
        "RETURN first, second"
    )
    assert rows == [], s.judged
