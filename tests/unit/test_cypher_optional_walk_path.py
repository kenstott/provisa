# Copyright (c) 2026 Kenneth Stott
# Canary: 5a9c2e71-8b40-4d13-a6f9-2c7e1b5d0f38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An OPTIONAL MATCH's walk that found nothing gives a null path (REQ-751, REQ-348).

A session walk's path is ``JSON_OBJECT('nodes', <walk>.nodes, 'edges', <walk>.rels)`` over the
walk's LEFT JOIN. Where the walk found nothing that row is nulls, and the path was still an object
-- ``{"nodes": null}``, or a first hop's nulls before it (``[null, null]``) -- which the assembler
failed on (TypeError, AttributeError), answered as a refusal. The path is null there."""

# Requirements: REQ-751, REQ-348

from __future__ import annotations

import pytest

from tests.unit.governed_session_harness import GovernedSession

pytestmark = pytest.mark.unit


def test_an_unmatched_walk_from_a_hop_is_a_null_path(monkeypatch):
    """Match7 [19]."""
    s = GovernedSession(monkeypatch)
    s.run("CREATE (a {name: 'A'}), (b {name: 'B'}) CREATE (a)-[:X]->(b)")
    rows = s.run("MATCH (a {name: 'A'}) OPTIONAL MATCH p = (a)-->(b)-[*]->(c) RETURN p")
    assert rows == [{"p": None}], s.judged


def test_an_unmatched_walk_between_bound_nodes_is_a_null_path(monkeypatch):
    """Match7 [20]."""
    s = GovernedSession(monkeypatch)
    s.run(
        "CREATE (s:Single), (a:A {num: 42}), (b:B {num: 46}), (c:C) "
        "CREATE (s)-[:REL]->(a), (s)-[:REL]->(b), (a)-[:REL]->(c), (b)-[:LOOP]->(b)"
    )
    rows = s.run("MATCH (a:A), (b:B) OPTIONAL MATCH p = (a)-[*]->(b) RETURN p")
    assert rows == [{"p": None}], s.judged


def test_matched_and_unmatched_walks_side_by_side(monkeypatch):
    """Match9 [9]: B is reached (its relationships and path), C is not (both null)."""
    s = GovernedSession(monkeypatch)
    s.run("CREATE (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}) CREATE (a)-[:X]->(b)")
    rows = s.run(
        "MATCH (a {name: 'A'}), (x) WHERE x.name IN ['B', 'C'] "
        "OPTIONAL MATCH p = (a)-[r*]->(x) RETURN r, x, p"
    )
    by_name = {row["x"]["properties"]["name"]: row for row in rows}
    assert set(by_name) == {"B", "C"}, s.judged
    reached = by_name["B"]
    assert [rel["type"] for rel in reached["r"]] == ["X"]
    assert [n["properties"]["name"] for n in reached["p"]["nodes"]] == ["A", "B"]
    assert [rel["type"] for rel in reached["p"]["edges"]] == ["X"]
    assert by_name["C"]["r"] is None
    assert by_name["C"]["p"] is None
