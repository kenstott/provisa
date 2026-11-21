# Copyright (c) 2026 Kenneth Stott
# Canary: e4a8b2c6-1f93-4d57-9a0e-7c5d3b1f8e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A graph function of a path a WITH carries reads the path's JSON (REQ-751, REQ-348).

``MATCH p = ()-[*]->() WITH count(*) AS count, p AS p RETURN nodes(p)``: past the WITH, ``p``
is the path's JSON (``{"nodes": [...], "edges": [...]}``), no pattern variable, and nodes() of it
was refused ("graph function 'nodes' needs a variable argument"). nodes(), relationships(),
length(), startNode() and endNode() of such a value read its part, as labels() and type() of a
list's element already did."""

# Requirements: REQ-751, REQ-348

from __future__ import annotations

import pytest

from tests.unit.governed_session_harness import GovernedSession

pytestmark = pytest.mark.unit

_TWO_EDGES = (
    "CREATE (n1 {num: 1}), (n2 {num: 2}), (n3 {num: 3}), (n4 {num: 4}) "
    "CREATE (n1)-[:T]->(n2), (n3)-[:T]->(n4)"
)


def _nums(nodes: list[dict]) -> list[int]:
    return [n["properties"]["num"] for n in nodes]


def test_nodes_of_a_path_a_with_carries(monkeypatch):
    """With6 [4]."""
    s = GovernedSession(monkeypatch)
    s.run(_TWO_EDGES)
    rows = s.run("MATCH p = ()-[*]->() WITH count(*) AS count, p AS p RETURN nodes(p) AS nodes")
    assert sorted(_nums(r["nodes"]) for r in rows) == [[1, 2], [3, 4]], s.judged


def test_the_parts_of_a_carried_path(monkeypatch):
    s = GovernedSession(monkeypatch)
    s.run(_TWO_EDGES)
    rows = s.run(
        "MATCH p = ()-[*]->() WITH p AS p "
        "RETURN length(p) AS len, size(relationships(p)) AS rels, "
        "startNode(relationships(p)[0]).num AS first, endNode(relationships(p)[0]).num AS last"
    )
    assert sorted((r["len"], r["rels"], r["first"], r["last"]) for r in rows) == [
        (1, 1, 1, 2),
        (1, 1, 3, 4),
    ], s.judged
