# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7f0d94-6c31-4a8e-b5d2-f1e9a3c7086d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""collect(nodes(p)) of a walk is a list of node lists, each node read as a node (REQ-751).

``MATCH p = (a)-[*]->(b) RETURN collect(nodes(p)) AS paths, length(p) AS l``: each row's
nodes(p) is a list of node JSON values, collected into a list of them. The item had no graph
kind (it was keyed by ``collect``), so the answer gave each node as its JSON text, and the
assembler read only a flat list of nodes (ReturnOrderBy2 [12])."""

# Requirements: REQ-751

from __future__ import annotations

import pytest

from tests.unit.governed_session_harness import GovernedSession

pytestmark = pytest.mark.unit


def test_collected_node_lists_are_nodes(monkeypatch):
    """ReturnOrderBy2 [12]."""
    s = GovernedSession(monkeypatch)
    s.run(
        "CREATE (a:A), (b:B), (c:C), (d:D), (e:E), (f:F) "
        "CREATE (a)-[:R]->(b) CREATE (c)-[:R]->(d) CREATE (d)-[:R]->(e) CREATE (e)-[:R]->(f)"
    )
    rows = s.run(
        "MATCH p = (a)-[*]->(b) RETURN collect(nodes(p)) AS paths, length(p) AS l ORDER BY l"
    )
    got = [(r["l"], sorted([n["label"] for n in path] for path in r["paths"])) for r in rows]
    assert got == [
        (1, [["A", "B"], ["C", "D"], ["D", "E"], ["E", "F"]]),
        (2, [["C", "D", "E"], ["D", "E", "F"]]),
        (3, [["C", "D", "E", "F"]]),
    ], s.judged
