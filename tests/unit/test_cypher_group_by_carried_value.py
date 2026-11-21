# Copyright (c) 2026 Kenneth Stott
# Canary: 9d1e6f3a-7b25-4c80-b4a2-e5f08c3d6a71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A value a WITH carries is grouped by as the column it is read as (REQ-653, REQ-347).

``WITH a, other, min(length(p)) AS len RETURN a.name, collect(other.name), len``: ``len`` is in
the translator's var table (the WITH's CTE carries it) with no node mapping. The SELECT list
reads it as ``_w0."len"``; the implicit GROUP BY grouped it as ``len.*``, a node's star, which
the engine refused ("STAR expression is not supported here", Return6 [13]). It is grouped by its
column; a node is still grouped by its star."""

# Requirements: REQ-653, REQ-347

from __future__ import annotations

import pytest

from tests.unit.governed_session_harness import GovernedSession

pytestmark = pytest.mark.unit


def test_a_carried_aggregate_is_a_grouping_key(monkeypatch):
    """Return6 [13]."""
    s = GovernedSession(monkeypatch)
    s.run(
        "CREATE (a:T {name: 'a'}), (b:T {name: 'b'}), (c:T {name: 'c'}) "
        "CREATE (a)-[:R]->(b) CREATE (a)-[:R]->(c) CREATE (c)-[:R]->(b)"
    )
    rows = s.run(
        "MATCH p = (a:T {name: 'a'})-[:R*]->(other:T) WHERE other <> a "
        "WITH a, other, min(length(p)) AS len "
        "RETURN a.name AS name, collect(other.name) AS others, len"
    )
    assert [(r["name"], sorted(r["others"]), r["len"]) for r in rows] == [("a", ["b", "c"], 1)], (
        s.judged
    )


def test_a_carried_node_is_still_grouped_as_a_node(monkeypatch):
    s = GovernedSession(monkeypatch)
    s.run("CREATE (:T {name: 'a'})-[:R]->(:T {name: 'b'}), (:T {name: 'c'})")
    rows = s.run("MATCH (n:T) WITH n, 1 AS one RETURN n.name AS name, count(one) AS c")
    assert sorted((r["name"], r["c"]) for r in rows) == [("a", 1), ("b", 1), ("c", 1)], s.judged
