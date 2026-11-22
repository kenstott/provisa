# Copyright (c) 2026 Kenneth Stott
# Canary: c3f7a1e9-4d62-4b08-8e15-9a0d6b2f7c43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A variable-length relationship variable with no path variable is the relationships the walk
took (REQ-348, REQ-751).

``MATCH (a:Blue)-[r*]->(b:Green)`` resolves, by the labels' relationships, to fixed hops
(Blue -T-> Red -T-> Green). ``r`` was registered as that walk's relationships only when the
pattern named a path (``p = ...``): with none, ``count(r)`` read a column ``r`` no relation has
(DuckDB's binder error). The walk is now held under a name no statement can write, and ``r``
reads its steps in an expression as in RETURN."""

# Requirements: REQ-348, REQ-751

from __future__ import annotations

import pytest

from tests.unit.governed_session_harness import GovernedSession

pytestmark = pytest.mark.unit

_GRAPH = (
    "CREATE (a:Blue), (b:Red), (c:Green), (d:Yellow) "
    "CREATE (a)-[:T]->(b), (b)-[:T]->(c), (b)-[:T]->(d)"
)


def test_count_of_a_walk_s_relationships(monkeypatch):
    """Match9 [5]."""
    s = GovernedSession(monkeypatch)
    s.run(_GRAPH)
    assert s.run("MATCH (a:Blue)-[r*]->(b:Green) RETURN count(r)") == [{"count(r)": 1}], s.judged


def test_a_walk_s_relationships_in_an_expression_and_returned_agree(monkeypatch):
    s = GovernedSession(monkeypatch)
    s.run(_GRAPH)
    rows = s.run("MATCH (a:Blue)-[r*]->(b:Green) RETURN r, size(r) AS n")
    assert len(rows) == 1, s.judged
    assert [rel["type"] for rel in rows[0]["r"]] == ["T", "T"]
    assert rows[0]["n"] == 2
