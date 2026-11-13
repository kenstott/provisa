# Copyright (c) 2026 Kenneth Stott
# Canary: 6e0c4b8a-3d17-4f92-a5e1-b8f2d9c04a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A variable-length pattern comprehension is one list of paths (REQ-348, REQ-751).

``[p = (a)-[*]->(b) | p]`` is the embedded statement ``MATCH p = (a)-[*]->(b) RETURN p``,
whose walk has no upper bound and so carries its depth-limit column (CUT_COLUMN) beside ``p``:
two columns where the list takes one ("Subquery returns 2 columns - expected 1", Pattern2 [9]).
The column is taken off; a walk that reached the limit and could go on fails by name in the
row's value, as the statement's own walk does, never answered cut short."""

# Requirements: REQ-348, REQ-751

from __future__ import annotations

import pytest

from provisa.gql.translator_types import CUT_COLUMN
from tests.unit.governed_session_harness import GovernedSession

pytestmark = pytest.mark.unit


def test_a_variable_length_comprehension_in_with(monkeypatch):
    """Pattern2 [9]."""
    s = GovernedSession(monkeypatch)
    s.run("CREATE (:A)-[:T]->(:B)")
    rows = s.run(
        "MATCH (a:A), (b:B) WITH [p = (a)-[*]->(b) | p] AS paths, count(a) AS c RETURN paths, c"
    )
    assert len(rows) == 1 and rows[0]["c"] == 1, s.judged
    (path,) = rows[0]["paths"]
    assert [n["label"] for n in path["nodes"]] == ["A", "B"]
    assert [e["type"] for e in path["edges"]] == ["T"]


def test_the_comprehension_s_walk_still_fails_past_the_depth_limit(monkeypatch):
    s = GovernedSession(monkeypatch)
    s.run("CREATE (:A)-[:T]->(:B)")
    s.run("MATCH (a:A), (b:B) RETURN size([p = (a)-[*]->(b) | p]) AS n")
    sql = s.judged[-1]
    inner = sql[sql.index("ARRAY(") :]
    assert f"AS {CUT_COLUMN}" not in inner.split(") AS", 1)[0], sql
    assert "exceeded the maximum depth" in inner, sql
