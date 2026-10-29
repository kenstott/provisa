# Copyright (c) 2026 Kenneth Stott
# Canary: 72d93003-102c-4481-ad60-04d875d65826
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Patterns read in a graph session (REQ-347, REQ-348, REQ-2176): openCypher TCK scenarios of
the Match*, MatchWhere*, Pattern*, Path*, Quantifier*, ExistentialSubquery* and
TriadicSelection* features, run on DuckDB through the session's own read path."""

# Requirements: REQ-347, REQ-348, REQ-2176

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def _sorted(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


# --- a session's own node carries only its own properties ----------------------------------------


def test_a_session_node_returned_by_its_label_carries_no_key_property(monkeypatch):
    # TCK MatchWhere5 [1]: a node the session created has the properties it was created with;
    # the built-in _id is a model node's key (REQ-2136), which a session node has not.
    s = Session(monkeypatch, {})
    s.run("CREATE (root:Root {name: 'x'})-[:T]->(:TextNode {var: 'text'})")
    rows = s.run("MATCH (:Root {name: 'x'})-->(i:TextNode) RETURN i")
    assert [r["i"]["properties"] for r in rows] == [{"var": "text"}]
