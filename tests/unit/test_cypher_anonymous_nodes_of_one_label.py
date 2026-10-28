# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two anonymous nodes of one label in a pattern are two nodes, each read under its own name
(REQ-345): (:T)-[:R]->(:T) over a relationship from T to T, on the model and in the session."""

# Requirements: REQ-345, REQ-2176

from __future__ import annotations

import pytest

from provisa.gql.label_map import RelationshipMapping
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_STAFF = {
    "Person": (
        {"id": "integer", "name": "text", "boss": "integer"},
        "(1, 'Ann', NULL), (2, 'Bo', 1), (3, 'Cy', 1)",
    )
}
_BOSS = RelationshipMapping("REPORTS_TO", "Person", "Person", "boss", "id", "reports_to")


def test_two_anonymous_model_nodes_of_one_label(monkeypatch):
    s = Session(monkeypatch, _STAFF, relationships=[_BOSS])
    assert s.run("MATCH (:Person)-[r:REPORTS_TO]->(:Person) RETURN count(r) AS n") == [{"n": 2}]


def test_two_anonymous_session_nodes_of_one_label(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:T {k: 1})-[:R]->(:T {k: 2}), (:T {k: 3})-[:R]->(:T {k: 4})")
    assert s.run("MATCH (:T)-[r:R]->(:T) RETURN count(r) AS n") == [{"n": 2}]
