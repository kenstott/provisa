# Copyright (c) 2026 Kenneth Stott
# Canary: f974809c-1952-48bf-8327-b839292abd21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A node matched by no label is drawn from every label's rows, whatever its variable is named
(REQ-350): a variable `a` is never taken for the label A's node, nor `person` for Person's."""

# Requirements: REQ-350, REQ-2176

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def test_a_variable_named_as_a_model_label_returns_every_node(monkeypatch):
    s = Session(
        monkeypatch,
        {
            "Person": ({"id": "integer", "name": "text"}, "(1, 'Ann')"),
            "Pet": ({"id": "integer", "name": "text"}, "(1, 'Rex')"),
        },
    )
    rows = s.run("MATCH (person) RETURN person")
    assert sorted(r["person"]["label"] for r in rows) == ["Person", "Pet"]


def test_a_variable_named_as_a_session_label_returns_every_node(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:A), (:B)")
    rows = s.run("MATCH (a) RETURN a")
    assert sorted(r["a"]["label"] for r in rows) == ["A", "B"]
