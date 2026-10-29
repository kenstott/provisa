# Copyright (c) 2026 Kenneth Stott
# Canary: 8d0b6a0e-3c1f-4b0e-9a52-6e1c7f4a2d91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read of a session table no write has created yet answers as an empty table of that table's
columns, not as an engine error (REQ-2176: a session's tables are created as statements write)."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest

from provisa.ephemeral import store
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_PEOPLE = {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Bo')")}


def _forget(s: Session, table: str) -> None:
    """The session as it stands before ``table`` was created."""
    s.duck.execute(f'DROP TABLE "{table}"')
    del s.session.tables[table]


def _values(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


def test_a_match_on_a_label_with_no_nodes_is_empty(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:Temp {k: 1})")
    s.run("MATCH (n:Temp) DELETE n")
    assert s.run("MATCH (n:Temp) RETURN n.k") == []


def test_a_node_labels_read_in_a_fresh_session_is_empty(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:Temp {k: 1})")
    _forget(s, store.NODE_LABELS)
    assert s.run("MATCH (n:Temp) RETURN n.k") == []
    assert s.run("MATCH (n:Person) RETURN n.name AS name") != []


def test_an_anchor_read_before_its_create_is_empty(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:Temp {k: 1})")
    _forget(s, store.anchor_table("Temp"))
    assert s.run("MATCH (n:Temp) RETURN n.k") == []
