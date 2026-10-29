# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7c2d54-91e8-4f0a-b6d3-5a1e8c7f3a29
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ORDER BY over values of several kinds (REQ-2167, REQ-347): the openCypher TCK's
ReturnOrderBy1 [11], [12] and WithOrderBy1 [21], [22] on DuckDB. A map, a node and a
relationship are each a JSON object, told apart by shape and ranked in openCypher's order."""

# Requirements: REQ-347, REQ-2167

from __future__ import annotations

import json

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_ITEMS = "[n, r, p, 1.5, ['list'], 'text', null, false, {a: 'map'}]"
_ASCENDING = ["map", "node", "rel", "list", "path", "text", False, 1.5, None]


def _kind(v) -> object:
    v = json.loads(v) if isinstance(v, str) and v[:1] in '{["' else v
    if isinstance(v, dict):
        if "nodes" in v and "edges" in v:
            return "path"
        if "startNode" in v or "type" in v:
            return "rel"
        if "label" in v:
            return "node"
        return "map"
    if isinstance(v, list):
        return "list"
    if v == "text":
        return "text"
    return v


def _session(monkeypatch) -> Session:
    s = Session(monkeypatch, {})
    s.run("CREATE (:N)-[:REL]->()")
    return s


def _kinds(rows) -> list:
    return [_kind(r["types"]) for r in rows]


@pytest.mark.parametrize("clause", ["RETURN types", "WITH types RETURN types"])
def test_mixed_kinds_ascending(monkeypatch, clause):
    # ReturnOrderBy1 [11], WithOrderBy1 [21]
    s = _session(monkeypatch)
    rows = s.run(f"MATCH p = (n:N)-[r:REL]->() UNWIND {_ITEMS} AS types {clause} ORDER BY types")
    assert _kinds(rows) == _ASCENDING


@pytest.mark.parametrize("clause", ["RETURN types", "WITH types RETURN types"])
def test_mixed_kinds_descending(monkeypatch, clause):
    # ReturnOrderBy1 [12], WithOrderBy1 [22]: descending is the reverse, null first.
    s = _session(monkeypatch)
    rows = s.run(
        f"MATCH p = (n:N)-[r:REL]->() UNWIND {_ITEMS} AS types {clause} ORDER BY types DESC"
    )
    assert _kinds(rows) == list(reversed(_ASCENDING))
