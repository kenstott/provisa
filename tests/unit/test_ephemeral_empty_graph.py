# Copyright (c) 2026 Kenneth Stott
# Canary: bd5e0e38-d505-419a-a3b8-fe6b94abf180
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An unlabelled match over an empty graph (REQ-2176, REQ-2154): with no node type to draw from
(a role served no tables, a session holding no nodes) or types holding no rows, MATCH (n) reads
no rows, OPTIONAL MATCH (n) one null row, and a DELETE of the null node deletes nothing. None is
refused."""

# Requirements: REQ-2176, REQ-2154

from __future__ import annotations

import asyncio

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

# Only the session's graph, no model type; then model types holding no rows.
_GRAPHS = pytest.mark.parametrize(
    "model",
    [{}, {"Person": ({"id": "integer", "name": "text"}, "")}],
    ids=["session-only", "empty-model"],
)


@_GRAPHS
def test_match_n_reads_no_rows(monkeypatch, model):
    s = Session(monkeypatch, model)
    assert s.run("MATCH (n) RETURN n") == []
    assert s.run("MATCH (n) RETURN n.name") == []


@_GRAPHS
def test_optional_match_n_reads_one_null_row(monkeypatch, model):
    s = Session(monkeypatch, model)
    assert s.run("OPTIONAL MATCH (n) RETURN n") == [{"n": None}]


@_GRAPHS
def test_delete_of_a_null_node_deletes_nothing(monkeypatch, model):
    # The session holds no node, so the statement is wholly the governed write: its read matches
    # nothing and it sends no write.
    s = Session(monkeypatch, model)
    asyncio.run(s.write("OPTIONAL MATCH (n) DELETE n", {}))
    assert s.sent == []
    assert s.run("MATCH (n) RETURN n") == []
