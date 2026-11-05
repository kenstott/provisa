# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2c4a71-e5b8-4f30-8a6e-1c7b3f05d942
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A node an unlabelled MATCH returns keeps its properties' types (REQ-347): the value each
label's arm of the union builds for it (_node_value) had cast every property to text, so a list
came back as '[1, 2]' and a number as '3'. The scenarios are the openCypher TCK's (Return2 [7],
WithOrderBy1 [31], [32]) returning the node, run on DuckDB over a session graph."""

# Requirements: REQ-347, REQ-2167

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def test_a_returned_node_keeps_its_lists(monkeypatch):
    # Return2 [7], the node returned
    s = Session(monkeypatch, {})
    s.run("CREATE ({list1: [1, 2, 3], list2: [4, 5]})")
    [row] = s.run("MATCH (a) RETURN a")
    assert row["a"]["properties"] == {"list1": [1, 2, 3], "list2": [4, 5]}


def test_a_returned_node_keeps_numbers_text_and_booleans(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:A {n: 3, x: 1.5, name: 'x', ok: true}), (:B {n: 4})")
    rows = s.run("MATCH (a) RETURN a ORDER BY a.n")
    assert [r["a"]["properties"] for r in rows] == [
        {"n": 3, "x": 1.5, "name": "x", "ok": True},
        {"n": 4},
    ]


@pytest.mark.parametrize(
    ("sort", "first"),
    [("list", [[1, -20], [1, 2], [2, -2]]), ("list DESC", [[300, 0], [2, -2, 100], [2, -2]])],
)
def test_a_node_sorted_by_its_list_keeps_it(monkeypatch, sort, first):
    # WithOrderBy1 [31], [32], the node returned
    s = Session(monkeypatch, {})
    s.run(
        "CREATE (:A {list: [2, -2]}), (:B {list: [1, 2]}), (:C {list: [300, 0]}), "
        "(:D {list: [1, -20]}), (:E {list: [2, -2, 100]})"
    )
    rows = s.run(f"MATCH (a) WITH a, a.list AS list WITH a, list ORDER BY {sort} LIMIT 3 RETURN a")
    assert [r["a"]["properties"]["list"] for r in rows] == first


def test_keys_of_a_node_with_a_list(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:A {list: [1, 2], name: 'x'}), (:B {n: 1})")
    rows = s.run("MATCH (a) RETURN keys(a) AS k ORDER BY size(keys(a))")
    assert [sorted(r["k"]) for r in rows] == [["n"], ["list", "name"]]
