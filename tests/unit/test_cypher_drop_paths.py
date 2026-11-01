# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6a9c14-7e3b-4d08-b5c1-94d0e7a3f261
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
# ruff: noqa: F811 -- a fixture imported here is redefined by each test that takes it

"""What a drop on a Cypher frame writes (REQ-2135, provisa-ui's graph-drop.ts) runs, on DuckDB
against the rows it must answer: a class's schema path, and a node dropped on a node."""

# Requirements: REQ-2135, REQ-348

import json

import pytest

from tests.unit.test_cypher_find_paths_by_mapping import _rows, chain  # noqa: F401 -- the fixture


def test_a_class_pill_two_hops_away_is_read_through_its_path(chain):
    assert sorted(
        _rows(
            chain,
            "MATCH (a:A) OPTIONAL MATCH (a)-[r1:T]-(b:B)-[r2:T]-(c:C) RETURN a.name, b.name, c.name",
        )
    ) == [("a1", "b10", "c100"), ("a2", "b11", "c101")]


@pytest.mark.parametrize("func", ["shortestPath", "allShortestPaths"])
def test_a_node_dropped_on_a_node_finds_the_path_between_them(chain, func):
    rows = _rows(
        chain,
        f"MATCH p = {func}((a:A)-[*..4]-(c:C)) WHERE a.id = 1 AND c.id = 100 "
        "RETURN a.name, c.name LIMIT 25",
    )
    assert rows == [("a1", "c100")]


@pytest.mark.parametrize("func", ["shortestPath", "allShortestPaths"])
def test_two_nodes_nothing_joins_answer_no_path(chain, func):
    rows = _rows(
        chain,
        f"MATCH p = {func}((a:A)-[*..4]-(c:C)) WHERE a.id = 1 AND c.id = 101 "
        "RETURN a.name, c.name LIMIT 25",
    )
    assert rows == []


def test_a_node_path_returns_the_path_itself(chain):
    rows = _rows(
        chain,
        "MATCH p = allShortestPaths((a:A)-[*..4]-(c:C)) WHERE a.id = 1 AND c.id = 100 "
        "RETURN p LIMIT 25",
    )
    assert len(rows) == 1
    assert [n["id"] for n in json.loads(rows[0][0])["nodes"]] == ["A|1", "B|10", "C|100"]
