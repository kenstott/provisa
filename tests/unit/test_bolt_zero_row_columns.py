# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0e7c41-8b2a-4f39-a6e1-2c9b7f40d318
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bolt read that matches no row still answers its columns (REQ-347, REQ-2167): the statement
names them, rows or none, as /data/cypher answers them (cypher_router._answered_columns)."""

# Requirements: REQ-347, REQ-2167

from __future__ import annotations

import pytest

from tests.unit.test_buffered_auto_threshold import _Source
from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the shared pipeline fixture)

pytestmark = pytest.mark.unit


@pytest.fixture
def graph(pipe):  # noqa: F811
    state = pipe.state
    state.cypher_label_maps = {}
    state.schema_build_cache = {"tables": None, "relationships": None, "column_types": None}
    state.contexts["analyst"].aggregate_columns[1] = [("id", "integer")]
    return pipe


async def test_a_bolt_read_of_no_rows_answers_its_columns(graph):
    from provisa.bolt.session import _execute_cypher

    graph.state.federation_engine = _Source(lambda sql, params: [])
    cypher = "MATCH (o:Orders) WHERE o.id = $v RETURN o.id AS id"
    assert await _execute_cypher(cypher, {"v": 7}, "analyst") == (["id"], [], None)
