# Copyright (c) 2026 Kenneth Stott
# Canary: bce4ad4f-33ed-49b9-8b1b-d120ff5ff863
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bolt read that answers no rows names its columns, as Cypher over HTTP does: a client reads
the fields of a RUN's SUCCESS whether or not records follow (openCypher TCK Call1[3]:
``MATCH (n) CALL test.doNothing() RETURN n`` over an empty graph answers the column ``n``)."""

# Requirements: REQ-347, REQ-2167

from __future__ import annotations

import pytest

from tests.unit.test_buffered_auto_threshold import _Source
from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the shared pipeline fixture)

pytestmark = [pytest.mark.unit]


@pytest.fixture
def graph(pipe):  # noqa: F811
    state = pipe.state
    state.cypher_label_maps = {}
    state.schema_build_cache = {"tables": None, "relationships": None, "column_types": None}
    state.contexts["analyst"].aggregate_columns[1] = [("id", "integer")]
    return pipe


async def test_a_bolt_read_of_no_rows_names_its_columns(graph):
    from provisa.bolt.session import _execute_cypher

    graph.state.federation_engine = _Source(lambda _sql, _params: [])
    query = "MATCH (o:Orders) WHERE o.id = $v RETURN o.id AS id"
    assert await _execute_cypher(query, {"v": 7}, "analyst") == (["id"], [], None)
