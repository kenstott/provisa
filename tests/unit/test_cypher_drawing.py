# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4f2a61-3c7e-4b95-a0d8-e21f6c9b7a53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher read the graph explorer draws (REQ-392) -- one returning nodes, relationships or
paths -- is answered inline, never delivered to a file, whatever the redirect threshold; one
returning rows only goes through the redirect decision as on every surface. The columns drawn are
named from the translation's graph_vars."""

# Requirements: REQ-392, REQ-768, REQ-769, REQ-1224

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from provisa.api.rest.cypher_drawing import graph_columns
from provisa.gql.translator_types import GraphVarKind
from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the shared pipeline fixture)

_QUERY = "MATCH (o:Orders) RETURN o.id AS id"


@pytest.fixture
def graph(pipe, monkeypatch):  # noqa: F811
    """The pipeline stand-in, with the redirect decision recorded: a drawn read must never reach
    it, a read of rows must."""
    import provisa.executor.redirect as redirect

    state = pipe.state
    state.cypher_label_maps = {}
    state.schema_build_cache = {"tables": None, "relationships": None, "column_types": None}
    state.contexts["analyst"].aggregate_columns[1] = [("id", "integer")]
    pipe.redirect_asked = []
    monkeypatch.setattr(
        redirect,
        "auto_delivery_for_buffered",
        lambda role, *a: pipe.redirect_asked.append(role),  # None: inline, as when disabled
    )
    return pipe


async def _ask(graph, rows: int):
    from provisa.api.rest.cypher_router import CypherRequest, cypher_query

    graph.state.federation_engine._rows_for = lambda sql, params: [(i,) for i in range(rows)]
    request = MagicMock()
    request.state.role = "analyst"
    request.headers = {}
    resp = await cypher_query(
        CypherRequest(query=_QUERY), request, query_id=None, x_provisa_stats=None
    )
    return resp.status_code, json.loads(bytes(resp.body))


@pytest.mark.asyncio
async def test_a_drawn_read_is_answered_inline_without_the_redirect_decision(graph, monkeypatch):
    import provisa.api.rest.cypher_drawing as drawing

    monkeypatch.setattr(drawing, "graph_columns", lambda gv: {"id": "node"})  # a drawn column
    status, body = await _ask(graph, rows=1160)
    assert status == 200, body
    assert len(body["rows"]) == 1160 and "redirect" not in body
    assert body["graph_columns"] == {"id": "node"}
    assert graph.redirect_asked == []


@pytest.mark.asyncio
async def test_a_rows_only_read_meets_the_redirect_rule(graph):
    status, body = await _ask(graph, rows=5)
    assert status == 200, body
    assert body["graph_columns"] == {} and len(body["rows"]) == 5
    assert graph.redirect_asked == ["analyst"]  # the threshold decides, as on every surface


def test_graph_columns_name_what_the_translation_returns():
    kinds = {
        "n": GraphVarKind.NODE,
        "r": GraphVarKind.EDGE,
        "p": GraphVarKind.PATH,
        "m": GraphVarKind.MAP,
    }
    assert graph_columns(kinds) == {"n": "node", "r": "edge", "p": "path"}
