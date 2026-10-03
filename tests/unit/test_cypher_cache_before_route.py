# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8b2d17-9a4c-4e60-b7d1-6c0e5a2f9b48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher request answered from the response cache does no route work (REQ-1897).

The planner reads the cache before it routes, for a caller that says it serves a cached plan.
Every Cypher surface — Bolt, ``/data/cypher``, Arrow Flight — says so and hands the cached plan to
the pipeline terminal: a repeated opted-in statement is then served with no routing, no SQL parse,
no control-plane read and nothing sent to the source."""

# Requirements: REQ-1897, REQ-1877, REQ-345

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from tests.unit.test_buffered_auto_threshold import _ControlPlane
from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the shared pipeline fixture)

_CYPHER = "// @provisa cache=true\nMATCH (o:Orders) WHERE o.id = $v RETURN o.id AS id"
_UNHINTED = "MATCH (o:Orders) WHERE o.id = $v RETURN o.id AS id"


@pytest.fixture
def graph(pipe):  # noqa: F811
    """The pipeline stand-in with what the Cypher surfaces read beside it."""
    state = pipe.state
    state.cypher_label_maps = {}
    state.schema_build_cache = {"tables": None, "relationships": None, "column_types": None}
    ctx = state.contexts["analyst"]
    ctx.aggregate_columns[1] = [("id", "integer")]
    return pipe


def _assert_no_route_work(graph, before: dict, sent: int) -> None:
    assert graph.counts == before, "a cache hit routed, looked up API tables or parsed SQL"
    assert len(graph.state.federation_engine.statements) == sent, "a cache hit reached the source"
    assert graph.state.tenant_db.acquires == 0, "a cache hit read the control plane"
    assert graph.audits[-1]["route"] == "cache"


async def test_a_bolt_cache_hit_does_no_route_work(graph):
    from provisa.bolt.session import _execute_cypher

    assert await _execute_cypher(_CYPHER, {"v": 7}, "analyst") == (["id"], [[7]], None)
    before, sent = dict(graph.counts), len(graph.state.federation_engine.statements)
    assert sent == 1
    graph.state.tenant_db = _ControlPlane()  # any control-plane statement fails the request
    graph.state.model_db = graph.state.tenant_db
    for _ in range(3):
        assert await _execute_cypher(_CYPHER, {"v": 7}, "analyst") == (["id"], [[7]], None)
    _assert_no_route_work(graph, before, sent)


async def test_a_bolt_request_that_did_not_opt_in_is_routed_and_read(graph):
    from provisa.bolt.session import _execute_cypher

    for _ in range(2):
        assert (await _execute_cypher(_UNHINTED, {"v": 7}, "analyst"))[1] == [[7]]
    assert len(graph.state.federation_engine.statements) == 2
    assert graph.state.response_cache_store.gets == 0


async def test_a_data_cypher_cache_hit_does_no_route_work(graph):
    from provisa.api.rest.cypher_router import CypherRequest, cypher_query

    request = MagicMock()
    request.state.role = "analyst"

    async def _call():
        body = CypherRequest(query=_CYPHER, params={"v": 7})
        return await cypher_query(body, request, query_id=None, x_provisa_stats=None)

    miss = await _call()
    assert miss.status_code == 200 and miss.headers["X-Provisa-Cache"] == "MISS", miss.body
    before, sent = dict(graph.counts), len(graph.state.federation_engine.statements)
    assert sent == 1
    graph.state.tenant_db = _ControlPlane()
    graph.state.model_db = graph.state.tenant_db
    for _ in range(3):
        hit = await _call()
        assert hit.headers["X-Provisa-Cache"] == "HIT" and "X-Provisa-Cache-Age" in hit.headers
        assert hit.body == miss.body
    _assert_no_route_work(graph, before, sent)


async def test_a_flight_cypher_cache_hit_does_no_route_work(graph):
    from provisa.api.flight.server import ProvisaFlightServer

    srv = ProvisaFlightServer.__new__(ProvisaFlightServer)
    srv._state = graph.state  # pyright: ignore[reportAttributeAccessIssue]
    loop = asyncio.get_running_loop()

    def _run_on_loop(coro, *, timeout=None):  # noqa: ARG001  # mirrors the real signature
        return asyncio.run_coroutine_threadsafe(coro, loop).result()

    srv._run_on_loop = _run_on_loop  # type: ignore[method-assign]
    ticket = {"query": _CYPHER, "role": "analyst", "params": {"v": 7}}

    async def _get() -> list[dict]:
        stream = await asyncio.to_thread(srv._do_get_cypher, ticket)
        assert isinstance(stream, pa.flight.RecordBatchStream)
        return SimpleNamespace(stream=stream)  # type: ignore[return-value]

    await _get()
    before, sent = dict(graph.counts), len(graph.state.federation_engine.statements)
    assert sent == 1
    graph.state.tenant_db = _ControlPlane()
    graph.state.model_db = graph.state.tenant_db
    for _ in range(3):
        await _get()
    _assert_no_route_work(graph, before, sent)
