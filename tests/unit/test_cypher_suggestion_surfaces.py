# Copyright (c) 2026 Kenneth Stott
# Canary: 4cf6adb7-0b96-4cc6-9cf9-b65257712c75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-778: every surface that parses a Cypher read with the role at hand says the same thing of an
unbound name that is a bound variable's property -- HTTP, Bolt, Flight, the MCP graph check, and
NL's validator and executor (the refusal the NL retry gives back to the model)."""

# Requirements: REQ-778

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock

import pytest

from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the shared pipeline fixture)
from tests.unit.test_cypher_cache_before_route import graph  # noqa: F401

_QUERY = "MATCH (o:Orders) RETURN id AS x"
_OFFER = "Did you mean o.id? o is bound to :Orders, which has a property id."


async def test_http(graph):  # noqa: F811
    from provisa.api.rest.cypher_router import CypherRequest, cypher_query

    request = MagicMock()
    request.state.role = "analyst"
    request.headers = {}
    response = await cypher_query(
        CypherRequest(query=_QUERY), request, query_id=None, x_provisa_stats=None
    )
    body = json.loads(bytes(response.body))
    assert response.status_code == 400
    assert body["code"] == "cypher.undefined_variable_property"
    assert body["detail"].endswith(_OFFER)


async def test_bolt(graph):  # noqa: F811
    from provisa.bolt.session import _execute_cypher

    with pytest.raises(ValueError, match="Did you mean o.id"):
        await _execute_cypher(_QUERY, {}, "analyst")


async def test_flight(graph):  # noqa: F811
    from provisa.api.flight.server import ProvisaFlightServer

    srv = ProvisaFlightServer.__new__(ProvisaFlightServer)
    srv._state = graph.state  # pyright: ignore[reportAttributeAccessIssue]
    loop = asyncio.get_running_loop()

    def _run_on_loop(coro, *, timeout=None):  # noqa: ARG001  # mirrors the real signature
        return asyncio.run_coroutine_threadsafe(coro, loop).result()

    srv._run_on_loop = _run_on_loop  # type: ignore[method-assign]
    with pytest.raises(Exception, match="Did you mean o.id"):
        await asyncio.to_thread(srv._do_get_cypher, {"query": _QUERY, "role": "analyst"})


async def test_mcp_graph_check(graph):  # noqa: F811
    from provisa.api.mcp.cypher_check import check_graph_read

    checked = await check_graph_read(graph.state, "analyst", _QUERY)
    assert checked.problem is not None and checked.problem.endswith(_OFFER)


async def test_nl_validator():
    from provisa.nl.loop import make_cypher_compiler
    from tests.unit.test_cypher_user_mistakes import _pets_label_map

    result = make_cypher_compiler(_pets_label_map())(
        "MATCH (a:Pets) RETURN a.breedName, COUNT(id) AS pet_count"
    )
    assert not result.valid
    assert result.error is not None and result.error.endswith(
        "Did you mean a.id? a is bound to :Pets, which has a property id."
    )


async def test_nl_executor(graph):  # noqa: F811
    from provisa.gql.translator_types import CypherMistake
    from provisa.nl.executor import _execute_cypher

    with pytest.raises(CypherMistake, match="Did you mean o.id"):
        await _execute_cypher(_QUERY, "analyst", graph.state, None)


async def test_the_nl_jobs_cypher_validator(graph):  # noqa: F811
    """The validator the NL job refines its Cypher against is built with the role's label map, so
    the retry is offered the bound variable's property as every other surface offers it."""
    from provisa.nl.runner import _nl_cypher_compiler, role_surface

    compile_cypher = _nl_cypher_compiler(
        role_surface(graph.state, "analyst"), "analyst", graph.state
    )
    result = compile_cypher(_QUERY)
    assert not result.valid
    assert result.error is not None and "Did you mean o.id" in result.error
