# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7d1e95-6c42-4b80-9f2e-8d5b0c71a6e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2176: MCP's Cypher runs in its caller's graph session, the one the Cypher page reads.

What an MCP statement makes the page lists, what the page's statements make MCP reads, and MCP's
clear tool ends the session for both. A remote caller is its identity's principal; a local one
(stdio, the unsecured loopback) is the anonymous principal an unsecured page shares. A cached
answer makes one control-plane read: the caller's graph session.
"""

# Requirements: REQ-2176

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.api import graph_session
from provisa.api.mcp import tools
from provisa.api.rest import cypher_session
from provisa.compiler import temp_tables
from provisa.ephemeral import seam
from tests.unit.ephemeral_harness import Session
from tests.unit.test_buffered_auto_threshold import _ControlPlane
from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the shared pipeline fixture)
from tests.unit.test_cypher_cache_before_route import _CYPHER, _assert_no_route_work, graph  # noqa: F401
from tests.unit.test_cypher_page_session import _MODEL, _request, _row, _state, dbs, ended  # noqa: F401

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

_ADA = ("ada", ["analyst"])


@pytest.fixture
def served(dbs, monkeypatch):  # noqa: F811
    """The page's state and MCP's, one control plane; Bolt's Cypher pipeline (which MCP runs
    through) as the ephemeral harness runs a statement, in the session bound for it."""
    import provisa.api.app as appmod
    from provisa.bolt import session as bolt_session

    h = Session(monkeypatch, _MODEL)
    state = _state(dbs, h)
    state.roles["analyst"] = {**state.roles["analyst"], "capabilities": ["query_development"]}
    monkeypatch.setattr(appmod, "state", state)
    monkeypatch.setattr(tools, "require_role", lambda _role, _state: None)

    async def execute(cypher, params, role, **_kwargs):
        held = temp_tables.current()
        assert held is not None, "MCP's Cypher runs in a graph session"
        h.session = held
        made = await seam.write(cypher, params, h.model, held, h.state, h.read, h.write)
        if made is not None:
            return [], [], None
        rows = await h.read(cypher, params)
        columns = list(rows[0]) if rows else []
        return columns, [[r[c] for c in columns] for r in rows], None

    monkeypatch.setattr(bolt_session, "_execute_cypher", execute)
    return SimpleNamespace(state=state, harness=h)


async def test_what_mcp_makes_the_page_lists(served):
    await tools.run_cypher(served.state, "analyst", "CREATE (:Note {text: 'mcp'})", principal=_ADA)
    body = json.loads(bytes((await cypher_session.describe(_request())).body))
    assert body["labels"] == [{"label": "Note", "nodes": 1, "tagged_model_nodes": 0}]


async def test_what_the_page_makes_mcp_reads(served):
    h = served.harness
    async with graph_session.session_for(served.state, "ada", ["analyst"], writes=True) as held:
        h.session = held.session
        await seam.write(
            "CREATE (:Draft {text: 'page'})", {}, h.model, h.session, h.state, h.read, h.write
        )
    read = await tools.run_cypher(
        served.state, "analyst", "MATCH (d:Draft) RETURN d.text AS t", principal=_ADA
    )
    assert read["rows"] == [{"t": "page"}]


async def test_mcps_clear_tool_ends_the_session_for_every_client(served, ended):  # noqa: F811
    await tools.run_cypher(served.state, "analyst", "CREATE (:Note {text: 'mcp'})", principal=_ADA)
    assert await tools.clear_graph_session(served.state, _ADA) == {"cleared": True}
    assert ended == [graph_session.session_id("ada")]
    body = json.loads(bytes((await cypher_session.describe(_request())).body))
    assert body == {"labels": [], "types": [], "empty": True}


async def test_a_local_caller_is_the_anonymous_principal_and_a_remote_one_its_identity():
    from provisa.api.mcp import server

    assert server._call_principal(SimpleNamespace()) == (graph_session.ANONYMOUS, [])
    identity = SimpleNamespace(user_id="ada", roles=["analyst"], derived_roles={"steward": ""})
    token = server._request_identity.set(identity)
    try:
        assert server._call_principal(SimpleNamespace()) == ("ada", ["analyst", "steward"])
    finally:
        server._request_identity.reset(token)


async def test_the_anonymous_principal_holds_no_roles_whichever_it_names():
    assert graph_session.claims_of(graph_session.ANONYMOUS, ["analyst"], {"x": ""}) == []
    assert graph_session.claims_of("ada", ["analyst"], {"x": ""}) == ["analyst", "x"]


async def test_an_mcp_cache_hit_reads_the_callers_graph_session_and_nothing_else(graph):  # noqa: F811
    async def _call():
        return await tools.run_cypher(graph.state, "analyst", _CYPHER, {"v": 7}, principal=_ADA)

    assert (await _call())["rows"] == [{"id": 7}]
    before, sent = dict(graph.counts), len(graph.state.federation_engine.statements)
    graph.state.tenant_db = _ControlPlane()  # any other control-plane statement fails the call
    graph.state.model_db = graph.state.tenant_db
    read = len(graph.sessions)
    for _ in range(3):
        assert (await _call())["rows"] == [{"id": 7}]
    _assert_no_route_work(graph, before, sent)
    assert len(graph.sessions) == read + 3
