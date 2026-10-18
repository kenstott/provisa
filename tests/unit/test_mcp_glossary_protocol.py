# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8b2d61-9c5f-4a7e-b0d3-7f2c6a1e8d99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1387: search_terms through the MCP protocol layer, not just the tools module.

FastMCP's call_tool drives the registered closure with real argument marshalling, so
this pins what an MCP client actually receives: the tool is registered, the role gate
fires through the protocol, and the term payload round-trips serialization.
"""

# Requirements: REQ-1387

from __future__ import annotations

import json
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import insert

from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources
from tests.role_state import RoleState

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _server(control_plane):
    from provisa.api.mcp.server import build_mcp_server

    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="d", description=""))
        c.execute(insert(sources).values(id="__derived__", type="postgresql"))
    db = control_plane.org()
    async with db.acquire() as conn:
        await table_repo.upsert(
            conn,
            Table(
                source_id="__derived__",
                domain_id="d",
                schema_name="s",
                table_name="orders",
                columns=[Column(name="cust_id", data_type="text", visible_to=[])],
                view_sql="SELECT 1",
            ),
        )
    state = RoleState(
        contexts={"analyst": object()},
        model_db=db,
        tenant_db=db,
        # REQ-1591: the search narrows to the domains the ROLE reaches; "*" is unlimited.
        roles={"analyst": {"domain_access": ["*"]}},
    )
    yield build_mcp_server(state), db


async def test_search_terms_is_a_registered_tool(control_plane):
    async with _server(control_plane) as (server, _db):
        names = {t.name for t in await server.list_tools()}
        assert "search_terms" in names


async def test_search_terms_round_trips_through_the_protocol(control_plane, monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_ROLE", "analyst")  # stdio names roles within its pin
    async with _server(control_plane) as (server, db):
        # Derived and undefined, so the protocol offers nothing: an agent may only bind a
        # question to a term that is in service, defined, and grounded in a column.
        empty = await server.call_tool("search_terms", {"query": "customer", "role": "analyst"})
        # No hits serializes as no content blocks, so the structured half carries the answer.
        assert empty[1] == {"result": []}
        async with db.acquire() as conn:
            term = (await glossary_repo.list_terms(conn))[0]
            await glossary_repo.set_definition(conn, term["id"], "The buyer.")
        content = await server.call_tool("search_terms", {"query": "customer", "role": "analyst"})
        blocks = content[0] if isinstance(content, tuple) else content
        payload = json.loads(blocks[0].text)
        term = payload[0] if isinstance(payload, list) else payload
        assert term["name"] == "customer"
        assert term["refs"][0]["column_name"] == "cust_id"


async def test_unknown_role_fails_through_the_protocol(control_plane, monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_ROLE", "nobody")  # pinned, yet no role the model knows
    async with _server(control_plane) as (server, _db):
        with pytest.raises(Exception, match="No schema for role"):
            await server.call_tool("search_terms", {"query": "customer", "role": "nobody"})
