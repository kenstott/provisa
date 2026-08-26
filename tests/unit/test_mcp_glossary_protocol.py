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
import types
from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    glossary_term_domains,
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    roles,
    table_columns,
)

pytestmark = pytest.mark.asyncio

_TABLES = [
    registered_tables,
    table_columns,
    roles,
    glossary_terms,
    glossary_term_refs,
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_domains,
]


@asynccontextmanager
async def _server(tmp_path):
    from provisa.api.mcp.server import build_mcp_server

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'mcp.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: registered_tables.metadata.create_all(s, tables=_TABLES))
    db = Database(engine, name="mcp")
    try:
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
        state = types.SimpleNamespace(
            contexts={"analyst": object()},
            tenant_db=db,
            # REQ-1591: the search narrows to the domains the ROLE reaches; "*" is unlimited.
            roles={"analyst": {"domain_access": ["*"]}},
        )
        yield build_mcp_server(state), db
    finally:
        await engine.dispose()


async def test_search_terms_is_a_registered_tool(tmp_path):
    async with _server(tmp_path) as (server, _db):
        names = {t.name for t in await server.list_tools()}
        assert "search_terms" in names


async def test_search_terms_round_trips_through_the_protocol(tmp_path):
    async with _server(tmp_path) as (server, db):
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


async def test_unknown_role_fails_through_the_protocol(tmp_path):
    async with _server(tmp_path) as (server, _db):
        with pytest.raises(Exception, match="No schema for role"):
            await server.call_tool("search_terms", {"query": "customer", "role": "nobody"})
