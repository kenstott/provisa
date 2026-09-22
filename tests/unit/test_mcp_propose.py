# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4c7e21-5b3a-4f8e-a1c6-2d9f5e3b7a04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1792: MCP propose_source/propose_table — discover+propose, human approves.

Both tools must ONLY ever queue a pending creation request (REQ-434's queue), never create the
live Source/Table directly, regardless of what capability the calling role holds. Approval still
goes through the existing execute_creation_request/reject_creation_request admin mutations.
"""

from __future__ import annotations

import types
from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.repositories import creation_request as cr_repo
from provisa.core.schema_org import creation_requests

pytestmark = pytest.mark.asyncio

_TABLES = [creation_requests]


@asynccontextmanager
async def _db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'propose.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: creation_requests.metadata.create_all(s, tables=_TABLES))
    db = Database(engine, name="propose")
    try:
        yield db
    finally:
        await engine.dispose()


def _state(db: Database) -> types.SimpleNamespace:
    return types.SimpleNamespace(contexts={"analyst": object()}, tenant_db=db)


class TestProposeSource:
    async def test_queues_pending_request_never_creates_live_source(self, tmp_path):
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            result = await tools.propose_source(
                _state(db),
                "analyst",
                {"id": "new_pg", "type": "postgresql", "host": "db.internal", "port": 5432},
                "found via network scan",
            )
            assert result["status"] == "pending"
            assert isinstance(result["request_id"], int)

            async with db.acquire() as conn:
                pending = await cr_repo.list_pending(conn)
            assert len(pending) == 1
            row = pending[0]
            assert row["request_type"] == "source"
            assert row["capability"] == "source_registration"
            assert row["payload"]["id"] == "new_pg"
            assert row["payload"]["_proposed_reason"] == "found via network scan"
            assert row["payload"]["_proposed_via"] == "mcp"
            assert row["requested_by"] == "analyst"

    async def test_rejects_missing_required_fields(self, tmp_path):
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            with pytest.raises(ValueError):
                await tools.propose_source(_state(db), "analyst", {"host": "db.internal"}, "why")

    async def test_reason_is_required(self, tmp_path):
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            with pytest.raises(ValueError, match="reason"):
                await tools.propose_source(
                    _state(db), "analyst", {"id": "x", "type": "postgresql"}, ""
                )

    async def test_role_is_required(self, tmp_path):
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            with pytest.raises(ValueError):
                await tools.propose_source(_state(db), "", {"id": "x", "type": "postgresql"}, "y")

    async def test_unknown_role_rejected(self, tmp_path):
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            with pytest.raises(PermissionError):
                await tools.propose_source(
                    _state(db), "nobody", {"id": "x", "type": "postgresql"}, "y"
                )


class TestProposeTable:
    async def test_queues_pending_request_with_columns(self, tmp_path):
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            result = await tools.propose_table(
                _state(db),
                "analyst",
                {
                    "source_id": "pg1",
                    "domain_id": "sales",
                    "schema_name": "public",
                    "table_name": "orders",
                    "columns": [{"name": "id", "visible_to": ["admin"]}],
                },
                "discovered during crawl",
            )
            assert result["status"] == "pending"

            async with db.acquire() as conn:
                pending = await cr_repo.list_pending(conn)
            assert len(pending) == 1
            row = pending[0]
            assert row["request_type"] == "table"
            assert row["capability"] == "table_registration"
            assert row["payload"]["table_name"] == "orders"
            assert row["payload"]["columns"][0]["name"] == "id"

    async def test_rejects_malformed_columns(self, tmp_path):
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            with pytest.raises(ValueError):
                await tools.propose_table(
                    _state(db),
                    "analyst",
                    {
                        "source_id": "pg1",
                        "domain_id": "sales",
                        "schema_name": "public",
                        "table_name": "orders",
                        "columns": [{"not_a_real_field": True}],
                    },
                    "why",
                )


class TestProposeToolsRegistered:
    async def test_both_tools_are_registered_on_the_mcp_server(self, tmp_path):
        from provisa.api.mcp.server import build_mcp_server

        async with _db(tmp_path) as db:
            server = build_mcp_server(_state(db))
            names = {t.name for t in await server.list_tools()}
            assert {"propose_source", "propose_table"} <= names
