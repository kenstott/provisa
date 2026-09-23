# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4c7e21-5b3a-4f8e-a1c6-2d9f5e3b7a04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1792: MCP propose_source/propose_table — discover+propose, human approves.

Both tools queue a pending creation request (REQ-434's queue) by default, regardless of what
capability the calling role holds — approval goes through the existing
execute_creation_request/reject_creation_request admin mutations. REQ-1799 is the one exception:
when the caller's role already holds the needed capability AND a verified `request` is passed in,
these return a `confirm_required` result instead of queuing (see test_mcp_org_scoped_config-style
tests in test_mcp_chat.py's TestSubscriptionSources for the direct-create dispatch path,
create_source_now/register_table_now). Passing no `request` (the default, and every test below
that doesn't set one) always falls back to the original queue-only behavior.
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

    async def test_role_with_capability_and_request_gets_confirm_required_not_queued(
        self, tmp_path
    ):  # REQ-1799
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            state = _state(db)
            state.roles = {"analyst": {"capabilities": ["source_registration"]}}
            result = await tools.propose_source(
                state,
                "analyst",
                {"id": "new_pg", "type": "postgresql"},
                "found via network scan",
                request=object(),
            )
            assert result["status"] == "confirm_required"
            assert result["capability"] == "source_registration"

            async with db.acquire() as conn:
                pending = await cr_repo.list_pending(conn)
            assert pending == []  # nothing queued — the human confirms in-chat instead

    async def test_all_role_gets_confirm_required_via_any_held_assignment(
        self, tmp_path
    ):  # REQ-1799
        # The UI's "Role: All" means every role GRANTED to the user, unioned — its single
        # x-provisa-role header collapses to whichever assignment is first client-side (often a
        # control-plane role like platform_admin, which correctly holds NO data capability), even
        # when the person also holds org_admin. The pinned `role` string alone can't see that; the
        # caller's real request.state.assignments (AuthMiddleware-verified) can.
        import types as _types

        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            state = _state(db)
            state.contexts["platform_admin"] = object()
            state.roles = {
                "platform_admin": {"capabilities": []},
                "org_admin": {"capabilities": ["source_registration"]},
            }
            fake_request = _types.SimpleNamespace(
                state=_types.SimpleNamespace(
                    assignments=[
                        _types.SimpleNamespace(role_id="platform_admin", domain_id="*"),
                        _types.SimpleNamespace(role_id="org_admin", domain_id="*"),
                    ]
                )
            )
            # The pinned MCP role is "platform_admin" (activeRoles[0], the first assignment) —
            # exactly what the UI's "All" mode sends as x-provisa-role — but the request carries
            # BOTH assignments, so the union correctly finds org_admin's capability.
            result = await tools.propose_source(
                state,
                "platform_admin",
                {"id": "new_pg", "type": "postgresql"},
                "found via network scan",
                request=fake_request,
            )
            assert result["status"] == "confirm_required"

    async def test_role_with_capability_but_no_request_still_queues(self, tmp_path):  # REQ-1799
        # No verified request context (e.g. a non-HTTP caller) — always falls back to the queue,
        # even if the role would otherwise qualify for the confirm_required shortcut.
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            state = _state(db)
            state.roles = {"analyst": {"capabilities": ["source_registration"]}}
            result = await tools.propose_source(
                state, "analyst", {"id": "new_pg", "type": "postgresql"}, "found via network scan"
            )
            assert result["status"] == "pending"

    async def test_role_without_capability_queues_even_with_request(self, tmp_path):  # REQ-1799
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            state = _state(db)
            state.roles = {"analyst": {"capabilities": []}}
            result = await tools.propose_source(
                state,
                "analyst",
                {"id": "new_pg", "type": "postgresql"},
                "found via network scan",
                request=object(),
            )
            assert result["status"] == "pending"


class TestCreateSourceNow:  # REQ-1799
    async def test_rejects_role_without_capability(self, tmp_path):
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            state = _state(db)
            state.roles = {"analyst": {"capabilities": []}}
            with pytest.raises(PermissionError):
                await tools.create_source_now(
                    state,
                    "analyst",
                    {"id": "x", "type": "postgresql"},
                    "why",
                    request=object(),
                )

    async def test_calls_the_same_graphql_resolver_create_source_uses(self, tmp_path, monkeypatch):
        from provisa.api.mcp import tools

        seen = {}

        class _FakeMutationResult:
            success = True
            message = "created"
            code = "schema.source_created"

        class _FakeMutation:
            async def create_source(self, info, input):
                seen["info"] = info
                seen["input"] = input
                return _FakeMutationResult()

        monkeypatch.setattr("provisa.api.admin.schema_mutation.Mutation", _FakeMutation)

        async with _db(tmp_path) as db:
            state = _state(db)
            state.roles = {"analyst": {"capabilities": ["source_registration"]}}
            sentinel_request = object()
            result = await tools.create_source_now(
                state,
                "analyst",
                {"id": "new_pg", "type": "postgresql"},
                "user confirmed in chat",
                request=sentinel_request,
            )
            assert result == {
                "success": True,
                "message": "created",
                "code": "schema.source_created",
            }
            assert seen["info"].context["request"] is sentinel_request
            assert seen["input"].id == "new_pg"

    async def test_reason_is_required(self, tmp_path):
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            state = _state(db)
            state.roles = {"analyst": {"capabilities": ["source_registration"]}}
            with pytest.raises(ValueError, match="reason"):
                await tools.create_source_now(
                    state, "analyst", {"id": "x", "type": "postgresql"}, "", request=object()
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

    async def test_role_with_capability_and_request_gets_confirm_required_not_queued(
        self, tmp_path
    ):  # REQ-1799
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            state = _state(db)
            state.roles = {"analyst": {"capabilities": ["table_registration"]}}
            result = await tools.propose_table(
                state,
                "analyst",
                {
                    "source_id": "pg1",
                    "domain_id": "sales",
                    "schema_name": "public",
                    "table_name": "orders",
                    "columns": [{"name": "id", "visible_to": ["admin"]}],
                },
                "discovered during crawl",
                request=object(),
            )
            assert result["status"] == "confirm_required"
            assert result["capability"] == "table_registration"

            async with db.acquire() as conn:
                pending = await cr_repo.list_pending(conn)
            assert pending == []


class TestRegisterTableNow:  # REQ-1799
    async def test_rejects_role_without_capability(self, tmp_path):
        from provisa.api.mcp import tools

        async with _db(tmp_path) as db:
            state = _state(db)
            state.roles = {"analyst": {"capabilities": []}}
            with pytest.raises(PermissionError):
                await tools.register_table_now(
                    state,
                    "analyst",
                    {
                        "source_id": "pg1",
                        "domain_id": "sales",
                        "schema_name": "public",
                        "table_name": "orders",
                        "columns": [{"name": "id", "visible_to": ["admin"]}],
                    },
                    "why",
                    request=object(),
                )

    async def test_calls_the_same_graphql_resolver_register_table_uses(self, tmp_path, monkeypatch):
        from provisa.api.mcp import tools

        seen = {}

        class _FakeMutationResult:
            success = True
            message = "registered"
            code = "schema.table_registered"

        class _FakeMutation:
            async def register_table(self, info, input):
                seen["info"] = info
                seen["input"] = input
                return _FakeMutationResult()

        monkeypatch.setattr("provisa.api.admin.schema_mutation.Mutation", _FakeMutation)

        async with _db(tmp_path) as db:
            state = _state(db)
            state.roles = {"analyst": {"capabilities": ["table_registration"]}}
            sentinel_request = object()
            result = await tools.register_table_now(
                state,
                "analyst",
                {
                    "source_id": "pg1",
                    "domain_id": "sales",
                    "schema_name": "public",
                    "table_name": "orders",
                    "columns": [{"name": "id", "visible_to": ["admin"]}],
                },
                "user confirmed in chat",
                request=sentinel_request,
            )
            assert result == {
                "success": True,
                "message": "registered",
                "code": "schema.table_registered",
            }
            assert seen["info"].context["request"] is sentinel_request
            assert seen["input"].table_name == "orders"


class TestProposeToolsRegistered:
    async def test_both_tools_are_registered_on_the_mcp_server(self, tmp_path):
        from provisa.api.mcp.server import build_mcp_server

        async with _db(tmp_path) as db:
            server = build_mcp_server(_state(db))
            names = {t.name for t in await server.list_tools()}
            assert {"propose_source", "propose_table"} <= names
