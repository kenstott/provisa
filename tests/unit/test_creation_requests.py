# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-434/366: creation-request queue — repo, capability gating, payload round-trip."""

from __future__ import annotations

import dataclasses
import json
import types

import pytest

from provisa.core.repositories import creation_request as cr_repo

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


class _Conn:
    def __init__(self, *, fetchval=None, rows=None, row=None, execute="UPDATE 1"):
        self._fetchval = fetchval
        self._rows = rows or []
        self._row = row
        self._execute = execute
        self.calls: list[tuple] = []

    async def fetchval(self, query, *args):
        self.calls.append(("fetchval", query, args))
        return self._fetchval

    async def fetch(self, query, *args):
        self.calls.append(("fetch", query, args))
        return self._rows

    async def fetchrow(self, query, *args):
        self.calls.append(("fetchrow", query, args))
        return self._row

    async def execute(self, query, *args):
        self.calls.append(("execute", query, args))
        return self._execute


class TestRepo:
    async def test_create_returns_id_and_serializes_payload(self):
        conn = _Conn(fetchval=7)
        rid = await cr_repo.create(conn, "relationship", "create_relationship", {"id": "r1"}, "alice")
        assert rid == 7
        # payload is JSON-encoded for the jsonb column
        _, _, args = conn.calls[0]
        assert json.loads(args[2]) == {"id": "r1"}

    async def test_list_pending_decodes_payload(self):
        conn = _Conn(rows=[{"id": 1, "request_type": "view", "payload": '{"a": 1}', "status": "pending"}])
        out = await cr_repo.list_pending(conn)
        assert out[0]["payload"] == {"a": 1}

    async def test_mark_executed_true_on_update(self):
        conn = _Conn(execute="UPDATE 1")
        assert await cr_repo.mark_executed(conn, 1, "bob") is True

    async def test_mark_executed_false_when_not_pending(self):
        conn = _Conn(execute="UPDATE 0")
        assert await cr_repo.mark_executed(conn, 1, "bob") is False

    async def test_mark_rejected_records_reason(self):
        conn = _Conn(execute="UPDATE 1")
        assert await cr_repo.mark_rejected(conn, 1, "bad join", "bob") is True
        _, _, args = conn.calls[0]
        assert "bad join" in args


# --- has_capability gating -------------------------------------------------


def _info(identity):
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))
    return types.SimpleNamespace(context={"request": request})


class TestHasCapability:
    def _setup_roles(self, monkeypatch, roles):
        import provisa.api.app as appmod

        monkeypatch.setattr(appmod.state, "roles", roles)

    async def test_dev_mode_anonymous_is_authorized(self, monkeypatch):
        from provisa.api.admin.capabilities import has_capability

        self._setup_roles(monkeypatch, {})
        anon = types.SimpleNamespace(user_id="anonymous", roles=[])
        assert has_capability(_info(anon), "create_relationship") is True

    async def test_admin_bypasses(self, monkeypatch):
        from provisa.api.admin.capabilities import has_capability

        self._setup_roles(monkeypatch, {"admin": {"capabilities": ["admin"]}})
        ident = types.SimpleNamespace(user_id="u1", roles=["admin"])
        assert has_capability(_info(ident), "create_view") is True

    async def test_holder_true_nonholder_false(self, monkeypatch):
        from provisa.api.admin.capabilities import has_capability

        self._setup_roles(
            monkeypatch, {"steward": {"capabilities": ["create_relationship"]}}
        )
        ident = types.SimpleNamespace(user_id="u1", roles=["steward"])
        assert has_capability(_info(ident), "create_relationship") is True
        assert has_capability(_info(ident), "create_view") is False


# --- payload round-trip ----------------------------------------------------


class TestPayloadRoundTrip:
    async def test_relationship_input_round_trips(self):
        from provisa.api.admin.schema import _rebuild_relationship_input
        from provisa.api.admin.types import RelationshipInput

        ri = RelationshipInput(
            id="r1", source_table_id="orders", target_table_id="customers",
            source_column="customer_id", target_column="id", cardinality="many-to-one",
        )
        rebuilt = _rebuild_relationship_input(dataclasses.asdict(ri))
        assert rebuilt.id == "r1"
        assert rebuilt.cardinality == "many-to-one"

    async def test_table_input_round_trips_with_nested_columns(self):
        from provisa.api.admin.schema import _rebuild_table_input
        from provisa.api.admin.types import ColumnInput, TableInput

        ti = TableInput(
            source_id="__provisa__", domain_id="sales", schema_name="public",
            table_name="rev", columns=[ColumnInput(name="id", visible_to=["admin"])],
            view_sql="SELECT 1 AS id",
        )
        rebuilt = _rebuild_table_input(dataclasses.asdict(ti))
        assert rebuilt.table_name == "rev"
        assert rebuilt.view_sql == "SELECT 1 AS id"
        assert rebuilt.columns[0].name == "id"
