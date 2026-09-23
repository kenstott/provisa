# Copyright (c) 2026 Kenneth Stott
# Canary: aee9e000-fab6-4027-942a-bbd7bc151b5f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-434/366: creation-request queue — repo, capability gating, payload round-trip."""

from __future__ import annotations

import dataclasses
import types
from unittest.mock import MagicMock

import pytest

from provisa.core.repositories import creation_request as cr_repo

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


class _Conn:
    """Vanilla SQLAlchemy Core connection stub."""

    def __init__(
        self, *, insert_returning=None, rows=None, row=None, rowcount=1, fetchrow_result=None
    ):
        self._insert_returning = insert_returning
        self._rows = rows or []
        self._row = row
        self._rowcount = rowcount
        self._fetchrow_result = fetchrow_result
        self.insert_calls: list[tuple] = []
        self.core_calls: list = []
        self.fetch_calls: list[tuple] = []

    async def insert_returning(self, table, values, returning="id"):
        self.insert_calls.append((table, values, returning))
        return self._insert_returning

    async def fetchrow(self, sql, *args):
        self.fetch_calls.append((sql, args))
        return self._fetchrow_result

    async def execute_core(self, stmt):
        self.core_calls.append(stmt)
        result = MagicMock()
        result.fetchall.return_value = [MagicMock(_mapping=r) for r in self._rows]
        result.fetchone.return_value = (
            MagicMock(_mapping=self._row) if self._row is not None else None
        )
        result.rowcount = self._rowcount
        return result


class TestRepo:
    async def test_create_returns_id_and_serializes_payload(self):
        conn = _Conn(insert_returning=7)
        rid = await cr_repo.create(
            conn, "relationship", "create_relationship", {"id": "r1"}, "alice"
        )
        assert rid == 7
        # payload is a native dict passed straight to the JSON column (no manual encoding)
        _, values, _ = conn.insert_calls[0]
        assert values["payload"] == {"id": "r1"}

    async def test_list_pending_decodes_payload(self):
        conn = _Conn(
            rows=[{"id": 1, "request_type": "view", "payload": {"a": 1}, "status": "pending"}]
        )
        out = await cr_repo.list_pending(conn)
        assert out[0]["payload"] == {"a": 1}

    async def test_mark_executed_true_on_update(self):
        conn = _Conn(rowcount=1)
        assert await cr_repo.mark_executed(conn, 1, "bob") is True

    async def test_mark_executed_false_when_not_pending(self):
        conn = _Conn(rowcount=0)
        assert await cr_repo.mark_executed(conn, 1, "bob") is False

    async def test_mark_rejected_records_reason(self):
        conn = _Conn(rowcount=1)
        assert await cr_repo.mark_rejected(conn, 1, "bad join", "bob") is True
        stmt = conn.core_calls[0]
        assert "bad join" in stmt.compile().params.values()


# --- REQ-209 config pre-approval gate --------------------------------------


class TestEnsureExecuted:
    """ensure_executed pre-approves config-declared webhooks so the exposure gate passes."""

    async def test_latest_status_returns_status(self):
        conn = _Conn(fetchrow_result={"status": "executed"})
        assert await cr_repo.latest_status(conn, "webhook", "add_pet") == "executed"
        # filters by request_type + payload name (mirrors the app_loaders exposure gate)
        _, args = conn.fetch_calls[0]
        assert args == ("webhook", "add_pet")

    async def test_latest_status_none_when_no_request(self):
        conn = _Conn(fetchrow_result=None)
        assert await cr_repo.latest_status(conn, "webhook", "add_pet") is None

    async def test_ensure_executed_skips_when_already_executed(self):
        conn = _Conn(fetchrow_result={"status": "executed"})
        await cr_repo.ensure_executed(conn, "webhook", "add_pet", "config")
        # idempotent: no new row inserted on restart when already approved
        assert conn.insert_calls == []

    async def test_ensure_executed_inserts_when_no_request(self):
        conn = _Conn(fetchrow_result=None, insert_returning=5)
        await cr_repo.ensure_executed(conn, "webhook", "add_pet", "config")
        assert len(conn.insert_calls) == 1
        _, values, _ = conn.insert_calls[0]
        assert values["status"] == "executed"
        assert values["payload"] == {"name": "add_pet"}
        assert values["resolved_by"] == "config"
        assert values["capability"] == "webhook_registration"

    async def test_ensure_executed_inserts_when_latest_pending(self):
        # a UI edit enqueued a pending request; config reload re-approves (trusted source)
        conn = _Conn(fetchrow_result={"status": "pending"}, insert_returning=6)
        await cr_repo.ensure_executed(conn, "webhook", "add_pet", "config")
        assert len(conn.insert_calls) == 1
        _, values, _ = conn.insert_calls[0]
        assert values["status"] == "executed"


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

        self._setup_roles(monkeypatch, {"steward": {"capabilities": ["create_relationship"]}})
        ident = types.SimpleNamespace(user_id="u1", roles=["steward"])
        assert has_capability(_info(ident), "create_relationship") is True
        assert has_capability(_info(ident), "create_view") is False


# --- payload round-trip ----------------------------------------------------


class TestPayloadRoundTrip:
    async def test_relationship_input_round_trips(self):
        from provisa.api.admin.schema import _rebuild_relationship_input
        from provisa.api.admin.types import RelationshipInput

        ri = RelationshipInput(
            id="r1",
            source_table_id="orders",
            target_table_id="customers",
            source_column="customer_id",
            target_column="id",
            cardinality="many-to-one",
        )
        rebuilt = _rebuild_relationship_input(dataclasses.asdict(ri))
        assert rebuilt.id == "r1"
        assert rebuilt.cardinality == "many-to-one"

    async def test_table_input_round_trips_with_nested_columns(self):
        from provisa.api.admin.schema import _rebuild_table_input
        from provisa.api.admin.types import ColumnInput, TableInput

        ti = TableInput(
            source_id="__derived__",
            domain_id="sales",
            schema_name="public",
            table_name="rev",
            columns=[ColumnInput(name="id", visible_to=["admin"])],
            view_sql="SELECT 1 AS id",
        )
        rebuilt = _rebuild_table_input(dataclasses.asdict(ti))
        assert rebuilt.table_name == "rev"
        assert rebuilt.view_sql == "SELECT 1 AS id"
        assert rebuilt.columns[0].name == "id"

    async def test_table_input_round_trips_with_unique_constraints(self):  # REQ-1792
        from provisa.api.admin.schema import _rebuild_table_input
        from provisa.api.admin.types import ColumnInput, TableInput, UniqueConstraintInput

        ti = TableInput(
            source_id="pg1",
            domain_id="sales",
            schema_name="public",
            table_name="orders",
            columns=[ColumnInput(name="id", visible_to=["admin"])],
            unique_constraints=[UniqueConstraintInput(name="uq_orders_id", columns=["id"])],
        )
        rebuilt = _rebuild_table_input(dataclasses.asdict(ti))
        assert rebuilt.unique_constraints[0].name == "uq_orders_id"
        assert rebuilt.unique_constraints[0].columns == ["id"]

    async def test_source_input_round_trips(self):  # REQ-1792
        from provisa.api.admin.schema import _rebuild_source_input
        from provisa.api.admin.types import SourceInput

        si = SourceInput(id="new_pg", type="postgresql", host="db.internal", port=5432)
        rebuilt = _rebuild_source_input(dataclasses.asdict(si))
        assert rebuilt.id == "new_pg"
        assert rebuilt.host == "db.internal"

    async def test_source_input_round_trips_with_cdc(self):  # REQ-1792
        from provisa.api.admin.schema import _rebuild_source_input
        from provisa.api.admin.types import SourceCdcConfigInput, SourceInput

        si = SourceInput(
            id="new_pg",
            type="postgresql",
            cdc=SourceCdcConfigInput(bootstrap_servers="kafka:9092", topic_prefix="pg."),
        )
        rebuilt = _rebuild_source_input(dataclasses.asdict(si))
        assert rebuilt.cdc.bootstrap_servers == "kafka:9092"


class TestRejectionReasons:  # REQ-1814
    """Every request_type a creation request can actually be stored with must have a non-empty
    rejection-reason list — an unlisted request_type resolves to [] and the Requests page's
    reject dialog has nothing to offer, silently blocking rejection outright."""

    @pytest.mark.parametrize(
        "request_type",
        [
            "relationship",  # provisa/api/admin/schema_mutation.py
            "view",  # provisa/api/admin/schema_mutation_ops.py
            "webhook",  # provisa/api/admin/actions_router.py — NOT "webhook_registration"
            "source",  # provisa/api/mcp/tools.py propose_source (REQ-1792/1798)
            "table",  # provisa/api/mcp/tools.py propose_table (REQ-1792/1798)
        ],
    )
    async def test_every_real_request_type_has_rejection_reasons(self, request_type):
        from provisa.api.admin.creation_requests_router import _REJECTION_REASONS

        assert _REJECTION_REASONS.get(request_type)
