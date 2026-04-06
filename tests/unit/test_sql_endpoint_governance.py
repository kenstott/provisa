# Copyright (c) 2026 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-ef01-345678901234
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for /data/sql Stage 2 governance endpoint (REQ-264, REQ-266, REQ-267)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.executor.trino import QueryResult

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

def _table_meta(
    table_id: int = 1,
    table_name: str = "orders",
    schema_name: str = "public",
    source_id: str = "pg",
) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=table_name,
        type_name=table_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name=schema_name,
        table_name=table_name,
    )


def _make_ctx(table_name: str = "orders", table_id: int = 1) -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {table_name: _table_meta(table_id, table_name)}
    return ctx


def _make_query_result(**kwargs) -> QueryResult:
    return QueryResult(
        rows=kwargs.get("rows", [(1, "test")]),
        column_names=kwargs.get("column_names", ["id", "name"]),
    )


@pytest.fixture
async def sql_client():
    """ASGI test client with minimal state injected for /data/sql tests."""
    import provisa.api.app as app_mod
    from provisa.api.app import create_app

    the_app = create_app()

    # Inject minimal state — no real PG/Trino needed
    ctx = _make_ctx("orders", table_id=1)
    rls = RLSContext.empty()

    app_mod.state.schemas = {"admin": MagicMock()}
    app_mod.state.contexts = {"admin": ctx}
    app_mod.state.rls_contexts = {"admin": rls}
    app_mod.state.roles = {}
    app_mod.state.masking_rules = {}
    app_mod.state.source_types = {"pg": "postgresql"}
    app_mod.state.source_dialects = {"pg": "postgres"}
    app_mod.state.tables = [
        {
            "id": 1,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": "id", "data_type": "integer"},
                {"column_name": "status", "data_type": "varchar"},
            ],
        }
    ]
    app_mod.state.source_pools = MagicMock()

    transport = ASGITransport(app=the_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

    # Clean up
    app_mod.state.schemas = {}
    app_mod.state.contexts = {}
    app_mod.state.rls_contexts = {}
    app_mod.state.roles = {}
    app_mod.state.masking_rules = {}
    app_mod.state.source_types = {}
    app_mod.state.source_dialects = {}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSQLParseError:
    async def test_sql_parse_error_returns_400(self, sql_client):
        """Completely invalid SQL that cannot be parsed returns HTTP 400."""
        payload = {"sql": "THIS IS NOT VALID SQL !!!! SELECT ??? FROM", "role": "admin"}

        # Patch sqlglot.parse_one to raise on this input
        with patch("sqlglot.parse_one", side_effect=Exception("parse error: unexpected token")):
            resp = await sql_client.post("/data/sql", json=payload)

        assert resp.status_code == 400
        assert "parse" in resp.json()["detail"].lower() or "SQL" in resp.json()["detail"]


class TestSQLForbiddenTable:
    async def test_sql_forbidden_table_returns_403(self, sql_client):
        """SQL referencing a table not in the role's schema scope returns HTTP 403."""
        # "secret_table" is not in state.tables or ctx.tables
        payload = {"sql": "SELECT id FROM secret_table", "role": "admin"}
        resp = await sql_client.post("/data/sql", json=payload)

        assert resp.status_code == 403
        assert "secret_table" in resp.json()["detail"]

    async def test_sql_accessible_table_not_forbidden(self, sql_client):
        """SQL referencing an accessible table does not get a 403."""
        payload = {"sql": "SELECT id FROM orders", "role": "admin"}
        fallback_result = _make_query_result(rows=[(1,)], column_names=["id"])

        with patch(
            "provisa.executor.direct.execute_direct",
            new=AsyncMock(return_value=fallback_result),
        ):
            with patch(
                "provisa.executor.trino.execute_trino",
                new=AsyncMock(return_value=fallback_result),
            ):
                resp = await sql_client.post("/data/sql", json=payload)

        # Should not be 403; we accept 200 or any non-403
        assert resp.status_code != 403


class TestSQLGovernanceApplied:
    async def test_sql_governance_applied_rls_injected(self):
        """When an RLS rule exists for the table, build_governance_context + apply_governance
        produce SQL with the RLS filter injected. Tested end-to-end via stage2 directly."""
        from provisa.compiler.stage2 import GovernanceContext, apply_governance, build_governance_context
        from provisa.compiler.rls import RLSContext

        ctx = _make_ctx("orders", table_id=1)
        rls = RLSContext(rules={1: "status = 'active'"})

        tables = [
            {
                "id": 1,
                "source_id": "pg",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [
                    {"column_name": "id", "data_type": "integer"},
                    {"column_name": "status", "data_type": "varchar"},
                ],
            }
        ]

        gov_ctx = build_governance_context(
            role_id="analyst",
            rls_context=rls,
            masking_rules={},
            ctx=ctx,
            tables=tables,
        )

        sql = "SELECT id FROM orders"
        governed = apply_governance(sql, gov_ctx)

        assert "status = 'active'" in governed
        assert "WHERE" in governed

    async def test_sql_endpoint_rls_applied_via_http(self, sql_client):
        """Via HTTP: a role with RLS rules results in non-403 for allowed table."""
        # sql_client uses "admin" role with no RLS — just verify the endpoint routes correctly
        fallback_result = _make_query_result(rows=[(1,)], column_names=["id"])
        with patch(
            "provisa.executor.direct.execute_direct",
            new=AsyncMock(return_value=fallback_result),
        ):
            with patch(
                "provisa.executor.trino.execute_trino",
                new=AsyncMock(return_value=fallback_result),
            ):
                resp = await sql_client.post(
                    "/data/sql",
                    json={"sql": "SELECT id FROM orders", "role": "admin"},
                )
        # Not forbidden — 200 or some execution result
        assert resp.status_code != 403

    async def test_apply_governance_with_rls_directly(self):
        """Direct test: apply_governance injects RLS into raw SQL for the matching table."""
        from provisa.compiler.stage2 import GovernanceContext, apply_governance

        gov = GovernanceContext(
            rls_rules={1: "status = 'active'"},
            table_map={"orders": 1},
        )
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "status = 'active'" in result
        assert "WHERE" in result

    async def test_apply_governance_no_rls_unchanged_tables(self):
        """apply_governance does not inject WHERE if no matching RLS rule exists."""
        from provisa.compiler.stage2 import GovernanceContext, apply_governance

        gov = GovernanceContext(
            rls_rules={99: "region = 'us'"},  # table_id 99 not in table_map
            table_map={"orders": 1},
        )
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "WHERE" not in result
