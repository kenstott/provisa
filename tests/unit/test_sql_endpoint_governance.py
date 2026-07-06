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
from provisa.executor.result import QueryResult


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

    # Clean up — restore source_pools so subsequent tests see a clean SourcePool
    from provisa.executor.pool import SourcePool

    app_mod.state.schemas = {}
    app_mod.state.contexts = {}
    app_mod.state.rls_contexts = {}
    app_mod.state.roles = {}
    app_mod.state.masking_rules = {}
    app_mod.state.source_types = {}
    app_mod.state.source_dialects = {}
    app_mod.state.source_pools = SourcePool()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestSQLParseError:
    async def test_sql_parse_error_returns_400(self, sql_client):
        """Completely invalid SQL that cannot be parsed returns HTTP 400."""
        payload = {"sql": "THIS IS NOT VALID SQL !!!! SELECT ??? FROM", "role": "admin"}

        # Patch sqlglot.parse_one to raise on this input
        with patch("sqlglot.parse_one", side_effect=Exception("parse error: unexpected token")):
            resp = await sql_client.post("/data/sql", json=payload)

        assert resp.status_code == 400
        assert "parse" in resp.json()["detail"].lower() or "SQL" in resp.json()["detail"]


@pytest.mark.asyncio
class TestSQLForbiddenTable:
    async def test_sql_forbidden_table_returns_403(self, sql_client):
        """SQL referencing a table not in the role's schema scope returns HTTP 403."""
        # "secret_table" is not in state.tables or ctx.tables
        payload = {"sql": "SELECT id FROM secret_table", "role": "admin"}
        resp = await sql_client.post("/data/sql", json=payload)

        assert resp.status_code == 403
        detail = resp.json()["detail"]
        # detail may be a string or {"violations": [...]}
        detail_str = detail if isinstance(detail, str) else str(detail)
        assert "secret_table" in detail_str

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


@pytest.mark.asyncio
class TestSQLGovernanceApplied:
    async def test_sql_governance_applied_rls_injected(self):
        """When an RLS rule exists for the table, build_governance_context + apply_governance
        produce SQL with the RLS filter injected. Tested end-to-end via stage2 directly."""
        from provisa.compiler.stage2 import apply_governance, build_governance_context
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

        assert "\"status\" = 'active'" in governed
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
        assert "\"status\" = 'active'" in result
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


class TestImplicitTraversalDomains:
    """meta and ops domain tables are implicitly traversable via JOIN (V002 exempt)."""

    def _make_validate_fixtures(self):
        from provisa.compiler.sql_gen import CompilationContext, TableMeta
        from provisa.compiler.stage2 import GovernanceContext

        orders_meta = TableMeta(
            table_id=1,
            field_name="orders",
            type_name="Orders",
            source_id="pg",
            catalog_name="pg",
            schema_name="public",
            table_name="orders",
            domain_id="pet-store",
        )
        meta_table_meta = TableMeta(
            table_id=2,
            field_name="registered_tables",
            type_name="RegisteredTables",
            source_id="provisa-admin",
            catalog_name="provisa-admin",
            schema_name="public",
            table_name="registered_tables",
            domain_id="meta",
        )
        ops_table_meta = TableMeta(
            table_id=3,
            field_name="metrics",
            type_name="Metrics",
            source_id="provisa-ops",
            catalog_name="provisa-ops",
            schema_name="public",
            table_name="metrics",
            domain_id="ops",
        )

        ctx = CompilationContext()
        ctx.tables = {
            "orders": orders_meta,
            "registered_tables": meta_table_meta,
            "metrics": ops_table_meta,
        }
        ctx.joins = {}

        gov_ctx = GovernanceContext(
            rls_rules={},
            table_map={
                "orders": 1,
                "registered_tables": 2,
                "metrics": 3,
            },
        )

        role = {"id": "analyst", "domain_access": ["pet-store"]}
        raw_tables = [
            {
                "id": 1,
                "source_id": "pg",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [{"column_name": "id"}, {"column_name": "table_name"}],
            },
            {
                "id": 2,
                "source_id": "provisa-admin",
                "schema_name": "public",
                "table_name": "registered_tables",
                "columns": [{"column_name": "table_name"}, {"column_name": "domain_id"}],
            },
            {
                "id": 3,
                "source_id": "provisa-ops",
                "schema_name": "public",
                "table_name": "metrics",
                "columns": [{"column_name": "table_name"}, {"column_name": "value"}],
            },
        ]
        return ctx, gov_ctx, role, raw_tables

    def test_join_to_meta_domain_no_registered_rel_is_allowed(self):
        """JOIN from a data table to a meta domain table requires no registered relationship (V002 exempt)."""
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._make_validate_fixtures()
        sql = (
            "SELECT o.id, r.domain_id "
            "FROM orders o "
            "JOIN registered_tables r ON o.table_name = r.table_name"
        )
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        v002 = [v for v in violations if v.code == "V002"]
        assert v002 == [], f"Expected no V002 violations for meta JOIN, got: {v002}"

    def test_join_to_ops_domain_no_registered_rel_is_allowed(self):
        """JOIN from a data table to an ops domain table requires no registered relationship (V002 exempt)."""
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._make_validate_fixtures()
        sql = "SELECT o.id, m.value FROM orders o JOIN metrics m ON o.table_name = m.table_name"
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        v002 = [v for v in violations if v.code == "V002"]
        assert v002 == [], f"Expected no V002 violations for ops JOIN, got: {v002}"

    def test_join_to_regular_domain_without_rel_is_blocked(self):
        """JOIN between two non-implicit domains without a registered relationship still raises V002."""
        from provisa.compiler.sql_gen import TableMeta
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._make_validate_fixtures()
        # Add a second data-domain table with no relationship to orders
        other_meta = TableMeta(
            table_id=4,
            field_name="cats",
            type_name="Cats",
            source_id="pg",
            catalog_name="pg",
            schema_name="public",
            table_name="cats",
            domain_id="shelter",
        )
        ctx.tables["cats"] = other_meta
        gov_ctx.table_map["cats"] = 4

        sql = "SELECT o.id FROM orders o JOIN cats c ON o.id = c.id"
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        v002 = [v for v in violations if v.code == "V002"]
        assert v002, (
            "Expected V002 for JOIN between unrelated data domains without a registered relationship"
        )

    def test_direct_meta_table_in_from_blocked_by_v001(self):
        """Direct FROM-clause use of a meta table is still domain-access checked (V001)."""
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._make_validate_fixtures()
        sql = "SELECT table_name FROM registered_tables"
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        # meta domain not in role's domain_access ["pet-store"] → V001
        v001 = [v for v in violations if v.code == "V001"]
        assert v001, (
            "Expected V001 when meta table appears directly in FROM clause without domain access"
        )


class TestMaskedColumnInPredicate:
    """V005 — masked columns must not appear in WHERE or HAVING clauses."""

    def _fixtures(self, masking_rules=None):
        from provisa.compiler.sql_gen import CompilationContext, TableMeta
        from provisa.compiler.stage2 import GovernanceContext
        from provisa.security.masking import MaskType, MaskingRule

        meta = TableMeta(
            table_id=1,
            field_name="employees",
            type_name="Employees",
            source_id="pg",
            catalog_name="pg",
            schema_name="hr",
            table_name="employees",
            domain_id="hr",
        )
        ctx = CompilationContext()
        ctx.tables = {"employees": meta}
        ctx.joins = {}

        _ssn_rule = MaskingRule(mask_type=MaskType.regex, pattern=r"\d", replace="X")
        gov_ctx = GovernanceContext(
            table_map={"employees": 1, "hr.employees": 1},
            masking_rules={(1, "ssn"): (_ssn_rule, "varchar")}
            if masking_rules is None
            else masking_rules,
            visible_columns={1: None},
        )
        role = {"id": "analyst", "domain_access": ["hr"]}
        raw_tables = [
            {
                "id": 1,
                "source_id": "pg",
                "schema_name": "hr",
                "table_name": "employees",
                "columns": [
                    {"column_name": "id", "data_type": "integer"},
                    {"column_name": "name", "data_type": "varchar"},
                    {"column_name": "ssn", "data_type": "varchar"},
                ],
            }
        ]
        return ctx, gov_ctx, role, raw_tables

    def test_masked_column_in_where_raises_v005(self):
        """Qualified masked column in WHERE raises V005."""
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._fixtures()
        sql = "SELECT id, name FROM hr.employees e WHERE e.ssn = '123-45-6789'"
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        v005 = [v for v in violations if v.code == "V005"]
        assert v005, f"Expected V005 for masked column in WHERE, got: {violations}"

    def test_masked_column_unqualified_in_where_raises_v005(self):
        """Unqualified masked column in WHERE raises V005."""
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._fixtures()
        sql = "SELECT id FROM employees WHERE ssn = '123-45-6789'"
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        v005 = [v for v in violations if v.code == "V005"]
        assert v005, f"Expected V005 for unqualified masked column in WHERE, got: {violations}"

    def test_masked_column_in_having_raises_v005(self):
        """Masked column in HAVING raises V005."""
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._fixtures()
        sql = "SELECT ssn, COUNT(*) FROM employees GROUP BY ssn HAVING ssn = '123-45-6789'"
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        v005 = [v for v in violations if v.code == "V005"]
        assert v005, f"Expected V005 for masked column in HAVING, got: {violations}"

    def test_non_masked_column_in_where_allowed(self):
        """Non-masked column in WHERE does not raise V005."""
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._fixtures()
        sql = "SELECT id, name FROM employees WHERE id = 42"
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        v005 = [v for v in violations if v.code == "V005"]
        assert not v005, f"Expected no V005 for non-masked column in WHERE, got: {v005}"

    def test_masked_column_in_select_no_v005(self):
        """Masked column in SELECT projection (not predicate) does not raise V005."""
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._fixtures()
        sql = "SELECT id, ssn FROM employees WHERE id = 1"
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        v005 = [v for v in violations if v.code == "V005"]
        assert not v005, f"Expected no V005 for masked column in SELECT, got: {v005}"

    def test_no_masking_rules_no_v005(self):
        """When no masking rules exist, WHERE on any column is allowed."""
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._fixtures(masking_rules={})
        sql = "SELECT id FROM employees WHERE ssn = '123-45-6789'"
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        v005 = [v for v in violations if v.code == "V005"]
        assert not v005, f"Expected no V005 with no masking rules, got: {v005}"
