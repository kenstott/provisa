# Copyright (c) 2026 Kenneth Stott
# Canary: 7dff8727-9ce1-4e2f-ac89-e73c400733e7
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
import sqlglot.errors
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


AUDIT_ROWS: list[dict] = []  # query_audit_log appends the endpoint made, newest last


def _make_query_result(**kwargs) -> QueryResult:
    return QueryResult(
        rows=kwargs.get("rows", [(1, "test")]),
        column_names=kwargs.get("column_names", ["id", "name"]),
    )


@pytest.fixture
async def sql_client(monkeypatch):
    """ASGI test client with minimal state injected for /data/sql tests."""
    import provisa.api.app as app_mod
    from provisa.api.app import create_app

    # REQ-074/REQ-1386: a governance refusal appends a 403 row to query_audit_log in the org's
    # tenant database — that row is what the policy_denials report reads. Stand in for the tenant
    # database and record the appends so a test can assert the refusal was recorded.
    # The row is inserted by the audit writer's thread (provisa.audit.writer), so a test waits for
    # it with flush_audit before reading AUDIT_ROWS.
    AUDIT_ROWS.clear()

    async def _log_queries(pool, rows):
        AUDIT_ROWS.extend(rows)

    monkeypatch.setattr("provisa.audit.query_log.log_queries", _log_queries)
    _prev_tenant_db = app_mod.state.tenant_db
    app_mod.state.tenant_db = MagicMock()

    # Pin unsecured auth BEFORE create_app(): wire_auth reads state.auth_config at
    # app-construction time. These governance tests post a `role` with no bearer
    # token, so a leaked secured auth_config from a prior test would install a
    # token-enforcing AuthMiddleware and every request would 401 before reaching
    # the governance checks under test (isolation bug: passed alone, failed in
    # suite). None = provider:none = unsecured (honors the body/x-provisa-role).
    _prev_auth_config = getattr(app_mod.state, "auth_config", None)
    app_mod.state.auth_config = None

    the_app = create_app()

    # Inject minimal state — no real PG/Trino needed
    ctx = _make_ctx("orders", table_id=1)
    rls = RLSContext.empty()

    app_mod.state.schemas = {"org_admin": MagicMock()}
    app_mod.state.contexts = {"org_admin": ctx}
    app_mod.state.rls_contexts = {"org_admin": rls}
    # Role must exist in state.roles or the rate-limit middleware rejects it as unknown
    # (REQ-369). admin capability keeps the endpoint's QUERY_DEVELOPMENT check passing;
    # empty domain_access preserves the table-access (V000) governance under test.
    app_mod.state.roles = {
        "org_admin": {
            "id": "org_admin",
            "capabilities": ["query_development", "full_results", "usage", "write"],
            "domain_access": ["*"],  # the seeded org_admin: every domain, said explicitly
        }
    }
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

    app_mod.state.auth_config = _prev_auth_config
    app_mod.state.tenant_db = _prev_tenant_db
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
        payload = {"sql": "THIS IS NOT VALID SQL !!!! SELECT ??? FROM", "role": "org_admin"}

        # Patch sqlglot.parse_one to raise the SqlglotError real sqlglot raises on unparseable input
        # (the endpoint narrows its 400 catch to SqlglotError, not a blanket Exception).
        with patch(
            "sqlglot.parse_one",
            side_effect=sqlglot.errors.ParseError("parse error: unexpected token"),
        ):
            resp = await sql_client.post("/data/sql", json=payload)

        assert resp.status_code == 400
        assert "parse" in resp.json()["detail"].lower() or "SQL" in resp.json()["detail"]


@pytest.mark.asyncio
class TestSQLForbiddenTable:
    async def test_sql_forbidden_table_returns_403(self, sql_client):
        """SQL referencing a table not in the role's schema scope returns HTTP 403."""
        # "secret_table" is not in state.tables or ctx.tables
        payload = {"sql": "SELECT id FROM secret_table", "role": "org_admin"}
        resp = await sql_client.post("/data/sql", json=payload)

        assert resp.status_code == 403
        detail = resp.json()["detail"]
        # detail may be a string or {"violations": [...]}
        detail_str = detail if isinstance(detail, str) else str(detail)
        assert "secret_table" in detail_str
        # REQ-074/REQ-1386: the refusal is the policy_denials fact — it has to be recorded
        from provisa.audit.writer import audit_writer_status, flush_audit

        assert flush_audit(5.0), audit_writer_status()
        assert [r["status_code"] for r in AUDIT_ROWS] == [403]

    async def test_sql_accessible_table_not_forbidden(self, sql_client):
        """SQL referencing an accessible table does not get a 403."""
        payload = {"sql": "SELECT id FROM orders", "role": "org_admin"}
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
            role={"id": "analyst", "capabilities": [], "domain_access": ["*"]},
        )

        sql = "SELECT id FROM orders"
        governed = apply_governance(sql, gov_ctx)

        assert "\"status\" = 'active'" in governed
        assert "WHERE" in governed

    async def test_sql_endpoint_rls_applied_via_http(self, sql_client):
        """Via HTTP: a role with RLS rules results in non-403 for allowed table."""
        # sql_client uses "org_admin" role with no RLS — just verify the endpoint routes correctly
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
                    json={"sql": "SELECT id FROM orders", "role": "org_admin"},
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
    """meta (catalog) domain tables are implicitly traversable via JOIN (V002 exempt); ops is a normal
    grantable domain and is NOT implicit (REQ-1132/REQ-1133)."""

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

    def test_join_to_ops_domain_without_grant_is_blocked(self):
        """REQ-1133: ops is a normal grantable domain, NOT implicitly traversable. A role without an
        ops grant JOINing to an ops table has no registered relationship → V002 (same as any regular
        cross-domain JOIN). Only the meta (catalog) domain stays implicitly traversable (REQ-1132)."""
        from provisa.compiler.sql_validator import validate_sql

        ctx, gov_ctx, role, raw_tables = self._make_validate_fixtures()
        # role's domain_access is ["pet-store"] — no ops grant.
        sql = "SELECT o.id, m.value FROM orders o JOIN metrics m ON o.table_name = m.table_name"
        violations = validate_sql(sql, ctx, gov_ctx, role, raw_tables)
        v002 = [v for v in violations if v.code == "V002"]
        assert v002, "Expected V002 for an ungranted JOIN to the ops domain (no longer implicit)"

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


# -- the role a request names in its body (REQ-273, amended 2026-10-01) ---------------------------
#
# Observed on an unsecured deployment with the default row limit at 1: POST /data/sql with
# {"role": "analyst"} in the body returned every row, while the same statement with the role in
# the X-Provisa-Role header (and over pgwire, Flight, GraphQL) returned one. The unsecured auth
# middleware names org_admin as the acting role when the request carries no role HEADER, and the
# endpoint preferred that to the body's role — so the request ran as org_admin, which holds
# full_results and has no row ceiling. The cap was never skipped; the role was not the one asked.
# The header (or the authenticated identity) is the one role carrier; a body role that differs
# from it is refused.


@pytest.fixture
def two_roles(sql_client, monkeypatch):
    """`sql_client` with an `analyst` role (no full_results) beside org_admin, the default row
    limit at 1, and the plan the pipeline hands its terminal recorded instead of executed."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline, governed_plan
    from provisa.transpiler.router import Route, RouteDecision

    monkeypatch.setenv("PROVISA_DEFAULT_ROW_LIMIT", "1")
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    state = app_mod.state
    state.schemas["analyst"] = MagicMock()
    state.contexts["analyst"] = state.contexts["org_admin"]
    state.rls_contexts["analyst"] = RLSContext.empty()
    state.roles["analyst"] = {
        "id": "analyst",
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    state.roles["org_admin"]["domain_access"] = ["*"]
    plans: list = []

    async def _route(exec_sql, governed_sql, gov_ctx, ctx, st, **kwargs):
        decision = RouteDecision(route=Route.DIRECT, source_id="pg", dialect="postgres", reason="t")
        return exec_sql, decision, "pg", False, {"pg"}, ()

    async def _execute(plan, st=None):
        plans.append(plan)
        return _make_query_result(rows=[(1,)], column_names=["id"])

    monkeypatch.setattr(_pipeline, "_optimize_and_route", _route)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    return sql_client, plans


@pytest.mark.asyncio
class TestRoleNamedByTheRequest:
    _SQL = "SELECT id FROM orders"

    async def test_a_body_role_that_is_not_the_acting_role_is_refused(self, two_roles):
        client, plans = two_roles
        resp = await client.post("/data/sql", json={"sql": self._SQL, "role": "analyst"})
        assert resp.status_code == 400, resp.text
        detail = resp.json()["detail"]
        assert "'analyst'" in detail and "'org_admin'" in detail, detail
        assert plans == [], "the statement ran as a role the request did not ask for"

    async def test_the_header_names_the_role_the_statement_runs_as(self, two_roles):
        client, plans = two_roles
        resp = await client.post(
            "/data/sql", json={"sql": self._SQL}, headers={"X-Provisa-Role": "analyst"}
        )
        assert resp.status_code == 200, resp.text
        (plan,) = plans
        assert plan.role_id == "analyst" and plan.sql.rstrip().endswith("LIMIT 1"), plan.sql

    async def test_a_body_role_equal_to_the_acting_role_is_accepted(self, two_roles):
        client, plans = two_roles
        resp = await client.post(
            "/data/sql",
            json={"sql": self._SQL, "role": "analyst"},
            headers={"X-Provisa-Role": "analyst"},
        )
        assert resp.status_code == 200, resp.text
        assert plans[0].role_id == "analyst" and plans[0].sql.rstrip().endswith("LIMIT 1")
        resp = await client.post("/data/sql", json={"sql": self._SQL, "role": "org_admin"})
        assert resp.status_code == 200, resp.text
        assert plans[1].role_id == "org_admin"

    async def test_a_body_role_cannot_override_the_header(self, two_roles):
        client, plans = two_roles
        resp = await client.post(
            "/data/sql",
            json={"sql": self._SQL, "role": "org_admin"},
            headers={"X-Provisa-Role": "analyst"},
        )
        assert resp.status_code == 400 and plans == []

    async def test_no_role_named_anywhere_is_the_data_plane_admin(self, two_roles):
        client, plans = two_roles
        await client.post("/data/sql", json={"sql": self._SQL})
        assert plans[0].role_id == "org_admin" and "LIMIT" not in plans[0].sql

    async def test_graphql_refuses_a_body_role_that_is_not_the_acting_role(self, two_roles):
        client, _plans = two_roles
        resp = await client.post(
            "/data/graphql", json={"query": "{ __typename }", "role": "analyst"}
        )
        assert resp.status_code == 400, resp.text
        assert "'analyst'" in resp.json()["detail"] and "'org_admin'" in resp.json()["detail"]

    async def test_nl_refuses_a_body_role_and_runs_the_job_as_the_acting_role(
        self, two_roles, monkeypatch
    ):
        from provisa.api.rest import nl_router

        client, _plans = two_roles
        ran: list[str] = []

        async def _run_job(job_id, nl_query, role, app_state, llm, strict=False):
            ran.append(role)

        async def _llm(state):
            return object()

        class _Jobs:  # the job store, in memory
            def __init__(self):
                self.jobs = {}

            async def put(self, job):
                self.jobs[job.job_id] = job

            async def get(self, job_id):
                return self.jobs.get(job_id)

        monkeypatch.setattr(nl_router, "_job_store", _Jobs())
        monkeypatch.setattr(nl_router, "_run_job", _run_job)
        monkeypatch.setattr(nl_router, "_get_llm", _llm)
        resp = await client.post("/query/nl", json={"q": "count orders", "role": "analyst"})
        assert resp.status_code == 400, resp.text
        assert ran == [], "the NL job ran as a role the request body picked"
        resp = await client.post(
            "/query/nl", json={"q": "count orders"}, headers={"X-Provisa-Role": "analyst"}
        )
        assert resp.status_code == 202, resp.text
        job = await nl_router._job_store.get(resp.json()["job_id"])
        assert job.role == "analyst"


def test_an_authenticated_role_is_never_replaced_by_the_body():
    """REQ-273: with an auth provider, the acting role is the validated identity's; a role named
    in a request body is not a way to pick another."""
    from types import SimpleNamespace

    from provisa.api.acting_role import acting_role
    from provisa.api.errors import ApiError

    secured = SimpleNamespace(state=SimpleNamespace(role="analyst"))
    assert acting_role(secured, None, None, "org_admin") == "analyst"
    assert acting_role(secured, None, "analyst", "org_admin") == "analyst"
    with pytest.raises(ApiError) as refused:
        acting_role(secured, None, "org_admin", "org_admin")
    assert refused.value.status_code == 400 and refused.value.code == "data.role_mismatch"
    # no auth layer at all (a bare router): the body's role, else its default
    bare = SimpleNamespace(state=SimpleNamespace())
    assert acting_role(bare, None, "analyst", "org_admin") == "analyst"
    assert acting_role(bare, None, None, "org_admin") == "org_admin"
    assert acting_role(bare, "analyst", None, "org_admin") == "analyst"


@pytest.mark.asyncio
async def test_a_statement_that_outruns_its_deadline_is_a_504_naming_the_setting(
    two_roles, monkeypatch
):
    """REQ-1905: /data/sql binds no deadline of its own; the pipeline gives the statement its
    transport's budget and the endpoint reports the expiry as a gateway timeout."""
    from provisa.pgwire import _pipeline

    client, _plans = two_roles

    from provisa.core.request_deadline import RequestTimedOut

    async def _never(plan, st=None):
        raise RequestTimedOut("http", 60.0, "limits.request_timeouts.http")

    monkeypatch.setattr(_pipeline, "_execute_plan", _never)
    resp = await client.post("/data/sql", json={"sql": "SELECT id FROM orders"})
    assert resp.status_code == 504, resp.text
    body = resp.json()
    assert body["detail"] == (
        "http request exceeded its 60s request timeout (limits.request_timeouts.http)"
    )
    # the params the catalog's `Query timed out after {{timeout_s}}s` renders
    assert body["code"] == "data.query_timeout"
    assert body["params"] == {
        "timeout_s": "60",
        "transport": "http",
        "setting": "limits.request_timeouts.http",
    }

    async def _stopping(plan, st=None):
        raise TimeoutError("the server is stopping")  # no budget to name: its own code

    monkeypatch.setattr(_pipeline, "_execute_plan", _stopping)
    resp = await client.post("/data/sql", json={"sql": "SELECT id FROM orders"})
    assert resp.status_code == 504, resp.text
    assert resp.json()["code"] == "data.request_interrupted"
    assert resp.json()["params"] == {"error": "the server is stopping"}
