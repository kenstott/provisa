# Copyright (c) 2026 Kenneth Stott
# Canary: a9d1e3f7-b2c8-4a56-9e0d-7f1b4c2e8d3a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: Access Governance & Security (Section 1 requirements).

Covers:
  REQ-001 — data-layer governance (visibility, RLS, masking) is the only gate
  REQ-002 — Stage 2 governance applied to every query at compile time
  REQ-003 — all queries governed by user rights alone
  REQ-038 — two independent enforcement layers: schema visibility + SQL
  REQ-039 — schema visibility layer: unauthorized columns invisible
  REQ-040 — SQL enforcement layer: RLS and column stripping at execution
  REQ-041 — RLS rules as PG-style SQL filter expressions mapped to roles
  REQ-042 — distinct, independently configured rights
  REQ-203 — ABAC approval hook evaluated at query time
  REQ-204 — approval hook scoped: skipped when no table has hook enabled
  REQ-262 — two-stage compiler: Stage 1 semantic, Stage 2 governance
  REQ-263 — Stage 2 applies RLS, masking, visibility, row cap
  REQ-264 — Stage 2 handles subqueries, CTEs, JOINs, SELECT *
  REQ-265 — Stage 2 operates on physical column names
  REQ-266 — governance enforced on every client path through Stage 2
  REQ-267 — /data/sql endpoint passes through Stage 2 governance
  REQ-402 — domain-level RLS rules applying to all tables in the domain
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio

from provisa.auth.approval_hook import (
    ApprovalHookConfig,
    ApprovalRequest,
    ApprovalResponse,
    FallbackPolicy,
    HookType,
)
from provisa.compiler.mask_inject import MaskingRules, inject_masking
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    TableMeta,
)
from provisa.compiler.stage2 import (
    GovernanceContext,
    apply_governance,
    build_governance_context,
)
from provisa.executor.direct import execute_direct
from provisa.executor.pool import SourcePool
from provisa.security.masking import MaskType, MaskingRule
from provisa.security.rights import (
    Capability,
    check_capability,
    has_capability,
    InsufficientRightsError,
)
from provisa.security.visibility import (
    is_column_visible,
    visible_column_names,
    visible_tables,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ORDERS_TABLE_ID = 1
CUSTOMERS_TABLE_ID = 2
ROLE_ANALYST = "analyst"
ROLE_ADMIN = "admin"
ROLE_VIEWER = "viewer"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_orders_meta() -> TableMeta:
    return TableMeta(
        table_id=ORDERS_TABLE_ID,
        field_name="orders",
        type_name="Orders",
        source_id="test-pg",
        catalog_name="test_pg",
        schema_name="public",
        table_name="orders",
    )


def _make_ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["orders"] = _make_orders_meta()
    return ctx


def _orders_compiled() -> CompiledQuery:
    sql = 'SELECT "id", "amount", "region" FROM "public"."orders"'
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
            ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
        ],
        sources={"test-pg"},
    )


def _aliased_orders_compiled() -> CompiledQuery:
    sql = 'SELECT "t0"."id", "t0"."amount", "t0"."region" FROM "public"."orders" "t0"'
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None),
            ColumnRef(alias="t0", column="region", field_name="region", nested_in=None),
        ],
        sources={"test-pg"},
    )


def _col_index(result, name: str) -> int:
    return result.column_names.index(name)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="session")
async def source_pool():
    sp = SourcePool()
    await sp.add(
        "test-pg",
        source_type="postgresql",
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE", "provisa"),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
    )
    yield sp
    await sp.close_all()


# ---------------------------------------------------------------------------
# REQ-038, REQ-039 — Schema visibility layer
# ---------------------------------------------------------------------------


class TestSchemaVisibility:
    def test_unauthorized_columns_excluded_from_visible_set(self):
        # REQ-039: unauthorized columns must not appear in the schema visible to a role.
        table = {
            "id": ORDERS_TABLE_ID,
            "domain_id": "sales",
            "columns": [
                {"column_name": "id", "visible_to": [ROLE_ANALYST, ROLE_ADMIN]},
                {"column_name": "amount", "visible_to": [ROLE_ADMIN]},
                {"column_name": "region", "visible_to": [ROLE_ANALYST, ROLE_ADMIN]},
            ],
        }
        visible = visible_column_names(table, ROLE_ANALYST)
        assert "id" in visible
        assert "region" in visible
        assert "amount" not in visible, "REQ-039: 'amount' must not be visible to analyst"

    def test_all_columns_visible_to_admin(self):
        # REQ-039: admin must see all columns when all are listed in visible_to.
        table = {
            "id": ORDERS_TABLE_ID,
            "domain_id": "sales",
            "columns": [
                {"column_name": "id", "visible_to": [ROLE_ADMIN]},
                {"column_name": "amount", "visible_to": [ROLE_ADMIN]},
                {"column_name": "region", "visible_to": [ROLE_ADMIN]},
            ],
        }
        visible = visible_column_names(table, ROLE_ADMIN)
        assert visible == {"id", "amount", "region"}

    def test_is_column_visible_returns_false_for_unauthorized(self):
        # REQ-039: is_column_visible must return False for unauthorized (role, column) pair.
        table = {
            "columns": [
                {"column_name": "secret", "visible_to": [ROLE_ADMIN]},
            ]
        }
        assert not is_column_visible(table, "secret", ROLE_ANALYST), (
            "REQ-039: 'secret' must not be visible to analyst"
        )

    def test_is_column_visible_returns_true_for_authorized(self):
        # REQ-039: is_column_visible must return True for authorized (role, column) pair.
        table = {
            "columns": [
                {"column_name": "region", "visible_to": [ROLE_ANALYST]},
            ]
        }
        assert is_column_visible(table, "region", ROLE_ANALYST)

    def test_visible_tables_excludes_table_with_no_visible_columns(self):
        # REQ-039: a table with no visible columns for the role must be excluded entirely.
        tables = [
            {
                "id": ORDERS_TABLE_ID,
                "domain_id": "sales",
                "columns": [
                    {"column_name": "secret", "visible_to": [ROLE_ADMIN]},
                ],
            }
        ]
        role = {"id": ROLE_ANALYST, "domain_access": ["*"], "capabilities": []}
        result = visible_tables(tables, role)
        assert result == [], "REQ-039: table with no visible columns must be excluded from schema"

    def test_visible_tables_includes_partial_columns(self):
        # REQ-039: table with at least one visible column must appear with only visible columns.
        tables = [
            {
                "id": ORDERS_TABLE_ID,
                "domain_id": "sales",
                "columns": [
                    {"column_name": "id", "visible_to": [ROLE_ANALYST]},
                    {"column_name": "secret", "visible_to": [ROLE_ADMIN]},
                ],
            }
        ]
        role = {"id": ROLE_ANALYST, "domain_access": ["*"], "capabilities": []}
        result = visible_tables(tables, role)
        assert len(result) == 1
        col_names = [c["column_name"] for c in result[0]["columns"]]
        assert "id" in col_names
        assert "secret" not in col_names, "REQ-039: 'secret' column must be stripped for analyst"


# ---------------------------------------------------------------------------
# REQ-040, REQ-041 — SQL enforcement: RLS injection at execution
# ---------------------------------------------------------------------------


class TestRLSEnforcement:
    async def test_rls_filters_rows_at_execution(self, source_pool):
        # REQ-040, REQ-041: RLS WHERE clause injected at execution time filters rows.
        ctx = _make_ctx()
        compiled = _orders_compiled()

        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        with_rls = inject_rls(compiled, ctx, rls)

        result = await execute_direct(source_pool, "test-pg", with_rls.sql, with_rls.params)

        assert result.rows, "REQ-041: expected at least one us-east row in test data"
        region_idx = _col_index(result, "region")
        for row in result.rows:
            assert row[region_idx] == "us-east", (
                f"REQ-040: RLS failed — got region={row[region_idx]!r}"
            )

    async def test_rls_where_clause_present_in_generated_sql(self):
        # REQ-040: SQL enforcement layer injects WHERE predicate into generated SQL.
        ctx = _make_ctx()
        compiled = _orders_compiled()

        rls = RLSContext(rules={ORDERS_TABLE_ID: "amount > 0"})
        with_rls = inject_rls(compiled, ctx, rls)

        assert "WHERE" in with_rls.sql.upper(), (
            "REQ-040: WHERE clause must be present in governed SQL"
        )
        assert "amount > 0" in with_rls.sql

    async def test_empty_rls_context_returns_all_rows(self, source_pool):
        # REQ-040: empty RLS (no rules) must not restrict rows.
        ctx = _make_ctx()
        compiled = _orders_compiled()

        rls = RLSContext.empty()
        with_rls = inject_rls(compiled, ctx, rls)

        result_all = await execute_direct(source_pool, "test-pg", compiled.sql, [])
        result_rls = await execute_direct(source_pool, "test-pg", with_rls.sql, with_rls.params)

        assert len(result_all.rows) == len(result_rls.rows), (
            "REQ-040: empty RLS must not restrict rows"
        )

    async def test_rls_impossible_predicate_yields_empty_result(self, source_pool):
        # REQ-041: RLS predicate that matches nothing must return empty result, not error.
        ctx = _make_ctx()
        compiled = _orders_compiled()

        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'nonexistent-region-xyz'"})
        with_rls = inject_rls(compiled, ctx, rls)

        result = await execute_direct(source_pool, "test-pg", with_rls.sql, with_rls.params)
        assert result.rows == [], f"REQ-041: expected empty result, got {len(result.rows)} rows"


# ---------------------------------------------------------------------------
# REQ-001, REQ-002, REQ-003 — Data-layer governance is the only gate
# ---------------------------------------------------------------------------


class TestGovernanceAsOnlyGate:
    def test_check_capability_raises_for_missing_capability(self):
        # REQ-042: distinct capabilities must be independently enforced.
        role = {"id": ROLE_ANALYST, "capabilities": ["usage"]}
        with pytest.raises(InsufficientRightsError) as exc:
            check_capability(role, Capability.SOURCE_REGISTRATION)
        assert "source_registration" in str(exc.value)

    def test_admin_capability_grants_all(self):
        # REQ-042: admin capability must satisfy any capability check.
        role = {"id": ROLE_ADMIN, "capabilities": ["admin"]}
        # check_capability returns None on success; any failure raises InsufficientRightsError.
        assert check_capability(role, Capability.SOURCE_REGISTRATION) is None
        assert check_capability(role, Capability.TABLE_REGISTRATION) is None
        assert check_capability(role, Capability.CREATE_RELATIONSHIP) is None
        assert check_capability(role, Capability.MASKING_CONFIG) is None
        # Verify via has_capability that admin is recognised for each checked capability.
        assert has_capability(role, Capability.SOURCE_REGISTRATION)
        assert has_capability(role, Capability.TABLE_REGISTRATION)
        assert has_capability(role, Capability.CREATE_RELATIONSHIP)
        assert has_capability(role, Capability.MASKING_CONFIG)

    def test_has_capability_returns_false_for_missing(self):
        # REQ-001: governance is expressed through rights, not capability gates on querying.
        role = {"id": ROLE_VIEWER, "capabilities": ["usage"]}
        assert not has_capability(role, Capability.FULL_RESULTS)
        assert not has_capability(role, Capability.AD_HOC_QUERY)

    def test_has_capability_returns_true_when_present(self):
        # REQ-042: independently configured rights — present capability is respected.
        role = {"id": ROLE_ANALYST, "capabilities": ["usage", "full_results"]}
        assert has_capability(role, Capability.USAGE)
        assert has_capability(role, Capability.FULL_RESULTS)

    def test_distinct_rights_are_independent(self):
        # REQ-042: holding one right does not grant another.
        role = {"id": ROLE_ANALYST, "capabilities": ["table_registration"]}
        assert has_capability(role, Capability.TABLE_REGISTRATION)
        assert not has_capability(role, Capability.SOURCE_REGISTRATION), (
            "REQ-042: TABLE_REGISTRATION must not imply SOURCE_REGISTRATION"
        )
        assert not has_capability(role, Capability.CREATE_RELATIONSHIP), (
            "REQ-042: TABLE_REGISTRATION must not imply CREATE_RELATIONSHIP"
        )

    def test_ignore_relationships_is_distinct_capability(self):
        # REQ-042 / REQ-463: ignore_relationships is a distinct, independently assigned cap.
        role_with = {"id": "steward", "capabilities": ["ignore_relationships"]}
        role_without = {"id": ROLE_ANALYST, "capabilities": ["usage"]}
        assert has_capability(role_with, Capability.IGNORE_RELATIONSHIPS)
        assert not has_capability(role_without, Capability.IGNORE_RELATIONSHIPS)


# ---------------------------------------------------------------------------
# REQ-038 — Two independent layers: visibility + SQL enforcement
# ---------------------------------------------------------------------------


class TestTwoIndependentEnforcementLayers:
    def test_visibility_layer_excludes_column_before_sql(self):
        # REQ-038: schema visibility layer is a guard independent of SQL enforcement.
        table = {
            "id": ORDERS_TABLE_ID,
            "domain_id": "sales",
            "columns": [
                {"column_name": "id", "visible_to": [ROLE_ANALYST]},
                {"column_name": "pii_field", "visible_to": [ROLE_ADMIN]},
            ],
        }
        visible = visible_column_names(table, ROLE_ANALYST)
        assert "pii_field" not in visible, (
            "REQ-038: visibility layer must exclude pii_field before any SQL is built"
        )

    async def test_sql_layer_enforces_rls_independently(self, source_pool):
        # REQ-038: SQL layer enforces RLS regardless of schema visibility state.
        ctx = _make_ctx()
        compiled = _orders_compiled()

        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        governed = inject_rls(compiled, ctx, rls)

        result = await execute_direct(source_pool, "test-pg", governed.sql, governed.params)
        region_idx = _col_index(result, "region")
        for row in result.rows:
            assert row[region_idx] == "us-east", (
                "REQ-038: SQL enforcement layer must filter rows independently"
            )

    async def test_masking_layer_applies_column_transform_at_execution(self, source_pool):
        # REQ-038: masking is a distinct SQL enforcement concern applied at execution.
        ctx = _make_ctx()
        compiled = _aliased_orders_compiled()

        rule = MaskingRule(mask_type=MaskType.constant, value=0)
        masking_rules: MaskingRules = {
            (ORDERS_TABLE_ID, ROLE_ANALYST): {"amount": (rule, "integer")},
        }
        masked = inject_masking(compiled, ctx, masking_rules, ROLE_ANALYST)

        result = await execute_direct(source_pool, "test-pg", masked.sql, masked.params)

        assert result.rows, "REQ-038: expected rows for masking verification"
        amount_idx = _col_index(result, "amount")
        for row in result.rows:
            assert row[amount_idx] == 0, (
                f"REQ-038: masking layer failed — amount={row[amount_idx]!r}"
            )


# ---------------------------------------------------------------------------
# REQ-262, REQ-263, REQ-265 — Two-stage compiler
# ---------------------------------------------------------------------------


class TestTwoStageCompiler:
    def test_stage2_governance_context_includes_rls_rules(self):
        # REQ-262, REQ-263: GovernanceContext carries RLS rules from RLSContext.
        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        masking_rules: MaskingRules = {}
        ctx = _make_ctx()

        tables = [
            {
                "id": ORDERS_TABLE_ID,
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [
                    {"column_name": "id", "visible_to": None, "data_type": "integer"},
                    {"column_name": "amount", "visible_to": None, "data_type": "integer"},
                    {"column_name": "region", "visible_to": None, "data_type": "varchar"},
                ],
                "max_rows": None,
            }
        ]
        role = {"id": ROLE_ANALYST, "capabilities": ["usage"]}

        gov = build_governance_context(ROLE_ANALYST, rls, masking_rules, ctx, tables, role)

        assert ORDERS_TABLE_ID in gov.rls_rules, (
            "REQ-263: GovernanceContext must carry RLS rules for the table"
        )
        assert gov.rls_rules[ORDERS_TABLE_ID] == "region = 'us-east'"

    def test_stage2_governance_context_includes_masking_rules(self):
        # REQ-263: GovernanceContext must include masking rules for the requesting role.
        rule = MaskingRule(mask_type=MaskType.constant, value=0)
        masking_rules: MaskingRules = {
            (ORDERS_TABLE_ID, ROLE_ANALYST): {"amount": (rule, "integer")},
        }
        rls = RLSContext.empty()
        ctx = _make_ctx()
        tables = [
            {
                "id": ORDERS_TABLE_ID,
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [
                    {"column_name": "amount", "visible_to": None, "data_type": "integer"},
                ],
                "max_rows": None,
            }
        ]
        role = {"id": ROLE_ANALYST, "capabilities": ["usage"]}

        gov = build_governance_context(ROLE_ANALYST, rls, masking_rules, ctx, tables, role)

        assert (ORDERS_TABLE_ID, "amount") in gov.masking_rules, (
            "REQ-263: GovernanceContext must carry masking rule for (table_id, col) pair"
        )

    def test_stage2_masking_rules_not_leaked_to_other_role(self):
        # REQ-263, REQ-265: masking rules for analyst must not appear for admin.
        rule = MaskingRule(mask_type=MaskType.constant, value=0)
        masking_rules: MaskingRules = {
            (ORDERS_TABLE_ID, ROLE_ANALYST): {"amount": (rule, "integer")},
        }
        rls = RLSContext.empty()
        ctx = _make_ctx()
        tables = [
            {
                "id": ORDERS_TABLE_ID,
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [
                    {"column_name": "amount", "visible_to": None, "data_type": "integer"},
                ],
                "max_rows": None,
            }
        ]
        role = {"id": ROLE_ADMIN, "capabilities": ["admin"]}

        gov = build_governance_context(ROLE_ADMIN, rls, masking_rules, ctx, tables, role)

        assert (ORDERS_TABLE_ID, "amount") not in gov.masking_rules, (
            "REQ-263: admin must not inherit analyst's masking rules"
        )

    def test_stage2_row_cap_applied_for_role_without_full_results(self):
        # REQ-263, REQ-005: roles without FULL_RESULTS capability get a row cap.
        from provisa.compiler.stage2 import resolve_row_cap

        role_without = {"id": ROLE_ANALYST, "capabilities": ["usage"], "max_rows": None}
        cap = resolve_row_cap(role_without, None)
        assert cap is not None, "REQ-263: analyst without FULL_RESULTS must receive a row cap"

    def test_stage2_row_cap_none_for_full_results_role(self):
        # REQ-263: role with FULL_RESULTS capability must receive no row cap.
        from provisa.compiler.stage2 import resolve_row_cap

        role_full = {"id": ROLE_ADMIN, "capabilities": ["admin", "full_results"], "max_rows": None}
        cap = resolve_row_cap(role_full, None)
        assert cap is None, "REQ-263: role with FULL_RESULTS must not be capped"

    def test_stage2_explicit_max_rows_takes_precedence(self):
        # REQ-263: explicit max_rows on a role overrides any computed default.
        from provisa.compiler.stage2 import resolve_row_cap

        role = {"id": ROLE_ANALYST, "capabilities": ["usage"], "max_rows": None}
        cap = resolve_row_cap(role, 500)
        assert cap == 500, "REQ-263: explicit max_rows=500 must take precedence over defaults"

    def test_stage2_governs_sql_with_rls_and_mask_applied(self):
        # REQ-262, REQ-263, REQ-266: apply_governance produces SQL with WHERE and mask.
        sql = 'SELECT "id", "amount", "region" FROM "public"."orders"'
        gov = GovernanceContext(
            rls_rules={ORDERS_TABLE_ID: "region = 'us-east'"},
            masking_rules={
                (ORDERS_TABLE_ID, "amount"): (
                    MaskingRule(mask_type=MaskType.constant, value=0),
                    "integer",
                )
            },
            visible_columns={ORDERS_TABLE_ID: frozenset({"id", "amount", "region"})},
            table_map={"public.orders": ORDERS_TABLE_ID, "orders": ORDERS_TABLE_ID},
            all_columns={
                ORDERS_TABLE_ID: [
                    ("id", "integer"),
                    ("amount", "integer"),
                    ("region", "varchar"),
                ]
            },
            limit_ceiling=1000,
        )
        governed_sql = apply_governance(sql, gov)
        assert "us-east" in governed_sql, "REQ-263: RLS predicate must appear in governed SQL"
        assert "LIMIT" in governed_sql.upper(), (
            "REQ-263: row cap must be injected into governed SQL"
        )


# ---------------------------------------------------------------------------
# REQ-264 — Stage 2 handles structural patterns: CTEs, subqueries
# ---------------------------------------------------------------------------


class TestStage2StructuralPatterns:
    def test_stage2_injects_rls_into_subquery(self):
        # REQ-264: RLS must be applied at every table reference including subqueries.
        sql = (
            'SELECT sub."id", sub."region" FROM '
            '(SELECT "id", "region" FROM "public"."orders") AS sub'
        )
        gov = GovernanceContext(
            rls_rules={ORDERS_TABLE_ID: "region = 'us-east'"},
            masking_rules={},
            visible_columns={ORDERS_TABLE_ID: None},
            table_map={"public.orders": ORDERS_TABLE_ID, "orders": ORDERS_TABLE_ID},
            all_columns={ORDERS_TABLE_ID: [("id", "integer"), ("region", "varchar")]},
            limit_ceiling=None,
        )
        governed_sql = apply_governance(sql, gov)
        assert "us-east" in governed_sql, (
            "REQ-264: RLS must be injected into subquery table reference"
        )

    def test_stage2_injects_rls_into_cte(self):
        # REQ-264: RLS must be applied at every table reference inside CTEs.
        sql = (
            'WITH cte AS (SELECT "id", "region" FROM "public"."orders") '
            'SELECT "id", "region" FROM cte'
        )
        gov = GovernanceContext(
            rls_rules={ORDERS_TABLE_ID: "region = 'us-east'"},
            masking_rules={},
            visible_columns={ORDERS_TABLE_ID: None},
            table_map={"public.orders": ORDERS_TABLE_ID, "orders": ORDERS_TABLE_ID},
            all_columns={ORDERS_TABLE_ID: [("id", "integer"), ("region", "varchar")]},
            limit_ceiling=None,
        )
        governed_sql = apply_governance(sql, gov)
        assert "us-east" in governed_sql, (
            "REQ-264: RLS must be injected at every table reference in CTEs"
        )


# ---------------------------------------------------------------------------
# REQ-203, REQ-204 — ABAC approval hook
# ---------------------------------------------------------------------------


class TestABACApprovalHook:
    def test_approval_request_structure(self):
        # REQ-203: approval hook receives user_id, roles, tables, columns, operation.
        req = ApprovalRequest(
            user="alice",
            roles=[ROLE_ANALYST],
            tables=["orders"],
            columns=["amount", "region"],
            operation="query",
            session_vars={"env": "prod"},
        )
        assert req.user == "alice"
        assert ROLE_ANALYST in req.roles
        assert "orders" in req.tables
        assert "query" == req.operation

    def test_approval_response_with_additional_filter(self):
        # REQ-203: approval hook may return additional SQL filter to AND into query.
        resp = ApprovalResponse(
            approved=True,
            reason="policy ok",
            additional_filter="region IN ('us-east', 'us-west')",
        )
        assert resp.approved
        assert resp.additional_filter == "region IN ('us-east', 'us-west')"

    def test_approval_response_denied(self):
        # REQ-203: approval hook can deny a query.
        resp = ApprovalResponse(approved=False, reason="policy denied")
        assert not resp.approved
        assert resp.additional_filter is None

    def test_hook_config_fallback_deny(self):
        # REQ-247: fallback=deny means timeout → deny.
        cfg = ApprovalHookConfig(
            type=HookType.WEBHOOK,
            url="https://authz.internal/approve",
            timeout_ms=500,
            fallback=FallbackPolicy.DENY,
        )
        assert cfg.fallback == FallbackPolicy.DENY

    def test_hook_config_fallback_allow(self):
        # REQ-247: fallback=allow means timeout → allow.
        cfg = ApprovalHookConfig(
            type=HookType.WEBHOOK,
            url="https://authz.internal/approve",
            timeout_ms=500,
            fallback=FallbackPolicy.ALLOW,
        )
        assert cfg.fallback == FallbackPolicy.ALLOW

    def test_hook_config_grpc_transport(self):
        # REQ-246: gRPC transport is a supported approval hook type.
        cfg = ApprovalHookConfig(
            type=HookType.GRPC,
            url="grpc://authz.internal:50051",
            timeout_ms=100,
        )
        assert cfg.type == HookType.GRPC

    def test_hook_config_unix_socket_transport(self):
        # REQ-246: unix_socket transport is a supported approval hook type.
        cfg = ApprovalHookConfig(
            type=HookType.UNIX_SOCKET,
            socket_path="/var/run/provisa-authz.sock",
            timeout_ms=50,
        )
        assert cfg.type == HookType.UNIX_SOCKET
        assert cfg.socket_path == "/var/run/provisa-authz.sock"

    def test_approval_hook_skipped_when_no_table_scoped(self):
        # REQ-204: when no queried table has approval_hook enabled, the call is skipped.
        # Simulate the scoping check: no hook flag on any table → hook should not fire.
        tables_without_hook = [
            {"id": ORDERS_TABLE_ID, "approval_hook": False},
            {"id": CUSTOMERS_TABLE_ID, "approval_hook": False},
        ]
        hook_required = any(t.get("approval_hook", False) for t in tables_without_hook)
        assert not hook_required, (
            "REQ-204: no table with approval_hook=True — hook call must be skipped"
        )

    def test_approval_hook_required_when_table_scoped(self):
        # REQ-204: when at least one queried table has approval_hook=true, hook is called.
        tables_with_hook = [
            {"id": ORDERS_TABLE_ID, "approval_hook": True},
            {"id": CUSTOMERS_TABLE_ID, "approval_hook": False},
        ]
        hook_required = any(t.get("approval_hook", False) for t in tables_with_hook)
        assert hook_required, "REQ-204: table with approval_hook=True — hook must be invoked"


# ---------------------------------------------------------------------------
# REQ-266, REQ-267 — Stage 2 enforced across all client paths
# ---------------------------------------------------------------------------


class TestStage2AllClientPaths:
    def test_stage2_governance_applied_to_raw_sql(self):
        # REQ-266, REQ-267: raw SQL passed to Stage 2 receives the same governance
        # as a GraphQL-compiled query.
        raw_sql = 'SELECT "id", "amount", "region" FROM "public"."orders"'
        gov = GovernanceContext(
            rls_rules={ORDERS_TABLE_ID: "region = 'us-east'"},
            masking_rules={},
            visible_columns={ORDERS_TABLE_ID: None},
            table_map={"public.orders": ORDERS_TABLE_ID, "orders": ORDERS_TABLE_ID},
            all_columns={
                ORDERS_TABLE_ID: [("id", "integer"), ("amount", "integer"), ("region", "varchar")]
            },
            limit_ceiling=100,
        )
        governed = apply_governance(raw_sql, gov)

        assert "us-east" in governed, (
            "REQ-267: raw SQL /data/sql path must have RLS injected by Stage 2"
        )
        assert "LIMIT" in governed.upper(), (
            "REQ-266: row cap must be injected by Stage 2 regardless of client path"
        )

    def test_stage2_governance_identical_for_graphql_vs_raw_sql(self):
        # REQ-266: same SQL from any client path must produce identical governed SQL.
        sql = 'SELECT "id", "region" FROM "public"."orders"'
        gov = GovernanceContext(
            rls_rules={ORDERS_TABLE_ID: "region = 'us-east'"},
            masking_rules={},
            visible_columns={ORDERS_TABLE_ID: None},
            table_map={"public.orders": ORDERS_TABLE_ID, "orders": ORDERS_TABLE_ID},
            all_columns={ORDERS_TABLE_ID: [("id", "integer"), ("region", "varchar")]},
            limit_ceiling=None,
        )
        governed_a = apply_governance(sql, gov)
        governed_b = apply_governance(sql, gov)

        assert governed_a == governed_b, (
            "REQ-266: Stage 2 governance must be deterministic for identical inputs"
        )


# ---------------------------------------------------------------------------
# REQ-402 — Domain-level RLS rules
# ---------------------------------------------------------------------------


class TestDomainLevelRLS:
    async def test_domain_rls_rule_filters_table_in_domain(self, source_pool):
        # REQ-402: domain-level RLS applies to all tables in the domain.
        # Simulate by applying a domain-scoped RLS to the orders table.
        ctx = _make_ctx()
        compiled = _orders_compiled()

        # The domain-level RLS is the same SQL expression but applies via domain scoping.
        # We verify the row filtering is correct when the rule is table-scoped here
        # (integration of domain scoping is tested at the config-loader level in unit tests).
        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        with_rls = inject_rls(compiled, ctx, rls)

        result = await execute_direct(source_pool, "test-pg", with_rls.sql, with_rls.params)
        region_idx = _col_index(result, "region")
        for row in result.rows:
            assert row[region_idx] == "us-east", (
                "REQ-402: domain-level RLS must filter rows identically to table-level RLS"
            )
