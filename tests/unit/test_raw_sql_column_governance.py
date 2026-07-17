# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-def0-456789abcdef
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holders.

"""Regression tests: column VISIBILITY and MASKING must be enforced on the raw-SQL path
(pgwire / Flight SQL / airport).

REQ-263, REQ-264, REQ-265: build_governance_context derives visible_columns and all_columns
from its ``tables`` argument. Every raw-SQL caller passes ``getattr(state, "tables", [])``.

ROOT CAUSE: state.tables was never declared as an AppState attribute, so it was never set
at startup, and getattr(state, "tables", []) always returned []. build_governance_context
received tables=[] → visible_columns/all_columns empty → column visibility and masking
silently skipped on pgwire, Flight SQL, and airport transports.

FIX: AppState declares tables: list[dict] = [] and _rebuild_schemas populates it after
_fetch_tables + _filter_tables_by_schema_cfg so the governance pipeline receives the full
table+column+visible_to metadata.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import build_governance_context, apply_governance
from provisa.security.masking import MaskingRule, MaskType

# asyncio_mode = "auto" in pyproject.toml picks up async tests automatically.

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

TABLE_ID = 42
SOURCE_ID = "pg"
DOMAIN_ID = "sales"


def _orders_meta() -> TableMeta:
    return TableMeta(
        table_id=TABLE_ID,
        field_name="orders",
        type_name="Orders",
        source_id=SOURCE_ID,
        catalog_name=SOURCE_ID,
        schema_name="public",
        table_name="orders",
        domain_id=DOMAIN_ID,
    )


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {"orders": _orders_meta()}
    return ctx


# orders columns: id and region are visible to analyst; amount is admin-only.
_ORDERS_COLUMNS = [
    {"column_name": "id", "data_type": "integer", "visible_to": ["admin", "analyst"]},
    {"column_name": "region", "data_type": "varchar", "visible_to": ["admin", "analyst"]},
    {"column_name": "amount", "data_type": "numeric", "visible_to": ["admin"]},  # analyst CANNOT see
]

_ORDERS_TABLE_DICT = {
    "id": TABLE_ID,
    "source_id": SOURCE_ID,
    "schema_name": "public",
    "table_name": "orders",
    "domain_id": DOMAIN_ID,
    "columns": _ORDERS_COLUMNS,
}

_MASKING_RULE = MaskingRule(mask_type=MaskType.constant, value="***")

# Masking: analyst sees masked region (redacted to '***'), not the real value.
_MASKING_RULES = {
    (TABLE_ID, "analyst"): {"region": (_MASKING_RULE, "varchar")},
}


# ---------------------------------------------------------------------------
# Regression: AppState.tables attribute must be declared (FAILS before fix)
# ---------------------------------------------------------------------------


class TestAppStateTablesAttribute:
    """Regression: AppState must declare tables so _rebuild_schemas can populate it.

    Before fix: 'tables' was not in the AppState class body. getattr(state, "tables", [])
    returned [] via the default every time — the attribute was never set at startup.
    After fix: tables: list[dict] = [] is declared and _rebuild_schemas sets it.
    """

    def test_tables_attribute_declared_on_appstate(self):
        """FAILS before fix (AttributeError), PASSES after fix (tables=[])."""
        from provisa.api.app import AppState

        s = AppState()
        # Before fix: AttributeError — tables not in AppState class body.
        # After fix: tables = [] (declared, populated by _rebuild_schemas).
        assert hasattr(s, "tables"), (
            "AppState must declare 'tables' so _rebuild_schemas can populate it "
            "and the raw-SQL governance path receives real table+column metadata."
        )
        assert isinstance(s.tables, list)

    def test_tables_attribute_is_empty_list_at_init(self):
        """state.tables starts as [] at init; _rebuild_schemas populates it."""
        from provisa.api.app import AppState

        s = AppState()
        assert s.tables == []


# ---------------------------------------------------------------------------
# Root-cause: build_governance_context behaviour with empty vs populated tables
# ---------------------------------------------------------------------------


class TestBuildGovernanceContextTables:
    """Document the root cause: build_governance_context with tables=[] produces empty
    visible_columns/all_columns, silently bypassing column governance.
    """

    def test_empty_tables_produces_empty_visible_columns(self):
        """tables=[] → visible_columns empty → no per-column restrictions."""
        ctx = _ctx()
        rls = RLSContext.empty()
        gov_ctx = build_governance_context("analyst", rls, {}, ctx, tables=[])
        assert gov_ctx.visible_columns == {}

    def test_empty_tables_produces_empty_all_columns(self):
        """tables=[] → all_columns empty → SELECT * cannot be expanded to visible cols."""
        ctx = _ctx()
        rls = RLSContext.empty()
        gov_ctx = build_governance_context("analyst", rls, {}, ctx, tables=[])
        assert gov_ctx.all_columns == {}

    def test_populated_tables_restricts_analyst_visible_columns(self):
        """With real tables: analyst can see id, region — NOT amount."""
        ctx = _ctx()
        rls = RLSContext.empty()
        gov_ctx = build_governance_context(
            "analyst", rls, {}, ctx, tables=[_ORDERS_TABLE_DICT]
        )
        assert gov_ctx.visible_columns[TABLE_ID] == frozenset({"id", "region"})

    def test_populated_tables_all_columns_populated(self):
        """With real tables: all_columns contains all three columns."""
        ctx = _ctx()
        rls = RLSContext.empty()
        gov_ctx = build_governance_context(
            "analyst", rls, {}, ctx, tables=[_ORDERS_TABLE_DICT]
        )
        col_names = [c for c, _ in gov_ctx.all_columns[TABLE_ID]]
        assert col_names == ["id", "region", "amount"]


# ---------------------------------------------------------------------------
# Governance correctness: column visibility and masking with populated tables
# ---------------------------------------------------------------------------


class TestColumnGovernanceWithPopulatedTables:
    """Verify that governance enforces visibility/masking when state.tables is populated
    (the state after the fix).
    """

    def test_hidden_column_dropped_from_explicit_select(self):
        """analyst cannot SELECT amount (visible_to=[admin] only)."""
        ctx = _ctx()
        rls = RLSContext.empty()
        gov_ctx = build_governance_context(
            "analyst", rls, {}, ctx, tables=[_ORDERS_TABLE_DICT]
        )
        governed = apply_governance("SELECT amount FROM orders", gov_ctx)
        assert "amount" not in governed

    def test_select_star_expanded_to_visible_columns_only(self):
        """SELECT * expands to id, region — amount (admin-only) is dropped."""
        ctx = _ctx()
        rls = RLSContext.empty()
        gov_ctx = build_governance_context(
            "analyst", rls, {}, ctx, tables=[_ORDERS_TABLE_DICT]
        )
        governed = apply_governance("SELECT * FROM orders", gov_ctx)
        assert "SELECT *" not in governed  # wildcard expanded
        assert "id" in governed
        assert "region" in governed
        assert "amount" not in governed

    def test_masking_applied_on_visible_column(self):
        """region is masked for analyst: SELECT * contains '***', not raw region."""
        ctx = _ctx()
        rls = RLSContext.empty()
        gov_ctx = build_governance_context(
            "analyst", rls, _MASKING_RULES, ctx, tables=[_ORDERS_TABLE_DICT]
        )
        governed = apply_governance("SELECT * FROM orders", gov_ctx)
        assert "'***'" in governed  # mask value in governed SQL
        assert "amount" not in governed  # hidden col dropped

    def test_admin_sees_all_columns(self):
        """admin role has visible_to access to all columns."""
        ctx = _ctx()
        rls = RLSContext.empty()
        gov_ctx = build_governance_context(
            "admin", rls, {}, ctx, tables=[_ORDERS_TABLE_DICT]
        )
        governed = apply_governance("SELECT * FROM orders", gov_ctx)
        assert "SELECT *" not in governed  # wildcard still expanded
        assert "id" in governed
        assert "region" in governed
        assert "amount" in governed


# ---------------------------------------------------------------------------
# Pipeline-level regression: _govern_and_route with populated state.tables
# ---------------------------------------------------------------------------


def _fake_pipeline_state(*, tables: list[dict]) -> SimpleNamespace:
    """Minimal AppState for _govern_and_route — orders table + analyst role."""
    ctx = _ctx()
    return SimpleNamespace(
        contexts={"analyst": ctx},
        rls_contexts={},
        roles={
            "analyst": {
                "id": "analyst",
                "capabilities": ["ad_hoc_query"],
                "domain_access": ["*"],
            }
        },
        masking_rules=_MASKING_RULES,
        source_types={SOURCE_ID: "postgresql"},
        source_dialects={SOURCE_ID: "postgres"},
        source_catalogs={},
        source_dsns={},
        source_pools=SimpleNamespace(source_ids={SOURCE_ID}, has=lambda _: False),
        view_sql_map={},
        tables=tables,
    )


async def _run_govern_and_route(monkeypatch, sql: str, *, tables: list[dict]) -> str:
    """Run _govern_and_route with patched state; return the governed semantic SQL."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route, RouteDecision

    fake_state = _fake_pipeline_state(tables=tables)
    monkeypatch.setattr(app_mod, "state", fake_state, raising=False)

    decision = RouteDecision(
        route=Route.DIRECT, source_id=SOURCE_ID, dialect="postgres", reason="test"
    )

    async def _mock_optimize_route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        return exec_sql, decision, SOURCE_ID, False, {SOURCE_ID}

    with patch.object(_pipeline, "_optimize_and_route", side_effect=_mock_optimize_route):
        plan = await _pipeline._govern_and_route(sql, "analyst")

    return plan.sql


async def test_pipeline_column_governance_enforced_with_populated_tables(monkeypatch):
    """After fix: state.tables populated → SELECT * drops hidden col, masks region."""
    governed = await _run_govern_and_route(
        monkeypatch,
        "SELECT * FROM orders",
        tables=[_ORDERS_TABLE_DICT],
    )
    assert "SELECT *" not in governed  # wildcard expanded; '***' mask literal is fine
    assert "amount" not in governed
    assert "id" in governed
    assert "region" in governed
    assert "'***'" in governed  # masking applied for region column


async def test_pipeline_hidden_column_blocked_with_populated_tables(monkeypatch):
    """After fix: explicit SELECT of non-visible column raises PermissionError."""
    with pytest.raises(PermissionError, match="amount"):
        await _run_govern_and_route(
            monkeypatch,
            "SELECT amount FROM orders",
            tables=[_ORDERS_TABLE_DICT],
        )
