# Copyright (c) 2026 Kenneth Stott
# Canary: 7f2c9a4e-b1d3-4e6f-8c0b-2a5d7e9f1c3b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for pgwire catalog — role-based column visibility (REQ-128).

Tests the auth/role capability lookup → pgwire catalog handler → column list boundary.
getColumns() is served by information_schema.columns via _build_catalog_index /
_populate_is_columns. The compilation context is per-role, so only columns the role
can see in the schema are registered into the CatalogIndex.

These tests call real functions (_build_catalog_index, _populate_is_columns,
_build_catalog_db / answer) with real objects; no mocks at the component boundary
under test.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler import naming as _naming
from provisa.compiler.schema_gen import SchemaInput
from provisa.compiler.context import build_context
from provisa.pgwire.catalog import (
    _build_catalog_index,
    _populate_is_columns,
    answer,
    classify,
)

pytestmark = [pytest.mark.integration]

# integration: mock-justified — avoids live Trino/DuckDB row-count fetch;
# col_types and contexts are real objects built from schema inputs.
duckdb = pytest.importorskip("duckdb", reason="duckdb required for catalog tests")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col(name: str, data_type: str = "varchar", nullable: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _make_role(role_id: str, domain_access: list[str] | None = None) -> dict:
    return {
        "id": role_id,
        "domain_access": domain_access if domain_access is not None else ["*"],
        "capabilities": [],
    }


def _make_si(
    tables: list[dict],
    column_types: dict[int, list[ColumnMetadata]],
    role_id: str,
) -> SchemaInput:
    _naming.configure(gql="snake")
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=_make_role(role_id),
        domains=[{"id": "sales", "graphql_alias": None}],
    )


def _make_state(ctx: Any, col_types: dict) -> Any:
    """Build a minimal state object for catalog functions.

    integration: mock-justified — state is a protocol duck-type; only .contexts
    and .schema_build_cache are accessed by _build_catalog_db and answer().
    """
    state = MagicMock()
    state.contexts = {ctx._role_id: ctx} if hasattr(ctx, "_role_id") else {}
    state.schema_build_cache = {"column_types": col_types, "tables": [], "domains": []}
    state.engine_conn = None
    return state


def _catalog_columns_for_role(
    table_id: int,
    tables: list[dict],
    column_types: dict[int, list[ColumnMetadata]],
    role_id: str,
) -> list[str]:
    """Build the CatalogIndex for a role and return visible column names for table_id."""
    si = _make_si(tables, column_types, role_id)
    ctx = build_context(si)
    idx = _build_catalog_index(ctx, column_types)
    return [col_name for toid, col_name, *_ in idx.all_cols if toid == 16384 + table_id]


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_TABLES = [
    {
        "id": 1,
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "governance": "pre-approved",
        "columns": [
            # visible to everyone (empty visible_to)
            {"column_name": "id", "visible_to": []},
            {"column_name": "amount", "visible_to": []},
            # restricted to admin only
            {"column_name": "customer_pii", "visible_to": ["admin"]},
            # restricted to analyst only
            {"column_name": "region_code", "visible_to": ["analyst"]},
        ],
    }
]

_COL_TYPES: dict[int, list[ColumnMetadata]] = {
    1: [
        _col("id", "integer", nullable=False),
        _col("amount", "double"),
        _col("customer_pii", "varchar"),
        _col("region_code", "varchar"),
    ]
}


# ---------------------------------------------------------------------------
# REQ-128: JDBC getColumns() with role-based column visibility
# ---------------------------------------------------------------------------


class TestJDBCGetColumnsRoleVisibility:
    """REQ-128: getColumns() result filtered by role visibility via CatalogIndex."""

    def test_hidden_column_absent_from_get_columns_result(self):
        # REQ-128: analyst cannot see customer_pii (visible_to=["admin"])
        cols = _catalog_columns_for_role(1, _TABLES, _COL_TYPES, role_id="analyst")
        assert "customer_pii" not in cols, (
            f"analyst must not see customer_pii in getColumns(), got: {cols}"
        )

    def test_visible_column_present_in_get_columns_result(self):
        # REQ-128: unrestricted columns appear for every role
        for role_id in ("admin", "analyst"):
            cols = _catalog_columns_for_role(1, _TABLES, _COL_TYPES, role_id=role_id)
            assert "id" in cols, f"id must be visible to {role_id}, got: {cols}"
            assert "amount" in cols, f"amount must be visible to {role_id}, got: {cols}"

    def test_different_roles_see_different_columns(self):
        # REQ-128: admin sees customer_pii; analyst sees region_code; neither sees the other's
        admin_cols = _catalog_columns_for_role(1, _TABLES, _COL_TYPES, role_id="admin")
        analyst_cols = _catalog_columns_for_role(1, _TABLES, _COL_TYPES, role_id="analyst")

        assert "customer_pii" in admin_cols, f"admin must see customer_pii, got: {admin_cols}"
        assert "region_code" not in admin_cols, (
            f"admin must not see region_code (analyst-only), got: {admin_cols}"
        )

        assert "region_code" in analyst_cols, f"analyst must see region_code, got: {analyst_cols}"
        assert "customer_pii" not in analyst_cols, (
            f"analyst must not see customer_pii (admin-only), got: {analyst_cols}"
        )

    def test_admin_role_sees_all_columns(self):
        # REQ-128: admin (visible_to includes "admin") sees all columns including restricted
        cols = _catalog_columns_for_role(1, _TABLES, _COL_TYPES, role_id="admin")
        assert "id" in cols
        assert "amount" in cols
        assert "customer_pii" in cols, f"admin must see customer_pii, got: {cols}"

    def test_catalog_index_column_count_matches_role_visible_columns(self):
        # REQ-128: CatalogIndex column count equals the role-visible column count from SchemaInput
        for role_id, expected_cols in [
            ("admin", {"id", "amount", "customer_pii"}),
            ("analyst", {"id", "amount", "region_code"}),
        ]:
            cols = _catalog_columns_for_role(1, _TABLES, _COL_TYPES, role_id=role_id)
            col_set = set(cols)
            assert expected_cols <= col_set, f"role={role_id}: expected {expected_cols} ⊆ {col_set}"
            # Virtual system columns (_name_, _domain_) may also be present — that is OK
            non_expected = col_set - expected_cols - {"_name_", "_domain_"}
            assert not non_expected, (
                f"role={role_id}: unexpected extra columns in catalog: {non_expected}"
            )

    def test_is_columns_populated_with_role_visible_cols_only(self):
        # REQ-128: _populate_is_columns creates DuckDB rows only for role-visible columns
        si = _make_si(_TABLES, _COL_TYPES, role_id="analyst")
        ctx = build_context(si)
        idx = _build_catalog_index(ctx, _COL_TYPES)

        db = duckdb.connect(":memory:")
        _populate_is_columns(db, idx)

        rows = db.execute("SELECT column_name FROM _is_columns").fetchall()
        col_names = {r[0] for r in rows}

        assert "id" in col_names
        assert "amount" in col_names
        assert "region_code" in col_names
        # customer_pii is admin-only — must not appear
        assert "customer_pii" not in col_names, (
            f"customer_pii must not appear in _is_columns for analyst, got: {col_names}"
        )

    def test_answer_get_columns_returns_role_filtered_columns(self):
        # REQ-128: answer() for an information_schema.columns query respects role visibility;
        # verify via classify() then answer() end-to-end with a mocked state.
        sql = (
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'sales' AND table_name = 'orders'"
        )
        assert classify(sql) == "INTERCEPT"

        # Build real compilation context for analyst
        si = _make_si(_TABLES, _COL_TYPES, role_id="analyst")
        ctx = build_context(si)

        state = MagicMock()
        state.contexts = {"analyst": ctx}
        state.schema_build_cache = {
            "column_types": _COL_TYPES,
            "tables": _TABLES,
            "domains": [{"id": "sales", "graphql_alias": None}],
        }
        state.engine_conn = None

        result = answer(sql, "analyst", state)
        returned_cols = {row[0] for row in result.rows}

        assert "customer_pii" not in returned_cols, (
            f"answer() must not return customer_pii for analyst, got: {returned_cols}"
        )
        # id and amount should be present when available
        # (virtual cols _name_/_domain_ may also appear — that is acceptable)
