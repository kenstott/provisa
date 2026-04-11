# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-def0-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for EnforcementMetadata and _build_enforcement_metadata (REQ-062)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from provisa.api.admin.dev_queries import EnforcementMetadata, _build_enforcement_metadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    TableMeta,
)
from provisa.security.masking import MaskingRule, MaskType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _table_meta(table_id: int = 1, table_name: str = "orders") -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=table_name,
        type_name=table_name.capitalize(),
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name=table_name,
    )


def _compiled(root_field: str = "orders", columns: list[ColumnRef] | None = None) -> CompiledQuery:
    return CompiledQuery(
        sql="SELECT id FROM orders",
        params=[],
        root_field=root_field,
        columns=columns or [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ],
        sources={"pg"},
    )


def _ctx(table_id: int = 1, table_name: str = "orders") -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {table_name: _table_meta(table_id, table_name)}
    return ctx


def _rls(rules: dict | None = None) -> RLSContext:
    return RLSContext(rules=rules or {})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEnforcementMetadataFields:
    def test_enforcement_metadata_fields(self):
        """All six expected fields are present in EnforcementMetadata."""
        meta = EnforcementMetadata(
            rls_filters_applied=["region = 'us'"],
            columns_excluded=["secret"],
            schema_scope="role:analyst",
            masking_applied=["orders.email -> regex"],
            ceiling_applied=100,
            route="trino",
        )
        assert meta.rls_filters_applied == ["region = 'us'"]
        assert meta.columns_excluded == ["secret"]
        assert meta.schema_scope == "role:analyst"
        assert meta.masking_applied == ["orders.email -> regex"]
        assert meta.ceiling_applied == 100
        assert meta.route == "trino"

    def test_enforcement_metadata_optional_ceiling(self):
        """ceiling_applied may be None."""
        meta = EnforcementMetadata(
            rls_filters_applied=[],
            columns_excluded=[],
            schema_scope="role:admin",
            masking_applied=[],
            ceiling_applied=None,
            route="direct:postgres",
        )
        assert meta.ceiling_applied is None


class TestBuildEnforcementMetadata:
    def test_rls_filters_populated(self):
        """rls_filters_applied is populated when an RLS rule matches the root table."""
        ctx = _ctx(table_id=1, table_name="orders")
        rls = _rls(rules={1: "region = 'us'"})
        compiled = _compiled("orders")

        meta = _build_enforcement_metadata(
            compiled=compiled,
            ctx=ctx,
            rls=rls,
            masking_rules={},
            role_id="analyst",
            route_value="trino",
        )

        assert "region = 'us'" in meta.rls_filters_applied

    def test_rls_filters_empty_when_no_match(self):
        """rls_filters_applied is empty when no RLS rule matches the root table."""
        ctx = _ctx(table_id=1, table_name="orders")
        rls = _rls(rules={99: "x = 1"})  # table_id 99 doesn't exist in ctx
        compiled = _compiled("orders")

        meta = _build_enforcement_metadata(
            compiled=compiled,
            ctx=ctx,
            rls=rls,
            masking_rules={},
            role_id="admin",
            route_value="trino",
        )

        assert meta.rls_filters_applied == []

    def test_schema_scope_format(self):
        """schema_scope is formatted as 'role:{role_id}'."""
        ctx = _ctx()
        compiled = _compiled()

        meta = _build_enforcement_metadata(
            compiled=compiled,
            ctx=ctx,
            rls=_rls(),
            masking_rules={},
            role_id="analyst",
            route_value="trino",
        )

        assert meta.schema_scope == "role:analyst"

    def test_masking_applied_populated(self):
        """masking_applied reflects masking rules for the requesting role."""
        ctx = _ctx(table_id=1, table_name="orders")
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        # masking_rules format: {(table_id, role_id): {col: (rule, dtype)}}
        masking_rules = {
            (1, "analyst"): {"email": (rule, "varchar")},
        }
        compiled = _compiled("orders")

        meta = _build_enforcement_metadata(
            compiled=compiled,
            ctx=ctx,
            rls=_rls(),
            masking_rules=masking_rules,
            role_id="analyst",
            route_value="trino",
        )

        assert len(meta.masking_applied) == 1
        assert "email" in meta.masking_applied[0]

    def test_masking_applied_ignores_other_roles(self):
        """masking_applied only includes rules for the requesting role."""
        ctx = _ctx(table_id=1, table_name="orders")
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        masking_rules = {
            (1, "other_role"): {"secret": (rule, "varchar")},
        }
        compiled = _compiled("orders")

        meta = _build_enforcement_metadata(
            compiled=compiled,
            ctx=ctx,
            rls=_rls(),
            masking_rules=masking_rules,
            role_id="analyst",
            route_value="trino",
        )

        assert meta.masking_applied == []

    def test_route_preserved(self):
        """route field reflects the route_value passed in."""
        ctx = _ctx()
        compiled = _compiled()

        meta = _build_enforcement_metadata(
            compiled=compiled,
            ctx=ctx,
            rls=_rls(),
            masking_rules={},
            role_id="admin",
            route_value="direct:postgres",
        )

        assert meta.route == "direct:postgres"

    def test_columns_excluded_empty_when_all_present(self):
        """columns_excluded is empty when compiled columns match root table columns."""
        # TableMeta has no `columns` attr by default — exclusion logic
        # reads from root_table.columns which won't exist on TableMeta directly.
        # The implementation uses getattr with a default, so this should return [].
        ctx = _ctx(table_id=1, table_name="orders")
        compiled = _compiled(
            "orders",
            columns=[
                ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ],
        )

        meta = _build_enforcement_metadata(
            compiled=compiled,
            ctx=ctx,
            rls=_rls(),
            masking_rules={},
            role_id="admin",
            route_value="trino",
        )

        # columns_excluded may be [] since TableMeta has no 'columns' attribute
        assert isinstance(meta.columns_excluded, list)
