# Copyright (c) 2026 Kenneth Stott
# Canary: 11f8c32b-4658-4756-b6a8-7d1b845fa3ef
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for column-level masking (REQ-087 through REQ-091).

Tests the full masking pipeline: config → compile → govern (compiler/stage2) → execute.
Uses mocked execution to verify SQL transformations without a live DB.
"""

from __future__ import annotations

from provisa.compiler.mask_inject import MaskingRules
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    JoinMeta,
    TableMeta,
)
from provisa.security.masking import (
    MaskType,
    MaskingRule,
    MaskingValidationError,
    validate_masking_rule,
)

import pytest

from tests.helpers import governed_for_role


# --- Fixtures ---


def _meta(table_id=1, field_name="orders", table_name="orders", source_id="pg"):
    return TableMeta(
        table_id=table_id,
        field_name=field_name,
        type_name=field_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name="public",
        table_name=table_name,
    )


def _ctx_with_customers_and_orders():
    orders_meta = _meta(table_id=1, field_name="orders", table_name="orders")
    customers_meta = _meta(table_id=2, field_name="customers", table_name="customers")
    ctx = CompilationContext()
    ctx.tables = {"orders": orders_meta, "customers": customers_meta}
    ctx.joins = {
        ("Orders", "customers"): JoinMeta(
            source_column="customer_id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=customers_meta,
            cardinality="many-to-one",
        ),
    }
    return ctx


# --- Full Pipeline Tests ---


class TestAdminSeesRawData:
    """Admin role with no masking rules sees raw values (REQ-087)."""

    def test_admin_email_unmasked(self):
        compiled = CompiledQuery(
            sql='SELECT "email", "name" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[
                ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
                ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
            ],
            sources={"pg"},
        )
        ctx = CompilationContext()
        ctx.tables = {
            "customers": _meta(table_id=2, field_name="customers", table_name="customers")
        }
        ctx.joins = {}

        # Analyst has masking, admin does not
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(
                        mask_type=MaskType.regex, pattern="^(.{2}).*(@.*)$", replace="$1***$2"
                    ),
                    "varchar",
                ),
            }
        }
        result = governed_for_role(compiled.sql, ctx, "admin", masking_rules=rules)
        # Admin SQL unchanged — raw columns
        assert result == compiled.sql
        assert "REGEXP_REPLACE" not in result


class TestAnalystRegexMasking:
    """Analyst sees regex-masked email: al***@example.com (REQ-088)."""

    def test_analyst_email_regex_masked(self):
        compiled = CompiledQuery(
            sql='SELECT "email", "region" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[
                ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
                ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
            ],
            sources={"pg"},
        )
        ctx = CompilationContext()
        ctx.tables = {
            "customers": _meta(table_id=2, field_name="customers", table_name="customers")
        }
        ctx.joins = {}

        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(
                        mask_type=MaskType.regex, pattern="^(.{2}).*(@.*)$", replace="$1***$2"
                    ),
                    "varchar",
                ),
            }
        }
        result = governed_for_role(compiled.sql, ctx, "analyst", masking_rules=rules)
        assert "REGEXP_REPLACE" in result
        assert 'AS "email"' in result
        # region not masked
        assert '"region"' in result
        assert result.count("REGEXP_REPLACE") == 1

    def test_analyst_name_initials_masked(self):
        compiled = CompiledQuery(
            sql='SELECT "name" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[
                ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
            ],
            sources={"pg"},
        )
        ctx = CompilationContext()
        ctx.tables = {
            "customers": _meta(table_id=2, field_name="customers", table_name="customers")
        }
        ctx.joins = {}

        rules: MaskingRules = {
            (2, "analyst"): {
                "name": (
                    MaskingRule(
                        mask_type=MaskType.regex, pattern="^(.).* (.).*$", replace="$1. $2."
                    ),
                    "varchar",
                ),
            }
        }
        result = governed_for_role(compiled.sql, ctx, "analyst", masking_rules=rules)
        assert "REGEXP_REPLACE" in result
        assert 'AS "name"' in result


class TestMaskedViewerConstantMasking:
    """masked_viewer sees constant-masked values (REQ-089)."""

    def test_email_constant_masked(self):
        compiled = CompiledQuery(
            sql='SELECT "email" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[
                ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
            ],
            sources={"pg"},
        )
        ctx = CompilationContext()
        ctx.tables = {
            "customers": _meta(table_id=2, field_name="customers", table_name="customers")
        }
        ctx.joins = {}

        rules: MaskingRules = {
            (2, "masked_viewer"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="***@***.***"),
                    "varchar",
                ),
            }
        }
        result = governed_for_role(compiled.sql, ctx, "masked_viewer", masking_rules=rules)
        assert "'***@***.***' AS \"email\"" in result


class TestNumericConstantMasking:
    """Numeric column masked with constant value (REQ-089)."""

    def test_amount_masked_to_zero(self):
        compiled = CompiledQuery(
            sql='SELECT "t0"."amount" FROM "public"."orders" "t0"',
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None),
            ],
            sources={"pg"},
        )
        ctx = CompilationContext()
        ctx.tables = {"orders": _meta()}
        ctx.joins = {}

        rules: MaskingRules = {
            (1, "masked_viewer"): {
                "amount": (
                    MaskingRule(mask_type=MaskType.constant, value=0),
                    "integer",
                ),
            }
        }
        result = governed_for_role(compiled.sql, ctx, "masked_viewer", masking_rules=rules)
        assert '0 AS "amount"' in result


class TestDateTruncateMasking:
    """Date column truncated to month precision (REQ-090)."""

    def test_created_at_truncated(self):
        compiled = CompiledQuery(
            sql='SELECT "t0"."created_at" FROM "public"."orders" "t0"',
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="created_at", field_name="created_at", nested_in=None),
            ],
            sources={"pg"},
        )
        ctx = CompilationContext()
        ctx.tables = {"orders": _meta()}
        ctx.joins = {}

        rules: MaskingRules = {
            (1, "analyst"): {
                "created_at": (
                    MaskingRule(mask_type=MaskType.truncate, precision="month"),
                    "timestamp",
                ),
            }
        }
        result = governed_for_role(compiled.sql, ctx, "analyst", masking_rules=rules)
        # sqlglot re-serializes the DATE_TRUNC field literal in uppercase (postgres accepts
        # either case) when it regenerates SQL through the governed pipeline.
        assert "DATE_TRUNC('MONTH'" in result
        assert 'AS "created_at"' in result


class TestJoinedTableMasking:
    """Masking applies correctly across JOINed tables."""

    def test_joined_customer_email_masked(self):
        ctx = _ctx_with_customers_and_orders()
        compiled = CompiledQuery(
            sql='SELECT "t0"."id", "t1"."email" FROM "public"."orders" "t0" LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"',
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
                ColumnRef(alias="t1", column="email", field_name="email", nested_in="customers"),
            ],
            sources={"pg"},
        )
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(
                        mask_type=MaskType.regex, pattern="^(.{2}).*(@.*)$", replace="$1***$2"
                    ),
                    "varchar",
                ),
            }
        }
        result = governed_for_role(compiled.sql, ctx, "analyst", masking_rules=rules)
        assert "REGEXP_REPLACE" in result
        assert 'AS "email"' in result
        # Root table columns unchanged
        assert '"t0"."id"' in result


class TestDifferentMasksPerRole:
    """Same column, different mask for different roles (REQ-087)."""

    def test_analyst_gets_regex_viewer_gets_constant(self):
        compiled = CompiledQuery(
            sql='SELECT "email" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[
                ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
            ],
            sources={"pg"},
        )
        ctx = CompilationContext()
        ctx.tables = {
            "customers": _meta(table_id=2, field_name="customers", table_name="customers")
        }
        ctx.joins = {}

        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(
                        mask_type=MaskType.regex, pattern="^(.{2}).*(@.*)$", replace="$1***$2"
                    ),
                    "varchar",
                ),
            },
            (2, "masked_viewer"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="***@***.***"),
                    "varchar",
                ),
            },
        }

        analyst_result = governed_for_role(compiled.sql, ctx, "analyst", masking_rules=rules)
        assert "REGEXP_REPLACE" in analyst_result

        viewer_result = governed_for_role(compiled.sql, ctx, "masked_viewer", masking_rules=rules)
        assert "'***@***.***'" in viewer_result
        assert "REGEXP_REPLACE" not in viewer_result


class TestTypeValidationAtConfigTime:
    """Invalid masking configs rejected at config load time (REQ-091)."""

    def test_regex_on_integer_rejected(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=".*", replace="X")
        with pytest.raises(MaskingValidationError):
            validate_masking_rule(rule, "amount", "integer", True)

    def test_truncate_on_varchar_rejected(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision="month")
        with pytest.raises(MaskingValidationError):
            validate_masking_rule(rule, "name", "varchar", True)

    def test_null_on_not_null_column_rejected(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        with pytest.raises(MaskingValidationError):
            validate_masking_rule(rule, "id", "integer", is_nullable=False)
