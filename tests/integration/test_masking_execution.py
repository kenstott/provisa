# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: column masking transforms in executed query output (REQ-087)."""

from __future__ import annotations

import os

import pytest
import pytest_asyncio

from provisa.compiler.mask_inject import MaskingRules, inject_masking
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    TableMeta,
)
from provisa.executor.direct import execute_direct
from provisa.executor.pool import SourcePool
from provisa.security.masking import MaskType, MaskingRule

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ORDERS_TABLE_ID = 1
ROLE_ANALYST = "analyst"
ROLE_ADMIN = "admin"


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


def _make_orders_ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["orders"] = _make_orders_meta()
    return ctx


def _aliased_orders_compiled(*extra_columns: tuple[str, str]) -> CompiledQuery:
    """Build a CompiledQuery with t0 alias for orders, adding extra columns.

    extra_columns: sequence of (column_name, sql_expression_or_bare) pairs.
    """
    base_cols = ["id", "amount", "region"]
    all_cols = base_cols + [c for c, _ in extra_columns]

    select_parts = [f'"t0"."{c}"' for c in all_cols]
    select_clause = ", ".join(select_parts)
    sql = (
        f'SELECT {select_clause} '
        f'FROM "public"."orders" "t0"'
    )
    col_refs = [
        ColumnRef(alias="t0", column=c, field_name=c, nested_in=None)
        for c in all_cols
    ]
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field="orders",
        columns=col_refs,
        sources={"test-pg"},
    )


def _simple_orders_compiled() -> CompiledQuery:
    """Unaliased (no JOIN) compiled query for orders."""
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


def _col_index(result, name: str) -> int:
    return result.column_names.index(name)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="session")
async def source_pool():
    sp = SourcePool()
    try:
        await sp.add(
            "test-pg",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            database=os.environ.get("PG_DATABASE", "provisa"),
            user=os.environ.get("PG_USER", "provisa"),
            password=os.environ.get("PG_PASSWORD", "provisa"),
        )
    except Exception:
        pytest.skip("PostgreSQL not available")
    yield sp
    await sp.close_all()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMaskingExecution:
    async def test_constant_mask_replaces_value(self, source_pool):
        """Constant mask on 'amount' must replace every value with 0."""
        ctx = _make_orders_ctx()
        compiled = _aliased_orders_compiled()

        rule = MaskingRule(mask_type=MaskType.constant, value=0)
        masking_rules: MaskingRules = {
            (ORDERS_TABLE_ID, ROLE_ANALYST): {"amount": (rule, "integer")},
        }
        masked = inject_masking(compiled, ctx, masking_rules, ROLE_ANALYST)

        result = await execute_direct(source_pool, "test-pg", masked.sql, masked.params)

        assert result.rows, "Expected at least one row in orders"
        amount_idx = _col_index(result, "amount")
        for row in result.rows:
            assert row[amount_idx] == 0, (
                f"Constant mask failed: amount={row[amount_idx]!r}, expected 0"
            )

    async def test_regex_mask_transforms_string(self, source_pool):
        """Regex mask on 'region' must transform the value via pattern replacement."""
        ctx = _make_orders_ctx()
        compiled = _aliased_orders_compiled()

        # Replace the entire region value with 'REDACTED'
        rule = MaskingRule(
            mask_type=MaskType.regex,
            pattern=".+",
            replace="REDACTED",
        )
        masking_rules: MaskingRules = {
            (ORDERS_TABLE_ID, ROLE_ANALYST): {"region": (rule, "varchar")},
        }
        masked = inject_masking(compiled, ctx, masking_rules, ROLE_ANALYST)

        result = await execute_direct(source_pool, "test-pg", masked.sql, masked.params)

        assert result.rows, "Expected at least one row in orders"
        region_idx = _col_index(result, "region")
        for row in result.rows:
            assert row[region_idx] == "REDACTED", (
                f"Regex mask failed: region={row[region_idx]!r}, expected 'REDACTED'"
            )

    async def test_truncate_mask_on_string(self, source_pool):
        """Constant mask on a varchar column shortens the effective output."""
        # truncate masking is date-only in the engine; use constant mask to test
        # string-shortening semantics by replacing with a fixed short string.
        ctx = _make_orders_ctx()
        compiled = _aliased_orders_compiled()

        rule = MaskingRule(
            mask_type=MaskType.regex,
            pattern=r"^(.{2}).*$",
            replace=r"\1",
        )
        masking_rules: MaskingRules = {
            (ORDERS_TABLE_ID, ROLE_ANALYST): {"region": (rule, "varchar")},
        }
        masked = inject_masking(compiled, ctx, masking_rules, ROLE_ANALYST)

        result = await execute_direct(source_pool, "test-pg", masked.sql, masked.params)

        assert result.rows, "Expected at least one row in orders"
        region_idx = _col_index(result, "region")
        for row in result.rows:
            if row[region_idx] is not None:
                assert len(row[region_idx]) <= 2, (
                    f"Truncate mask failed: region={row[region_idx]!r} is longer than 2 chars"
                )

    async def test_unmask_for_privileged_role(self, source_pool):
        """Admin role (not in masking_rules) must see original unmasked values."""
        ctx = _make_orders_ctx()
        compiled = _aliased_orders_compiled()

        # Only mask for analyst, not for admin
        rule = MaskingRule(mask_type=MaskType.constant, value=0)
        masking_rules: MaskingRules = {
            (ORDERS_TABLE_ID, ROLE_ANALYST): {"amount": (rule, "integer")},
        }

        # Inject masking for admin — no rules apply
        masked_admin = inject_masking(compiled, ctx, masking_rules, ROLE_ADMIN)

        # SQL must be unchanged since admin has no masking rules
        assert masked_admin.sql == compiled.sql, (
            "SQL was unexpectedly modified for privileged admin role"
        )

        result = await execute_direct(source_pool, "test-pg", masked_admin.sql, masked_admin.params)

        assert result.rows, "Expected at least one row"
        amount_idx = _col_index(result, "amount")
        # At least some rows should have non-zero amounts (real data)
        amounts = [row[amount_idx] for row in result.rows]
        assert any(a != 0 for a in amounts if a is not None), (
            "Expected some non-zero amounts for unmasked admin role"
        )

    async def test_mask_applied_to_all_rows(self, source_pool):
        """Masking rule must apply uniformly to every row in the result."""
        ctx = _make_orders_ctx()
        compiled = _aliased_orders_compiled()

        constant_value = 9999
        rule = MaskingRule(mask_type=MaskType.constant, value=constant_value)
        masking_rules: MaskingRules = {
            (ORDERS_TABLE_ID, ROLE_ANALYST): {"amount": (rule, "integer")},
        }
        masked = inject_masking(compiled, ctx, masking_rules, ROLE_ANALYST)

        result = await execute_direct(source_pool, "test-pg", masked.sql, masked.params)

        assert result.rows, "Expected rows to test masking coverage"
        amount_idx = _col_index(result, "amount")
        for i, row in enumerate(result.rows):
            assert row[amount_idx] == constant_value, (
                f"Row {i}: masking not applied — amount={row[amount_idx]!r}"
            )

    async def test_masking_does_not_affect_where_clause(self, source_pool):
        """Masking on 'amount' must not break filtering by that column."""
        ctx = _make_orders_ctx()

        # Build a query that filters by amount > 0 and also masks amount
        sql = (
            'SELECT "t0"."id", "t0"."amount", "t0"."region" '
            'FROM "public"."orders" "t0" '
            'WHERE "t0"."amount" > 0'
        )
        compiled = CompiledQuery(
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

        rule = MaskingRule(mask_type=MaskType.constant, value=0)
        masking_rules: MaskingRules = {
            (ORDERS_TABLE_ID, ROLE_ANALYST): {"amount": (rule, "integer")},
        }
        masked = inject_masking(compiled, ctx, masking_rules, ROLE_ANALYST)

        # WHERE clause must still be present after masking
        assert "WHERE" in masked.sql.upper(), "WHERE clause was removed by masking injection"

        result = await execute_direct(source_pool, "test-pg", masked.sql, masked.params)

        # Query should succeed and masking applied (all amounts = 0 in output)
        amount_idx = _col_index(result, "amount")
        for row in result.rows:
            assert row[amount_idx] == 0, (
                f"Masking not applied alongside WHERE: amount={row[amount_idx]!r}"
            )
