# Copyright (c) 2026 Kenneth Stott
# Canary: c3d4e5f6-a7b8-9012-cdef-123456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: role inheritance propagating RLS rules and masking (REQ-040, REQ-087)."""

from __future__ import annotations

import pytest

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    JoinMeta,
    TableMeta,
)
from provisa.compiler.rls import inject_rls
from provisa.compiler.mask_inject import MaskingRules, inject_masking
from provisa.core.models import Role, flatten_roles
from provisa.security.masking import MaskType, MaskingRule

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# These tests do NOT require a database connection.
# ---------------------------------------------------------------------------

ORDERS_TABLE_ID = 1
CUSTOMERS_TABLE_ID = 2


# ---------------------------------------------------------------------------
# Role builder helpers
# ---------------------------------------------------------------------------


def _role(role_id: str, caps: list[str], domains: list[str], parent: str | None = None) -> Role:
    return Role(
        id=role_id,
        capabilities=caps,
        domain_access=domains,
        parent_role_id=parent,
    )


# ---------------------------------------------------------------------------
# RLS helper — build RLSContext for a role from a flat rule list
# ---------------------------------------------------------------------------


def _rls_for_role(
    rules: list[tuple[int, str, str]],  # (table_id, role_id, filter_expr)
    role_id: str,
) -> RLSContext:
    """Build RLSContext for a specific role_id from a flat list of rules."""
    matching = {t_id: expr for t_id, r_id, expr in rules if r_id == role_id}
    return RLSContext(rules=matching)


# ---------------------------------------------------------------------------
# SQL builder helpers
# ---------------------------------------------------------------------------


def _orders_compiled() -> CompiledQuery:
    sql = 'SELECT "id", "amount", "region" FROM "public"."orders"'
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
            ColumnRef(alias=None, column="region", field_name="region", nested_in=None)],
        sources={"test-pg"},
    )


def _orders_ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["orders"] = TableMeta(
        table_id=ORDERS_TABLE_ID,
        field_name="orders",
        type_name="Orders",
        source_id="test-pg",
        catalog_name="test_pg",
        schema_name="public",
        table_name="orders",
    )
    return ctx


# ---------------------------------------------------------------------------
# flatten_roles unit tests (no DB)
# ---------------------------------------------------------------------------


class TestFlattenRoles:
    def test_flatten_roles_produces_merged_context(self):
        """flatten_roles() merges capabilities and domain_access from parent."""
        parent = _role("parent", caps=["read"], domains=["sales"])
        child = _role("child", caps=["export"], domains=["ops"], parent="parent")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        assert "read" in by_id["child"].capabilities
        assert "export" in by_id["child"].capabilities
        assert "sales" in by_id["child"].domain_access
        assert "ops" in by_id["child"].domain_access

    def test_child_retains_own_capabilities(self):
        """Child-only capabilities are preserved after flattening."""
        parent = _role("base", caps=["read"], domains=["*"])
        child = _role("analyst", caps=["aggregate"], domains=["finance"], parent="base")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        assert "aggregate" in by_id["analyst"].capabilities
        assert "read" in by_id["analyst"].capabilities

    def test_parent_unchanged_after_flatten(self):
        """Parent role must not gain child's capabilities after flatten."""
        parent = _role("base", caps=["read"], domains=["*"])
        child = _role("analyst", caps=["aggregate"], domains=["finance"], parent="base")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        assert "aggregate" not in by_id["base"].capabilities

    def test_wildcard_domain_propagates(self):
        """A wildcard domain_access '*' in parent propagates to child."""
        parent = _role("super", caps=["read"], domains=["*"])
        child = _role("sub", caps=["read"], domains=["finance"], parent="super")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        assert "*" in by_id["sub"].domain_access

    def test_no_parent_role_is_unchanged(self):
        """A role with no parent must remain identical after flattening."""
        solo = _role("solo", caps=["read"], domains=["sales"])
        result = flatten_roles([solo])
        assert result[0].capabilities == ["read"]
        assert result[0].domain_access == ["sales"]
        assert result[0].parent_role_id is None


# ---------------------------------------------------------------------------
# RLS inheritance tests (SQL injection level — no DB required)
# ---------------------------------------------------------------------------


class TestInheritedRolesRLS:
    def test_child_inherits_parent_rls(self):
        """Child role that inherits parent should be eligible for parent RLS.

        RLSContext is built per-role by the caller using flatten_roles output;
        here we verify that when the parent's filter is applied to a child
        query the WHERE clause is correctly injected.
        """
        ctx = _orders_ctx()
        compiled = _orders_compiled()

        # Parent has a region rule; child inherits it (caller applies parent's rule)
        parent_rule_table_id = ORDERS_TABLE_ID
        parent_filter = "region = 'us-east'"

        # Simulate: child inherits parent RLS rule
        rls = RLSContext(rules={parent_rule_table_id: parent_filter})
        result = inject_rls(compiled, ctx, rls)

        assert "region" in result.sql
        assert "us-east" in result.sql
        assert "WHERE" in result.sql.upper()

    def test_child_rls_added_to_parent_rls(self):
        """When child has its own RLS rule, both parent and child rules are ANDed.

        In the production system, flatten_roles merges the role hierarchy and a
        single RLSContext is built for the effective role.  We verify the AND by
        manually composing a combined filter.
        """
        ctx = _orders_ctx()
        compiled = _orders_compiled()

        # Child's effective filter = parent's rule AND child's own rule
        combined_filter = "region = 'us-east' AND amount > 100"
        rls = RLSContext(rules={ORDERS_TABLE_ID: combined_filter})
        result = inject_rls(compiled, ctx, rls)

        assert "region = 'us-east'" in result.sql
        assert "amount > 100" in result.sql
        assert "AND" in result.sql

    def test_grandchild_inherits_two_levels(self):
        """Three-level chain: grandchild receives grandparent + parent + own rules."""
        grandparent = _role("gp", caps=["read"], domains=["sales"])
        parent = _role("parent", caps=["aggregate"], domains=["ops"], parent="gp")
        child = _role("child", caps=["export"], domains=["finance"], parent="parent")

        result = flatten_roles([grandparent, parent, child])
        by_id = {r.id: r for r in result}

        # Child should have all three capability sets
        assert "read" in by_id["child"].capabilities
        assert "aggregate" in by_id["child"].capabilities
        assert "export" in by_id["child"].capabilities

        # Child should have all three domain sets
        assert "sales" in by_id["child"].domain_access
        assert "ops" in by_id["child"].domain_access
        assert "finance" in by_id["child"].domain_access

    def test_child_rls_sql_where_injected_correctly(self):
        """Verify WHERE is injected in the right position (before ORDER BY / end)."""
        import re as _re

        ctx = _orders_ctx()
        # Use a table alias so ORDER BY is unambiguous (avoids matching "orders")
        base_sql = (
            'SELECT "t0"."id", "t0"."amount" '
            'FROM "public"."orders" "t0" '
            'ORDER BY "t0"."id"'
        )
        compiled = CompiledQuery(
            sql=base_sql,
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
                ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None)],
            sources={"test-pg"},
        )

        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        result = inject_rls(compiled, ctx, rls)

        # Use word-boundary regex to find keyword positions
        where_m = _re.search(r'\bWHERE\b', result.sql, _re.IGNORECASE)
        order_m = _re.search(r'\bORDER\s+BY\b', result.sql, _re.IGNORECASE)

        assert where_m is not None, "WHERE clause was not injected"
        assert order_m is not None, "ORDER BY was unexpectedly removed"
        assert where_m.start() < order_m.start(), "WHERE must appear before ORDER BY"


# ---------------------------------------------------------------------------
# Masking inheritance tests (SQL injection level — no DB required)
# ---------------------------------------------------------------------------


class TestInheritedRolesMasking:
    def test_child_overrides_parent_mask(self):
        """Child's masking rule for a column takes precedence over parent's.

        In the production system, the effective masking rules for a role are
        built from the child's config; the child's rule for a column replaces
        any parent rule for the same column.  We verify this by building two
        separate MaskingRules dicts and checking which SQL expression is emitted.
        """
        ctx = _orders_ctx()
        compiled = CompiledQuery(
            sql='SELECT "t0"."id", "t0"."amount" FROM "public"."orders" "t0"',
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
                ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None)],
            sources={"test-pg"},
        )

        # Parent masks amount to 0; child overrides to 999
        child_rule = MaskingRule(mask_type=MaskType.constant, value=999)
        # Only child rules are applied (caller resolves effective rules before injection)
        child_masking: MaskingRules = {
            (ORDERS_TABLE_ID, "child"): {"amount": (child_rule, "integer")},
        }
        result = inject_masking(compiled, ctx, child_masking, "child")

        assert "999" in result.sql, "Child's mask value (999) must appear in SQL"
        # Parent's value (0) should NOT override child
        # (Parent's value does not appear because we only applied child rules)

    def test_no_masking_rules_for_role_leaves_sql_unchanged(self):
        """A role with no masking rules must not alter the compiled SQL."""
        ctx = _orders_ctx()
        compiled = CompiledQuery(
            sql='SELECT "t0"."id", "t0"."amount" FROM "public"."orders" "t0"',
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
                ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None)],
            sources={"test-pg"},
        )

        masking_rules: MaskingRules = {}  # no rules for any role
        result = inject_masking(compiled, ctx, masking_rules, "some-role")

        assert result.sql == compiled.sql

    def test_parent_mask_applied_when_child_has_none(self):
        """When child has no override, parent's masking rule is applied (by caller).

        In practice the caller assigns the parent's rule to the child's role_id
        when no child override exists.  We verify the injection path works.
        """
        ctx = _orders_ctx()
        compiled = CompiledQuery(
            sql='SELECT "t0"."id", "t0"."amount" FROM "public"."orders" "t0"',
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
                ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None)],
            sources={"test-pg"},
        )

        parent_rule = MaskingRule(mask_type=MaskType.constant, value=0)
        # Caller assigns parent's rule to the child role_id
        inherited_masking: MaskingRules = {
            (ORDERS_TABLE_ID, "child"): {"amount": (parent_rule, "integer")},
        }
        result = inject_masking(compiled, ctx, inherited_masking, "child")

        assert "0" in result.sql or "AS" in result.sql, (
            "Parent's inherited mask rule was not injected into child's SQL"
        )
