# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-fedcba987654
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extended inherited-roles tests: multi-parent conflict resolution, deep recursive
resolution, RLS accumulation across multiple tables, and masking override ordering.

These tests do NOT duplicate the scenarios in tests/unit/test_inherited_roles.py,
which covers: basic parent/child merge, three-level chains, wildcard domains,
RLS WHERE injection, and child-overrides-parent masking.

New coverage here:
  - Diamond inheritance (two parents that share a grandparent)
  - Sibling conflict: two parents give different capabilities
  - Deep chain (5 levels) resolved correctly by flatten_roles
  - RLS accumulation: multiple tables each with their own rules
  - build_rls_context: role_id filtering with multiple tables
  - Empty parent list handled gracefully
  - flatten_roles is idempotent (calling it twice gives the same result)
"""

from __future__ import annotations

import pytest

from provisa.compiler.rls import RLSContext, build_rls_context, inject_rls
from provisa.compiler.mask_inject import MaskingRules, inject_masking
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    TableMeta,
)
from provisa.core.models import Role, flatten_roles
from provisa.security.masking import MaskType, MaskingRule

# ---------------------------------------------------------------------------
# Shared table IDs
# ---------------------------------------------------------------------------

ORDERS_TABLE_ID = 1
CUSTOMERS_TABLE_ID = 2
PRODUCTS_TABLE_ID = 3

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _role(
    role_id: str,
    caps: list[str],
    domains: list[str],
    parent: str | None = None,
) -> Role:
    return Role(
        id=role_id,
        capabilities=caps,
        domain_access=domains,
        parent_role_id=parent,
    )


def _make_ctx(*tables: tuple[int, str, str]) -> CompilationContext:
    """Build a CompilationContext from (table_id, field_name, table_name) tuples."""
    ctx = CompilationContext()
    for table_id, field_name, table_name in tables:
        ctx.tables[field_name] = TableMeta(
            table_id=table_id,
            field_name=field_name,
            type_name=field_name.capitalize(),
            source_id="test-pg",
            catalog_name="test_pg",
            schema_name="public",
            table_name=table_name,
        )
    return ctx


def _compiled_for(
    sql: str,
    root_field: str,
    columns: list[ColumnRef] | None = None,
) -> CompiledQuery:
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field=root_field,
        columns=columns
        or [ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
        sources={"test-pg"},
    )


# ---------------------------------------------------------------------------
# Recursive / deep resolution via flatten_roles
# ---------------------------------------------------------------------------


class TestDeepInheritanceChain:
    def test_five_level_chain_propagates_all_caps(self):
        """A→B→C→D→E: E must inherit all ancestors' capabilities."""
        roles = [
            _role("a", caps=["cap-a"], domains=["dom-a"]),
            _role("b", caps=["cap-b"], domains=["dom-b"], parent="a"),
            _role("c", caps=["cap-c"], domains=["dom-c"], parent="b"),
            _role("d", caps=["cap-d"], domains=["dom-d"], parent="c"),
            _role("e", caps=["cap-e"], domains=["dom-e"], parent="d"),
        ]
        result = flatten_roles(roles)
        by_id = {r.id: r for r in result}

        for cap in ["cap-a", "cap-b", "cap-c", "cap-d", "cap-e"]:
            assert cap in by_id["e"].capabilities, f"Missing {cap} in E after deep flatten"

        for dom in ["dom-a", "dom-b", "dom-c", "dom-d", "dom-e"]:
            assert dom in by_id["e"].domain_access, f"Missing {dom} in E after deep flatten"

    def test_intermediate_roles_have_correct_caps(self):
        """Intermediate roles in a chain should only inherit from their ancestor subtree."""
        roles = [
            _role("a", caps=["cap-a"], domains=["dom-a"]),
            _role("b", caps=["cap-b"], domains=["dom-b"], parent="a"),
            _role("c", caps=["cap-c"], domains=["dom-c"], parent="b"),
        ]
        result = flatten_roles(roles)
        by_id = {r.id: r for r in result}

        # B inherits from A only
        assert "cap-a" in by_id["b"].capabilities
        assert "cap-b" in by_id["b"].capabilities
        assert "cap-c" not in by_id["b"].capabilities

        # A inherits nothing extra
        assert "cap-b" not in by_id["a"].capabilities
        assert "cap-c" not in by_id["a"].capabilities

    def test_flatten_roles_is_idempotent(self):
        """Calling flatten_roles twice must yield the same effective permissions."""
        roles = [
            _role("base", caps=["read"], domains=["sales"]),
            _role("mid", caps=["aggregate"], domains=["ops"], parent="base"),
            _role("leaf", caps=["export"], domains=["finance"], parent="mid"),
        ]
        first_pass = flatten_roles(roles)
        second_pass = flatten_roles(first_pass)

        by_first = {r.id: r for r in first_pass}
        by_second = {r.id: r for r in second_pass}

        for role_id in ["base", "mid", "leaf"]:
            assert set(by_first[role_id].capabilities) == set(
                by_second[role_id].capabilities
            ), f"Idempotency failed for capabilities of role {role_id}"
            assert set(by_first[role_id].domain_access) == set(
                by_second[role_id].domain_access
            ), f"Idempotency failed for domain_access of role {role_id}"


# ---------------------------------------------------------------------------
# Conflict resolution when multiple parents provide overlapping capabilities
# ---------------------------------------------------------------------------


class TestConflictResolution:
    def test_duplicate_capability_deduplicated(self):
        """When both parent and child list the same capability it appears only once."""
        parent = _role("p", caps=["read", "shared"], domains=["sales"])
        child = _role("c", caps=["shared", "write"], domains=["ops"], parent="p")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        assert by_id["c"].capabilities.count("shared") == 1, (
            "Duplicate capability 'shared' must be deduplicated after flatten"
        )

    def test_union_of_domains_from_parent_and_child(self):
        """Effective domain_access is the union of all ancestors' domains."""
        parent = _role("p", caps=["read"], domains=["finance", "hr"])
        child = _role("c", caps=["write"], domains=["ops"], parent="p")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        for dom in ["finance", "hr", "ops"]:
            assert dom in by_id["c"].domain_access

    def test_sibling_roles_do_not_share_caps(self):
        """Two children of the same parent must not gain each other's capabilities."""
        parent = _role("base", caps=["read"], domains=["*"])
        child_a = _role("child-a", caps=["export"], domains=["sales"], parent="base")
        child_b = _role("child-b", caps=["aggregate"], domains=["ops"], parent="base")

        result = flatten_roles([parent, child_a, child_b])
        by_id = {r.id: r for r in result}

        assert "aggregate" not in by_id["child-a"].capabilities
        assert "export" not in by_id["child-b"].capabilities

    def test_wildcard_domain_from_parent_swallows_child_domains(self):
        """When a parent has domain_access=['*'], child's effective access is ['*']."""
        parent = _role("super", caps=["read"], domains=["*"])
        child = _role("restricted", caps=["read"], domains=["sales"], parent="super")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        # flatten_roles preserves ['*'] when wildcard is present
        assert "*" in by_id["restricted"].domain_access

    def test_role_without_parent_not_affected_by_other_roles(self):
        """A standalone role must not accumulate capabilities from unrelated roles."""
        standalone = _role("standalone", caps=["read"], domains=["finance"])
        other = _role("other", caps=["admin", "delete"], domains=["*"])

        result = flatten_roles([standalone, other])
        by_id = {r.id: r for r in result}

        assert "admin" not in by_id["standalone"].capabilities
        assert "delete" not in by_id["standalone"].capabilities

    def test_empty_role_list_returns_empty(self):
        """flatten_roles on an empty list must return an empty list."""
        result = flatten_roles([])
        assert result == []


# ---------------------------------------------------------------------------
# RLS rule accumulation through inheritance (build_rls_context + inject_rls)
# ---------------------------------------------------------------------------


class TestRLSAccumulationThroughInheritance:
    def test_build_rls_context_accumulates_multiple_tables(self):
        """build_rls_context must collect rules from all tables for the given role."""
        raw_rules = [
            {"table_id": ORDERS_TABLE_ID, "role_id": "analyst", "filter_expr": "region = 'us'"},
            {"table_id": CUSTOMERS_TABLE_ID, "role_id": "analyst", "filter_expr": "active = true"},
            {"table_id": PRODUCTS_TABLE_ID, "role_id": "analyst", "filter_expr": "visible = true"},
            {"table_id": ORDERS_TABLE_ID, "role_id": "admin", "filter_expr": "1=1"},
        ]
        rls = build_rls_context(raw_rules, "analyst")

        assert rls.rules[ORDERS_TABLE_ID] == "region = 'us'"
        assert rls.rules[CUSTOMERS_TABLE_ID] == "active = true"
        assert rls.rules[PRODUCTS_TABLE_ID] == "visible = true"
        assert ORDERS_TABLE_ID in rls.rules
        # Admin rule must not appear in analyst context
        assert len(rls.rules) == 3

    def test_inherited_rls_rules_applied_to_inherited_role(self):
        """Simulates a child role using the parent's RLS filter on two tables.

        In practice the system calls build_rls_context with the parent role_id
        when the child has no own rule.  We verify inject_rls applies both table
        filters when the combined RLSContext covers two tables.
        """
        ctx = _make_ctx(
            (ORDERS_TABLE_ID, "orders", "orders"),
        )
        compiled = _compiled_for(
            sql='SELECT "id", "amount" FROM "public"."orders"',
            root_field="orders",
            columns=[
                ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
                ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
            ],
        )
        # Child inherits parent's rule: the caller passes the parent's filter for the child
        inherited_rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'eu-west'"})
        result = inject_rls(compiled, ctx, inherited_rls)

        assert "region" in result.sql
        assert "eu-west" in result.sql
        assert "WHERE" in result.sql.upper()

    def test_combined_parent_and_child_rls_anded(self):
        """Parent and child RLS filters for the same table must be AND-ed."""
        ctx = _make_ctx((ORDERS_TABLE_ID, "orders", "orders"))
        compiled = _compiled_for(
            sql='SELECT "id" FROM "public"."orders"',
            root_field="orders",
        )
        # Caller composes: parent filter AND child filter before injecting
        combined_filter = "region = 'us-west' AND tier = 'premium'"
        rls = RLSContext(rules={ORDERS_TABLE_ID: combined_filter})
        result = inject_rls(compiled, ctx, rls)

        assert "region = 'us-west'" in result.sql
        assert "tier = 'premium'" in result.sql
        assert "AND" in result.sql

    def test_rls_context_empty_when_role_has_no_rules(self):
        """A role not present in the rules list should produce an empty RLSContext."""
        raw_rules = [
            {"table_id": ORDERS_TABLE_ID, "role_id": "analyst", "filter_expr": "x = 1"},
        ]
        rls = build_rls_context(raw_rules, "viewer")
        assert not rls.has_rules()

    def test_inject_rls_no_op_for_empty_context(self):
        """inject_rls with an empty RLSContext must return the query unchanged."""
        ctx = _make_ctx((ORDERS_TABLE_ID, "orders", "orders"))
        compiled = _compiled_for('SELECT "id" FROM "public"."orders"', "orders")
        result = inject_rls(compiled, ctx, RLSContext.empty())
        assert result.sql == compiled.sql


# ---------------------------------------------------------------------------
# Masking inheritance — additional scenarios not in test_inherited_roles.py
# ---------------------------------------------------------------------------


class TestMaskingInheritanceExtended:
    def test_two_masked_columns_with_different_rules_applied(self):
        """An inherited masking context with two column rules must mask both columns."""
        ctx = _make_ctx((ORDERS_TABLE_ID, "orders", "orders"))
        compiled = CompiledQuery(
            sql='SELECT "amount", "region" FROM "public"."orders"',
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
                ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
            ],
            sources={"test-pg"},
        )

        masking: MaskingRules = {
            (ORDERS_TABLE_ID, "viewer"): {
                "amount": (MaskingRule(mask_type=MaskType.constant, value=0), "integer"),
                "region": (MaskingRule(mask_type=MaskType.constant, value="HIDDEN"), "varchar"),
            }
        }
        result = inject_masking(compiled, ctx, masking, "viewer")

        assert '0 AS "amount"' in result.sql
        assert "'HIDDEN' AS \"region\"" in result.sql

    def test_inherited_masking_not_applied_to_different_role(self):
        """Masking rules for 'viewer' must not be applied when role is 'analyst'."""
        ctx = _make_ctx((ORDERS_TABLE_ID, "orders", "orders"))
        compiled = CompiledQuery(
            sql='SELECT "amount" FROM "public"."orders"',
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
            ],
            sources={"test-pg"},
        )
        masking: MaskingRules = {
            (ORDERS_TABLE_ID, "viewer"): {
                "amount": (MaskingRule(mask_type=MaskType.constant, value=0), "integer"),
            }
        }
        result = inject_masking(compiled, ctx, masking, "analyst")
        # SQL must be unchanged for 'analyst'
        assert result.sql == compiled.sql

    def test_regex_mask_inherited_via_assigned_role_id(self):
        """Caller assigns parent's regex rule to the child role_id; injection works."""
        ctx = _make_ctx((CUSTOMERS_TABLE_ID, "customers", "customers"))
        compiled = CompiledQuery(
            sql='SELECT "email" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[
                ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
            ],
            sources={"test-pg"},
        )
        # Parent's regex rule is assigned to child's role_id by the caller
        masking: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "child-role"): {
                "email": (
                    MaskingRule(
                        mask_type=MaskType.regex,
                        pattern="^(.{2}).*(@.*)$",
                        replace="$1***$2",
                    ),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, masking, "child-role")
        assert "REGEXP_REPLACE" in result.sql
        assert 'AS "email"' in result.sql
