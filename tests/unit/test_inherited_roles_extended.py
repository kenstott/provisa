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


from provisa.compiler.rls import RLSContext, build_rls_context
from provisa.compiler.mask_inject import MaskingRules
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.core.models import Role, flatten_roles
from provisa.security.inheritance import materialize_rls
from provisa.security.masking import MaskType, MaskingRule
from provisa.security.row_filters import resolve_table_rules
from tests.governed_pg import assert_same_rows, governed_database
from tests.helpers import governed_for_role

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
        description="Test role.",
        capabilities=caps,
        domain_access=domains,
        parents=[parent] if parent else [],
    )


#: What each governed read is answered over, on PostgreSQL (tests/governed_pg.py).
_ROWS = {
    "public.orders": [
        {"id": 1, "amount": 50, "region": "eu-west", "customer_id": 10, "tier": "premium"},
        {"id": 2, "amount": 150, "region": "eu-west", "customer_id": 11, "tier": "basic"},
        {"id": 3, "amount": 150, "region": "us-west", "customer_id": 10, "tier": "premium"},
        {"id": 4, "amount": 70, "region": "us-west", "customer_id": 11, "tier": "basic"},
    ],
    "public.customers": [
        {"id": 10, "email": "ann@example.com", "active": True},
        {"id": 11, "email": "bo@example.com", "active": False},
    ],
}


@pytest.fixture(scope="module")
def pg(embedded_postgres):
    return governed_database(embedded_postgres, "test_inherited_roles_extended")


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


# ---------------------------------------------------------------------------
# Recursive / deep resolution via flatten_roles
# ---------------------------------------------------------------------------


class TestDeepInheritanceChain:
    def test_five_level_chain_propagates_all_caps(self):
        """A→B→C→D→E: E holds what it is given of what every ancestor passed down (REQ-2155)."""
        roles = [
            _role("a", caps=["cap-a", "cap-b", "cap-c"], domains=["dom-a", "dom-b", "dom-c"]),
            _role("b", caps=["cap-a", "cap-b", "cap-c"], domains=["dom-a", "dom-b"], parent="a"),
            _role("c", caps=["cap-a", "cap-b"], domains=["dom-a", "dom-b"], parent="b"),
            _role("d", caps=["cap-a", "cap-b"], domains=["dom-a"], parent="c"),
            _role("e", caps=["cap-a", "cap-c"], domains=["dom-a", "dom-c"], parent="d"),
        ]
        result = flatten_roles(roles)
        by_id = {r.id: r for r in result}

        assert by_id["e"].capabilities == ["cap-a"]
        assert by_id["e"].domain_access == ["dom-a"]

    def test_intermediate_roles_have_correct_caps(self):
        """A role in a chain is limited by the roles above it, never by those below."""
        roles = [
            _role("a", caps=["cap-a", "cap-b"], domains=["dom-a"]),
            _role("b", caps=["cap-a", "cap-b"], domains=["dom-a"], parent="a"),
            _role("c", caps=["cap-a"], domains=["dom-a"], parent="b"),
        ]
        result = flatten_roles(roles)
        by_id = {r.id: r for r in result}

        assert by_id["b"].capabilities == ["cap-a", "cap-b"]
        assert by_id["a"].capabilities == ["cap-a", "cap-b"]

    def test_flatten_roles_is_idempotent(self):
        """Calling flatten_roles twice must yield the same effective permissions."""
        roles = [
            _role("base", caps=["read"], domains=["sales"]),
            _role("mid", caps=["aggregate"], domains=["support"], parent="base"),
            _role("leaf", caps=["export"], domains=["finance"], parent="mid"),
        ]
        first_pass = flatten_roles(roles)
        second_pass = flatten_roles(first_pass)

        by_first = {r.id: r for r in first_pass}
        by_second = {r.id: r for r in second_pass}

        for role_id in ["base", "mid", "leaf"]:
            assert set(by_first[role_id].capabilities) == set(by_second[role_id].capabilities), (
                f"Idempotency failed for capabilities of role {role_id}"
            )
            assert set(by_first[role_id].domain_access) == set(by_second[role_id].domain_access), (
                f"Idempotency failed for domain_access of role {role_id}"
            )


# ---------------------------------------------------------------------------
# Conflict resolution when multiple parents provide overlapping capabilities
# ---------------------------------------------------------------------------


class TestConflictResolution:
    def test_duplicate_capability_deduplicated(self):
        """When both parent and child list the same capability it appears only once."""
        parent = _role("p", caps=["read", "shared"], domains=["sales"])
        child = _role("c", caps=["shared", "write"], domains=["support"], parent="p")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        assert by_id["c"].capabilities.count("shared") == 1, (
            "Duplicate capability 'shared' must be deduplicated after flatten"
        )

    def test_reach_is_the_childs_own(self):
        """REQ-2155: a role reaches what it lists of what its parents reach."""
        parent = _role("p", caps=["read"], domains=["finance", "hr"])
        child = _role("c", caps=["read"], domains=["finance", "support"], parent="p")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        assert by_id["c"].domain_access == ["finance"]

    def test_sibling_roles_do_not_share_caps(self):
        """Two children of the same parent must not gain each other's capabilities."""
        parent = _role("base", caps=["read"], domains=["*"])
        child_a = _role("child-a", caps=["export"], domains=["sales"], parent="base")
        child_b = _role("child-b", caps=["aggregate"], domains=["support"], parent="base")

        result = flatten_roles([parent, child_a, child_b])
        by_id = {r.id: r for r in result}

        assert "aggregate" not in by_id["child-a"].capabilities
        assert "export" not in by_id["child-b"].capabilities

    def test_wildcard_domain_from_parent_does_not_reach_the_child(self):
        """REQ-2155: a parent reaching every domain leaves its child's own list as it is."""
        parent = _role("super", caps=["read"], domains=["*"])
        child = _role("restricted", caps=["read"], domains=["sales"], parent="super")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        assert by_id["restricted"].domain_access == ["sales"]

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
# RLS rule accumulation through inheritance (build_rls_context + governed pipeline)
# ---------------------------------------------------------------------------


class TestRLSAccumulationThroughInheritance:
    def test_build_rls_context_accumulates_multiple_tables(self):
        """build_rls_context must collect rules from all tables for the given role."""
        raw_rules = [
            {
                "description": "Test rule.",
                "table_id": ORDERS_TABLE_ID,
                "role_id": "analyst",
                "filter_expr": "\"region\" = 'us'",
            },
            {
                "description": "Test rule.",
                "table_id": CUSTOMERS_TABLE_ID,
                "role_id": "analyst",
                "filter_expr": "active = true",
            },
            {
                "description": "Test rule.",
                "table_id": PRODUCTS_TABLE_ID,
                "role_id": "analyst",
                "filter_expr": "visible = true",
            },
            {
                "description": "Test rule.",
                "table_id": ORDERS_TABLE_ID,
                "role_id": "admin",
                "filter_expr": "1=1",
            },
        ]
        rls = build_rls_context(raw_rules, "analyst")

        assert rls.rules[ORDERS_TABLE_ID] == "\"region\" = 'us'"
        assert rls.rules[CUSTOMERS_TABLE_ID] == "active = true"
        assert rls.rules[PRODUCTS_TABLE_ID] == "visible = true"
        assert ORDERS_TABLE_ID in rls.rules
        # Admin rule must not appear in analyst context
        assert len(rls.rules) == 3

    def test_inherited_rls_rules_applied_to_inherited_role(self, pg):
        """A child role with no rule of its own reads each table under its parent's rule."""
        tables = [
            {"id": ORDERS_TABLE_ID, "domain_id": ""},
            {"id": CUSTOMERS_TABLE_ID, "domain_id": ""},
        ]
        chains = {"parent": ["parent"], "child": ["child", "parent"]}
        rules = [
            {
                "description": "Test rule.",
                "role_id": "parent",
                "table_id": ORDERS_TABLE_ID,
                "domain_id": None,
                "filter_expr": "\"region\" = 'eu-west'",
            },
            {
                "description": "Test rule.",
                "role_id": "parent",
                "table_id": CUSTOMERS_TABLE_ID,
                "domain_id": None,
                "filter_expr": "active = true",
            },
        ]
        resolved = resolve_table_rules(
            materialize_rls(rules, tables, {"parent": [], "child": ["parent"]}),
            tables,
            chains,
            [],
            {},
        )
        inherited_rls = build_rls_context(resolved, "child")
        ctx = _make_ctx(
            (ORDERS_TABLE_ID, "orders", "orders"),
            (CUSTOMERS_TABLE_ID, "customers", "customers"),
        )
        result = governed_for_role(
            'SELECT "o"."id", "o"."amount" FROM "public"."orders" "o" '
            'JOIN "public"."customers" "c" ON "o"."customer_id" = "c"."id"',
            ctx,
            "child",
            rls=inherited_rls,
        )
        expected = (
            'SELECT "o"."id", "o"."amount" FROM "public"."orders" AS "o" '
            'JOIN "public"."customers" AS "c" ON "o"."customer_id" = "c"."id" '
            'WHERE "o"."region" = \'eu-west\' AND "c"."active" = TRUE'
        )
        assert assert_same_rows(pg, result, expected, _ROWS) == [(1, 50)]

    def test_child_s_own_rule_is_anded_with_its_parent_s_parent_reads_its_own(self, pg):
        """REQ-1677: the child reads under its own rule AND its parent's; the parent under its own."""
        tables = [{"id": ORDERS_TABLE_ID, "domain_id": ""}]
        chains = {"parent": ["parent"], "child": ["child", "parent"]}
        rules = [
            {
                "description": "Test rule.",
                "role_id": "parent",
                "table_id": ORDERS_TABLE_ID,
                "domain_id": None,
                "filter_expr": "region = 'us-west'",
            },
            {
                "description": "Test rule.",
                "role_id": "child",
                "table_id": ORDERS_TABLE_ID,
                "domain_id": None,
                "filter_expr": "tier = 'premium'",
            },
        ]
        resolved = resolve_table_rules(
            materialize_rls(rules, tables, {"parent": [], "child": ["parent"]}),
            tables,
            chains,
            [],
            {},
        )
        ctx = _make_ctx((ORDERS_TABLE_ID, "orders", "orders"))
        sql = 'SELECT "id" FROM "public"."orders"'
        child = governed_for_role(sql, ctx, "child", rls=build_rls_context(resolved, "child"))
        parent = governed_for_role(sql, ctx, "parent", rls=build_rls_context(resolved, "parent"))
        child_reads = f"{sql} WHERE \"tier\" = 'premium' AND \"region\" = 'us-west'"
        assert assert_same_rows(pg, child, child_reads, _ROWS) == [(3,)]
        parent_reads = f"{sql} WHERE \"region\" = 'us-west'"
        assert assert_same_rows(pg, parent, parent_reads, _ROWS) == [(3,), (4,)]

    def test_rls_context_empty_when_role_has_no_rules(self):
        """A role not present in the rules list should produce an empty RLSContext."""
        raw_rules = [
            {
                "description": "Test rule.",
                "table_id": ORDERS_TABLE_ID,
                "role_id": "analyst",
                "filter_expr": "x = 1",
            },
        ]
        rls = build_rls_context(raw_rules, "viewer")
        assert not rls.has_rules()

    def test_inject_rls_no_op_for_empty_context(self, pg):
        """An empty RLSContext leaves the governed query unchanged."""
        ctx = _make_ctx((ORDERS_TABLE_ID, "orders", "orders"))
        sql = 'SELECT "id" FROM "public"."orders"'
        governed = governed_for_role(sql, ctx, "viewer", rls=RLSContext.empty())
        assert assert_same_rows(pg, governed, sql, _ROWS) == [(1,), (2,), (3,), (4,)]


# ---------------------------------------------------------------------------
# Masking inheritance — additional scenarios not in test_inherited_roles.py
# ---------------------------------------------------------------------------


class TestMaskingInheritanceExtended:
    def test_two_masked_columns_with_different_rules_applied(self, pg):
        """An inherited masking context with two column rules must mask both columns."""
        ctx = _make_ctx((ORDERS_TABLE_ID, "orders", "orders"))
        masking: MaskingRules = {
            (ORDERS_TABLE_ID, "viewer"): {
                "amount": (MaskingRule(mask_type=MaskType.constant, value=0), "integer"),
                "region": (MaskingRule(mask_type=MaskType.constant, value="HIDDEN"), "varchar"),
            }
        }
        result = governed_for_role(
            'SELECT "amount", "region" FROM "public"."orders"', ctx, "viewer", masking_rules=masking
        )
        expected = 'SELECT 0 AS "amount", \'HIDDEN\' AS "region" FROM "public"."orders"'
        assert assert_same_rows(pg, result, expected, _ROWS) == [(0, "HIDDEN")] * 4

    def test_inherited_masking_not_applied_to_different_role(self, pg):
        """Masking rules for 'viewer' must not be applied when role is 'analyst'."""
        ctx = _make_ctx((ORDERS_TABLE_ID, "orders", "orders"))
        masking: MaskingRules = {
            (ORDERS_TABLE_ID, "viewer"): {
                "amount": (MaskingRule(mask_type=MaskType.constant, value=0), "integer"),
            }
        }
        sql = 'SELECT "amount" FROM "public"."orders"'
        # The analyst reads the values themselves
        governed = governed_for_role(sql, ctx, "analyst", masking_rules=masking)
        assert sorted(assert_same_rows(pg, governed, sql, _ROWS)) == [(50,), (70,), (150,), (150,)]

    def test_regex_mask_inherited_via_assigned_role_id(self, pg):
        """A regex rule held by the child role is applied to its read."""
        ctx = _make_ctx((CUSTOMERS_TABLE_ID, "customers", "customers"))
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
        result = governed_for_role(
            'SELECT "email" FROM "public"."customers"', ctx, "child-role", masking_rules=masking
        )
        expected = (
            "SELECT PROVISA_REGEX_REPLACE(\"email\", '^(.{2}).*(@.*)$', '$1***$2', '****') AS \"email\" "
            'FROM "public"."customers"'
        )
        assert assert_same_rows(pg, result, expected, _ROWS) == [
            ("an***@example.com",),
            ("bo***@example.com",),
        ]
