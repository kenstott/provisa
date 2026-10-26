# Copyright (c) 2026 Kenneth Stott
# Canary: a8bf11c1-080f-4d14-a986-e647b63f0cc4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: role inheritance propagating RLS rules and masking (REQ-040, REQ-087)."""

from __future__ import annotations


from provisa.compiler.rls import RLSContext, build_rls_context
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.mask_inject import MaskingRules
from provisa.core.models import Role, flatten_roles
from provisa.security.inheritance import materialize_rls
from provisa.security.masking import MaskType, MaskingRule
from provisa.security.row_filters import resolve_table_rules
from tests.helpers import governed_for_role


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
        description="Test role.",
        capabilities=caps,
        domain_access=domains,
        parents=[parent] if parent else [],
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


_ORDERS_SQL = 'SELECT "id", "amount", "region" FROM "public"."orders"'
_ORDERS_T0_SQL = 'SELECT "t0"."id", "t0"."amount" FROM "public"."orders" "t0"'


def _inherited_rls(rules: list[dict], chains: dict[str, list[str]], role_id: str) -> RLSContext:
    """The role's row filters as the server builds them: inherited rules materialized down the
    role chain (security/inheritance.materialize_rls), folded per table (row_filters)."""
    tables = [{"id": ORDERS_TABLE_ID, "domain_id": ""}]
    parents = {role: chain[1:2] for role, chain in chains.items()}  # each chain is one parent deep
    resolved = resolve_table_rules(materialize_rls(rules, tables, parents), tables, chains, [], {})
    return build_rls_context(resolved, role_id)


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
        """flatten_roles() merges the parent's capabilities; reach is the role's own (REQ-2155)."""
        parent = _role("parent", caps=["read"], domains=["sales"])
        child = _role("child", caps=["export"], domains=["ops"], parent="parent")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        assert "read" in by_id["child"].capabilities
        assert "export" in by_id["child"].capabilities
        assert by_id["child"].domain_access == ["ops"]

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

    def test_wildcard_domain_does_not_propagate(self):
        """REQ-2155: a parent reaching every domain gives its child no reach; the child's own list
        stands."""
        parent = _role("super", caps=["read"], domains=["*"])
        child = _role("sub", caps=["read"], domains=["finance"], parent="super")

        result = flatten_roles([parent, child])
        by_id = {r.id: r for r in result}

        assert by_id["sub"].domain_access == ["finance"]

    def test_no_parent_role_is_unchanged(self):
        """A role with no parent must remain identical after flattening."""
        solo = _role("solo", caps=["read"], domains=["sales"])
        result = flatten_roles([solo])
        assert result[0].capabilities == ["read"]
        assert result[0].domain_access == ["sales"]
        assert result[0].parents == []


# ---------------------------------------------------------------------------
# RLS inheritance tests (SQL injection level — no DB required)
# ---------------------------------------------------------------------------


class TestInheritedRolesRLS:
    def test_child_inherits_parent_rls(self):
        """A child role with no rule of its own for a table reads under its parent's rule."""
        parent_rule = {
            "description": "Test rule.",
            "role_id": "parent",
            "table_id": ORDERS_TABLE_ID,
            "domain_id": None,
            "filter_expr": "\"region\" = 'us-east'",
        }
        rls = _inherited_rls(
            [parent_rule], {"parent": ["parent"], "child": ["child", "parent"]}, "child"
        )
        result = governed_for_role(_ORDERS_SQL, _orders_ctx(), "child", rls=rls)
        assert result == f'{_ORDERS_SQL} WHERE ("orders"."region" = \'us-east\')'

    def test_child_s_own_rule_replaces_its_parent_s(self):
        """Child-first (REQ-1677): a child's own rule for a table replaces its parent's; the two
        are never ANDed."""
        rules = [
            {
                "description": "Test rule.",
                "role_id": "parent",
                "table_id": ORDERS_TABLE_ID,
                "domain_id": None,
                "filter_expr": "\"region\" = 'us-east'",
            },
            {
                "description": "Test rule.",
                "role_id": "child",
                "table_id": ORDERS_TABLE_ID,
                "domain_id": None,
                "filter_expr": '"amount" > 100',
            },
        ]
        rls = _inherited_rls(rules, {"parent": ["parent"], "child": ["child", "parent"]}, "child")
        result = governed_for_role(_ORDERS_SQL, _orders_ctx(), "child", rls=rls)
        assert result == f'{_ORDERS_SQL} WHERE ("orders"."amount" > 100)'

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

        # REQ-2155: reach is the child's own
        assert by_id["child"].domain_access == ["finance"]

    def test_child_rls_sql_where_injected_correctly(self):
        """The row filter is placed before ORDER BY, on the table's alias."""
        result = governed_for_role(
            f'{_ORDERS_T0_SQL} ORDER BY "t0"."id"',
            _orders_ctx(),
            "child",
            rls={ORDERS_TABLE_ID: "\"region\" = 'us-east'"},
        )
        assert result == (
            'SELECT "t0"."id", "t0"."amount" FROM "public"."orders" AS "t0" '
            'WHERE ("t0"."region" = \'us-east\') ORDER BY "t0"."id"'
        )


# ---------------------------------------------------------------------------
# Masking inheritance tests (SQL injection level — no DB required)
# ---------------------------------------------------------------------------


class TestInheritedRolesMasking:
    def test_child_overrides_parent_mask(self):
        """The mask the child role is given for a column is the one its reads carry."""
        child_rule = MaskingRule(mask_type=MaskType.constant, value=999)
        child_masking: MaskingRules = {
            (ORDERS_TABLE_ID, "child"): {"amount": (child_rule, "integer")},
            (ORDERS_TABLE_ID, "parent"): {
                "amount": (MaskingRule(mask_type=MaskType.constant, value=0), "integer")
            },
        }
        result = governed_for_role(
            _ORDERS_T0_SQL, _orders_ctx(), "child", masking_rules=child_masking
        )
        assert result == ('SELECT "t0"."id", 999 AS "amount" FROM "public"."orders" AS "t0"')

    def test_no_masking_rules_for_role_leaves_sql_unchanged(self):
        """A role with no masking rules must not alter the compiled SQL."""
        result = governed_for_role(_ORDERS_T0_SQL, _orders_ctx(), "some-role", masking_rules={})
        assert result == 'SELECT "t0"."id", "t0"."amount" FROM "public"."orders" AS "t0"'

    def test_parent_mask_applied_when_child_has_none(self):
        """The parent's mask, held by the child (it is not in the column's ``unmasked_to``),
        is applied to the child's read."""
        parent_rule = MaskingRule(mask_type=MaskType.constant, value=0)
        inherited_masking: MaskingRules = {
            (ORDERS_TABLE_ID, "child"): {"amount": (parent_rule, "integer")},
        }
        result = governed_for_role(
            _ORDERS_T0_SQL, _orders_ctx(), "child", masking_rules=inherited_masking
        )
        assert result == 'SELECT "t0"."id", 0 AS "amount" FROM "public"."orders" AS "t0"'
