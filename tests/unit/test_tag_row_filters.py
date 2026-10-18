# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0f6c1e-2b7a-4f39-9a84-c3e1b7d2f640
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1384: a row filter attached to a tag applies to every table carrying the tag.

The rule list is resolved to ONE predicate per (role, table) before the role's RLS context is
built, so the governance path reads the same ``rules[table_id]`` it always has.
"""

# Requirements: REQ-1384, REQ-402

from provisa.compiler.rls import build_rls_context
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import build_governance_context
from provisa.security.row_filters import resolve_table_rules
from tests.helpers import unscoped_role

TABLES = [
    {"id": 1, "domain_id": "sales", "table_name": "orders"},
    {"id": 2, "domain_id": "sales", "table_name": "customers"},
    {"id": 3, "domain_id": "hr", "table_name": "staff"},
]
CHAINS = {"analyst": ["analyst"], "junior": ["junior", "analyst"], "other": ["other"]}


def _tag(tag_id: str, table_id: int) -> dict:
    return {
        "tag_id": tag_id,
        "base_tag_id": tag_id.split(":", 1)[0],
        "object_type": "table",
        "table_id": table_id,
    }


def _tag_rule(role: str, tag: str, expr: str) -> dict:
    return {
        "description": "Test rule.",
        "id": 9,
        "role_id": role,
        "tag_id": tag,
        "table_id": None,
        "domain_id": None,
        "action_name": None,
        "filter_expr": expr,
    }


def _table_rule(role: str, table_id: int, expr: str) -> dict:
    return {
        "description": "Test rule.",
        "role_id": role,
        "tag_id": None,
        "table_id": table_id,
        "domain_id": None,
        "action_name": None,
        "filter_expr": expr,
    }


def _domain_rule(role: str, domain: str, expr: str) -> dict:
    return {
        "description": "Test rule.",
        "role_id": role,
        "tag_id": None,
        "table_id": None,
        "domain_id": domain,
        "action_name": None,
        "filter_expr": expr,
    }


def _rules_for(role: str, rules: list[dict], assignments: list[dict]) -> dict[int, str]:
    return build_rls_context(
        resolve_table_rules(rules, TABLES, CHAINS, assignments, {}), role
    ).rules


class TestTagRuleReach:
    def test_applies_to_every_table_carrying_the_tag(self):
        rules = [_tag_rule("analyst", "regional", "region = 'us'")]
        assignments = [_tag("regional", 1), _tag("regional", 3)]
        assert _rules_for("analyst", rules, assignments) == {
            1: "region = 'us'",
            3: "region = 'us'",
        }

    def test_untagged_table_is_untouched(self):
        rules = [_tag_rule("analyst", "regional", "region = 'us'")]
        assert _rules_for("analyst", rules, [_tag("regional", 1)]).keys() == {1}

    def test_other_role_is_untouched(self):
        rules = [_tag_rule("analyst", "regional", "region = 'us'")]
        assert _rules_for("other", rules, [_tag("regional", 1)]) == {}

    def test_column_level_assignment_does_not_tag_the_table(self):
        rules = [_tag_rule("analyst", "regional", "region = 'us'")]
        column = {**_tag("regional", 1), "object_type": "column", "column_name": "region"}
        assert _rules_for("analyst", rules, [column]) == {}

    def test_rule_on_base_tag_covers_every_parameter_value(self):
        rules = [_tag_rule("analyst", "entity", "active")]
        assert _rules_for("analyst", rules, [_tag("entity:customer", 2)]) == {2: "active"}


class TestTagRuleIsAnded:
    def test_anded_with_the_table_rule(self):
        rules = [
            _table_rule("analyst", 1, "amount > 0"),
            _tag_rule("analyst", "regional", "region = 'us'"),
        ]
        assert _rules_for("analyst", rules, [_tag("regional", 1)]) == {
            1: "(amount > 0) AND (region = 'us')"
        }

    def test_anded_with_the_domain_rule(self):
        rules = [
            _domain_rule("analyst", "sales", "tenant = 't1'"),
            _tag_rule("analyst", "regional", "region = 'us'"),
        ]
        resolved = _rules_for("analyst", rules, [_tag("regional", 1)])
        assert resolved[1] == "(tenant = 't1') AND (region = 'us')"
        # The domain rule alone still governs the domain's untagged table.
        assert resolved[2] == "tenant = 't1'"
        assert 3 not in resolved

    def test_every_matching_tag_contributes(self):
        rules = [
            _tag_rule("analyst", "regional", "region = 'us'"),
            _tag_rule("analyst", "active_only", "active"),
        ]
        assignments = [_tag("regional", 1), _tag("active_only", 1)]
        assert _rules_for("analyst", rules, assignments) == {1: "(active) AND (region = 'us')"}

    def test_table_rule_still_takes_the_place_of_the_domain_rule(self):
        rules = [
            _table_rule("analyst", 1, "amount > 0"),
            _domain_rule("analyst", "sales", "tenant = 't1'"),
        ]
        assert _rules_for("analyst", rules, []) == {1: "amount > 0", 2: "tenant = 't1'"}


class TestTagRuleInheritance:
    def test_child_role_takes_an_ancestors_tag_rule(self):
        rules = [_tag_rule("analyst", "regional", "region = 'us'")]
        assert _rules_for("junior", rules, [_tag("regional", 1)]) == {1: "region = 'us'"}

    def test_child_cannot_shed_an_ancestors_tag_rule(self):
        # Tagging only ever adds restriction: the child's own rule for the tag is AND-ed with
        # the ancestor's, nearest role first — it does not replace it.
        rules = [
            _tag_rule("analyst", "regional", "region = 'us'"),
            _tag_rule("junior", "regional", "amount < 100"),
        ]
        assert _rules_for("junior", rules, [_tag("regional", 1)]) == {
            1: "(amount < 100) AND (region = 'us')"
        }


class TestResolvedRulesReachGovernance:
    """The resolved predicate is what ``build_governance_context`` hands the one SQL path."""

    def _gov(self, rules: list[dict], assignments: list[dict]):
        ctx = CompilationContext()
        ctx.tables["orders"] = TableMeta(
            table_id=1,
            field_name="orders",
            type_name="Orders",
            source_id="src-1",
            catalog_name="main",
            schema_name="public",
            table_name="orders",
        )
        tables = [
            {
                "id": 1,
                "domain_id": "sales",
                "table_name": "orders",
                "schema_name": "public",
                "columns": [{"column_name": "id"}, {"column_name": "region"}],
            }
        ]
        rls = build_rls_context(
            resolve_table_rules(rules, TABLES, CHAINS, assignments, {}), "analyst"
        )
        return build_governance_context(
            "analyst", rls, {}, ctx, tables, role=unscoped_role("analyst")
        )

    def test_tag_rule_reaches_governance(self):
        gov = self._gov([_tag_rule("analyst", "regional", "region = 'us'")], [_tag("regional", 1)])
        assert gov.rls_rules == {1: "region = 'us'"}

    def test_domain_rule_reaches_governance(self):
        # REQ-402: a domain rule on a role with no ancestors was dropped here — the governance
        # context reads table-keyed rules only.
        gov = self._gov([_domain_rule("analyst", "sales", "tenant = 't1'")], [])
        assert gov.rls_rules[1] == "tenant = 't1'"
