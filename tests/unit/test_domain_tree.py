# Copyright (c) 2026 Kenneth Stott
# Canary: 0606f4d2-1c07-4ee3-ad5c-c955d60fc7b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The domain tree and what is flattened from it (REQ-1363).

A business unit (``retail``) holds two organizations (``retail_emea``, ``retail_apac``), one of
which holds a team (``retail_emea_uk``); ``finance`` stands alone.
"""

# Requirements: REQ-1363

import pytest

from provisa.compiler.rls import build_rls_context
from provisa.security.domain_tree import (
    ancestry,
    children_of,
    descendants,
    expand,
    effective_steward,
    flatten_role_domains,
    parent_map,
    parent_problem,
    tree_problem,
)
from provisa.security.row_filters import resolve_table_rules
from tests.rules import rule_of, rules_of

DOMAINS = [
    {"id": "retail", "parent_domain_id": None},
    {"id": "retail_emea", "parent_domain_id": "retail"},
    {"id": "retail_apac", "parent_domain_id": "retail"},
    {"id": "retail_emea_uk", "parent_domain_id": "retail_emea"},
    {"id": "finance"},
]
PARENTS = parent_map(DOMAINS)


class TestTheTree:
    def test_a_domain_with_no_parent_column_has_no_parent(self):
        assert PARENTS["finance"] is None and PARENTS["retail"] is None

    def test_ancestry_is_nearest_first(self):
        assert ancestry("retail_emea_uk", PARENTS) == ["retail_emea_uk", "retail_emea", "retail"]

    def test_children_and_descendants(self):
        assert children_of("retail", PARENTS) == ["retail_apac", "retail_emea"]
        assert descendants("retail", PARENTS) == {"retail_emea", "retail_apac", "retail_emea_uk"}
        assert descendants("finance", PARENTS) == set()


class TestAParentIsRefusedByName:
    def test_no_parent_is_always_allowed(self):
        assert parent_problem("retail_emea", None, PARENTS) is None

    def test_a_domain_is_not_its_own_parent(self):
        assert parent_problem("retail", "retail", PARENTS) == (
            "Domain 'retail' cannot be its own parent"
        )

    def test_an_unknown_parent_is_named(self):
        assert parent_problem("finance", "ghost", PARENTS) == "Parent domain 'ghost' not found"

    def test_a_cycle_names_the_domains_in_it(self):
        problem = parent_problem("retail", "retail_emea_uk", PARENTS)
        assert problem == (
            "Domain 'retail' cannot sit under 'retail_emea_uk': domain hierarchy cycle: "
            "retail -> retail_emea_uk -> retail_emea -> retail"
        )

    def test_moving_a_domain_to_another_branch_is_allowed(self):
        assert parent_problem("retail_emea_uk", "retail_apac", PARENTS) is None
        assert parent_problem("finance", "retail", PARENTS) is None

    def test_a_whole_tree_is_checked_for_a_config(self):
        assert tree_problem(PARENTS) is None
        assert tree_problem({"a": "b", "b": None, "c": "zz"}) == (
            "Domain 'c' names the parent 'zz', which is not a domain"
        )
        assert tree_problem({"a": "b", "b": "a"}) == "domain hierarchy cycle: a -> b -> a"


class TestAGrantReachesTheSubtree:
    def test_a_business_unit_grant_reaches_every_organization_under_it(self):
        assert expand(["retail"], PARENTS) == [
            "retail",
            "retail_apac",
            "retail_emea",
            "retail_emea_uk",
        ]

    def test_a_grant_reaches_nothing_above_or_beside_it(self):
        assert expand(["retail_emea"], PARENTS) == ["retail_emea", "retail_emea_uk"]
        assert expand(["finance"], PARENTS) == ["finance"]

    def test_all_domains_stays_all(self):
        assert expand(["*", "retail"], PARENTS) == ["*"]

    def test_no_domains_stays_none(self):
        assert expand([], PARENTS) == []

    def test_an_organization_added_later_is_reached_without_editing_the_role(self):
        role = {"id": "bu_reader", "domain_access": ["retail"], "capabilities": ["x"]}
        later = parent_map([*DOMAINS, {"id": "retail_latam", "parent_domain_id": "retail"}])
        (flat,) = flatten_role_domains([role], later)
        assert "retail_latam" in flat["domain_access"]
        assert role["domain_access"] == ["retail"]  # the stored grant is not rewritten
        assert flat["capabilities"] == ["x"]


def _domain_rule(role: str, domain: str, expr: str) -> dict:
    return {
        "id": 1,
        "role_id": role,
        "domain_id": domain,
        "table_id": None,
        "action_name": None,
        "tag_id": None,
        "filter_expr": expr,
    }


TABLES = [
    {"id": 1, "domain_id": "retail"},
    {"id": 2, "domain_id": "retail_emea"},
    {"id": 3, "domain_id": "retail_emea_uk"},
    {"id": 4, "domain_id": "finance"},
]


def _table_rules(role: str, rules: list[dict]) -> dict[int, str]:
    resolved = resolve_table_rules(rules, TABLES, {role: [role]}, [], PARENTS)
    return build_rls_context(resolved, role).rules


class TestADomainRowFilterCoversTheSubtree:
    def test_a_business_unit_rule_covers_the_tables_of_every_domain_under_it(self):
        rules = [_domain_rule("r", "retail", "tenant = 1")]
        assert _table_rules("r", rules) == rules_of(
            {1: "tenant = 1", 2: "tenant = 1", 3: "tenant = 1"}
        )

    def test_every_ancestor_domain_rule_applies_together(self):
        rules = [
            _domain_rule("r", "retail", "tenant = 1"),
            _domain_rule("r", "retail_emea", "region = 'emea'"),
        ]
        assert _table_rules("r", rules) == rules_of(
            {
                1: "tenant = 1",
                2: "(region = 'emea') AND (tenant = 1)",
                # the team is filtered by its organization's rule and by the unit's
                3: "(region = 'emea') AND (tenant = 1)",
            }
        )

    def test_a_table_rule_stands_for_its_own_domain_and_the_ancestors_still_apply(self):
        table_rule = {**_domain_rule("r", "", "store = 7"), "domain_id": None, "table_id": 3}
        rules = [
            table_rule,
            _domain_rule("r", "retail_emea_uk", "uk = true"),
            _domain_rule("r", "retail", "tenant = 1"),
        ]
        assert _table_rules("r", rules)[3] == rule_of("(store = 7) AND (tenant = 1)")

    def test_another_roles_rule_is_not_handed_down_to_this_one(self):
        rules = [_domain_rule("other", "retail", "tenant = 1")]
        assert _table_rules("r", rules) == {}


class TestAStewardIsInherited:
    STEWARDS = {"retail": "bu_steward", "retail_emea": None, "retail_emea_uk": "uk_steward"}

    def test_a_domain_with_no_steward_answers_to_its_nearest_ancestors(self):
        assert effective_steward("retail_emea", self.STEWARDS, PARENTS) == "bu_steward"

    def test_a_domain_keeps_its_own_steward(self):
        assert effective_steward("retail_emea_uk", self.STEWARDS, PARENTS) == "uk_steward"

    def test_no_steward_up_the_tree_is_none(self):
        assert effective_steward("finance", self.STEWARDS, PARENTS) is None


@pytest.mark.parametrize("domain_id", sorted(PARENTS))
def test_every_domain_has_an_acyclic_ancestry(domain_id):
    assert ancestry(domain_id, PARENTS)[0] == domain_id
