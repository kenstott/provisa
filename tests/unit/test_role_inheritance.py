# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8a1c5f-9d2b-4f7e-8c6a-1b4d7e0f2a93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1677: one parent per role, walked child-first, folded into the build once."""

import pytest

from provisa.compiler.rls import build_rls_context
from provisa.core.models import Role, flatten_roles
from provisa.security.inheritance import (
    children_of,
    expand_column_grants,
    expand_grants,
    flatten_role_dicts,
    holds_grant,
    materialize_rls,
    parent_map,
    parent_problem,
    role_chain,
    role_chains,
)


def _role(rid, parent=None, caps=(), domains=(), rate_limit=None):
    return {
        "id": rid,
        "capabilities": list(caps),
        "domain_access": list(domains),
        "rate_limit": rate_limit,
        "parent_role_id": parent,
    }


ROLES = [
    _role("reader", caps=["query_development"], domains=["sales"], rate_limit={"rps": 5}),
    _role("analyst", parent="reader", caps=["full_results"], domains=["ops"]),
    _role("junior", parent="analyst"),
    _role("auditor"),
]
CHAINS = role_chains(ROLES)


class TestChain:
    def test_child_first(self):
        assert CHAINS["junior"] == ["junior", "analyst", "reader"]
        assert CHAINS["auditor"] == ["auditor"]

    def test_cycle_refused(self):
        with pytest.raises(ValueError, match="cycle"):
            role_chain("a", {"a": "b", "b": "a"})

    def test_parent_problem_self(self):
        assert parent_problem("a", "a", parent_map(ROLES)) == "Role 'a' cannot inherit from itself"

    def test_parent_problem_unknown(self):
        assert (
            parent_problem("junior", "ghost", parent_map(ROLES)) == "Parent role 'ghost' not found"
        )

    def test_parent_problem_cycle(self):
        msg = parent_problem("reader", "junior", parent_map(ROLES))
        assert msg == "Role 'reader' cannot inherit from 'junior': that would close a cycle"

    def test_parent_problem_ok(self):
        assert parent_problem("auditor", "reader", parent_map(ROLES)) is None
        assert parent_problem("auditor", None, parent_map(ROLES)) is None

    def test_children_of(self):
        assert children_of("analyst", ROLES) == ["junior"]
        assert children_of("junior", ROLES) == []


class TestFlattenDicts:
    def test_union_up_the_chain(self):
        by_id = {r["id"]: r for r in flatten_role_dicts(ROLES)}
        assert by_id["junior"]["capabilities"] == ["full_results", "query_development"]
        assert by_id["junior"]["domain_access"] == ["ops", "sales"]

    def test_rate_limit_inherited_when_unset(self):
        by_id = {r["id"]: r for r in flatten_role_dicts(ROLES)}
        assert by_id["junior"]["rate_limit"] == {"rps": 5}

    def test_own_rate_limit_wins(self):
        roles = [_role("p", rate_limit={"rps": 5}), _role("c", parent="p", rate_limit={"rps": 1})]
        assert flatten_role_dicts(roles)[1]["rate_limit"] == {"rps": 1}

    def test_input_unmodified(self):
        flatten_role_dicts(ROLES)
        assert ROLES[2]["capabilities"] == []

    def test_model_flatten_refuses_cycle(self):
        with pytest.raises(ValueError, match="cycle"):
            flatten_roles(
                [
                    Role(id="a", capabilities=[], domain_access=[], parent_role_id="b"),
                    Role(id="b", capabilities=[], domain_access=[], parent_role_id="a"),
                ]
            )


class TestGrants:
    def test_column_grants_gain_descendants(self):
        tables = [
            {
                "id": 1,
                "columns": [
                    {"column_name": "amount", "visible_to": ["reader"], "writable_by": ["analyst"]},
                    {"column_name": "ssn", "visible_to": ["auditor"], "unmasked_to": ["auditor"]},
                ],
            }
        ]
        expand_column_grants(tables, CHAINS)
        amount, ssn = tables[0]["columns"]
        assert amount["visible_to"] == ["reader", "analyst", "junior"]
        assert amount["writable_by"] == ["analyst", "junior"]
        assert ssn["visible_to"] == ["auditor"]
        assert ssn["unmasked_to"] == ["auditor"]

    def test_empty_grant_stays_empty(self):
        items = [{"visible_to": []}]
        expand_grants(items, CHAINS)
        assert items[0]["visible_to"] == []

    def test_holds_grant_through_ancestor(self):
        assert holds_grant("junior", ["reader"], CHAINS)
        assert not holds_grant("auditor", ["reader"], CHAINS)
        assert holds_grant("unknown", ["unknown"], CHAINS)


ORDERS = {"id": 10, "domain_id": "sales"}
CUSTOMERS = {"id": 11, "domain_id": "sales"}
TICKETS = {"id": 12, "domain_id": "ops"}
TABLES = [ORDERS, CUSTOMERS, TICKETS]


def _rule(role, expr, table_id=None, domain_id=None):
    return {
        "id": 1,
        "role_id": role,
        "table_id": table_id,
        "domain_id": domain_id,
        "filter_expr": expr,
    }


class TestMaterializeRls:
    def test_child_inherits_parent_table_rule(self):
        rules = [_rule("reader", "region = 'east'", table_id=10)]
        out = materialize_rls(rules, TABLES, CHAINS)
        junior = build_rls_context(out, "junior")
        assert junior.rules == {10: "region = 'east'"}
        assert junior.domain_rules == {}

    def test_child_table_rule_replaces_parent(self):
        rules = [
            _rule("reader", "region = 'east'", table_id=10),
            _rule("junior", "region = 'west'", table_id=10),
        ]
        junior = build_rls_context(materialize_rls(rules, TABLES, CHAINS), "junior")
        assert junior.rules == {10: "region = 'west'"}

    def test_nearer_domain_rule_beats_farther_table_rule(self):
        rules = [
            _rule("reader", "region = 'east'", table_id=10),
            _rule("analyst", "tenant = 't1'", domain_id="sales"),
        ]
        junior = build_rls_context(materialize_rls(rules, TABLES, CHAINS), "junior")
        assert junior.rules == {10: "tenant = 't1'", 11: "tenant = 't1'"}

    def test_own_domain_rule_stands_and_is_not_duplicated(self):
        rules = [
            _rule("reader", "region = 'east'", table_id=10),
            _rule("junior", "tenant = 't1'", domain_id="sales"),
        ]
        out = materialize_rls(rules, TABLES, CHAINS)
        junior = build_rls_context(out, "junior")
        assert junior.rules == {}
        assert junior.domain_rules == {"sales": "tenant = 't1'"}
        assert [r for r in out if r["role_id"] == "junior" and r.get("inherited_from")] == []

    def test_parent_domain_rule_lands_per_table_of_that_domain_only(self):
        rules = [_rule("reader", "tenant = 't1'", domain_id="sales")]
        junior = build_rls_context(materialize_rls(rules, TABLES, CHAINS), "junior")
        assert set(junior.rules) == {10, 11}
        assert 12 not in junior.rules

    def test_parent_rules_untouched_and_root_roles_untouched(self):
        rules = [_rule("reader", "region = 'east'", table_id=10)]
        out = materialize_rls(rules, TABLES, CHAINS)
        assert build_rls_context(out, "reader").rules == {10: "region = 'east'"}
        assert build_rls_context(out, "auditor").rules == {}

    def test_inherited_rule_records_its_source(self):
        rules = [_rule("reader", "region = 'east'", table_id=10)]
        added = [r for r in materialize_rls(rules, TABLES, CHAINS) if r["role_id"] == "junior"]
        assert added[0]["inherited_from"] == "reader"
