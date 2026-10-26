# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8a1c5f-9d2b-4f7e-8c6a-1b4d7e0f2a93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1677, REQ-2155: a role inherits from its parents, walked child-first, depth-first in
declared parent order, folded into the build once."""

import pytest

from provisa.compiler.rls import build_rls_context
from provisa.core.models import Role, flatten_roles
from provisa.security.inheritance import (
    children_of,
    expand_column_grants,
    expand_grants,
    expand_write_grants,
    flatten_role_dicts,
    holds_grant,
    materialize_rls,
    parent_map,
    parents_problem,
    role_chain,
    role_chains,
)


def _role(rid, parent=None, caps=(), domains=(), rate_limit=None, parents=None):
    return {
        "id": rid,
        "description": "Test role.",
        "capabilities": list(caps),
        "domain_access": list(domains),
        "rate_limit": rate_limit,
        "parents": list(parents) if parents is not None else ([parent] if parent else []),
    }


ROLES = [
    _role("reader", caps=["query_development"], domains=["sales"], rate_limit={"rps": 5}),
    _role("analyst", parent="reader", caps=["full_results"], domains=["ops"]),
    _role("junior", parent="analyst"),
    _role("auditor"),
]
CHAINS = role_chains(ROLES)
PARENTS = parent_map(ROLES)


class TestChain:
    def test_child_first(self):
        assert CHAINS["junior"] == ["junior", "analyst", "reader"]
        assert CHAINS["auditor"] == ["auditor"]

    def test_cycle_refused(self):
        with pytest.raises(ValueError, match="cycle"):
            role_chain("a", {"a": ["b"], "b": ["a"]})

    def test_parents_problem_self(self):
        assert (
            parents_problem("a", ["a"], parent_map(ROLES)) == "Role 'a' cannot inherit from itself"
        )

    def test_parents_problem_unknown(self):
        assert (
            parents_problem("junior", ["analyst", "ghost"], parent_map(ROLES))
            == "Parent role 'ghost' not found"
        )

    def test_parents_problem_cycle(self):
        msg = parents_problem("reader", ["auditor", "junior"], parent_map(ROLES))
        assert msg == "Role 'reader' cannot inherit from 'junior': that would close a cycle"

    def test_parents_problem_repeated(self):
        msg = parents_problem("auditor", ["reader", "reader"], parent_map(ROLES))
        assert msg == "Role 'auditor' names parent 'reader' twice"

    def test_parents_problem_ok(self):
        assert parents_problem("auditor", ["reader", "analyst"], parent_map(ROLES)) is None
        assert parents_problem("auditor", [], parent_map(ROLES)) is None

    def test_children_of(self):
        assert children_of("analyst", ROLES) == ["junior"]
        assert children_of("junior", ROLES) == []


class TestFlattenDicts:
    def test_capabilities_union_up_the_chain(self):
        by_id = {r["id"]: r for r in flatten_role_dicts(ROLES)}
        assert by_id["junior"]["capabilities"] == ["full_results", "query_development"]

    def test_reach_is_the_roles_own_and_never_inherited(self):  # REQ-2155
        by_id = {r["id"]: r for r in flatten_role_dicts(ROLES)}
        assert by_id["analyst"]["domain_access"] == ["ops"]
        assert by_id["junior"]["domain_access"] == []

    def test_a_domain_child_of_an_every_domain_parent_reaches_its_domain_only(self):  # REQ-2155
        roles = [
            _role("steward", caps=["glossary_rw"], domains=["*"]),
            _role("finance_steward", parent="steward", domains=["finance"]),
        ]
        child = flatten_role_dicts(roles)[1]
        assert child["domain_access"] == ["finance"]
        assert child["capabilities"] == ["glossary_rw"]

    def test_rate_limit_inherited_when_unset(self):
        by_id = {r["id"]: r for r in flatten_role_dicts(ROLES)}
        assert by_id["junior"]["rate_limit"] == {"rps": 5}

    def test_own_rate_limit_wins(self):
        roles = [_role("p", rate_limit={"rps": 5}), _role("c", parent="p", rate_limit={"rps": 1})]
        assert flatten_role_dicts(roles)[1]["rate_limit"] == {"rps": 1}

    def test_max_rows_inherited_when_unset(self):
        roles = [{**_role("p"), "max_rows": 100}, {**_role("c", parent="p"), "max_rows": None}]
        assert flatten_role_dicts(roles)[1]["max_rows"] == 100

    def test_own_max_rows_wins_even_above_the_parent(self):
        # A property is set by the nearest role that declares it; a child may raise it.
        roles = [{**_role("p"), "max_rows": 100}, {**_role("c", parent="p"), "max_rows": 1000}]
        assert flatten_role_dicts(roles)[1]["max_rows"] == 1000

    def test_input_unmodified(self):
        flatten_role_dicts(ROLES)
        assert ROLES[2]["capabilities"] == []

    def test_model_flatten_refuses_cycle(self):
        with pytest.raises(ValueError, match="cycle"):
            flatten_roles(
                [
                    Role(
                        id="a",
                        description="Test role.",
                        capabilities=[],
                        domain_access=["*"],
                        parents=["b"],
                    ),
                    Role(
                        id="b",
                        description="Test role.",
                        capabilities=[],
                        domain_access=["*"],
                        parents=["a"],
                    ),
                ]
            )


class TestGrants:
    def test_column_grants_gain_descendants(self):
        tables = [
            {
                "id": 1,
                "columns": [
                    {"column_name": "amount", "visible_to": ["reader"]},
                    {"column_name": "ssn", "visible_to": ["auditor"], "unmasked_to": ["auditor"]},
                ],
            }
        ]
        expand_column_grants(tables, CHAINS)
        amount, ssn = tables[0]["columns"]
        assert amount["visible_to"] == ["reader", "analyst", "junior"]
        assert ssn["visible_to"] == ["auditor"]
        assert ssn["unmasked_to"] == ["auditor"]

    def test_write_grants_reach_descendants_child_first(self):  # REQ-2074
        def grant(role, op, columns):
            return {
                "role": role,
                "operation": op,
                "columns": columns,
                "rows": None,
                "check": None,
                "presets": [],
            }

        tables = [
            {
                "id": 1,
                "write_grants": [
                    grant("reader", "update", ["a", "b"]),
                    grant("analyst", "update", ["a"]),
                    grant("reader", "delete", []),
                ],
            }
        ]
        expand_write_grants(tables, PARENTS)
        held = {(g["role"], g["operation"]): g["columns"] for g in tables[0]["write_grants"]}
        assert held[("junior", "update")] == ["a"]  # the nearest: analyst's, not reader's
        assert ("junior", "delete") in held and ("analyst", "delete") in held
        assert ("auditor", "update") not in held  # no ancestor holds one

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
        "description": "Test rule.",
        "id": 1,
        "role_id": role,
        "table_id": table_id,
        "domain_id": domain_id,
        "filter_expr": expr,
    }


class TestMaterializeRls:
    def test_child_inherits_parent_table_rule(self):
        rules = [_rule("reader", "region = 'east'", table_id=10)]
        out = materialize_rls(rules, TABLES, PARENTS)
        junior = build_rls_context(out, "junior")
        assert junior.rules == {10: "region = 'east'"}
        assert junior.domain_rules == {}

    def test_child_table_rule_replaces_parent(self):
        rules = [
            _rule("reader", "region = 'east'", table_id=10),
            _rule("junior", "region = 'west'", table_id=10),
        ]
        junior = build_rls_context(materialize_rls(rules, TABLES, PARENTS), "junior")
        assert junior.rules == {10: "region = 'west'"}

    def test_nearer_domain_rule_beats_farther_table_rule(self):
        rules = [
            _rule("reader", "region = 'east'", table_id=10),
            _rule("analyst", "tenant = 't1'", domain_id="sales"),
        ]
        junior = build_rls_context(materialize_rls(rules, TABLES, PARENTS), "junior")
        assert junior.rules == {10: "tenant = 't1'", 11: "tenant = 't1'"}

    def test_own_domain_rule_stands_and_is_not_duplicated(self):
        rules = [
            _rule("reader", "region = 'east'", table_id=10),
            _rule("junior", "tenant = 't1'", domain_id="sales"),
        ]
        out = materialize_rls(rules, TABLES, PARENTS)
        junior = build_rls_context(out, "junior")
        assert junior.rules == {}
        assert junior.domain_rules == {"sales": "tenant = 't1'"}
        assert [r for r in out if r["role_id"] == "junior" and r.get("inherited_from")] == []

    def test_parent_domain_rule_lands_per_table_of_that_domain_only(self):
        rules = [_rule("reader", "tenant = 't1'", domain_id="sales")]
        junior = build_rls_context(materialize_rls(rules, TABLES, PARENTS), "junior")
        assert set(junior.rules) == {10, 11}
        assert 12 not in junior.rules

    def test_parent_rules_untouched_and_root_roles_untouched(self):
        rules = [_rule("reader", "region = 'east'", table_id=10)]
        out = materialize_rls(rules, TABLES, PARENTS)
        assert build_rls_context(out, "reader").rules == {10: "region = 'east'"}
        assert build_rls_context(out, "auditor").rules == {}

    def test_inherited_rule_records_its_source(self):
        rules = [_rule("reader", "region = 'east'", table_id=10)]
        added = [r for r in materialize_rls(rules, TABLES, PARENTS) if r["role_id"] == "junior"]
        assert added[0]["inherited_from"] == "reader"


# REQ-2155: several parents. The ancestors are walked depth-first in declared parent order, a role
# met twice kept at its first place; rights are unions over them.
VIEWER = _role("viewer", caps=["glossary_read"])
STEWARD = _role("steward", parents=["viewer"], caps=["glossary_rw"], domains=["sales"])
OWNER = _role("owner", parents=["viewer"], caps=["data_product_owned_rw"], rate_limit={"rps": 9})
LEAD = _role("lead", parents=["steward", "owner"])
MULTI = [VIEWER, STEWARD, OWNER, LEAD]
MULTI_CHAINS = role_chains(MULTI)
MULTI_PARENTS = parent_map(MULTI)


class TestSeveralParents:
    def test_depth_first_in_declared_order_each_ancestor_once(self):
        assert MULTI_CHAINS["lead"] == ["lead", "steward", "viewer", "owner"]

    def test_capabilities_are_the_union_of_every_ancestor(self):
        lead = {r["id"]: r for r in flatten_role_dicts(MULTI)}["lead"]
        assert lead["capabilities"] == ["data_product_owned_rw", "glossary_read", "glossary_rw"]
        assert lead["domain_access"] == []  # reach is its own (REQ-2155)

    def test_a_property_comes_from_the_first_ancestor_that_sets_one(self):
        lead = {r["id"]: r for r in flatten_role_dicts(MULTI)}["lead"]
        assert lead["rate_limit"] == {"rps": 9}

    def test_a_grant_to_either_parent_reaches_the_child(self):
        assert holds_grant("lead", ["owner"], MULTI_CHAINS)
        assert holds_grant("lead", ["steward"], MULTI_CHAINS)

    def test_a_cycle_through_a_second_parent_is_refused(self):
        with pytest.raises(ValueError, match="cycle"):
            role_chain("a", {"a": ["b", "c"], "b": [], "c": ["a"]})


class TestSeveralParentsRowRules:
    """A role's own rule for a table stands; otherwise the rules its parents resolve are ORed, and a
    parent that resolves none contributes nothing: adding a parent never lifts a row rule."""

    def test_a_child_of_viewer_and_a_restricted_parent_stays_restricted(self):
        rules = [_rule("steward", "region = 'east'", table_id=10)]
        lead = build_rls_context(materialize_rls(rules, TABLES, MULTI_PARENTS), "lead")
        assert lead.rules == {10: "region = 'east'"}

    def test_two_parents_rules_are_ored(self):
        rules = [
            _rule("steward", "region = 'east'", table_id=10),
            _rule("owner", "owner_id = 7", domain_id="sales"),
        ]
        lead = build_rls_context(materialize_rls(rules, TABLES, MULTI_PARENTS), "lead")
        assert lead.rules == {10: "(region = 'east') OR (owner_id = 7)", 11: "owner_id = 7"}

    def test_one_rule_reached_through_both_parents_is_not_repeated(self):
        rules = [_rule("viewer", "region = 'east'", table_id=10)]
        lead = build_rls_context(materialize_rls(rules, TABLES, MULTI_PARENTS), "lead")
        assert lead.rules == {10: "region = 'east'"}

    def test_the_childs_own_rule_stands(self):
        rules = [
            _rule("steward", "region = 'east'", table_id=10),
            _rule("lead", "region = 'west'", table_id=10),
        ]
        lead = build_rls_context(materialize_rls(rules, TABLES, MULTI_PARENTS), "lead")
        assert lead.rules == {10: "region = 'west'"}

    def test_no_rule_anywhere_leaves_the_role_unrestricted(self):
        lead = build_rls_context(materialize_rls([], TABLES, MULTI_PARENTS), "lead")
        assert lead.rules == {}

    def test_write_grant_row_conditions_follow_the_same_rule(self):  # REQ-2074
        def grant(role, rows, columns):
            return {
                "role": role,
                "operation": "update",
                "columns": columns,
                "rows": rows,
                "check": None,
                "presets": [],
            }

        tables = [
            {
                "id": 1,
                "write_grants": [
                    grant("steward", "region = 'east'", ["a"]),
                    grant("owner", None, ["b"]),
                ],
            }
        ]
        expand_write_grants(tables, MULTI_PARENTS)
        lead = next(g for g in tables[0]["write_grants"] if g["role"] == "lead")
        # A parent's grant with no row condition writes rows its read filter reaches (schema_org
        # table_write_grants: NULL is the role's read row filter), so it lifts no restriction.
        assert lead["rows"] == "region = 'east'"
        assert lead["columns"] == ["a", "b"]
