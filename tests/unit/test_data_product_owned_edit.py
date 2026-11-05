# Copyright (c) 2026 Kenneth Stott
# Canary: 1d3c0219-67b3-40b7-810b-e3673ec0b44f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2188, superseding REQ-2155's data_product_owned_rw: editing a data product can be limited to
the products a role owns, by a rule and not by a right. data_product_rw edits any product its reach
covers; RW from a product's Owner field carries the generated rules, which admit the products whose
owner_role the caller holds, an ancestor included, within its reach. RW includes create, which has
no row to narrow."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.admin.capabilities import right_domain_refusal
from provisa.core.request_context import session_vars_for
from provisa.security.inheritance import flatten_role_dicts
from provisa.security.row_filters import (
    OWNER_RULE,
    acting_model_rule,
    model_write_rules,
    with_owner_grant,
)

_STORED = [
    {
        "id": "steward",
        "capabilities": ["data_product_rw"],
        "domain_access": ["sales"],
        "parents": [],
    },
    {"id": "owner", "capabilities": [], "domain_access": ["*"], "parents": []},
    {"id": "sales_lead", "capabilities": [], "domain_access": ["sales"], "parents": ["owner"]},
    {
        "id": "reader",
        "capabilities": ["data_product_read"],
        "domain_access": ["*"],
        "parents": [],
    },
]
_FOLDED = flatten_role_dicts(with_owner_grant(_STORED, {"owner"}))
_CHAINS = {
    "steward": ["steward"],
    "owner": ["owner"],
    "sales_lead": ["sales_lead", "owner"],
    "reader": ["reader"],
}
_STATE = SimpleNamespace(
    roles={r["id"]: {**r, "acting_chain": _CHAINS[r["id"]]} for r in _FOLDED},
    role_chains=_CHAINS,
    meta_roles={},
    role_build_inputs={
        "model_write_rules": model_write_rules(
            [], [], [r["id"] for r in _FOLDED if r.get("owner_held")]
        ),
        "physical_table_map": {},
    },
)


def _as(role):
    return SimpleNamespace(user_id="u", acting_roles=[role], env_capabilities=set())


def _refusal(role, domains):
    return right_domain_refusal(_as(role), _STATE, "data_product_rw", domains)


def _rule(role):
    return acting_model_rule(_STATE, role, "data_products", "U")


def test_the_edit_any_right_edits_any_product_in_its_reach():
    assert _refusal("steward", {"sales"}) is None
    assert _refusal("steward", {"ops"}) is not None
    assert _rule("steward") is None


def test_an_owner_edits_the_product_it_owns():
    assert _refusal("owner", {"sales"}) is None
    rule = _rule("owner")
    assert rule is not None and rule.predicate == OWNER_RULE


def test_ownership_held_through_an_ancestor_counts():
    assert _refusal("sales_lead", {"sales"}) is None
    assert session_vars_for(_STATE.roles["sales_lead"])["acting_roles"] == "sales_lead,owner"
    assert _rule("sales_lead") is not None


def test_an_owner_edits_no_product_another_role_owns():
    assert session_vars_for(_STATE.roles["owner"])["acting_roles"] == "owner"


def test_an_owners_edit_stays_within_its_reach():
    assert _refusal("sales_lead", {"ops"}) is not None


def test_an_owner_creates_as_rw_does():
    """REQ-2188: RW includes create; the rules narrow what exists."""
    assert _refusal("owner", {"sales"}) is None


def test_a_reader_edits_nothing():
    assert _refusal("reader", {"sales"})
