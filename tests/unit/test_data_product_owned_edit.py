# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2155: editing a data product can be limited to the products a role owns. data_product_rw
edits any product its reach covers; data_product_owned_rw edits an existing product whose
owner_role the caller holds, an ancestor included, within its reach -- and creates or deletes
none."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.admin.capabilities import data_product_edit_refusal

_ROLES = {
    "steward": {"capabilities": ["data_product_rw"], "domain_access": ["sales"]},
    "owner": {"capabilities": ["data_product_owned_rw"], "domain_access": ["*"]},
    "sales_lead": {"capabilities": ["data_product_owned_rw"], "domain_access": ["sales"]},
    "reader": {"capabilities": ["data_product_read"], "domain_access": ["*"]},
}
_STATE = SimpleNamespace(
    roles=_ROLES,
    role_chains={
        "steward": ["steward"],
        "owner": ["owner"],
        "sales_lead": ["sales_lead", "owner"],
        "reader": ["reader"],
    },
)


def _as(role):
    return SimpleNamespace(user_id="u", acting_roles=[role], env_capabilities=set())


def _product(owner_role, domain="sales"):
    return {"id": "p", "domain_id": domain, "owner_role": owner_role}


def test_the_edit_any_right_edits_any_product_in_its_reach():
    assert data_product_edit_refusal(_as("steward"), _STATE, _product("x"), {"sales"}) is None
    assert data_product_edit_refusal(_as("steward"), _STATE, _product("x"), {"ops"}) is not None


def test_an_owner_edits_the_product_it_owns():
    assert data_product_edit_refusal(_as("owner"), _STATE, _product("owner"), {"sales"}) is None


def test_ownership_held_through_an_ancestor_counts():
    assert (
        data_product_edit_refusal(_as("sales_lead"), _STATE, _product("owner"), {"sales"}) is None
    )


def test_an_owner_edits_no_product_another_role_owns():
    assert data_product_edit_refusal(_as("owner"), _STATE, _product("other"), {"sales"}) is not None


def test_an_owners_edit_stays_within_its_reach():
    refusal = data_product_edit_refusal(_as("sales_lead"), _STATE, _product("owner"), {"ops"})
    assert refusal is not None


def test_an_owner_creates_nothing():
    assert data_product_edit_refusal(_as("owner"), _STATE, None, {"sales"}) is not None


def test_a_reader_edits_nothing():
    assert data_product_edit_refusal(_as("reader"), _STATE, _product("reader"), {"sales"})
