# Copyright (c) 2026 Kenneth Stott
# Canary: 1ad7d9a7-a9ae-4e81-b55f-e37d0facbbc9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219, OWNER KINDS: owning is a capability of its own. data_product_owner, relationship_owner
and domain_steward are scoped by the data products, relationships and domains they own;
term_owner by glossary terms, named domain.term (an asterisk at either level); and
judgment_basis_owner by judgment bases, named by their id."""

from __future__ import annotations

import pytest

from provisa.entitlements import store
from provisa.entitlements.index import (
    GLOSSARY_TERMS,
    JUDGMENT_BASES,
    KIND_ORDER,
    KINDS,
    capability_scopes,
    check_lists,
    identifier_problem,
    owned_objects,
    selects_term,
)
from provisa.entitlements.lists import EffectiveList
from provisa.security.rights import Capability
from tests.unit.test_entitlement_lists import _effective, _model_index
from tests.unit.test_entitlements_api import _role, _set
from tests.unit.test_entitlements_api import api as api  # noqa: F401  (fixture)

OWNERS = {
    Capability.DATA_PRODUCT_OWNER.value: "data_products",
    Capability.RELATIONSHIP_OWNER.value: "relationships",
    Capability.DOMAIN_STEWARD.value: "domains",
    Capability.TERM_OWNER.value: GLOSSARY_TERMS,
    Capability.JUDGMENT_BASIS_OWNER.value: JUDGMENT_BASES,
}

_OWNED = owned_objects(
    terms=[
        ("Customer Account", ("sales",)),
        ("Revenue", ("finance", "sales")),
        ("Fiscal Year", ("*",)),
    ],
    bases=["gdpr-art-6", "sox.404"],
)


# -- the capabilities and their kinds -----------------------------------------------------------


def test_each_owner_capability_is_scoped_by_the_kind_it_owns():
    scopes = capability_scopes()
    assert {cap: scopes[cap] for cap in OWNERS} == OWNERS


def test_the_write_accepts_the_two_new_kinds():
    assert GLOSSARY_TERMS in KINDS and JUDGMENT_BASES in KINDS
    assert KIND_ORDER.index(GLOSSARY_TERMS) > KIND_ORDER.index("data_products")


# -- identifiers ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "identifier",
    ["sales.customer_account", "sales.*", "*.revenue", "*.*", "*", "SALES.Customer_Account"],
)
def test_a_term_identifier_names_domain_and_term(identifier):
    assert identifier_problem(GLOSSARY_TERMS, identifier) is None


@pytest.mark.parametrize(
    "identifier", ["customer_account", "sales.customer_account.extra", "sales.", ".revenue"]
)
def test_a_term_identifier_not_of_the_form_domain_term_is_a_problem_naming_it(identifier):
    problem = identifier_problem(GLOSSARY_TERMS, identifier)
    assert problem is not None and repr(identifier) in problem


@pytest.mark.parametrize("identifier", ["gdpr-art-6", "sox.404", "*"])
def test_a_judgment_basis_identifier_is_its_id(identifier):
    assert identifier_problem(JUDGMENT_BASES, identifier) is None


@pytest.mark.parametrize("identifier", ["GDPR Art 6", "-lead", "sox.*"])
def test_a_judgment_basis_identifier_not_an_id_is_a_problem_naming_it(identifier):
    problem = identifier_problem(JUDGMENT_BASES, identifier)
    assert problem is not None and repr(identifier) in problem


def test_the_other_kinds_keep_their_own_identifier_forms():
    assert identifier_problem("tables", "sales.orders.extra.segments") is None
    assert identifier_problem("data_products", "sales.p1") is None


# -- a term identifier resolves per domain -------------------------------------------------------


def test_domain_term_selects_the_term_in_that_domain_only():
    assert selects_term("sales.customer_account", "Customer Account", ("sales",))
    assert not selects_term("finance.customer_account", "Customer Account", ("sales",))
    assert selects_term("finance.revenue", "Revenue", ("finance", "sales"))


def test_an_asterisk_at_either_level_selects_across_it():
    assert selects_term("sales.*", "Customer Account", ("sales",))
    assert not selects_term("finance.*", "Customer Account", ("sales",))
    assert selects_term("*.revenue", "Revenue", ("finance",))
    assert selects_term("*.*", "Revenue", ("finance",))
    assert selects_term("*", "Revenue", ("finance",))


def test_an_enterprise_term_belongs_to_no_one_domain():
    assert selects_term("*.fiscal_year", "Fiscal Year", ("*",))
    assert not selects_term("sales.fiscal_year", "Fiscal Year", ("*",))


def test_a_term_identifier_matches_ignoring_case():
    assert selects_term("Sales.CUSTOMER_ACCOUNT", "customer account", ("Sales",))


# -- checks --------------------------------------------------------------------------------------


def _check(kind, identifiers, capabilities):
    lists = {kind: EffectiveList(kind, tuple(identifiers), "provisa")}
    return check_lists("steward", _role(capabilities), lists, _model_index(), _OWNED)


def test_a_known_term_or_basis_is_not_flagged():
    assert _check(GLOSSARY_TERMS, ["sales.customer_account", "*.fiscal_year"], ["term_owner"]) == []
    assert _check(JUDGMENT_BASES, ["gdpr-art-6", "sox.404", "*"], ["judgment_basis_owner"]) == []


def test_an_unknown_term_is_flagged_by_name():
    findings = _check(GLOSSARY_TERMS, ["sales.revenue", "finance.customer_account"], ["term_owner"])
    assert [(f.code, f.kind, f.identifier) for f in findings] == [
        ("unknown_identifier", GLOSSARY_TERMS, "finance.customer_account")
    ]
    assert "finance.customer_account" in findings[0].message


def test_a_term_wildcard_naming_nothing_is_flagged():
    findings = _check(GLOSSARY_TERMS, ["hr.*"], ["term_owner"])
    assert [(f.code, f.identifier) for f in findings] == [("unknown_identifier", "hr.*")]


def test_an_unknown_basis_is_flagged_by_name():
    findings = _check(JUDGMENT_BASES, ["gdpr-art-6", "hipaa"], ["judgment_basis_owner"])
    assert [(f.code, f.kind, f.identifier) for f in findings] == [
        ("unknown_identifier", JUDGMENT_BASES, "hipaa")
    ]


def test_a_term_list_on_a_role_without_term_owner_is_unused():
    findings = _check(GLOSSARY_TERMS, ["sales.customer_account"], [])
    assert [(f.code, f.kind) for f in findings] == [("unused_kind", GLOSSARY_TERMS)]


def test_the_semantic_kinds_are_still_checked_against_the_names_table():
    lists = _effective({"steward": {"data_products": ("sales.nope",)}}, role="steward")
    findings = check_lists("steward", _role(["data_product_owner"]), lists, _model_index(), _OWNED)
    assert [(f.code, f.identifier) for f in findings] == [("unknown_identifier", "sales.nope")]


# -- granted through the write API ---------------------------------------------------------------


def _steward(api_):
    api_.state.roles["steward"] = _role(list(OWNERS))
    api_.state.role_chains["steward"] = ["steward"]
    api_.state.glossary_term_refs = (("Customer Account", ("sales",)),)
    api_.state.judgment_basis_ids = ("gdpr-art-6",)


async def _capabilities(api_):
    response = await api_.client.get(
        "/admin/entitlements/effective/capabilities",
        params={"principal": "alice", "role_ids": "steward"},
    )
    assert response.status_code == 200, response.text
    return {row["capability"]: row for row in response.json()}


@pytest.mark.parametrize(
    ("kind", "identifier", "capability"),
    [
        (GLOSSARY_TERMS, "sales.customer_account", "term_owner"),
        (JUDGMENT_BASES, "gdpr-art-6", "judgment_basis_owner"),
    ],
)
async def test_a_list_of_a_new_kind_scopes_its_owner_capability(api, kind, identifier, capability):
    _steward(api)
    response = await api.client.put(
        "/admin/entitlements/lists",
        json=_set(role="steward", kind=kind, ids=[identifier]),
    )
    assert response.status_code == 200, response.text
    assert response.json()["findings"] == []
    view = (await _capabilities(api))[capability]
    assert view["kind"] == kind
    assert view["identifiers"] == [identifier]


@pytest.mark.parametrize(
    ("capability", "kind", "identifier"),
    [
        ("data_product_owner", "data_products", "sales.p1"),
        ("relationship_owner", "relationships", "sales.orders_customer"),
        ("domain_steward", "domains", "sales"),
        ("term_owner", GLOSSARY_TERMS, "sales.customer_account"),
        ("judgment_basis_owner", JUDGMENT_BASES, "gdpr-art-6"),
    ],
)
async def test_each_owner_capability_is_granted_by_a_row_of_its_own(
    api, capability, kind, identifier
):
    _steward(api)
    async with api.db.acquire() as conn:
        await store.set_list(
            conn,
            "alice",
            "steward",
            kind,
            [identifier],
            set_by="admin",
            reason="owns it",
            capability=capability,
        )
        api.state.held_entitlements = await store.load_held(conn)
    caps = await _capabilities(api)
    assert caps[capability]["kind"] == kind
    assert caps[capability]["identifiers"] == [identifier]
    # The row is the capability's alone: another owner capability on the same kind is untouched.
    others = [c for c, k in OWNERS.items() if k == kind and c != capability]
    assert all(caps[c]["identifiers"] == ["*"] for c in others)


@pytest.mark.parametrize(
    ("kind", "identifier"),
    [(GLOSSARY_TERMS, "customer_account"), (JUDGMENT_BASES, "GDPR Art 6")],
)
async def test_an_identifier_not_of_its_kinds_form_is_refused_by_name(api, kind, identifier):
    _steward(api)
    for path, method, body in (
        ("/admin/entitlements/lists", "put", _set(role="steward", kind=kind, ids=[identifier])),
        (
            "/admin/entitlements/lists/add",
            "post",
            _set(role="steward", kind=kind, ids=[identifier]),
        ),
    ):
        response = await api.client.request(method, path, json=body)
        assert response.status_code == 422, response.text
        assert response.json()["code"] == "entitlements.invalid_identifier"
        assert response.json()["params"] == {"kind": kind, "identifier": identifier}
