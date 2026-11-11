# Copyright (c) 2026 Kenneth Stott
# Canary: 5e8d2b17-4c63-4a90-8f1e-7b3a9c6d0e24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (amended 2026-11-11, AN OFFER'S ROLE IS ONE ITS OWNER MAY GRANT; OFFERS): who decides a
bid, who may put a role in an offer, and the columns an offer's contract states, as the offers
router asks them."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api.admin import data_product_offers_router as router


def _request(user_id: str = "anonymous", role: str = "") -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(identity=SimpleNamespace(user_id=user_id), role=role)
    )


def _state(**more):
    return SimpleNamespace(
        roles={"viewer": {"capabilities": []}, "owner": {"capabilities": []}},
        role_chains={"steward": ["steward", "owner"], "owner": ["owner"], "viewer": ["viewer"]},
        meta_roles={"meta:viewer+steward": ["viewer", "steward"]},
        **more,
    )


@pytest.mark.parametrize(
    ("acting", "owner"),
    [
        ("owner", True),
        ("steward", True),  # beneath the owner role
        ("meta:viewer+steward", True),  # a set acting together, one beneath it
        ("viewer", False),
    ],
)
def test_a_bid_is_decided_by_the_products_owner_role(acting, owner):
    request = _request("u1", acting)
    assert router._is_owner(request, _state(), {"owner_role": "owner"}) is owner


def test_a_product_with_no_owner_role_has_no_one_to_decide_its_bids():
    assert router._is_owner(_request("u1", "owner"), _state(), {"owner_role": None}) is False


def test_dev_mode_opens_every_gate_but_an_undefined_role():
    state = _state()
    assert router._is_owner(_request(), state, {"owner_role": None}) is True
    assert router.may_grant(_request(), state, "viewer") is None
    assert "not defined" in (router.may_grant(_request(), state, "ghost") or "")


def test_a_signed_in_caller_grants_only_what_their_roles_list_reaches(monkeypatch):
    monkeypatch.setattr(router, "reaches_role", lambda _state, role, _caps: role == "viewer")
    monkeypatch.setattr(
        "provisa.api.admin._platform_guard.granter_capabilities",
        lambda _request: frozenset({"assign_entitlements"}),
    )
    state = _state()
    assert router.may_grant(_request("u1", "owner"), state, "viewer") is None
    refusal = router.may_grant(_request("u1", "owner"), state, "owner")
    assert refusal == "role 'owner' is outside the roles you may grant"


def test_an_open_offer_with_no_publisher_is_listed_for_review():
    refusal = asyncio.run(router._publisher_refusal(None, _state(), None, "viewer"))
    assert refusal == router.REVIEW_RIGHT_GONE


def test_an_offers_contract_states_the_columns_its_role_reads(monkeypatch):
    table = {
        "id": 1,
        "domain_id": "sales",
        "columns": [
            {"column_name": "id", "data_type": "integer"},
            {"column_name": "email", "data_type": "text", "description": "Where to write"},
            {"column_name": "salary", "data_type": "numeric"},
            {"column_name": "region", "data_type": "text", "native_filter_type": "text"},
        ],
    }

    def view(state, role_id, **kw):
        assert (role_id, kw["governance"], kw["tables"]) == ("viewer", True, [table])
        return [
            {
                "columns": [
                    {"name": "id", "read": True, "mask": None},
                    {"name": "email", "read": True, "mask": {"type": "regex"}},
                    {"name": "salary", "read": False, "mask": None},  # written, not read
                ]
            }
        ]

    monkeypatch.setattr("provisa.security.effective_rights.data_view_from_state", view)
    seen = router._seen_columns(_state(), "viewer", table)
    assert [(c.name, c.read, c.mask, c.description) for c in seen] == [
        ("id", True, None, None),
        ("email", True, "regex", "Where to write"),
        ("salary", False, None, None),
    ]
