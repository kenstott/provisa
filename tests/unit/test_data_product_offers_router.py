# Copyright (c) 2026 Kenneth Stott
# Canary: 5e8d2b17-4c63-4a90-8f1e-7b3a9c6d0e24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (amended 2026-11-11, AN OFFER'S ROLE IS ONE ITS OWNER MAY GRANT; OFFERS; REQUESTS GO
THROUGH HOOKS): who decides a bid, who may put a role in an offer or decide a request, and the
columns an offer's contract states, as the offers router asks them."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api import offer_derivation
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


def test_an_external_deciders_right_is_asked_of_the_roles_they_hold(monkeypatch):
    held_by: dict[str, list[str]] = {"o1": ["owner"], "u9": ["viewer"]}

    async def held_roles(_conn, principal, _idp, _auth, _roles):
        return [SimpleNamespace(role_id=r) for r in held_by[principal]]

    monkeypatch.setattr("provisa.entitlements.roles.held_roles", held_roles)
    monkeypatch.setattr(
        "provisa.security.rights.capabilities_for_claims",
        lambda acting, _roles: {"assign_entitlements"} if "owner" in acting else set(),
    )
    from provisa.entitlements import runtime

    def reaches(_state, _role, _caps):
        bound = runtime._current.get()
        assert bound is not None  # asked of the decider, bound as they hold their roles
        return "owner" in bound.acting_roles

    monkeypatch.setattr(router, "reaches_role", reaches)
    state = _state(idp_entitlements={}, auth_config={})
    assert asyncio.run(router.principal_refusal(None, state, "o1", "viewer")) is None
    refusal = asyncio.run(router.principal_refusal(None, state, "u9", "viewer"))
    assert refusal == "role 'viewer' is outside the roles you may grant"
    assert "not defined" in (
        asyncio.run(router.principal_refusal(None, state, "o1", "ghost")) or ""
    )


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
    seen = offer_derivation.seen_columns(_state(), "viewer", table)
    assert [(c.name, c.read, c.mask, c.description) for c in seen] == [
        ("id", True, None, None),
        ("email", True, "regex", "Where to write"),
        ("salary", False, None, None),
    ]


@pytest.mark.parametrize(
    ("code", "reason"),
    [
        ("synthetic_needs_profiler", router.REVIEW_PROFILER_GONE),
        ("synthetic_needs_fake", router.REVIEW_FAKE_GONE),
    ],
)
def test_a_decision_refused_for_a_synthetic_need_lists_the_offer_for_review(
    monkeypatch, code, reason
):
    from provisa.core.repositories.data_product_offer import OfferRefused

    listed: list[tuple[int, str | None]] = []

    async def decide(*_a: Any, **_k: Any) -> dict:
        raise OfferRefused(code, "refused", status=409)

    async def require_request(_conn: Any, _rid: int) -> dict:
        return {"offer_id": 7}

    async def set_review(_conn: Any, offer_id: int, why: str | None) -> None:
        listed.append((offer_id, why))

    class _Tx:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *_exc: Any) -> bool:
            return False

    conn = SimpleNamespace(transaction=lambda: _Tx())
    monkeypatch.setattr(router.offer_hooks, "decide", decide)
    monkeypatch.setattr(router.offer_hooks, "held_by_idp", lambda _a: True)
    monkeypatch.setattr(router.offers, "require_request", require_request)
    monkeypatch.setattr(router.offers, "set_review", set_review)
    decision = SimpleNamespace(request_id=3)
    with pytest.raises(Exception):  # the ApiError the refusal becomes
        asyncio.run(router._decide(conn, _state(auth_config={}), decision, None))
    assert listed == [(7, reason)]


def test_a_decision_refused_for_another_reason_does_not_list_the_offer(monkeypatch):
    from provisa.core.repositories.data_product_offer import OfferRefused

    listed: list[Any] = []

    async def decide(*_a: Any, **_k: Any) -> dict:
        raise OfferRefused("role_not_found", "refused", status=409)

    async def set_review(*a: Any) -> None:
        listed.append(a)

    class _Tx:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *_exc: Any) -> bool:
            return False

    conn = SimpleNamespace(transaction=lambda: _Tx())
    monkeypatch.setattr(router.offer_hooks, "decide", decide)
    monkeypatch.setattr(router.offer_hooks, "held_by_idp", lambda _a: True)
    monkeypatch.setattr(router.offers, "set_review", set_review)
    with pytest.raises(Exception):
        asyncio.run(
            router._decide(conn, _state(auth_config={}), SimpleNamespace(request_id=3), None)
        )
    assert listed == []


def test_a_bid_names_the_products_owner_role_as_its_approver_with_no_hook():
    """REQ-2180 (amended 2026-11-11, A BID NAMES ITS CONSUMER AND AGREES TO THE TERMS): the
    request form names who approves it -- the product's owner role, where requests stay local."""
    assert router.approver_of({"owner_role": "data_owner"}, None) == {
        "via": "owner",
        "name": "data_owner",
    }
    assert router.approver_of({"owner_role": None}, None) == {"via": "owner", "name": None}


def test_a_bid_names_the_authority_its_hook_reaches_as_its_approver():
    from provisa.core.offer_hooks import HookTarget

    target = HookTarget(
        url="https://user:pw@access.example.com:8443/requests?token=x", secret="${HOOK_SECRET}"
    )
    # The host alone: the URL's credentials, path and query are never shown to a requester.
    assert router.approver_of({"owner_role": "data_owner"}, target) == {
        "via": "hook",
        "name": "access.example.com",
    }
