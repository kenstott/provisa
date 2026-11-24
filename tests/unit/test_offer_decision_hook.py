# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e6f12-8a4d-4c71-b5e0-2d7f1a9c4e68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (amended 2026-11-11, REQUESTS GO THROUGH HOOKS): the one inbound decision hook takes
the request id, accepted or declined, who decided and when. An accepted decision grants and is
recorded with who and when; a declined one records and grants nothing. A decision naming no
request, one that does not match its request, a second decision of a decided request and one
from a decider who may not grant the role are each refused by name and change nothing. Against a
real tenant DB: the core hook (:func:`provisa.core.offer_hooks.decide`) and its HTTP front door
(``POST /admin/data-products/decisions``)."""

# Requirements: REQ-2180

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api.admin import data_product_offers_router as router
from provisa.api.errors import ApiError
from provisa.core import offer_grant, offer_hooks
from provisa.core.repositories import data_product_offer as offers
from provisa.core.repositories.data_product_offer import OfferRefused
from provisa.entitlements import store
from tests.unit.test_data_product_offers import (
    _NOW,
    _anyone,
    _assignment,
    _bid,
    _cancel,
    _decision,
    _default,
    _provisa_held,
    _store,
)


async def _unchanged(conn, held: dict) -> None:
    """The request still pending, undecided, and nothing granted to its requester."""
    row = await offers.get_bid(conn, held["id"])
    assert row is not None
    assert (row["status"], row["decided_by"], row["decided_at"], row["granted"]) == (
        "pending",
        None,
        None,
        None,
    )
    assert await _assignment(conn) == []
    assert "u1" not in await store.load_held(conn)


# -- the core hook ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_accepted_decision_grants_and_records_who_decided_and_when(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        held = await _bid(conn, await _default(conn))
        accepted = await offer_hooks.decide(
            conn, _decision(held, decider="o9"), may_grant=_anyone, grants=_provisa_held
        )
        stored = await offers.get_bid(conn, held["id"])
        assert stored is not None
        assert (stored["status"], stored["decided_by"]) == ("accepted", "o9")
        assert offer_grant._aware(stored["decided_at"]) == _NOW
        assert stored["granted"] == accepted["granted"]
        assert stored["granted"]["added"]
        assert await _assignment(conn) == [("viewer", None)]
        (listed,) = [x for x in (await store.load_held(conn))["u1"].lists if x.kind == "tables"]
        assert listed.set_by == "o9"


@pytest.mark.asyncio
async def test_a_declined_decision_records_who_when_and_why_and_grants_nothing(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        held = await _bid(conn, await _default(conn))
        await offer_hooks.decide(
            conn,
            _decision(held, "declined", "too broad", decider="o9"),
            may_grant=_anyone,
            grants=_provisa_held,
        )
        stored = await offers.get_bid(conn, held["id"])
        assert stored is not None
        assert (stored["status"], stored["decided_by"], stored["decline_reason"]) == (
            "declined",
            "o9",
            "too broad",
        )
        assert offer_grant._aware(stored["decided_at"]) == _NOW
        assert stored["granted"] is None
        assert await _assignment(conn) == []
        assert "u1" not in await store.load_held(conn)


@pytest.mark.asyncio
async def test_a_decision_naming_an_unknown_request_is_refused_and_changes_nothing(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        held = await _bid(conn, await _default(conn))
        with pytest.raises(OfferRefused) as refused:
            await offer_hooks.decide(
                conn,
                offer_hooks.Decision("no-such-request", "accepted", "o1", _NOW),
                may_grant=_anyone,
                grants=_provisa_held,
            )
        assert (refused.value.code, refused.value.status) == ("request_not_found", 404)
        assert refused.value.params == {"request": "no-such-request"}
        await _unchanged(conn, held)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "reason", "decider"),
    [
        ("declined", "too broad", "o1"),  # the other outcome
        ("accepted", None, "o2"),  # the same outcome, another decider
    ],
)
async def test_a_second_decision_of_a_decided_request_is_refused_and_changes_nothing(
    control_plane, outcome, reason, decider
):
    async with _store(control_plane) as db, db.acquire() as conn:
        held = await _bid(conn, await _default(conn))
        await offer_hooks.decide(conn, _decision(held), may_grant=_anyone, grants=_provisa_held)
        before = await offers.get_bid(conn, held["id"])
        held_before = await store.load_held(conn)
        with pytest.raises(OfferRefused) as refused:
            await offer_hooks.decide(
                conn,
                _decision(held, outcome, reason, decider=decider, at=_NOW + timedelta(hours=1)),
                may_grant=_anyone,
                grants=_provisa_held,
            )
        assert (refused.value.code, refused.value.status) == ("request_decided", 409)
        assert refused.value.params == {"status_now": "accepted"}
        assert await offers.get_bid(conn, held["id"]) == before
        assert await store.load_held(conn) == held_before
        assert await _assignment(conn) == [("viewer", None)]


@pytest.mark.asyncio
async def test_the_same_decision_sent_again_is_answered_as_it_stands_and_grants_once(
    control_plane,
):
    async with _store(control_plane) as db, db.acquire() as conn:
        held = await _bid(conn, await _default(conn))
        first = await offer_hooks.decide(
            conn, _decision(held), may_grant=_anyone, grants=_provisa_held
        )
        held_before = await store.load_held(conn)
        again = await offer_hooks.decide(
            conn,
            _decision(held, at=_NOW + timedelta(hours=1)),
            may_grant=_anyone,
            grants=_provisa_held,
        )
        assert again == first
        assert offer_grant._aware(again["decided_at"]) == _NOW
        assert await store.load_held(conn) == held_before
        assert await _assignment(conn) == [("viewer", None)]


@pytest.mark.asyncio
@pytest.mark.parametrize(("outcome", "reason"), [("accepted", None), ("declined", "too broad")])
async def test_a_decider_who_may_not_grant_the_role_is_refused_and_changes_nothing(
    control_plane, outcome, reason
):
    async with _store(control_plane) as db, db.acquire() as conn:
        held = await _bid(conn, await _default(conn))

        async def cannot(role: str) -> str:
            return f"role {role!r} is outside the roles you may grant"

        with pytest.raises(OfferRefused) as refused:
            await offer_hooks.decide(
                conn, _decision(held, outcome, reason), may_grant=cannot, grants=_provisa_held
            )
        assert (refused.value.code, refused.value.status) == ("role_not_grantable", 403)
        assert refused.value.params == {"role": "viewer"}
        await _unchanged(conn, held)


# -- the HTTP front door ---------------------------------------------------------------------------


def _request(user_id: str = "anonymous", role: str = "") -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(identity=SimpleNamespace(user_id=user_id), role=role)
    )


def _app_state(db) -> Any:
    """The runtime as the router reads it; access held by an identity provider, so an accepted
    decision only updates the request's status."""
    return SimpleNamespace(
        model_db=db,
        roles={"viewer": {"capabilities": []}, "owner": {"capabilities": []}},
        role_chains={"owner": ["owner"], "viewer": ["viewer"]},
        meta_roles={},
        auth_config={"assignments_source": "claims"},
    )


@pytest.fixture
def front_door(monkeypatch):
    """Point the router at ``db``; the capability gate and the runtime reload are not the
    subject here."""

    def wire(db) -> None:
        monkeypatch.setattr(router, "_state", lambda: _app_state(db))
        monkeypatch.setattr(router, "require_capability_request", lambda *_a: None)

        async def reload(*_a: Any) -> None:
            return None

        monkeypatch.setattr(router, "_reload_held", reload)

    return wire


async def _post(body: dict, request: Any) -> dict:
    return await router.decision_hook(router.DecisionBody(**body), request)


@pytest.mark.asyncio
async def test_a_decision_naming_a_decider_other_than_the_caller_is_refused(
    control_plane, front_door
):
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        front_door(db)
        with pytest.raises(ApiError) as refused:
            await _post(
                {"request_id": held["request_id"], "outcome": "accepted", "decider": "mallory"},
                _request("o1", "owner"),
            )
        assert refused.value.status_code == 403
        assert refused.value.code == "data_product.decision.not_the_decider"
        async with db.acquire() as conn:
            await _unchanged(conn, held)


@pytest.mark.asyncio
async def test_a_decision_naming_an_unknown_request_is_refused_at_the_front_door(
    control_plane, front_door
):
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        front_door(db)
        with pytest.raises(ApiError) as refused:
            await _post({"request_id": "no-such-request", "outcome": "accepted"}, _request())
        assert refused.value.status_code == 404
        assert refused.value.code == "data_product.offer.request_not_found"
        async with db.acquire() as conn:
            await _unchanged(conn, held)


@pytest.mark.asyncio
async def test_a_cancel_sent_to_a_hook_is_decided_through_the_front_door(control_plane, front_door):
    """A cancel is a request of its own: its decision returns through the same decision hook."""
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
            accepted = await offer_hooks.decide(
                conn, _decision(held), may_grant=_anyone, grants=None
            )
            cancel = await _cancel(conn, accepted, hook="https://grants.example/requests")
        front_door(db)
        answer = await _post(
            {"request_id": cancel["request_id"], "outcome": "accepted"}, _request()
        )
        assert (answer["id"], answer["status"]) == (cancel["id"], "accepted")
        async with db.acquire() as conn:
            row = await offers.get_bid(conn, held["id"])
        assert row is not None
        assert row["status"] == "cancelled"
