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

import json
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


class _Request:
    """A request as the router reads it: who the middleware bound, its headers and its body."""

    def __init__(
        self,
        user_id: str = "anonymous",
        role: str = "",
        *,
        org_id: str | None = None,
        headers: dict[str, str] | None = None,
        raw: bytes = b"",
    ) -> None:
        self.state = SimpleNamespace(
            identity=SimpleNamespace(user_id=user_id), role=role, active_org_id=org_id
        )
        self.headers = headers or {}
        self._raw = raw

    async def body(self) -> bytes:
        return self._raw


def _request(user_id: str = "anonymous", role: str = "") -> Any:
    return _Request(user_id, role)


def _app_state(db) -> Any:
    """The runtime as the router reads it; access held by an identity provider, so an accepted
    decision only updates the request's status."""
    return SimpleNamespace(
        model_db=db,
        roles={"viewer": {"capabilities": []}, "owner": {"capabilities": []}},
        role_chains={"owner": ["owner"], "viewer": ["viewer"]},
        meta_roles={},
        auth_config={"provider": "oidc", "assignments_source": "claims"},
        idp_entitlements={},
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


# -- the signed external caller (amended 2026-11-24) -----------------------------------------------

_SECRET = "hook-s3cret"
_HOOK = offer_hooks.HookTarget(url="https://grants.example/requests", secret=_SECRET)
#: Who may grant the offer's role, as principal_refusal answers for an external decider.
_GRANTERS = frozenset({"o9"})


@pytest.fixture
def external(monkeypatch, front_door):
    """The front door with the org's request hook configured, and the decider's right to grant
    read through principal_refusal (its own rule is tested in test_data_product_offers_router);
    returns the (principal, role) pairs it was asked."""
    asked: list[tuple[str, str]] = []

    def wire(db, target: offer_hooks.HookTarget | None = _HOOK) -> list[tuple[str, str]]:
        front_door(db)

        async def hook_target(_state: Any) -> offer_hooks.HookTarget | None:
            return target

        async def principal_refusal(_conn, _state, principal: str, role_id: str) -> str | None:
            asked.append((principal, role_id))
            if principal in _GRANTERS:
                return None
            return f"role {role_id!r} is outside the roles you may grant"

        monkeypatch.setattr(router, "_hook_target", hook_target)
        monkeypatch.setattr(router, "principal_refusal", principal_refusal)
        return asked

    return wire


def _decision_body(held: dict, **more: Any) -> dict:
    body = {
        "request_id": held["request_id"],
        "outcome": "accepted",
        "decider": "o9",
        "decided_at": _NOW.isoformat(),
    }
    body.update(more)
    return {k: v for k, v in body.items() if v is not None}


async def _post_signed(
    body: dict, *, secret: str = _SECRET, signature: str | None = None, raw: bytes | None = None
) -> dict:
    """POST ``body`` as an external caller does: no credential (the org's public visitor), the
    body signed under ``secret`` as the outbound hook signs a request."""
    sent = json.dumps(body).encode() if raw is None else raw
    signed = offer_hooks.sign(sent, secret) if signature is None else signature
    request = _Request(
        "public:acme",
        "public",
        org_id="acme",
        headers={offer_hooks.SIGNATURE_HEADER: signed},
        raw=sent,
    )
    return await router.decision_hook(router.DecisionBody.model_validate_json(sent), request)


@pytest.mark.asyncio
async def test_a_signed_decision_from_a_decider_who_may_grant_is_applied(control_plane, external):
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        asked = external(db)
        answer = await _post_signed(_decision_body(held))
        assert (answer["id"], answer["status"]) == (held["id"], "accepted")
        assert asked == [("o9", "viewer")]
        async with db.acquire() as conn:
            stored = await offers.get_bid(conn, held["id"])
        assert stored is not None
        assert (stored["status"], stored["decided_by"]) == ("accepted", "o9")
        assert offer_grant._aware(stored["decided_at"]) == _NOW


@pytest.mark.asyncio
async def test_a_signed_decline_records_the_deciders_reason(control_plane, external):
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        external(db)
        await _post_signed(_decision_body(held, outcome="declined", reason="too broad"))
        async with db.acquire() as conn:
            stored = await offers.get_bid(conn, held["id"])
        assert stored is not None
        assert (stored["status"], stored["decided_by"], stored["decline_reason"]) == (
            "declined",
            "o9",
            "too broad",
        )


@pytest.mark.asyncio
async def test_an_unsigned_decision_from_no_signed_in_caller_is_refused(control_plane, external):
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        asked = external(db)
        sent = json.dumps(_decision_body(held)).encode()
        visitor = _Request("public:acme", "public", org_id="acme", raw=sent)
        with pytest.raises(ApiError) as refused:
            await router.decision_hook(router.DecisionBody.model_validate_json(sent), visitor)
        assert refused.value.status_code == 401
        assert refused.value.code == "data_product.decision.unsigned"
        assert asked == []
        async with db.acquire() as conn:
            await _unchanged(conn, held)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forge",
    ["another_secret", "another_body", "empty", "bare_hex", "uppercase_prefix"],
)
async def test_a_mis_signed_decision_is_refused_and_changes_nothing(control_plane, external, forge):
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        asked = external(db)
        body = _decision_body(held)
        sent = json.dumps(body).encode()
        good = offer_hooks.sign(sent, _SECRET)
        signature = {
            "another_secret": offer_hooks.sign(sent, "not-the-secret"),
            # Signed for a decline, sent as an accept: the signature covers the body it came with.
            "another_body": offer_hooks.sign(
                json.dumps({**body, "outcome": "declined"}).encode(), _SECRET
            ),
            "empty": "",
            "bare_hex": good.removeprefix("sha256="),
            "uppercase_prefix": "SHA256=" + good.removeprefix("sha256="),
        }[forge]
        with pytest.raises(ApiError) as refused:
            await _post_signed(body, signature=signature, raw=sent)
        assert refused.value.status_code == 401
        assert refused.value.code == "data_product.decision.bad_signature"
        assert asked == []
        async with db.acquire() as conn:
            await _unchanged(conn, held)


@pytest.mark.asyncio
async def test_a_signed_decision_with_no_hook_configured_is_refused(control_plane, external):
    """With no request hook there is no secret to sign with: no external decision verifies."""
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        external(db, target=None)
        with pytest.raises(ApiError) as refused:
            await _post_signed(_decision_body(held))
        assert refused.value.code == "data_product.decision.bad_signature"
        async with db.acquire() as conn:
            await _unchanged(conn, held)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["decider", "decided_at"])
async def test_a_signed_decision_names_who_decided_and_when(control_plane, external, missing):
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        external(db)
        with pytest.raises(ApiError) as refused:
            await _post_signed(_decision_body(held, **{missing: None}))
        assert refused.value.status_code == 422
        assert refused.value.code == "data_product.decision.incomplete"
        async with db.acquire() as conn:
            await _unchanged(conn, held)


@pytest.mark.asyncio
@pytest.mark.parametrize(("outcome", "reason"), [("accepted", None), ("declined", "too broad")])
async def test_a_signed_decision_from_a_decider_who_may_not_grant_is_refused(
    control_plane, external, outcome, reason
):
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        asked = external(db)
        with pytest.raises(ApiError) as refused:
            await _post_signed(
                _decision_body(held, outcome=outcome, reason=reason, decider="mallory")
            )
        assert refused.value.status_code == 403
        assert refused.value.code == "data_product.offer.role_not_grantable"
        assert asked == [("mallory", "viewer")]
        async with db.acquire() as conn:
            await _unchanged(conn, held)


@pytest.mark.asyncio
async def test_a_replayed_signed_decision_is_answered_as_it_stands_and_changes_nothing(
    control_plane, external
):
    """The scheme signs the body, decided_at in it: a replay is the same decision sent again,
    answered with the request as it stands, never decided anew."""
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        external(db)
        body = _decision_body(held)
        first = await _post_signed(body)
        async with db.acquire() as conn:
            before = await offers.get_bid(conn, held["id"])
        again = await _post_signed(body)
        assert again == first
        async with db.acquire() as conn:
            assert await offers.get_bid(conn, held["id"]) == before


@pytest.mark.asyncio
async def test_a_replayed_signed_accept_after_a_decline_is_refused(control_plane, external):
    """A captured decision replayed against a request decided otherwise changes nothing."""
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        external(db)
        await _post_signed(_decision_body(held, outcome="declined", reason="too broad"))
        async with db.acquire() as conn:
            before = await offers.get_bid(conn, held["id"])
        with pytest.raises(ApiError) as refused:
            await _post_signed(_decision_body(held))
        assert refused.value.code == "data_product.offer.request_decided"
        async with db.acquire() as conn:
            assert await offers.get_bid(conn, held["id"]) == before


@pytest.mark.asyncio
async def test_a_signed_in_owner_still_decides_unsigned_as_themselves(
    control_plane, front_door, monkeypatch
):
    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            held = await _bid(conn, await _default(conn))
        front_door(db)
        monkeypatch.setattr(router, "may_grant", lambda *_a: None)
        answer = await _post(
            {"request_id": held["request_id"], "outcome": "accepted"}, _request("o1", "owner")
        )
        assert answer["status"] == "accepted"
        async with db.acquire() as conn:
            stored = await offers.get_bid(conn, held["id"])
        assert stored is not None
        assert stored["decided_by"] == "o1"
