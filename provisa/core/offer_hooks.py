# Copyright (c) 2026 Kenneth Stott
# Canary: d47afbc8-7102-47ac-9886-ee75f102e846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Requests go through hooks (REQ-2180, REQUESTS GO THROUGH HOOKS).

Provisa defines access and enforces it on every query, whatever granted it; it does not decide
grants. A bid on an offer, and the cancel of an accepted bid, each only fire an OUTBOUND REQUEST
HOOK carrying who asks, the offer's terms, the contract versions, the reason and an id that makes
a repeat harmless (:func:`payload`). Whatever receives it decides, and the decision returns through
one INBOUND DECISION HOOK (:func:`decide`): the request id, accepted or declined, who decided and
when. It refuses a decision that does not match its request, or one from a decider who may not
grant the offer's role.

WHERE A REQUEST GOES. The org's ``offer_hook`` setting names a URL and the secret, as the operator
entered it and resolved at use like any credential, the request is signed with
(:data:`SIGNATURE_HEADER`) and an external decision is verified against. With no hook configured and no external identity provider, the
default hook writes the request into Provisa's own request queue (:data:`LOCAL`): the bid stays
pending there for the product's owner, and a cancel is applied at once as the matching removal.
Where an identity provider holds access (``auth.assignments_source: claims``) there is no local
default: a request with no hook to go to is refused.

WHAT A DECISION DOES. Where Provisa holds access, an accepted bid is applied as an ordinary admin
grant of exactly what was bid, attributed to the decider (provisa.core.offer_grant), and an
accepted cancel as the matching removal. Where an identity provider holds it, the decision only
updates the request's status: the grant arrives as claims once that system makes it.
"""

# Requirements: REQ-2180

from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from provisa.core import offer_grant
from provisa.core.assignments_source import CLAIMS
from provisa.core.repositories import data_product_offer as offers
from provisa.core.repositories.data_product_offer import OfferRefused

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The org setting (``org_settings``) naming the outbound hook: ``{url, secret}``.
SETTINGS_KEY = "offer_hook"
#: Where a request goes with no hook configured: the org's own request queue.
LOCAL = "local"
#: The header carrying ``sha256=<hex HMAC of the body>`` under the hook's secret, both ways.
SIGNATURE_HEADER = "X-Provisa-Signature"
#: The auth providers that sign no one in (provisa.core.assignments_source).
_NO_PROVIDER = (None, "", "none")

Outcome = Literal["accepted", "declined"]


@dataclass(frozen=True)
class HookTarget:
    """The org's outbound request hook: where requests are posted, and the secret they are
    signed with, as the operator entered it."""

    url: str
    secret: str


def target_of(settings: Mapping[str, Any]) -> HookTarget | None:
    """The hook the org's resolved settings name, or None when they name none. A hook missing
    its URL or its secret is refused by name."""
    block = settings.get(SETTINGS_KEY)
    if not block:
        return None
    return checked(
        HookTarget(url=str(block.get("url") or ""), secret=str(block.get("secret") or ""))
    )


def checked(target: HookTarget) -> HookTarget:
    """``target``, refused by name (ValueError) when its URL is not http(s) or it has no
    secret."""
    if not target.url.startswith(("https://", "http://")):
        raise ValueError(f"{SETTINGS_KEY}.url must be an http(s) URL, not {target.url!r}")
    if not target.secret:
        raise ValueError(f"{SETTINGS_KEY}.secret is required: the secret requests are signed with")
    return target


def held_by_idp(auth: Mapping[str, Any]) -> bool:
    """Whether an identity provider holds access: someone signs in, and the roles they hold are
    the ones their sign-in carries (``auth.assignments_source: claims``)."""
    return (
        auth.get("provider") not in _NO_PROVIDER
        and auth.get("assignments_source", CLAIMS) == CLAIMS
    )


def destination(target: HookTarget | None, auth: Mapping[str, Any]) -> str:
    """Where a request goes: the configured hook's URL, else the local queue -- refused where an
    identity provider holds access, since nothing in Provisa can decide for it."""
    if target is not None:
        return target.url
    if held_by_idp(auth):
        raise OfferRefused(
            "no_request_hook",
            f"access is held by the identity provider and no request hook is configured "
            f"({SETTINGS_KEY}); a request has nowhere to go",
            status=409,
        )
    return LOCAL


def new_request_id() -> str:
    """The id a request carries: a repeat of it is the same request."""
    return uuid.uuid4().hex


async def payload(
    conn: "Connection",
    *,
    request_id: str,
    requester: str,
    product_id: str,
    offer_row: Mapping[str, Any],
    reason: str,
    consumer_kind: str,
) -> dict[str, Any]:
    """The bid the outbound hook carries: who asks, the offer and its terms (role, data level,
    sample views, data quality results, profiles, duration, and the terms of use the requester
    agreed to), the version of each contract the offer gives, the reason (the bid's purpose), the
    kind of consumer it is for, and the request's id. Datasets and views are named as an
    entitlement list names them, as what accepting grants is."""
    contracts = await offers.contracts(conn, int(offer_row["id"]))
    names = await offers.sql_names(conn, [*contracts, *offer_row["sample_views"]])
    return {
        "id": request_id,
        "kind": "bid",
        "requester": requester,
        "product": product_id,
        "offer": {
            "name": offer_row["name"],
            "role": offer_row["role_id"],
            "data_level": offer_row["data_level"],
            "sample_views": [names[v] for v in offer_row["sample_views"]],
            "dq_results": offer_row["dq_results"],
            "profiles": offer_row["profiles"],
            "duration_days": offer_row["duration_days"],
            "purpose": offer_row["purpose"],
            "usage": offer_row["usage"],
            "limitations": offer_row["limitations"],
        },
        "contracts": {names[t]: c.version for t, c in sorted(contracts.items())},
        "reason": reason,
        "consumer_kind": consumer_kind,
    }


def cancel_payload(bid: Mapping[str, Any], *, request_id: str, reason: str) -> dict[str, Any]:
    """The cancel of ``bid`` (its stored request) as the outbound hook carries it: the bid's own
    requester, offer terms and contract versions, the cancel's reason and id, and the id of the
    request it cancels."""
    return {
        **bid["payload"],
        "id": request_id,
        "kind": "cancel",
        "reason": reason,
        "cancels": bid["request_id"],
    }


def sign(body: bytes, secret: str) -> str:
    """``body``'s signature under ``secret``, as :data:`SIGNATURE_HEADER` carries it."""
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verified(body: bytes, signature: str | None, secret: str) -> bool:
    """Whether ``signature`` is ``body``'s under ``secret``."""
    return signature is not None and hmac.compare_digest(signature, sign(body, secret))


async def resolved_secret(target: HookTarget) -> str:
    """The hook's secret as entered, resolved at use the way any credential is: itself, or what
    its reference names in the requesting org's vault."""
    from provisa.core.secrets_store import resolved_credential

    return await resolved_credential(target.secret)


class HookFailed(RuntimeError):
    """The outbound hook did not take the request."""


async def deliver(target: HookTarget, body: Mapping[str, Any]) -> None:
    """Post the request to the hook, signed, its id as the ``Idempotency-Key``. Anything but a
    2xx answer is :class:`HookFailed`."""
    import httpx

    raw = json.dumps(body, sort_keys=True).encode()
    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": str(body["id"]),
        SIGNATURE_HEADER: sign(raw, await resolved_secret(target)),
    }
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            answer = await client.post(target.url, content=raw, headers=headers)
    except httpx.HTTPError as exc:
        raise HookFailed(f"the request hook at {target.url} could not be reached: {exc}") from exc
    if not answer.is_success:
        raise HookFailed(
            f"the request hook at {target.url} answered {answer.status_code}: {answer.text[:200]}"
        )


# -- the decision hook -----------------------------------------------------------------------------


@dataclass(frozen=True)
class Decision:
    """A decision as the inbound hook takes it: the request it decides, accepted or declined,
    who decided and when, and a decline's reason."""

    request_id: str
    outcome: Outcome
    decider: str
    decided_at: datetime
    reason: str | None = None


#: Why the decider may not grant a role, or None when they may.
GrantRefusal = Callable[[str], Awaitable[str | None]]


@dataclass(frozen=True)
class Grants:
    """What applying an accepted decision needs where Provisa holds access: the product's domain
    and the role defaults of the offer's role (``role_defaults(state.roles[role])``)."""

    domain_id: str
    role_defaults: Mapping[str, Any]


async def decide(
    conn: "Connection",
    decision: Decision,
    *,
    may_grant: GrantRefusal,
    grants: Callable[["Connection", dict], Awaitable[Grants]] | None,
) -> dict:
    """Apply ``decision`` to its request; the request as stored after it. ``grants`` is None
    where an identity provider holds access: the decision then only updates the request's status.

    Refused by name: no such request; one already decided otherwise (the same decision again is
    a harmless repeat, answered with the request as it stands); a decider ``may_grant`` refuses
    for the offer's role; an accepted bid whose offer has gone or changed since; a decline with
    no reason. The caller holds the transaction."""
    held = await offers.require_request(conn, decision.request_id)
    if held["status"] == decision.outcome and held["decided_by"] == decision.decider:
        return held
    if held["status"] != "pending":
        raise OfferRefused(
            "request_decided",
            f"request {decision.request_id!r} is {held['status']}, not pending",
            status=409,
            status_now=held["status"],
        )
    role_id = str(held["terms"]["role_id"])
    refusal = await may_grant(role_id)
    if refusal is not None:
        raise OfferRefused("role_not_grantable", refusal, status=403, role=role_id)
    if decision.outcome == "declined":
        reason = (decision.reason or "").strip()
        if not reason:
            raise OfferRefused("reason_required", "declining a request states its reason")
        return await offers.decided(
            conn, held["id"], "declined", by=decision.decider, at=decision.decided_at, reason=reason
        )
    if held["kind"] == "cancel":
        return await apply_cancel(
            conn, held, by=decision.decider, at=decision.decided_at, provisa_held=grants is not None
        )
    offer_row = await offers.offer_as_bid(conn, held)
    granted = None
    if grants is not None:
        offer = offers.as_offer(offer_row)
        held_grants = await grants(conn, held)
        granted = await offer_grant.grant(
            conn,
            principal=held["requester"],
            offer=offer,
            domain_id=held_grants.domain_id,
            identifiers=await offer_grant.entries(conn, held["product_id"], offer),
            role_defaults=held_grants.role_defaults,
            set_by=decision.decider,
            reason=f"accepted request {held['request_id']} for offer {offer.name!r} of data "
            f"product {held['product_id']!r}: {held['reason']}",
            now=decision.decided_at,
        )
    return await offers.decided(
        conn, held["id"], "accepted", by=decision.decider, at=decision.decided_at, granted=granted
    )


async def apply_cancel(
    conn: "Connection", cancel: dict, *, by: str, at: datetime | None, provisa_held: bool
) -> dict:
    """Apply an accepted cancel: a pending bid is withdrawn; an accepted one is cancelled, and
    where Provisa holds access what it granted is removed (the matching removal). The cancel as
    stored."""
    accepted = await offers.get_bid(conn, int(cancel["cancels"]))
    assert accepted is not None, "a cancel names the bid it cancels, which cascades with it"
    if accepted["status"] == "pending":
        await offers.decided(conn, accepted["id"], "withdrawn", by=by, at=at)
        return await offers.decided(conn, cancel["id"], "accepted", by=by, at=at)
    if accepted["status"] != "accepted":
        raise OfferRefused(
            "bid_not_cancellable",
            f"bid {accepted['id']} is {accepted['status']}; there is nothing to cancel",
            status=409,
            status_now=accepted["status"],
        )
    if provisa_held and accepted["granted"] is not None:
        await offer_grant.revoke(
            conn,
            principal=accepted["requester"],
            granted=accepted["granted"],
            set_by=by,
            reason=f"cancelled request {accepted['request_id']}: {cancel['reason']}",
        )
    await offers.decided(conn, accepted["id"], "cancelled", by=by, at=at)
    return await offers.decided(conn, cancel["id"], "accepted", by=by, at=at)
