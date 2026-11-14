# Copyright (c) 2026 Kenneth Stott
# Canary: 8a3f6c25-1e94-4d7b-b2a0-9c5e8d4f2a17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product's offers, their contracts, and the bids for them, over HTTP (REQ-2180).

* Offers are the product's: listing them asks what reading the product asks
  (``data_product_read``); adding, changing or removing one asks read-write on the product in its
  domain (``data_product_rw``), and its row rules (REQ-2188) hold the write. An offer's role is
  one its owner may grant (:func:`may_grant`): the role picker lists only those, and saving any
  other is refused by name.
* Each offer's contracts are derived when it is saved: one per dataset, the offer's terms applied
  to it, as the offer's role reads it (provisa.api.offer_derivation).
* A signed-in requester bids for an offer, stating why, and the bid fires the outbound request
  hook (provisa.core.offer_hooks); so does their cancel of it. Whatever receives the request
  decides, and the decision returns through the one inbound decision hook (``POST
  /decisions``). With no hook configured the request lands in the product's own queue, and its
  accept and decline here are that queue's client of the decision hook: the product's owner role
  decides, and must still be able to grant the offer's role.
* An offer may be open only when its data is already public (provisa.core.offer_public): an open
  offer is no request at all, and the API reports ``query_now`` for it. When its data stops being
  public, it is listed for review.
"""

# Requirements: REQ-2180, REQ-2219, REQ-2188

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, Literal
from urllib.parse import urlsplit

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin.capabilities import (
    require_capability_request,
    require_right_in_domains_request,
)
from provisa.api.errors import ApiError
from provisa.api.offer_derivation import derive, seen_columns
from provisa.core import offer_grant, offer_hooks, offer_public
from provisa.core.models import ConsumerKind, DataProductOffer
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import data_product_offer as offers
from provisa.core.repositories.data_product_offer import OfferRefused
from provisa.entitlements.runtime import entitlements_scope, reaches_role, role_defaults
from provisa.security.rights import Capability

router = APIRouter(prefix="/admin/data-products", tags=["admin"])

_ANONYMOUS = "anonymous"
#: The rights that let a person grant a role, within the roles their ``roles`` list reaches.
GRANTING = frozenset({Capability.ASSIGN_ENTITLEMENTS.value, Capability.USER_MANAGEMENT.value})
#: Why an offer is listed for review.
REVIEW_PROFILER_GONE = "a dataset of this synthetic offer has no profiler"
REVIEW_FAKE_GONE = "a column of a dataset of this synthetic offer has no fake"


class BidBody(BaseModel):
    """A bid: its purpose (``reason``), the kind of consumer it is for, and whether its requester
    agrees to the offer's terms of use (amended 2026-11-11, A BID NAMES ITS CONSUMER AND AGREES TO
    THE TERMS)."""

    reason: str
    consumer_kind: ConsumerKind
    agreed: bool


class DeclineBody(BaseModel):
    reason: str


class CancelBody(BaseModel):
    reason: str


def _state() -> Any:
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "data_product.database_not_connected", "Database not connected")
    return state


def _caller(request: Request) -> str | None:
    """The signed-in caller's user id; None in dev/no-auth mode, where every gate is open."""
    user_id = getattr(getattr(request.state, "identity", None), "user_id", _ANONYMOUS)
    return None if user_id == _ANONYMOUS else str(user_id)


def _refused(refusal: OfferRefused) -> ApiError:
    return ApiError(
        refusal.status, f"data_product.offer.{refusal.code}", str(refusal), **refusal.params
    )


def _role_grant_refusal(state: Any, role_id: str, granter: frozenset[str]) -> str | None:
    """Why the bound person, granting with ``granter`` (their capabilities), may not grant
    ``role_id``, or None: the role is defined, their ``roles`` list reaches it through a role
    they act as that holds a granting right, and it carries no platform right they lack."""
    from provisa.security.rights import PlatformRoleGrantError, check_role_grant

    role = state.roles.get(role_id)
    if role is None:
        return f"role {role_id!r} is not defined"
    if not reaches_role(state, role_id, GRANTING):
        return f"role {role_id!r} is outside the roles you may grant"
    try:
        check_role_grant(role_id, role.get("capabilities"), granter)
    except PlatformRoleGrantError:
        return f"role {role_id!r} carries platform rights, which you do not hold"
    return None


def may_grant(request: Request, state: Any, role_id: str) -> str | None:
    """Why the caller may not grant ``role_id``, or None when they may. In dev/no-auth mode
    every gate is open."""
    from provisa.api.admin._platform_guard import granter_capabilities

    if _caller(request) is None:
        return None if role_id in state.roles else f"role {role_id!r} is not defined"
    return _role_grant_refusal(state, role_id, granter_capabilities(request))


async def principal_refusal(conn: Any, state: Any, principal: str, role_id: str) -> str | None:
    """Why ``principal`` may not grant ``role_id``, or None: the rule of :func:`may_grant`, asked
    of them as they hold their roles now -- for a decider who is not the caller (the inbound
    decision hook's external caller)."""
    from provisa.entitlements import roles as held_roles
    from provisa.security.rights import capabilities_for_claims

    idp = state.idp_entitlements.get(principal, {})
    held = await held_roles.held_roles(conn, principal, idp, state.auth_config or {}, state.roles)
    acting = [r.role_id for r in held]
    identity = SimpleNamespace(
        user_id=principal,
        raw_claims={
            "entitlements": {r: {k: list(v) for k, v in ks.items()} for r, ks in idp.items()}
        },
        entitlements_claim="entitlements",
        acting_roles=acting,
    )
    with entitlements_scope(identity):
        return _role_grant_refusal(
            state, role_id, frozenset(capabilities_for_claims(acting, state.roles))
        )


def _is_owner(request: Request, state: Any, product: dict) -> bool:
    """Whether the caller acts as the product's owner role, or a role beneath it. In dev/no-auth
    mode every gate is open."""
    if _caller(request) is None:
        return True
    owner = product.get("owner_role")
    if not owner:
        return False
    acting = str(getattr(request.state, "role", "") or "")
    members = state.meta_roles.get(acting) or [acting]
    return any(owner in state.role_chains.get(m, [m]) for m in members)


async def _product(conn: Any, product_id: str) -> dict:
    product = await data_product_repo.get(conn, product_id)
    if product is None:
        raise ApiError(
            404,
            "data_product.not_found",
            f"data product {product_id!r} not found",
            data_product=product_id,
        )
    return product


# -- contracts -------------------------------------------------------------------------------------


async def _not_public(conn: Any, state: Any, product_id: str, role_id: str) -> list[str]:
    """What keeps the product's data, read through ``role_id``, from being public; empty when
    it is public (provisa.core.offer_public)."""
    datasets = await offers.dataset_ids(conn, product_id)
    names = await offers.sql_names(conn, datasets)
    built = {t["id"]: t for t in state.tables}
    unread = [
        names[d]
        for d in datasets
        if d not in built or not any(c.read for c in seen_columns(state, role_id, built[d]))
    ]
    role = state.roles.get(role_id)
    return offer_public.not_public(
        role_id=role_id,
        role_chains=state.role_chains,
        roles=state.roles,
        role_defaults={} if role is None else role_defaults(role),
        held=state.held_entitlements,
        idp=state.idp_entitlements,
        datasets=[names[d] for d in datasets],
        unread=unread,
    )


async def _query_now(conn: Any, state: Any, offer_row: dict) -> bool:
    """Whether the offer is open and its data still public; an open offer whose data stopped
    being public is listed for review."""
    if not offer_row["open"]:
        return False
    if await _not_public(conn, state, offer_row["product_id"], offer_row["role_id"]):
        if offer_row.get("review") != offer_public.REVIEW_NOT_PUBLIC:
            await offers.set_review(conn, offer_row["id"], offer_public.REVIEW_NOT_PUBLIC)
            offer_row["review"] = offer_public.REVIEW_NOT_PUBLIC
        return False
    return offer_row.get("review") is None


async def _offer_view(
    conn: Any, request: Request, offer_row: dict, *, now: datetime
) -> dict[str, Any]:
    """An offer as listed: its terms, its contracts (one per dataset), the extension accepting it
    would make for the caller, and whether it is open public data to query now."""
    query_now = await _query_now(conn, _state(), offer_row)
    contracts = await offers.contracts(conn, offer_row["id"])
    names = await offers.table_names(conn, contracts)
    caller = _caller(request)
    extension = (
        None
        if caller is None
        else await offer_grant.extension(conn, caller, offers.as_offer(offer_row), now)
    )
    return {
        **{k: _json(v) for k, v in offer_row.items()},
        "contracts": [
            {
                "table_id": table_id,
                "table": names.get(table_id, str(table_id)),
                "contract": contract.model_dump(mode="json"),
            }
            for table_id, contract in sorted(contracts.items())
        ],
        "summary": {"extension": None if extension is None else extension.statement()},
        "query_now": query_now,
    }


def _json(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


# -- offers ----------------------------------------------------------------------------------------


@router.get("/{product_id}/offers")
async def list_offers(product_id: str, request: Request) -> dict:
    """The product's offers, each with its contracts and what accepting it would extend."""
    require_capability_request(request, "data_product_read")
    require_active_org_id(request)
    state = _state()
    now = datetime.now(UTC)
    async with state.model_db.acquire() as conn:
        await _product(conn, product_id)
        rows = await offers.list_offers(conn, product_id)
        return {"offers": [await _offer_view(conn, request, r, now=now) for r in rows]}


@router.get("/{product_id}/grantable-roles")
async def grantable_roles(product_id: str, request: Request) -> list[str]:
    """The roles the caller may put in an offer of the product: those they may grant."""
    state = _state()
    async with state.model_db.acquire() as conn:
        product = await _product(conn, product_id)
    require_right_in_domains_request(request, "data_product_rw", {product["domain_id"]})
    return sorted(r for r in state.roles if may_grant(request, state, r) is None)


async def _save(
    request: Request, product_id: str, body: DataProductOffer, offer_id: int | None
) -> dict:
    state = _state()
    async with state.model_db.acquire() as conn:
        product = await _product(conn, product_id)
        require_right_in_domains_request(request, "data_product_rw", {product["domain_id"]})
        refusal = may_grant(request, state, body.role_id)
        if refusal is not None:
            raise ApiError(403, "data_product.offer.role_not_grantable", refusal, role=body.role_id)
        if body.open:
            found = await _not_public(conn, state, product_id, body.role_id)
            if found:
                raise ApiError(
                    422,
                    "data_product.offer.not_public",
                    "an offer can be open only when its data is already public: "
                    + "; ".join(found),
                    reasons="; ".join(found),
                )
        try:
            async with conn.transaction():
                row = await offers.save(
                    conn, product_id, body, offer_id=offer_id, by=_caller(request)
                )
                await derive(conn, state, product_id, row)
        except OfferRefused as refused:
            raise _refused(refused) from refused
        return await _offer_view(conn, request, row, now=datetime.now(UTC))


@router.post("/{product_id}/offers")
async def add_offer(product_id: str, body: DataProductOffer, request: Request) -> dict:
    """Add an offer to the product, deriving its contracts."""
    return await _save(request, product_id, body, None)


@router.put("/{product_id}/offers/{offer_id}")
async def change_offer(
    product_id: str, offer_id: int, body: DataProductOffer, request: Request
) -> dict:
    """Change the product's offer: its contracts take their next versions where their terms
    changed, a column removed or masked being a breaking change."""
    return await _save(request, product_id, body, offer_id)


@router.delete("/{product_id}/offers/{offer_id}")
async def remove_offer(product_id: str, offer_id: int, request: Request) -> dict:
    """Remove the product's offer and its contracts; a pending bid for it is declined."""
    state = _state()
    async with state.model_db.acquire() as conn:
        product = await _product(conn, product_id)
        require_right_in_domains_request(request, "data_product_rw", {product["domain_id"]})
        try:
            await offers.delete_offer(conn, product_id, offer_id)
        except OfferRefused as refused:
            raise _refused(refused) from refused
    return {"deleted": offer_id}


# -- bids ------------------------------------------------------------------------------------------


def _bid_view(row: dict) -> dict:
    return {k: _json(v) for k, v in row.items()}


def _requester(request: Request) -> str:
    caller = _caller(request)
    if caller is None:
        raise ApiError(
            401, "data_product.bid.sign_in_required", "a bid is made by a signed-in requester"
        )
    return caller


async def _hook_target(state: Any) -> offer_hooks.HookTarget | None:
    """The org's outbound request hook, or None when it configures none."""
    from provisa.core.org_settings import resolve_org_config

    try:
        return offer_hooks.target_of(await resolve_org_config(state.model_db))
    except ValueError as exc:
        raise ApiError(409, "data_product.offer.hook_misconfigured", str(exc)) from exc


async def _destination(state: Any) -> tuple[str, offer_hooks.HookTarget | None]:
    """Where a request goes, and the hook it is posted to (None: the local queue)."""
    target = await _hook_target(state)
    return offer_hooks.destination(target, state.auth_config or {}), target


def approver_of(product: dict, target: offer_hooks.HookTarget | None) -> dict[str, str | None]:
    """Who approves a request for ``product`` (amended 2026-11-11, A BID NAMES ITS CONSUMER AND
    AGREES TO THE TERMS): the authority the org's request hook reaches, named by its host alone --
    a requester never reads the hook's credentials, path or query -- else the product's owner
    role, whose queue holds the request (None: the product names no owner role)."""
    if target is not None:
        return {"via": "hook", "name": urlsplit(target.url).hostname}
    return {"via": "owner", "name": product.get("owner_role")}


@router.get("/{product_id}/approver")
async def approver(product_id: str, request: Request) -> dict[str, str | None]:
    """Who a request for the product goes to for approval, as its request form names it; refused
    where an identity provider holds access and no request hook is configured, as a bid is."""
    require_capability_request(request, "data_product_read")
    state = _state()
    async with state.model_db.acquire() as conn:
        product = await _product(conn, product_id)
    try:
        _, target = await _destination(state)
    except OfferRefused as refused:
        raise _refused(refused) from refused
    return approver_of(product, target)


async def _fire(target: offer_hooks.HookTarget | None, body: dict) -> None:
    """Post the request to the org's hook; the local queue holds it already."""
    if target is None:
        return
    try:
        await offer_hooks.deliver(target, body)
    except offer_hooks.HookFailed as failed:
        raise ApiError(502, "data_product.offer.hook_failed", str(failed)) from failed


async def _reload_held(conn: Any, state: Any) -> None:
    """What the runtime holds of the entitlement store, after a grant or removal wrote to it."""
    from provisa.entitlements import store

    state.held_entitlements = await store.load_held(conn)


@router.post("/{product_id}/offers/{offer_id}/bids")
async def bid(product_id: str, offer_id: int, body: BidBody, request: Request) -> dict:
    """Bid for the product's offer for a kind of consumer, stating its purpose and agreeing to the
    offer's terms of use: the bid fires the outbound request hook, names who approves it, and
    waits for its decision. An open offer is no request at all: query it now."""
    require_capability_request(request, "data_product_read")
    requester = _requester(request)
    state = _state()
    async with state.model_db.acquire() as conn:
        product = await _product(conn, product_id)
        try:
            offer_row = await offers.require_offer(conn, product_id, offer_id)
            if offer_row["open"]:
                raise OfferRefused(
                    "open_no_request",
                    f"offer {offer_row['name']!r} is open: its data is public, so there is "
                    "nothing to request; query it now",
                    status=409,
                    query_now=True,
                )
            hook, target = await _destination(state)
            request_id = offer_hooks.new_request_id()
            payload = await offer_hooks.payload(
                conn,
                request_id=request_id,
                requester=requester,
                product_id=product_id,
                offer_row=offer_row,
                reason=body.reason.strip(),
                consumer_kind=body.consumer_kind,
            )
            async with conn.transaction():
                held = await offers.bid(
                    conn,
                    product_id,
                    offer_id,
                    requester=requester,
                    reason=body.reason,
                    consumer_kind=body.consumer_kind,
                    agreed=body.agreed,
                    approver=approver_of(product, target)["name"],
                    request_id=request_id,
                    hook=hook,
                    payload=payload,
                )
                await _fire(target, payload)
        except OfferRefused as refused:
            raise _refused(refused) from refused
    return _bid_view(held)


async def _cancel(product_id: str, bid_id: int, reason: str, request: Request, status: str) -> dict:
    """The requester cancels their bid, pending (a withdrawal) or accepted: the cancel fires the
    outbound request hook, and the local default applies it at once -- withdrawing the bid, or
    removing what accepting it granted."""
    requester = _requester(request)
    state = _state()
    async with state.model_db.acquire() as conn:
        await _product(conn, product_id)
        try:
            held = await offers.own_bid(
                conn, product_id, bid_id, requester=requester, status=status
            )
            hook, target = await _destination(state)
            request_id = offer_hooks.new_request_id()
            payload = offer_hooks.cancel_payload(held, request_id=request_id, reason=reason.strip())
            async with conn.transaction():
                cancel = await offers.cancel(
                    conn, held, reason=reason, request_id=request_id, hook=hook, payload=payload
                )
                if hook == offer_hooks.LOCAL:
                    cancel = await offer_hooks.apply_cancel(
                        conn, cancel, by=requester, at=None, provisa_held=True
                    )
                await _fire(target, payload)
        except OfferRefused as refused:
            raise _refused(refused) from refused
        await _reload_held(conn, state)
    return _bid_view(cancel)


@router.get("/{product_id}/bids")
async def list_bids(product_id: str, request: Request) -> list[dict]:
    """The product's bids and cancels, newest first: all of them for its owner role, else the
    caller's own."""
    require_capability_request(request, "data_product_read")
    state = _state()
    async with state.model_db.acquire() as conn:
        product = await _product(conn, product_id)
        mine = None if _is_owner(request, state, product) else _requester(request)
        return [_bid_view(b) for b in await offers.list_bids(conn, product_id, requester=mine)]


def _require_owner(request: Request, state: Any, product: dict) -> None:
    if not _is_owner(request, state, product):
        raise ApiError(
            403,
            "data_product.bid.owner_role_required",
            f"a bid for data product {product['id']!r} is decided by its owner role "
            f"{product.get('owner_role')!r}",
            owner_role=product.get("owner_role"),
        )


def _grants(state: Any) -> Any:
    """What applying an accepted bid needs where Provisa holds access; None where an identity
    provider holds it, and a decision only updates the request's status."""
    if offer_hooks.held_by_idp(state.auth_config or {}):
        return None

    async def grants(conn: Any, held: dict) -> offer_hooks.Grants:
        product = await _product(conn, held["product_id"])
        role_id = held["terms"]["role_id"]
        if role_id not in state.roles:
            raise OfferRefused(
                "role_not_found", f"role {role_id!r} is not defined", status=409, role=role_id
            )
        return offer_hooks.Grants(product["domain_id"], role_defaults(state.roles[role_id]))

    return grants


#: The refusals of a synthetic offer's acceptance that list it for review, and why.
_SYNTHETIC_REVIEWS = {
    "synthetic_needs_profiler": REVIEW_PROFILER_GONE,
    "synthetic_needs_fake": REVIEW_FAKE_GONE,
}


async def _decide(conn: Any, state: Any, decision: offer_hooks.Decision, may: Any) -> dict:
    """Apply ``decision`` through the decision hook, in one transaction."""
    try:
        async with conn.transaction():
            row = await offer_hooks.decide(conn, decision, may_grant=may, grants=_grants(state))
    except OfferRefused as refused:
        why = _SYNTHETIC_REVIEWS.get(refused.code)
        if why is not None:
            held = await offers.require_request(conn, decision.request_id)
            if held["offer_id"] is not None:
                await offers.set_review(conn, held["offer_id"], why)
        raise _refused(refused) from refused
    await _reload_held(conn, state)
    return _bid_view(row)


async def _decide_locally(
    product_id: str,
    bid_id: int,
    outcome: offer_hooks.Outcome,
    reason: str | None,
    request: Request,
) -> dict:
    """The local request queue's client of the decision hook: the product's owner role decides
    a pending bid, as the signed-in caller, now."""
    state = _state()
    async with state.model_db.acquire() as conn:
        product = await _product(conn, product_id)
        _require_owner(request, state, product)
        try:
            held = await offers.require_pending(conn, product_id, bid_id)
        except OfferRefused as refused:
            raise _refused(refused) from refused

        async def may(role_id: str) -> str | None:
            return may_grant(request, state, role_id)

        decision = offer_hooks.Decision(
            request_id=held["request_id"],
            outcome=outcome,
            decider=_caller(request) or _ANONYMOUS,
            decided_at=datetime.now(UTC),
            reason=reason,
        )
        return await _decide(conn, state, decision, may)


@router.post("/{product_id}/bids/{bid_id}/accept")
async def accept(product_id: str, bid_id: int, request: Request) -> dict:
    """Accept the bid as bid, through the decision hook: the product's owner role, who must
    still be able to grant the offer's role."""
    require_capability_request(request, "data_product_read")
    return await _decide_locally(product_id, bid_id, "accepted", None, request)


@router.post("/{product_id}/bids/{bid_id}/decline")
async def decline(product_id: str, bid_id: int, body: DeclineBody, request: Request) -> dict:
    """Decline the bid, stating why, through the decision hook: the product's owner role."""
    require_capability_request(request, "data_product_read")
    return await _decide_locally(product_id, bid_id, "declined", body.reason, request)


@router.post("/{product_id}/bids/{bid_id}/withdraw")
async def withdraw(product_id: str, bid_id: int, body: CancelBody, request: Request) -> dict:
    """The requester withdraws their pending bid: a cancel of it."""
    require_capability_request(request, "data_product_read")
    return await _cancel(product_id, bid_id, body.reason, request, "pending")


@router.post("/{product_id}/bids/{bid_id}/cancel")
async def cancel(product_id: str, bid_id: int, body: CancelBody, request: Request) -> dict:
    """The requester cancels their accepted bid: the cancel fires the outbound request hook, and
    the local default applies it as the matching removal."""
    require_capability_request(request, "data_product_read")
    return await _cancel(product_id, bid_id, body.reason, request, "accepted")


# -- the org's request hook ------------------------------------------------------------------------


class HookBody(BaseModel):
    url: str
    secret: str


def _hook_view(target: offer_hooks.HookTarget) -> dict:
    """The hook as a response may carry it: its URL, and whether its secret is set (REQ-1575)."""
    return {"url": target.url, "secret_set": bool(target.secret)}


@router.get("/request-hook")
async def get_request_hook(request: Request) -> dict:
    """Where the org's access requests go: its hook (its URL, and whether its secret is set; the
    secret never comes back out, REQ-1575), or the local request queue (``hook`` None) -- refused where an identity provider holds access, which has
    no local default."""
    from provisa.api.admin._platform_guard import require_org_settings

    require_org_settings(request)
    require_active_org_id(request)
    target = await _hook_target(_state())
    return {
        "hook": None if target is None else _hook_view(target),
        "local_default": not offer_hooks.held_by_idp(_state().auth_config or {}),
    }


@router.put("/request-hook")
async def set_request_hook(body: HookBody | None, request: Request) -> dict:
    """Keep the org's request hook: an http(s) URL, and its secret as entered, resolved at use
    like any credential (``null`` clears it, sending requests to the local queue)."""
    from provisa.api.admin._platform_guard import require_org_settings
    from provisa.core.org_settings import write_org_overrides

    require_org_settings(request)
    require_active_org_id(request)
    value = None
    target = None
    if body is not None:
        try:
            target = offer_hooks.checked(offer_hooks.HookTarget(url=body.url, secret=body.secret))
        except ValueError as exc:
            raise ApiError(422, "data_product.offer.hook_invalid", str(exc)) from exc
        value = {"url": target.url, "secret": target.secret}
    await write_org_overrides(
        _state().model_db,
        {offer_hooks.SETTINGS_KEY: value},
        updated_by=_caller(request) or _ANONYMOUS,
    )
    return {"hook": None if target is None else _hook_view(target)}


# -- the inbound decision hook ---------------------------------------------------------------------


class DecisionBody(BaseModel):
    request_id: str
    outcome: Literal["accepted", "declined"]
    decider: str | None = None
    decided_at: datetime | None = None
    reason: str | None = None


@router.post("/decisions")
async def decision_hook(body: DecisionBody, request: Request) -> dict:
    """The inbound decision hook for a signed-in caller: a request's id, accepted or declined. The
    caller decides as themselves, now, as the product's owner role -- as the local queue does. A
    signed external caller is not served here."""
    require_capability_request(request, "data_product_read")
    state = _state()
    async with state.model_db.acquire() as conn:
        try:
            held = await offers.require_request(conn, body.request_id)
        except OfferRefused as refused:
            raise _refused(refused) from refused
    caller = _caller(request)
    if body.decider is not None and caller is not None and body.decider != caller:
        raise ApiError(
            403,
            "data_product.decision.not_the_decider",
            f"a signed-in caller decides as themselves, not as {body.decider!r}",
        )
    return await _decide_locally(held["product_id"], held["id"], body.outcome, body.reason, request)


# -- profiles --------------------------------------------------------------------------------------


@router.get("/{product_id}/profiles")
async def product_profiles(product_id: str, request: Request) -> list[dict]:
    """Each dataset of the product with its latest profile, as the caller's roles see the data
    (REQ-2180, A PROFILE SHOWN IS SANITIZED): a column they are not served is left out, and a
    masked or PII-tagged one shows its type, null rate and distinct count and no value. Sanitized
    here, on the server. ``run`` and ``results`` are the profiler's own shapes (ProfileRun,
    ProfileRunResults); both None for a dataset with no profiler or no successful run."""
    require_capability_request(request, "data_product_read")
    from provisa.core import offer_profiles

    roles = _viewer_roles(request)
    state = _state()
    async with state.model_db.acquire() as conn:
        await _product(conn, product_id)
        datasets = await offers.dataset_ids(conn, product_id)
        names = await offers.table_names(conn, datasets)
        out = []
        for dataset in datasets:
            latest = await offer_profiles.latest(conn, state, dataset, roles)
            out.append(
                {
                    "table_id": dataset,
                    "table": names.get(dataset, str(dataset)),
                    "run": None if latest is None else latest.run,
                    "results": None if latest is None else latest.results,
                }
            )
    return out


def _viewer_roles(request: Request) -> frozenset[str]:
    """The roles the caller reads as: their acting role, a meta-role's members."""
    from provisa.security.meta_role import acting_roles

    acting = str(getattr(request.state, "role", "") or "")
    if not acting:
        raise ApiError(
            400,
            "data_product.profile.role_required",
            "a profile is shown as its reader's role sees the data; no role is acting",
        )
    return frozenset(m for r in acting.split(",") if r.strip() for m in acting_roles(_state(), r))
