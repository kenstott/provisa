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
  to it, as the offer's role reads it (provisa.core.offer_contracts).
* A signed-in requester bids for an offer, stating why. The product's owner role accepts or
  declines the bid as bid; accepting checks again that the one accepting may grant the offer's
  role. An open offer is accepted at once against the right of the owner who published it; while
  that right is gone the bid stays pending and the offer is listed for review.
"""

# Requirements: REQ-2180, REQ-2219, REQ-2188

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin.capabilities import (
    require_capability_request,
    require_right_in_domains_request,
)
from provisa.api.errors import ApiError
from provisa.core import offer_contracts, offer_grant
from provisa.core.models import DataProductOffer, OfferContract
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
REVIEW_RIGHT_GONE = "the owner who published this open offer may no longer grant its role"
REVIEW_PROFILER_GONE = "a dataset of this synthetic offer has no profiler"


class BidBody(BaseModel):
    reason: str


class DeclineBody(BaseModel):
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


async def _publisher_refusal(
    conn: Any, state: Any, publisher: str | None, role_id: str
) -> str | None:
    """Why the owner who published an open offer may no longer grant its role, or None: the
    same rule as :func:`may_grant`, asked of them as they hold their roles now."""
    from provisa.entitlements import roles as held_roles
    from provisa.security.rights import capabilities_for_claims

    if publisher is None:
        return REVIEW_RIGHT_GONE
    idp = state.idp_entitlements.get(publisher, {})
    held = await held_roles.held_roles(conn, publisher, idp, state.auth_config or {}, state.roles)
    acting = [r.role_id for r in held]
    identity = SimpleNamespace(
        user_id=publisher,
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


def _seen_columns(state: Any, role_id: str, table: dict) -> list[offer_contracts.SeenColumn]:
    """``table``'s columns as ``role_id`` reads them, from what the runtime serves."""
    from provisa.security.effective_rights import data_view_from_state
    from provisa.security.inheritance import parent_map

    parents = parent_map({"id": rid, **r} for rid, r in state.roles.items())
    view = data_view_from_state(
        state,
        role_id,
        parents=parents,
        stored={},
        readable=lambda *_: True,
        governance=True,
        tables=[table],
    )
    read = {c["name"]: c for t in view for c in t["columns"] if c["read"]}
    return [
        offer_contracts.SeenColumn(
            name=c["column_name"],
            type=c.get("data_type") or "unknown",
            description=c.get("description"),
            read=c["column_name"] in read,
            mask=(read[c["column_name"]].get("mask") or {}).get("type")
            if c["column_name"] in read
            else None,
        )
        for c in table["columns"]
        if not c.get("native_filter_type")
    ]


async def derive(
    conn: Any, state: Any, product_id: str, offer_row: dict
) -> dict[int, OfferContract]:
    """The offer's contract on each of the product's datasets, as its role reads them now, each
    the next version of the one stored where its terms changed. Stored, and returned."""
    from provisa.security.meta_role import reader_chain

    offer = offers.as_offer(offer_row)
    if offer.role_id not in state.roles:
        raise ApiError(
            422,
            "data_product.offer.role_not_found",
            f"role {offer.role_id!r} is not defined",
            role=offer.role_id,
        )
    datasets = await offers.dataset_ids(conn, product_id)
    built = {t["id"]: t for t in state.tables}
    stored = await offers.contracts(conn, offer_row["id"])
    read_by = await offers.views_read(conn, offer.sample_views)
    names = await offers.table_names(conn, offer.sample_views)
    chain = reader_chain(state, offer.role_id)
    now = datetime.now(UTC)
    derived: dict[int, OfferContract] = {}
    for dataset in datasets:
        table = built.get(dataset)
        if table is None:
            raise ApiError(
                409,
                "data_product.offer.not_built",
                f"dataset {dataset} is not in the last model build; build the model first",
                table=dataset,
            )
        profiles = await offers.profile_tables(conn, dataset) if offer.profiles else []
        terms = offer_contracts.terms(
            offer,
            _seen_columns(state, offer.role_id, table),
            row_rules=await offers.row_rule_words(conn, chain, dataset, table["domain_id"]),
            sample_views=[names[v] for v in offer.sample_views if dataset in read_by.get(v, ())],
            checks=await data_product_repo.promised_checks(conn, dataset),
            profiles=list((await offers.table_names(conn, profiles)).values()),
        )
        derived[dataset] = offer_contracts.recorded(stored.get(dataset), terms, at=now)
    await offers.store_contracts(conn, offer_row["id"], derived)
    return derived


async def _offer_view(
    conn: Any, request: Request, offer_row: dict, *, now: datetime
) -> dict[str, Any]:
    """An offer as listed: its terms, its contracts (one per dataset), and the extension accepting
    it would make for the caller."""
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


async def _accept(
    conn: Any, state: Any, product: dict, held: dict, offer_row: dict, *, by: str | None
) -> dict:
    """Grant what the bid's offer gives and record the bid accepted. The caller holds the
    transaction and has checked who may accept."""
    offer = offers.as_offer(offer_row)
    now = datetime.now(UTC)
    identifiers = await offer_grant.entries(conn, product["id"], offer)
    granted = await offer_grant.grant(
        conn,
        principal=held["requester"],
        offer=offer,
        domain_id=product["domain_id"],
        identifiers=identifiers,
        role_defaults=role_defaults(state.roles[offer.role_id]),
        set_by=by or _ANONYMOUS,
        reason=f"accepted bid {held['id']} for offer {offer.name!r} of data product "
        f"{product['id']!r}: {held['reason']}",
        now=now,
    )
    await offers.mark_accepted(conn, held["id"], by=by, granted=granted)
    stored = await offers.get_bid(conn, held["id"])
    assert stored is not None
    return stored


async def _reload_held(conn: Any, state: Any) -> None:
    """What the runtime holds of the entitlement store, after a grant wrote to it."""
    from provisa.entitlements import store

    state.held_entitlements = await store.load_held(conn)


async def _review(conn: Any, offer_row: dict, why: str) -> None:
    if offer_row.get("review") != why:
        await offers.set_review(conn, offer_row["id"], why)


@router.post("/{product_id}/offers/{offer_id}/bids")
async def bid(product_id: str, offer_id: int, body: BidBody, request: Request) -> dict:
    """Bid for the product's offer, stating why. An open offer is accepted at once, against the
    right of the owner who published it; while that right is gone the bid stays pending and the
    offer is listed for review."""
    require_capability_request(request, "data_product_read")
    requester = _requester(request)
    state = _state()
    async with state.model_db.acquire() as conn:
        product = await _product(conn, product_id)
        try:
            offer_row = await offers.require_offer(conn, product_id, offer_id)
            held = await offers.bid(
                conn, product_id, offer_id, requester=requester, reason=body.reason
            )
        except OfferRefused as refused:
            raise _refused(refused) from refused
        try:
            if not offer_row["open"]:
                return _bid_view(held)
            refusal = await _publisher_refusal(
                conn, state, offer_row["published_by"], offer_row["role_id"]
            )
            if refusal is not None:
                await _review(conn, offer_row, REVIEW_RIGHT_GONE)
                return _bid_view(held)
            async with conn.transaction():
                accepted = await _accept(
                    conn, state, product, held, offer_row, by=offer_row["published_by"]
                )
        except OfferRefused as refused:
            if refused.code == "synthetic_needs_profiler":
                await _review(conn, offer_row, REVIEW_PROFILER_GONE)
            raise _refused(refused) from refused
        await _reload_held(conn, state)
    return _bid_view(accepted)


@router.get("/{product_id}/bids")
async def list_bids(product_id: str, request: Request) -> list[dict]:
    """The product's bids, newest first: all of them for its owner role, else the caller's own."""
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


@router.post("/{product_id}/bids/{bid_id}/accept")
async def accept(product_id: str, bid_id: int, request: Request) -> dict:
    """Accept the bid as bid: the product's owner role, who must still be able to grant the
    offer's role."""
    state = _state()
    async with state.model_db.acquire() as conn:
        product = await _product(conn, product_id)
        _require_owner(request, state, product)
        try:
            async with conn.transaction():
                held = await offers.require_pending(conn, product_id, bid_id)
                offer_row = await offers.offer_as_bid(conn, held)
                refusal = may_grant(request, state, offer_row["role_id"])
                if refusal is not None:
                    raise ApiError(
                        403,
                        "data_product.offer.role_not_grantable",
                        refusal,
                        role=offer_row["role_id"],
                    )
                accepted = await _accept(conn, state, product, held, offer_row, by=_caller(request))
        except OfferRefused as refused:
            raise _refused(refused) from refused
        await _reload_held(conn, state)
    return _bid_view(accepted)


@router.post("/{product_id}/bids/{bid_id}/decline")
async def decline(product_id: str, bid_id: int, body: DeclineBody, request: Request) -> dict:
    """Decline the bid, stating why: the product's owner role."""
    state = _state()
    async with state.model_db.acquire() as conn:
        product = await _product(conn, product_id)
        _require_owner(request, state, product)
        try:
            row = await offers.decline(
                conn, product_id, bid_id, by=_caller(request), reason=body.reason
            )
        except OfferRefused as refused:
            raise _refused(refused) from refused
    return _bid_view(row)


@router.post("/{product_id}/bids/{bid_id}/withdraw")
async def withdraw(product_id: str, bid_id: int, request: Request) -> dict:
    """The requester withdraws their pending bid."""
    requester = _requester(request)
    state = _state()
    async with state.model_db.acquire() as conn:
        await _product(conn, product_id)
        try:
            row = await offers.withdraw(conn, product_id, bid_id, by=requester)
        except OfferRefused as refused:
            raise _refused(refused) from refused
    return _bid_view(row)
