# Copyright (c) 2026 Kenneth Stott
# Canary: 2e6b9d41-8f37-4a15-b0c2-5d9a7e3f1c86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product's offers, the contracts derived from them, and the bids for them (REQ-2180).

Access to a data product is by offers. A product starts with one, the default (viewer, the full
data, with data quality results, without profiles, not open); any other is its owner's to add. An
offer always covers every dataset of the product: its member tables and views, a checker table
(the product's data quality results, REQ-2157) being none. A requester bids for an offer, and the
bid is a request fired through the outbound request hook (provisa.core.offer_hooks); whatever
receives it accepts or declines the offer as bid, never altering it. An open offer is no request
at all.

What is checked here is what the store alone can answer: a sample's views exist, are views and
read only the product's datasets, together all of them; a synthetic offer's datasets each have a
profiler. Whether the one saving or accepting an offer may grant its role asks the runtime, and is
asked where it is (provisa.api.admin.data_product_offers_router).
"""

# Requirements: REQ-2180

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select, update

from provisa.core import model_change
from provisa.core.models import CONSUMER_KINDS, DEFAULT_OFFER, DataProductOffer, OfferContract
from provisa.core.schema_org import (
    checker_targets,
    data_product_bids,
    data_product_offer_contracts,
    data_product_offers,
    data_product_tables,
    data_products,
    lineage_edges,
    model_names,
    registered_tables,
    sources,
    table_columns,
)

if TYPE_CHECKING:
    from datetime import datetime

    from provisa.core.database import Connection

_OFFER_FIELDS = tuple(DataProductOffer.model_fields)


class OfferRefused(ValueError):
    """An offer or a bid refused by name: ``code`` (data_product.offer.<code>), ``params``, and
    the HTTP status it answers with."""

    def __init__(self, code: str, message: str, *, status: int = 422, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.params = params


def _offer(row: Any) -> dict:
    out = dict(row._mapping)
    out["sample_views"] = list(out["sample_views"] or [])
    return out


def as_offer(row: dict) -> DataProductOffer:
    """A stored offer as the model it was saved from."""
    return DataProductOffer.model_validate({k: row[k] for k in _OFFER_FIELDS})


async def _product(conn: "Connection", product_id: str) -> Any:
    row = (
        await conn.execute_core(select(data_products).where(data_products.c.id == product_id))
    ).fetchone()
    if row is None:
        raise OfferRefused(
            "product_not_found",
            f"data product {product_id!r} not found",
            status=404,
            data_product=product_id,
        )
    return row


# -- datasets and what an offer gives on them ------------------------------------------------------


async def dataset_ids(conn: "Connection", product_id: str) -> list[int]:
    """The product's datasets: its member tables and views, in id order. A checker table it owns
    holds its data quality results, and is no dataset (REQ-2157)."""
    rows = await conn.execute_core(
        select(registered_tables.c.id)
        .join(data_product_tables, data_product_tables.c.table_id == registered_tables.c.id)
        .where(data_product_tables.c.product_id == product_id)
        .where(registered_tables.c.dq_contract.is_(None))
        .order_by(registered_tables.c.id)
    )
    return [int(r.id) for r in rows.fetchall()]


async def table_names(conn: "Connection", table_ids: Iterable[int]) -> dict[int, str]:
    """Each table's name as an operator reads it (its alias, else its table name)."""
    ids = list(table_ids)
    if not ids:
        return {}
    rows = await conn.execute_core(
        select(
            registered_tables.c.id, registered_tables.c.alias, registered_tables.c.table_name
        ).where(registered_tables.c.id.in_(ids))
    )
    return {int(r.id): r.alias or r.table_name for r in rows.fetchall()}


async def views_read(conn: "Connection", view_ids: Iterable[int]) -> dict[int, set[int]]:
    """Each of ``view_ids`` that is a view, and the tables it reads, by the model's lineage. A
    table that is no view is absent."""
    ids = list(view_ids)
    if not ids:
        return {}
    views = (
        await conn.execute_core(
            select(registered_tables.c.id)
            .where(registered_tables.c.id.in_(ids))
            .where(registered_tables.c.view_sql.is_not(None))
        )
    ).fetchall()
    out: dict[int, set[int]] = {int(r.id): set() for r in views}
    if not out:
        return out
    edges = await conn.execute_core(
        select(lineage_edges.c.target_table_id, lineage_edges.c.source_table_id)
        .where(lineage_edges.c.via_kind == "view")
        .where(lineage_edges.c.target_table_id.in_(list(out)))
        .where(lineage_edges.c.source_table_id.is_not(None))
    )
    for r in edges.fetchall():
        out[int(r.target_table_id)].add(int(r.source_table_id))
    return out


async def _check_sample(conn: "Connection", datasets: list[int], views: list[int]) -> None:
    """A sample's views each exist in the model as a view over the product's datasets alone, and
    together they read every dataset: an offer covers all of them."""
    read = await views_read(conn, views)
    names = await table_names(conn, [*views, *datasets])
    for view in views:
        if view not in read:
            raise OfferRefused(
                "sample_not_a_view",
                f"{names.get(view, view)!r} is not a view of the model; a sample is one or more "
                "existing views over the product's datasets",
                view=names.get(view, str(view)),
            )
        outside = sorted(read[view] - set(datasets))
        if not read[view] or outside:
            raise OfferRefused(
                "sample_outside_product",
                f"view {names[view]!r} reads more than the product's datasets"
                if outside
                else f"view {names[view]!r} reads none of the product's datasets",
                view=names[view],
            )
    uncovered = [names[d] for d in datasets if not any(d in read[v] for v in views)]
    if uncovered:
        raise OfferRefused(
            "sample_misses_dataset",
            f"no view of the sample reads {', '.join(uncovered)}; an offer covers every dataset "
            "of the product",
            datasets=", ".join(uncovered),
        )


async def without_profiler(conn: "Connection", datasets: Iterable[int]) -> list[str]:
    """The names of the datasets with no profiler (REQ-1934), in id order."""
    ids = list(datasets)
    if not ids:
        return []
    rows = await conn.execute_core(
        select(registered_tables.c.id, registered_tables.c.alias, registered_tables.c.table_name)
        .where(registered_tables.c.id.in_(ids))
        .where(registered_tables.c.profiler_source_id.is_(None))
        .order_by(registered_tables.c.id)
    )
    return [r.alias or r.table_name for r in rows.fetchall()]


async def _check_synthetic(conn: "Connection", datasets: list[int]) -> None:
    missing = await without_profiler(conn, datasets)
    if missing:
        raise OfferRefused(
            "synthetic_needs_profiler",
            f"synthetic data is generated from each dataset's profile, and "
            f"{', '.join(missing)} has no profiler",
            datasets=", ".join(missing),
        )


async def without_fake(conn: "Connection", datasets: Iterable[int]) -> list[str]:
    """The columns of the datasets that declare no fake (REQ-1494), as ``table.column`` in table
    id order: a synthetic offer's role reads each column through its fake, and these have none."""
    ids = list(datasets)
    if not ids:
        return []
    rows = await conn.execute_core(
        select(
            registered_tables.c.alias, registered_tables.c.table_name, table_columns.c.column_name
        )
        .join(table_columns, table_columns.c.table_id == registered_tables.c.id)
        .where(registered_tables.c.id.in_(ids))
        .where(table_columns.c.fake.is_(None))
        .order_by(registered_tables.c.id, table_columns.c.id)
    )
    return [f"{r.alias or r.table_name}.{r.column_name}" for r in rows.fetchall()]


async def synthetic_readers(conn: "Connection") -> dict[int, list[str]]:
    """Per dataset of a product with a synthetic offer, the roles of those offers, sorted: each
    reads every column of the dataset through its fake (REQ-2180, REQ-1494). A checker table is
    no dataset (REQ-2157)."""
    rows = await conn.execute_core(
        select(data_product_tables.c.table_id, data_product_offers.c.role_id)
        .join(
            data_product_offers,
            data_product_offers.c.product_id == data_product_tables.c.product_id,
        )
        .join(registered_tables, registered_tables.c.id == data_product_tables.c.table_id)
        .where(data_product_offers.c.data_level == "synthetic")
        .where(registered_tables.c.dq_contract.is_(None))
    )
    out: dict[int, set[str]] = {}
    for r in rows.fetchall():
        out.setdefault(int(r.table_id), set()).add(r.role_id)
    return {table_id: sorted(roles) for table_id, roles in sorted(out.items())}


async def quality_tables(conn: "Connection", product_id: str, dataset: int) -> list[int]:
    """The checker tables the product owns whose contracts scan ``dataset``: its data quality
    results on it (REQ-2157)."""
    owned = select(data_product_tables.c.table_id).where(
        data_product_tables.c.product_id == product_id
    )
    rows = await conn.execute_core(
        select(checker_targets.c.table_id)
        .where(checker_targets.c.checked_table_id == dataset)
        .where(checker_targets.c.table_id.in_(owned))
        .order_by(checker_targets.c.table_id)
    )
    return [int(r.table_id) for r in rows.fetchall()]


async def profile_tables(conn: "Connection", dataset: int) -> list[int]:
    """The profile result tables registered for ``dataset`` (REQ-1934): its profiles."""
    from provisa.profiler.schema import is_result_table_name, parse_result_table_name
    from provisa.profiler.source import PROFILER_SOURCE_TYPE

    rows = await conn.execute_core(
        select(registered_tables.c.id, registered_tables.c.table_name)
        .join(sources, sources.c.id == registered_tables.c.source_id)
        .where(sources.c.type == PROFILER_SOURCE_TYPE)
        .order_by(registered_tables.c.id)
    )
    return [
        int(r.id)
        for r in rows.fetchall()
        if is_result_table_name(r.table_name)
        and parse_result_table_name(r.table_name)[1] == dataset
    ]


async def sql_names(conn: "Connection", table_ids: Iterable[int]) -> dict[int, str]:
    """Each table's identifier as an entitlement list names it (REQ-2219): its domain's SQL name
    and its own, from the names table of the last model build. A table the build has not named
    is refused: its entitlement entry cannot be written."""
    from provisa.compiler.naming import domain_to_sql_name

    ids = sorted(set(table_ids))
    if not ids:
        return {}
    rows = await conn.execute_core(
        select(model_names.c.object_id, model_names.c.name, model_names.c.domain_id)
        .where(model_names.c.surface == "sql")
        .where(model_names.c.object_kind.in_(("table", "view")))
        .where(model_names.c.object_id.in_([str(i) for i in ids]))
    )
    out = {int(r.object_id): f"{domain_to_sql_name(r.domain_id)}.{r.name}" for r in rows.fetchall()}
    unnamed = [i for i in ids if i not in out]
    if unnamed:
        names = await table_names(conn, unnamed)
        raise OfferRefused(
            "not_built",
            f"{', '.join(names.get(i, str(i)) for i in unnamed)} has no name in the last model "
            "build; build the model before granting it",
            status=409,
            tables=", ".join(names.get(i, str(i)) for i in unnamed),
        )
    return out


# -- offers ----------------------------------------------------------------------------------------


async def list_offers(conn: "Connection", product_id: str) -> list[dict]:
    """The product's offers, in id order."""
    rows = await conn.execute_core(
        select(data_product_offers)
        .where(data_product_offers.c.product_id == product_id)
        .order_by(data_product_offers.c.id)
    )
    return [_offer(r) for r in rows.fetchall()]


async def get_offer(conn: "Connection", offer_id: int) -> dict | None:
    row = (
        await conn.execute_core(
            select(data_product_offers).where(data_product_offers.c.id == offer_id)
        )
    ).fetchone()
    return None if row is None else _offer(row)


async def require_offer(conn: "Connection", product_id: str, offer_id: int) -> dict:
    """The product's offer ``offer_id``; refused (404) when it has none of that id."""
    offer = await get_offer(conn, offer_id)
    if offer is None or offer["product_id"] != product_id:
        raise OfferRefused(
            "not_found",
            f"data product {product_id!r} has no offer {offer_id}",
            status=404,
            offer=offer_id,
        )
    return offer


async def add_default(conn: "Connection", product_id: str, *, by: str | None) -> int:
    """Give a new product its one default offer; returns its id. Part of creating the product,
    whose writer names the change (REQ-1524): one write, one commit named by its writer."""
    return await conn.insert_returning(
        data_product_offers,
        {
            "product_id": product_id,
            **DEFAULT_OFFER.model_dump(),
            "published_by": by,
        },
        "id",
    )


async def check(conn: "Connection", product_id: str, offer: DataProductOffer) -> None:
    """Refuse ``offer`` by name where the store says it cannot be given: its product does not
    exist, a sample's views are not views over the product's datasets alone, together all of them,
    or a synthetic offer's dataset has no profiler."""
    await _product(conn, product_id)
    datasets = await dataset_ids(conn, product_id)
    if offer.data_level == "sample":
        await _check_sample(conn, datasets, offer.sample_views)
    elif offer.data_level == "synthetic":
        await _check_synthetic(conn, datasets)


async def save(
    conn: "Connection",
    product_id: str,
    offer: DataProductOffer,
    *,
    offer_id: int | None,
    by: str | None,
) -> dict:
    """Add ``offer`` to the product (``offer_id`` None), or replace the one of that id: checked
    (:func:`check`), its version counted up by a change, its publisher ``by`` and its review
    cleared. The offer as stored."""
    async with conn.transaction():
        await check(conn, product_id, offer)
        await conn.require_admitted(update(data_products).where(data_products.c.id == product_id))
        clash = (
            await conn.execute_core(
                select(data_product_offers.c.id)
                .where(data_product_offers.c.product_id == product_id)
                .where(data_product_offers.c.name == offer.name)
            )
        ).fetchone()
        if clash is not None and clash.id != offer_id:
            raise OfferRefused(
                "name_taken",
                f"data product {product_id!r} already has an offer named {offer.name!r}",
                status=409,
                name=offer.name,
            )
        model_change.name("upsert", "data product offer", f"{product_id}/{offer.name}")
        values = {**offer.model_dump(), "published_by": by, "review": None}
        if offer_id is None:
            saved_id = int(
                await conn.insert_returning(
                    data_product_offers, {"product_id": product_id, **values}, "id"
                )
            )
        else:
            saved_id = offer_id
            held = await require_offer(conn, product_id, offer_id)
            changed = as_offer(held) != offer
            await conn.execute_core(
                update(data_product_offers)
                .where(data_product_offers.c.id == offer_id)
                .values(
                    **values,
                    version=held["version"] + (1 if changed else 0),
                    updated_at=func.now(),
                )
            )
        stored = await get_offer(conn, saved_id)
    assert stored is not None
    return stored


async def delete_offer(conn: "Connection", product_id: str, offer_id: int) -> None:
    """Remove the product's offer ``offer_id`` and its contracts. A pending bid for it ends with
    it: it is declined, the offer being gone."""
    async with conn.transaction():
        offer = await require_offer(conn, product_id, offer_id)
        await conn.require_admitted(update(data_products).where(data_products.c.id == product_id))
        model_change.name("delete", "data product offer", f"{product_id}/{offer['name']}")
        await conn.execute_core(
            update(data_product_bids)
            .where(data_product_bids.c.offer_id == offer_id)
            .where(data_product_bids.c.status == "pending")
            .values(
                status="declined",
                decline_reason=f"the offer {offer['name']!r} was removed",
                decided_at=func.now(),
            )
        )
        await conn.execute_core(
            delete(data_product_offers).where(data_product_offers.c.id == offer_id)
        )


async def set_review(conn: "Connection", offer_id: int, why: str | None) -> None:
    """List the offer for review, saying why; None takes it off the list."""
    await conn.execute_core(
        update(data_product_offers).where(data_product_offers.c.id == offer_id).values(review=why)
    )


# -- contracts -------------------------------------------------------------------------------------


async def contracts(conn: "Connection", offer_id: int) -> dict[int, OfferContract]:
    """The offer's contract on each dataset, by table id."""
    rows = await conn.execute_core(
        select(data_product_offer_contracts).where(
            data_product_offer_contracts.c.offer_id == offer_id
        )
    )
    return {int(r.table_id): OfferContract.model_validate(r.contract) for r in rows.fetchall()}


async def store_contracts(
    conn: "Connection", offer_id: int, derived: dict[int, OfferContract]
) -> None:
    """The offer's contracts become ``derived``: one per dataset, a dataset no longer the
    product's losing its own."""
    await conn.execute_core(
        delete(data_product_offer_contracts)
        .where(data_product_offer_contracts.c.offer_id == offer_id)
        .where(data_product_offer_contracts.c.table_id.not_in(list(derived)))
    )
    for table_id, contract in derived.items():
        await conn.upsert(
            data_product_offer_contracts,
            {
                "offer_id": offer_id,
                "table_id": table_id,
                "contract": contract.model_dump(mode="json"),
            },
            index_elements=["offer_id", "table_id"],
            update_columns=["contract"],
        )


# -- bids ------------------------------------------------------------------------------------------


def _bid(row: Any) -> dict:
    return dict(row._mapping)


async def get_bid(conn: "Connection", bid_id: int) -> dict | None:
    row = (
        await conn.execute_core(select(data_product_bids).where(data_product_bids.c.id == bid_id))
    ).fetchone()
    return None if row is None else _bid(row)


async def list_bids(
    conn: "Connection", product_id: str, *, requester: str | None = None
) -> list[dict]:
    """The product's bids, newest first; ``requester``'s alone when given."""
    query = select(data_product_bids).where(data_product_bids.c.product_id == product_id)
    if requester is not None:
        query = query.where(data_product_bids.c.requester == requester)
    rows = await conn.execute_core(
        query.order_by(data_product_bids.c.created_at.desc(), data_product_bids.c.id.desc())
    )
    return [_bid(r) for r in rows.fetchall()]


async def get_request(conn: "Connection", request_id: str) -> dict | None:
    """The bid or cancel that fired the request ``request_id``, or None."""
    row = (
        await conn.execute_core(
            select(data_product_bids).where(data_product_bids.c.request_id == request_id)
        )
    ).fetchone()
    return None if row is None else _bid(row)


async def require_request(conn: "Connection", request_id: str) -> dict:
    """The bid or cancel of the request ``request_id``, refused by name when there is none."""
    held = await get_request(conn, request_id)
    if held is None:
        raise OfferRefused(
            "request_not_found",
            f"no request {request_id!r} was made",
            status=404,
            request=request_id,
        )
    return held


async def bid(
    conn: "Connection",
    product_id: str,
    offer_id: int,
    *,
    requester: str,
    reason: str,
    consumer_kind: str,
    agreed: bool,
    approver: str | None,
    request_id: str,
    hook: str,
    payload: dict,
) -> dict:
    """``requester`` bids for the product's offer for a consumer of ``consumer_kind``, stating its
    purpose (``reason``), naming ``approver`` as who approves it, and having agreed to the offer's
    terms of use (amended 2026-11-11, A BID NAMES ITS CONSUMER AND AGREES TO THE TERMS): the offer
    as it stands, its terms of use with it, is what is bid for and agreed to, and ``payload`` the
    request fired for it, as ``request_id`` to ``hook``. A bid that does not agree is refused. One
    pending bid per requester and offer."""
    text = reason.strip()
    if not text:
        raise OfferRefused("reason_required", "a bid states its reason")
    if consumer_kind not in CONSUMER_KINDS:
        raise OfferRefused(
            "consumer_kind_unknown",
            f"a bid is for a {', a '.join(CONSUMER_KINDS)}; not {consumer_kind!r}",
            consumer_kind=consumer_kind,
        )
    if not agreed:
        raise OfferRefused(
            "terms_not_agreed", "a bid is sent only once its requester agrees to the offer's terms"
        )
    offer = await require_offer(conn, product_id, offer_id)
    pending = (
        await conn.execute_core(
            select(data_product_bids.c.id)
            .where(data_product_bids.c.offer_id == offer_id)
            .where(data_product_bids.c.requester == requester)
            .where(data_product_bids.c.kind == "bid")
            .where(data_product_bids.c.status == "pending")
        )
    ).fetchone()
    if pending is not None:
        raise OfferRefused(
            "bid_pending",
            f"{requester} already has a pending bid ({pending.id}) for offer {offer['name']!r}",
            status=409,
            bid=pending.id,
        )
    bid_id = await conn.insert_returning(
        data_product_bids,
        {
            "request_id": request_id,
            "kind": "bid",
            "product_id": product_id,
            "offer_id": offer_id,
            "offer_version": offer["version"],
            "terms": as_offer(offer).model_dump(),
            "requester": requester,
            "reason": text,
            "consumer_kind": consumer_kind,
            "approver": approver,
            "agreed_at": func.now(),
            "hook": hook,
            "payload": payload,
        },
        "id",
    )
    stored = await get_bid(conn, bid_id)
    assert stored is not None
    return stored


async def require_bid(conn: "Connection", product_id: str, bid_id: int) -> dict:
    """The product's bid ``bid_id``, refused by name when it has none of that id."""
    held = await get_bid(conn, bid_id)
    if held is None or held["product_id"] != product_id or held["kind"] != "bid":
        raise OfferRefused(
            "bid_not_found", f"data product {product_id!r} has no bid {bid_id}", status=404
        )
    return held


async def require_pending(conn: "Connection", product_id: str, bid_id: int) -> dict:
    """The product's pending bid ``bid_id``, refused by name when it is none."""
    held = await require_bid(conn, product_id, bid_id)
    _require_status(held, "pending")
    return held


def _require_status(held: dict, status: str) -> None:
    if held["status"] != status:
        raise OfferRefused(
            "bid_decided" if status == "pending" else f"bid_not_{status}",
            f"{held['kind']} {held['id']} is {held['status']}, not {status}",
            status=409,
            status_now=held["status"],
        )


async def own_bid(
    conn: "Connection", product_id: str, bid_id: int, *, requester: str, status: str
) -> dict:
    """``requester``'s bid ``bid_id`` of the product, in ``status``: refused by name when it is
    another's, or in another status."""
    held = await require_bid(conn, product_id, bid_id)
    if held["requester"] != requester:
        raise OfferRefused(
            "not_requester", "only the requester cancels a bid", status=403, bid=bid_id
        )
    _require_status(held, status)
    return held


async def cancel(
    conn: "Connection",
    bid: dict,
    *,
    reason: str,
    request_id: str,
    hook: str,
    payload: dict,
) -> dict:
    """The requester cancels their bid, pending (a withdrawal) or accepted, stating why: the
    cancel is a request of its own, ``payload`` fired as ``request_id`` to ``hook``. One pending
    cancel per bid."""
    text = reason.strip()
    if not text:
        raise OfferRefused("reason_required", "a cancel states its reason")
    if bid["kind"] != "bid" or bid["status"] not in ("pending", "accepted"):
        raise OfferRefused(
            "bid_not_cancellable",
            f"{bid['kind']} {bid['id']} is {bid['status']}; only a pending or accepted bid is "
            "cancelled",
            status=409,
            status_now=bid["status"],
        )
    accepted = bid
    pending = (
        await conn.execute_core(
            select(data_product_bids.c.id)
            .where(data_product_bids.c.cancels == accepted["id"])
            .where(data_product_bids.c.status == "pending")
        )
    ).fetchone()
    if pending is not None:
        raise OfferRefused(
            "cancel_pending",
            f"bid {accepted['id']} already has a pending cancel ({pending.id})",
            status=409,
            cancel=pending.id,
        )
    cancel_id = await conn.insert_returning(
        data_product_bids,
        {
            "request_id": request_id,
            "kind": "cancel",
            "cancels": accepted["id"],
            "product_id": accepted["product_id"],
            "offer_id": accepted["offer_id"],
            "offer_version": accepted["offer_version"],
            "terms": accepted["terms"],
            "requester": accepted["requester"],
            "reason": text,
            "hook": hook,
            "payload": payload,
        },
        "id",
    )
    stored = await get_bid(conn, cancel_id)
    assert stored is not None
    return stored


async def offer_as_bid(conn: "Connection", held: dict) -> dict:
    """The offer ``held`` bid for, as it was bid for: refused when it is gone or has changed since,
    since an offer is accepted as bid and never altered."""
    offer = None if held["offer_id"] is None else await get_offer(conn, held["offer_id"])
    if offer is None:
        raise OfferRefused("offer_gone", "the offer this bid is for was removed", status=409)
    if offer["version"] != held["offer_version"]:
        raise OfferRefused(
            "offer_changed",
            f"offer {offer['name']!r} changed after this bid; bid again for it as it is now",
            status=409,
            offer=offer["name"],
        )
    return offer


async def decided(
    conn: "Connection",
    row_id: int,
    status: str,
    *,
    by: str | None,
    at: "datetime | None",
    reason: str | None = None,
    granted: dict | None = None,
) -> dict:
    """Record the bid or cancel ``row_id`` as ``status``, decided ``by`` at ``at`` (None: now),
    with the decline's ``reason`` or what accepting ``granted``. The row as stored."""
    values: dict[str, Any] = {
        "status": status,
        "decided_by": by,
        "decided_at": func.now() if at is None else at,
    }
    if reason is not None:
        values["decline_reason"] = reason
    if granted is not None:
        values["granted"] = granted
    await conn.execute_core(
        update(data_product_bids).where(data_product_bids.c.id == row_id).values(**values)
    )
    stored = await get_bid(conn, row_id)
    assert stored is not None
    return stored


async def row_rule_words(
    conn: "Connection", chain: Iterable[str], table_id: int, domain_id: str
) -> list[str]:
    """The row rules the roles of ``chain`` read ``table_id`` under, each in words: its stated
    description (REQ-2081). A rule on the table, on its domain, or on a tag the table carries."""
    from sqlalchemy import and_, or_

    from provisa.core.schema_org import rls_rules, tag_assignments

    tagged = select(tag_assignments.c.tag_id).where(
        tag_assignments.c.table_id == table_id, tag_assignments.c.column_name.is_(None)
    )
    rows = await conn.execute_core(
        select(rls_rules.c.description, rls_rules.c.rights)
        .where(rls_rules.c.role_id.in_(list(chain)))
        .where(rls_rules.c.action_name.is_(None))
        .where(
            or_(
                rls_rules.c.table_id == table_id,
                and_(
                    rls_rules.c.table_id.is_(None),
                    rls_rules.c.tag_id.is_(None),
                    rls_rules.c.domain_id == domain_id,
                ),
                rls_rules.c.tag_id.in_(tagged),
            )
        )
        .order_by(rls_rules.c.id)
    )
    return [r.description for r in rows.fetchall() if "R" in r.rights]
