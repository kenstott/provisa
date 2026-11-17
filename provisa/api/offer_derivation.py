# Copyright (c) 2026 Kenneth Stott
# Canary: a1856fc3-b078-4511-a748-b0912c11c97f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The contracts a product's offers give, derived from the model the runtime serves (REQ-2180,
CONTRACTS ARE DERIVED FROM THE OFFERS).

An offer's contract on a dataset is the dataset as the offer's role reads it: its columns (a
masked one stated as masked), its row rules in words, the offer's data level, the product's
promises and, where the offer includes them, its profiles. What the role reads is what the
runtime serves, so the contracts are derived from a built model:

* when an offer is saved (the offers router), its own;
* after every build this process announces -- a configuration load, an admin's save of a member,
  a column, a mask or a row rule, a change of a product's members -- every offer's
  (:func:`rederive_all`), so a contract follows what its terms are applied to. A contract whose
  terms did not change keeps its version; one whose terms did takes its next
  (provisa.core.offer_contracts). A reload of another process's change derives nothing: the
  process that made it did.

An offer the build cannot derive -- its role no longer defined, a dataset of its product not in
the build (a draft member), or a sample whose views are not views over the product's datasets
alone, together all of them -- is listed for review, saying why, and keeps the contracts it had.
The sample is checked here because an offer a configuration declares is saved by the load
unchecked against the build (REQ-2180, amended 2026-11-16): the views it names may be declared in
the same file, and only the build that follows the load has published them. Every build checks
every sample offer, so one an API save checked is listed too when a later build breaks it.

Every request that acts on a product reads the build the runtime has published (:func:`published`,
REQ-194 and REQ-2125 as amended 2026-11-16): its names table, its checker targets and the tables
its views read. The stored copies are written after the build answers and are the meta domain's.
"""

# Requirements: REQ-2180, REQ-194, REQ-2125

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from provisa.core import offer_contracts
from provisa.core.models import OfferContract
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import data_product_offer as offers
from provisa.core.repositories.data_product_offer import OfferRefused

#: Why an offer is listed for review when the build cannot derive its contracts: each a reason
#: the UI's catalog states (dataProductOffers.reviewReason).
REVIEW_ROLE_GONE = "role_gone"
REVIEW_NOT_BUILT = "not_built"
REVIEW_SAMPLE_NOT_A_VIEW = "sample_not_a_view"
REVIEW_SAMPLE_OUTSIDE_PRODUCT = "sample_outside_product"
REVIEW_SAMPLE_MISSES_DATASET = "sample_misses_dataset"
_DERIVATION_REVIEWS = {
    "role_not_found": REVIEW_ROLE_GONE,
    "not_built": REVIEW_NOT_BUILT,
    "sample_not_a_view": REVIEW_SAMPLE_NOT_A_VIEW,
    "sample_outside_product": REVIEW_SAMPLE_OUTSIDE_PRODUCT,
    "sample_misses_dataset": REVIEW_SAMPLE_MISSES_DATASET,
}


def published(state: Any) -> offers.Published:
    """The build ``state``'s runtime has published, as a request acting on a product reads it."""
    return offers.Published(
        names=state.names_table,
        checked=state.checked_targets,
        view_reads=lambda view_ids: view_reads(state, view_ids),
    )


def view_reads(state: Any, view_ids: list[int]) -> dict[int, set[int]]:
    """Each of ``view_ids`` that is a view of the published model, with the ids of the registered
    tables and views its SQL reads, resolved against that model (provisa.mv.view_inputs, as a
    view's refresh resolves them). A table that is no view is absent. A reference to a
    materialized view's stored table is the view's storage, which is no registered table; it is
    not among them."""
    from provisa.mv.view_inputs import ModelIndex, call_table_refs, table_refs

    built = {int(t["id"]): t for t in state.tables}
    views = [v for v in view_ids if v in built and built[v].get("view_sql")]
    if not views:
        return {}
    index = ModelIndex(state)
    out: dict[int, set[int]] = {}
    for view in views:
        sql = built[view]["view_sql"]
        refs = table_refs(sql)
        refs += [r for r in call_table_refs(sql, state.tracked_functions) if r not in refs]
        resolved = (index.resolve(built[view]["table_name"], ref) for ref in refs)
        out[view] = {r.table_id for r in resolved if r.table_id is not None}
    return out


def seen_columns(state: Any, role_id: str, table: dict) -> list[offer_contracts.SeenColumn]:
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
            type=c["data_type"],
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
    the next version of the one stored where its terms changed. Stored, and returned. Refused
    (:class:`OfferRefused`) when the offer's role is not defined, a sample's views do not hold
    against the build (:func:`offers.check_sample`) or a dataset is not built."""
    from provisa.security.meta_role import reader_chain

    offer = offers.as_offer(offer_row)
    if offer.role_id not in state.roles:
        raise OfferRefused(
            "role_not_found", f"role {offer.role_id!r} is not defined", role=offer.role_id
        )
    datasets = await offers.dataset_ids(conn, product_id)
    if offer.data_level == "sample":
        await offers.check_sample(conn, datasets, list(offer.sample_views), published(state))
    built = {t["id"]: t for t in state.tables}
    stored = await offers.contracts(conn, offer_row["id"])
    read_by = view_reads(state, list(offer.sample_views))
    names = await offers.table_names(conn, offer.sample_views)
    chain = reader_chain(state, offer.role_id)
    now = datetime.now(UTC)
    derived: dict[int, OfferContract] = {}
    for dataset in datasets:
        table = built.get(dataset)
        if table is None:
            raise OfferRefused(
                "not_built",
                f"dataset {dataset} is not in the last model build; build the model first",
                status=409,
                table=dataset,
            )
        profiles = await offers.profile_tables(conn, dataset) if offer.profiles else []
        terms = offer_contracts.terms(
            offer,
            seen_columns(state, offer.role_id, table),
            row_rules=await offers.row_rule_words(conn, chain, dataset, table["domain_id"]),
            sample_views=[names[v] for v in offer.sample_views if dataset in read_by.get(v, ())],
            checks=await data_product_repo.promised_checks(conn, dataset, state.checked_targets),
            profiles=list((await offers.table_names(conn, profiles)).values()),
        )
        derived[dataset] = offer_contracts.recorded(stored.get(dataset), terms, at=now)
    if derived != stored:
        await offers.store_contracts(conn, offer_row["id"], derived)
    return derived


async def rederive_all(conn: Any, state: Any) -> None:
    """Every offer's contracts derived again from the model ``state`` serves, one transaction per
    offer. An offer the build cannot derive is listed for review, saying why; one listed for
    that reason that derives again is taken off the list."""
    for product in await data_product_repo.list_all(conn):
        for row in await offers.list_offers(conn, product["id"]):
            try:
                async with conn.transaction():
                    await derive(conn, state, product["id"], row)
            except OfferRefused as refused:
                why = _DERIVATION_REVIEWS.get(refused.code)
                if why is None:
                    raise
                if row.get("review") != why:
                    await offers.set_review(conn, row["id"], why)
                continue
            if row.get("review") in _DERIVATION_REVIEWS.values():
                await offers.set_review(conn, row["id"], None)
