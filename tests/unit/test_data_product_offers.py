# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2a9c41-3d87-4b15-a0e6-1c8b5d2f7a93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (amended 2026-11-11, OFFERS; ONE DEFAULT OFFER; AN OFFER'S DATA IS FULL, A SAMPLE VIEW,
OR SYNTHETIC; A SYNTHETIC OFFER NEEDS A PROFILER ON EVERY DATASET; REQUESTS GO THROUGH HOOKS): a
product's offers, the bids for them as requests through the hooks, their decisions and cancels,
and what accepting one grants, against a real SQLite tenant DB."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import insert, select

from provisa.core import offer_contracts, offer_grant, offer_hooks
from provisa.core.models import DataProduct, DataProductOffer
from provisa.core.repositories import data_product as product_repo
from provisa.core.repositories import data_product_offer as offers
from provisa.core.repositories.data_product_offer import OfferRefused
from provisa.core.schema_org import (
    checker_targets,
    data_product_tables,
    domains,
    entitlements,
    lineage_edges,
    model_names,
    registered_tables,
    sources,
    table_columns,
    user_role_assignments,
)
from provisa.entitlements import store

_NOW = datetime(2026, 11, 11, 12, tzinfo=UTC)

#: id, name, view?, source, profiler
_TABLES = (
    (1, "orders", False, "crm", None),
    (2, "customers", False, "crm", None),
    (3, "orders_sample", True, "crm", None),
    (4, "wide_view", True, "crm", None),
    (5, "orders_checks", False, "crm", None),
    (6, "orders_1_profile_columns", False, "prof", None),
)


@asynccontextmanager
async def _store(control_plane):
    """Product c360 in sales: datasets orders (1) and customers (2); a view orders_sample (3) over
    orders; a view wide_view (4) over orders and ledger (7); a checker orders_checks (5) of orders
    the product owns; a profile result table of orders (6). Every table named by the build."""
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(domains).values(id="sales"))
            await conn.execute_core(insert(sources).values(id="crm", type="postgres"))
            await conn.execute_core(insert(sources).values(id="prof", type="data_profiler"))
            for tid, name, view, source, _ in (*_TABLES, (7, "ledger", False, "crm", None)):
                await conn.execute_core(
                    insert(registered_tables).values(
                        id=tid,
                        source_id=source,
                        domain_id="sales",
                        schema_name="public",
                        table_name=name,
                        view_sql="select 1" if view else None,
                        dq_contract="checks: []" if tid == 5 else None,
                    )
                )
                await conn.execute_core(
                    insert(model_names).values(
                        object_kind="view" if view else "table",
                        object_id=str(tid),
                        surface="sql",
                        name=name,
                        physical_name=name,
                        label=name,
                        domain_id="sales",
                        table_id=tid,
                    )
                )
            for view, source in ((3, 1), (4, 1), (4, 7)):
                await conn.execute_core(
                    insert(lineage_edges).values(
                        source_relation="s",
                        source_column="c",
                        target_relation="t",
                        target_column="c",
                        via="v",
                        via_kind="view",
                        source_table_id=source,
                        target_table_id=view,
                    )
                )
            await conn.execute_core(insert(checker_targets).values(table_id=5, checked_table_id=1))
            await product_repo.upsert(
                conn,
                DataProduct(
                    id="c360", domain_id="sales", name="C360", owner_role="owner", created_by="o1"
                ),
            )
            for tid in (1, 2, 5):
                await conn.execute_core(
                    insert(data_product_tables).values(product_id="c360", table_id=tid)
                )
        yield db
    finally:
        await db.close()


async def _default(conn) -> dict:
    (offer,) = await offers.list_offers(conn, "c360")
    return offer


# -- offers ----------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_product_starts_with_one_default_offer(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        assert offers.as_offer(offer) == DataProductOffer(name="default", role_id="viewer")
        assert (offer["dq_results"], offer["profiles"], offer["open"]) == (True, False, False)
        assert offer["published_by"] == "o1"
        # Saving the product again adds no second one.
        await product_repo.upsert(conn, DataProduct(id="c360", domain_id="sales", name="C"))
        assert len(await offers.list_offers(conn, "c360")) == 1


@pytest.mark.asyncio
async def test_the_datasets_are_the_members_but_the_checker(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        assert await offers.dataset_ids(conn, "c360") == [1, 2]


@pytest.mark.asyncio
async def test_a_changed_offer_counts_its_version_up_and_an_unchanged_one_does_not(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        same = await offers.save(
            conn, "c360", offers.as_offer(offer), offer_id=offer["id"], by="o1"
        )
        assert same["version"] == 1
        changed = await offers.save(
            conn,
            "c360",
            DataProductOffer(name="default", role_id="viewer", profiles=True),
            offer_id=offer["id"],
            by="o2",
        )
        assert (changed["version"], changed["published_by"]) == (2, "o2")


_TERMS_OF_USE = {
    "purpose": "Churn analysis for the retention team.",
    "usage": "Monthly churn models and their dashboards.",
    "limitations": "Not for individual credit decisions; no re-identification.",
}


@pytest.mark.asyncio
async def test_an_offer_stores_and_returns_its_terms_of_use(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        saved = await offers.save(
            conn,
            "c360",
            DataProductOffer(name="analysts", role_id="viewer", **_TERMS_OF_USE),
            offer_id=None,
            by="o1",
        )
        held = await offers.get_offer(conn, saved["id"])
        assert held is not None
        assert {k: held[k] for k in _TERMS_OF_USE} == _TERMS_OF_USE
        assert offers.as_offer(held) == DataProductOffer(
            name="analysts", role_id="viewer", **_TERMS_OF_USE
        )
        listed = {o["name"]: o for o in await offers.list_offers(conn, "c360")}
        assert listed["analysts"]["limitations"] == _TERMS_OF_USE["limitations"]
        # The default offer states none until its owner writes them.
        assert (listed["default"]["purpose"], listed["default"]["usage"]) == ("", "")


@pytest.mark.asyncio
async def test_changing_an_offers_terms_of_use_counts_its_version_up(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        changed = await offers.save(
            conn,
            "c360",
            DataProductOffer(name="default", role_id="viewer", usage="Reporting only."),
            offer_id=offer["id"],
            by="o1",
        )
        assert (changed["version"], changed["usage"]) == (2, "Reporting only.")


@pytest.mark.asyncio
async def test_two_offers_of_a_product_have_two_names(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        with pytest.raises(OfferRefused) as refused:
            await offers.save(
                conn, "c360", DataProductOffer(name="default", role_id="x"), offer_id=None, by="o"
            )
        assert refused.value.code == "name_taken"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("views", "code"),
    [
        ([2], "sample_not_a_view"),
        ([4], "sample_outside_product"),
        ([3], "sample_misses_dataset"),
    ],
)
async def test_a_sample_is_views_over_the_products_datasets_covering_them(
    control_plane, views, code
):
    async with _store(control_plane) as db, db.acquire() as conn:
        sample = DataProductOffer(
            name="s", role_id="viewer", data_level="sample", sample_views=views
        )
        with pytest.raises(OfferRefused) as refused:
            await offers.save(conn, "c360", sample, offer_id=None, by="o")
        assert refused.value.code == code


@pytest.mark.asyncio
async def test_a_sample_over_every_dataset_is_saved(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        # customers' own sample: a view over it.
        await conn.execute_core(
            insert(lineage_edges).values(
                source_relation="s",
                source_column="c",
                target_relation="t",
                target_column="c",
                via="v",
                via_kind="view",
                source_table_id=2,
                target_table_id=3,
            )
        )
        sample = DataProductOffer(name="s", role_id="viewer", data_level="sample", sample_views=[3])
        saved = await offers.save(conn, "c360", sample, offer_id=None, by="o")
        assert saved["sample_views"] == [3]


@pytest.mark.asyncio
async def test_a_synthetic_offer_needs_a_profiler_on_every_dataset(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        synthetic = DataProductOffer(name="syn", role_id="viewer", data_level="synthetic")
        with pytest.raises(OfferRefused) as refused:
            await offers.save(conn, "c360", synthetic, offer_id=None, by="o")
        assert refused.value.code == "synthetic_needs_profiler"
        assert refused.value.params == {"datasets": "orders, customers"}
        await conn.execute_core(
            registered_tables.update()
            .values(profiler_source_id="prof")
            .where(registered_tables.c.id.in_([1, 2]))
        )
        assert (await offers.save(conn, "c360", synthetic, offer_id=None, by="o"))["name"] == "syn"


# -- contracts -------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_offers_contracts_are_stored_one_per_dataset(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        model = offers.as_offer(offer)
        derived = {
            t: offer_contracts.recorded(
                None,
                offer_contracts.terms(
                    model,
                    [offer_contracts.SeenColumn("id", "integer")],
                    row_rules=[],
                    sample_views=[],
                    checks=[],
                    profiles=[],
                ),
                at=_NOW,
            )
            for t in (1, 2)
        }
        await offers.store_contracts(conn, offer["id"], derived)
        assert set(await offers.contracts(conn, offer["id"])) == {1, 2}
        await offers.store_contracts(conn, offer["id"], {1: derived[1]})
        assert set(await offers.contracts(conn, offer["id"])) == {1}


# -- bids ------------------------------------------------------------------------------------------


async def _bid(
    conn,
    offer: dict,
    requester: str = "u1",
    reason: str = "r",
    *,
    consumer_kind: str = "person",
    agreed: bool = True,
) -> dict:
    """``requester`` bids for ``offer`` for a consumer of ``consumer_kind``, stating its purpose
    (``reason``) and agreeing (or not) to its terms of use; the request lands in the local queue,
    the product's owner role approving it."""
    request_id = offer_hooks.new_request_id()
    return await offers.bid(
        conn,
        "c360",
        offer["id"],
        requester=requester,
        reason=reason,
        consumer_kind=consumer_kind,
        agreed=agreed,
        approver="owner",
        request_id=request_id,
        hook=offer_hooks.LOCAL,
        payload=await offer_hooks.payload(
            conn,
            request_id=request_id,
            requester=requester,
            product_id="c360",
            offer_row=offer,
            reason=reason,
            consumer_kind=consumer_kind,
        ),
    )


async def _anyone(_role: str) -> None:
    return None


def _decision(held: dict, outcome: str = "accepted", reason: str | None = None, **more):
    return offer_hooks.Decision(
        request_id=held["request_id"],
        outcome=outcome,  # type: ignore[arg-type]
        decider=more.get("decider", "o1"),
        decided_at=more.get("at", _NOW),
        reason=reason,
    )


async def _provisa_held(_conn, _held: dict) -> offer_hooks.Grants:
    return offer_hooks.Grants(domain_id="sales", role_defaults={})


@pytest.mark.asyncio
async def test_a_bid_states_why_and_one_is_pending_per_requester(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        with pytest.raises(OfferRefused) as refused:
            await _bid(conn, offer, reason="  ")
        assert refused.value.code == "reason_required"
        held = await _bid(conn, offer, reason="churn study")
        assert (held["status"], held["offer_version"], held["kind"]) == ("pending", 1, "bid")
        assert held["terms"]["role_id"] == "viewer"
        assert held["hook"] == offer_hooks.LOCAL
        with pytest.raises(OfferRefused) as again:
            await _bid(conn, offer, reason="again")
        assert again.value.code == "bid_pending"


@pytest.mark.asyncio
async def test_a_bid_carries_its_approver_consumer_kind_purpose_and_agreement(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        offer = await offers.save(
            conn,
            "c360",
            DataProductOffer(name="default", role_id="viewer", **_TERMS_OF_USE),
            offer_id=offer["id"],
            by="o1",
        )
        held = await _bid(conn, offer, reason="Retention dashboard", consumer_kind="team")
        assert (held["approver"], held["consumer_kind"], held["reason"]) == (
            "owner",
            "team",
            "Retention dashboard",
        )
        assert held["agreed_at"] is not None
        # What was agreed to is the offer's terms of use as bid.
        assert {k: held["terms"][k] for k in _TERMS_OF_USE} == _TERMS_OF_USE
        assert held["payload"]["consumer_kind"] == "team"
        assert held["payload"]["offer"]["limitations"] == _TERMS_OF_USE["limitations"]


@pytest.mark.asyncio
async def test_a_bid_that_does_not_agree_to_the_terms_is_refused(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        with pytest.raises(OfferRefused) as refused:
            await _bid(conn, offer, agreed=False)
        assert refused.value.code == "terms_not_agreed"
        assert await offers.list_bids(conn, "c360") == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["person", "team", "data_product", "agent"])
async def test_a_bid_is_for_a_person_a_team_a_data_product_or_an_agent(control_plane, kind):
    async with _store(control_plane) as db, db.acquire() as conn:
        held = await _bid(conn, await _default(conn), consumer_kind=kind)
        assert held["consumer_kind"] == kind


@pytest.mark.asyncio
async def test_a_bid_for_another_kind_of_consumer_is_refused(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        with pytest.raises(OfferRefused) as refused:
            await _bid(conn, await _default(conn), consumer_kind="robot")
        assert refused.value.code == "consumer_kind_unknown"


@pytest.mark.asyncio
async def test_an_offer_changed_after_the_bid_is_not_accepted_as_bid(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await _bid(conn, offer)
        assert (await offers.offer_as_bid(conn, held))["id"] == offer["id"]
        await offers.save(
            conn,
            "c360",
            DataProductOffer(name="default", role_id="viewer", dq_results=False),
            offer_id=offer["id"],
            by="o",
        )
        with pytest.raises(OfferRefused) as refused:
            await offer_hooks.decide(conn, _decision(held), may_grant=_anyone, grants=_provisa_held)
        assert refused.value.code == "offer_changed"
        assert (await offers.get_bid(conn, held["id"]))["status"] == "pending"  # type: ignore[index]


@pytest.mark.asyncio
async def test_declining_states_why_and_a_decided_bid_is_decided_once(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await _bid(conn, offer)
        with pytest.raises(OfferRefused):
            await offer_hooks.decide(
                conn, _decision(held, "declined", ""), may_grant=_anyone, grants=_provisa_held
            )
        declined = await offer_hooks.decide(
            conn, _decision(held, "declined", "too broad"), may_grant=_anyone, grants=_provisa_held
        )
        assert (declined["status"], declined["decline_reason"]) == ("declined", "too broad")
        assert declined["decided_by"] == "o1"
        # The same decision again is a harmless repeat; another one is refused.
        again = await offer_hooks.decide(
            conn, _decision(held, "declined", "too broad"), may_grant=_anyone, grants=_provisa_held
        )
        assert again["id"] == held["id"]
        with pytest.raises(OfferRefused) as refused:
            await offer_hooks.decide(conn, _decision(held), may_grant=_anyone, grants=_provisa_held)
        assert refused.value.code == "request_decided"


@pytest.mark.asyncio
async def test_only_the_requester_cancels_a_bid(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await _bid(conn, offer)
        with pytest.raises(OfferRefused) as refused:
            await offers.own_bid(conn, "c360", held["id"], requester="u2", status="pending")
        assert refused.value.code == "not_requester"
        with pytest.raises(OfferRefused) as not_accepted:
            await offers.own_bid(conn, "c360", held["id"], requester="u1", status="accepted")
        assert not_accepted.value.code == "bid_not_accepted"
        own = await offers.own_bid(conn, "c360", held["id"], requester="u1", status="pending")
        assert own["id"] == held["id"]


@pytest.mark.asyncio
async def test_removing_an_offer_declines_its_pending_bids(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await _bid(conn, offer)
        await offers.delete_offer(conn, "c360", offer["id"])
        after = await offers.get_bid(conn, held["id"])
        assert after is not None
        assert (after["status"], after["offer_id"]) == ("declined", None)
        assert await offers.list_offers(conn, "c360") == []


@pytest.mark.asyncio
async def test_a_requester_lists_their_own_bids(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        await _bid(conn, offer, requester="u1")
        await _bid(conn, offer, requester="u2")
        assert len(await offers.list_bids(conn, "c360")) == 2
        assert [b["requester"] for b in await offers.list_bids(conn, "c360", requester="u2")] == [
            "u2"
        ]


# -- requests go through hooks ---------------------------------------------------------------------


async def _with_contracts(conn) -> dict:
    """The default offer, with a contract on each dataset."""
    offer = await _default(conn)
    model = offers.as_offer(offer)
    terms = offer_contracts.terms(
        model,
        [offer_contracts.SeenColumn("id", "integer")],
        row_rules=[],
        sample_views=[],
        checks=[],
        profiles=[],
    )
    await offers.store_contracts(
        conn, offer["id"], {t: offer_contracts.recorded(None, terms, at=_NOW) for t in (1, 2)}
    )
    return offer


@pytest.mark.asyncio
async def test_a_bid_carries_who_asks_the_terms_the_contract_versions_the_reason_and_an_id(
    control_plane,
):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _with_contracts(conn)
        held = await _bid(conn, offer, reason="churn study")
        body = held["payload"]
        assert body == {
            "id": held["request_id"],
            "kind": "bid",
            "requester": "u1",
            "product": "c360",
            "offer": {
                "name": "default",
                "role": "viewer",
                "data_level": "full",
                "sample_views": [],
                "dq_results": True,
                "profiles": False,
                "duration_days": None,
                "purpose": "",
                "usage": "",
                "limitations": "",
            },
            "contracts": {"sales.orders": "1.0.0", "sales.customers": "1.0.0"},
            "reason": "churn study",
            "consumer_kind": "person",
        }


@pytest.mark.asyncio
async def test_a_repeated_request_id_is_the_same_request(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await _bid(conn, offer, requester="u1")
        assert await offers.get_request(conn, held["request_id"]) == held
        with pytest.raises(Exception, match="(?i)unique"):
            await offers.bid(
                conn,
                "c360",
                offer["id"],
                requester="u2",
                reason="r",
                consumer_kind="person",
                agreed=True,
                approver="owner",
                request_id=held["request_id"],
                hook=offer_hooks.LOCAL,
                payload=held["payload"],
            )


@pytest.mark.asyncio
async def test_an_accepted_bid_is_an_admin_grant_of_exactly_what_was_bid_by_the_decider(
    control_plane,
):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await _bid(conn, offer)
        accepted = await offer_hooks.decide(
            conn, _decision(held, decider="o9"), may_grant=_anyone, grants=_provisa_held
        )
        assert (accepted["status"], accepted["decided_by"]) == ("accepted", "o9")
        assert offer_grant._aware(accepted["decided_at"]) == _NOW
        assert accepted["granted"]["added"] == [
            "sales.orders",
            "sales.customers",
            "sales.orders_checks",
        ]
        assert await _assignment(conn) == [("viewer", None)]
        (listed,) = [x for x in (await store.load_held(conn))["u1"].lists if x.kind == "tables"]
        assert listed.identifiers == ("sales.orders", "sales.customers", "sales.orders_checks")
        assert listed.set_by == "o9"


@pytest.mark.asyncio
async def test_a_decider_who_may_not_grant_the_role_is_refused(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await _bid(conn, offer)

        async def cannot(role: str) -> str:
            return f"role {role!r} is outside the roles you may grant"

        with pytest.raises(OfferRefused) as refused:
            await offer_hooks.decide(conn, _decision(held), may_grant=cannot, grants=_provisa_held)
        assert (refused.value.code, refused.value.params) == (
            "role_not_grantable",
            {"role": "viewer"},
        )
        assert (await offers.get_bid(conn, held["id"]))["status"] == "pending"  # type: ignore[index]
        assert await _assignment(conn) == []


@pytest.mark.asyncio
async def test_a_decision_matching_no_request_is_refused(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        with pytest.raises(OfferRefused) as refused:
            await offer_hooks.decide(
                conn,
                offer_hooks.Decision("nope", "accepted", "o1", _NOW),
                may_grant=_anyone,
                grants=_provisa_held,
            )
        assert (refused.value.code, refused.value.status) == ("request_not_found", 404)


@pytest.mark.asyncio
async def test_where_an_identity_provider_holds_access_a_decision_only_updates_status(
    control_plane,
):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await _bid(conn, offer)
        accepted = await offer_hooks.decide(conn, _decision(held), may_grant=_anyone, grants=None)
        assert (accepted["status"], accepted["granted"]) == ("accepted", None)
        assert await _assignment(conn) == []
        assert "u1" not in await store.load_held(conn)


async def _cancel(conn, held: dict, hook: str = offer_hooks.LOCAL) -> dict:
    request_id = offer_hooks.new_request_id()
    return await offers.cancel(
        conn,
        held,
        reason="done",
        request_id=request_id,
        hook=hook,
        payload=offer_hooks.cancel_payload(held, request_id=request_id, reason="done"),
    )


@pytest.mark.asyncio
async def test_cancelling_an_accepted_bid_is_the_matching_removal(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        await offers.save(
            conn,
            "c360",
            DataProductOffer(name="default", role_id="viewer", duration_days=30),
            offer_id=offer["id"],
            by="o1",
        )
        offer = await _default(conn)
        held = await _bid(conn, offer)
        accepted = await offer_hooks.decide(
            conn, _decision(held), may_grant=_anyone, grants=_provisa_held
        )
        cancel = await _cancel(conn, accepted)
        assert cancel["payload"]["cancels"] == held["request_id"]
        assert cancel["payload"]["offer"] == held["payload"]["offer"]
        applied = await offer_hooks.apply_cancel(conn, cancel, by="u1", at=None, provisa_held=True)
        assert applied["status"] == "accepted"
        assert (await offers.get_bid(conn, held["id"]))["status"] == "cancelled"  # type: ignore[index]
        # What the grant alone added is gone: the role it made, the entries and their expiry.
        assert await _assignment(conn) == []
        after = (await store.load_held(conn)).get("u1")
        assert after is None or (
            all(not x.identifiers for x in after.lists if x.kind == "tables")
            and after.expiries == ()
        )


@pytest.mark.asyncio
async def test_a_cancel_keeps_a_role_the_requester_already_held(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await _grant(conn, DataProductOffer(name="o", role_id="viewer", duration_days=10))
        offer = await _default(conn)
        held = await _bid(conn, offer)
        accepted = await offer_hooks.decide(
            conn, _decision(held), may_grant=_anyone, grants=_provisa_held
        )
        assert accepted["granted"]["role_held"] is True
        await offer_hooks.apply_cancel(
            conn, await _cancel(conn, accepted), by="u1", at=None, provisa_held=True
        )
        # The role stays, its extended date not brought back; only the bid's entries go.
        assert await _assignment(conn) == [("viewer", None)]
        (listed,) = [x for x in (await store.load_held(conn))["u1"].lists if x.kind == "tables"]
        assert listed.identifiers == ("sales.orders",)


@pytest.mark.asyncio
async def test_a_cancel_sent_to_a_hook_is_applied_when_its_decision_accepts_it(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await _bid(conn, offer)
        accepted = await offer_hooks.decide(
            conn, _decision(held), may_grant=_anyone, grants=_provisa_held
        )
        cancel = await _cancel(conn, accepted, hook="https://grants.example/requests")
        assert cancel["status"] == "pending"
        assert await _assignment(conn) == [("viewer", None)]
        await offer_hooks.decide(conn, _decision(cancel), may_grant=_anyone, grants=_provisa_held)
        assert (await offers.get_bid(conn, held["id"]))["status"] == "cancelled"  # type: ignore[index]
        assert await _assignment(conn) == []


@pytest.mark.asyncio
async def test_withdrawing_a_pending_bid_is_a_cancel_of_it(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await _bid(conn, offer)
        cancel = await _cancel(conn, held)
        await offer_hooks.apply_cancel(conn, cancel, by="u1", at=None, provisa_held=True)
        assert (await offers.get_bid(conn, held["id"]))["status"] == "withdrawn"  # type: ignore[index]
        with pytest.raises(OfferRefused) as again:
            await _cancel(conn, await offers.get_bid(conn, held["id"]))
        assert again.value.code == "bid_not_cancellable"


# -- what accepting grants -------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_data_grants_the_datasets_and_the_dq_results(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        full = DataProductOffer(name="o", role_id="viewer")
        assert await offer_grant.entries(conn, "c360", full) == [
            "sales.orders",
            "sales.customers",
            "sales.orders_checks",
        ]
        bare = DataProductOffer(name="o", role_id="viewer", dq_results=False, profiles=True)
        assert await offer_grant.entries(conn, "c360", bare) == [
            "sales.orders",
            "sales.customers",
            "sales.orders_1_profile_columns",
        ]


@pytest.mark.asyncio
async def test_a_sample_grants_its_views_never_the_tables_beneath(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        sample = DataProductOffer(
            name="s", role_id="viewer", data_level="sample", sample_views=[3], dq_results=False
        )
        assert await offer_grant.entries(conn, "c360", sample) == ["sales.orders_sample"]


@pytest.mark.asyncio
async def test_a_synthetic_offer_grants_the_datasets_once_each_has_a_profiler_and_fakes(
    control_plane,
):
    async with _store(control_plane) as db, db.acquire() as conn:
        synthetic = DataProductOffer(name="syn", role_id="viewer", data_level="synthetic")
        with pytest.raises(OfferRefused) as gone:
            await offer_grant.entries(conn, "c360", synthetic)
        # A dataset's profiler removed: the offer is not accepted, named so.
        assert gone.value.code == "synthetic_needs_profiler"
        await conn.execute_core(
            registered_tables.update()
            .values(profiler_source_id="prof")
            .where(registered_tables.c.id.in_([1, 2]))
        )
        for tid, column, fake in ((1, "id", "hash()"), (1, "total", None), (2, "email", "email()")):
            await conn.execute_core(
                insert(table_columns).values(table_id=tid, column_name=column, fake=fake)
            )
        # Its role reads every column through its fake: a column with none is named.
        with pytest.raises(OfferRefused) as unfaked:
            await offer_grant.entries(conn, "c360", synthetic)
        assert (unfaked.value.code, unfaked.value.params) == (
            "synthetic_needs_fake",
            {"columns": "orders.total"},
        )
        await conn.execute_core(
            table_columns.update()
            .values(fake="uniform(min=0, max=10)")
            .where(table_columns.c.column_name == "total")
        )
        # Accepted: the datasets (read through their fakes) and the dq results it includes.
        assert await offer_grant.entries(conn, "c360", synthetic) == [
            "sales.orders",
            "sales.customers",
            "sales.orders_checks",
        ]


@pytest.mark.asyncio
async def test_accepting_a_synthetic_offer_grants_its_role(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await conn.execute_core(
            registered_tables.update()
            .values(profiler_source_id="prof")
            .where(registered_tables.c.id.in_([1, 2]))
        )
        saved = await offers.save(
            conn,
            "c360",
            DataProductOffer(name="syn", role_id="analyst", data_level="synthetic"),
            offer_id=None,
            by="o1",
        )
        held = await _bid(conn, saved)
        accepted = await offer_hooks.decide(
            conn, _decision(held), may_grant=_anyone, grants=_provisa_held
        )
        assert accepted["status"] == "accepted"
        assert accepted["granted"]["role_id"] == "analyst"
        assert accepted["granted"]["entries"] == [
            "sales.orders",
            "sales.customers",
            "sales.orders_checks",
        ]
        assert await offers.synthetic_readers(conn) == {1: ["analyst"], 2: ["analyst"]}


@pytest.mark.asyncio
async def test_a_table_the_build_has_not_named_is_refused(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await conn.execute_core(model_names.delete().where(model_names.c.object_id == "2"))
        with pytest.raises(OfferRefused) as refused:
            await offer_grant.entries(conn, "c360", DataProductOffer(name="o", role_id="viewer"))
        assert (refused.value.code, refused.value.params) == ("not_built", {"tables": "customers"})


async def _grant(conn, offer: DataProductOffer, now: datetime = _NOW) -> dict:
    return await offer_grant.grant(
        conn,
        principal="u1",
        offer=offer,
        domain_id="sales",
        identifiers=["sales.orders"],
        role_defaults={},
        set_by="o1",
        reason="accepted",
        now=now,
    )


async def _assignment(conn) -> list[tuple[str, datetime | None]]:
    rows = (
        await conn.execute_core(
            select(user_role_assignments.c.role_id, user_role_assignments.c.expires_at).where(
                user_role_assignments.c.user_id == "u1"
            )
        )
    ).fetchall()
    return [
        (r.role_id, None if r.expires_at is None else offer_grant._aware(r.expires_at))
        for r in rows
    ]


@pytest.mark.asyncio
async def test_a_requester_without_the_role_is_granted_it_narrowed_to_the_entries(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = DataProductOffer(name="o", role_id="viewer", duration_days=30)
        granted = await _grant(conn, offer)
        ends = _NOW + timedelta(days=30)
        assert await _assignment(conn) == [("viewer", ends)]
        assert granted["role_held"] is False
        assert granted["added"] == ["sales.orders"]
        held = (await store.load_held(conn))["u1"]
        (listed,) = [x for x in held.lists if x.kind == "tables"]
        assert listed.identifiers == ("sales.orders",)
        (rule,) = held.expiries
        assert offer_grant._aware(rule.ends_at) == ends
        assert rule.ends == {"*": ("sales.orders",)}


@pytest.mark.asyncio
async def test_a_role_granted_again_ends_at_the_latest_of_its_grants(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await _grant(conn, DataProductOffer(name="o", role_id="viewer", duration_days=10))
        await _grant(conn, DataProductOffer(name="o", role_id="viewer", duration_days=30))
        assert await _assignment(conn) == [("viewer", _NOW + timedelta(days=30))]
        # A shorter one does not shorten it.
        granted = await _grant(conn, DataProductOffer(name="o", role_id="viewer", duration_days=5))
        assert await _assignment(conn) == [("viewer", _NOW + timedelta(days=30))]
        assert granted["role_held"] is True
        # No duration: no end, later than any date.
        await _grant(conn, DataProductOffer(name="o", role_id="viewer"))
        assert await _assignment(conn) == [("viewer", None)]
        await _grant(conn, DataProductOffer(name="o", role_id="viewer", duration_days=5))
        assert await _assignment(conn) == [("viewer", None)]


@pytest.mark.asyncio
async def test_an_offers_summary_states_the_extension_accepting_would_make(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        assert (
            await offer_grant.extension(
                conn, "u1", DataProductOffer(name="o", role_id="viewer"), _NOW
            )
            is None
        )
        await _grant(conn, DataProductOffer(name="o", role_id="viewer", duration_days=10))
        forever = await offer_grant.extension(
            conn, "u1", DataProductOffer(name="o", role_id="viewer"), _NOW
        )
        assert forever is not None
        assert forever.statement() == "extends your viewer role from 21 Nov 2026 to no end date"
        assert (
            await offer_grant.extension(
                conn, "u1", DataProductOffer(name="o", role_id="viewer", duration_days=5), _NOW
            )
            is None
        )


@pytest.mark.asyncio
async def test_a_list_the_identity_provider_sends_is_not_added_to(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await store.mirror_idp(conn, "u1", {"viewer": {"tables": ("sales.customers",)}})
        with pytest.raises(OfferRefused) as refused:
            await _grant(conn, DataProductOffer(name="o", role_id="viewer"))
        assert refused.value.code == "idp_supersedes"
        assert await _assignment(conn) == []
        provisa_rows = (
            await conn.execute_core(select(entitlements).where(entitlements.c.source == "provisa"))
        ).fetchall()
        assert provisa_rows == []
