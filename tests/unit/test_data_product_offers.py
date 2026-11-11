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
OR SYNTHETIC; A SYNTHETIC OFFER NEEDS A PROFILER ON EVERY DATASET): a product's offers, the bids
for them and what accepting one grants, against a real SQLite tenant DB."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import insert, select

from provisa.core import offer_contracts, offer_grant
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


@pytest.mark.asyncio
async def test_a_bid_states_why_and_one_is_pending_per_requester(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        with pytest.raises(OfferRefused) as refused:
            await offers.bid(conn, "c360", offer["id"], requester="u1", reason="  ")
        assert refused.value.code == "reason_required"
        held = await offers.bid(conn, "c360", offer["id"], requester="u1", reason="churn study")
        assert (held["status"], held["offer_version"]) == ("pending", 1)
        assert held["terms"]["role_id"] == "viewer"
        with pytest.raises(OfferRefused) as again:
            await offers.bid(conn, "c360", offer["id"], requester="u1", reason="again")
        assert again.value.code == "bid_pending"


@pytest.mark.asyncio
async def test_an_offer_changed_after_the_bid_is_not_accepted_as_bid(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await offers.bid(conn, "c360", offer["id"], requester="u1", reason="r")
        assert (await offers.offer_as_bid(conn, held))["id"] == offer["id"]
        await offers.save(
            conn,
            "c360",
            DataProductOffer(name="default", role_id="viewer", dq_results=False),
            offer_id=offer["id"],
            by="o",
        )
        with pytest.raises(OfferRefused) as refused:
            await offers.offer_as_bid(conn, held)
        assert refused.value.code == "offer_changed"


@pytest.mark.asyncio
async def test_declining_states_why_and_a_decided_bid_is_decided_once(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await offers.bid(conn, "c360", offer["id"], requester="u1", reason="r")
        with pytest.raises(OfferRefused):
            await offers.decline(conn, "c360", held["id"], by="o", reason="")
        declined = await offers.decline(conn, "c360", held["id"], by="o", reason="too broad")
        assert (declined["status"], declined["decline_reason"]) == ("declined", "too broad")
        with pytest.raises(OfferRefused) as refused:
            await offers.decline(conn, "c360", held["id"], by="o", reason="again")
        assert refused.value.code == "bid_decided"


@pytest.mark.asyncio
async def test_only_the_requester_withdraws(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await offers.bid(conn, "c360", offer["id"], requester="u1", reason="r")
        with pytest.raises(OfferRefused) as refused:
            await offers.withdraw(conn, "c360", held["id"], by="u2")
        assert refused.value.code == "not_requester"
        assert (await offers.withdraw(conn, "c360", held["id"], by="u1"))["status"] == "withdrawn"


@pytest.mark.asyncio
async def test_removing_an_offer_declines_its_pending_bids(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        held = await offers.bid(conn, "c360", offer["id"], requester="u1", reason="r")
        await offers.delete_offer(conn, "c360", offer["id"])
        after = await offers.get_bid(conn, held["id"])
        assert after is not None
        assert (after["status"], after["offer_id"]) == ("declined", None)
        assert await offers.list_offers(conn, "c360") == []


@pytest.mark.asyncio
async def test_a_requester_lists_their_own_bids(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        offer = await _default(conn)
        await offers.bid(conn, "c360", offer["id"], requester="u1", reason="r")
        await offers.bid(conn, "c360", offer["id"], requester="u2", reason="r")
        assert len(await offers.list_bids(conn, "c360")) == 2
        assert [b["requester"] for b in await offers.list_bids(conn, "c360", requester="u2")] == [
            "u2"
        ]


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
async def test_a_synthetic_offer_is_not_accepted(control_plane):
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
        with pytest.raises(OfferRefused) as refused:
            await offer_grant.entries(conn, "c360", synthetic)
        assert refused.value.code == "synthetic_not_served"


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
