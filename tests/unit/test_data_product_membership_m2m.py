# Copyright (c) 2026 Kenneth Stott
# Canary: 3d9b1e52-7a40-4c8e-b6f1-2e8a5c0d9f17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1634 (amended 2026-11-07, MEMBERSHIP IS MANY-TO-MANY AND EDITED ON THE PRODUCT): a table
or command may belong to any number of data products, and a product to any number of members;
membership is a relation of its own, set only on the product, and a member joins only products
owned by its own domain."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import insert

from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.openlineage import to_events
from provisa.core.models import (
    Column,
    DataProduct,
    Domain,
    Function,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
)
from provisa.core.repositories import data_product as repo
from provisa.core.schema_org import (
    data_products,
    domains,
    registered_tables,
    sources,
    table_columns,
    tracked_functions,
)


@asynccontextmanager
async def _store(control_plane):
    """sales holding c360 and churn and the tables customers (1) and orders (2) and the command
    refresh_customer; finance holding ops_kpis and the table ledger (3) and the command close_books."""
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(domains).values(id="sales"))
            await conn.execute_core(insert(domains).values(id="finance"))
            await conn.execute_core(insert(sources).values(id="crm", type="postgres"))
            for pid, domain in (("c360", "sales"), ("churn", "sales"), ("ops_kpis", "finance")):
                await conn.execute_core(
                    insert(data_products).values(id=pid, domain_id=domain, name=pid.upper())
                )
            for tid, name, domain in (
                (1, "customers", "sales"),
                (2, "orders", "sales"),
                (3, "ledger", "finance"),
            ):
                await conn.execute_core(
                    insert(registered_tables).values(
                        id=tid,
                        source_id="crm",
                        domain_id=domain,
                        schema_name="public",
                        table_name=name,
                    )
                )
                await conn.execute_core(
                    insert(table_columns).values(
                        table_id=tid, column_name="id", data_type="integer", domain_id=domain
                    )
                )
            await conn.execute_core(
                insert(tracked_functions).values(name="refresh_customer", domain_id="sales")
            )
            await conn.execute_core(
                insert(tracked_functions).values(name="close_books", domain_id="finance")
            )
        yield db
    finally:
        await db.close()


async def _contracts(conn, built, product_id: str) -> dict[int, Any]:
    """The contracts the product's default offer gives after a build derives them (REQ-2180)."""
    from provisa.api.offer_derivation import rederive_all
    from provisa.core.repositories import data_product_offer as offers

    await rederive_all(conn, await built.state(conn))
    (offer,) = await offers.list_offers(conn, product_id)
    return await offers.contracts(conn, offer["id"])


@pytest.mark.asyncio
async def test_a_table_and_a_command_belong_to_two_products_of_their_domain(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await repo.set_members(conn, "c360", table_ids=[1, 2], commands=["refresh_customer"])
        await repo.set_members(conn, "churn", table_ids=[1], commands=["refresh_customer"])

        assert await repo.products_of_table(conn, 1) == ["c360", "churn"]
        assert await repo.products_of_table(conn, 2) == ["c360"]
        assert await repo.products_of_command(conn, "refresh_customer") == ["c360", "churn"]
        assert await repo.member_table_ids(conn, "c360") == [1, 2]
        assert await repo.member_table_ids(conn, "churn") == [1]
        assert await repo.table_memberships(conn) == {1: ["c360", "churn"], 2: ["c360"]}
        assert await repo.member_count(conn, "churn") == 2


@pytest.mark.asyncio
async def test_set_members_replaces_the_products_members_and_leaves_other_products_alone(
    control_plane,
):
    async with _store(control_plane) as db, db.acquire() as conn:
        await repo.set_members(conn, "c360", table_ids=[1, 2], commands=["refresh_customer"])
        await repo.set_members(conn, "churn", table_ids=[1], commands=[])

        await repo.set_members(conn, "c360", table_ids=[2], commands=[])

        assert await repo.member_table_ids(conn, "c360") == [2]
        assert await repo.member_command_names(conn, "c360") == []
        assert await repo.products_of_table(conn, 1) == ["churn"]


@pytest.mark.asyncio
async def test_a_member_of_another_domain_is_refused_and_nothing_changes(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await repo.set_members(conn, "c360", table_ids=[1], commands=[])

        with pytest.raises(repo.MembershipRefused) as table_refused:
            await repo.set_members(conn, "c360", table_ids=[1, 3], commands=[])
        assert table_refused.value.code == "other_domain"
        assert table_refused.value.params["domain"] == "finance"

        with pytest.raises(repo.MembershipRefused) as command_refused:
            await repo.set_members(conn, "c360", table_ids=[], commands=["close_books"])
        assert command_refused.value.code == "other_domain"

        assert await repo.member_table_ids(conn, "c360") == [1]
        assert await repo.products_of_table(conn, 3) == []
        assert await repo.products_of_command(conn, "close_books") == []


@pytest.mark.asyncio
async def test_a_member_that_does_not_exist_is_refused(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        with pytest.raises(repo.MembershipRefused) as no_table:
            await repo.set_members(conn, "c360", table_ids=[99], commands=[])
        assert no_table.value.code == "table_not_found"
        with pytest.raises(repo.MembershipRefused) as no_command:
            await repo.set_members(conn, "c360", table_ids=[], commands=["nope"])
        assert no_command.value.code == "command_not_found"
        with pytest.raises(repo.MembershipRefused) as no_product:
            await repo.set_members(conn, "nope", table_ids=[1], commands=[])
        assert no_product.value.code == "not_found"


@pytest.mark.asyncio
async def test_a_member_moved_to_another_domain_is_refused_while_it_is_a_member(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await repo.set_members(conn, "c360", table_ids=[1], commands=["refresh_customer"])
        with pytest.raises(repo.MembershipRefused, match="take it out of the product first"):
            await repo.require_domain_of_products(
                conn, await repo.products_of_table(conn, 1), "finance", "table customers"
            )
        # its own domain is no move
        await repo.require_domain_of_products(
            conn, await repo.products_of_table(conn, 1), "sales", "table customers"
        )


@pytest.mark.asyncio
async def test_a_table_has_a_contract_under_each_product_it_is_a_member_of(
    control_plane, monkeypatch
):
    """REQ-2180: a member's contracts are its products' offers' -- one under each product it is a
    member of, derived by the build -- and it loses a product's when it leaves that product."""
    from provisa.core.repositories import data_product_offer as offers
    from tests.unit.offer_build import installed

    built = installed(monkeypatch)
    async with _store(control_plane) as db, db.acquire() as conn:
        for pid in ("c360", "churn"):
            await offers.add_default(conn, pid, by=None)
        assert await _contracts(conn, built, "c360") == {}
        await repo.set_members(conn, "c360", table_ids=[1], commands=[])
        c360 = await _contracts(conn, built, "c360")
        assert list(c360) == [1]
        # A contract's schema is the member's columns as the offer's role reads them.
        assert [f.name for f in c360[1].terms.fields] == ["id"]

        await repo.set_members(conn, "churn", table_ids=[1], commands=[])
        assert list(await _contracts(conn, built, "churn")) == [1]
        assert await _contracts(conn, built, "c360") == c360  # joining a second keeps the first

        await repo.set_members(conn, "c360", table_ids=[], commands=[])
        assert await _contracts(conn, built, "c360") == {}
        assert list(await _contracts(conn, built, "churn")) == [1]  # still a member of churn


@pytest.mark.asyncio
async def test_retiring_moves_each_member_to_the_successor_once(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await conn.execute_core(
            data_products.update().where(data_products.c.id == "c360").values(status="live")
        )
        await repo.set_members(conn, "c360", table_ids=[1, 2], commands=["refresh_customer"])
        await repo.set_members(conn, "churn", table_ids=[1], commands=[])

        await repo.retire(conn, "c360", reason="Merged.", successor="churn", by=None)

        assert await repo.member_table_ids(conn, "c360") == []
        assert await repo.member_table_ids(conn, "churn") == [1, 2]
        assert await repo.member_command_names(conn, "churn") == ["refresh_customer"]


@pytest.mark.asyncio
async def test_a_product_with_members_is_not_deleted(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await repo.set_members(conn, "churn", table_ids=[1], commands=["refresh_customer"])
        with pytest.raises(repo.DataProductDeleteRefused) as refused:
            await repo.delete(conn, "churn")
        assert {(d.ref.kind, d.ref.id) for d in refused.value.dependents} == {
            ("table", 1),
            ("command", "refresh_customer"),
        }
        await repo.set_members(conn, "churn", table_ids=[], commands=[])
        assert await repo.delete(conn, "churn") is True


@pytest.mark.asyncio
async def test_a_product_with_members_is_not_moved_to_another_domain(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await repo.set_members(conn, "c360", table_ids=[1, 2], commands=["refresh_customer"])
        with pytest.raises(repo.MembershipRefused) as refused:
            await repo.require_members_in_domain(conn, "c360", "finance")
        assert refused.value.code == "domain_has_members"
        assert refused.value.params == {
            "data_product": "c360",
            "domain": "finance",
            "members": "customers, orders, refresh_customer",
        }
        await repo.require_members_in_domain(conn, "c360", "sales")  # its own domain is no move
        await repo.set_members(conn, "c360", table_ids=[], commands=[])
        await repo.require_members_in_domain(conn, "c360", "finance")  # no members, free to move


@pytest.mark.asyncio
async def test_the_product_save_refuses_a_domain_move_while_it_has_members(
    control_plane, monkeypatch
):
    from provisa.api.admin import capabilities, schema_mutation
    from provisa.api.admin.types import DataProductInput

    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            await repo.set_members(conn, "c360", table_ids=[1], commands=[])

        async def _pool():
            return db

        monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
        monkeypatch.setattr(capabilities, "require_capability", lambda _info, _cap: None)
        monkeypatch.setattr(capabilities, "require_right_in_domains", lambda _i, _r, _d: None)
        monkeypatch.setattr(capabilities, "_identity_from_info", lambda _info: None)
        mutation: Any = schema_mutation.Mutation()
        moved = DataProductInput(id="c360", domain_id="finance", name="C360")
        result = await mutation.update_data_product(None, moved)
        assert (result.success, result.code) == (False, "data_product.members.domain_has_members")
        async with db.acquire() as conn:
            assert (await repo.get(conn, "c360"))["domain_id"] == "sales"  # nothing written


def test_a_table_or_command_declaring_product_id_is_refused_by_name():
    with pytest.raises(ValueError, match="declared on the product"):
        Table.model_validate(
            {
                "source_id": "s",
                "domain_id": "sales",
                "table": "orders",
                "product_id": "c360",
                "columns": [],
            }
        )
    with pytest.raises(ValueError, match="declared on the product"):
        Function.model_validate(
            {
                "name": "refresh",
                "source_id": "s",
                "function_name": "refresh",
                "returns": "",
                "product_id": "c360",
            }
        )


def _export_config() -> ProvisaConfig:
    def table(name: str) -> Table:
        return Table(
            source_id="wh",
            domain_id="sales",
            schema_name="public",
            table_name=name,
            columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
        )

    return ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql)],
        domains=[Domain(id="sales", description="Sales", steward="data-steward")],
        tables=[table("customers"), table("orders")],
        data_products=[
            DataProduct(id="c360", domain_id="sales", name="C360", tables=["customers", "orders"]),
            DataProduct(id="churn", domain_id="sales", name="Churn", tables=["customers"]),
        ],
        roles=[],
    )


def test_an_export_lists_every_product_a_table_belongs_to():
    snapshot = build_snapshot(_export_config(), org_id="acme", dialect="postgres", contexts={})

    by_table = {t.name: t for t in snapshot.tables}
    assert by_table["customers"].product_ids == ("c360", "churn")
    assert by_table["orders"].product_ids == ("c360",)
    assert by_table["customers"].data_product is True
    members = {p.id: sorted(m.fqn() for m in p.members) for p in snapshot.data_products}
    assert members == {
        "c360": ["wh.public.customers", "wh.public.orders"],
        "churn": ["wh.public.customers"],
    }


def test_openlineage_carries_every_product_on_a_member_table():
    snapshot = build_snapshot(_export_config(), org_id="acme", dialect="postgres", contexts={})
    events = to_events(snapshot, event_time=datetime(2026, 11, 7, tzinfo=timezone.utc))
    customers = next(
        e for e in events if e.payload.get("dataset", {}).get("name") == "wh.public.customers"
    )
    facet = customers.payload["dataset"]["facets"]["provisa_data_products"]
    assert [p["productId"] for p in facet["products"]] == [
        "c360",
        "churn",
    ]


def test_an_export_refuses_a_product_listing_a_table_it_does_not_hold():
    config = _export_config()
    config.data_products[1].tables.append("nowhere")
    with pytest.raises(ValueError, match="churn.*nowhere"):
        build_snapshot(config, org_id="acme", dialect="postgres", contexts={})


def test_a_configuration_listing_a_member_of_another_domain_is_refused_as_it_is_read():
    config = _export_config()
    finance = Table(
        source_id="wh",
        domain_id="finance",
        schema_name="public",
        table_name="ledger",
        columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
    )
    with pytest.raises(ValueError, match="'c360' lists table 'ledger'.*finance"):
        ProvisaConfig(
            sources=config.sources,
            domains=[*config.domains, Domain(id="finance", description="Finance")],
            tables=[*config.tables, finance],
            data_products=[
                DataProduct(id="c360", domain_id="sales", name="C360", tables=["ledger"])
            ],
            roles=[],
        )
