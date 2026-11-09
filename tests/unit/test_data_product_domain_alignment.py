# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2b6f4a-9c1d-4b8e-a3f0-5d6c8e1a9b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.


"""REQ-1634: a data product's members all share its domain. A table joins only products of its own
domain (data_product.set_members, the one membership write), and a member's save -- the last write
gate, covering config load, admin GraphQL and introspection -- may not move it out of the domain
of any product it is a member of."""

import pytest
from sqlalchemy import insert, select

from provisa.core.database import Database
from provisa.core.models import Column, DataProduct, Table
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables, sources


def _table(*, domain_id: str) -> Table:
    return Table(
        source_id="s",
        domain_id=domain_id,
        schema_name="public",
        table_name="orders",
        columns=[Column(name="id", data_type="integer", visible_to=["analyst"])],
    )


@pytest.fixture
async def plane(control_plane) -> Database:
    """sales holding checkout, finance holding ledger, and orders registered in sales."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="s", type="postgresql"))
        for domain in ("sales", "finance"):
            await conn.execute_core(insert(domains).values(id=domain))
        await data_product_repo.upsert(
            conn, DataProduct(id="checkout", domain_id="sales", name="Checkout")
        )
        await data_product_repo.upsert(
            conn, DataProduct(id="ledger", domain_id="finance", name="Ledger")
        )
        await table_repo.upsert(conn, _table(domain_id="sales"))
    return db


async def _orders(conn) -> int:
    return (
        await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.table_name == "orders")
        )
    ).scalar_one()


@pytest.mark.asyncio
async def test_a_table_joining_a_product_in_a_different_domain_is_refused(plane):
    async with plane.acquire() as conn:
        with pytest.raises(
            data_product_repo.MembershipRefused, match="sales.*finance|finance.*sales"
        ):
            await data_product_repo.set_members(
                conn, "ledger", table_ids=[await _orders(conn)], commands=[]
            )
        assert await data_product_repo.member_table_ids(conn, "ledger") == []


@pytest.mark.asyncio
async def test_a_nonexistent_product_is_refused(plane):
    async with plane.acquire() as conn:
        with pytest.raises(data_product_repo.MembershipRefused, match="missing"):
            await data_product_repo.set_members(
                conn, "missing", table_ids=[await _orders(conn)], commands=[]
            )


@pytest.mark.asyncio
async def test_a_table_joins_a_product_in_its_own_domain(plane):
    async with plane.acquire() as conn:
        table_id = await _orders(conn)
        await data_product_repo.set_members(conn, "checkout", table_ids=[table_id], commands=[])
        assert await data_product_repo.products_of_table(conn, table_id) == ["checkout"]


@pytest.mark.asyncio
async def test_a_member_saved_into_another_domain_is_refused(plane):
    async with plane.acquire() as conn:
        table_id = await _orders(conn)
        await data_product_repo.set_members(conn, "checkout", table_ids=[table_id], commands=[])
        with pytest.raises(data_product_repo.MembershipRefused, match="take it out"):
            await table_repo.upsert(conn, _table(domain_id="finance"))
        # Saved in its own domain, it is accepted.
        await table_repo.upsert(conn, _table(domain_id="sales"))


@pytest.mark.asyncio
async def test_a_table_in_no_product_moves_domain_freely(plane):
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table(domain_id="finance"))
        domain = (
            await conn.execute_core(
                select(registered_tables.c.domain_id).where(
                    registered_tables.c.table_name == "orders"
                )
            )
        ).scalar_one()
        assert domain == "finance"
