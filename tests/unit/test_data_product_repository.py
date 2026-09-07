# Copyright (c) 2026 Kenneth Stott
# Canary: 9b3d5e17-4a2c-4f6b-8e9d-1c7a0f3b6d84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1634: DataProduct repository CRUD, domain-scoped, against a real SQLite tenant DB."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.models import DataProduct
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.schema_org import data_products, domains

_TABLES = [domains, data_products]


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'dp.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: domains.metadata.create_all(s, tables=_TABLES))
        await c.execute(domains.insert().values(id="sales", description="Sales"))
        await c.execute(domains.insert().values(id="finance", description="Finance"))
    try:
        async with Database(engine, name="dp").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_upsert_then_get_round_trips_all_fields(tmp_path):
    async with _conn(tmp_path) as conn:
        await data_product_repo.upsert(
            conn,
            DataProduct(
                id="checkout", domain_id="sales", name="Checkout", owner_role="alice", purpose="d"
            ),
        )

        row = await data_product_repo.get(conn, "checkout")

        assert row is not None
        assert row["domain_id"] == "sales"
        assert row["name"] == "Checkout"
        assert row["owner_role"] == "alice"
        assert row["purpose"] == "d"


@pytest.mark.asyncio
async def test_get_missing_returns_none(tmp_path):
    async with _conn(tmp_path) as conn:
        assert await data_product_repo.get(conn, "missing") is None


@pytest.mark.asyncio
async def test_upsert_is_idempotent_by_id(tmp_path):
    async with _conn(tmp_path) as conn:
        await data_product_repo.upsert(
            conn, DataProduct(id="checkout", domain_id="sales", name="Checkout")
        )
        await data_product_repo.upsert(
            conn, DataProduct(id="checkout", domain_id="sales", name="Checkout Renamed")
        )

        rows = await data_product_repo.list_all(conn)

        assert [r["id"] for r in rows] == ["checkout"]
        assert rows[0]["name"] == "Checkout Renamed"


@pytest.mark.asyncio
async def test_list_by_domain_only_returns_that_domains_products(tmp_path):
    async with _conn(tmp_path) as conn:
        await data_product_repo.upsert(
            conn, DataProduct(id="checkout", domain_id="sales", name="Checkout")
        )
        await data_product_repo.upsert(
            conn, DataProduct(id="ledger", domain_id="finance", name="Ledger")
        )

        sales_products = await data_product_repo.list_by_domain(conn, "sales")

        assert [r["id"] for r in sales_products] == ["checkout"]


@pytest.mark.asyncio
async def test_delete_removes_the_row_and_reports_whether_one_existed(tmp_path):
    async with _conn(tmp_path) as conn:
        await data_product_repo.upsert(
            conn, DataProduct(id="checkout", domain_id="sales", name="Checkout")
        )

        assert await data_product_repo.delete(conn, "checkout") is True
        assert await data_product_repo.get(conn, "checkout") is None
        assert await data_product_repo.delete(conn, "checkout") is False
