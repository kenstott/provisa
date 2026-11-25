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

from provisa.core.models import DataProduct
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.schema_org import domains


@asynccontextmanager
async def _conn(control_plane):
    async with control_plane.org().acquire() as conn:
        await conn.execute_core(domains.insert().values(id="sales", description="Sales"))
        await conn.execute_core(domains.insert().values(id="finance", description="Finance"))
        yield conn


@pytest.mark.asyncio
async def test_upsert_then_get_round_trips_all_fields(control_plane):
    async with _conn(control_plane) as conn:
        await data_product_repo.upsert(
            conn,
            DataProduct(
                id="checkout", domain_id="sales", name="Checkout", team_role="alice", purpose="d"
            ),
        )

        row = await data_product_repo.get(conn, "checkout")

        assert row is not None
        assert row["domain_id"] == "sales"
        assert row["name"] == "Checkout"
        assert row["team_role"] == "alice"
        assert "owner_role" not in row  # REQ-2219: owners are entitlement rows
        assert row["purpose"] == "d"


@pytest.mark.asyncio
async def test_get_missing_returns_none(control_plane):
    async with _conn(control_plane) as conn:
        assert await data_product_repo.get(conn, "missing") is None


@pytest.mark.asyncio
async def test_upsert_is_idempotent_by_id(control_plane):
    async with _conn(control_plane) as conn:
        await data_product_repo.upsert(
            conn, DataProduct(id="checkout", domain_id="sales", name="Checkout")
        )
        await data_product_repo.upsert(
            conn,
            DataProduct(id="checkout", domain_id="sales", name="Checkout Renamed"),
        )

        rows = await data_product_repo.list_all(conn)

        assert [r["id"] for r in rows] == ["checkout"]
        assert rows[0]["name"] == "Checkout Renamed"


@pytest.mark.asyncio
async def test_list_by_domain_only_returns_that_domains_products(control_plane):
    async with _conn(control_plane) as conn:
        await data_product_repo.upsert(
            conn, DataProduct(id="checkout", domain_id="sales", name="Checkout")
        )
        await data_product_repo.upsert(
            conn, DataProduct(id="ledger", domain_id="finance", name="Ledger")
        )

        sales_products = await data_product_repo.list_by_domain(conn, "sales")

        assert [r["id"] for r in sales_products] == ["checkout"]


@pytest.mark.asyncio
async def test_delete_removes_the_row_and_reports_whether_one_existed(control_plane):
    async with _conn(control_plane) as conn:
        await data_product_repo.upsert(
            conn, DataProduct(id="checkout", domain_id="sales", name="Checkout")
        )

        assert await data_product_repo.delete(conn, "checkout") is True
        assert await data_product_repo.get(conn, "checkout") is None
        assert await data_product_repo.delete(conn, "checkout") is False
