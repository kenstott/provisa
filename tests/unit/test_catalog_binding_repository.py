# Copyright (c) 2026 Kenneth Stott
# Canary: 0c5e9d21-7f43-4b8a-b1d6-2e94a70c3f85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1389: the catalog_bindings repository round-trips vendor identities portably.

A real SQLite control plane — the upsert/delete semantics under test are the store's own.
"""

# Requirements: REQ-1389

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.repositories import catalog_binding
from provisa.core.schema_org import catalog_bindings

URI_ORDERS = "provisa://acme/sales/tables/orders"
URI_TOTALS = "provisa://acme/sales/tables/order_totals"


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'bindings.db'}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: catalog_bindings.metadata.create_all(s, tables=[catalog_bindings])
        )
    try:
        async with Database(engine, name="bindings").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_bindings_round_trip_per_provider(tmp_path):
    async with _conn(tmp_path) as conn:
        await catalog_binding.upsert_bindings(
            conn,
            "atlas",
            {
                URI_ORDERS: ("guid-1", "wh.public.orders@provisa"),
                URI_TOTALS: ("guid-2", "wh.public.order_totals@provisa"),
            },
        )
        await catalog_binding.upsert_bindings(
            conn, "datahub", {URI_ORDERS: ("urn:li:dataset:x", "wh.public.orders")}
        )

        atlas = await catalog_binding.load_bindings(conn, "atlas")
        assert atlas == {
            URI_ORDERS: ("guid-1", "wh.public.orders@provisa"),
            URI_TOTALS: ("guid-2", "wh.public.order_totals@provisa"),
        }
        # Providers do not see each other's bindings: one asset has one identity PER catalog.
        assert await catalog_binding.load_bindings(conn, "datahub") == {
            URI_ORDERS: ("urn:li:dataset:x", "wh.public.orders")
        }


@pytest.mark.asyncio
async def test_a_republish_updates_the_binding_in_place(tmp_path):
    async with _conn(tmp_path) as conn:
        await catalog_binding.upsert_bindings(
            conn, "atlas", {URI_ORDERS: ("guid-1", "wh.public.orders@provisa")}
        )
        # The table re-platformed: same URN, new physical key, same catalog guid.
        await catalog_binding.upsert_bindings(
            conn, "atlas", {URI_ORDERS: ("guid-1", "lake.sales.orders@provisa")}
        )
        assert await catalog_binding.load_bindings(conn, "atlas") == {
            URI_ORDERS: ("guid-1", "lake.sales.orders@provisa")
        }


@pytest.mark.asyncio
async def test_remove_stale_bindings_keeps_only_the_published_set(tmp_path):
    async with _conn(tmp_path) as conn:
        await catalog_binding.upsert_bindings(
            conn,
            "atlas",
            {
                URI_ORDERS: ("guid-1", "wh.public.orders@provisa"),
                URI_TOTALS: ("guid-2", "wh.public.order_totals@provisa"),
            },
        )
        await catalog_binding.upsert_bindings(
            conn, "datahub", {URI_TOTALS: ("urn:li:dataset:y", "wh.public.order_totals")}
        )

        removed = await catalog_binding.remove_stale_bindings(conn, "atlas", keep_uris={URI_ORDERS})

        assert removed == 1
        assert await catalog_binding.load_bindings(conn, "atlas") == {
            URI_ORDERS: ("guid-1", "wh.public.orders@provisa")
        }
        # Another provider's bindings are not this provider's staleness to judge.
        assert await catalog_binding.load_bindings(conn, "datahub") == {
            URI_TOTALS: ("urn:li:dataset:y", "wh.public.order_totals")
        }
