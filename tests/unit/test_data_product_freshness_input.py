# Copyright (c) 2026 Kenneth Stott
# Canary: 40831fa1-7ebd-4f02-a2fb-53ff8700ac66
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (amended 2026-11-11, CONTRACTS ARE DERIVED FROM THE OFFERS): what all of a product's
contracts share is written once, on the product -- its freshness among it. The admin API takes it
on the product's input and answers it on the product, so it round-trips."""

# Requirements: REQ-2180

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import schema_mutation, schema_query
from provisa.api.admin.types import DataProductInput
from provisa.core.database import Database
from provisa.core.schema_org import data_products, domains

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales", description="Sales"))

    async def _pool():
        return db

    from provisa.api.admin import capabilities

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    monkeypatch.setattr(schema_query, "_resolve_admin_context", lambda *_: None)
    monkeypatch.setattr(capabilities, "require_capability", lambda *_: None)
    monkeypatch.setattr(capabilities, "require_right_in_domains", lambda *_: None)
    monkeypatch.setattr(capabilities, "_identity_from_info", lambda *_: None)
    # The products' findings (REQ-2146) read the org's runtime settings; none are asked here.
    from provisa.core import product_findings

    async def _none(*_: Any) -> Any:
        return {}

    monkeypatch.setattr(product_findings, "context", _none)
    monkeypatch.setattr(product_findings, "findings", _none)
    return db


def _input(freshness: int | None) -> DataProductInput:
    return DataProductInput(id="orders", domain_id="sales", name="Orders", freshness=freshness)


async def _stored(db: Database) -> int | None:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(data_products.c.freshness).where(data_products.c.id == "orders")
            )
        ).scalar_one()


async def _read() -> int | None:
    query: Any = schema_query.Query()
    (product,) = [p for p in await query.data_products(None) if p.id == "orders"]
    return product.freshness


async def test_freshness_round_trips_through_the_products_input(plane):
    mutation: Any = schema_mutation.Mutation()
    created = await mutation.create_data_product(None, _input(900))
    assert created.success, created.message
    assert await _stored(plane) == 900
    assert await _read() == 900

    updated = await mutation.update_data_product(None, _input(60))
    assert updated.success, updated.message
    assert await _read() == 60

    # None: each dataset's own freshness, as the product states none.
    cleared = await mutation.update_data_product(None, _input(None))
    assert cleared.success, cleared.message
    assert await _stored(plane) is None
    assert await _read() is None
