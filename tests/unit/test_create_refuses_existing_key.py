# Copyright (c) 2026 Kenneth Stott
# Canary: 8f3b6d27-1e94-4c58-a0b7-3d9e2c6f5a81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A create makes a new object: a key that exists is refused by name before anything is written,
and the store is unchanged. A create used to overwrite the object holding the key and report it
created; changing an object is its update."""

# Requirements: REQ-021, REQ-1634, REQ-1373

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import schema_mutation
from provisa.api.admin.types import DataProductInput, DomainInput, TagInput
from provisa.core.database import Database
from provisa.core.schema_org import data_products, domains, tags

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(domains).values(id="sales", description="The sales team's data")
        )
        await conn.execute_core(
            insert(data_products).values(id="orders_product", domain_id="sales", name="Orders")
        )
        await conn.execute_core(
            insert(tags).values(id="regional", description="By region", applies_to=["table"])
        )

    async def _pool():
        return db

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    from provisa.api.admin import capabilities

    monkeypatch.setattr(capabilities, "require_capability", lambda _info, _cap: None)
    monkeypatch.setattr(capabilities, "require_right_in_domains", lambda _i, _r, _d: None)
    # REQ-2155: the data product gate reads the caller from the resolver info; none is dev mode.
    monkeypatch.setattr(capabilities, "_identity_from_info", lambda _info: None)
    return db


async def _rows(db: Database, table) -> list[tuple]:
    async with db.acquire() as conn:
        return [tuple(r) for r in (await conn.execute_core(select(table))).fetchall()]


async def test_a_domain_create_on_an_existing_id_is_refused_and_writes_nothing(plane):
    before = await _rows(plane, domains)
    mutation: Any = schema_mutation.Mutation()
    result = await mutation.create_domain(None, DomainInput(id="sales", description="overwritten"))
    assert (result.success, result.code, result.params) == (
        False,
        "schema.domain_id_taken",
        {"domain": "sales"},
    )
    assert await _rows(plane, domains) == before


async def test_a_domain_create_on_a_new_id_creates_it(plane):
    mutation: Any = schema_mutation.Mutation()
    result = await mutation.create_domain(None, DomainInput(id="finance", description="Money"))
    assert (result.success, result.code) == (True, "schema.domain_created")
    assert {"sales", "finance"} <= {r[0] for r in await _rows(plane, domains)}


def _product(product_id: str, name: str) -> DataProductInput:
    return DataProductInput(id=product_id, domain_id="sales", name=name)


async def test_a_data_product_create_on_an_existing_id_is_refused_and_writes_nothing(plane):
    before = await _rows(plane, data_products)
    mutation: Any = schema_mutation.Mutation()
    result = await mutation.create_data_product(None, _product("orders_product", "Overwritten"))
    assert (result.success, result.code, result.params) == (
        False,
        "schema.data_product_id_taken",
        {"data_product": "orders_product"},
    )
    assert await _rows(plane, data_products) == before


async def test_a_data_product_update_changes_one_that_exists_and_refuses_one_that_does_not(plane):
    mutation: Any = schema_mutation.Mutation()
    result = await mutation.update_data_product(None, _product("orders_product", "Orders, all"))
    assert (result.success, result.code) == (True, "schema.data_product_updated")
    before = await _rows(plane, data_products)
    assert ("Orders, all") in {r[2] for r in before}
    result = await mutation.update_data_product(None, _product("missing_product", "Nothing"))
    assert (result.success, result.code) == (False, "schema.data_product_not_found")
    assert await _rows(plane, data_products) == before


async def test_a_data_product_update_saves_its_support_contact(plane):
    # REQ-1635: the support contact a catalog listing requires is set on the product's form.
    mutation: Any = schema_mutation.Mutation()
    product = _product("orders_product", "Orders")
    product.support_contact = "data-team@acme.example"
    result = await mutation.update_data_product(None, product)
    assert (result.success, result.code) == (True, "schema.data_product_updated")
    async with plane.acquire() as conn:
        stored = (
            await conn.execute_core(
                select(data_products.c.support_contact).where(
                    data_products.c.id == "orders_product"
                )
            )
        ).scalar_one()
    assert stored == "data-team@acme.example"


async def test_a_tag_create_on_an_existing_id_is_refused_and_an_update_changes_it(
    plane, monkeypatch
):
    async def _nothing():
        return None

    monkeypatch.setattr(schema_mutation, "_require_tag_editor", lambda _info: None)
    monkeypatch.setattr(schema_mutation, "require_capability", lambda *_a, **_k: None)
    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _nothing)
    mutation: Any = schema_mutation.Mutation()
    before = await _rows(plane, tags)
    taken = await mutation.create_tag(
        None, TagInput(id="regional", description="Overwritten", applies_to=["column"])
    )
    assert (taken.success, taken.code, taken.params) == (
        False,
        "schema.tag_id_taken",
        {"tag": "regional"},
    )
    assert await _rows(plane, tags) == before
    missing = await mutation.update_tag(
        None, TagInput(id="nowhere", description="", applies_to=["table"])
    )
    assert (missing.success, missing.code) == (False, "schema.tag_not_found")
    assert await _rows(plane, tags) == before
    updated = await mutation.update_tag(
        None, TagInput(id="regional", description="By sales region", applies_to=["table"])
    )
    assert (updated.success, updated.code) == (True, "schema.tag_saved"), updated.message
