# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2c5a91-3d47-4e6b-a0c8-1b9e7d4f6a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2267: the relationships a code source's @discover declares are written to the semantic
layer once both of their tables are registered, and offered by relationship discovery as a
declared foreign key is."""

from __future__ import annotations

import contextlib
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import _code_source_relationships as code_rels
from provisa.codesource import discovery as code_discovery
from provisa.codesource.manifest import Relationship
from provisa.core import secrets_store
from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    sources,
    table_columns,
)

pytestmark = pytest.mark.asyncio

_SOURCE = "shop"

_ORDER_CUSTOMER = Relationship(
    name="order_customer",
    source_table="orders",
    source_column="customer_id",
    target_table="customers",
    target_column="id",
    cardinality="many-to-one",
)
_CUSTOMER_ORDERS = Relationship(
    name="customer_orders",
    source_table="customers",
    source_column="id",
    target_table="orders",
    target_column="customer_id",
    cardinality="one-to-many",
)
_ORDER_REFUNDS = Relationship(
    name="order_refunds",
    source_table="orders",
    source_column="id",
    target_table="refunds",
    target_column="order_id",
    cardinality="one-to-many",
)
_DECLARED = (_ORDER_CUSTOMER, _CUSTOMER_ORDERS, _ORDER_REFUNDS)

_COLUMNS = {
    "orders": ("id", "customer_id"),
    "customers": ("id", "name"),
    "refunds": ("id", "order_id"),
}


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


@pytest.fixture
def declared(monkeypatch):
    """The source's code declares _DECLARED; each read of it is counted by source."""
    reads: list[str] = []

    async def relationships_of(state, source_id):
        reads.append(source_id)
        return _DECLARED if source_id == _SOURCE else None

    monkeypatch.setattr(code_discovery, "relationships", relationships_of)
    return reads


@pytest.fixture
async def org_plane(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(insert(sources).values(id=_SOURCE, type="python"))
    return db


async def _register(db, table_name: str) -> int:
    async with db.acquire() as conn:
        table_id = (
            await conn.execute_core(
                insert(registered_tables)
                .values(
                    source_id=_SOURCE,
                    domain_id="sales",
                    schema_name="default",
                    table_name=table_name,
                )
                .returning(registered_tables.c.id)
            )
        ).scalar_one()
        for column in _COLUMNS[table_name]:
            await conn.execute_core(
                insert(table_columns).values(
                    table_id=table_id, column_name=column, data_type="integer"
                )
            )
    return table_id


async def _stored(db) -> dict[str, tuple]:
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(
                    relationships.c.id,
                    relationships.c.source_table_id,
                    relationships.c.source_column,
                    relationships.c.target_table_id,
                    relationships.c.target_column,
                    relationships.c.cardinality,
                )
            )
        ).fetchall()
    return {r.id: tuple(r[1:]) for r in rows}


_STATE = SimpleNamespace(config=None)


async def test_written_once_both_tables_are_registered(org_plane, declared):
    orders = await _register(org_plane, "orders")
    customers = await _register(org_plane, "customers")
    written = await code_rels.store_declared_relationships(org_plane, _STATE, _SOURCE, "customers")
    assert written == 2
    assert await _stored(org_plane) == {
        "shop__order_customer": (orders, "customer_id", customers, "id", "many-to-one"),
        "shop__customer_orders": (customers, "id", orders, "customer_id", "one-to-many"),
    }


async def test_a_table_registered_before_its_partner_is_related_when_the_partner_is(
    org_plane, declared
):
    orders = await _register(org_plane, "orders")
    assert await code_rels.store_declared_relationships(org_plane, _STATE, _SOURCE, "orders") == 0
    assert await _stored(org_plane) == {}
    customers = await _register(org_plane, "customers")
    await code_rels.store_declared_relationships(org_plane, _STATE, _SOURCE, "customers")
    assert await _stored(org_plane) == {
        "shop__order_customer": (orders, "customer_id", customers, "id", "many-to-one"),
        "shop__customer_orders": (customers, "id", orders, "customer_id", "one-to-many"),
    }


async def test_a_relationship_to_a_table_never_registered_is_not_written(org_plane, declared):
    await _register(org_plane, "orders")
    await _register(org_plane, "customers")
    await code_rels.store_declared_relationships(org_plane, _STATE, _SOURCE, "orders")
    assert "shop__order_refunds" not in await _stored(org_plane)


async def test_a_registration_writes_only_the_relationships_its_table_completes():
    registered = {"orders": "orders", "customers": "clients", "refunds": "refunds"}
    completed = code_rels.completed_relationships(_SOURCE, _DECLARED, registered, "refunds")
    assert completed == [
        {
            "id": "shop__order_refunds",
            "source_table_id": "orders",
            "target_table_id": "refunds",
            "source_column": "id",
            "target_column": "order_id",
            "cardinality": "one-to-many",
        }
    ]
    # A table is named to the upsert as the model knows it: its alias.
    by_alias = code_rels.completed_relationships(_SOURCE, _DECLARED, registered, "customers")
    assert {r["target_table_id"] for r in by_alias if r["id"] == "shop__order_customer"} == {
        "clients"
    }


async def test_discovery_offers_a_declared_relationship_not_stored(org_plane):
    from provisa.discovery.collector import collect_fk_for_tables

    orders = await _register(org_plane, "orders")
    customers = await _register(org_plane, "customers")

    async def relationships_of(source_id):
        assert source_id == _SOURCE
        return _DECLARED

    async with org_plane.acquire() as conn:
        candidates = await collect_fk_for_tables(
            SimpleNamespace(),
            {_SOURCE: "python"},
            conn,
            {orders, customers},
            code_relationships=relationships_of,
        )
    # Each is offered from its many side, named as declared; refunds is not registered.
    assert sorted(
        (c.source_table_id, c.source_column, c.target_table_id, c.target_column, c.suggested_name)
        for c in candidates
    ) == sorted(
        [
            (orders, "customer_id", customers, "id", "order_customer"),
            (orders, "customer_id", customers, "id", "customer_orders"),
        ]
    )
    assert {(c.cardinality, c.confidence) for c in candidates} == {("many-to-one", 1.0)}


async def test_discovery_does_not_offer_a_declared_relationship_already_stored(org_plane, declared):
    from provisa.discovery.collector import collect_fk_for_tables

    orders = await _register(org_plane, "orders")
    customers = await _register(org_plane, "customers")
    await code_rels.store_declared_relationships(org_plane, _STATE, _SOURCE, "customers")

    async with org_plane.acquire() as conn:
        candidates = await collect_fk_for_tables(
            SimpleNamespace(),
            {_SOURCE: "python"},
            conn,
            {orders, customers},
            code_relationships=lambda source_id: code_discovery.relationships(_STATE, source_id),
        )
    assert candidates == []
