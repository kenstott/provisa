# Copyright (c) 2026 Kenneth Stott
# Canary: e640dbf4-d1e6-4007-ba4b-cf7b76a9ccd0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product is blocked by its members; a tag takes its assignments with it (REQ-1918).

Deleting a data product used to detach its member tables and commands on PostgreSQL and leave
them naming a product that was gone on SQLite. It is now refused, naming each member, until
they are taken out of it. A tag's assignments and parameter values are its parts and go with
it, in one transaction.
"""

# Requirements: REQ-1918, REQ-1919, REQ-1634, REQ-1467

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import (
    data_products,
    domains,
    registered_tables,
    sources,
    tag_assignments,
    tag_param_values,
    tags,
    tracked_functions,
)


@pytest.fixture
async def plane(monkeypatch) -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="product-delete-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql", origin="admin"))
        await conn.execute_core(insert(domains).values(id="sales", origin="admin"))
        for product_id in ("orders_product", "empty_product"):
            await conn.execute_core(
                insert(data_products).values(
                    id=product_id, domain_id="sales", name=product_id, origin="admin"
                )
            )
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="pg",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                product_id="orders_product",
                origin="admin",
            )
        )
        await conn.execute_core(
            insert(tracked_functions).values(
                name="refund", domain_id="sales", product_id="orders_product", origin="admin"
            )
        )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    return db


async def _count(db: Database, table) -> int:
    async with db.acquire() as conn:
        return (await conn.execute_core(select(func.count()).select_from(table))).scalar_one()


async def test_a_data_product_with_members_is_refused_naming_each(plane):
    async with plane.acquire() as conn:
        table_id = (await conn.execute_core(select(registered_tables.c.id))).scalar_one()
        with pytest.raises(data_product_repo.DataProductDeleteRefused) as err:
            await data_product_repo.delete(conn, "orders_product")
        still = (await conn.execute_core(select(registered_tables.c.product_id))).scalar_one()
    assert [(d.ref.kind, d.ref.id) for d in err.value.dependents] == [
        ("command", "refund"),
        ("table", table_id),
    ]
    assert still == "orders_product" and await _count(plane, data_products) == 2


async def test_a_data_product_with_no_members_goes_with_its_tags(plane):
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(tag_assignments).values(
                tag_id="gold",
                base_tag_id="gold",
                object_type="product",
                object_key="empty_product",
                product_id="empty_product",
                origin="admin",
            )
        )
        assert await data_product_repo.delete(conn, "empty_product") is True
        assert await data_product_repo.delete(conn, "empty_product") is False
    assert await _count(plane, tag_assignments) == 0 and await _count(plane, data_products) == 1


async def test_the_mutation_refuses_with_the_members(plane, monkeypatch):
    async def _pool():
        return plane

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    info: Any = types.SimpleNamespace(context={"request": request})
    result = await schema_mutation.Mutation().delete_data_product(info, "orders_product")
    assert (result.success, result.code) == (False, "schema.data_product_has_dependents")
    assert {d["kind"] for d in result.params["dependents"]} == {"command", "table"}
    assert await _count(plane, data_products) == 2


async def test_a_tag_takes_its_assignments_and_values_with_it(plane):
    async with plane.acquire() as conn:
        for tag_id in ("pii", "gold"):
            await conn.execute_core(insert(tags).values(id=tag_id, origin="admin"))
        await conn.execute_core(insert(tag_param_values).values(tag_id="pii", value="high"))
        for tag_id, base in (("pii:high", "pii"), ("pii", "pii"), ("gold", "gold")):
            await conn.execute_core(
                insert(tag_assignments).values(
                    tag_id=tag_id,
                    base_tag_id=base,
                    object_type="table",
                    object_key=f"orders-{tag_id}",
                    origin="admin",
                )
            )
        # What the confirmation shows before a tag that carries a policy is deleted.
        assert await tag_repo.assignment_count(conn, "pii") == 2
        assert await tag_repo.delete(conn, "pii:high") is True  # any value names the tag
        assert await tag_repo.delete(conn, "pii") is False
        left = (await conn.execute_core(select(tag_assignments.c.base_tag_id))).fetchall()
    assert [r[0] for r in left] == ["gold"]
    assert await _count(plane, tag_param_values) == 0 and await _count(plane, tags) == 1
