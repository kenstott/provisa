# Copyright (c) 2026 Kenneth Stott
# Canary: 82ff8502-c65a-4bb5-b231-2b30d0f3d663
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship goes before the tables it joins, unless a published view relies on it (REQ-1918).

Nothing refers to a relationship by its id, so it can always be deleted before its tables. One
thing does rely on it: a materialized view is published only over approved relationships
(REQ-1140), matched by the joined tables and columns. While such a view joins on a
relationship's columns and no other relationship approves that join, deleting the relationship
is refused naming the view.
"""

# Requirements: REQ-1918, REQ-1919, REQ-1140, REQ-019

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.repositories import relationship as rel_repo
from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    sources,
    tag_assignments,
)

JOIN = "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id"


@pytest.fixture
async def plane(monkeypatch) -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="rel-delete-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
    for name in ("orders", "customers"):
        await _table(db, name)
    await _relate(db, "orders_customers", "orders", "customer_id", "customers", "id")
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", {}, raising=False)
    return db


async def _table(db: Database, name: str, view_sql: str | None = None, materialize=False) -> int:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="pg",
                domain_id="sales",
                schema_name="public",
                table_name=name,
                view_sql=view_sql,
                materialize=materialize,
            )
        )
        return (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == name)
            )
        ).scalar_one()


async def _id(db: Database, name: str) -> int:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == name)
            )
        ).scalar_one()


async def _relate(db: Database, rel_id: str, source: str, column: str, target: str, key: str):
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(relationships).values(
                id=rel_id,
                source_table_id=await _id(db, source),
                target_table_id=await _id(db, target),
                source_column=column,
                target_column=key,
                cardinality="many-to-one",
            )
        )


async def _relationship_ids(db: Database) -> set[str]:
    async with db.acquire() as conn:
        return {r[0] for r in (await conn.execute_core(select(relationships.c.id))).fetchall()}


async def test_a_relationship_nothing_is_published_over_goes_with_its_tags(plane):
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(tag_assignments).values(
                tag_id="pii",
                base_tag_id="pii",
                object_type="relationship",
                object_key="orders_customers",
                relationship_id="orders_customers",
            )
        )
        assert await rel_repo.delete(conn, "orders_customers") is True
        assert await rel_repo.delete(conn, "orders_customers") is False
        left = (
            await conn.execute_core(select(func.count()).select_from(tag_assignments))
        ).scalar_one()
    assert left == 0 and await _relationship_ids(plane) == set()


async def test_a_view_that_is_not_materialized_does_not_block(plane):
    await _table(plane, "order_names", JOIN)
    async with plane.acquire() as conn:
        assert await rel_repo.delete(conn, "orders_customers") is True


async def test_a_published_view_that_joins_over_it_blocks_and_is_named(plane):
    view = await _table(plane, "order_names", JOIN, materialize=True)
    async with plane.acquire() as conn:
        with pytest.raises(rel_repo.RelationshipDeleteRefused) as err:
            await rel_repo.delete(conn, "orders_customers")
    assert [(d.ref.kind, d.ref.id, d.via) for d in err.value.dependents] == [
        ("table", view, ("registered_tables.view_sql",))
    ]
    assert await _relationship_ids(plane) == {"orders_customers"}


async def test_another_relationship_approving_the_same_join_frees_it(plane):
    await _table(plane, "order_names", JOIN, materialize=True)
    # The same join, declared from the other side.
    await _relate(plane, "customers_orders", "customers", "id", "orders", "customer_id")
    async with plane.acquire() as conn:
        assert await rel_repo.delete(conn, "orders_customers") is True
        # The one left is now the only approval of the view's join.
        with pytest.raises(rel_repo.RelationshipDeleteRefused):
            await rel_repo.delete(conn, "customers_orders")


async def test_a_published_view_joining_on_other_columns_does_not_block(plane):
    await _table(
        plane,
        "by_name",
        "SELECT o.id FROM orders o JOIN customers c ON o.note = c.name",
        materialize=True,
    )
    async with plane.acquire() as conn:
        assert await rel_repo.delete(conn, "orders_customers") is True


async def test_a_declared_set_is_removed_without_the_guard(plane):
    await _table(plane, "order_names", JOIN, materialize=True)
    async with plane.acquire() as conn:
        await rel_repo.remove_where(conn, relationships.c.id.notlike("meta:%"))
    assert await _relationship_ids(plane) == set()


# --- the mutation --------------------------------------------------------------------------------


@pytest.fixture
def served(plane, monkeypatch) -> list[int]:
    rebuilds: list[int] = []

    async def _pool():
        return plane

    async def _rebuild() -> None:
        rebuilds.append(1)

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    return rebuilds


async def _delete_relationship(rel_id: str) -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    info = types.SimpleNamespace(context={"request": request})
    return await schema_mutation.Mutation().delete_relationship(info, rel_id)  # type: ignore[arg-type]


async def test_the_mutation_refuses_with_the_view_and_removes_nothing(plane, served):
    view = await _table(plane, "order_names", JOIN, materialize=True)
    result = await _delete_relationship("orders_customers")
    assert (result.success, result.code) == (False, "schema.relationship_has_dependents")
    assert result.params == {
        "relationship": "orders_customers",
        "dependents": [{"kind": "table", "id": view, "via": ["registered_tables.view_sql"]}],
    }
    assert await _relationship_ids(plane) == {"orders_customers"} and served == []


async def test_the_mutation_deletes_and_rebuilds(plane, served):
    result = await _delete_relationship("orders_customers")
    assert (result.success, result.code) == (True, "schema.relationship_deleted")
    assert await _relationship_ids(plane) == set() and served == [1]
