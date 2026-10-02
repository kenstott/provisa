# Copyright (c) 2026 Kenneth Stott
# Canary: f81a2ed2-fe17-4235-a3e9-a965fa9032f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table or view is deleted only when nothing refers to it, and a view loop is refused at save (REQ-1918).

The model store's ``table.delete`` asks the dependency guard: while a relationship takes part
in the table, a view reads it, a metric reads it or a command returns it, the delete is refused
naming each one and nothing is removed. When it may go, its parts go with it by explicit
statements — on SQLite, where no foreign key would have removed them. ``table.upsert`` refuses
a view whose SQL would read the view itself through other views.
"""

# Requirements: REQ-1918, REQ-1919, REQ-014

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.models import Column, Table
from provisa.core.repositories import integrity
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    domains,
    metrics,
    registered_tables,
    relationships,
    rls_rules,
    roles,
    sources,
    table_columns,
    tag_assignments,
)


@pytest.fixture
async def plane(monkeypatch) -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="table-delete-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id="seller", capabilities=[], domain_access=["sales"])
        )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    return db


async def _table(db: Database, name: str, view_sql: str | None = None) -> int:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="pg",
                domain_id="sales",
                schema_name="public",
                table_name=name,
                view_sql=view_sql,
            )
        )
        return (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == name)
            )
        ).scalar_one()


async def _count(db: Database, table) -> int:
    async with db.acquire() as conn:
        return (await conn.execute_core(select(func.count()).select_from(table))).scalar_one()


# --- delete --------------------------------------------------------------------------------------


async def test_a_table_something_refers_to_is_refused_naming_each_and_nothing_is_removed(plane):
    orders = await _table(plane, "orders")
    customers = await _table(plane, "customers")
    view = await _table(plane, "open_orders", "SELECT id FROM orders")
    async with plane.acquire() as conn:
        await conn.execute_core(insert(table_columns).values(table_id=orders, column_name="id"))
        await conn.execute_core(
            insert(relationships).values(
                id="orders_customers",
                source_table_id=orders,
                target_table_id=customers,
                source_column="id",
                target_column="id",
                cardinality="many-to-one",
            )
        )
        await conn.execute_core(insert(metrics).values(name="revenue", expression="SUM(orders.a)"))
        with pytest.raises(table_repo.TableDeleteRefused) as err:
            await table_repo.delete(conn, orders)
    assert err.value.name == "orders"
    assert [(d.ref.kind, d.ref.id) for d in err.value.dependents] == [
        ("metric", "revenue"),
        ("relationship", "orders_customers"),
        ("table", view),
    ]
    assert "relationship orders_customers" in str(err.value)
    assert await _count(plane, registered_tables) == 3
    assert await _count(plane, table_columns) == 1


async def test_a_table_nothing_refers_to_goes_with_its_parts(plane):
    orders = await _table(plane, "orders")
    other = await _table(plane, "customers")
    async with plane.acquire() as conn:
        for table_id, column in ((orders, "id"), (orders, "amount"), (other, "id")):
            await conn.execute_core(
                insert(table_columns).values(table_id=table_id, column_name=column)
            )
        await conn.execute_core(
            insert(rls_rules).values(role_id="seller", table_id=orders, filter_expr=b"1=1")
        )
        await conn.execute_core(
            insert(tag_assignments).values(
                tag_id="pii",
                base_tag_id="pii",
                object_type="table",
                object_key="orders",
                table_id=orders,
            )
        )
        assert await table_repo.delete(conn, orders) is True
        assert await table_repo.delete(conn, orders) is False
    assert await _count(plane, table_columns) == 1  # the other table's
    assert await _count(plane, rls_rules) == 0 and await _count(plane, tag_assignments) == 0
    assert await _count(plane, registered_tables) == 1


async def test_a_set_of_registrations_is_removed_without_the_guard(plane):
    await _table(plane, "orders")
    await _table(plane, "open_orders", "SELECT id FROM orders")
    async with plane.acquire() as conn:
        await table_repo.remove_registrations(conn, registered_tables.c.source_id == "pg")
    assert await _count(plane, registered_tables) == 0


# --- a view loop is refused at save --------------------------------------------------------------


async def _loop(db: Database, name: str, sql: str) -> list[str]:
    async with db.acquire() as conn:
        return await integrity.view_loop(conn, name, sql)


async def test_a_view_that_reads_tables_and_other_views_closes_no_loop(plane):
    await _table(plane, "orders")
    await _table(plane, "v_a", "SELECT id FROM orders")
    assert await _loop(plane, "v_b", "SELECT id FROM v_a") == []
    assert await _loop(plane, "v_a", "SELECT id, 1 AS n FROM orders") == []  # re-saving it


async def test_a_view_saved_to_read_a_view_that_reads_it_is_a_loop(plane):
    await _table(plane, "v_a", "SELECT 1 AS id")
    await _table(plane, "v_b", "SELECT id FROM v_a")
    assert await _loop(plane, "v_a", "SELECT id FROM v_b") == ["v_a", "v_b", "v_a"]


async def test_a_longer_loop_names_every_view_in_it(plane):
    await _table(plane, "v_a", "SELECT 1 AS id")
    await _table(plane, "v_b", "SELECT id FROM v_a")
    await _table(plane, "v_c", "SELECT id FROM v_b JOIN v_a USING (id)")
    # v_c reads v_a directly and through v_b; the views are followed in name order, so the
    # shortest loop is the one named.
    assert await _loop(plane, "v_a", "SELECT id FROM v_c") == ["v_a", "v_c", "v_a"]
    assert await _loop(plane, "v_b", "SELECT id FROM v_c") == ["v_b", "v_c", "v_b"]


async def test_a_view_that_reads_itself_is_a_loop(plane):
    assert await _loop(plane, "v_self", "SELECT id FROM v_self") == ["v_self", "v_self"]


async def test_view_sql_that_does_not_parse_is_refused_naming_the_view(plane):
    with pytest.raises(ValueError, match="view 'v_x'"):
        await _loop(plane, "v_x", "SELECT FROM WHERE (")


def _view(name: str, sql: str) -> Table:
    return Table(
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        view_sql=sql,
        columns=[Column(name="id", visible_to=["seller"], data_type="integer")],
    )


async def test_the_write_path_refuses_the_loop_and_writes_nothing(plane):
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _view("v_a", "SELECT 1 AS id"))
        await table_repo.upsert(conn, _view("v_b", "SELECT id FROM v_a"))
        with pytest.raises(table_repo.ViewLoopRefused) as err:
            await table_repo.upsert(conn, _view("v_a", "SELECT id FROM v_b"))
        assert err.value.loop == ["v_a", "v_b", "v_a"]
        assert "v_a -> v_b -> v_a" in str(err.value)
        stored = (
            await conn.execute_core(
                select(registered_tables.c.view_sql).where(registered_tables.c.table_name == "v_a")
            )
        ).scalar_one()
    assert stored == "SELECT 1 AS id"


# --- the mutation --------------------------------------------------------------------------------


@pytest.fixture
def served(plane, monkeypatch) -> list[int]:
    rebuilds: list[int] = []

    async def _pool():
        return plane

    async def _rebuild() -> None:
        rebuilds.append(1)

    async def _invalidate(_state, table_ids) -> int:
        rebuilds.append(-len(list(table_ids)))
        return 0

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr("provisa.cache.tenancy.invalidate_tables", _invalidate)
    monkeypatch.setattr(appmod.state, "hot_manager", None, raising=False)
    return rebuilds


async def _delete_table(table_id: int) -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    info = types.SimpleNamespace(context={"request": request})
    return await schema_mutation.Mutation().delete_table(info, table_id)  # type: ignore[arg-type]


async def test_the_mutation_refuses_with_the_list_and_removes_nothing(plane, served):
    orders = await _table(plane, "orders")
    view = await _table(plane, "open_orders", "SELECT id FROM orders")
    result = await _delete_table(orders)
    assert (result.success, result.code) == (False, "schema.table_has_dependents")
    assert result.params == {
        "table": orders,
        "name": "orders",
        "dependents": [{"kind": "table", "id": view, "via": ["registered_tables.view_sql"]}],
    }
    assert await _count(plane, registered_tables) == 2 and served == []


async def test_the_mutation_deletes_drops_its_cached_results_and_rebuilds(plane, served):
    orders = await _table(plane, "orders")
    result = await _delete_table(orders)
    assert (result.success, result.code) == (True, "schema.table_deleted")
    assert await _count(plane, registered_tables) == 0
    assert served == [-1, 1]  # its response-cache entries invalidated, then the rebuild


async def test_the_mutation_answers_not_found(plane, served):
    result = await _delete_table(4242)
    assert (result.success, result.code) == (False, "schema.table_not_found") and served == []
