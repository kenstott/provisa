# Copyright (c) 2026 Kenneth Stott
# Canary: b79a86c7-ba08-4cda-a54d-27c59355ad48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A re-registration that drops a column something refers to is refused, with the list (REQ-1918).

Registering a table again replaces its columns. A column the new registration does not list is
dropped — and a relationship keyed on it, or a view, materialized view or metric that names it,
was left pointing at nothing. The registration is now refused naming each such column and what
refers to it, and nothing is changed. A dropped column nothing refers to goes, with its tag
assignments.
"""

# Requirements: REQ-1918, REQ-1919

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

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
    sources,
    table_columns,
    tag_assignments,
)


def _table(name: str, *columns: str, view_sql: str | None = None) -> Table:
    return Table(
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        view_sql=view_sql,
        columns=[Column(name=c, visible_to=["analyst"], data_type="integer") for c in columns],
    )


@pytest.fixture
async def plane() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="column-drop-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await table_repo.upsert(conn, _table("orders", "id", "customer_id", "amount", "note"))
        await table_repo.upsert(conn, _table("customers", "id", "name"))
    return db


async def _id(db: Database, name: str) -> int:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == name)
            )
        ).scalar_one()


async def _columns(db: Database, name: str) -> list[str]:
    table_id = await _id(db, name)
    async with db.acquire() as conn:
        rows = await conn.execute_core(
            select(table_columns.c.column_name).where(table_columns.c.table_id == table_id)
        )
        return sorted(r[0] for r in rows.fetchall())


async def _relate(db: Database) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(relationships).values(
                id="orders_customers",
                source_table_id=await _id(db, "orders"),
                target_table_id=await _id(db, "customers"),
                source_column="customer_id",
                target_column="id",
                cardinality="many-to-one",
            )
        )


# --- what refers to a column ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "names"),
    [
        ("SELECT o.amount FROM orders o", True),
        ("SELECT orders.amount FROM orders", True),
        ("SELECT amount FROM orders", True),
        ("SELECT o.id FROM orders o WHERE o.amount > 0", True),
        ("SUM(orders.amount)", True),
        ("SELECT o.id FROM orders o", False),
        ("SELECT c.amount FROM customers c JOIN orders o ON o.id = c.id", False),
        ("SELECT amount FROM orders o JOIN customers c ON o.id = c.id", False),  # ambiguous
        ("SELECT * FROM orders", False),  # a star names no column
        ("SUM(invoices.amount)", False),
    ],
)
def test_whether_sql_names_a_column_of_a_table(sql, names):
    assert integrity._names_column(sql, "orders", "amount") is names


async def test_the_relationships_views_and_metrics_that_name_a_column(plane):
    await _relate(plane)
    async with plane.acquire() as conn:
        await table_repo.upsert(
            conn,
            _table("big_orders", "id", view_sql="SELECT o.id FROM orders o WHERE o.amount > 9"),
        )
        await conn.execute_core(
            insert(metrics).values(name="revenue", expression="SUM(orders.amount)")
        )
        orders = await _id(plane, "orders")
        view = await _id(plane, "big_orders")
        amount = await integrity.column_dependents(conn, orders, "amount")
        customer = await integrity.column_dependents(conn, orders, "customer_id")
        note = await integrity.column_dependents(conn, orders, "note")
    assert [(d.ref.kind, d.ref.id, d.via) for d in amount] == [
        ("metric", "revenue", ("metrics.expression",)),
        ("table", view, ("registered_tables.view_sql",)),
    ]
    assert [(d.ref.kind, d.ref.id, d.via) for d in customer] == [
        ("relationship", "orders_customers", ("relationships.source_column",))
    ]
    assert note == []


# --- the registration ----------------------------------------------------------------------------


async def test_dropping_a_column_a_relationship_is_keyed_on_is_refused_and_nothing_changes(plane):
    await _relate(plane)
    async with plane.acquire() as conn:
        with pytest.raises(table_repo.ColumnDropRefused) as err:
            await table_repo.upsert(conn, _table("orders", "id", "amount", "added"))
    assert err.value.table_name == "orders"
    assert err.value.report() == {
        "customer_id": [
            {
                "kind": "relationship",
                "id": "orders_customers",
                "name": "orders_customers",
                "via": ["relationships.source_column"],
            }
        ]
    }
    assert "customer_id (referred to by: relationship orders_customers)" in str(err.value)
    # Neither the drop of `note`, which nothing refers to, nor the added column was applied.
    assert await _columns(plane, "orders") == ["amount", "customer_id", "id", "note"]


async def test_dropping_the_other_ends_key_column_is_refused_too(plane):
    await _relate(plane)
    async with plane.acquire() as conn:
        with pytest.raises(table_repo.ColumnDropRefused) as err:
            await table_repo.upsert(conn, _table("customers", "name"))
    assert list(err.value.columns) == ["id"]


async def test_a_column_nothing_refers_to_is_dropped_with_its_tags(plane):
    orders = await _id(plane, "orders")
    async with plane.acquire() as conn:
        for column in ("note", "amount"):
            await conn.execute_core(
                insert(tag_assignments).values(
                    tag_id="pii",
                    base_tag_id="pii",
                    object_type="column",
                    object_key=f"orders.{column}",
                    table_id=orders,
                    column_name=column,
                )
            )
        await table_repo.upsert(conn, _table("orders", "id", "customer_id", "amount"))
        tagged = (await conn.execute_core(select(tag_assignments.c.column_name))).fetchall()
    assert await _columns(plane, "orders") == ["amount", "customer_id", "id"]
    assert [t[0] for t in tagged] == ["amount"]
    assert await _id(plane, "orders") == orders


async def test_registering_the_same_columns_again_changes_nothing_and_is_not_refused(plane):
    await _relate(plane)
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders", "id", "customer_id", "amount", "note"))
    assert await _columns(plane, "orders") == ["amount", "customer_id", "id", "note"]


async def test_a_row_filter_on_the_table_that_names_the_column_blocks_the_drop(plane):
    """The predicate is stored encrypted; the guard reads it in the server that holds the key."""
    from provisa.core.models import RLSRule
    from provisa.core.repositories import rls as rls_repo
    from provisa.core.schema_org import rls_rules, roles

    orders = await _id(plane, "orders")
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(roles).values(id="seller", capabilities=[], domain_access=["*"])
        )
        await rls_repo.upsert(
            conn, RLSRule(table_id="orders", role_id="seller", filter="note = 'public'")
        )
        stored = (
            await conn.execute_core(select(rls_rules.c.id, rls_rules.c.filter_expr))
        ).fetchone()
        assert await integrity.column_dependents(conn, orders, "amount") == []
        note = await integrity.column_dependents(conn, orders, "note")
        assert [(d.ref.kind, d.ref.id, d.via) for d in note] == [
            ("row_filter", stored[0], ("rls_rules.filter_expr",))
        ]
        with pytest.raises(table_repo.ColumnDropRefused) as err:
            await table_repo.upsert(conn, _table("orders", "id", "customer_id", "amount"))
    assert list(err.value.columns) == ["note"]
    assert "note = 'public'" not in str(err.value)  # the predicate is not echoed
    assert await _columns(plane, "orders") == ["amount", "customer_id", "id", "note"]
