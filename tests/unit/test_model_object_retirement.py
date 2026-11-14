# Copyright (c) 2026 Kenneth Stott
# Canary: c7fd6670-ac59-4274-9c63-704b624f2fb6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Deleting a table, column, relationship, tag or metric retires it (REQ-2248).

Per kind: the delete keeps the row, marked retired with who (the person the change commits as),
when and why; the projected model file carries the retirement; a merge carries a retirement and
never removes the object; and the store refuses a second object at a retired name.
"""

# Requirements: REQ-2248

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.core import model_change
from provisa.core.database import Database
from provisa.core.env_merge import merged_tree
from provisa.core.env_project import project
from provisa.core.models import Cardinality, Column, Metric, Relationship, Table, Tag
from provisa.core.repositories import metric as metric_repo
from provisa.core.repositories import relationship as rel_repo
from provisa.core.repositories import table as table_repo
from provisa.core.repositories import tag as tag_repo
from provisa.core.repositories.retirement import RetiredNameTaken
from provisa.core.schema_org import (
    domains,
    metrics,
    registered_tables,
    relationships,
    roles,
    sources,
    table_columns,
    tag_assignments,
    tags,
)
from tests.unit.gate_identity import serve_every_column

SCHEMA = "org_default"
ORDERS = "sales/tables/orders.yaml"
CUSTOMERS = "sales/tables/customers.yaml"
WHY = "superseded by the order ledger"


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


@pytest.fixture
async def plane(control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id="seller", capabilities=[], domain_access=["sales"])
        )
        for name in ("orders", "customers"):
            await table_repo.upsert(conn, _table(name, "id", "amount"))
    return db


def _table(name: str, *columns: str) -> Table:
    return Table(
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=[Column(name=c, visible_to=["seller"], data_type="integer") for c in columns],
    )


async def _id(conn, name: str) -> int:
    return (
        await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.table_name == name)
        )
    ).scalar_one()


async def _row(conn, table, *where) -> dict[str, Any]:
    return dict((await conn.execute_core(select(table).where(*where))).one()._mapping)


def _assert_retired(row: dict[str, Any], why: str | None = WHY) -> None:
    assert row["retired"] is True
    assert row["status_by"] == "ada"
    assert row["status_reason"] == why
    assert row["status_at"] is not None


def _assert_retired_in_file(body: dict[str, Any], why: str | None = WHY) -> None:
    assert body["retired"] is True
    assert body["status_by"] == "ada"
    assert body["status_reason"] == why
    assert body["status_at"] is not None


async def _tag_on_table(conn, table_id: int, column: str | None = None) -> None:
    await conn.execute_core(
        insert(tag_assignments).values(
            tag_id="pii",
            base_tag_id="pii",
            object_type="column" if column else "table",
            object_key=f"{table_id}:{column}" if column else str(table_id),
            table_id=table_id,
            column_name=column,
        )
    )


# --- tables --------------------------------------------------------------------------------------


async def test_deleting_a_table_retires_it_with_who_when_and_why(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        orders = await _id(conn, "orders")
        await _tag_on_table(conn, orders)
        assert await table_repo.delete(conn, orders, reason=WHY) is True
        _assert_retired(await _row(conn, registered_tables, registered_tables.c.id == orders))
        # Its columns and what is attached to it are kept with it.
        kept = await conn.execute_core(
            select(table_columns.c.column_name).where(table_columns.c.table_id == orders)
        )
        assert sorted(r[0] for r in kept) == ["amount", "id"]
        assert await _row(conn, tag_assignments, tag_assignments.c.table_id == orders)
        # Retired already: there is nothing left to delete, and the first retirement stands.
        assert await table_repo.delete(conn, orders, reason="again") is False
        _assert_retired(await _row(conn, registered_tables, registered_tables.c.id == orders))


async def test_a_table_deleted_without_a_reason_is_retired_without_one(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        orders = await _id(conn, "orders")
        assert await table_repo.delete(conn, orders) is True
        _assert_retired(
            await _row(conn, registered_tables, registered_tables.c.id == orders), why=None
        )


async def test_a_retired_tables_name_is_refused_to_a_second_registration(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await table_repo.delete(conn, await _id(conn, "orders"), reason=WHY)
        with pytest.raises(RetiredNameTaken) as err:
            await table_repo.upsert(conn, _table("orders", "id"))
        assert (err.value.kind, err.value.name) == ("table", "orders")
        assert (await _row(conn, registered_tables, registered_tables.c.table_name == "orders"))[
            "retired"
        ]


async def test_a_retired_tables_file_carries_the_retirement(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await table_repo.delete(conn, await _id(conn, "orders"), reason=WHY)
        tree = await project(conn, SCHEMA)
    _assert_retired_in_file(tree[ORDERS])
    assert tree[CUSTOMERS]["retired"] is False


# --- columns -------------------------------------------------------------------------------------


async def test_a_column_dropped_from_the_registration_is_retired_with_what_is_on_it(plane):
    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        orders = await _id(conn, "orders")
        await _tag_on_table(conn, orders, "amount")
        await table_repo.upsert(conn, _table("orders", "id"))
        amount = await _row(
            conn,
            table_columns,
            table_columns.c.table_id == orders,
            table_columns.c.column_name == "amount",
        )
        _assert_retired(amount, why="dropped from the table's registration")
        assert amount["data_type"] == "integer"  # the column is kept whole
        live = await _row(
            conn,
            table_columns,
            table_columns.c.table_id == orders,
            table_columns.c.column_name == "id",
        )
        assert live["retired"] is False
        assert await _row(conn, tag_assignments, tag_assignments.c.column_name == "amount")
        # Saved again without it, it stays retired as it was.
        await table_repo.upsert(conn, _table("orders", "id"))
        again = await _row(
            conn,
            table_columns,
            table_columns.c.table_id == orders,
            table_columns.c.column_name == "amount",
        )
        assert again["status_at"] == amount["status_at"]


async def test_a_retired_columns_name_is_refused_to_a_second_column(plane):
    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders", "id"))
        with pytest.raises(RetiredNameTaken) as err:
            await table_repo.upsert(conn, _table("orders", "id", "amount"))
    assert (err.value.kind, err.value.name) == ("column", "orders.amount")


async def test_a_retired_columns_entry_in_its_tables_file_carries_the_retirement(plane):
    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders", "id"))
        tree = await project(conn, SCHEMA)
    columns = {c["column_name"]: c for c in tree[ORDERS]["columns"]}
    _assert_retired_in_file(columns["amount"], why="dropped from the table's registration")
    assert columns["id"]["retired"] is False


# --- relationships -------------------------------------------------------------------------------


def _relationship() -> Relationship:
    return Relationship(
        id="orders_customer",
        source_table_id="orders",
        target_table_id="customers",
        source_column="id",
        target_column="id",
        cardinality=Cardinality.many_to_one,
        alias="placed_by",
    )


async def test_deleting_a_relationship_retires_it_with_who_when_and_why(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await rel_repo.upsert(conn, _relationship())
        assert await rel_repo.delete(conn, "orders_customer", reason=WHY) is True
        _assert_retired(await _row(conn, relationships, relationships.c.id == "orders_customer"))
        assert await rel_repo.delete(conn, "orders_customer") is False


async def test_a_retired_relationships_id_and_alias_are_refused_to_a_second_one(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await rel_repo.upsert(conn, _relationship())
        await rel_repo.delete(conn, "orders_customer", reason=WHY)
        with pytest.raises(RetiredNameTaken) as err:
            await rel_repo.upsert(conn, _relationship())
        assert (err.value.kind, err.value.name) == ("relationship", "orders_customer")
        # Its alias stays taken on its source table, by the store's uniqueness.
        other = _relationship().model_copy(update={"id": "orders_buyer", "source_column": "amount"})
        with pytest.raises(ValueError, match="placed_by"):
            await rel_repo.upsert(conn, other)


async def test_a_retired_relationships_entry_in_its_tables_file_carries_the_retirement(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await rel_repo.upsert(conn, _relationship())
        await rel_repo.delete(conn, "orders_customer", reason=WHY)
        tree = await project(conn, SCHEMA)
    [rel] = tree[ORDERS]["relationships"]
    assert rel["id"] == "orders_customer"
    _assert_retired_in_file(rel)


# --- tags ----------------------------------------------------------------------------------------


async def test_deleting_a_tag_retires_it_with_who_when_and_why(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await tag_repo.upsert(conn, Tag(id="legacy", applies_to=["table"]))
        await conn.execute_core(
            insert(tag_assignments).values(
                tag_id="legacy",
                base_tag_id="legacy",
                object_type="table",
                object_key="orders",
                table_id=await _id(conn, "orders"),
            )
        )
        assert await tag_repo.delete(conn, "legacy", reason=WHY) is True
        _assert_retired(await _row(conn, tags, tags.c.id == "legacy"))
        assert await _row(conn, tag_assignments, tag_assignments.c.tag_id == "legacy")
        assert await tag_repo.delete(conn, "legacy") is False


async def test_a_retired_tags_id_is_refused_to_a_second_tag(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await tag_repo.upsert(conn, Tag(id="legacy"))
        await tag_repo.delete(conn, "legacy", reason=WHY)
        with pytest.raises(RetiredNameTaken) as err:
            await tag_repo.upsert(conn, Tag(id="legacy", description="a new one"))
    assert (err.value.kind, err.value.name) == ("tag", "legacy")


async def test_a_retired_tags_file_carries_the_retirement(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await tag_repo.upsert(conn, Tag(id="legacy"))
        await tag_repo.delete(conn, "legacy", reason=WHY)
        tree = await project(conn, SCHEMA)
    _assert_retired_in_file(tree["tags/legacy.yaml"])


# --- metrics -------------------------------------------------------------------------------------


async def test_deleting_a_metric_retires_it_with_who_when_and_why(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await metric_repo.upsert(conn, Metric(name="revenue", expression="SUM(orders.amount)"))
        assert await metric_repo.delete(conn, "revenue", reason=WHY) is True
        _assert_retired(await _row(conn, metrics, metrics.c.name == "revenue"))
        assert await metric_repo.delete(conn, "revenue") is False


async def test_a_retired_metrics_name_is_refused_to_a_second_metric(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await metric_repo.upsert(conn, Metric(name="revenue", expression="SUM(orders.amount)"))
        await metric_repo.delete(conn, "revenue", reason=WHY)
        with pytest.raises(RetiredNameTaken) as err:
            await metric_repo.upsert(conn, Metric(name="revenue", expression="SUM(orders.id)"))
    assert (err.value.kind, err.value.name) == ("metric", "revenue")


async def test_a_retired_metrics_file_carries_the_retirement(plane):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        await metric_repo.upsert(conn, Metric(name="revenue", expression="SUM(orders.amount)"))
        await metric_repo.delete(conn, "revenue", reason=WHY)
        tree = await project(conn, SCHEMA)
    _assert_retired_in_file(tree["metrics/revenue.yaml"])


# --- a merge carries a retirement and never removes ----------------------------------------------

MERGE_WHY = "removed by a merge from dev"
STAMP = {"retired": True, "status_by": "bo", "status_at": "2026-11-01", "status_reason": WHY}


def _order(**extra: Any) -> dict[str, Any]:
    return {
        "table_name": "orders",
        "retired": False,
        "columns": [
            {"column_name": "amount", "retired": False},
            {"column_name": "id", "retired": False},
        ],
        "relationships": [{"id": "orders_customer", "retired": False}],
        **extra,
    }


def _merge(source: dict, target: dict, removals: bool) -> dict:
    return merged_tree(source, target, set(source) | set(target), removals, by="ada", why=MERGE_WHY)


@pytest.mark.parametrize(
    "path",
    [ORDERS, "tags/legacy.yaml", "metrics/revenue.yaml"],
)
def test_a_retirement_in_the_source_travels_to_the_target(path):
    target = {path: {"retired": False}}
    merged = _merge({path: dict(STAMP)}, target, removals=False)
    assert merged[path] == STAMP


@pytest.mark.parametrize("path", [ORDERS, "tags/legacy.yaml", "metrics/revenue.yaml"])
def test_a_merge_that_removes_an_object_retires_it_in_the_target(path):
    merged = _merge({}, {path: {"retired": False, "description": "kept"}}, removals=True)
    body = merged[path]
    assert body["description"] == "kept"
    assert (body["retired"], body["status_by"], body["status_reason"]) == (True, "ada", MERGE_WHY)
    assert body["status_at"] is not None


def test_a_merge_that_removes_a_retired_object_keeps_its_own_retirement():
    merged = _merge({}, {"tags/legacy.yaml": dict(STAMP)}, removals=True)
    assert merged["tags/legacy.yaml"] == STAMP


def test_a_domain_a_merge_removes_still_goes():
    """Only the kinds REQ-2248 names are retired; a domain is not one of them."""
    merged = _merge({}, {"finance/domain.yaml": {"description": "ledgers"}}, removals=True)
    assert "finance/domain.yaml" not in merged


@pytest.mark.parametrize("removals", [False, True])
def test_a_column_and_relationship_the_source_lacks_are_kept_retired_in_the_table(removals):
    source = {ORDERS: _order(columns=[{"column_name": "id", "retired": False}], relationships=[])}
    merged = _merge(source, {ORDERS: _order()}, removals=removals)
    columns = {c["column_name"]: c for c in merged[ORDERS]["columns"]}
    assert [c["column_name"] for c in merged[ORDERS]["columns"]] == ["amount", "id"]
    assert columns["id"]["retired"] is False
    assert (columns["amount"]["retired"], columns["amount"]["status_by"]) == (True, "ada")
    [rel] = merged[ORDERS]["relationships"]
    assert (rel["id"], rel["retired"], rel["status_reason"]) == ("orders_customer", True, MERGE_WHY)


def test_a_table_the_source_carries_whole_is_carried_as_it_is():
    merged = _merge({ORDERS: _order(description="new")}, {ORDERS: _order()}, removals=False)
    assert merged[ORDERS] == _order(description="new")
