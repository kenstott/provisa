# Copyright (c) 2026 Kenneth Stott
# Canary: 022498c3-66eb-43f0-93a1-3490c0622e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Deleting a relationship, metric, command or webhook needs its domain (REQ-1531).

Each of these deletes was gated on a right alone, so a member whose role reaches one domain could
remove an object of another. The caller must now reach the object's domain: both tables' domains
for a relationship, the domains of the tables a metric's expression reads, and the domain a
command or webhook sits in.
"""

# Requirements: REQ-1531, REQ-1530

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import actions_router, schema_mutation
from provisa.api.errors import ApiError
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.schema_org import (
    domains,
    metrics,
    registered_tables,
    relationships,
    sources,
    tracked_functions,
    tracked_webhooks,
)

RIGHTS = ["table_registration", "create_relationship"]
ROLES = {
    "sales_steward": {"id": "sales_steward", "capabilities": RIGHTS, "domain_access": ["sales"]},
    "everywhere": {"id": "everywhere", "capabilities": RIGHTS, "domain_access": ["*"]},
}
TABLES = {"orders": "sales", "customers": "sales", "invoices": "finance", "ledgers": "finance"}


@pytest.fixture
async def plane(monkeypatch) -> Database:
    """Two domains; a relationship inside sales, one inside finance and one across the two; a
    metric per domain; a command and a webhook per domain and one of each in no domain."""
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="delete-domain-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql", origin="admin"))
        for domain_id in ("sales", "finance"):
            await conn.execute_core(insert(domains).values(id=domain_id, origin="admin"))
        ids: dict[str, int] = {}
        for table_name, domain_id in TABLES.items():
            await conn.execute_core(
                insert(registered_tables).values(
                    source_id="pg",
                    domain_id=domain_id,
                    schema_name="public",
                    table_name=table_name,
                    origin="admin",
                )
            )
            ids[table_name] = (
                await conn.execute_core(
                    select(registered_tables.c.id).where(
                        registered_tables.c.table_name == table_name
                    )
                )
            ).scalar_one()
        for rel_id, source, target in (
            ("in_sales", "orders", "customers"),
            ("in_finance", "invoices", "ledgers"),
            ("across", "orders", "invoices"),
        ):
            await conn.execute_core(
                insert(relationships).values(
                    id=rel_id,
                    source_table_id=ids[source],
                    target_table_id=ids[target],
                    source_column="id",
                    target_column="id",
                    cardinality="many-to-one",
                )
            )
        # sales to sales, through a finance table.
        await conn.execute_core(
            insert(relationships).values(
                id="through_finance",
                source_table_id=ids["orders"],
                target_table_id=ids["customers"],
                via_table_id=ids["ledgers"],
                source_column="id",
                target_column="id",
                cardinality="many-to-one",
            )
        )
        for name, expression in (
            ("revenue", "SUM(orders.amount)"),
            ("billed", "SUM(invoices.amount)"),
            ("collected", "SUM(orders.amount) - SUM(invoices.amount)"),
        ):
            await conn.execute_core(insert(metrics).values(name=name, expression=expression))
        for table in (tracked_functions, tracked_webhooks):
            for name, domain_id in (
                ("of_sales", "sales"),
                ("of_finance", "finance"),
                ("of_none", ""),
            ):
                await conn.execute_core(insert(table).values(name=name, domain_id=domain_id))
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", ROLES, raising=False)
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)

    async def _pool():
        return db

    async def _rebuild() -> None:
        return None

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    return db


def _request(role_id: str) -> Any:
    identity = types.SimpleNamespace(user_id="u1", roles=[role_id])
    return types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="acme")
    )


def _info(role_id: str) -> Any:
    return types.SimpleNamespace(context={"request": _request(role_id)})


async def _names(db: Database, table, column: str = "name") -> set[str]:
    async with db.acquire() as conn:
        return {r[0] for r in (await conn.execute_core(select(table.c[column]))).fetchall()}


# --- relationship: both tables' domains ----------------------------------------------------------


async def test_a_relationship_inside_the_callers_domain_is_deleted(plane):
    result = await schema_mutation.Mutation().delete_relationship(
        _info("sales_steward"), "in_sales"
    )
    assert result.success is True
    assert "in_sales" not in await _names(plane, relationships, "id")


@pytest.mark.parametrize("rel_id", ["in_finance", "across", "through_finance"])
async def test_a_relationship_touching_another_domain_is_refused(plane, rel_id):
    with pytest.raises(PermissionError, match="No access to domain 'finance'"):
        await schema_mutation.Mutation().delete_relationship(_info("sales_steward"), rel_id)
    assert rel_id in await _names(plane, relationships, "id")


async def test_a_caller_reaching_every_domain_deletes_a_relationship_across_two(plane):
    result = await schema_mutation.Mutation().delete_relationship(_info("everywhere"), "across")
    assert result.success is True


async def test_a_relationship_that_is_not_there_is_not_found(plane):
    result = await schema_mutation.Mutation().delete_relationship(_info("sales_steward"), "nope")
    assert (result.success, result.code) == (False, "schema.relationship_not_found")


# --- metric: the domains its expression reads ----------------------------------------------------


async def test_a_metric_over_the_callers_domain_is_deleted(plane):
    result = await schema_mutation.Mutation().delete_metric(_info("sales_steward"), "revenue")
    assert result.success is True
    assert "revenue" not in await _names(plane, metrics)


@pytest.mark.parametrize("name", ["billed", "collected"])
async def test_a_metric_reading_another_domain_is_refused(plane, name):
    with pytest.raises(PermissionError, match="No access to domain 'finance'"):
        await schema_mutation.Mutation().delete_metric(_info("sales_steward"), name)
    assert name in await _names(plane, metrics)


async def test_a_caller_reaching_every_domain_deletes_a_metric_over_two(plane):
    result = await schema_mutation.Mutation().delete_metric(_info("everywhere"), "collected")
    assert result.success is True


async def test_a_metric_that_is_not_there_is_not_found(plane):
    result = await schema_mutation.Mutation().delete_metric(_info("sales_steward"), "nope")
    assert (result.success, result.code) == (False, "schema.metric_not_found")


# --- command and webhook: the domain it sits in --------------------------------------------------

KINDS = [
    pytest.param(actions_router.delete_function, tracked_functions, id="command"),
    pytest.param(actions_router.delete_webhook, tracked_webhooks, id="webhook"),
]


@pytest.mark.parametrize(("delete", "table"), KINDS)
async def test_one_in_the_callers_domain_is_deleted(plane, delete, table):
    await delete(_request("sales_steward"), "of_sales")
    assert "of_sales" not in await _names(plane, table)


@pytest.mark.parametrize(("delete", "table"), KINDS)
async def test_one_in_another_domain_is_refused(plane, delete, table):
    with pytest.raises(ApiError) as err:
        await delete(_request("sales_steward"), "of_finance")
    assert (err.value.status_code, err.value.code) == (403, "auth.domain_denied")
    assert "of_finance" in await _names(plane, table)


@pytest.mark.parametrize(("delete", "table"), KINDS)
async def test_one_in_no_domain_has_no_domain_to_hold(plane, delete, table):
    await delete(_request("sales_steward"), "of_none")
    assert "of_none" not in await _names(plane, table)


@pytest.mark.parametrize(("delete", "table"), KINDS)
async def test_a_caller_reaching_every_domain_deletes_one_anywhere(plane, delete, table):
    await delete(_request("everywhere"), "of_finance")
    assert "of_finance" not in await _names(plane, table)


@pytest.mark.parametrize(("delete", "table"), KINDS)
async def test_one_that_is_not_there_is_not_found(plane, delete, table):
    with pytest.raises(ApiError) as err:
        await delete(_request("sales_steward"), "nope")
    assert err.value.status_code == 404
