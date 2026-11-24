# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2e9f14-8a3b-4d71-b5e0-2f9a7c1d4e83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship save ends in success or a refusal that names what is wrong, never an error
(REQ-019, REQ-166). A table name that resolves to no registered table or to more than one, and
an alias another relationship of the same source table already holds, are refused by name before
anything is written: the store is unchanged after a refused save.

A table is named by its virtual name, its alias when it has one: a relationship to the weblogs
demo's shop_access table names access_log. Its physical name resolves to nothing, and the
reverse form once sent it, which answered a GraphQL error the page never cleared."""

# Requirements: REQ-019, REQ-166

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.entitlements.owners import Changer
from provisa.api.admin import schema_mutation
from provisa.api.admin.types import RelationshipInput
from provisa.core.database import Database
from provisa.core.models import Cardinality, Relationship
from provisa.core.repositories import relationship as rel_repo
from provisa.core.schema_org import domains, registered_tables, relationships, sources


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    """customers, and shop_access registered under the alias access_log, with the weblogs demo's
    forward relationship access_log.customer_id -> customers.id, alias BY_CUSTOMER."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="shop-db", type="postgresql"))
        await conn.execute_core(insert(sources).values(id="other-db", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="shop"))
        for source_id, table, alias in (
            ("shop-db", "customers", None),
            ("shop-db", "shop_access", "access_log"),
            ("shop-db", "orders", None),
            ("other-db", "orders", None),
        ):
            await conn.execute_core(
                insert(registered_tables).values(
                    source_id=source_id,
                    domain_id="shop",
                    schema_name="shop",
                    table_name=table,
                    alias=alias,
                )
            )
        await rel_repo.upsert(
            conn,
            Relationship(
                id="access-to-customer",
                source_table_id="access_log",
                target_table_id="customers",
                source_column="customer_id",
                target_column="id",
                cardinality=Cardinality("many-to-one"),
                alias="BY_CUSTOMER",
            ),
        )

    async def _pool():
        return db

    async def _no_rebuild():
        return None

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _no_rebuild)
    return db


async def _stored(db: Database) -> list[tuple]:
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(
                    relationships.c.id,
                    relationships.c.source_table_id,
                    relationships.c.target_table_id,
                    relationships.c.alias,
                    relationships.c.graphql_alias,
                    relationships.c.version,
                ).order_by(relationships.c.id)
            )
        ).fetchall()
    return [tuple(r) for r in rows]


def _input(**fields: Any) -> RelationshipInput:
    base: dict[str, Any] = {
        "id": "customers-to-access_log",
        "source_table_id": "customers",
        "target_table_id": "access_log",
        "source_column": "id",
        "target_column": "customer_id",
        "cardinality": "one-to-many",
        "alias": "BY_CUSTOMER",
        "graphql_alias": "byCustomer",
    }
    return RelationshipInput(**{**base, **fields})


async def _save(input: RelationshipInput, *, creating: bool = True):
    return await schema_mutation.save_relationship(
        input,
        set_by=None,
        changer=Changer(None, org_admin=True),
        needs_review=False,
        creating=creating,
    )


async def test_a_table_named_by_its_physical_name_when_it_has_an_alias_is_refused(plane):
    before = await _stored(plane)
    result = await _save(_input(id="customers-to-shop_access", target_table_id="shop_access"))
    assert (result.success, result.code, result.params) == (
        False,
        "schema.relationship_unknown_target",
        {"table": "shop_access"},
    )
    assert await _stored(plane) == before


async def test_an_alias_another_relationship_of_the_source_holds_is_refused_naming_it(plane):
    before = await _stored(plane)
    result = await _save(
        _input(
            id="access-to-customer-2",
            source_table_id="access_log",
            target_table_id="customers",
            source_column="customer_id",
            target_column="id",
            cardinality="many-to-one",
            graphql_alias="customerAgain",
        )
    )
    assert (result.success, result.code, result.params) == (
        False,
        "schema.relationship_alias_taken",
        {"alias": "BY_CUSTOMER", "table": "access_log", "relationship": "access-to-customer"},
    )
    assert await _stored(plane) == before


async def test_a_table_name_two_sources_answer_to_is_refused_naming_them(plane):
    before = await _stored(plane)
    result = await _save(_input(id="customers-to-orders", target_table_id="orders"))
    assert (result.success, result.code) == (False, "schema.relationship_ambiguous_table")
    assert result.params == {"end": "target", "table": "orders", "sources": "shop-db, other-db"}
    assert await _stored(plane) == before


async def test_the_mutation_refuses_an_ambiguous_source_rather_than_raising(plane, monkeypatch):
    from provisa.api.admin import capabilities

    monkeypatch.setattr(capabilities, "has_capability", lambda _info, _cap: True)
    before = await _stored(plane)
    info: Any = types.SimpleNamespace(context={})
    result = await schema_mutation._save_relationship_as_caller(
        info,
        _input(id="orders-to-customers", source_table_id="orders", target_table_id="customers"),
        creating=True,
    )
    assert result.params is not None
    assert (result.success, result.code, result.params["end"]) == (
        False,
        "schema.relationship_ambiguous_table",
        "source",
    )
    assert await _stored(plane) == before


async def test_the_reverse_of_a_relationship_saves_with_the_same_alias_on_its_own_source(plane):
    """An alias tells apart one source table's relationships: the reverse, from customers, may
    carry the forward's alias."""
    result = await _save(_input())
    assert (result.success, result.code) == (True, "schema.relationship_saved")
    assert [r[0] for r in await _stored(plane)] == [
        "access-to-customer",
        "customers-to-access_log",
    ]


async def test_saving_a_relationship_again_keeps_its_own_alias(plane):
    result = await _save(
        creating=False,
        input=_input(
            id="access-to-customer",
            source_table_id="access_log",
            target_table_id="customers",
            source_column="customer_id",
            target_column="id",
            cardinality="many-to-one",
            graphql_alias=None,
        ),
    )
    assert (result.success, result.code) == (True, "schema.relationship_saved")


async def test_a_create_on_an_id_that_exists_is_refused_and_an_update_on_a_missing_one_too(plane):
    before = await _stored(plane)
    taken = await _save(_input(id="access-to-customer", alias="OWNS", graphql_alias="owns"))
    assert (taken.success, taken.code, taken.params) == (
        False,
        "schema.relationship_id_taken",
        {"relationship": "access-to-customer"},
    )
    missing = await _save(_input(id="customers-to-access_log"), creating=False)
    assert (missing.success, missing.code) == (False, "schema.relationship_not_found")
    assert await _stored(plane) == before


async def test_a_graphql_alias_another_relationship_of_the_source_holds_is_refused(plane):
    first = await _save(_input())
    assert first.success, first.message
    before = await _stored(plane)
    result = await _save(
        _input(id="customers-to-access_log-2", alias="VISITED", graphql_alias="byCustomer")
    )
    assert (result.success, result.code, result.params) == (
        False,
        "schema.relationship_graphql_alias_taken",
        {"alias": "byCustomer", "table": "customers", "relationship": "customers-to-access_log"},
    )
    assert await _stored(plane) == before
