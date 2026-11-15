# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e94b1-3d58-4a60-b1f7-9e0a52d8c613
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship id with a prefix reserved for Provisa's built-in relationships (``meta:``,
``ontology:``) is never saved by a person: the startup seed removes built-in rows it no longer
declares, recognising them by that prefix, so a person's relationship under one would be removed
at the next boot. The repository's upsert refuses such an id by name; the seed writes its rows
itself (``startup_seed._seed_meta_relationships``) and does not go through it."""

# Requirements: REQ-019

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.api._meta_seed import seed_relationship_id_prefixes
from provisa.api.admin import schema_mutation
from provisa.api.admin.types import RelationshipInput
from provisa.core.database import Database
from provisa.core.models import Cardinality, Relationship
from provisa.core.repositories import relationship as rel_repo
from provisa.core.schema_org import domains, registered_tables, relationships, sources


@pytest.fixture
async def plane(control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="shop-db", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="shop"))
        for table in ("customers", "orders"):
            await conn.execute_core(
                insert(registered_tables).values(
                    source_id="shop-db",
                    domain_id="shop",
                    schema_name="shop",
                    table_name=table,
                )
            )
    return db


def _rel(rel_id: str) -> Relationship:
    return Relationship(
        id=rel_id,
        source_table_id="orders",
        target_table_id="customers",
        source_column="customer_id",
        target_column="id",
        cardinality=Cardinality("many-to-one"),
    )


async def _ids(db: Database) -> list[str]:
    async with db.acquire() as conn:
        return [r[0] for r in (await conn.execute_core(select(relationships.c.id))).fetchall()]


async def test_ordinary_id_saves(plane: Database) -> None:
    async with plane.acquire() as conn:
        await rel_repo.upsert(conn, _rel("orders-to-customer"))
    assert await _ids(plane) == ["orders-to-customer"]


@pytest.mark.parametrize("prefix", seed_relationship_id_prefixes())
async def test_reserved_prefix_is_refused_by_name(plane: Database, prefix: str) -> None:
    assert prefix  # the seed declares at least one reserved prefix
    async with plane.acquire() as conn:
        with pytest.raises(rel_repo.ReservedRelationshipPrefix) as refused:
            await rel_repo.upsert(conn, _rel(f"{prefix}orders:customers"))
    assert refused.value.prefix == prefix
    assert "reserved for built-in relationships" in str(refused.value)
    assert await _ids(plane) == []


def test_reserved_prefixes_are_the_seed_s_own() -> None:
    assert set(seed_relationship_id_prefixes()) >= {"meta:", "ontology:"}


async def test_the_admin_save_refuses_a_reserved_prefix_with_a_code(
    plane: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _pool() -> Database:
        return plane

    async def _no_rebuild() -> None:
        return None

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _no_rebuild)
    result = await schema_mutation.save_relationship(
        RelationshipInput(
            id="meta:orders:customers",
            source_table_id="orders",
            target_table_id="customers",
            source_column="customer_id",
            target_column="id",
            cardinality="many-to-one",
        ),
        owner=None,
        needs_review=False,
        creating=True,
    )
    assert (result.success, result.code, result.params) == (
        False,
        "schema.relationship_id_reserved",
        {"id": "meta:orders:customers", "prefix": "meta:"},
    )
    assert await _ids(plane) == []
