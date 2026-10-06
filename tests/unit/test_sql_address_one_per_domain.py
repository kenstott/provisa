# Copyright (c) 2026 Kenneth Stott
# Canary: 69bb6793-9854-46c9-aa6a-0cd6a11bc82b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered table's SQL address is its own within its domain (REQ-1933).

The write path the admin and config load share refuses a table whose ``domain.table`` address
another registered table holds, naming that table. Before, two OpenAPI sources each registering
``getInventory`` in one domain both took ``pet_store.get_inventory``, and which of them a
statement read was decided by nothing the operator chose."""

# Requirements: REQ-1933

from __future__ import annotations

import pytest
from sqlalchemy import insert

from provisa.compiler.naming import SqlAddressTaken
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources


@pytest.fixture
async def control_plane(tmp_path):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    db = Database(engine, name="org")
    await init_schema(db, "", org_id="default")
    async with db.acquire() as conn:
        for domain in ("pet-store", "copies"):
            await conn.execute_core(insert(domains).values(id=domain))
        for source in ("petstore-api", "copy-api"):
            await conn.execute_core(insert(sources).values(id=source, type="openapi"))
    try:
        yield db
    finally:
        engine.dispose()


def _table(source: str, *, domain: str = "pet-store", alias: str | None = None) -> Table:
    return Table(
        source_id=source,
        domain_id=domain,
        schema_name="openapi",
        table_name="getInventory",
        alias=alias,
        columns=[Column(name="status", data_type="text", visible_to=["admin"])],
    )


async def test_a_second_table_at_a_taken_address_is_refused_naming_the_holder(control_plane):
    async with control_plane.acquire() as conn:
        await table_repo.upsert(conn, _table("petstore-api"))
        with pytest.raises(SqlAddressTaken) as refused:
            await table_repo.upsert(conn, _table("copy-api"))
    assert refused.value.address == "get_inventory"
    assert refused.value.holder == "petstore-api.openapi.getInventory"
    assert refused.value.newcomer == "copy-api.openapi.getInventory"


async def test_config_load_refuses_it_the_same_way(control_plane):
    async with control_plane.acquire() as conn:
        await table_repo.upsert(conn, _table("petstore-api"))
        with pytest.raises(SqlAddressTaken):
            await table_repo.upsert(conn, _table("copy-api"))


async def test_an_alias_or_another_domain_is_its_own_address(control_plane):
    async with control_plane.acquire() as conn:
        await table_repo.upsert(conn, _table("petstore-api"))
        await table_repo.upsert(conn, _table("copy-api", alias="copy_inventory"))
        await table_repo.upsert(conn, _table("copy-api", domain="copies"))


async def test_saving_a_table_again_is_not_a_second_table(control_plane):
    async with control_plane.acquire() as conn:
        await table_repo.upsert(conn, _table("petstore-api"))
        await table_repo.upsert(conn, _table("petstore-api"))
