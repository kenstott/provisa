# Copyright (c) 2026 Kenneth Stott
# Canary: 5dc05c86-b88c-4dea-a7ba-1593a4eaf86f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The region an object made through the admin starts in, and who decides where a table's data
lives (REQ-1921, "a table carries its own region; a source's region is its default"): the
table's stored region alone. A new table starts in its source's region, else the connected one;
a new view in the connected one; an edit keeps the stored region; changing a source's region
moves no table. With no platform regions nothing has one."""

# Requirements: REQ-1921

from __future__ import annotations

from types import SimpleNamespace

import pytest
import strawberry

from provisa.core.models import Source, Table
from provisa.core.regions import OrgRegion, StoreConfig

_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.fixture
def node():
    """Bind this process to a region (or none) for the test, and unbind it after."""
    from provisa.core import process_region

    was = process_region._region

    def _bind(region: str | None) -> None:
        process_region.bind_launch(_PLATFORM if region else {}, requested=region)

    yield _bind
    process_region._region = was


@pytest.fixture
async def model(tmp_path):
    """An org model with regions eu and us selected."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.repositories import region as region_repo
    from provisa.core.schema_org import metadata

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'model.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw)
    db = Database(engine, "test")
    async with db.acquire() as conn:
        for rid in ("eu", "us"):
            await region_repo.upsert_store(
                conn, StoreConfig(id=f"{rid}-pg", url=f"postgresql://{rid}/db"), origin="admin"
            )
            await region_repo.upsert_store(
                conn,
                StoreConfig(id=f"{rid}-trino", url=f"trino://{rid}:8080", kind="trino-byo"),
                origin="admin",
            )
            await region_repo.upsert_region(
                conn,
                OrgRegion(
                    id=rid,
                    engine=f"{rid}-trino",
                    replicas=f"{rid}-pg",
                    views=f"{rid}-pg",
                    cache=f"{rid}-pg",
                    state=f"{rid}-pg",
                    record=f"{rid}-pg",
                ),
                origin="admin",
            )
    return db


async def _source(conn, sid: str, region: str | None) -> None:
    from provisa.core.repositories import source as source_repo

    await source_repo.upsert(
        conn,
        Source(
            id=sid,
            type="postgresql",
            host="h",
            database="d",
            username="u",
            password="",
            region=region,
        ),
        origin="admin",
    )


def _form(source_id: str, region=strawberry.UNSET):
    return SimpleNamespace(source_id=source_id, region=region)


async def test_a_new_table_starts_in_its_sources_region(model, node):
    from provisa.api.admin.region_defaults import registration_region

    node("us")
    async with model.acquire() as conn:
        await _source(conn, "crm", "eu")
        assert await registration_region(conn, _form("crm"), is_view=False) == "eu"


async def test_a_new_table_of_a_source_naming_none_starts_in_the_connected_region(model, node):
    from provisa.api.admin.region_defaults import registration_region

    node("us")
    async with model.acquire() as conn:
        await _source(conn, "crm", None)
        assert await registration_region(conn, _form("crm"), is_view=False) == "us"


async def test_a_new_view_starts_in_the_connected_region_not_its_sources(model, node):
    from provisa.api.admin.region_defaults import registration_region

    node("us")
    async with model.acquire() as conn:
        await _source(conn, "crm", "eu")
        assert await registration_region(conn, _form("crm"), is_view=True) == "us"


async def test_the_operator_may_choose_a_region_or_remove_it(model, node):
    from provisa.api.admin.region_defaults import registration_region

    node("us")
    async with model.acquire() as conn:
        await _source(conn, "crm", "eu")
        assert await registration_region(conn, _form("crm", "us"), is_view=False) == "us"
        assert await registration_region(conn, _form("crm", None), is_view=False) is None


async def test_with_no_platform_regions_nothing_starts_in_one(model, node):
    from provisa.api.admin.region_defaults import registration_region

    node(None)
    async with model.acquire() as conn:
        await _source(conn, "crm", None)
        assert await registration_region(conn, _form("crm"), is_view=False) is None
        assert await registration_region(conn, _form("crm"), is_view=True) is None


def _orders(region: str | None) -> Table:
    return Table.model_validate(
        {
            "source_id": "crm",
            "domain_id": "sales",
            "schema": "public",
            "table": "orders",
            "region": region,
            "columns": [{"name": "id", "visible_to": ["admin"], "data_type": "integer"}],
        }
    )


async def test_an_edit_keeps_the_stored_region(model, node):
    from provisa.api.admin.region_defaults import kept_region
    from provisa.core.repositories import table as table_repo

    node("us")
    async with model.acquire() as conn:
        await _source(conn, "crm", None)
        await table_repo.upsert(conn, _orders("eu"), origin="admin")
        edited = _orders(None)  # the form rebuilt it without a region
        assert await kept_region(conn, edited) == "eu"


async def test_changing_a_sources_region_moves_no_table(model, node):
    from sqlalchemy import select

    from provisa.core.repositories import region as region_repo
    from provisa.core.repositories import table as table_repo
    from provisa.core.schema_org import registered_tables
    from provisa.federation.replica_converge import home_region

    node("us")
    async with model.acquire() as conn:
        await _source(conn, "crm", "eu")
        await table_repo.upsert(conn, _orders(None), origin="admin")
        await region_repo.set_source_region(conn, "crm", "us")
        row = (await conn.execute_core(select(registered_tables))).fetchone()
    assert row.region is None
    assert home_region(dict(row._mapping)) is None  # homed nowhere: its source decides nothing


async def test_a_tables_region_is_set_and_removed_by_its_own_change(model, node):
    from provisa.core.repositories import region as region_repo
    from provisa.core.repositories import table as table_repo

    node("us")
    async with model.acquire() as conn:
        await _source(conn, "crm", None)
        table_id = await table_repo.upsert(conn, _orders(None), origin="admin")
        assert table_id is not None
        await region_repo.set_table_region(conn, table_id, "eu")
        with pytest.raises(region_repo.RegionNotSelected):
            await region_repo.set_table_region(conn, table_id, "ap")
        await region_repo.set_table_region(conn, table_id, None)
