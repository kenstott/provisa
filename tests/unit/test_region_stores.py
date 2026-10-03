# Copyright (c) 2026 Kenneth Stott
# Canary: 368ee31d-547c-4284-a20e-35c3da03200f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org's state and record are kept in the stores its model names for this node's region
(REQ-1921, REQ-1922); with no platform regions they are kept with its model."""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

import pytest
from sqlalchemy import select

from provisa.core import process_region
from provisa.core.database import create_engine_from_url
from provisa.core.model_change import ModelPlane
from provisa.core.region_stores import StoreNotDeclared, bind_region_stores, open_org_stores
from provisa.core.regions import OrgRegion, StoreConfig
from provisa.core.repositories.region import OrgNotInRegion, upsert_region, upsert_store
from provisa.core.schema_org import metadata, query_audit_log, replica_state

_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.fixture(autouse=True)
def _restore_region():
    was = process_region._region
    yield
    process_region._region = was


def _sqlite(path) -> str:
    return f"sqlite+pysqlite:///{path}"


@pytest.fixture
def control_plane(tmp_path):
    engine = create_engine_from_url(_sqlite(tmp_path / "model.db"))
    with engine.begin() as raw:
        metadata.create_all(raw)
    return engine


async def _declare(model_db, tmp_path, *, state="eu-state", record="eu-record", region="eu"):
    async with model_db.acquire() as conn:
        for store in ("eu-state", "eu-record"):
            await upsert_store(
                conn, StoreConfig(id=store, url=_sqlite(tmp_path / f"{store}.db")), origin="config"
            )
        await upsert_region(
            conn,
            OrgRegion(
                id=region,
                engine="eu-state",
                replicas="eu-state",
                views="eu-state",
                cache="eu-state",
                state=state,
                record=record,
            ),
            origin="config",
        )


async def _bind(stores):
    return await bind_region_stores(
        "acme",
        None,
        stores,
        pool_size=1,
        max_overflow=0,
        schema_sql="-- the portable path lays the schema out from metadata",
        initialise=True,
    )


async def test_with_no_platform_regions_all_three_are_kept_with_the_model(control_plane):
    process_region.bind_launch({}, requested=None)
    stores = open_org_stores("org_acme", ModelPlane("acme", None), model_engine=control_plane)
    assert {stores.model_db.engine, stores.tenant_db.engine, stores.record_db.engine} == {
        control_plane
    }
    assert await _bind(stores) == stores


async def test_in_a_region_the_state_and_record_wait_for_the_model(control_plane):
    process_region.bind_launch(_PLATFORM, requested="eu")
    stores = open_org_stores("org_acme", ModelPlane("acme", None), model_engine=control_plane)
    assert (stores.tenant_db, stores.record_db) == (None, None)


async def test_the_regions_state_and_record_are_its_own_stores(control_plane, tmp_path):
    process_region.bind_launch(_PLATFORM, requested="eu")
    stores = open_org_stores("org_acme", ModelPlane("acme", None), model_engine=control_plane)
    await _declare(stores.model_db, tmp_path)
    bound = await _bind(stores)
    assert bound.model_db is stores.model_db
    assert str(bound.tenant_db.engine.url).endswith("eu-state.db")
    assert str(bound.record_db.engine.url).endswith("eu-record.db")
    # Laid out in each store, and each handle reads its own side there.
    async with bound.tenant_db.acquire() as conn:
        await conn.execute_core(select(replica_state.c.source_id))
    async with bound.record_db.acquire() as conn:
        await conn.execute_core(select(query_audit_log.c.id))


async def test_an_org_not_in_the_nodes_region_is_refused_by_name(control_plane, tmp_path):
    process_region.bind_launch(_PLATFORM, requested="us")
    stores = open_org_stores("org_acme", ModelPlane("acme", None), model_engine=control_plane)
    await _declare(stores.model_db, tmp_path)
    with pytest.raises(OrgNotInRegion, match="does not select region 'us'.*it selects eu"):
        await _bind(stores)


async def test_a_store_the_org_does_not_declare_is_refused(control_plane, tmp_path):
    """Load and save refuse this; a model store written around them is refused here too."""
    process_region.bind_launch(_PLATFORM, requested="eu")
    stores = open_org_stores("org_acme", ModelPlane("acme", None), model_engine=control_plane)
    await _declare(stores.model_db, tmp_path, record="nowhere")
    with pytest.raises(StoreNotDeclared, match="keeps its record in store 'nowhere'"):
        await _bind(stores)
