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
from provisa.core.schema_org import metadata, org_regions, query_audit_log, replica_state

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
        await upsert_store(
            conn,
            StoreConfig(id="eu-duck", url="duckdb:///eu.duckdb", kind="duckdb"),
            origin="config",
        )
        values = {
            "id": region,
            "engine": "eu-duck",
            "replicas": "eu-state",
            "views": "eu-state",
            "cache": "eu-state",
            "state": state,
            "record": record,
        }
        if {state, record} <= {"eu-state", "eu-record"}:
            await upsert_region(conn, OrgRegion(**values), origin="config")
        else:
            # A model store written around the save gate (which refuses an undeclared store).
            await conn.execute_core(org_regions.insert().values(**values, origin="config"))


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


def _engine_region(engine_store: str) -> list[OrgRegion]:
    return [
        OrgRegion(
            id="eu",
            engine=engine_store,
            replicas="eu-pg",
            views="eu-pg",
            cache="eu-pg",
            state="eu-pg",
            record="eu-pg",
        )
    ]


_ENGINE_STORES = [
    StoreConfig(id="eu-trino", url="trino://coordinator.eu:8443", kind="trino-byo"),
    StoreConfig(id="eu-snow", url="snowflake://acct/db", kind="snowflake"),
    StoreConfig(id="eu-pg", url="postgresql://eu/db"),
]


def test_a_regions_engine_is_its_engine_store():
    """A Trino kind is addressed by its coordinator endpoint, every other kind by its DSN."""
    from provisa.core.region_stores import RegionLane, region_lane

    process_region.bind_launch(_PLATFORM, requested="eu")
    assert region_lane("acme", _engine_region("eu-trino"), _ENGINE_STORES) == RegionLane(
        "trino-byo", ("coordinator.eu", 8443), None, "postgresql://eu/db"
    )
    assert region_lane("acme", _engine_region("eu-snow"), _ENGINE_STORES) == RegionLane(
        "snowflake", None, "snowflake://acct/db", "postgresql://eu/db"
    )


def test_with_no_platform_regions_no_engine_is_named():
    from provisa.core.region_stores import region_lane

    process_region.bind_launch({}, requested=None)
    assert region_lane("acme", [], []) is None


def test_the_admin_plane_row_may_not_name_an_engine_or_store_beside_the_region():
    from provisa.core.region_stores import RegionLaneConflict, refuse_lane_conflict

    process_region.bind_launch(_PLATFORM, requested="eu")
    refuse_lane_conflict(
        "acme", engine_kind=None, engine_url=None, external_engine=None, storage_url=None
    )
    with pytest.raises(
        RegionLaneConflict, match="region 'eu' of its model.*also sets engine_url, storage_url"
    ):
        refuse_lane_conflict(
            "acme",
            engine_kind=None,
            engine_url="snowflake://x/y",
            external_engine=None,
            storage_url="postgresql://byo/db",
        )


def test_the_boot_orgs_engine_is_the_one_its_region_names(monkeypatch):
    """REQ-1922: the boot org binds the engine its region names in the config file, the lane
    every other org in a region deployment is built on."""
    from provisa.api import app as app_mod

    built: list = []

    class _Runtime:
        def __init__(self, engine, _state):
            built.append(engine)

        def bind_terminal(self):
            built.append("bound")

    state = app_mod.AppState()
    monkeypatch.setattr(app_mod, "state", state)
    monkeypatch.setattr("provisa.federation.engine.build_engine", lambda kind: f"engine:{kind}")
    monkeypatch.setattr("provisa.federation.runtime.EngineRuntime", _Runtime)
    process_region.bind_launch(_PLATFORM, requested="eu")
    app_mod._bind_boot_engine(
        {
            "regions": [r.model_dump() for r in _engine_region("eu-snow")],
            "stores": [s.model_dump() for s in _ENGINE_STORES],
        }
    )
    rt = state._active_runtime()
    assert built == ["engine:snowflake", "bound"]
    assert (rt.isolated_engine, rt.engine_kind, rt.engine_url, rt.storage_url) == (
        True,
        "snowflake",
        "snowflake://acct/db",
        "postgresql://eu/db",
    )


def test_with_no_platform_regions_the_boot_engine_is_left_as_it_is(monkeypatch):
    from provisa.api import app as app_mod

    state = app_mod.AppState()
    monkeypatch.setattr(app_mod, "state", state)
    before = state.federation_engine
    process_region.bind_launch({}, requested=None)
    app_mod._bind_boot_engine({})
    assert state.federation_engine is before and not state._active_runtime().isolated_engine
