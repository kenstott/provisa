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
        "trino-byo", ("coordinator.eu", 8443), None, "postgresql://eu/db", "postgresql://eu/db"
    )
    assert region_lane("acme", _engine_region("eu-snow"), _ENGINE_STORES) == RegionLane(
        "snowflake", None, "snowflake://acct/db", "postgresql://eu/db", "postgresql://eu/db"
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


@pytest.fixture()
def deployment_org():
    """The boot's work is the deployment org's ("default" on a fresh AppState), bound as the boot
    binds it (REQ-1266)."""
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org("default")
    yield
    reset_current_org(token)


def test_the_boot_orgs_engine_is_the_one_its_region_names(deployment_org, monkeypatch):
    """REQ-1922: the boot org binds the engine its region names in the config file, the lane
    every other org in a region deployment is built on."""
    from provisa.api import app as app_mod

    built: list = []

    class _Engine:
        def __init__(self, kind):
            self.kind = kind

        def pin_materialize_store(self, dsn):
            built.append(f"store:{dsn}")

    class _Runtime:
        def __init__(self, engine, _state):
            self.engine = engine
            built.append(f"engine:{engine.kind}")

        def bind_terminal(self):
            built.append("bound")

    state = app_mod.AppState()
    monkeypatch.setattr(app_mod, "state", state)
    monkeypatch.setattr("provisa.federation.engine.build_engine", _Engine)
    monkeypatch.setattr("provisa.federation.runtime.EngineRuntime", _Runtime)
    process_region.bind_launch(_PLATFORM, requested="eu")
    app_mod._bind_boot_engine(
        {
            "regions": [r.model_dump() for r in _engine_region("eu-snow")],
            "stores": [s.model_dump() for s in _ENGINE_STORES],
        }
    )
    rt = state._active_runtime()
    # Its engine lands in the region's store, pinned before its first use (REQ-1922).
    assert built == ["engine:snowflake", "store:postgresql://eu/db", "bound"]
    assert (rt.isolated_engine, rt.engine_kind, rt.engine_url, rt.storage_url) == (
        True,
        "snowflake",
        "snowflake://acct/db",
        "postgresql://eu/db",
    )


def test_with_no_platform_regions_the_boot_engine_is_left_as_it_is(deployment_org, monkeypatch):
    from provisa.api import app as app_mod

    state = app_mod.AppState()
    monkeypatch.setattr(app_mod, "state", state)
    before = state.federation_engine
    process_region.bind_launch({}, requested=None)
    app_mod._bind_boot_engine({})
    assert state.federation_engine is before and not state._active_runtime().isolated_engine


def test_an_org_without_its_own_cache_is_served_the_deployments(deployment_org, monkeypatch):
    """REQ-1922: with no platform regions no runtime holds a cache of its own."""
    from provisa.api import app as app_mod
    from provisa.api.org_runtime import OrgRuntime
    from provisa.core.request_context import reset_current_org, set_current_org

    state = app_mod.AppState()
    deployment = object()
    state.response_cache_store = deployment
    state.org_registry.set("acme", OrgRuntime(org_id="acme"))
    token = set_current_org("acme")
    try:
        assert state.response_cache_store is deployment
        own = object()
        state._active_runtime().response_cache_store = own
        assert state.response_cache_store is own
    finally:
        reset_current_org(token)
    assert state.response_cache_store is deployment


async def test_in_a_region_an_org_keeps_its_cache_on_the_store_its_region_names(
    deployment_org, monkeypatch, control_plane, tmp_path
):
    from provisa.api import app as app_mod
    from provisa.core.database import Database

    state = app_mod.AppState()
    monkeypatch.setattr(app_mod, "state", state)
    monkeypatch.setattr(app_mod, "_region_caches", {})
    monkeypatch.setattr("provisa.core.settings_registry.value", lambda key: True)
    process_region.bind_launch(_PLATFORM, requested="eu")
    state.model_db = Database(
        control_plane, "org-model", search_path=None, model=None, holds="model"
    )
    await _declare(state.model_db, tmp_path)
    async with state.model_db.acquire() as conn:
        await upsert_store(
            conn, StoreConfig(id="eu-state", url="rediss://cache.eu:6379/0"), origin="config"
        )
    await app_mod._bind_region_cache("acme")
    rt = state._active_runtime()
    assert rt.response_cache_store is not None and rt.hot_counts is not None
    assert app_mod._region_caches.keys() == {"rediss://cache.eu:6379/0"}
    assert state.response_cache_store is rt.response_cache_store


def test_a_lanes_engine_lands_in_the_lanes_store_whoever_uses_it_first():
    """REQ-1922: an engine attaches its store once, at its first use — a boot reconcile or a
    background loop, with no org bound. A lane's engine is pinned to the lane's store, so that
    first use attaches the region's store, not the deployment's embedded default."""
    from provisa.core.request_context import current_org
    from provisa.federation.engine import build_engine

    lane = "postgresql://reader@eu-store:5432/eu"
    engine = build_engine("duckdb")
    unbound = current_org.set(None)  # a boot reconcile, a background loop: no org bound
    try:
        with pytest.raises(RuntimeError, match="No active org bound"):
            engine.materialize_store()  # unpinned: the store is a bound org's to decide (REQ-1266)
        engine.pin_materialize_store(lane)
        assert engine.materialize_store() == lane
    finally:
        current_org.reset(unbound)


def test_the_boot_reconcile_runs_once_the_orgs_regions_are_bound():
    """REQ-1922: the boot's landed-table reconcile asks, per table, whether it is read here or
    from another region's store — which reads the org's other regions. It ran before they were
    bound, so a node of one region raised KeyError naming another at boot."""
    import inspect

    from provisa.api import app as app_mod

    boot = inspect.getsource(app_mod._load_and_build)
    assert boot.index("await _bind_region_stores(") < boot.index("reconcile_landed_tables()")
