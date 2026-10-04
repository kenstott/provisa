# Copyright (c) 2026 Kenneth Stott
# Canary: 1209619c-1831-4ff7-ab52-d5e74d335dd9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regions in the model (REQ-1921, REQ-1922): the platform declares physical regions, an org
selects the ones it uses and declares its stores in each, and a source or table may name one of
the org's regions. Everything is checked when the model is loaded."""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.core.models import ProvisaConfig

_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}
_STORES = [
    {"id": "eu-pg", "url": "postgresql://eu/db"},
    {"id": "eu-redis", "url": "redis://eu:6379/0"},
    {"id": "eu-trino", "url": "trino://eu:8080", "kind": "trino-byo"},
    {"id": "us-pg", "url": "postgresql://us/db"},
    {"id": "us-redis", "url": "redis://us:6379/0"},
    {"id": "us-trino", "url": "trino://us:8080", "kind": "trino-byo"},
]


def _region(rid: str, **over) -> dict:
    stores = {
        "engine": f"{rid}-trino",
        "replicas": f"{rid}-pg",
        "views": f"{rid}-pg",
        "cache": f"{rid}-redis",
        "state": f"{rid}-pg",
        "record": f"{rid}-pg",
    }
    return {"id": rid, **stores, **over}


def _config(**over) -> dict:
    base = {
        "sources": [
            {
                "id": "crm",
                "type": "postgresql",
                "host": "h",
                "database": "d",
                "username": "u",
                "password": "p",
            }
        ],
        "domains": [{"id": "sales"}],
        "tables": [
            {
                "source_id": "crm",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [{"name": "id", "visible_to": ["admin"]}],
            }
        ],
        "roles": [{"id": "admin", "capabilities": [], "domain_access": ["*"]}],
    }
    base.update(over)
    return base


def _refused(cfg: dict) -> str:
    with pytest.raises(ValidationError) as refused:
        ProvisaConfig.model_validate(cfg)
    return str(refused.value)


# -- no platform regions: one implicit region, nothing about regions may be said ----------------


def test_without_platform_regions_a_model_names_none():
    cfg = ProvisaConfig.model_validate(_config())
    assert cfg.platform.regions == [] and cfg.regions == [] and cfg.stores == []


def test_without_platform_regions_a_region_anywhere_is_refused_naming_that():
    said = _refused(_config(tables=[{**_config()["tables"][0], "region": "eu"}]))
    assert "the platform declares no regions" in said
    said = _refused(_config(regions=[_region("eu")], stores=_STORES))
    assert "the platform declares no regions" in said


# -- the platform declares regions; the org selects its own -------------------------------------


def test_an_org_selects_platform_regions_and_names_them_on_sources_and_tables():
    cfg = ProvisaConfig.model_validate(
        _config(
            platform=_PLATFORM,
            stores=_STORES,
            regions=[_region("eu"), _region("us")],
            sources=[{**_config()["sources"][0], "region": "eu"}],
            tables=[{**_config()["tables"][0], "region": "us"}],
        )
    )
    assert [r.id for r in cfg.regions] == ["eu", "us"]
    assert cfg.sources[0].region == "eu" and cfg.tables[0].region == "us"


def test_an_org_that_selects_no_region_is_refused_naming_the_platforms():
    said = _refused(_config(platform=_PLATFORM))
    assert "selects none of the platform's regions" in said
    assert "eu (https://eu.example.com)" in said and "us (https://us.example.com)" in said


def test_a_region_the_platform_does_not_declare_is_refused():
    said = _refused(_config(platform=_PLATFORM, stores=_STORES, regions=[_region("ap")]))
    assert "region 'ap' is not one of the platform's (eu, us)" in said


def test_a_store_no_store_declares_is_refused():
    said = _refused(
        _config(platform=_PLATFORM, stores=_STORES, regions=[_region("eu", state="eu-mysql")])
    )
    assert "region 'eu' state store 'eu-mysql' is not declared" in said


def test_a_source_or_table_region_the_org_does_not_select_is_refused():
    said = _refused(
        _config(
            platform=_PLATFORM,
            stores=_STORES,
            regions=[_region("eu")],
            tables=[{**_config()["tables"][0], "region": "us"}],
        )
    )
    assert "table crm/public.orders names region 'us', which the org does not select (eu)" in said


def test_a_regions_engine_store_names_an_engine_kind():
    """REQ-1922: a URL does not identify an engine kind, so the engine store names one."""
    no_kind = [{**s, "kind": None} if s["id"] == "eu-trino" else s for s in _STORES]
    said = _refused(_config(platform=_PLATFORM, stores=no_kind, regions=[_region("eu")]))
    assert "region 'eu' engine store 'eu-trino' names no engine kind" in said
    bad = [{**s, "kind": "oracle-rac"} if s["id"] == "eu-trino" else s for s in _STORES]
    said = _refused(_config(platform=_PLATFORM, stores=bad, regions=[_region("eu")]))
    assert "names engine kind 'oracle-rac', which is not one of" in said


def test_a_region_keeps_its_replicas_and_views_in_one_store():
    """MAINTAINER (REQ-1922): one store for both, for now; the model keeps both fields."""
    stores = [*_STORES, {"id": "eu-pg2", "url": "postgresql://eu2/db"}]
    said = _refused(
        _config(platform=_PLATFORM, stores=stores, regions=[_region("eu", views="eu-pg2")])
    )
    assert "names replicas store 'eu-pg' and views store 'eu-pg2'" in said


def test_a_region_id_must_be_a_short_lowercase_name():
    said = _refused(_config(platform={"regions": [{"id": "EU-west", "address": "https://x"}]}))
    assert "EU-west" in said


def test_a_region_others_read_needs_a_replica_store_they_can_attach():
    """A table naming eu is read from eu's replica by the org's other regions: an embedded
    DuckDB file is reachable by no other engine."""
    stores = [*_STORES, {"id": "eu-duck", "url": "duckdb:///data/eu.duckdb"}]
    said = _refused(
        _config(
            platform=_PLATFORM,
            stores=stores,
            regions=[_region("eu", replicas="eu-duck", views="eu-duck"), _region("us")],
            tables=[{**_config()["tables"][0], "region": "eu"}],
        )
    )
    assert "region 'eu' replicas store 'eu-duck' is an embedded DuckDB file" in said
    assert "table crm/public.orders keeps its data there" in said


def test_a_region_others_read_needs_a_postgresql_replica_store():
    stores = [*_STORES, {"id": "eu-my", "url": "mysql://eu/db"}]
    said = _refused(
        _config(
            platform=_PLATFORM,
            stores=stores,
            regions=[_region("eu", replicas="eu-my", views="eu-my"), _region("us")],
            sources=[{**_config()["sources"][0], "region": "eu"}],
        )
    )
    assert "region 'eu' replicas store 'eu-my' is not a PostgreSQL store" in said
    assert "source crm keeps its data there" in said


def test_a_region_whose_engine_cannot_read_another_region_is_refused_naming_both():
    """A table kept in eu is read by us in place: us's engine must be one that can."""
    stores = [*_STORES, {"id": "us-snow", "url": "snowflake://acct/db", "kind": "snowflake"}]
    said = _refused(
        _config(
            platform=_PLATFORM,
            stores=stores,
            regions=[_region("eu"), _region("us", engine="us-snow")],
            tables=[{**_config()["tables"][0], "region": "eu"}],
        )
    )
    assert (
        "region 'us' runs the snowflake engine, which cannot read another region's replicas, "
        "and table crm/public.orders keeps its data in region 'eu'"
    ) in said
    # One region, nothing is read elsewhere: any engine.
    ProvisaConfig.model_validate(
        _config(
            platform=_PLATFORM,
            stores=stores,
            regions=[_region("us", engine="us-snow")],
            tables=[{**_config()["tables"][0], "region": "us"}],
        )
    )


# -- saved through the model store ---------------------------------------------------------------


@pytest.fixture
async def model(tmp_path):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_org import metadata

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'model.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw)  # the whole model: a guard reads every table that may refer
    return Database(engine, "test")


def _source(region: str | None):
    from provisa.core.models import Source

    return Source(
        id="crm",
        type="postgresql",
        host="h",
        database="d",
        username="u",
        password="",
        region=region,
    )


async def test_a_source_saved_naming_a_region_the_org_does_not_select_is_refused(model):
    from provisa.core.regions import OrgRegion, StoreConfig
    from provisa.core.repositories import region as region_repo
    from provisa.core.repositories import source as source_repo

    async with model.acquire() as conn:
        with pytest.raises(region_repo.RegionNotSelected, match="source crm names region 'eu'"):
            await source_repo.upsert(conn, _source("eu"), origin="admin")
        for s in _STORES[:3]:
            await region_repo.upsert_store(conn, StoreConfig(**s), origin="admin")
        await region_repo.upsert_region(conn, OrgRegion(**_region("eu")), origin="admin")
        await source_repo.upsert(conn, _source("eu"), origin="admin")
        assert [r.id for r in await region_repo.list_regions(conn)] == ["eu"]
        with pytest.raises(
            region_repo.RegionNotSelected, match=r"which the org does not select \(eu\)"
        ):
            await source_repo.upsert(conn, _source("us"), origin="admin")


async def test_a_region_saved_with_an_engine_store_of_no_kind_is_refused(model):
    from provisa.core.regions import OrgRegion, StoreConfig
    from provisa.core.repositories import region as region_repo

    async with model.acquire() as conn:
        for s in _STORES[:3]:
            await region_repo.upsert_store(conn, StoreConfig(**{**s, "kind": None}), origin="admin")
        with pytest.raises(ValueError, match="engine store 'eu-trino' names no engine kind"):
            await region_repo.upsert_region(conn, OrgRegion(**_region("eu")), origin="admin")
        with pytest.raises(ValueError, match="state store 'eu-mysql' is not declared"):
            await region_repo.upsert_region(
                conn, OrgRegion(**_region("eu", state="eu-mysql")), origin="admin"
            )


async def test_a_region_saved_with_replicas_and_views_apart_is_refused(model):
    from provisa.core.regions import OrgRegion, StoreConfig
    from provisa.core.repositories import region as region_repo

    async with model.acquire() as conn:
        for s in [*_STORES[:3], {"id": "eu-pg2", "url": "postgresql://eu2/db"}]:
            await region_repo.upsert_store(conn, StoreConfig(**s), origin="admin")
        with pytest.raises(ValueError, match="keeps its replicas and its views in one store"):
            await region_repo.upsert_region(
                conn, OrgRegion(**_region("eu", views="eu-pg2")), origin="admin"
            )


async def test_a_region_a_source_names_and_a_store_a_region_names_are_held(model):
    from provisa.core.regions import OrgRegion, StoreConfig
    from provisa.core.repositories import region as region_repo
    from provisa.core.repositories import source as source_repo
    from provisa.core.repositories.integrity import ObjectRef, guard

    async with model.acquire() as conn:
        for s in _STORES[:3]:
            await region_repo.upsert_store(conn, StoreConfig(**s), origin="admin")
        await region_repo.upsert_region(conn, OrgRegion(**_region("eu")), origin="admin")
        await source_repo.upsert(conn, _source("eu"), origin="admin")
        assert [d.ref for d in await guard(conn, ObjectRef("region", "eu"))] == [
            ObjectRef("source", "crm")
        ]
        assert {d.ref for d in await guard(conn, ObjectRef("store", "eu-pg"))} == {
            ObjectRef("region", "eu")
        }


async def test_a_node_serves_an_org_only_in_a_region_the_org_selects(model):
    """A node runs in one platform region and serves that region of every org that selects it;
    an org that does not select it is refused there by name."""
    from provisa.core.regions import DEFAULT_REGION, OrgRegion, StoreConfig
    from provisa.core.repositories import region as region_repo

    async with model.acquire() as conn:
        for s in _STORES[:3]:
            await region_repo.upsert_store(conn, StoreConfig(**s), origin="admin")
        await region_repo.upsert_region(conn, OrgRegion(**_region("eu")), origin="admin")
        await region_repo.require_serves_here(conn, "acme", "eu")
        with pytest.raises(region_repo.OrgNotInRegion) as refused:
            await region_repo.require_serves_here(conn, "acme", "us")
        assert str(refused.value) == (
            "org 'acme' does not select region 'us', which this node serves (it selects eu)"
        )
        # The one implicit region: every org is served, none selects anything.
        await region_repo.require_serves_here(conn, "acme", DEFAULT_REGION)


def test_the_engine_kinds_the_model_names_are_the_ones_built():
    from provisa.core.engine_kinds import ENGINE_KINDS
    from provisa.federation.engine import engine_kinds

    assert ENGINE_KINDS == engine_kinds()


def test_the_engine_kinds_that_read_other_regions_are_the_backends_that_do(monkeypatch):
    from provisa.core.engine_kinds import REGION_READERS
    from provisa.federation.engine import build_engine, engine_kinds

    # A generic SQLAlchemy engine kind is built only with a URL; none is dialed.
    monkeypatch.setenv("PROVISA_ENGINE_URL", "postgresql://h/db")
    readers = {k for k in engine_kinds() if build_engine(k).backend.reads_other_regions}
    assert readers == REGION_READERS


async def test_a_save_that_leaves_a_region_unreadable_by_the_others_is_refused(model):
    """Saving refuses what loading refuses — from whichever side the change comes: the source
    naming the region, the other region's engine, or the region's replicas store."""
    from provisa.core.regions import OrgRegion, StoreConfig
    from provisa.core.repositories import region as region_repo
    from provisa.core.repositories import source as source_repo

    snow = StoreConfig(id="us-snow", url="snowflake://acct/db", kind="snowflake")
    async with model.acquire() as conn:
        for s in _STORES:
            await region_repo.upsert_store(conn, StoreConfig(**s), origin="admin")
        await region_repo.upsert_store(conn, snow, origin="admin")
        await region_repo.upsert_region(conn, OrgRegion(**_region("eu")), origin="admin")
        await region_repo.upsert_region(
            conn, OrgRegion(**_region("us", engine="us-snow")), origin="admin"
        )
        with pytest.raises(ValueError, match="region 'us' runs the snowflake engine"):
            await source_repo.upsert(conn, _source("eu"), origin="admin")
        await region_repo.upsert_region(conn, OrgRegion(**_region("us")), origin="admin")
        await source_repo.upsert(conn, _source("eu"), origin="admin")
        with pytest.raises(ValueError, match="region 'us' runs the snowflake engine"):
            await region_repo.upsert_region(
                conn, OrgRegion(**_region("us", engine="us-snow")), origin="admin"
            )
        with pytest.raises(ValueError, match="'eu-pg' is not a PostgreSQL store"):
            await region_repo.upsert_store(
                conn, StoreConfig(id="eu-pg", url="mysql://eu/db"), origin="admin"
            )
