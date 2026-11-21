# Copyright (c) 2026 Kenneth Stott
# Canary: 8a2f6c19-4e7d-4b30-9c51-d3e7b0a4f826
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2068, REQ-1922: an org provisioned on a node of a region runs on that region's lane.

Provisioning builds a new org (a demo template among them) before its model -- which that build
seeds -- names a region; that build had no region lane and pinned the embedded file, and a regions
seeding node's replicas landed there, never in the region's store its seed is written from. The
org is built again on the lane its seeded model names, as every later build of it is."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api import app
from provisa.api.admin import orgs_router
from provisa.core.region_stores import RegionLane
from provisa.core.repositories.region import OrgNotInRegion

_EU = RegionLane(
    "duckdb", None, "duckdb:////data/eu-engine.duckdb", "postgresql://eu/demo", "redis://c"
)


@pytest.fixture
def registry(monkeypatch):
    """The org registry provisioning builds into: each rebuild runs its builder."""
    rebuilt: list[Any] = []

    async def _rebuild(org_id, builder):
        rebuilt.append(await builder(org_id))
        return rebuilt[-1]

    monkeypatch.setattr(
        app, "state", SimpleNamespace(org_registry=SimpleNamespace(rebuild=_rebuild))
    )
    return rebuilt


def _lane(lane):
    async def _region_lane_of(org_id, env):
        if isinstance(lane, Exception):
            raise lane
        return lane

    return _region_lane_of


@pytest.mark.asyncio
async def test_off_a_region_the_provisioned_runtime_stands(registry, monkeypatch):
    monkeypatch.setattr(app, "_region_lane_of", _lane(None))
    first = object()
    assert await orgs_router.on_its_region_lane("sbxtplretail", first) is first
    assert registry == []


@pytest.mark.asyncio
async def test_on_a_region_the_org_is_built_again_on_the_lane_its_model_names(
    registry, monkeypatch
):
    monkeypatch.setattr(app, "_region_lane_of", _lane(_EU))
    built: list[dict] = []

    async def _build(org_id, **kwargs):
        built.append({"org": org_id, **kwargs})
        return SimpleNamespace(org_id=org_id, storage_url=kwargs["storage_url"])

    monkeypatch.setattr(app, "build_org_runtime", _build)

    async def _flags(org_id):
        return app.OrgLane(True, False, None, None, None, "", None)

    monkeypatch.setattr(app, "_read_org_flags", _flags)
    rt = await orgs_router.on_its_region_lane("sbxtplretail", object())
    assert rt.storage_url == "postgresql://eu/demo"
    [call] = built
    assert call["engine_url"] == _EU.url and call["engine_kind"] == "duckdb"
    assert call["include_demo"] is True


@pytest.mark.asyncio
async def test_an_org_that_does_not_select_the_nodes_region_is_refused(registry, monkeypatch):
    monkeypatch.setattr(app, "_region_lane_of", _lane(OrgNotInRegion("acme", "eu", ["us"])))
    with pytest.raises(OrgNotInRegion):
        await orgs_router.on_its_region_lane("acme", object())


@pytest.mark.asyncio
async def test_the_lane_build_is_an_ordinary_build_whose_schema_says_it_is_current(
    registry, monkeypatch
):
    """The org was laid out a moment ago -- by provision_org, and again by the provisioning build
    this one replaces -- and that build's lifecycle wiring, detached from it, is still reading
    the org's model. schema.sql's ALTER TABLEs, run again here, took AccessExclusive locks beside
    that reader's and deadlocked it (the regions tier's retail template, sbxtplretail). The
    schema's layout stamp now says so (test_schema_layout_stamp); the caller passes nothing."""
    monkeypatch.setattr(app, "_region_lane_of", _lane(_EU))
    built: list[dict] = []

    async def _build(org_id, **kwargs):
        built.append(kwargs)
        return SimpleNamespace(org_id=org_id, storage_url=kwargs["storage_url"])

    monkeypatch.setattr(app, "build_org_runtime", _build)

    async def _flags(org_id):
        return app.OrgLane(True, False, None, None, None, "", None)

    monkeypatch.setattr(app, "_read_org_flags", _flags)
    await orgs_router.on_its_region_lane("sbxtplretail", object())
    await app.build_on_its_lane("sbxtplretail", None)  # any later build of it
    assert built[0] == built[1]
    assert "schema_current" not in built[0]


@pytest.mark.asyncio
async def test_a_schema_already_current_is_given_its_grants_and_no_ddl(monkeypatch):
    from provisa.core import db, org_provisioning

    ran: list[str] = []
    current = {"stamp": False}

    async def _lay_out(*args, **kwargs):
        if current["stamp"]:
            return False
        ran.append("layout")
        return True

    async def _grants(*args, **kwargs):
        ran.append("grants")

    monkeypatch.setattr(org_provisioning, "lay_out_schema", _lay_out)
    monkeypatch.setattr(db, "apply_tenancy_role_grants", _grants)
    monkeypatch.setattr(app, "state", SimpleNamespace(model_db=object(), multitenancy=True))
    current["stamp"] = True
    await app._set_up_org_schema("sbxtplretail", "prod")
    assert ran == ["grants"]
    ran.clear()
    current["stamp"] = False
    await app._set_up_org_schema("sbxtplretail", "prod")
    assert ran == ["layout", "grants"]
