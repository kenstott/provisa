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
