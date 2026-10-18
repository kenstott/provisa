# Copyright (c) 2026 Kenneth Stott
# Canary: 4d9b1e72-6a3f-4c08-b5e2-9f17c3a8d640
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1922: a demo org built a second time on a node of a region resets a model that names its
region, and what a removed table leaves in its state store is removed from the region's store.
That store is bound before the reset: bound only after it, the reset failed ("the active org's
state store is not bound") and the org was left with no runtime -- a demo template rebuilt on its
region's lane at provisioning, or after a restart."""

from __future__ import annotations

import contextlib
from types import SimpleNamespace

import pytest

from provisa.api import app

_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.fixture
def node(monkeypatch):
    """A node of ``region`` (None: no platform regions) with a model selecting ``selected``."""
    from provisa.core import process_region

    was = process_region._region
    bound: list[tuple[str, str]] = []

    async def _bind(org_id, env, *, initialise):
        bound.append((org_id, env))

    monkeypatch.setattr(app, "_bind_region_stores", _bind)

    def make(region: str | None, selected: list[str]) -> list:
        if region is None:
            process_region._region = was
        else:
            process_region.bind_launch(_PLATFORM, requested=region)

        @contextlib.asynccontextmanager
        async def _acquire():
            yield object()

        async def _regions(_conn):
            return [SimpleNamespace(id=r) for r in selected]

        monkeypatch.setattr("provisa.core.repositories.region.list_regions", _regions)
        monkeypatch.setattr(
            app, "state", SimpleNamespace(model_db=SimpleNamespace(acquire=_acquire))
        )
        return bound

    yield make
    process_region._region = was


@pytest.mark.asyncio
async def test_a_second_build_binds_the_regions_stores_before_its_reset(node):
    bound = node("eu", ["eu", "us"])  # the model the first build seeded
    await app._bind_region_stores_before_reset("sbxtplretail", "prod")
    assert bound == [("sbxtplretail", "prod")]


@pytest.mark.asyncio
async def test_a_first_build_has_no_model_to_reset_and_binds_after_it_names_a_region(node):
    bound = node("eu", [])
    await app._bind_region_stores_before_reset("sbxtplretail", "prod")
    assert bound == []


@pytest.mark.asyncio
async def test_a_node_of_no_region_binds_nothing(node):
    bound = node(None, ["eu"])
    await app._bind_region_stores_before_reset("acme", "prod")
    assert bound == []
