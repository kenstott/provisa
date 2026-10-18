# Copyright (c) 2026 Kenneth Stott
# Canary: 8a2f6c19-4e7d-4b30-9c51-d3e7b0a4f826
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2068: a demo template's first build, on a node of a region, lands in that region's store.

The build is its provisioning's, before its model -- which that build seeds from the demo's
configuration -- names a region; it took the embedded file, and a regions seeding node's replicas
landed there, never in the region's store its seed is written from."""

from __future__ import annotations

import pytest

from provisa.api import demo_region_seed, demo_templates
from provisa.core.models import ProvisaConfig

_SEED = {
    "platform": {
        "regions": [
            {"id": "eu", "address": "https://eu.example.com"},
            {"id": "us", "address": "https://us.example.com"},
        ]
    },
    "stores": [
        {"id": "eu_engine", "url": "duckdb:////data/eu-engine.duckdb", "kind": "duckdb"},
        {"id": "us_engine", "url": "duckdb:////data/us-engine.duckdb", "kind": "duckdb"},
        {"id": "eu_data", "url": "postgresql://eu-store/demo"},
        {"id": "us_data", "url": "postgresql://us-store/demo"},
        {"id": "cache", "url": "redis://cache:6379/0"},
    ],
    "regions": [
        {"id": r, "engine": f"{r}_engine", "replicas": f"{r}_data", "views": f"{r}_data",
         "cache": "cache", "state": f"{r}_data", "record": f"{r}_data"}
        for r in ("eu", "us")
    ],
    "sources": [],
    "domains": [{"id": "retail"}],
    "tables": [],
    "roles": [{"id": "admin", "description": "Test role.", "capabilities": [], "domain_access": ["*"]}],
}  # fmt: skip


@pytest.fixture
def on_eu(monkeypatch: pytest.MonkeyPatch):
    from provisa.core import process_region

    was = process_region._region
    process_region.bind_launch(_SEED["platform"], requested="eu")
    monkeypatch.setattr(
        demo_templates, "template_seed", lambda _org: ProvisaConfig.model_validate(_SEED)
    )
    yield
    process_region._region = was


def test_a_templates_store_is_its_regions_as_its_demo_names_it(on_eu):
    assert demo_region_seed.template_region_store("sbxtplretail") == "postgresql://eu-store/demo"
