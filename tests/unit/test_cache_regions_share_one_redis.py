# Copyright (c) 2026 Kenneth Stott
# Canary: 091a18e1-faac-45c2-936f-74fb363b9df4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1922: regions may share one Redis, and never one another's cached answers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

_REGIONS = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.mark.asyncio
async def test_an_answer_cached_in_one_region_is_never_served_in_another():
    from provisa.cache.store import RedisCacheStore
    from provisa.cache.tenancy import cache_tenant
    from provisa.core import process_region

    store = RedisCacheStore(None)  # one Redis, both regions' nodes
    state = SimpleNamespace(org_id="acme", model_stamp=7)
    was = process_region._region
    try:
        process_region.bind_launch(_REGIONS, requested="eu")
        eu = cache_tenant(state)
        await store.set("same-statement", b"eu rows", ttl=60, tenant_id=eu)
        process_region.bind_launch(_REGIONS, requested="us")
        us = cache_tenant(state)
        assert us != eu
        assert await store.get("same-statement", tenant_id=us) is None
        process_region.bind_launch(_REGIONS, requested="eu")
        hit = await store.get("same-statement", tenant_id=cache_tenant(state))
        assert hit is not None and hit.data == b"eu rows"
    finally:
        process_region._region = was
