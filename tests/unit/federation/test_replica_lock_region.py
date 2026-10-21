"""REQ-1915, REQ-1922: one build of a replica at a time is a limit each cluster applies to itself.
Each region builds a table's copy into its own store, so a build in eu and one in us do not wait on
each other; two builds of it in one region still do. The one implicit region keeps today's name."""

# Requirements: REQ-1915, REQ-1922
from __future__ import annotations

import pytest

from provisa.core import process_region
from provisa.federation.replica_locks import BuildLocks, replica_lock_name

pytestmark = pytest.mark.unit

ORG = "org1"
KEY = ("src", "public", "orders")
_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.fixture
def region():
    was = process_region._region, process_region._platform_regions
    yield process_region
    process_region._region, process_region._platform_regions = was


def test_the_implicit_region_keeps_its_lock_name(region):
    region.bind_launch(None, requested=None)
    assert replica_lock_name(ORG, KEY) == "org1|src|public|orders"


async def test_two_regions_build_one_table_at_once_and_one_region_builds_it_once(
    region, control_plane
):
    locks = BuildLocks(control_plane.url)
    region.bind_launch(_PLATFORM, requested="eu")
    eu, eu_again = locks.claim(), locks.claim()
    region.bind_launch(_PLATFORM, requested="us")
    us = locks.claim()
    try:
        region.bind_launch(_PLATFORM, requested="eu")
        assert eu.try_replica(ORG, KEY)
        assert not eu_again.try_replica(ORG, KEY)  # one build of eu's copy at a time
        region.bind_launch(_PLATFORM, requested="us")
        assert us.try_replica(ORG, KEY)  # us builds its own copy beside eu's
    finally:
        eu.close()
        eu_again.close()
        us.close()
