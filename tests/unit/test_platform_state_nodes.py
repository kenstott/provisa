# Copyright (c) 2026 Kenneth Stott
# Canary: e6db69af-f8e3-4ca3-ac49-d846f4691997
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The cluster's nodes (REQ-1916), kept in the PLATFORM STATE STORE: each node beats into the
platform database while it runs, and the node list shows every node that beat recently, with its
mode — and its region only when the platform declares regions."""

# Requirements: REQ-1916, REQ-1922

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from provisa.core import process_mode, process_region
from provisa.core.platform_state import nodes


@pytest.fixture
async def platform(tmp_path):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import cluster_nodes, metadata

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'platform.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[cluster_nodes])
    return Database(engine, "platform-state", holds="platform_state")


@pytest.fixture(autouse=True)
def _launch():
    mode, region = process_mode.mode(), process_region._region
    yield
    process_mode.set_mode(mode)
    process_region._region = region


async def test_a_running_node_is_listed_with_its_mode_and_region(platform):
    process_mode.set_mode(process_mode.QUERY)
    process_region.bind_launch(
        {"regions": [{"id": "eu", "address": "https://eu.example.com"}]}, requested="eu"
    )
    await nodes.register(platform)
    (node,) = await nodes.live(platform)
    assert (node["mode"], node["region"]) == ("query", "eu")
    assert node["node_id"] == nodes.NODE_ID and node["pid"] > 0 and node["host"]


async def test_without_platform_regions_the_list_carries_no_region(platform):
    process_region.bind_launch({}, requested=None)
    await nodes.register(platform)
    (node,) = await nodes.live(platform)
    assert "region" not in node


async def test_a_node_that_stopped_beating_drops_off_and_one_that_left_is_gone(platform):
    process_region.bind_launch({}, requested=None)
    await nodes.register(platform)
    later = datetime.now(UTC) + nodes.STALE_AFTER + timedelta(seconds=1)
    assert await nodes.live(platform, now=later) == []
    await nodes.beat(platform)
    assert len(await nodes.live(platform)) == 1
    await nodes.unregister(platform)
    assert await nodes.live(platform) == []


def test_the_platform_state_store_is_never_the_model_or_an_environments():
    """Platform state is per deployment: not an org table, never projected or copied."""
    from provisa.core.env_classes import CLASSIFIED
    from provisa.core.env_deploy import PROJECTED
    from provisa.core.schema_admin import metadata as platform_metadata
    from provisa.core.schema_org import metadata as org_metadata

    for table in nodes.TABLES:
        assert table in platform_metadata.tables
        assert table not in org_metadata.tables
        assert table not in PROJECTED and table not in CLASSIFIED


async def test_the_platform_state_handle_and_the_org_handles_keep_apart(tmp_path):
    """REQ-1922: the deployment's state is reached through its own handle alone, and that handle
    reaches no org table."""
    from sqlalchemy import select

    from provisa.core.database import Database, StoreSideViolation, create_engine_from_url
    from provisa.core.schema_admin import cluster_nodes
    from provisa.core.schema_admin import metadata as platform_metadata
    from provisa.core.schema_org import metadata as org_metadata
    from provisa.core.schema_org import registered_tables

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'one.db'}")
    with engine.begin() as raw:
        platform_metadata.create_all(raw, tables=[cluster_nodes])
        org_metadata.create_all(raw)
    platform_state_db = Database(engine, "platform-state", holds="platform_state")
    async with platform_state_db.acquire() as conn:
        await conn.execute_core(select(cluster_nodes.c.node_id))
        with pytest.raises(StoreSideViolation, match="registered_tables is a model table"):
            await conn.execute_core(select(registered_tables.c.id))
    for side in ("model", "state", "record"):
        async with Database(engine, side, holds=side).acquire() as conn:
            with pytest.raises(
                StoreSideViolation,
                match="cluster_nodes is a platform_state table, use the deployment's platform_state_db",
            ):
                await conn.execute_core(select(cluster_nodes.c.node_id))
