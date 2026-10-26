# Copyright (c) 2026 Kenneth Stott
# Canary: 039f4fe1-558c-445d-86ca-2c61d70cbd98
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox tier node of the regions tier serving a demo from its bundle (REQ-2068), as the
cloud's regions node runs it: config/provisa-sandbox-regions.yaml, a control plane, the EU and US
stores and a Redis of its own (_Stack), the bundle in a registry of the caller's own, pulled by the
node's sync. Used by the retail regions tier e2e and the visitor walk of a demo that names regions.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

_REPO = Path(__file__).resolve().parents[2]
REGIONS_TIER_CONFIG = _REPO / "config" / "provisa-sandbox-regions.yaml"


def serves_on_regions_tier(directory: Path) -> bool:
    """Whether the demo at ``directory`` is served by the regions tier: its demo.yaml names
    regions (``regions: {visitor, headless}``)."""
    return bool(yaml.safe_load((directory / "demo.yaml").read_text()).get("regions"))


def visitor_region(demo: str) -> str:
    """The region ``demo``'s visitor is served in (its demo.yaml's ``regions.visitor``): the node
    that serves it runs as that region, as start-ui-install.sh starts it (REQ-2035)."""
    declared = yaml.safe_load((_REPO / "demo" / "named" / demo / "demo.yaml").read_text())
    return declared["regions"]["visitor"]


@contextlib.contextmanager
def regions_tier(
    org_id: str,
    demo: str,
    bundle: Path,
    work: Path,
    *,
    auth: dict[str, Any],
    env: dict[str, str] | None = None,
) -> Iterator[tuple[Any, Any, Path]]:
    """Serve ``demo`` from ``bundle`` on a regions tier node in its visitor region; yields the started
    server, its stack and the directory its bundles are synced into. ``env`` adds to the node's."""
    from tests.integration.demo_registry import DemoRegistry
    from tests.integration.isolated_server import IsolatedServer
    from tests.integration.test_region_read_in_place_e2e import _MASTER_KEY, _Stack

    registry, stack = DemoRegistry(), _Stack()
    registry.start()
    stack.start()
    # The tier's config declares every regions demo's regions (retail's and banking's): each is
    # given a store, as each node of the tier is.
    import psycopg

    with psycopg.connect(stack.url(stack.engine_port, "provisa"), autocommit=True) as conn:
        for region in ("ledger", "warehouse"):
            conn.execute(f"CREATE DATABASE {region}_store")
    server = None
    try:
        registry.push(bundle)
        bundles = work / "data" / "demo-bundles"
        synced = registry.sync(demo, bundles)
        assert synced.returncode == 0, f"{synced.stdout}\n{synced.stderr}"
        control_plane = stack.url(stack.engine_port, "provisa", "+psycopg")
        server = IsolatedServer(
            org_id,
            engine="duckdb",
            config=str(REGIONS_TIER_CONFIG),
            control_plane="postgres",
            auth=auth,
            env={
                "PLATFORM_DATABASE_URL": control_plane,
                "TENANT_DATABASE_URL": control_plane,
                "PROVISA_REGION": visitor_region(demo),
                "PROVISA_ENCRYPTION_KEY": _MASTER_KEY,
                "PROVISA_DEMO_EU_ADDRESS": "http://127.0.0.1:1",
                "PROVISA_DEMO_US_ADDRESS": "http://127.0.0.1:2",
                "PROVISA_DEMO_EU_PG_URL": stack.url(stack.engine_port, "eu_store", "+psycopg"),
                "PROVISA_DEMO_US_PG_URL": stack.url(stack.engine_port, "us_store", "+psycopg"),
                "PROVISA_DEMO_LEDGER_ADDRESS": "http://127.0.0.1:3",
                "PROVISA_DEMO_WAREHOUSE_ADDRESS": "http://127.0.0.1:4",
                "PROVISA_DEMO_LEDGER_PG_URL": stack.url(
                    stack.engine_port, "ledger_store", "+psycopg"
                ),
                "PROVISA_DEMO_WAREHOUSE_PG_URL": stack.url(
                    stack.engine_port, "warehouse_store", "+psycopg"
                ),
                "REDIS_URL": f"redis://127.0.0.1:{stack.redis_port}/0",
                "PROVISA_SANDBOX_TIER": "true",
                "PROVISA_DEMO_BUNDLES": demo,
                "PROVISA_DEMO_REGISTRY": registry.url,
                "PROVISA_DATA_DIR": str(work / "data"),
                **(env or {}),
            },
        )
        server.start(timeout=900)
        yield server, stack, bundles
    finally:
        if server is not None:
            server.stop_process()
        stack.stop()
        registry.stop()
