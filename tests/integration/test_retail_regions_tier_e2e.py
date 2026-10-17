# Copyright (c) 2026 Kenneth Stott
# Canary: 61ed557e-2f46-4f56-bca5-ed77acd7a9e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the retail demo, live on the regions sandbox tier (REQ-2068).

The ``test`` instance. The bundle is built as demo-bundles.yml builds it (scripts/demo_bundle.py
build: a seeding node per region, each landing what its region keeps -- eu its customers, orders,
staff and mail, and its views -- into that region's own captive PostgreSQL, and writing it out as
that region's seed) and published to a registry of this test's own. The node's sync pulls it and
runs the demo's source database. Then the regions tier -- one node, the us region, its own boot
org and config (config/provisa-sandbox-regions.yaml), with a PostgreSQL store per region and no eu
node at all -- lands its template from the region seeds, and a visitor's sandbox (an ``sbxr`` id)
reads EU customers, served from eu's store in place: the record names eu, and us's store holds no
EU customer row."""

# Requirements: REQ-2068, REQ-2035, REQ-1922

from __future__ import annotations

import re
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg
from psycopg import sql
import pytest

from tests.integration.test_region_read_in_place_e2e import _MASTER_KEY, _Stack

# The setup builds the bundle (up to 2400 s, the build itself bounded at 900 s) and boots the tier
# node (up to 900 s): the test's own limit covers what its setup is allowed, so a build that does
# not finish fails by its own bound, saying why, rather than being cut off silently.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(3600)]

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "demo_bundle.py"
_TIER_CONFIG = _REPO / "config" / "provisa-sandbox-regions.yaml"


@pytest.fixture(scope="module")
def tier(tmp_path_factory):
    from tests.integration.demo_registry import DemoRegistry
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("retail_regions_tier")
    out = work / "out"
    done = subprocess.run(
        [sys.executable, str(_SCRIPT), "build", "retail", "--out", str(out), "--timeout", "900"],
        capture_output=True,
        text=True,
        timeout=2400,
    )
    print(done.stdout[-4000:])
    assert done.returncode == 0, (done.stdout[-4000:], done.stderr[-8000:])
    (bundle,) = out.glob("retail-*.tar.gz")
    registry, stack = DemoRegistry(), _Stack()
    registry.start()
    stack.start()
    server = None
    try:
        registry.push(bundle)
        bundles = work / "data" / "demo-bundles"
        synced = registry.sync("retail", bundles)
        assert synced.returncode == 0, (synced.stdout, synced.stderr)
        control_plane = stack.url(stack.engine_port, "provisa", "+psycopg")
        server = IsolatedServer(
            "sbxrtiere2e",
            engine="duckdb",
            config=str(_TIER_CONFIG),
            control_plane="postgres",
            env={
                "PLATFORM_DATABASE_URL": control_plane,
                "TENANT_DATABASE_URL": control_plane,
                "PROVISA_REGION": "us",
                "PROVISA_ENCRYPTION_KEY": _MASTER_KEY,
                "PROVISA_DEMO_EU_ADDRESS": "http://127.0.0.1:1",
                "PROVISA_DEMO_US_ADDRESS": "http://127.0.0.1:2",
                "PROVISA_DEMO_EU_PG_URL": stack.url(stack.engine_port, "eu_store", "+psycopg"),
                "PROVISA_DEMO_US_PG_URL": stack.url(stack.engine_port, "us_store", "+psycopg"),
                "REDIS_URL": f"redis://127.0.0.1:{stack.redis_port}/0",
                "PROVISA_SANDBOX_TIER": "true",
                "PROVISA_DEMO_BUNDLES": "retail",
                "PROVISA_DEMO_REGISTRY": registry.url,
                "PROVISA_DATA_DIR": str(work / "data"),
            },
        )
        server.start(timeout=900)
        yield {"server": server, "stack": stack}
    finally:
        if server is not None:
            server.stop_process()
        stack.stop()
        registry.stop()


def _open(server) -> dict:
    deadline = time.monotonic() + 600
    while True:
        opened = httpx.post(f"{server.base_url}/public/demos/retail", timeout=600)
        if opened.status_code == 200:
            return opened.json()
        assert opened.json().get("code") == "sandbox.demo_not_ready", (
            opened.text,
            server.dump_stderr_debug()[-6000:],
        )
        assert time.monotonic() < deadline, server.dump_stderr_debug()[-6000:]
        time.sleep(3)


def _rows(server, sandbox: dict, statement: str, role: str) -> list[dict]:
    resp = httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": statement},
        headers={
            "Authorization": f"Bearer {sandbox['token']}",
            "x-org-provisa": sandbox["org_id"],
            "X-Provisa-Role": role,
        },
        timeout=300,
    )
    assert resp.status_code == 200, (resp.text, server.dump_stderr_debug()[-4000:])
    return resp.json()["data"]["sql"]


def test_a_visitor_reads_eu_customers_served_from_eus_store_with_no_eu_node(tier):
    server, stack = tier["server"], tier["stack"]
    sandbox = _open(server)
    assert sandbox["org_id"].startswith("sbxr"), sandbox
    rows = _rows(
        server,
        sandbox,
        'SELECT subsidiary, customer FROM "retail"."view_group_customers"',
        "group_analyst",
    )
    eu = [r["customer"] for r in rows if r["subsidiary"] == "eu"]
    assert eu and all(re.fullmatch(r"[0-9a-f]{32}", c) for c in eu), rows[:5]
    record = _rows(
        server,
        sandbox,
        'SELECT region FROM "ops"."queries" WHERE domain_id = \'retail\'',
        "org_admin",
    )
    assert any(r["region"] == "eu" for r in record), record
    # The sandbox's copy of eu's replicas is in eu's own store; us's holds no EU customer.
    with psycopg.connect(stack.url(stack.engine_port, "eu_store")) as conn:
        schemas = {
            r[0]
            for r in conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
        }
    assert any(s.startswith(f"org_{sandbox['org_id']}") for s in schemas), sorted(schemas)
    with psycopg.connect(stack.url(stack.engine_port, "us_store")) as conn:
        tables = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.columns "
            "WHERE column_name = 'customer_id'"
        ).fetchall()
        for schema, table in tables:
            (eu_rows,) = conn.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE customer_id < 200000").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            ).fetchone()
            assert eu_rows == 0, f"{schema}.{table} holds EU customers in us"
