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
import shutil
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg
from psycopg import sql
import pytest

from tests.integration.test_demo_sandboxes_e2e import _JWT_SECRET
from tests.integration.regions_tier import regions_tier

# The setup builds the bundle (up to 2400 s, the build itself bounded at 900 s) and boots the tier
# node (up to 900 s): the test's own limit covers what its setup is allowed, so a build that does
# not finish fails by its own bound, saying why, rather than being cut off silently.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(3600)]

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "demo_bundle.py"


@pytest.fixture(scope="module")
def tier(tmp_path_factory):
    work = tmp_path_factory.mktemp("retail_regions_tier")
    out = work / "out"
    # The tier offers, and a bundle build's seeding node templates, only a published demo: retail
    # is one.
    named = work / "named"
    shutil.copytree(_REPO / "demo" / "named" / "retail", named / "retail")
    done = subprocess.run(
        [sys.executable, str(_SCRIPT), "build", "retail", "--out", str(out), "--timeout", "900"]
        + ["--named-dir", str(named)],
        capture_output=True,
        text=True,
        timeout=2400,
    )
    print(done.stdout[-4000:])
    assert done.returncode == 0, f"{done.stdout[-4000:]}\n{done.stderr[-12000:]}"
    (bundle,) = out.glob("retail-*.tar.gz")
    # A visitor's token is read only by a configured provider: unsecured, every request is the
    # anonymous principal on the boot org, never the visitor's sandbox.
    auth = {"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"}
    with regions_tier("sbxrtiere2e", "retail", bundle, work, auth=auth) as (server, stack, _):
        yield {"server": server, "stack": stack}


def _open(server) -> dict:
    deadline = time.monotonic() + 600
    while True:
        opened = httpx.post(f"{server.base_url}/public/demos/retail", timeout=600)
        if opened.status_code == 200:
            return opened.json()
        assert opened.json().get("code") == "sandbox.demo_not_ready", (
            f"{opened.text}\n{server.dump_stderr_debug()[-6000:]}"
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
    assert resp.status_code == 200, f"{resp.text}\n{server.dump_stderr_debug()[-12000:]}"
    return resp.json()["data"]["sql"]


def test_a_visitor_reads_eu_customers_served_from_eus_store_with_no_eu_node(tier):
    server, stack = tier["server"], tier["stack"]
    sandbox = _open(server)
    assert sandbox["org_id"].startswith("sbxr"), sandbox
    rows = _rows(
        server,
        sandbox,
        # The EU rows: the view lists the US ones first, and the demo answers 100 rows at most.
        'SELECT subsidiary, customer FROM "retail"."view_group_customers" '
        "WHERE subsidiary = 'eu'",
        "group_analyst",
    )
    eu = [r["customer"] for r in rows if r["subsidiary"] == "eu"]
    assert eu and all(re.fullmatch(r"[0-9a-f]{32}", c) for c in eu), rows[:5]
    # The visitor's own role reads the sandbox's record; a statement is recorded once answered.
    from provisa.api.demo_sandbox import SANDBOX_ROLE

    deadline = time.monotonic() + 60
    while True:
        record = _rows(
            server,
            sandbox,
            'SELECT region FROM "ops"."queries" WHERE domain_id = \'retail\'',
            SANDBOX_ROLE,
        )
        if any(r["region"] == "eu" for r in record):
            break
        assert time.monotonic() < deadline, record
        time.sleep(2)
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
