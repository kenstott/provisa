# Copyright (c) 2026 Kenneth Stott
# Canary: 19d3b952-7d59-47de-819d-f800fa380e1d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a published demo's first example query answers, in a visitor's sandbox, the rows its
bundle's data holds, run as the role the query's comment names (REQ-2006, REQ-2009, REQ-2049).

The ``test`` instance. The demo's bundle is built as demo-bundles.yml builds it and served by a
sandbox tier as the cloud serves it (test_sandbox_visitor_walk's tiers: a regions demo on the
regions tier). A visitor's sandbox is opened through the public route and the demo's first
``queries/*.graphql`` is sent as the demos page's GraphQL explorer sends it -- the visitor's token,
and, when its leading comment says "Run as <role>", that role as X-Provisa-Role; else the
sandbox's own role. Each demo's count is what its bundle's data holds:

- retail: its 20 newest group customers (``limit: 20``; db/20-eu-shop.sql and db/30-us-shop.sql
  hold thousands), as group_analyst;
- profiling: one row per column of the customers table the profile runs measured (db/init.sql:
  10 columns);
- provisioning: one group per product category (db/init.sql: beans, equipment, merchandise and
  services), as finance, once the feeds' orders have landed."""

# Requirements: REQ-2006, REQ-2009, REQ-2049

from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path

import httpx
import pytest

from tests.integration.regions_tier import serves_on_regions_tier
from tests.integration.test_sandbox_visitor_walk import (
    _bundle,
    _open_sandbox,
    _plain_tier,
    _regions_tier,
)

pytestmark = [pytest.mark.integration, pytest.mark.local_platform, pytest.mark.timeout(3600)]

_NAMED = Path(__file__).resolve().parents[2] / "demo" / "named"

#: Each demo's first example query's row count, from its bundle's data (the module's docstring).
_EXPECTED = {"retail": 20, "profiling": 10, "provisioning": 4}
#: How long a demo whose data lands live after the sandbox opens (provisioning's feeds) is given.
_LANDING_S = {"provisioning": 300.0}


def run_as(text: str) -> str | None:
    """The role a query's leading comment says to run it as ("Run as group_analyst"), its words
    joined across wrapped comment lines, as the demo walk reads it; None when it names none."""
    said = " ".join(line.lstrip("#").strip() for line in text.splitlines() if line.startswith("#"))
    found = re.search(r"\bRun as ([a-z][a-z0-9_]*)", said)
    return found.group(1) if found else None


def test_run_as_reads_the_role_across_wrapped_comment_lines():
    assert run_as("# reporting. Run as\n# group_analyst.\nquery { x }") == "group_analyst"
    assert run_as("# Every animal\nquery { x }") is None


@pytest.fixture(params=sorted(_EXPECTED))
def tier(request, tmp_path_factory):
    """A sandbox tier serving the demo ``request.param`` from its bundle, as the cloud's does."""
    demo = request.param
    work = tmp_path_factory.mktemp(f"first_query_{demo}")
    bundle = _bundle(demo, work / "built")
    version = bundle.name.removeprefix(f"{demo}-").removesuffix(".tar.gz")
    serve = _regions_tier if serves_on_regions_tier(_NAMED / demo) else _plain_tier
    try:
        with serve(demo, bundle, work) as (server, bundles):
            server.demo = demo
            server.directory = bundles / demo / version
            yield server
    finally:
        project = f"provisa-demo-{demo}-{version}"
        subprocess.run(["docker", "compose", "-p", project, "down", "-v"], check=False)


def _rows(resp: httpx.Response) -> int:
    body = resp.json()
    assert not body.get("errors"), json.dumps(body)[:2000]
    data = body.get("data") or {}
    return sum(len(v) if isinstance(v, list) else 1 for v in data.values())


def test_the_first_example_query_answers_the_bundles_rows_as_its_role(tier):
    (first, *_) = sorted((tier.directory / "queries").glob("*.graphql"))
    text = first.read_text()
    role = run_as(text)
    sandbox = _open_sandbox(tier)
    headers = {"Authorization": f"Bearer {sandbox['token']}", "x-org-provisa": sandbox["org_id"]}
    if role is not None:
        headers["X-Provisa-Role"] = role
    expected = _EXPECTED[tier.demo]
    deadline = time.monotonic() + _LANDING_S.get(tier.demo, 0.0)
    while True:
        resp = httpx.post(
            f"{tier.base_url}/data/graphql", json={"query": text}, headers=headers, timeout=300
        )
        assert resp.status_code == 200, (first.name, role, resp.text[:2000])
        rows = _rows(resp)
        if rows == expected or time.monotonic() > deadline:
            break
        time.sleep(5)
    assert rows == expected, (
        f"{tier.demo}: {first.name} as {role or 'the sandbox role'} answered {rows} rows, "
        f"not {expected}: {resp.text[:1500]}\nserver log end:\n"
        f"{tier.dump_stderr_debug()[-8000:]}"
    )
