# Copyright (c) 2026 Kenneth Stott
# Canary: c9519e47-8f38-498a-800c-82b3b72a19a0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo opened again, its data already loaded, opens in seconds (REQ-2006).

The tier builds the demo's template at its start; a visitor opens a sandbox of it and closes its
tab; a second visitor then opens the same demo. Nothing of that second open waits on a store or
on the template: what it spends is the server's own work of copying the template into a sandbox,
timed by the open's stages (demo_sandbox._stage), which the server logs. The second open's stages
add up to under ``_STEADY_BOUND_S``."""

# Requirements: REQ-2006, REQ-2049

from __future__ import annotations

import re

import httpx
import pytest

from tests.integration.test_demo_sandboxes_e2e import (
    _JWT_SECRET,
    _close_opened,
    _demo,
    _deployment_config,
    _opened,
)

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "sbxsteadyopene2e"
#: The server's time for an open of a demo whose template is ready and whose data is loaded. The
#: maintainer accepted the measured ~11 s (2026-11-03; 10.6 s in CI run 41630107701), most of it the
#: sandbox's model history (REQ-1524), which stays as it is.
_STEADY_BOUND_S = 11.0
#: The stages of an open that follow one another (demo_sandbox._open and _build); the stages
#: inside "its demo's readiness" are within it, and are not added again.
_TOP_STAGES = (
    "its demo's readiness",
    "a place",
    "its schema",
    "the template's model",
    "its uploads, secrets and files",
    "where its template's landing stopped",
    "the template's store",
    "the template's views",
    "its runtime",
    "its person's membership and roles",
    "its record, environments and ready mark",
)
_STAGE = re.compile(r"demo sandbox (sbx[0-9a-z]+) of [a-z0-9]+: (.+) took ([0-9.]+)s")
_STEP = re.compile(r"(?:runtime|seed) of (sbx[0-9a-z]+)[^:]*: (.+) took ([0-9.]+)s")


@pytest.fixture(scope="module")
def tier(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("demo_open_steady")
    demos = work / "named"
    _demo(demos, "alpha", published=True)
    config = work / "config.yaml"
    _deployment_config(config)
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(config),
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={
            "PROVISA_SANDBOX_TIER": "true",
            "PROVISA_NAMED_DEMOS_DIR": str(demos),
            "PROVISA_DATA_DIR": str(work / "data"),
        },
    )
    srv.start(timeout=240)
    try:
        yield srv
        _close_opened(srv)  # the control plane is shared: none of its sandboxes is left open
    finally:
        srv.stop_process()


def _steps(log: str, org_id: str) -> dict[str, float]:
    """The timed stages of sandbox ``org_id``'s open, and the runtime build's slow steps, by name
    (a runtime step is prefixed ``runtime: ``)."""
    steps: dict[str, float] = {}
    for line in log.splitlines():
        if (m := _STAGE.search(line)) and m.group(1) == org_id:
            steps[m.group(2)] = steps.get(m.group(2), 0.0) + float(m.group(3))
        elif (m := _STEP.search(line)) and m.group(1) == org_id:
            steps[f"runtime: {m.group(2)}"] = float(m.group(3))
    return steps


def _close(tier, sandbox: dict) -> None:
    resp = httpx.post(
        f"{tier.base_url}/sandboxes/closing",
        headers={"Authorization": f"Bearer {sandbox['token']}", "x-org-provisa": sandbox["org_id"]},
        timeout=60,
    )
    assert resp.status_code == 204, resp.text


def test_a_demo_opened_again_opens_in_seconds(tier):
    first = _opened(tier, "alpha")  # waits for the template its start builds
    _close(tier, first)
    second = _opened(tier, "alpha")
    log = tier.dump_stderr_debug()
    steps = _steps(log, second["org_id"])
    table = "\n".join(f"{name:45s} {took:7.1f}s" for name, took in steps.items())
    print(f"second open {second['org_id']}:\n{table}")
    # Each stage the demo has: alpha has no replica, view or Kafka landing to copy.
    optional = {
        "where its template's landing stopped",
        "the template's store",
        "the template's views",
    }
    assert set(_TOP_STAGES) - optional <= set(steps), table
    server_s = sum(steps.get(name, 0.0) for name in _TOP_STAGES)
    assert server_s < _STEADY_BOUND_S, f"second open took {server_s:.1f}s on the server:\n{table}"
