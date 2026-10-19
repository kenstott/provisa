# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox tier restarted serves its demos as they were built (REQ-2006).

A tier with several demos builds their templates once. Restarted on the same control plane and
data -- a deploy, or a wake from idle -- it serves every unchanged template at once: each demo says
it is ready on the first question asked after the start, none is prepared again, and a visitor's
open succeeds. The wake-to-first-open time is printed for both starts."""

# Requirements: REQ-2006

from __future__ import annotations

import time

import httpx
import pytest

from tests.integration.test_demo_sandboxes_e2e import (
    _JWT_SECRET,
    _demo,
    _deployment_config,
    _open,
)

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_DEMOS = ("alpha", "beta", "delta", "epsilon")


@pytest.fixture(scope="module")
def tier(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("demo_template_warm_start")
    demos = work / "named"
    for name in _DEMOS:
        _demo(demos, name, published=True)
    config = work / "config.yaml"
    _deployment_config(config)
    srv = IsolatedServer(
        "sbxwarme2e",
        engine="duckdb",
        config=str(config),
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={
            "PROVISA_SANDBOX_TIER": "true",
            "PROVISA_NAMED_DEMOS_DIR": str(demos),
            "PROVISA_DATA_DIR": str(work / "data"),
            "PROVISA_SANDBOX_MAX_CONCURRENT": "10",
        },
    )
    started = time.monotonic()
    srv.start(timeout=240)
    srv.first_start_s = time.monotonic() - started
    try:
        yield srv
    finally:
        srv.stop_process()


def _states(tier) -> dict[str, str]:
    return {
        name: httpx.get(f"{tier.base_url}/public/demos/{name}/status", timeout=60).json()["state"]
        for name in _DEMOS
    }


def _first_open_after(tier, started: float) -> float:
    deadline = started + 300
    while True:
        resp = _open(tier, "alpha")
        if resp.status_code == 200:
            return time.monotonic() - started
        assert resp.json().get("code") == "sandbox.demo_not_ready", resp.text
        assert time.monotonic() < deadline, tier.dump_stderr_debug()[-6000:]
        time.sleep(0.5)


def test_a_restarted_tier_serves_its_unchanged_templates_at_once(tier):
    cold_open = _first_open_after(tier, time.monotonic() - tier.first_start_s)
    deadline = time.monotonic() + 300
    while set(_states(tier).values()) != {"ready"}:
        assert time.monotonic() < deadline, _states(tier)
        time.sleep(1)

    started = time.monotonic()
    tier.restart(timeout=240)
    up = time.monotonic() - started
    assert _states(tier) == {name: "ready" for name in _DEMOS}
    warm_open = _first_open_after(tier, started)
    print(
        f"\nWAKE-TO-FIRST-OPEN cold start: {cold_open:.1f}s; "
        f"restart: server up {up:.1f}s, first open {warm_open:.1f}s"
    )
    # Nothing was prepared again: no template waited for its replicas at this start.
    assert "every replica landed" not in tier.dump_stderr_debug()
