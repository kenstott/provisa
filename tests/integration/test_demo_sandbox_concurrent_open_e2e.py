# Copyright (c) 2026 Kenneth Stott
# Canary: 4e27f82c-0edc-498f-81d3-038d9bc8c375
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Visitors opening demo sandboxes at the same moment (REQ-2006).

Every request runs on a thread and event loop of its own. The places were counted under an
asyncio lock made once for the module, bound to the first loop that used it, so a second
visitor's open, on another loop, failed with a 500 ("bound to a different event loop"). They are
counted under the control plane's advisory lock: concurrent opens all succeed until the cap, and
exactly ``sandbox.max_concurrent`` are open, every other visitor asked to try again."""

# Requirements: REQ-2006

from __future__ import annotations

import threading

import httpx
import pytest

from tests.integration.test_demo_sandboxes_e2e import (
    _JWT_SECRET,
    _demo,
    _deployment_config,
    _open,
    _opened,
)

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "sbxconcurrente2e"
_CAP = 3
_AT_ONCE = 8


@pytest.fixture(scope="module")
def tier(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("demo_sandbox_concurrent_open")
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
            "PROVISA_SANDBOX_MAX_CONCURRENT": str(_CAP),
        },
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def test_visitors_opening_at_once_all_succeed_up_to_the_cap_and_the_rest_are_asked_to_wait(
    tier,
):
    _opened(tier, "alpha")  # the demo's template is ready: one place is taken
    at_once = threading.Barrier(_AT_ONCE)
    answers: list[httpx.Response] = []

    def _visit() -> None:
        at_once.wait(timeout=30)
        answers.append(_open(tier, "alpha"))

    visitors = [threading.Thread(target=_visit) for _ in range(_AT_ONCE)]
    for visitor in visitors:
        visitor.start()
    for visitor in visitors:
        visitor.join(timeout=300)
    assert len(answers) == _AT_ONCE
    opened = [r for r in answers if r.status_code == 200]
    refused = [r for r in answers if r.status_code == 503]
    assert len(opened) + len(refused) == _AT_ONCE, [
        (r.status_code, r.text[:300]) for r in answers if r.status_code not in (200, 503)
    ]
    assert {r.json()["code"] for r in refused} == {"sandbox.at_capacity"}
    # Exactly the cap is open: the one opened first and those opened at once.
    assert 1 + len(opened) == _CAP
    assert len({r.json()["org_id"] for r in opened}) == len(opened)
