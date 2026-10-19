# Copyright (c) 2026 Kenneth Stott
# Canary: 32eb8a9b-de30-4707-9d2f-d3d6e4f0f9b0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo's readiness, asked without opening it, and a visitor's tab closing (REQ-2006, REQ-2065).

``GET /public/demos/{name}/status`` answers whether the demo can be opened now (its template's
place in the tier's builds) and how many places are free, and opens nothing. A visitor's tab says
it is closing with its own token and the sandbox's org header, exactly as the page sends it."""

# Requirements: REQ-2006, REQ-2065

from __future__ import annotations

import time

import httpx
import pytest

from tests.integration.test_demo_sandboxes_e2e import (
    _JWT_SECRET,
    _demo,
    _deployment_config,
    _opened,
)

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "sbxstatuse2e"
_CAP = 3


@pytest.fixture(scope="module")
def tier(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("demo_sandbox_status_close")
    demos = work / "named"
    _demo(demos, "alpha", published=True)
    _demo(demos, "gamma", published=False)
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


def _status(tier, name: str) -> httpx.Response:
    return httpx.get(f"{tier.base_url}/public/demos/{name}/status", timeout=60)


def test_a_demos_status_says_when_it_can_be_opened_and_opens_nothing(tier):
    deadline = time.monotonic() + 180
    states: list[str] = []
    while True:
        resp = _status(tier, "alpha")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        states.append(body["state"])
        assert body["state"] in ("queued", "building", "ready"), body
        if body["ready"]:
            break
        assert time.monotonic() < deadline, states
        time.sleep(1)
    assert body == {"demo": "alpha", "ready": True, "state": "ready", "places_free": _CAP}
    # Asked again and again, it opens nothing: every place is still free.
    for _ in range(3):
        assert _status(tier, "alpha").json()["places_free"] == _CAP


def test_an_unknown_or_unpublished_demos_status_is_refused_by_name(tier):
    for name in ("nosuchdemo", "gamma"):
        resp = _status(tier, name)
        assert resp.status_code == 404, resp.text
        assert resp.json()["code"] == "sandbox.unknown_demo"


def test_a_visitors_tab_closes_its_sandbox_as_the_page_says_it(tier):
    sandbox = _opened(tier, "alpha")
    assert _status(tier, "alpha").json()["places_free"] == _CAP - 1
    # What sandboxClosing.ts sends as the tab unloads: its token, and the org in the header.
    close = httpx.post(
        f"{tier.base_url}/sandboxes/closing",
        headers={
            "Authorization": f"Bearer {sandbox['token']}",
            "x-org-provisa": sandbox["org_id"],
        },
        timeout=60,
    )
    assert close.status_code == 204, close.text
