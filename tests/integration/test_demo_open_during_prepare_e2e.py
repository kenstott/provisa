# Copyright (c) 2026 Kenneth Stott
# Canary: 0701d61c-f2d8-4ecc-a08c-0dff2f9c5c0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox opened while its demo's template is being prepared waits for it (REQ-2006, REQ-2049).

A tier started with no template of its bundled demo prepares one. Visitors opening the demo
meanwhile are told it is being prepared (503 ``sandbox.demo_not_ready``) until it is ready, then
served. An open never takes the template being prepared for a stale one: it used to replace it --
the template org dropped under its own build, the build failing on its missing tables and the
open on a deadlock (500)."""

# Requirements: REQ-2006, REQ-2049

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from tests.integration.test_demo_bundle_build_e2e import published_demo
from tests.integration.test_demo_bundle_sandbox_e2e import (
    _close_what_the_test_opened,  # noqa: F401 -- autouse: the sandboxes opened here are removed
    _opened,
    _publish,
    _synced,
    _tier,
)

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_OPENERS = 4


@pytest.fixture(scope="module")
def registry():
    from tests.integration.demo_registry import DemoRegistry

    reg = DemoRegistry()
    reg.start()
    yield reg
    reg.stop()


def _open_until_served(tier) -> list[tuple[int, str]]:
    """One visitor opening alpha until served: every answer it was given."""
    answers: list[tuple[int, str]] = []
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        opened = httpx.post(f"{tier.base_url}/public/demos/alpha", timeout=300)
        code = opened.json().get("code", "") if opened.status_code != 200 else ""
        answers.append((opened.status_code, code or opened.text[:300]))
        if opened.status_code == 200:
            _opened.append(opened.json()["org_id"])
            return answers
        if code != "sandbox.demo_not_ready":
            return answers
        time.sleep(0.5)
    return answers


def test_visitors_opening_a_demo_while_its_template_is_prepared_wait_for_it(
    tmp_path_factory, registry
):
    work = tmp_path_factory.mktemp("open_during_prepare")
    named = work / "named"
    published_demo(named, "alpha")
    _publish(registry, named, work / "out", "e" * 40, ["ant", "ape"])
    _synced(work, registry)
    tier = _tier(work, registry)
    tier.start(timeout=240)
    try:
        # At once after the start, while its template is still being prepared.
        with ThreadPoolExecutor(_OPENERS) as pool:
            visitors = list(pool.map(lambda _i: _open_until_served(tier), range(_OPENERS)))
        log = tier.dump_stderr_debug()
        for answers in visitors:
            assert answers[-1][0] == 200, (answers, log[-6000:])
            assert all(
                status == 200 or (status == 503 and code == "sandbox.demo_not_ready")
                for status, code in answers
            ), (answers, log[-6000:])
        assert any(len(answers) > 1 for answers in visitors), "no open arrived during the prepare"
        # The template being prepared is the one served: never taken for a stale one and removed.
        assert "demo template sbxtplalpha removed" not in log, log[-6000:]
    finally:
        tier.stop_process()
