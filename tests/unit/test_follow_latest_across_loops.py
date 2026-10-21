# Copyright (c) 2026 Kenneth Stott
# Canary: 9647add6-9283-486c-820c-9f71fa986b31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two visitors opening one demo while its template is replaced (REQ-2049, REQ-1882).

Every request runs on a thread and event loop of its own, and opening a sandbox first serves the
demo's ready version (``follow_latest``), replacing its template when a newer one was published.
The per-demo lock that keeps two openers from replacing it at once was an ``asyncio.Lock``, bound
to the first loop that waited on it: the second opener, on another loop, failed. Both wait their
turn; the template is replaced (queued to be built) once and the second finds it replaced."""

# Requirements: REQ-2049, REQ-1882

from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace

import pytest

from provisa.api import demo_templates

pytestmark = pytest.mark.unit


def test_two_openers_on_their_own_loops_replace_the_template_once(monkeypatch):
    templated = {"alpha": "v1"}
    replaced: list[str] = []
    replacing = threading.Event()

    async def _queue_template(_pool, demo):
        # Its old template retired and the new one queued (demo_templates._queue_template), long
        # enough for the other opener to wait on the lock.
        replacing.set()
        await asyncio.sleep(0.5)
        templated[demo.name] = "v2"
        replaced.append(demo.name)
        return "building"

    monkeypatch.setattr(demo_templates, "_following", {})
    monkeypatch.setattr(demo_templates, "bundled_demos", lambda: {"alpha": object()})
    monkeypatch.setattr(demo_templates, "latest_published", lambda _name: "v2")
    monkeypatch.setattr("provisa.core.demo_bundles.ready_version", lambda _name: "v2")
    monkeypatch.setattr("provisa.core.demo_bundles.request", lambda _name: None)
    monkeypatch.setattr(demo_templates, "_templated", lambda name: templated[name])
    monkeypatch.setattr(demo_templates, "published_demo", lambda name: SimpleNamespace(name=name))
    monkeypatch.setattr(demo_templates, "_queue_template", _queue_template)
    monkeypatch.setattr(demo_templates, "_start_builds", lambda: None)
    monkeypatch.setattr(
        demo_templates,
        "template_builds",
        SimpleNamespace(state_of=lambda name: "queued" if name in replaced else "unknown"),
    )
    monkeypatch.setattr("provisa.api.admin.orgs_router._admin_pool", lambda: object())
    failures: list[BaseException] = []

    def _open(wait_for_the_first: bool) -> None:
        if wait_for_the_first:
            assert replacing.wait(10)
            time.sleep(0.05)
        try:
            asyncio.run(demo_templates.follow_latest("alpha"))  # a request's own loop
        except BaseException as exc:  # noqa: BLE001 -- the outcome is what this test reports
            failures.append(exc)

    # Daemons: an opener left waiting on a lock bound to the other's loop never returns.
    openers = [
        threading.Thread(target=_open, args=(False,), daemon=True),
        threading.Thread(target=_open, args=(True,), daemon=True),
    ]
    for opener in openers:
        opener.start()
    for opener in openers:
        opener.join(timeout=10)
    assert [o.is_alive() for o in openers] == [False, False], "an opener is still waiting"
    assert failures == []
    assert replaced == ["alpha"]
