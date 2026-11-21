# Copyright (c) 2026 Kenneth Stott
# Canary: 9b1e4c2a-6d3f-4a8e-b5c7-0e2f1a9d8c64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A template's live landing does not hold the build queue (REQ-2006, REQ-2049).

CLOUD-FIX found the 'provisioning' template held its place in the build queue for about 35
minutes while its listeners landed their Kafka topics, so after a cloud patch every template behind
it waited. Waiting on a live landing builds nothing: the template gives its place to the next one
and keeps waiting beside the queue."""

# Requirements: REQ-2006, REQ-2049

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.api import demo_templates
from provisa.api.demo_templates import PublishedDemo, TemplateBuilds, step_aside

pytestmark = pytest.mark.unit


def _demo(name: str) -> PublishedDemo:
    return PublishedDemo(name=name, title=name.title(), directory=Path("/bundles") / name / "v1")


async def _until(predicate) -> None:
    while not predicate():
        await asyncio.sleep(0.01)


async def test_a_template_that_steps_aside_frees_its_place_for_the_next():
    queue = TemplateBuilds()
    for name in ("live", "b", "c"):
        queue.add(SimpleNamespace(name=name), build=True)
    landed = asyncio.Event()

    async def _prepare(demo, _build):
        if demo.name == "live":
            step_aside()
            await landed.wait()
        return True

    builds = asyncio.ensure_future(queue.run(1, _prepare))
    await asyncio.wait_for(_until(lambda: queue.state_of("c") == "ready"), 5)
    assert queue.state_of("b") == "ready"
    assert queue.state_of("live") == "building"
    # A template queued while the one aside still waits is built by new workers (claim).
    queue.add(SimpleNamespace(name="d"), build=True)
    assert queue.claim() is True
    later = asyncio.ensure_future(queue.run(1, _prepare))
    await asyncio.wait_for(_until(lambda: queue.state_of("d") == "ready"), 5)
    await asyncio.wait_for(later, 5)
    assert queue.state_of("live") == "building"
    landed.set()
    await asyncio.wait_for(builds, 5)
    assert queue.state_of("live") == "ready"


async def test_a_slow_live_landing_does_not_delay_another_templates_readiness(monkeypatch):
    live, other = _demo("provisioning"), _demo("agriculture")
    landed = asyncio.Event()
    waits: list[str] = []

    async def _runtime(org_id):
        return SimpleNamespace(org_id=org_id)

    async def _nothing(*_args, **_kwargs):
        return None

    async def _none_failed(*_args):
        return []

    async def _all_landed(_rt):
        return 1, [], 1, ""

    async def _topic_ends(_seed):
        return {"orders": 9000}

    async def _live_waits(org_id, _seed, _ends):
        if org_id == live.template_org and not landed.is_set():
            waits.append(org_id)
            return ["orders: 3 of 9000"]
        return []

    monkeypatch.setattr("provisa.api.demo_region_seed.regional", lambda: False)
    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _runtime)
    monkeypatch.setattr("provisa.api.demo_replicas.live_topic_ends", _topic_ends)
    monkeypatch.setattr("provisa.api.demo_replicas.live_landing_waits", _live_waits)
    monkeypatch.setattr("provisa.api.demo_synthetic.prepare", _nothing)
    monkeypatch.setattr("provisa.api.demo_synthetic.synthetic_envs", lambda _envs: [])
    monkeypatch.setattr(demo_templates, "_LANDED_POLL_S", 0.01)
    monkeypatch.setattr(demo_templates, "template_seed", lambda _org: object())
    monkeypatch.setattr(demo_templates, "create_environments", _nothing)
    monkeypatch.setattr(demo_templates, "demo_environments", lambda _directory: [])
    monkeypatch.setattr(demo_templates, "seeding_release", lambda: False)
    monkeypatch.setattr(demo_templates, "_replica_progress", _all_landed)
    monkeypatch.setattr(demo_templates, "profile_template", _none_failed)
    monkeypatch.setattr(demo_templates, "scan_template", _none_failed)
    monkeypatch.setattr(demo_templates, "_ready_hooks", [])
    monkeypatch.setattr(demo_templates, "record_ready", _nothing)
    monkeypatch.setattr("provisa.api.admin.orgs_router._admin_pool", lambda: object())

    queue = TemplateBuilds()
    queue.add(live, build=False)
    queue.add(other, build=False)
    # One at a time, as a small tier builds them (sandbox.template_builds_at_once).
    builds = asyncio.ensure_future(queue.run(1, demo_templates._prepare))
    await asyncio.wait_for(_until(lambda: queue.state_of("agriculture") == "ready"), 5)
    assert waits, "the live template was still landing"
    assert queue.state_of("provisioning") == "building"
    landed.set()
    await asyncio.wait_for(builds, 5)
    assert queue.state_of("provisioning") == "ready"
