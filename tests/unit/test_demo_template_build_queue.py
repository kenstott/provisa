# Copyright (c) 2026 Kenneth Stott
# Canary: 484dc390-91b3-4093-bf2e-279938d365fd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The sandbox tier builds its demos' templates a few at a time (REQ-2006).

After a restart the tier built every published demo's template at once; on a 2-vCPU VM the load
reached 76 and visitors' opens were refused as busy. The templates wait in a queue and
``sandbox.template_builds_at_once`` are built together; a demo a visitor opens while it is still
waiting is built next, and the visitor is told it is still being prepared."""

# Requirements: REQ-2006

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.api.demo_templates import TemplateBuilds

pytestmark = pytest.mark.unit


def _demo(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name)


async def test_no_more_than_the_setting_are_built_at_once_and_an_opened_demo_goes_next():
    queue = TemplateBuilds()
    for name in ("a", "b", "c", "d", "e", "f"):
        queue.add(_demo(name), build=True)
    started: list[str] = []
    running: set[str] = set()
    most = 0
    first_two = asyncio.Event()
    go_on = asyncio.Event()

    async def _prepare(demo, build):
        nonlocal most
        assert build is True
        started.append(demo.name)
        running.add(demo.name)
        most = max(most, len(running))
        if len(started) == 2:
            first_two.set()
        await go_on.wait() if len(started) <= 2 else asyncio.sleep(0.01)
        running.discard(demo.name)

    builds = asyncio.ensure_future(queue.run(2, _prepare))
    await asyncio.wait_for(first_two.wait(), 5)
    # A visitor opens f while it is still waiting: it is built next.
    assert queue.prefer("f") is True
    assert queue.prefer("a") is False  # already being built: nothing to move
    go_on.set()
    await asyncio.wait_for(builds, 5)
    assert most == 2
    assert started[:2] == ["a", "b"]
    assert started[2] == "f"
    assert sorted(started) == ["a", "b", "c", "d", "e", "f"]


async def test_a_template_whose_build_fails_does_not_stop_the_others():
    queue = TemplateBuilds()
    for name in ("a", "b", "c"):
        queue.add(_demo(name), build=False)
    built: list[str] = []

    async def _prepare(demo, _build):
        if demo.name == "a":
            raise RuntimeError("its source is unreachable")
        built.append(demo.name)

    await asyncio.wait_for(queue.run(1, _prepare), 5)
    assert built == ["b", "c"]


async def test_each_template_says_whether_it_waits_is_built_is_ready_or_failed():
    queue = TemplateBuilds()
    assert queue.state_of("a") == "unknown"  # never queued: the tier has not started its builds
    for name in ("a", "b", "c"):
        queue.add(_demo(name), build=True)
    seen: dict[str, dict[str, str]] = {}
    release = asyncio.Event()

    async def _prepare(demo, _build):
        seen[demo.name] = {n: queue.state_of(n) for n in ("a", "b", "c")}
        if demo.name == "a":
            await release.wait()
        if demo.name == "b":
            return False  # its replicas failed to land
        if demo.name == "c":
            raise RuntimeError("its source is unreachable")
        return True

    builds = asyncio.ensure_future(queue.run(1, _prepare))
    await asyncio.sleep(0.05)
    assert seen["a"] == {"a": "building", "b": "queued", "c": "queued"}
    release.set()
    await asyncio.wait_for(builds, 5)
    assert {n: queue.state_of(n) for n in ("a", "b", "c")} == {
        "a": "ready",
        "b": "failed",
        "c": "failed",
    }
