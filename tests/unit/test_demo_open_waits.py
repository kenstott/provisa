# Copyright (c) 2026 Kenneth Stott
# Canary: 8a0e7c52-3b1d-4e0b-9f6a-2c5d1e7f4a33
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A live demo comes up without a visitor being refused on the way (REQ-2006): every published
template is built at the tier's start, each demo's stores asked of the node in turn, and an open
waits for a demo's stores and template rather than answering "not ready"."""

# Requirements: REQ-2006, REQ-2049

from __future__ import annotations

import asyncio
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.api import demo_sandbox, demo_templates
from provisa.api.demo_templates import PublishedDemo
from provisa.api.errors import ApiError

pytestmark = pytest.mark.unit


def _demo(name: str, version: str = "v1") -> PublishedDemo:
    return PublishedDemo(
        name=name, title=name.title(), directory=Path(f"/bundles/{name}/{version}")
    )


@pytest.fixture
def builds(monkeypatch):
    """A tier's builds of its own, no sleeps, and a node whose stores are up where ``ready``
    says."""
    fresh = demo_templates.TemplateBuilds()
    monkeypatch.setattr(demo_templates, "template_builds", fresh)
    monkeypatch.setattr(demo_templates, "_LANDED_POLL_S", 0)
    monkeypatch.setattr(demo_templates, "_tried_ahead", set())
    monkeypatch.setattr(demo_sandbox, "_OPEN_POLL_S", 0)
    monkeypatch.setattr(demo_templates, "bundled_demos", lambda: ("a", "b"))
    return fresh


@pytest.fixture
def node(monkeypatch, builds):
    """The node's side: ``asked`` the demos requested in order, ``up`` the demos with stores up.
    Asking brings a demo's stores up, and only one is ever asked while another is unbuilt."""
    from provisa.core import demo_bundles

    state = SimpleNamespace(asked=[], up=set(), failed={}, published=[_demo("a"), _demo("b")])

    def _request(name):
        state.asked.append(name)
        if name not in state.failed:
            state.up.add(name)

    monkeypatch.setattr(demo_bundles, "request", _request)
    monkeypatch.setattr(demo_bundles, "stores_failed", lambda name: state.failed.get(name))
    monkeypatch.setattr(demo_bundles, "ready_version", lambda n: "v1" if n in state.up else None)
    monkeypatch.setattr(
        demo_templates, "published_demos", lambda: {d.name: d for d in state.published}
    )

    async def _templated(_pool, _name):
        return None

    async def _queue(_pool, demo):
        assert demo.name in state.up, "a template is built from its stores"
        builds.record(demo.name, True)
        return "building"

    monkeypatch.setattr(demo_templates, "_templated", _templated)
    monkeypatch.setattr(demo_templates, "_queue_template", _queue)
    monkeypatch.setattr(demo_templates, "_start_builds", lambda: None)
    gone = []

    async def _note(name):
        gone.append(name)

    monkeypatch.setattr(demo_sandbox, "note_in_use", _note)
    state.let_go = gone
    return state


def test_every_published_template_is_built_ahead_one_demo_at_a_time(node):
    asyncio.run(demo_templates.build_ahead(object()))
    assert node.asked == ["a", "b"]
    assert demo_templates.template_builds.state_of("a") == "ready"
    assert demo_templates.template_builds.state_of("b") == "ready"
    # Each demo's stores are let go once its template is built: the node cannot hold them all.
    assert node.let_go == ["a", "b"]


def test_a_template_already_built_for_this_version_is_left_alone(node, monkeypatch):
    demo_templates.template_builds.record("a", True)

    async def _templated(_pool, name):
        return "v1" if name == "a" else None

    monkeypatch.setattr(demo_templates, "_templated", _templated)
    asyncio.run(demo_templates.build_ahead(object()))
    assert node.asked == ["b"]


def test_a_newer_version_pulled_since_the_start_is_built_ahead_too(node, monkeypatch):
    demo_templates.template_builds.record("a", True)
    demo_templates.template_builds.record("b", True)

    async def _templated(_pool, name):
        return "v0" if name == "a" else "v1"

    monkeypatch.setattr(demo_templates, "_templated", _templated)
    asyncio.run(demo_templates.build_ahead(object()))
    assert node.asked == ["a"]


def test_a_demo_whose_stores_fail_does_not_keep_the_others_and_is_not_retried(node, caplog):
    node.failed["a"] = "pull access denied"
    asyncio.run(demo_templates.build_ahead(object()))
    assert node.asked == ["a", "b"]
    assert "pull access denied" in caplog.text
    asyncio.run(demo_templates.build_ahead(object()))
    assert node.asked == ["a", "b"], "an open asks again; the tier does not, every pass"


def test_stores_that_never_come_up_are_given_up_on_by_name(node, monkeypatch, caplog):
    node.published = [_demo("a")]
    from provisa.core import demo_bundles

    monkeypatch.setattr(demo_bundles, "request", lambda name: node.asked.append(name))
    monkeypatch.setattr(demo_templates, "_STORES_LIMIT_S", 0)
    asyncio.run(demo_templates.build_ahead(object()))
    assert "a" not in node.up
    assert "stores did not come up" in caplog.text


def test_a_build_ahead_is_not_started_in_a_bundle_builds_seeding_run(monkeypatch):
    monkeypatch.setattr(demo_templates, "seeding_release", lambda: True)
    assert demo_templates.start_building_ahead(object()) is False


def _opening(monkeypatch, *, follow, ready=lambda d: None):
    """``follow`` is what follow_latest does; ``ready`` what _template_ready does."""

    async def _follow(name):
        follow(name)

    async def _ready(demo):
        ready(demo)

    monkeypatch.setattr(demo_sandbox, "follow_latest", _follow)
    monkeypatch.setattr(demo_sandbox, "_template_ready", _ready)
    monkeypatch.setattr(demo_sandbox, "published_demo", lambda name: _demo(name))


def _not_ready(name):
    raise ApiError(503, "sandbox.demo_not_ready", "still being prepared", demo=name)


def test_an_open_waits_for_a_demos_stores_instead_of_refusing(monkeypatch, node):
    calls = []

    def follow(name):
        calls.append(name)
        if len(calls) == 1:
            node.up.add(name)  # its stores come up while the open waits
            _not_ready(name)

    _opening(monkeypatch, follow=follow)
    assert asyncio.run(demo_sandbox._ready_demo("a", "sbxtest")).name == "a"
    assert len(calls) == 2


def test_an_open_past_its_wait_is_refused_as_not_ready(monkeypatch, node):
    monkeypatch.setattr(demo_sandbox, "OPEN_WAIT_S", 0)
    _opening(monkeypatch, follow=_not_ready)
    with pytest.raises(ApiError) as refused:
        asyncio.run(demo_sandbox._ready_demo("a", "sbxtest"))
    assert refused.value.code == "sandbox.demo_not_ready"


def test_stores_that_fail_during_the_wait_are_named_at_once(monkeypatch, node):
    node.failed["a"] = "no such image"
    _opening(monkeypatch, follow=_not_ready)
    with pytest.raises(ApiError) as refused:
        asyncio.run(demo_sandbox._ready_demo("a", "sbxtest"))
    assert refused.value.code == "sandbox.demo_stores_failed"


def test_a_template_that_fails_during_the_wait_is_named_at_once(monkeypatch, node, builds):
    node.up.add("a")
    builds.record("a", False)
    _opening(monkeypatch, follow=_not_ready)
    with pytest.raises(ApiError) as refused:
        asyncio.run(demo_sandbox._ready_demo("a", "sbxtest"))
    assert refused.value.code == "sandbox.demo_failed"


def test_any_other_refusal_is_not_waited_on(monkeypatch, node):
    def follow(name):
        raise ApiError(404, "sandbox.unknown_demo", "no such demo", demo=name)

    _opening(monkeypatch, follow=follow)
    with pytest.raises(ApiError) as refused:
        asyncio.run(demo_sandbox._ready_demo("a", "sbxtest"))
    assert refused.value.code == "sandbox.unknown_demo"


def _status(monkeypatch, node):
    from provisa.api.admin import orgs_router
    from provisa.core import settings_registry

    class _Conn:
        async def execute_core(self, _stmt):
            return SimpleNamespace(scalar_one=lambda: 0)

    class _Pool:
        def acquire(self):
            from contextlib import asynccontextmanager

            @asynccontextmanager
            async def conn():
                yield _Conn()

            return conn()

    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: _Pool())
    monkeypatch.setattr(settings_registry, "value", lambda _key: 10)
    monkeypatch.setattr(demo_sandbox, "published_demo", lambda name: _demo(name))
    return asyncio.run(demo_sandbox.demo_status("a"))


def test_a_built_template_is_ready_with_its_stores_down_as_an_open_waits_for_them(
    monkeypatch, node, builds
):
    builds.record("a", True)
    status = _status(monkeypatch, node)
    assert status["ready"] is True
    assert status["state"] == "ready"


def test_a_template_not_built_is_not_ready(monkeypatch, node):
    assert _status(monkeypatch, node)["ready"] is False


def test_the_status_of_a_template_that_failed_names_the_failure(monkeypatch, node, builds):
    builds.record("a", False)
    with pytest.raises(ApiError) as refused:
        _status(monkeypatch, node)
    assert refused.value.code == "sandbox.demo_failed"


def test_an_open_logs_how_long_it_waited_for_the_stores_and_the_template(monkeypatch, node, caplog):
    """Each part of an open's wait is in the log: the minutes an open took on alpha.498 could not
    be told apart from the tier's log, which named only "its demo's readiness"."""
    import logging

    calls = []

    def follow(name):
        calls.append(name)
        if len(calls) == 1:
            node.up.add(name)
            _not_ready(name)

    _opening(monkeypatch, follow=follow)
    with caplog.at_level(logging.WARNING, logger="provisa.api.demo_sandbox"):
        asyncio.run(demo_sandbox._ready_demo("a", "sbxtest"))
    said = caplog.text
    assert "demo sandbox sbxtest of a: its newest version took" in said
    assert re.search(r"sbxtest of a: waited \d+s for its stores, \d+s for its template", said)
