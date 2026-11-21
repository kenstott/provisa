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
demo is prepared at the tier's start, every demo's stores asked of the node at once, and an open
waits for a demo's stores and template rather than answering "not ready"."""

# Requirements: REQ-2006, REQ-2049

from __future__ import annotations

import asyncio
import re
import threading
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
    """The node's side: ``asked`` the demos requested, ``up`` the demos with stores up, ``queued``
    the templates queued to be built. Asking brings a demo's stores up."""
    from provisa.core import demo_bundles

    state = SimpleNamespace(
        asked=[], up=set(), failed={}, queued=[], published=[_demo("a"), _demo("b")]
    )

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
        state.queued.append(demo.name)
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


def test_every_published_template_is_built_ahead(node):
    asyncio.run(demo_templates.build_ahead(object()))
    assert sorted(node.asked) == ["a", "b"]
    assert sorted(node.queued) == ["a", "b"]
    assert demo_templates.template_builds.state_of("a") == "ready"
    assert demo_templates.template_builds.state_of("b") == "ready"
    # Each demo's stores are let go once it is prepared, to linger (sandbox.demo_stores_linger).
    assert sorted(node.let_go) == ["a", "b"]


def test_every_published_demos_stores_are_asked_for_at_once(node, monkeypatch):
    """REQ-2006 (amended 2026-11-04): the node prepares every demo together after a wake, not one
    after another's template: here no demo's stores come up until every demo was asked for."""
    from provisa.core import demo_bundles

    monkeypatch.setattr(demo_bundles, "request", lambda name: node.asked.append(name))
    monkeypatch.setattr(
        demo_bundles,
        "ready_version",
        lambda n: "v1" if sorted(node.asked) == ["a", "b"] else None,
    )
    monkeypatch.setattr(demo_templates, "_STORES_LIMIT_S", 2.0)
    for name in ("a", "b"):
        node.up.add(name)  # _queue's check: the stores are up once every demo was asked
    asyncio.run(demo_templates.build_ahead(object()))
    assert sorted(node.queued) == ["a", "b"]


def test_a_template_already_built_for_this_version_has_its_stores_prepared_not_its_build(
    node, monkeypatch
):
    """A template served as built after a wake has its stores down as any other's: they are asked
    for at the start, and the template is not built again."""
    demo_templates.template_builds.record("a", True)

    async def _templated(_pool, name):
        return "v1" if name == "a" else None

    monkeypatch.setattr(demo_templates, "_templated", _templated)
    asyncio.run(demo_templates.build_ahead(object()))
    assert sorted(node.asked) == ["a", "b"]
    assert node.queued == ["b"]
    assert sorted(node.let_go) == ["a", "b"]


def test_a_newer_version_pulled_since_the_start_is_built_ahead_too(node, monkeypatch):
    demo_templates.template_builds.record("a", True)
    demo_templates.template_builds.record("b", True)

    async def _templated(_pool, name):
        return "v0" if name == "a" else "v1"

    monkeypatch.setattr(demo_templates, "_templated", _templated)
    asyncio.run(demo_templates.build_ahead(object()))
    assert node.queued == ["a"]


def test_a_demo_whose_stores_fail_does_not_keep_the_others_and_is_not_retried(node, caplog):
    node.failed["a"] = "pull access denied"
    asyncio.run(demo_templates.build_ahead(object()))
    assert sorted(node.asked) == ["a", "b"]
    assert node.queued == ["b"]
    assert "pull access denied" in caplog.text
    asyncio.run(demo_templates.build_ahead(object()))
    assert sorted(node.asked) == ["a", "b"], "an open asks again; the tier does not, every pass"


def test_stores_that_never_come_up_are_given_up_on_by_name(node, monkeypatch, caplog):
    node.published = [_demo("a")]
    from provisa.core import demo_bundles

    monkeypatch.setattr(demo_bundles, "request", lambda name: node.asked.append(name))
    monkeypatch.setattr(demo_templates, "_STORES_LIMIT_S", 0)
    asyncio.run(demo_templates.build_ahead(object()))
    assert "a" not in node.up
    assert "stores did not come up" in caplog.text


async def _until(condition, limit_s: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + limit_s
    while not condition():
        assert asyncio.get_running_loop().time() < deadline, "never came about"
        await asyncio.sleep(0.01)


def test_an_open_during_the_starts_preparation_waits_on_it_and_builds_nothing_more(
    node, monkeypatch, builds
):
    """A visitor who opens demo ``b`` while the start prepares every demo finds ``b``'s template
    already queued by it, waits for that build, and queues no second one (follow_latest)."""
    from provisa.core import demo_bundles

    async def _queue(_pool, demo):
        node.queued.append(demo.name)
        builds.add(demo, build=True)  # waits in the queue until the test builds it
        return "building"

    async def _template_ready(demo):
        if builds.state_of(demo.name) in ("queued", "building"):
            _not_ready(demo.name)

    monkeypatch.setattr(demo_templates, "_queue_template", _queue)
    monkeypatch.setattr(demo_templates, "latest_published", lambda name: "v1")
    monkeypatch.setattr(demo_bundles, "hold", lambda name: None)
    monkeypatch.setattr("provisa.api.admin.orgs_router._admin_pool", lambda: object())
    monkeypatch.setattr(demo_sandbox, "_template_ready", _template_ready)
    monkeypatch.setattr(demo_sandbox, "_refuse_unhealthy_stores", lambda demo: None)

    async def _templated(_pool, name):
        # Its ready record is written when its build finishes (record_ready).
        return "v1" if builds.state_of(name) == "ready" else None

    monkeypatch.setattr(demo_templates, "_templated", _templated)

    def _build(name):
        builds.prefer(name)
        assert builds._next() is not None
        builds._done(name, "ready")

    async def _scenario():
        ahead = asyncio.create_task(demo_templates.build_ahead(object()))
        await _until(lambda: sorted(node.queued) == ["a", "b"])
        opening = asyncio.create_task(demo_sandbox._ready_demo("b", "sbxtest"))
        await asyncio.sleep(0.05)
        assert not opening.done(), "the open waits for the template being built"
        _build("a")
        _build("b")
        demo = await opening
        await ahead
        return demo

    assert asyncio.run(_scenario()).name == "b"
    assert sorted(node.queued) == ["a", "b"], "one build of each template"


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


def _startup(monkeypatch, *, sandbox_tier: bool) -> list[str]:
    """What the start of a process of either profile prepares: the demos' templates
    (ensure_demo_templates, which starts their preparation ahead) or the sandbox org."""
    import logging

    from provisa.api import app_startup

    prepared: list[str] = []

    async def _ensure(_pool):
        prepared.append("demo templates")
        return {}

    async def _reaped():
        return []

    async def _sandbox_org(_pool):
        prepared.append("sandbox org")
        return "ready"

    monkeypatch.setattr("provisa.api.sandbox_tier.serves_sandboxes", lambda: sandbox_tier)
    monkeypatch.setattr("provisa.core.commerce.enabled", lambda: True)
    monkeypatch.setattr("provisa.api.app.state", SimpleNamespace(admin_db=object()), raising=False)
    monkeypatch.setattr(demo_templates, "ensure_demo_templates", _ensure)
    monkeypatch.setattr(demo_templates, "prefetch_connector_bundles", lambda: None)
    monkeypatch.setattr(demo_templates, "load_embedding_models", lambda: None)
    monkeypatch.setattr(demo_sandbox, "reap_visitors_of_another_release", _reaped)
    monkeypatch.setattr("provisa.api.sandbox_org.ensure_sandbox_org", _sandbox_org)
    asyncio.run(app_startup._seed_sandbox_org(logging.getLogger("test")))
    return prepared


def test_the_sandbox_tiers_start_prepares_the_demos(monkeypatch):
    assert _startup(monkeypatch, sandbox_tier=True) == ["demo templates"]


def test_the_coordinators_start_prepares_no_demo(monkeypatch):
    assert _startup(monkeypatch, sandbox_tier=False) == ["sandbox org"]


def test_ensure_demo_templates_starts_preparing_every_demo_ahead(node, monkeypatch, builds):
    """The sandbox tier's start (ensure_demo_templates) begins the preparation of every published
    demo -- a template served as built included -- in the background (start_building_ahead)."""
    started: list[str] = []

    async def _served(_pool, demo):
        # ``a``'s template was built for this version and release before the VM stopped.
        if demo.name != "a":
            return False
        builds.record("a", True)
        return True

    async def _templated(_pool, name):
        return "v1" if name == "a" else None

    monkeypatch.setattr(demo_templates, "_served_as_built", _served)
    monkeypatch.setattr(demo_templates, "_templated", _templated)
    monkeypatch.setattr(demo_templates, "seeding_release", lambda: False)
    monkeypatch.setattr(demo_templates, "_ahead_running", threading.Lock())
    monkeypatch.setattr(demo_templates, "_startup_queued", threading.Event())
    monkeypatch.setattr(demo_templates, "_build_tasks", set())

    def _spawn(coro, *, name):
        started.append(name)
        return asyncio.get_running_loop().create_task(coro)

    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", _spawn)

    async def _start():
        outcome = await demo_templates.ensure_demo_templates(object())
        await _until(lambda: sorted(node.let_go) == ["a", "b"])
        return outcome

    outcome = asyncio.run(_start())
    assert outcome == {"a": "ready (as built)", "b": "built ahead"}
    assert started == ["demo-templates-ahead"]
    assert sorted(node.asked) == ["a", "b"]
    assert node.queued == ["b"]
