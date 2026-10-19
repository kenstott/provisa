# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox tier's start serves its demos' templates as they were built (REQ-2006).

Every start (each wake from idle, each deploy) used to prepare every template again: its runtime
built, then every replica polled until landed. A template that finished building is recorded --
its demo version and Provisa release, and the rows each replica and view holds -- and a start that
finds the same version and the same rows on disk serves it at once. A changed version, or a store
whose rows differ from the record, is built again through the build queue."""

# Requirements: REQ-2006

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import insert, select

from provisa.api import demo_templates
from provisa.api.demo_templates import PublishedDemo, TemplateBuilds
from provisa.core.schema_admin import demo_template_readiness, orgs

pytestmark = pytest.mark.unit

_BUILT = {"replicas": {"src__public__orders": 40}, "views": {"view_answer": 12}}


def _demo(name: str, version: str = "20261019-abc") -> PublishedDemo:
    return PublishedDemo(name=name, title=name.title(), directory=Path("/bundles") / name / version)


@pytest.fixture
async def platform(control_plane):
    return control_plane.platform


async def _built(pool, demo: PublishedDemo, *, version: str | None = None) -> None:
    """``demo``'s template as a finished build leaves it: its org ready, and its record."""
    async with pool.acquire() as conn:
        await conn.execute_core(
            insert(orgs).values(id=demo.template_org, name=demo.title, provisioning_state="ready")
        )
        await conn.execute_core(
            insert(demo_template_readiness).values(
                template_org=demo.template_org,
                version=version or demo_templates.readiness_version(demo),
                replicas=_BUILT["replicas"],
                views=_BUILT["views"],
            )
        )


def _on_disk(monkeypatch, counts: dict[str, dict]) -> None:
    monkeypatch.setattr(
        "provisa.api.demo_replicas.store_counts",
        lambda org: counts.get(org, {"replicas": {}, "views": {}}),
    )


async def test_an_unchanged_template_with_its_store_on_disk_is_ready_as_built(
    platform, monkeypatch
):
    demo = _demo("alpha")
    await _built(platform, demo)
    _on_disk(monkeypatch, {demo.template_org: _BUILT})
    assert await demo_templates.warm_ready(platform, demo) is True


async def test_a_changed_demo_version_is_built_again(platform, monkeypatch):
    demo = _demo("alpha")
    await _built(platform, demo, version="20261001-old@0.1.0a480")
    _on_disk(monkeypatch, {demo.template_org: _BUILT})
    assert await demo_templates.warm_ready(platform, demo) is False


@pytest.mark.parametrize(
    "held",
    [
        {"replicas": {}, "views": {}},  # the store file is gone
        {"replicas": {"src__public__orders": 40}, "views": {}},  # a view's table is missing
        {"replicas": {"src__public__orders": 39}, "views": {"view_answer": 12}},  # rows differ
    ],
    ids=["no-store", "missing-view", "rows-differ"],
)
async def test_a_store_that_differs_from_its_record_is_built_again(platform, monkeypatch, held):
    demo = _demo("alpha")
    await _built(platform, demo)
    _on_disk(monkeypatch, {demo.template_org: held})
    assert await demo_templates.warm_ready(platform, demo) is False


async def test_a_template_never_recorded_is_built(platform, monkeypatch):
    _on_disk(monkeypatch, {})
    assert await demo_templates.warm_ready(platform, _demo("alpha")) is False


async def test_a_finished_build_records_the_rows_its_store_holds(platform, monkeypatch):
    demo = _demo("alpha")
    async with platform.acquire() as conn:
        await conn.execute_core(insert(orgs).values(id=demo.template_org, name=demo.title))
    _on_disk(monkeypatch, {demo.template_org: _BUILT})
    await demo_templates.record_ready(platform, demo)
    assert await demo_templates.warm_ready(platform, demo) is True
    async with platform.acquire() as conn:
        row = (await conn.execute_core(select(demo_template_readiness))).fetchone()
    assert (row.version, row.replicas, row.views) == (
        demo_templates.readiness_version(demo),
        _BUILT["replicas"],
        _BUILT["views"],
    )


async def test_the_start_serves_an_unchanged_template_and_queues_only_the_changed(
    platform, monkeypatch
):
    warm, changed = _demo("alpha"), _demo("beta")
    await _built(platform, warm)
    await _built(platform, changed, version="20261001-old@0.1.0a480")
    _on_disk(monkeypatch, {warm.template_org: _BUILT, changed.template_org: _BUILT})
    queue = TemplateBuilds()
    rows: list[str] = []
    spawned: list[str] = []

    async def _template_row(_pool, demo):
        rows.append(demo.name)
        return True

    def _spawn(coro, *, name):
        spawned.append(name)
        coro.close()
        return _Task()

    monkeypatch.setattr(demo_templates, "template_builds", queue)
    monkeypatch.setattr(demo_templates, "published_demos", lambda: {"alpha": warm, "beta": changed})
    monkeypatch.setattr(demo_templates, "bundled_demos", lambda: {})
    monkeypatch.setattr(demo_templates, "seeding_release", lambda: False)
    monkeypatch.setattr("provisa.api.demo_region_seed.regional", lambda: False)
    monkeypatch.setattr(demo_templates, "_template_row", _template_row)
    monkeypatch.setattr(demo_templates, "_mark_templated", lambda _demo: None)
    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", _spawn)
    monkeypatch.setattr("provisa.core.settings_registry.value", lambda _key: 2)
    outcome = await demo_templates.ensure_demo_templates(platform)
    assert outcome == {"alpha": "ready (as built)", "beta": "building"}
    assert rows == ["beta"]  # the unchanged template is neither marked for building nor queued
    assert queue.state_of("alpha") == "ready"
    assert queue.state_of("beta") == "queued"
    assert spawned == ["demo-templates"]


class _Task:
    def add_done_callback(self, _callback) -> None:
        pass
