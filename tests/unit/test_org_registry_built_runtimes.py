# Copyright (c) 2026 Kenneth Stott
# Canary: 327e29b0-19e1-42da-a404-bddfd3c698d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Work bound to an org is served only a built runtime (REQ-1926, REQ-1266).

A build routes its own writes through the runtime it is building (the AppState shims), so that
runtime is resolved for the build before its stores are bound. Work outside the build -- the
temporary-table keeper, a scheduled replica pass, its housekeeping -- was served it too and read
its unset tenant database ("'NoneType' object has no attribute 'acquire'") on a sandbox tier
building its templates. Only the build resolves a runtime it is building; everyone else is served
the one registered before, until the build completes."""

# Requirements: REQ-1926, REQ-1266

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.api.org_runtime import OrgRegistry

pytestmark = pytest.mark.unit


def _runtime(org_id: str, tenant_db) -> SimpleNamespace:
    """A runtime as the registry and its retirement read one."""
    return SimpleNamespace(
        org_id=org_id,
        env="prod",
        tenant_db=tenant_db,
        live_engine=None,
        federation_engine=None,
        isolated_engine=False,
    )


async def test_a_runtime_being_built_is_not_listed_until_its_build_is_done():
    registry = OrgRegistry()
    building = asyncio.Event()
    finish = asyncio.Event()
    built = _runtime("acme", object())

    async def _builder(org_id):
        started = registry.begin_build(org_id, _runtime(org_id, None))  # as build_org_runtime
        building.set()
        await finish.wait()
        registry.end_build(started, built=False)
        return built

    build = asyncio.ensure_future(registry.get_or_build("acme", _builder))
    await asyncio.wait_for(building.wait(), 5)
    assert registry.runtimes() == []
    finish.set()
    await asyncio.wait_for(build, 5)
    assert registry.runtimes() == [built]


async def test_a_rebuild_hides_the_runtime_while_it_is_rebuilt():
    registry = OrgRegistry()
    registry.set("acme", _runtime("acme", object()))
    building = asyncio.Event()
    finish = asyncio.Event()

    async def _builder(org_id):
        started = registry.begin_build(org_id, _runtime(org_id, None))
        building.set()
        await finish.wait()
        registry.end_build(started, built=False)
        return _runtime(org_id, object())

    rebuild = asyncio.ensure_future(registry.rebuild("acme", _builder))
    await asyncio.wait_for(building.wait(), 5)
    assert registry.runtimes() == []
    finish.set()
    await asyncio.wait_for(rebuild, 5)
    assert [rt.tenant_db is not None for rt in registry.runtimes()] == [True]


async def test_work_outside_a_rebuild_is_served_the_runtime_built_before():
    """A scheduled pass for the org, running while its runtime is rebuilt, is served the runtime
    it had -- never the one being built, whose tenant database is not bound yet."""
    registry = OrgRegistry()
    before = _runtime("acme", object())
    registry.set("acme", before)
    building = asyncio.Event()
    finish = asyncio.Event()
    after = _runtime("acme", object())

    async def _build():
        started = registry.begin_build("acme", after)
        after.tenant_db, bound = None, after.tenant_db  # its stores are bound late in the build
        building.set()
        assert registry.get("acme") is after  # the build routes its writes through it
        await finish.wait()
        after.tenant_db = bound
        registry.end_build(started, built=True)

    async def _scheduled_pass():  # started apart from the build, as the scheduler's jobs are
        await asyncio.wait_for(building.wait(), 5)
        return registry.get("acme")

    passed = asyncio.ensure_future(_scheduled_pass())
    build = asyncio.ensure_future(_build())
    assert await asyncio.wait_for(passed, 5) is before
    finish.set()
    await asyncio.wait_for(build, 5)
    assert registry.get("acme") is after


async def test_an_org_whose_first_runtime_is_being_built_has_none_outside_the_build():
    registry = OrgRegistry()
    building = asyncio.Event()
    finish = asyncio.Event()

    async def _build():
        started = registry.begin_build("acme", _runtime("acme", None))
        building.set()
        await finish.wait()
        registry.end_build(started, built=True)

    async def _outside():
        await asyncio.wait_for(building.wait(), 5)
        return registry.get("acme")

    outside = asyncio.ensure_future(_outside())
    build = asyncio.ensure_future(_build())
    assert await asyncio.wait_for(outside, 5) is None
    finish.set()
    await asyncio.wait_for(build, 5)
    assert registry.get("acme") is not None


async def test_work_a_build_starts_resolves_the_registry_once_the_build_is_over():
    """What a build starts (its lifecycle wiring, a convergence) is served the runtime it is
    building while it builds, and the registry's once it has ended -- so a later replacement is
    seen as one."""
    registry = OrgRegistry()
    building = _runtime("acme", object())
    started = registry.begin_build("acme", building)
    seen: list = []
    release = asyncio.Event()

    async def _started_by_the_build():
        seen.append(registry.get("acme"))
        await release.wait()
        seen.append(registry.get("acme"))

    task = asyncio.ensure_future(_started_by_the_build())
    await asyncio.sleep(0)
    registry.end_build(started, built=True)
    replacement = _runtime("acme", object())
    registry.set("acme", replacement)
    release.set()
    await asyncio.wait_for(task, 5)
    assert seen == [building, replacement]


async def test_a_failed_build_registers_nothing():
    registry = OrgRegistry()
    started = registry.begin_build("acme", _runtime("acme", None))
    registry.end_build(started, built=False)
    assert registry.get("acme") is None


def test_the_org_runtime_build_registers_its_runtime_only_once_built():
    """build_org_runtime does not register its runtime before its build is done: it builds it
    through begin_build and registers it at end_build."""
    import inspect

    from provisa.api import app as app_module

    src = inspect.getsource(app_module._build_org_runtime)
    assert "org_registry.set(" not in src
    assert src.index("org_registry.begin_build(") < src.index("_bind_region_stores(")
    assert src.rindex("end_build(build, built=True)") > src.index("_bind_region_stores(")
