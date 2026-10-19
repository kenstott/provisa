# Copyright (c) 2026 Kenneth Stott
# Canary: 327e29b0-19e1-42da-a404-bddfd3c698d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A background job walks only the org runtimes that are built (REQ-1926, REQ-1266).

A runtime is registered before its build finishes (build_org_runtime routes build-time writes
through it), so its databases are not bound yet. The temporary-table keeper walked every
registered runtime and read a half-built one's unset tenant database ("'NoneType' object has no
attribute 'acquire'") on a sandbox tier building its templates at start."""

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
        half = _runtime(org_id, None)
        registry.set(org_id, half)  # registered early, as build_org_runtime does
        building.set()
        await finish.wait()
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
        registry.set(org_id, _runtime(org_id, None))
        building.set()
        await finish.wait()
        return _runtime(org_id, object())

    rebuild = asyncio.ensure_future(registry.rebuild("acme", _builder))
    await asyncio.wait_for(building.wait(), 5)
    assert registry.runtimes() == []
    finish.set()
    await asyncio.wait_for(rebuild, 5)
    assert [rt.tenant_db is not None for rt in registry.runtimes()] == [True]
