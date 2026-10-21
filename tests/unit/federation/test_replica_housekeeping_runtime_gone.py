# Copyright (c) 2026 Kenneth Stott
# Canary: 5476c7de-b889-4bac-89f4-1b02350a271a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A replica pass whose runtime was dropped under it (an environment's data changed while the
pass, detached from the build that kicked it, was still running) has no runtime to build for:
it is left to the next runtime, said once at debug, never logged as a failed housekeeping."""

from __future__ import annotations

import logging

import pytest

from provisa.core.runtime_gone import RuntimeNotBuilt
from provisa.federation.replica_runner import ReplicaRunner


def _runner(housekeeping, built: list) -> ReplicaRunner:
    async def build(key, _progress):
        built.append(key)

    async def none(*_a):
        return None

    return ReplicaRunner(
        db=None,
        org_id="default",
        locks=None,
        engine_key=lambda: "engine",
        build=build,
        source_cap=none,
        permits=None,
        next_refresh_at=none,
        store=lambda: "store",
        retry=lambda: None,
        housekeeping=housekeeping,
        builds_per_node=lambda: 1,
        engine_jobs=lambda: 1,
        spawn=lambda coro, name: None,
    )


@pytest.mark.asyncio
async def test_a_pass_whose_runtime_is_gone_is_left_to_the_next_runtime(caplog):
    async def housekeeping(_locks, _org):
        raise RuntimeNotBuilt("default", "recopied", "no runtime built for environment 'recopied'")

    built: list = []
    with caplog.at_level(logging.DEBUG):
        assert await _runner(housekeeping, built).run_pass() == 0
    assert built == []
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("left to the next runtime" in r.getMessage() for r in caplog.records)
