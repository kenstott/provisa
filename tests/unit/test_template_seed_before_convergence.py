# Copyright (c) 2026 Kenneth Stott
# Canary: 35f8dc9c-97a2-4be1-93a7-3d8791c631b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006: a demo template's seed replicas are recorded before its replica convergence runs.

The template's runtime build adopts the seed (``demo_templates.land_from_seed`` ->
``replica_state.record_adopted``: delete each record, then insert it). Its schema build ran
first and started the replica convergence detached, which found no record of the seed's
replicas yet and inserted a requested one for each (``replica_state.request_build``). The two
met on ``replica_state_pkey`` and the template's provisioning failed (cloud sandbox,
2026-11-03T20:59:39Z: sbxtplinsurance, govdata.disasters.wildfire_risk_by_county), so its open
waited on a second build. The template's build now holds its convergence until the seed is
adopted, and starts it after.
"""

# Requirements: REQ-2006, REQ-1915

from __future__ import annotations

import ast
import asyncio
from pathlib import Path

from provisa.core.environments import PROD

_APP = Path(__file__).resolve().parents[2] / "provisa/api/app.py"


def _function(name: str) -> ast.AsyncFunctionDef:
    for node in ast.walk(ast.parse(_APP.read_text())):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in {_APP}")


def _awaited_calls(func: ast.AsyncFunctionDef) -> list[ast.Call]:
    calls = [
        node.value
        for node in ast.walk(func)
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call)
    ]
    return sorted(calls, key=lambda call: call.lineno)


def _name(call: ast.Call) -> str:
    func = call.func
    return func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")


def test_the_org_build_starts_no_convergence_with_its_schemas():
    """The build's schema rebuild does not start the replica convergence itself: the build
    starts it once the template's seed is adopted (_converge_after_seed)."""
    awaited = _awaited_calls(_function("_build_org_runtime"))
    names = [_name(call) for call in awaited]
    assert "_rebuild_schemas" in names and "_converge_after_seed" in names
    rebuild = awaited[names.index("_rebuild_schemas")]
    converge = {kw.arg: kw.value for kw in rebuild.keywords}.get("converge")
    assert isinstance(converge, ast.Constant) and converge.value is False, (
        "the build's schema rebuild starts the replica convergence before the seed is adopted"
    )
    assert names.index("_rebuild_schemas") < names.index("_converge_after_seed")


async def _run_after_seed(monkeypatch, org_id: str, env: str) -> list[str]:
    """Run ``_converge_after_seed`` with the seed's adoption held open; what happened, in order."""
    from provisa.api import app as app_module
    from provisa.core import process_mode

    events: list[str] = []
    adopting = asyncio.Event()
    release = asyncio.Event()

    async def _land_from_seed(org):
        events.append(f"adopt {org} begins")
        adopting.set()
        await release.wait()
        events.append(f"adopt {org} ends")

    def _spawn(coro, *, name):
        events.append(f"spawn {name}")
        coro.close()

    monkeypatch.setattr("provisa.api.demo_templates.land_from_seed", _land_from_seed)
    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", _spawn)
    monkeypatch.setattr(process_mode, "runs_background_work", lambda: True)

    task = asyncio.create_task(app_module._converge_after_seed(org_id, env))
    if org_id.startswith("sbxtpl") and env == PROD:
        await asyncio.wait_for(adopting.wait(), 5)
        await asyncio.sleep(0)
        assert "spawn replica-converge" not in events, (
            "replica convergence started while the template's seed was being adopted"
        )
        release.set()
    await asyncio.wait_for(task, 5)
    return events


async def test_a_templates_convergence_starts_after_its_seed_is_adopted(monkeypatch):
    events = await _run_after_seed(monkeypatch, "sbxtplinsurance", PROD)
    assert events == [
        "adopt sbxtplinsurance begins",
        "adopt sbxtplinsurance ends",
        "spawn replica-converge",
    ]


async def test_any_other_build_converges_without_a_seed(monkeypatch):
    assert await _run_after_seed(monkeypatch, "acme", PROD) == ["spawn replica-converge"]
    # A template's environment other than prod has no seed of its own (land_from_seed is prod's).
    assert await _run_after_seed(monkeypatch, "sbxtplinsurance", "test") == [
        "spawn replica-converge"
    ]


async def test_a_process_without_background_work_converges_nothing(monkeypatch):
    from provisa.api import app as app_module
    from provisa.core import process_mode

    spawned: list[str] = []

    def _spawn(coro, *, name):
        spawned.append(name)
        coro.close()

    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", _spawn)
    monkeypatch.setattr(process_mode, "runs_background_work", lambda: False)
    app_module.start_replica_convergence()
    assert spawned == []
