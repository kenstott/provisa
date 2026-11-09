# Copyright (c) 2026 Kenneth Stott
# Canary: e1ffaff6-ee43-491b-a286-9231f9d30697
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A schema rebuild holds its own runtime alone, and a reload is not a second rebuild (REQ-1882,
REQ-1914).

One process-wide lock serialized every rebuild: while one org's model rebuilt (ten seconds and
more on a CI runner), another org's write waited for its turn before its own rebuild began. Each
runtime (an org's environment) now has its own lock.

After a write, the config watcher saw the new ``model`` stamp before the writer's own rebuild had
recorded it, and its reload waited for that rebuild and then built the same model again: a whole
second rebuild after every write. A reload whose turn finds the runtime built at the stored stamp
builds nothing.
"""

# Requirements: REQ-1882, REQ-1914

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from provisa.api.org_runtime import OrgRuntime
from provisa.core.request_context import reset_current_org, set_current_org

pytestmark = pytest.mark.unbound

_HELD_S = 10.0


@pytest.fixture
def state(monkeypatch):
    from provisa.api.app import AppState

    state = AppState()
    state.org_id = "root"
    for org in ("acme", "globex"):
        state.org_registry.set(org, OrgRuntime(org_id=org))
    monkeypatch.setattr("provisa.api.app.state", state)
    return state


@pytest.fixture
def built(monkeypatch) -> SimpleNamespace:
    """The orgs whose model was built, in order; acme's build holds its thread until released."""
    import provisa.api.app as app_mod
    from provisa.core.request_context import current_org

    done: list[str] = []
    acme_started = threading.Event()
    acme_release = threading.Event()

    async def _impl(raw_config=None, *, announce=True, converge=True):
        del raw_config, announce, converge
        org = current_org.get()
        if org == "acme":
            acme_started.set()
            # CPU-bound work holds its thread: nothing here gives the loop back.
            assert acme_release.wait(_HELD_S)
        assert org is not None
        done.append(org)

    monkeypatch.setattr(app_mod, "_rebuild_schemas_impl", _impl)
    return SimpleNamespace(orgs=done, acme_started=acme_started, acme_release=acme_release)


def _rebuild_on_own_thread(org: str) -> threading.Thread:
    from provisa.api.app import _rebuild_schemas

    def _run() -> None:
        token = set_current_org(org)
        try:
            asyncio.run(_rebuild_schemas())
        finally:
            reset_current_org(token)

    thread = threading.Thread(target=_run, name=f"rebuild-{org}", daemon=True)
    thread.start()
    return thread


@pytest.mark.usefixtures("state")
async def test_one_orgs_rebuild_does_not_hold_another_orgs(built) -> None:
    from provisa.api.app import _rebuild_schemas, rebuild_in_progress

    acme = _rebuild_on_own_thread("acme")
    assert built.acme_started.wait(_HELD_S)
    token = set_current_org("globex")
    try:
        assert not rebuild_in_progress()
        # Waiting for acme's turn, this would not finish before acme is released.
        await asyncio.wait_for(_rebuild_schemas(), timeout=2.0)
    finally:
        reset_current_org(token)
        built.acme_release.set()
        acme.join(_HELD_S)
    assert built.orgs == ["globex", "acme"]


@pytest.mark.usefixtures("state")
async def test_the_same_runtimes_rebuilds_still_take_turns(built) -> None:
    from provisa.api.app import _rebuild_schemas, rebuild_in_progress

    acme = _rebuild_on_own_thread("acme")
    assert built.acme_started.wait(_HELD_S)
    token = set_current_org("acme")
    try:
        assert rebuild_in_progress()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(_rebuild_schemas(), timeout=0.5)
    finally:
        reset_current_org(token)
        built.acme_release.set()
        acme.join(_HELD_S)


@pytest.mark.parametrize(("loaded", "rebuilt"), [(7, False), (6, True), (None, True)])
async def test_a_reload_builds_only_a_runtime_behind_the_stored_stamp(
    state, built, monkeypatch, loaded, rebuilt
) -> None:
    from provisa.api.app import _rebuild_schemas
    from provisa.core import config_stamp

    async def _read(db):
        del db
        return {config_stamp.MODEL: 7}

    monkeypatch.setattr(config_stamp, "read", _read)
    rt = state.org_registry.get("globex")
    rt.model_db = object()
    rt.model_stamp = loaded
    token = set_current_org("globex")
    try:
        await _rebuild_schemas(announce=False, if_stale=True)
    finally:
        reset_current_org(token)
    assert built.orgs == (["globex"] if rebuilt else [])


async def test_a_change_always_builds(state, built, monkeypatch) -> None:
    from provisa.api.app import _rebuild_schemas
    from provisa.core import config_stamp

    async def _read(db):
        del db
        return {config_stamp.MODEL: 7}

    monkeypatch.setattr(config_stamp, "read", _read)
    rt = state.org_registry.get("globex")
    rt.model_db = object()
    rt.model_stamp = 7
    token = set_current_org("globex")
    try:
        await _rebuild_schemas()
    finally:
        reset_current_org(token)
    assert built.orgs == ["globex"]
