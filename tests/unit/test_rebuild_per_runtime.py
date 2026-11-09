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
        return {config_stamp.MODEL: 7, config_stamp.BUILD: 7}

    monkeypatch.setattr(config_stamp, "read", _read)
    rt = state.org_registry.get("globex")
    rt.model_db = object()
    rt.model_stamp = loaded
    rt.build_stamp = loaded
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
        return {config_stamp.MODEL: 7, config_stamp.BUILD: 7}

    monkeypatch.setattr(config_stamp, "read", _read)
    rt = state.org_registry.get("globex")
    rt.model_db = object()
    rt.model_stamp = 7
    rt.build_stamp = 7
    token = set_current_org("globex")
    try:
        await _rebuild_schemas()
    finally:
        reset_current_org(token)
    assert built.orgs == ["globex"]


# REQ-1914 (amended 2026-11-09): a change the model build does not read moves the model stamp and
# not the build stamp. The runtime's stamp is brought forward; nothing is built.


@pytest.fixture
def stored(monkeypatch) -> dict[str, int]:
    """The stamps the control plane holds; a test changes them."""
    from provisa.core import config_stamp

    held = {config_stamp.MODEL: 9, config_stamp.BUILD: 4}

    async def _read(db):
        del db
        return dict(held)

    monkeypatch.setattr(config_stamp, "read", _read)
    return held


@pytest.fixture
def after_a_build(monkeypatch) -> list[str]:
    """Which of a reload's steps after the build ran."""
    import provisa.api.redaction_load as redaction_load
    from provisa.api import model_reload

    ran: list[str] = []

    async def _prune(rt) -> None:
        del rt
        ran.append("prune")

    async def _reschedule(rt) -> None:
        del rt
        ran.append("reschedule")

    async def _refresh(org_id) -> None:
        del org_id
        ran.append("refresh")

    monkeypatch.setattr(model_reload, "prune_region_state", _prune)
    monkeypatch.setattr(model_reload, "reschedule_triggers", _reschedule)
    monkeypatch.setattr(redaction_load, "refresh_org", _refresh)
    return ran


@pytest.fixture
def globex(state):
    """Globex's runtime, built at model stamp 7 and build stamp 4, with its org bound."""
    rt = state.org_registry.get("globex")
    rt.model_db = object()
    rt.model_stamp = 7
    rt.build_stamp = 4
    token = set_current_org("globex")
    yield rt
    reset_current_org(token)


@pytest.mark.usefixtures("stored")
async def test_a_change_the_build_does_not_read_moves_the_stamp_and_builds_nothing(
    globex, built, after_a_build
) -> None:
    from provisa.api.model_reload import reload_model

    await reload_model(globex)
    assert built.orgs == []
    assert globex.model_stamp == 9
    assert globex.build_stamp == 4
    assert after_a_build == [], "a reload that built nothing has no model change to apply"


async def test_a_change_the_build_reads_still_builds(globex, built, stored, after_a_build) -> None:
    from provisa.api.model_reload import reload_model
    from provisa.core import config_stamp

    stored[config_stamp.BUILD] = 5
    await reload_model(globex)
    assert built.orgs == ["globex"]
    assert after_a_build == ["prune", "reschedule", "refresh"]


@pytest.mark.usefixtures("stored")
async def test_a_reload_whose_turn_finds_the_build_current_brings_the_stamp_forward(
    globex, built
) -> None:
    """The build the reload waited for had read everything; only a status change landed since."""
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas(announce=False, if_stale=True)
    assert built.orgs == []
    assert globex.model_stamp == 9


@pytest.mark.usefixtures("stored", "globex")
async def test_a_change_made_on_this_worker_still_builds_whatever_the_stamps_say(built) -> None:
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    assert built.orgs == ["globex"]


@pytest.mark.usefixtures("stored", "after_a_build")
async def test_a_runtime_not_built_yet_is_not_brought_forward(globex, built) -> None:
    from provisa.api.model_reload import reload_model

    globex.model_stamp = None
    globex.build_stamp = None
    await reload_model(globex)
    assert built.orgs == ["globex"]


@pytest.mark.usefixtures("stored", "after_a_build")
async def test_a_cached_response_is_not_served_after_the_stamp_moves_without_a_build(
    globex, state, built
) -> None:
    """An entry cached before a status change is not read after it, though nothing rebuilt: the
    read goes to the store and sees the new status."""
    from provisa.api.model_reload import reload_model
    from provisa.cache.tenancy import cache_place, cache_tenant

    before = cache_tenant(state)
    assert before == f"{cache_place(state)}:m7"

    await reload_model(globex)

    assert built.orgs == []
    assert cache_tenant(state) == f"{cache_place(state)}:m9"


@pytest.mark.usefixtures("state", "stored", "after_a_build")
async def test_the_hot_tier_follows_the_stamp_moved_without_a_build(globex, built) -> None:
    """The tables loaded under the previous stamp stop being hot, as after a rebuild, and the next
    small read promotes them again: nothing has to have rebuilt for that."""
    from provisa.api.model_reload import reload_model
    from provisa.cache.hot_tables import HotTableEntry, HotTableManager, _scope_parts

    manager = HotTableManager(None, auto_threshold=10, max_rows=10)
    entry = HotTableEntry(1, "t", "c", "s", "id", rows=[{"id": 1}])
    manager._hot_tables[1] = entry
    assert _scope_parts()[1] == 7
    assert manager._hot_tables == {1: entry}

    await reload_model(globex)

    assert built.orgs == []
    assert _scope_parts()[1] == 9
    assert manager._hot_tables == {}


@pytest.mark.usefixtures("stored", "after_a_build")
async def test_the_audit_record_names_the_commit_of_the_stamp_moved_forward(
    globex, state, built
) -> None:
    """A statement after a status change is audited under the new stamp, and so under the commit
    that change made, without a rebuild having run."""
    from provisa.api.model_reload import reload_model
    from provisa.audit.writer import AuditWriter

    asked: list[int] = []

    async def _deployed(pool, org, env, stamp):
        del pool, org, env
        asked.append(stamp)
        return "c" * 40

    writer = AuditWriter(deployed_commit=_deployed)

    await reload_model(globex)

    assert built.orgs == []
    record = SimpleNamespace(
        meter_pool=object(), meter_org="globex", model_env="prod", model_stamp=state.model_stamp
    )
    assert await writer._model_commit(record) == "c" * 40  # type: ignore[arg-type]
    assert asked == [9]
