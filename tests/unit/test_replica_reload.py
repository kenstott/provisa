# Copyright (c) 2026 Kenneth Stott
# Canary: 204828ef-8ead-4ed9-9f10-e88c9e4d9674
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a promotion made in one process reaches the routes of every other (REQ-826, REQ-1914).

Each runtime remembers the ``replica`` stamp its routes were published at. The config watcher
compares it with the stored one and, when they differ, republishes the routes — nothing else:
no model reload, no schema build. The generation the routing cache is keyed on moves only when
the routes themselves changed."""

# Requirements: REQ-826, REQ-1914, REQ-1920

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api import model_reload
from provisa.api.org_runtime import OrgRuntime
from provisa.core import config_stamp, config_watch
from provisa.federation.replica_address import ReplicaRoutes

pytestmark = pytest.mark.unit

ORDERS = ("pg", "public", "orders")


@pytest.fixture
def worker(monkeypatch):
    """One runtime of one process; the control plane's stamp and what the registry read returns
    are the test's to move."""
    rt = OrgRuntime(org_id="default")
    rt.tenant_db = object()
    plane = SimpleNamespace(stamp=7, routes=ReplicaRoutes(), reads=0, rebuilds=0)

    async def _read(_db):
        return {config_stamp.MODEL: 1, config_stamp.SETTINGS: 1, config_stamp.REPLICA: plane.stamp}

    async def _routes(_state):
        plane.reads += 1
        return plane.routes

    async def _rebuild_schemas(**_kw):
        plane.rebuilds += 1

    monkeypatch.setattr(config_stamp, "read", _read)
    monkeypatch.setattr("provisa.federation.replica_routing.replica_routes", _routes)
    monkeypatch.setattr("provisa.api.app._rebuild_schemas", _rebuild_schemas)
    monkeypatch.setattr(model_reload, "_held", lambda _rt: True)
    return SimpleNamespace(rt=rt, plane=plane)


def _target(rt: OrgRuntime) -> config_watch.Target:
    return config_watch.Target(
        name="org default: replicas",
        db=rt.tenant_db,
        kind=config_stamp.REPLICA,
        loaded=lambda: rt.replica_stamp,
        reload=lambda: model_reload.reload_replicas(rt),
    )


async def test_publishing_records_the_stamp_the_routes_were_read_at(worker):
    await model_reload.publish_replica_routes(worker.rt)
    assert worker.rt.replica_stamp == 7
    # nothing is served from a replica and nothing was before: the routes did not change
    assert worker.rt.replica_routes.generation == 0


async def test_a_promotion_elsewhere_republishes_the_routes_and_moves_the_generation(worker):
    await model_reload.publish_replica_routes(worker.rt)
    assert await config_watch.check([_target(worker.rt)]) == []  # nothing moved: nothing reloads

    # another process: the table's first replica completed
    worker.plane.stamp = 8
    worker.plane.routes = ReplicaRoutes(
        floored={1: ("pg", "replicate")},
        promoted=frozenset({ORDERS}),
        serving=frozenset({ORDERS}),
    )
    assert await config_watch.check([_target(worker.rt)]) == ["org default: replicas"]
    assert worker.rt.replica_routes.serving == frozenset({ORDERS})
    assert worker.rt.replica_stamp == 8
    assert worker.rt.replica_routes.generation == 1
    assert worker.plane.rebuilds == 0, "a change to replica state rebuilt the model"
    # and the watcher is quiet again
    assert await config_watch.check([_target(worker.rt)]) == []


async def test_a_stamp_that_moved_for_nothing_this_engine_serves_leaves_the_generation(worker):
    """Promoted, not yet built: the routes say so, but no table moved onto a replica. And a
    stamp can move twice for one change this process has already read."""
    await model_reload.publish_replica_routes(worker.rt)
    worker.plane.stamp = 8
    worker.plane.routes = ReplicaRoutes(promoted=frozenset({ORDERS}))
    await model_reload.reload_replicas(worker.rt)
    assert worker.rt.replica_routes.generation == 1  # the admin summary's "being built" changed
    worker.plane.stamp = 9
    await model_reload.reload_replicas(worker.rt)
    assert (worker.rt.replica_stamp, worker.rt.replica_routes.generation) == (9, 1)


async def test_a_runtime_that_is_no_longer_served_is_not_reloaded(worker, monkeypatch):
    monkeypatch.setattr(model_reload, "_held", lambda _rt: False)
    await model_reload.reload_replicas(worker.rt)
    assert worker.plane.reads == 0 and worker.rt.replica_stamp is None


def test_every_org_runtime_is_watched_for_its_replica_stamp(monkeypatch):
    rt = OrgRuntime(org_id="acme")
    rt.tenant_db = object()
    rt.replica_stamp = 3
    state = SimpleNamespace(
        admin_db=None,
        org_registry=SimpleNamespace(all_org_ids=lambda: ["acme"], get=lambda _key: rt),
    )
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    by_kind = {t.kind: t for t in model_reload.targets()}
    assert set(by_kind) == {config_stamp.MODEL, config_stamp.SETTINGS, config_stamp.REPLICA}
    replicas = by_kind[config_stamp.REPLICA]
    assert (replicas.name, replicas.db, replicas.loaded()) == (
        "org acme: replicas",
        rt.tenant_db,
        3,
    )


def test_the_boot_runtime_carries_the_org_id_it_is_re_pointed_to():
    """The boot org's id arrives after the default runtime is registered; the runtime moves to
    the real id. Code that binds a request context FROM the runtime — the config watcher's
    reloads, the Hot promotion evaluation — binds ``rt.org_id``, so it must be the real id too:
    a Hot count is kept under the org a request binds, and an evaluation bound to the old id
    read no count at all."""
    from provisa.api.app import AppState

    state = AppState()
    boot = state.org_registry.get(state.org_id)
    state.org_id = "acme"
    assert state.org_registry.get("acme") is boot
    assert boot.org_id == "acme"


async def test_a_replica_stamp_change_drops_this_processs_record_copies_and_converges(
    worker, monkeypatch
):
    """The per-process copies of replica records (``replica_state_view``) were taken before the
    change; the replicator's convergence retires a demoted table's replica and keeps a promoted
    one — both follow the stamp, in a process that does background work."""
    from provisa.core import process_mode
    from provisa.federation import replica_state_view

    state = SimpleNamespace()
    view = replica_state_view.view_for(state)
    view.read("default", ORDERS, None)
    view.read("other-org", ORDERS, None)
    converged: list[object] = []

    async def _converge(st):
        converged.append(st)

    spawned: list[str] = []

    def _spawn(coro, *, name):
        spawned.append(name)
        coro.close()

    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr("provisa.federation.replica_converge.converge_logged", _converge)
    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", _spawn)
    monkeypatch.setattr(process_mode, "runs_background_work", lambda: True)
    await model_reload.reload_replicas(worker.rt)
    assert view.known("default", ORDERS) == (False, None)
    assert view.known("other-org", ORDERS) == (True, None)  # another org's copies are its own
    assert spawned == ["replica-converge"]

    monkeypatch.setattr(process_mode, "runs_background_work", lambda: False)
    await model_reload.reload_replicas(worker.rt)
    assert spawned == ["replica-converge"], "a query-only process converged replicas"


async def test_a_model_change_to_a_boot_org_not_named_default_is_reloaded_by_another_worker(
    monkeypatch,
):
    """REQ-1914 on a boot org whose id comes from ``ORG_ID`` (anything but "default"): another
    worker's change moves the stored model stamp, and this worker's watcher must rebuild THIS
    org's model, bound to its id. Before the runtime carried its real id the reload checked
    ``org_registry.get("default")`` — nothing there — and returned without rebuilding, every
    tick, so the change never reached this worker until it restarted."""
    from provisa.api.app import AppState
    from provisa.core.request_context import current_org

    state = AppState()
    boot = state.org_registry.get(state.org_id)
    state.org_id = "acme"  # what _init_control_planes does with ORG_ID=acme
    boot.tenant_db = object()
    boot.model_stamp = 4
    stored = {config_stamp.MODEL: 5, config_stamp.SETTINGS: 0, config_stamp.REPLICA: 0}
    rebuilt_for: list[str | None] = []

    async def _read(_db):
        return stored

    async def _rebuild_schemas(**_kw):
        rebuilt_for.append(current_org.get())
        boot.model_stamp = stored[config_stamp.MODEL]

    monkeypatch.setattr("provisa.api.app.state", state)
    monkeypatch.setattr("provisa.api.app._rebuild_schemas", _rebuild_schemas)
    monkeypatch.setattr(config_stamp, "read", _read)
    model = [t for t in model_reload.targets() if t.kind == config_stamp.MODEL]
    assert [t.name for t in model] == ["org acme: model"]
    assert await config_watch.check(model) == ["org acme: model"]
    assert rebuilt_for == ["acme"]
    # reloaded once: the stamp it recorded is the stored one, so the next tick is quiet
    assert await config_watch.check(model) == []
