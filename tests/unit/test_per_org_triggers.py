# Copyright (c) 2026 Kenneth Stott
# Canary: c8219197-a280-44fe-8477-89128d231875
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Scheduled triggers are each org's own (REQ-1003, REQ-1266, REQ-1919).

A trigger lives in the model store of the org that made it and runs bound to that org; its job id
names the org, so two orgs' triggers of one id are two jobs, and an org rescheduling touches only
its own. Only prod's triggers are scheduled. These run with nothing bound.
"""

# Requirements: REQ-1003, REQ-1266, REQ-1919

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.models import ScheduledTrigger
from provisa.core.repositories import scheduled_trigger as trigger_repo
from provisa.core.repositories.origin import ADMIN, CONFIG
from provisa.core.request_context import current_org
from provisa.scheduler import jobs
from provisa.scheduler.executor import background_scheduler

pytestmark = pytest.mark.unbound

_PURGE = "DELETE FROM s.orders WHERE day < '{{YYYY-MM-DD}}'"


def _store(path):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_org import metadata, scheduled_triggers

    engine = create_engine_from_url(f"sqlite+pysqlite:///{path}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[scheduled_triggers])
    return Database(engine, "model")


class _Orgs:
    """App state whose model store is the bound org's, as the runtime registry serves it."""

    _scheduler = None

    def __init__(self, stores):
        self._stores = stores

    @property
    def model_db(self):
        return self._stores[current_org.get()]


@pytest.fixture
def orgs(tmp_path, monkeypatch):
    stores = {org: _store(tmp_path / f"{org}.db") for org in ("a", "b")}
    monkeypatch.setattr("provisa.api.app.state", _Orgs(stores), raising=False)
    return stores


def _sql_trigger(trigger_id="nightly"):
    return ScheduledTrigger(id=trigger_id, cron="0 2 * * *", sql=_PURGE, role="ops")


async def _create(db, trigger, origin=ADMIN):
    async with db.acquire() as conn:
        await trigger_repo.create(conn, trigger, origin=origin)


async def test_two_orgs_triggers_of_one_id_are_two_jobs_each_bound_to_its_org(orgs, monkeypatch):
    for db in orgs.values():
        await _create(db, _sql_trigger())
    scheduler = background_scheduler()
    assert await jobs.register_org_triggers(scheduler, "a", None) == 1
    assert await jobs.register_org_triggers(scheduler, "b", None) == 1
    assert {j.id for j in scheduler.get_jobs()} == {"nightly:org_a", "nightly:org_b"}

    seen: list[tuple[str | None, str]] = []

    async def _govern(_sql, role):
        seen.append((current_org.get(), role))
        return object()

    async def _execute(_plan):
        return type("R", (), {"rows": []})()

    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route", _govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _execute)
    for org in ("a", "b"):
        job = scheduler.get_job(f"nightly:org_{org}")
        assert job is not None
        await job.func(*job.args)  # a job fires with nothing bound
        assert current_org.get() is None
    assert seen == [("a", "ops"), ("b", "ops")]


async def test_an_org_rescheduling_after_a_delete_leaves_the_other_orgs_trigger(orgs):
    for db in orgs.values():
        await _create(db, _sql_trigger())
    scheduler = background_scheduler()
    await jobs.register_org_triggers(scheduler, "a", None)
    await jobs.register_org_triggers(scheduler, "b", None)

    async with orgs["a"].acquire() as conn:
        assert await trigger_repo.delete_one(conn, "nightly") is True
    assert await jobs.register_org_triggers(scheduler, "a", None) == 0

    assert {j.id for j in scheduler.get_jobs()} == {"nightly:org_b"}
    async with orgs["b"].acquire() as conn:
        assert [r["id"] for r in await trigger_repo.list_all(conn)] == ["nightly"]


async def test_a_disabled_trigger_is_unscheduled(orgs):
    await _create(orgs["a"], _sql_trigger())
    scheduler = background_scheduler()
    await jobs.register_org_triggers(scheduler, "a", None)
    async with orgs["a"].acquire() as conn:
        assert await trigger_repo.set_enabled(conn, "nightly", False) is True
    assert await jobs.register_org_triggers(scheduler, "a", None) == 0
    assert scheduler.get_jobs() == []


async def test_only_prod_triggers_are_scheduled(orgs):
    await _create(orgs["a"], _sql_trigger())
    scheduler = background_scheduler()
    assert await jobs.register_org_triggers(scheduler, "a", "dev") == 0
    assert scheduler.get_jobs() == []
    assert await jobs.register_org_triggers(scheduler, "a", "prod") == 1
    assert {j.id for j in scheduler.get_jobs()} == {"nightly:org_a"}


async def test_config_triggers_load_into_the_model_store_as_origin_config(orgs):
    db = orgs["a"]
    await _create(db, _sql_trigger("made-here"))
    async with db.acquire() as conn:
        await trigger_repo.load_from_config(
            conn, [_sql_trigger("from-file"), _sql_trigger("dropped")], origin=CONFIG
        )
        # The file no longer declares "dropped": it goes; the admin-made trigger stays.
        await trigger_repo.load_from_config(conn, [_sql_trigger("from-file")], origin=CONFIG)
        rows = {r["id"]: r["origin"] for r in await trigger_repo.list_all(conn)}
    assert rows == {"from-file": CONFIG, "made-here": ADMIN}


async def test_a_trigger_naming_nothing_to_run_is_refused():
    with pytest.raises(ValueError, match="names nothing to run"):
        trigger_repo._values(ScheduledTrigger(id="empty", cron="0 2 * * *"))


async def test_scheduled_sql_runs_bound_to_its_org_and_unbinds(monkeypatch):
    seen: list[str | None] = []

    async def _govern(_sql, _role):
        seen.append(current_org.get())
        return object()

    async def _execute(_plan):
        return type("R", (), {"rows": []})()

    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route", _govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _execute)
    await jobs._execute_sql(_PURGE, "t1", "ops", "acme")
    assert seen == ["acme"]
    assert current_org.get() is None


async def test_scheduling_reads_refuse_with_no_org_bound():
    """The model store is per org: a read with nothing bound is refused, never served from
    another org's -- which is why every trigger job binds the org it was scheduled for."""
    from provisa.api.app import state

    with pytest.raises(RuntimeError, match="No active org bound"):
        _ = state.model_db


async def test_a_trigger_made_on_another_worker_is_scheduled_by_the_model_reload(orgs, monkeypatch):
    """Worker 1 creates the trigger and schedules it on its own scheduler. Worker 2 -- the holder,
    say -- learns of it only through its model reload when the org's model stamp moves
    (REQ-1914), and that reload schedules it there."""
    import provisa.api.app as app_mod
    from provisa.api import model_reload

    await _create(orgs["a"], _sql_trigger())
    worker_2 = background_scheduler()
    monkeypatch.setattr(app_mod.state, "_scheduler", worker_2, raising=False)

    async def _rebuild(**_kw):
        return None

    async def _prune(_rt):
        return None

    monkeypatch.setattr(app_mod, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(model_reload, "prune_region_state", _prune)
    monkeypatch.setattr(model_reload, "_held", lambda _rt: True)
    assert worker_2.get_jobs() == []

    await model_reload.reload_model(SimpleNamespace(org_id="a", env="prod"))  # type: ignore[arg-type]

    assert {j.id for j in worker_2.get_jobs()} == {"nightly:org_a"}
    assert current_org.get() is None


async def test_a_model_reload_of_a_non_prod_environment_schedules_nothing(orgs, monkeypatch):
    import provisa.api.app as app_mod
    from provisa.api import model_reload

    await _create(orgs["a"], _sql_trigger())
    worker = background_scheduler()
    monkeypatch.setattr(app_mod.state, "_scheduler", worker, raising=False)

    await model_reload.reschedule_triggers(SimpleNamespace(org_id="a", env="dev"))  # type: ignore[arg-type]
    assert worker.get_jobs() == []
