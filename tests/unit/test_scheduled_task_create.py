# Copyright (c) 2026 Kenneth Stott
# Canary: 88a43999-47c5-49ee-aa9b-f4534d41a40f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""create_scheduled_task / delete_scheduled_task admin mutations (REQ-1003, REQ-1004).

A trigger is written to the caller's org's model store (REQ-1919), never the config file."""

import pytest

from provisa.api.admin.schema_mutation import Mutation
from provisa.core.models import ScheduledTrigger
from provisa.scheduler import jobs


@pytest.fixture
def cfg_path(tmp_path, monkeypatch, control_plane):
    """The bound org's model store, holding the triggers table; no scheduler runs here."""
    import provisa.api.app as app_mod

    db = control_plane.org()
    monkeypatch.setattr(app_mod.state, "model_db", db, raising=False)
    monkeypatch.setattr(app_mod.state, "_scheduler", None, raising=False)
    # A config file is present and must stay untouched: triggers are never written to it.
    path = tmp_path / "provisa.yaml"
    path.write_text("scheduled_triggers: []\n")
    monkeypatch.setenv("PROVISA_CONFIG", str(path))
    yield db
    assert path.read_text() == "scheduled_triggers: []\n"


async def _read(db):
    from provisa.core.repositories import scheduled_trigger as trigger_repo

    async with db.acquire() as conn:
        return {"scheduled_triggers": await trigger_repo.list_all(conn)}


def _info(monkeypatch):
    import provisa.api.app as app_mod
    from tests.unit.gate_identity import grant

    from provisa.auth.models import RoleAssignment

    info, request = grant(monkeypatch, "query_development")  # REQ-1327
    # The role a SQL trigger runs as is one of this org's roles, and one the caller holds.
    app_mod.state.roles["ops"] = {"id": "ops"}
    request.state.assignments = [RoleAssignment(role_id="ops", domain_id="*")]
    return info


_WRITE = "INSERT INTO audit.d SELECT '{{YYYY-MM-DD}}'"


async def test_create_sql_trigger_persists(cfg_path, monkeypatch):
    m = Mutation()
    res = await m.create_scheduled_task(
        _info(monkeypatch),
        id="nightly",
        name="Nightly Rollup",
        cron="0 2 * * *",
        kind="sql",
        sql=_WRITE,
        role="ops",
    )
    assert res.success is True

    triggers = (await _read(cfg_path))["scheduled_triggers"]
    assert len(triggers) == 1
    t = triggers[0]
    assert t["id"] == "nightly"
    assert t["cron"] == "0 2 * * *"
    assert t["sql"] == _WRITE
    assert t["role"] == "ops"
    assert t["url"] is None

    # The persisted trigger feeds build_scheduler as a SQL job of the org that made it.
    fields = ("id", "name", "cron", "url", "webhook_name", "args", "sql", "role", "enabled")
    model = ScheduledTrigger(**{k: t[k] for k in fields})
    scheduler = jobs.build_scheduler([model], "default")
    job = scheduler.get_job("nightly:org_default")
    assert job.func is jobs._run_scheduled  # REQ-2054: every trigger's run is recorded
    assert job.args[0]["kind"] == "sql"


async def test_create_sql_trigger_requires_sql(cfg_path, monkeypatch):
    m = Mutation()
    res = await m.create_scheduled_task(
        _info(monkeypatch), id="bad", name="Bad", cron="0 2 * * *", kind="sql", sql="   "
    )
    assert res.success is False
    assert "sql is required" in res.message
    assert (await _read(cfg_path))["scheduled_triggers"] == []


async def test_create_unknown_kind_fails(cfg_path, monkeypatch):
    m = Mutation()
    res = await m.create_scheduled_task(
        _info(monkeypatch), id="x", name="X", cron="0 2 * * *", kind="frob"
    )
    assert res.success is False
    assert "Unknown trigger kind" in res.message


async def test_create_duplicate_id_fails(cfg_path, monkeypatch):
    m = Mutation()
    await m.create_scheduled_task(
        _info(monkeypatch),
        id="dup",
        name="Dup",
        cron="0 2 * * *",
        kind="sql",
        sql=_WRITE,
        role="ops",
    )
    res = await m.create_scheduled_task(
        _info(monkeypatch),
        id="dup",
        name="Dup2",
        cron="0 3 * * *",
        kind="sql",
        sql=_WRITE,
        role="ops",
    )
    assert res.success is False
    assert "already exists" in res.message
    assert len((await _read(cfg_path))["scheduled_triggers"]) == 1


async def test_delete_scheduled_task(cfg_path, monkeypatch):
    m = Mutation()
    await m.create_scheduled_task(
        _info(monkeypatch),
        id="gone",
        name="Gone",
        cron="0 2 * * *",
        kind="sql",
        sql=_WRITE,
        role="ops",
    )
    res = await m.delete_scheduled_task(_info(monkeypatch), task_id="gone")
    assert res.success is True
    assert (await _read(cfg_path))["scheduled_triggers"] == []

    res2 = await m.delete_scheduled_task(_info(monkeypatch), task_id="missing")
    assert res2.success is False
    assert "not found" in res2.message


async def test_a_sql_trigger_without_a_role_of_this_org_is_refused(cfg_path, monkeypatch):
    m = Mutation()
    for role in (None, "nobody"):
        res = await m.create_scheduled_task(
            _info(monkeypatch),
            id="r",
            name="R",
            cron="0 2 * * *",
            kind="sql",
            sql=_WRITE,
            role=role,
        )
        assert res.success is False and res.code == "schema.trigger_role_required"
    assert (await _read(cfg_path))["scheduled_triggers"] == []


async def test_a_create_is_refused_when_saved_naming_the_trigger(cfg_path, monkeypatch):
    m = Mutation()
    res = await m.create_scheduled_task(
        _info(monkeypatch),
        id="snap",
        name="Snap",
        cron="0 2 * * *",
        kind="sql",
        sql="CREATE TABLE s.x AS SELECT * FROM s.orders",
        role="ops",
    )
    assert res.success is False and res.code == "schema.trigger_sql_refused"
    assert "trigger 'snap': creates an object" in res.message
    assert (await _read(cfg_path))["scheduled_triggers"] == []


async def test_an_update_gives_the_trigger_its_new_definition_and_keeps_it_disabled(
    cfg_path, monkeypatch
):
    """REQ-2054: the Triggers page edits what is declared -- the same checks as a new trigger,
    the stored definition replaced, and whether it is enabled left as it was."""
    m = Mutation()
    await m.create_scheduled_task(
        _info(monkeypatch), id="t", name="T", cron="0 2 * * *", kind="sql", sql=_WRITE, role="ops"
    )
    await m.toggle_scheduled_task(_info(monkeypatch), task_id="t", enabled=False)
    res = await m.update_scheduled_task(
        _info(monkeypatch),
        id="t",
        name="Renamed",
        cron="30 4 * * 1",
        kind="sql",
        sql="INSERT INTO audit.w SELECT 1",
        role="ops",
    )
    assert (res.success, res.code) == (True, "schema.scheduled_task_updated"), res.message
    [t] = (await _read(cfg_path))["scheduled_triggers"]
    assert (t["name"], t["cron"], t["sql"], t["enabled"]) == (
        "Renamed",
        "30 4 * * 1",
        "INSERT INTO audit.w SELECT 1",
        False,
    )


async def test_an_update_is_checked_as_a_new_trigger_and_names_a_missing_one(cfg_path, monkeypatch):
    m = Mutation()
    await m.create_scheduled_task(
        _info(monkeypatch), id="t", name="T", cron="0 2 * * *", kind="sql", sql=_WRITE, role="ops"
    )
    refused = await m.update_scheduled_task(
        _info(monkeypatch),
        id="t",
        name="T",
        cron="0 2 * * *",
        kind="sql",
        sql=_WRITE,
        role="nobody",
    )
    assert refused.success is False and refused.code == "schema.trigger_role_required"
    assert (await _read(cfg_path))["scheduled_triggers"][0]["role"] == "ops"
    missing = await m.update_scheduled_task(
        _info(monkeypatch),
        id="gone",
        name="G",
        cron="0 2 * * *",
        kind="sql",
        sql=_WRITE,
        role="ops",
    )
    assert (missing.success, missing.code) == (False, "schema.task_not_found")
