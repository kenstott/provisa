# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""create_scheduled_task / delete_scheduled_task admin mutations (REQ-1003, REQ-1004)."""

import pytest
import yaml

import provisa.api.admin.schema_mutation_ops as sm_ops
from provisa.api.admin.schema_mutation import Mutation
from provisa.core.models import ScheduledTrigger
from provisa.scheduler import jobs


@pytest.fixture
def cfg_path(tmp_path, monkeypatch):
    path = tmp_path / "provisa.yaml"
    path.write_text("scheduled_triggers: []\n")
    monkeypatch.setenv("PROVISA_CONFIG", str(path))
    # Avoid importing the running app / scheduler in a unit test.
    monkeypatch.setattr(sm_ops, "_register_trigger_live", lambda _t: None)
    return path


def _read(path):
    return yaml.safe_load(path.read_text())


async def test_create_sql_trigger_persists(cfg_path):
    m = Mutation()
    res = await m.create_scheduled_task(
        id="nightly",
        name="Nightly Rollup",
        cron="0 2 * * *",
        kind="sql",
        sql="INSERT INTO audit.d SELECT '{{YYYY-MM-DD}}'",
    )
    assert res.success is True

    triggers = _read(cfg_path)["scheduled_triggers"]
    assert len(triggers) == 1
    t = triggers[0]
    assert t["id"] == "nightly"
    assert t["cron"] == "0 2 * * *"
    assert t["sql"] == "INSERT INTO audit.d SELECT '{{YYYY-MM-DD}}'"
    assert "url" not in t

    # The persisted trigger feeds build_scheduler as a SQL job.
    model = ScheduledTrigger(**{k: v for k, v in t.items() if k != "name"})
    scheduler = jobs.build_scheduler([model])
    job = scheduler.get_job("nightly")
    assert job.func is jobs._execute_sql


async def test_create_sql_trigger_requires_sql(cfg_path):
    m = Mutation()
    res = await m.create_scheduled_task(
        id="bad", name="Bad", cron="0 2 * * *", kind="sql", sql="   "
    )
    assert res.success is False
    assert "sql is required" in res.message
    assert _read(cfg_path)["scheduled_triggers"] == []


async def test_create_unknown_kind_fails(cfg_path):
    m = Mutation()
    res = await m.create_scheduled_task(id="x", name="X", cron="0 2 * * *", kind="frob")
    assert res.success is False
    assert "Unknown trigger kind" in res.message


async def test_create_duplicate_id_fails(cfg_path):
    m = Mutation()
    await m.create_scheduled_task(
        id="dup", name="Dup", cron="0 2 * * *", kind="sql", sql="SELECT 1"
    )
    res = await m.create_scheduled_task(
        id="dup", name="Dup2", cron="0 3 * * *", kind="sql", sql="SELECT 2"
    )
    assert res.success is False
    assert "already exists" in res.message
    assert len(_read(cfg_path)["scheduled_triggers"]) == 1


async def test_delete_scheduled_task(cfg_path, monkeypatch):
    m = Mutation()
    await m.create_scheduled_task(
        id="gone", name="Gone", cron="0 2 * * *", kind="sql", sql="SELECT 1"
    )
    # delete_scheduled_task imports app state; force the no-scheduler branch.
    import provisa.api.app as app_mod

    monkeypatch.setattr(app_mod.state, "_scheduler", None, raising=False)

    res = await m.delete_scheduled_task(task_id="gone")
    assert res.success is True
    assert _read(cfg_path)["scheduled_triggers"] == []

    res2 = await m.delete_scheduled_task(task_id="missing")
    assert res2.success is False
    assert "not found" in res2.message
