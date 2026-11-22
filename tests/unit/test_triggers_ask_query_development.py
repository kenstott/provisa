# Copyright (c) 2026 Kenneth Stott
# Canary: 1fc52b5b-980f-4f45-85be-0bd78c3d66d3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Scheduled triggers ask query_development, everywhere (REQ-1327, amended 2026-11-05).

A trigger runs a statement, so its page, its list, create/update/delete/toggle and its import
section all ask the right that develops queries -- not org_settings, not observability. A caller
holding only query_development reaches every trigger operation; a caller holding org_settings
and observability but not query_development is refused each one.
"""

# Requirements: REQ-1327

from __future__ import annotations

import pytest

import provisa.api.app as appmod
from provisa.api.admin import import_router
from provisa.api.admin.schema_mutation import Mutation
from provisa.api.admin.schema_query import Query
from provisa.auth.models import RoleAssignment
from tests.unit.gate_identity import grant, serve_every_column

_WRITE = "INSERT INTO audit.d SELECT '{{YYYY-MM-DD}}'"


@pytest.fixture
def model_store(monkeypatch, control_plane):
    """The bound org's model store, holding the triggers table; no scheduler runs here."""
    db = control_plane.org()
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "_scheduler", None, raising=False)
    return db


def _developer(monkeypatch):
    """A caller holding query_development only, acting as the role its SQL triggers run as."""
    info, request = grant(monkeypatch, "query_development")
    appmod.state.roles["ops"] = {"id": "ops"}
    request.state.assignments = [RoleAssignment(role_id="ops", domain_id="*")]
    return info


def _administrator(monkeypatch):
    """A caller holding the rights triggers used to ask, and not query_development."""
    info, _ = grant(monkeypatch, "org_settings", "observability")
    return info


async def _create(info, task_id: str = "t1"):
    return await Mutation().create_scheduled_task(
        info, id=task_id, name="T1", cron="0 2 * * *", kind="sql", sql=_WRITE, role="ops"
    )


async def _held(db) -> list[dict]:
    from provisa.core.repositories import scheduled_trigger as trigger_repo

    async with db.acquire() as conn:
        return await trigger_repo.list_all(conn)


async def test_query_development_lists_and_changes_triggers(model_store, monkeypatch):
    info = _developer(monkeypatch)
    serve_every_column(monkeypatch)

    async def no_runs(state):
        return {}

    monkeypatch.setattr("provisa.scheduler.trigger_runs.last_runs", no_runs)
    # The catalog the list reads table names against: none registered, none withdrawn.
    monkeypatch.setattr(appmod.state, "tables", [], raising=False)
    monkeypatch.setattr(appmod.state, "role_build_inputs", {"withdrawn_tables": []}, raising=False)

    assert (await _create(info)).success is True
    assert (
        await Mutation().update_scheduled_task(
            info, id="t1", name="T1b", cron="0 3 * * *", kind="sql", sql=_WRITE, role="ops"
        )
    ).success is True
    assert (await Mutation().toggle_scheduled_task(info, task_id="t1", enabled=False)).success
    listed = await Query().scheduled_tasks(info)
    assert [(t.id, t.name, t.enabled) for t in listed] == [("t1", "T1b", False)]

    assert (await Mutation().delete_scheduled_task(info, task_id="t1")).success is True
    assert await _held(model_store) == []
    assert (await _create(info, "t2")).success is True
    result = await Mutation().delete_scheduled_tasks(info, task_ids=["t2"])
    assert (result.deleted, result.refused) == (["T1"], [])
    assert await _held(model_store) == []


async def test_org_settings_and_observability_reach_no_trigger(model_store, monkeypatch):
    info = _developer(monkeypatch)
    assert (await _create(info)).success is True

    info = _administrator(monkeypatch)
    refused = [
        lambda: Query().scheduled_tasks(info),
        lambda: _create(info, "t9"),
        lambda: Mutation().update_scheduled_task(
            info, id="t1", name="X", cron="0 3 * * *", kind="sql", sql=_WRITE, role="ops"
        ),
        lambda: Mutation().toggle_scheduled_task(info, task_id="t1", enabled=False),
        lambda: Mutation().delete_scheduled_task(info, task_id="t1"),
        lambda: Mutation().delete_scheduled_tasks(info, task_ids=["t1"]),
    ]
    for call in refused:
        with pytest.raises(PermissionError, match="query_development"):
            await call()
    held = await _held(model_store)
    assert [(t["id"], t["name"], t["enabled"]) for t in held] == [("t1", "T1", True)]


def test_an_imported_trigger_asks_query_development():
    assert import_router.SECTION_RIGHTS["scheduled_triggers"] == ("query_development",)
    assert "query_development" in import_router.IMPORT_RIGHTS
