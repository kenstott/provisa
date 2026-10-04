# Copyright (c) 2026 Kenneth Stott
# Canary: c6256279-13b7-4a42-8f78-35192f7f7015
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A scheduled SQL trigger is saved only by a caller who holds its role, or holds cross_org.

A SQL trigger's statement runs as its role on every firing, so saving one is acting as that role.
Scheduling needs only ``org_settings``; without this rule that right would run statements as any
role of the org, ``org_admin`` included. It holds wherever a trigger is saved: the GraphQL create,
enabling a trigger, and the uploaded config. The refusal names the role.
"""

# Requirements: REQ-1003

from __future__ import annotations

import types

import pytest
import yaml

import provisa.api.admin.schema_mutation_ops as sm_ops
import provisa.api.app as appmod
from provisa.api.admin import settings_router
from provisa.api.admin.schema_mutation import Mutation
from provisa.api.errors import ApiError
from provisa.auth.models import RoleAssignment

_WRITE = "INSERT INTO audit.d SELECT '{{YYYY-MM-DD}}'"


@pytest.fixture
def cfg_path(tmp_path, monkeypatch):
    path = tmp_path / "provisa.yaml"
    path.write_text("scheduled_triggers: []\n")
    monkeypatch.setenv("PROVISA_CONFIG", str(path))
    monkeypatch.setattr(sm_ops, "_register_trigger_live", lambda _t: None)
    monkeypatch.setattr(
        appmod.state,
        "roles",
        {
            "scheduler": {"id": "scheduler", "capabilities": ["org_settings"]},
            "operator": {"id": "operator", "capabilities": ["org_settings", "cross_org"]},
            "ops": {"id": "ops", "capabilities": []},
            "org_admin": {"id": "org_admin", "capabilities": ["write"]},
        },
        raising=False,
    )
    return path


def _caller(*held: str):
    """An authenticated caller assigned ``held``; returns ``(info, request)``."""
    identity = types.SimpleNamespace(user_id="u1", roles=list(held))
    state = types.SimpleNamespace(
        identity=identity,
        assignments=[RoleAssignment(role_id=r, domain_id="*") for r in held],
    )
    request = types.SimpleNamespace(state=state)
    return types.SimpleNamespace(context={"request": request}), request


def _triggers(path):
    return yaml.safe_load(path.read_text())["scheduled_triggers"]


def _refused(err: pytest.ExceptionInfo[ApiError], role_id: str) -> None:
    assert (err.value.status_code, err.value.code) == (403, "auth.role_not_assigned")
    assert role_id in err.value.detail


async def _create(info, role: str):
    return await Mutation().create_scheduled_task(
        info, id="t1", name="T1", cron="0 2 * * *", kind="sql", sql=_WRITE, role=role
    )


# --- create --------------------------------------------------------------------------------------


async def test_a_trigger_cannot_run_as_a_role_its_creator_does_not_hold(cfg_path):
    info, _ = _caller("scheduler")
    with pytest.raises(ApiError) as err:
        await _create(info, "org_admin")
    _refused(err, "org_admin")
    assert _triggers(cfg_path) == []


async def test_a_trigger_runs_as_a_role_its_creator_holds(cfg_path):
    info, _ = _caller("scheduler", "ops")
    assert (await _create(info, "ops")).success is True
    assert _triggers(cfg_path)[0]["role"] == "ops"


async def test_the_cross_org_right_saves_a_trigger_as_any_role(cfg_path):
    info, _ = _caller("operator")
    assert (await _create(info, "org_admin")).success is True


# --- enable --------------------------------------------------------------------------------------


def _write_trigger(path, role: str, enabled: bool) -> None:
    trigger = {"id": "t1", "cron": "0 2 * * *", "sql": _WRITE, "role": role, "enabled": enabled}
    path.write_text(yaml.dump({"scheduled_triggers": [trigger]}))


async def test_enabling_a_trigger_needs_its_role(cfg_path):
    _write_trigger(cfg_path, "org_admin", enabled=False)
    info, _ = _caller("scheduler")
    with pytest.raises(ApiError) as err:
        await Mutation().toggle_scheduled_task(info, task_id="t1", enabled=True)
    _refused(err, "org_admin")
    assert _triggers(cfg_path)[0]["enabled"] is False


async def test_disabling_a_trigger_needs_no_role(cfg_path):
    _write_trigger(cfg_path, "org_admin", enabled=True)
    info, _ = _caller("scheduler")
    assert (await Mutation().toggle_scheduled_task(info, task_id="t1", enabled=False)).success
    assert _triggers(cfg_path)[0]["enabled"] is False


# --- the uploaded config -------------------------------------------------------------------------


def _upload(role: str, enabled: bool = True) -> bytes:
    trigger = {"id": "t1", "cron": "0 2 * * *", "sql": _WRITE, "role": role, "enabled": enabled}
    return yaml.dump({"scheduled_triggers": [trigger]}).encode()


def test_an_uploaded_config_adding_a_trigger_needs_its_role(cfg_path):
    _, request = _caller("scheduler")
    with pytest.raises(ApiError) as err:
        settings_router._require_trigger_roles(request, _upload("org_admin"))
    _refused(err, "org_admin")
    _, request = _caller("operator")
    settings_router._require_trigger_roles(request, _upload("org_admin"))


def test_an_uploaded_config_carrying_a_trigger_unchanged_needs_no_role(cfg_path):
    _write_trigger(cfg_path, "org_admin", enabled=True)
    _, request = _caller("scheduler")
    settings_router._require_trigger_roles(request, _upload("org_admin"))
    settings_router._require_trigger_roles(request, _upload("org_admin", enabled=False))
    with pytest.raises(ApiError) as err:
        # The same trigger moved onto another role is a new act as that role.
        settings_router._require_trigger_roles(request, _upload("ops"))
    _refused(err, "ops")
