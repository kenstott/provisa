# Copyright (c) 2026 Kenneth Stott
# Canary: c6cf4915-7104-4964-94f5-40707703d37a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin test call of a command runs as a role the caller may act as, and is the real call.

``POST /admin/actions/test`` takes the role to run as from its body. The role is required; it is
one the caller holds, or the caller holds ``access_config`` (the rule compileQuery follows). The
call is then the governed one every surface makes: the command admission (assigned to the role,
in a domain it reaches), and the role's governance of the rows. There is no ungoverned branch.
"""

# Requirements: REQ-004, REQ-273, REQ-1758

from __future__ import annotations

import types

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

import provisa.api.app as appmod
import provisa.api.data.action_exec as action_exec
import provisa.executor.function_dispatch as function_dispatch
from provisa.api.admin.actions_router import router
from provisa.api.errors import ApiError
from provisa.auth.models import RoleAssignment

_ROLES = {
    "tester": {"id": "tester", "capabilities": ["table_registration"], "domain_access": ["*"]},
    "auditor": {
        "id": "auditor",
        "capabilities": ["table_registration", "access_config"],
        "domain_access": ["*"],
    },
    "reader": {"id": "reader", "capabilities": [], "domain_access": ["*"]},
    "outsider": {"id": "outsider", "capabilities": [], "domain_access": ["*"]},
}

_COMMAND = {
    "name": "region_count",
    "domain_id": "sales",
    "kind": "query",
    "impl_kind": "python",
    "source_id": "",
    "function_name": "region_count",
    "returns": "",
    "arguments": [],
    "visible_to": ["reader"],
}


@pytest.fixture
def calls(monkeypatch):
    """Every dispatch of the command, by role; the command answers one row."""
    made: list[str | None] = []

    async def _dispatch(_fn, _args, _state, role_id):
        made.append(role_id)
        return [{"region": "east", "n": 2}]

    monkeypatch.setattr(function_dispatch, "dispatch_function", _dispatch)
    monkeypatch.setattr(action_exec, "dispatch_function", _dispatch)
    monkeypatch.setenv("PROVISA_ENABLE_TEST_ENDPOINTS", "true")
    for name, value in (
        ("roles", _ROLES),
        ("tracked_functions", {"region_count": _COMMAND}),
        ("tracked_webhooks", {}),
        ("model_db", object()),
    ):
        monkeypatch.setattr(appmod.state, name, value, raising=False)
    return made


def _client(*held: str) -> TestClient:
    """The router behind a stand-in for the auth middleware: the caller holds ``held``."""
    app = FastAPI()

    @app.middleware("http")
    async def _identity(request: Request, call_next):
        request.state.identity = types.SimpleNamespace(user_id="u1", roles=list(held))
        request.state.assignments = [RoleAssignment(role_id=r, domain_id="*") for r in held]
        return await call_next(request)

    @app.exception_handler(ApiError)
    async def _api_error(_request, exc: ApiError):
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": exc.detail, "code": exc.code}, status_code=exc.status_code)

    app.include_router(router)
    return TestClient(app)


def _test(client: TestClient, role_id: str | None):
    body = {"actionType": "function", "name": "region_count", "role_id": role_id}
    return client.post("/admin/actions/test", json=body)


def test_a_test_call_names_its_role(calls):
    resp = _test(_client("tester", "reader"), None)
    assert resp.status_code == 422, resp.text
    assert resp.json()["code"] == "actions.test_role_required"
    assert "role_id" in resp.json()["detail"]
    assert calls == []


def test_a_test_call_runs_only_as_a_role_the_caller_holds(calls):
    resp = _test(_client("tester"), "reader")
    assert resp.status_code == 403, resp.text
    assert resp.json()["detail"] == "Role 'reader' is not assigned to this user"
    assert calls == []


def test_a_held_role_the_command_is_not_assigned_to_finds_no_command(calls):
    # The real admission: the same answer as a command never registered.
    resp = _test(_client("tester", "outsider"), "outsider")
    assert resp.status_code == 404, resp.text
    assert resp.json()["code"] == "functions.unknown_command"
    assert calls == []


def test_a_held_role_the_command_is_assigned_to_runs_it(calls):
    resp = _test(_client("tester", "reader"), "reader")
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows"] == [{"region": "east", "n": 2}]
    assert calls == ["reader"]


def test_access_config_tests_as_any_role_through_the_same_admission(calls):
    assert _test(_client("auditor"), "reader").status_code == 200
    refused = _test(_client("auditor"), "outsider")
    assert refused.status_code == 404 and refused.json()["code"] == "functions.unknown_command"
    assert calls == ["reader"]
