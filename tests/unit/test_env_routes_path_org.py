# Copyright (c) 2026 Kenneth Stott
# Canary: 3d9b7e14-6c2a-4f58-9e01-b7a4c85d2f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1327: every environment route reads the caller's right in the organization its path names.

A request's rights are those of the roles it acts as in the organization it is bound to. A caller
who is org_admin of an organization of their own and a plain member of another was let through
the member guard on the other's paths: the guard read the right from the bound organization and
only membership from the path's. Every route of the router is enumerated here, so a route added
without the guard fails.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from provisa.api.admin import environments_router as er
from provisa.api.errors import ApiError
from provisa.core.schema_admin import user_org_memberships

ORG_A = "org_a"  # where the caller's role carries the environment rights
ORG_B = "org_b"  # the organization the path names
ENV = "feature"

_ROLES = {
    "org_admin": {
        "id": "org_admin",
        "description": "Test role.",
        "capabilities": [
            er.MANAGE_CAPABILITY,
            er.OWNED_CAPABILITY,
            er.SWITCH_CAPABILITY,
            er.DATA_CAPABILITY,
            "usage",
        ],
    },
}

#: Platform routes: guarded by the deployment check, not by a right held in an organization.
PLATFORM_ROUTES = {("POST", "/admin/orgs/{org_id}/environments/-/fix-sandbox-membership")}

#: What a route needs to be called at all. A route absent from here is called bare.
_PREFIX = "/admin/orgs/{org_id}/environments"
CALLS: dict[tuple[str, str], dict] = {
    ("POST", _PREFIX): {"json": {"name": ENV, "data_mode": "unbound"}},
    ("PATCH", _PREFIX + "/{name}"): {"json": {}},
    ("POST", _PREFIX + "/{name}/merge"): {"json": {"from_env": "dev", "message": "m"}},
    ("POST", _PREFIX + "/{name}/deploy"): {"json": {"ref": "abc"}},
    ("POST", _PREFIX + "/-/merge-requests/{request_id}/decide"): {"json": {"approve": True}},
    ("GET", _PREFIX + "/{name}/merge-preview"): {"params": {"from_env": "dev"}},
    ("PUT", _PREFIX + "/-/repo-integration"): {"json": {}},
    ("POST", _PREFIX + "/-/repo-integration/probe"): {"json": {}},
    ("POST", _PREFIX + "/-/repo-integration/create-remote"): {
        "json": {"remote": "https://git.example.invalid/acme/model.git"}
    },
    ("POST", _PREFIX + "/{name}/review"): {"json": {"message": "m"}},
    ("GET", _PREFIX + "/-/repo/history"): {"params": {"ref": "abc"}},
    ("GET", _PREFIX + "/-/repo/files"): {"params": {"ref": "abc"}},
    ("GET", _PREFIX + "/-/repo/file"): {"params": {"ref": "abc", "path": "a.yaml"}},
}


def _routes() -> list[tuple[str, str]]:
    found = []
    for route in er.router.routes:
        assert isinstance(route, APIRoute)
        for method in sorted(route.methods - {"HEAD", "OPTIONS"}):
            if (method, route.path) not in PLATFORM_ROUTES:
                found.append((method, route.path))
    return found


ROUTES = _routes()


class _GuardPassed(Exception):
    """Raised where a guard hands over to the route's own work: the caller was authorized."""


class _Conn:
    def __init__(self, pool: "_Pool"):
        self.pool = pool

    async def execute_core(self, stmt):
        self.pool.statements.append(stmt)
        tables = {t.name for t in stmt.get_final_froms()}
        assert tables == {user_org_memberships.name}, f"a refused caller read {tables}"
        column = stmt.selected_columns[0].name
        if not self.pool.member:
            return SimpleNamespace(fetchone=lambda: None)
        # membership row: the org id for the membership check, no pin for the confinement check
        value = ORG_B if column == "org_id" else None
        return SimpleNamespace(fetchone=lambda: (value,))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _Pool:
    def __init__(self):
        self.member = True
        self.statements: list = []

    def acquire(self):
        return _Conn(self)


@pytest.fixture
def served(monkeypatch):
    """The router behind a caller the test sets, with every step past a guard recorded."""
    caller = SimpleNamespace(active_org=None, owns=False)
    pool = _Pool()
    built: list[str] = []

    async def _ready(org_id):
        raise _GuardPassed(org_id)

    async def _ensure_org_runtime(org_id, env=None):
        built.append(org_id)
        raise AssertionError(f"the node built {org_id} for a caller no guard had passed")

    async def _owns(_admin_db, _org_id, _env, _user_id):
        return caller.owns

    async def _get_env(_admin_db, _org_id, _name):
        return None

    monkeypatch.setattr(er, "_admin_pool", lambda: pool)
    monkeypatch.setattr(er, "_org_ready_on_this_node", _ready)
    monkeypatch.setattr(er, "owns_environment", _owns)
    monkeypatch.setattr(er, "get_env", _get_env)
    monkeypatch.setattr("provisa.api.app.state", SimpleNamespace(roles=_ROLES))
    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _ensure_org_runtime)

    app = FastAPI()

    @app.middleware("http")
    async def _bind(request: Request, call_next):
        request.state.identity = SimpleNamespace(
            user_id="u1", roles=["org_admin"], acting_roles=["org_admin"]
        )
        request.state.active_org_id = caller.active_org
        return await call_next(request)

    @app.exception_handler(ApiError)
    async def _refused(_request: Request, exc: ApiError):
        return JSONResponse({"code": exc.code}, status_code=exc.status_code)

    @app.exception_handler(_GuardPassed)
    async def _passed(_request: Request, exc: _GuardPassed):
        return JSONResponse({"guard_passed_for": str(exc)}, status_code=299)

    app.include_router(er.router)
    return SimpleNamespace(
        http=TestClient(app, raise_server_exceptions=True), caller=caller, pool=pool, built=built
    )


def _call(served, method: str, path: str):
    url = path.format(org_id=ORG_B, name=ENV, request_id=7)
    return served.http.request(method, url, **CALLS.get((method, path), {}))


def test_the_router_has_routes_to_check():
    assert len(ROUTES) >= 27
    assert set(CALLS) <= set(ROUTES), "CALLS names a route the router no longer has"


@pytest.mark.parametrize(("method", "path"), ROUTES, ids=[f"{m} {p}" for m, p in ROUTES])
class TestEveryRoute:
    def test_a_right_held_in_another_org_is_refused(self, served, method, path):
        """org_admin of org_a, plain member of org_b, on org_b's path: refused, nothing done."""
        served.caller.active_org = ORG_A
        resp = _call(served, method, path)
        assert resp.status_code == 403, resp.text
        assert resp.json()["code"] == "environments.capability_required_in_org"
        assert served.built == []

    def test_a_right_held_in_the_paths_org_passes(self, served, method, path):
        served.caller.active_org = ORG_B
        resp = _call(served, method, path)
        assert resp.status_code == 299, resp.text
        assert resp.json() == {"guard_passed_for": ORG_B}

    def test_a_non_member_is_refused(self, served, method, path):
        served.caller.active_org = ORG_A
        served.pool.member = False
        resp = _call(served, method, path)
        assert resp.status_code == 403, resp.text
        assert resp.json()["code"] == "environments.membership_required"
        assert served.built == []


def test_owned_management_held_in_another_org_does_not_delete_an_owned_environment(served):
    """REQ-2155: deleting an environment one owns needs environment_owned_management, and that
    right too is read in the path's organization."""
    served.caller.active_org = ORG_A
    served.caller.owns = True
    resp = served.http.delete(f"/admin/orgs/{ORG_B}/environments/{ENV}")
    assert resp.status_code == 403, resp.text
    assert resp.json()["code"] == "environments.capability_required_in_org"


def test_an_owner_holding_owned_management_in_the_paths_org_deletes_it(served):
    served.caller.active_org = ORG_B
    served.caller.owns = True
    resp = served.http.delete(f"/admin/orgs/{ORG_B}/environments/{ENV}")
    assert resp.status_code == 299, resp.text
