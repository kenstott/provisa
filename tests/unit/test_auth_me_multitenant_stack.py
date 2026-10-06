# Copyright (c) 2026 Kenneth Stott
# Canary: ab737710-3985-43ce-a613-ab29a78fb4bc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GET /auth/me on a multitenant deployment, through the real middleware (REQ-1266, REQ-1327).

A signed-in user calls /auth/me naming no org -- the platform plane -- with and without an org
membership. The request passes the auth middleware and the per-role rate limiter, as on a server,
and reaches the real handler with no org bound: the limiter reads the caller's role on the
platform plane, never a tenant registry that no request bound.
"""

# Requirements: REQ-1266, REQ-1327

from __future__ import annotations

from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from provisa.api.middleware.rate_limit_middleware import RateLimitMiddleware
from provisa.api.rate_limit import NoopRateLimiter
from provisa.auth.middleware import AuthMiddleware
from tests.unit.test_auth_middleware_multitenancy import _Pool, _Provider

pytestmark = pytest.mark.unbound

_SEEDED = {
    "platform_admin": {"id": "platform_admin", "capabilities": ["platform_settings", "cross_org"]},
    "org_admin": {"id": "org_admin", "capabilities": ["user_management"]},
}


def _membership(org_id: str) -> dict:
    return {
        "org_id": org_id,
        "org_name": org_id.title(),
        "joined_via": "invite",
        "acknowledged_at": None,
        "env_name": None,
    }


@pytest.fixture
def stack(monkeypatch):
    from provisa.api.app import AppState
    from provisa.api.auth_router import router as auth_router

    state = AppState()
    state.org_id = "default"
    state.multitenancy = True
    state.auth_config = {"provider": "basic"}
    root = state.org_registry.get("default")
    assert root is not None
    root.model_db = cast("Any", _Pool(rows_by_table={"roles": [{"id": "org_admin"}]}))
    root.roles = _SEEDED
    state.rate_limiter = NoopRateLimiter()
    monkeypatch.setattr("provisa.api.app.state", state)

    def _app(memberships: list[str]) -> TestClient:
        admin = _Pool(
            rows_by_table={
                "user_org_memberships": [_membership(o) for o in memberships],
                "user_profiles": [],
            }
        )
        state.admin_db = cast("Any", admin)
        app = FastAPI()
        # Added before the auth middleware, so it runs inside it, as on the server (app.py).
        app.add_middleware(RateLimitMiddleware)
        app.add_middleware(
            AuthMiddleware,
            provider=_Provider(),
            assignments_source="provisa",
            db_pool=_Pool(
                rows_by_table={
                    "user_role_assignments": [{"role_id": "org_admin", "domain_id": "*"}]
                }
            ),
            admin_pool=admin,
            multitenancy=True,
            default_org_id="default",
        )
        app.include_router(auth_router)
        return TestClient(app, raise_server_exceptions=False)

    return _app


def test_a_member_naming_no_org_gets_their_memberships(stack):
    resp = stack(["default"]).get("/auth/me", headers={"Authorization": "Bearer tok:m1"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["active_org_id"] is None
    assert [m["org_id"] for m in body["org_memberships"]] == ["default"]


def test_a_user_with_no_membership_gets_none(stack):
    resp = stack([]).get("/auth/me", headers={"Authorization": "Bearer tok:newbie"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["active_org_id"] is None
    assert body["org_memberships"] == []
