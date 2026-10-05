# Copyright (c) 2026 Kenneth Stott
# Canary: 03637cb0-3fb2-4c95-9413-9c8dd658f078
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/auth/me reads roles from the org the request acts in, or from the platform plane (REQ-1266).

A signed-in user with no org yet (mid-onboarding) reaches /auth/me with nothing bound: their roles
are read on the platform plane, named as such. A request acting in an org reads that org's own,
never the platform's.
"""

# Requirements: REQ-1266, REQ-1297, REQ-1327

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.api.org_runtime import OrgRuntime
from provisa.auth.models import AuthIdentity
from provisa.core.request_context import reset_current_org, set_current_org

pytestmark = pytest.mark.unbound


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return None


class _Db:
    """A store whose roles table holds ``role_ids`` and which has no memberships or profile."""

    def __init__(self, role_ids: list[str]) -> None:
        self._role_ids = role_ids

    def acquire(self):
        db = self

        class _Conn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_exc):
                return False

            async def execute_core(self, stmt):
                if "roles" in str(stmt) and "user_profiles" not in str(stmt):
                    return _Result([(r,) for r in db._role_ids])
                return _Result([])

        return _Conn()


@pytest.fixture
def state(monkeypatch):
    from provisa.api.app import AppState

    state = AppState()
    state.org_id = "root"
    state.auth_config = {"provider": "oidc"}
    state.admin_db = cast("Any", _Db([]))
    root = state.org_registry.get("root")
    assert root is not None
    root.model_db = cast("Any", _Db(["platform_admin"]))
    acme = OrgRuntime(org_id="acme")
    acme.model_db = cast("Any", _Db(["acme_analyst"]))
    state.org_registry.set("acme", acme)
    monkeypatch.setattr("provisa.api.app.state", state)
    monkeypatch.setattr("provisa.federation.engine_wake.prewarm_engine", lambda *_a: None)
    return state


def _request(active_org_id: str | None, role_id: str):
    identity = AuthIdentity(
        user_id="u1",
        email=None,
        display_name=None,
        roles=[f"{role_id}:*"],
        raw_claims={},
    )
    request = SimpleNamespace(state=SimpleNamespace(identity=identity, active_org_id=active_org_id))
    return cast("Any", request)


async def test_a_user_with_no_org_reads_the_platform_plane(state):
    from provisa.api.auth_router import me

    body = await me(_request(None, "platform_admin"))
    assert body["active_org_id"] is None
    assert body["assignments"] == [{"role_id": "platform_admin", "domain_id": "*"}]


async def test_an_org_bound_request_reads_its_own_orgs_roles(state):
    from provisa.api.auth_router import me

    token = set_current_org("acme")  # as the org-routing middleware binds it
    try:
        body = await me(_request("acme", "acme_analyst"))
    finally:
        reset_current_org(token)
    assert body["assignments"] == [{"role_id": "acme_analyst", "domain_id": "*"}]
