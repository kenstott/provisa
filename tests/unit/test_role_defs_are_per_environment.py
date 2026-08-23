# Copyright (c) 2026 Kenneth Stott
# Canary: 5a1f8c30-47b2-4d6e-9f81-c30b2e7a4d95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a lower lane comes to hold different rights from prod (REQ-1539).

The ``roles`` table lives in the ENVIRONMENT's schema, so redefining ``developer`` is an edit to one
environment's own row. Two things have to be true for that to be a mechanism rather than a hazard:
the edit must be reachable by whoever administers that environment, and it must not be reachable by
anyone who merely inherited from it.
"""

# Requirements: REQ-1528, REQ-1531, REQ-1539

from __future__ import annotations

import pytest
from fastapi import Request

from provisa.api.admin import roles_router
from provisa.api.errors import ApiError
from provisa.core.db import _SEED_ROLES
from provisa.core.env_authority import ENVIRONMENT_OWNER_CAPABILITIES

pytestmark = pytest.mark.asyncio


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Row:
    def __init__(self, mapping):
        self._mapping = mapping


class _Conn:
    """Answers the SELECT with one seeded role and records what would be written."""

    def __init__(self, role_id):
        self.role_id = role_id
        self.writes: list = []

    async def execute_core(self, statement):
        if statement.is_select:
            return _Result(
                _Row(
                    {
                        "id": self.role_id,
                        "capabilities": ["query_development", "write"],
                        "domain_access": ["*"],
                        "org_id": None,  # seeded from the template, in THIS environment's schema
                    }
                )
            )
        self.writes.append(statement)
        return _Result(None)


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                return pool.conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


@pytest.fixture
def wired(monkeypatch):
    def wire(role_id):
        conn = _Conn(role_id)
        monkeypatch.setattr(roles_router, "_pool", lambda _request: _Pool(conn))
        monkeypatch.setattr(roles_router, "require_capability_request", lambda *a, **k: None)
        return conn

    return wire


def _request() -> Request:
    return Request({"type": "http", "headers": [], "method": "PUT", "path": "/admin/roles/x"})


class TestALaneRedefinesItsOwnRoles:
    @pytest.mark.parametrize("role_id", ["developer", "analyst", "modeler"])
    async def test_a_seeded_role_can_be_narrowed_in_this_environment(self, wired, role_id):
        # REQ-1539: "developer can be unrestricted in dev and hold zero data rights in prod" is only
        # true if this call succeeds — the row is this environment's own copy, and no merge, load or
        # checkout carries it anywhere else.
        conn = wired(role_id)
        answer = await roles_router.update_role(
            role_id, roles_router.UpdateRoleBody(capabilities=[]), _request()
        )
        assert answer["capabilities"] == []
        assert conn.writes  # it really was written, not reported

    async def test_what_is_not_named_is_left_alone(self, wired):
        wired("developer")
        answer = await roles_router.update_role(
            "developer", roles_router.UpdateRoleBody(capabilities=["usage"]), _request()
        )
        assert answer["domain_access"] == ["*"]


class TestTheRoleThatAdministersIsNotEditable:
    @pytest.mark.parametrize("role_id", ["org_admin", "platform_admin"])
    async def test_narrowing_the_administrator_is_refused(self, wired, role_id):
        # These carry user_management itself: an org_admin who narrowed their own role would leave
        # the environment with nobody able to undo it.
        conn = wired(role_id)
        with pytest.raises(ApiError) as exc:
            await roles_router.update_role(
                role_id, roles_router.UpdateRoleBody(capabilities=[]), _request()
            )
        assert exc.value.code == "roles.cannot_modify_administrative"
        assert conn.writes == []


class TestInheritingDoesNotConferTheAbilityToRedefine:
    async def test_creating_an_environment_confers_no_user_management(self):
        # REQ-1528: the branch owner holds the seeded developer role less the data rights. Every
        # role endpoint requires user_management, which developer does not hold — so a person who
        # inherited an environment's role definitions cannot rewrite them there or anywhere.
        assert "user_management" not in ENVIRONMENT_OWNER_CAPABILITIES
        assert "user_management" not in dict(_SEED_ROLES)["developer"]
