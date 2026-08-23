# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1c4e39-58bd-4f27-8a05-91b2c7d3e4f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1573: the environments router's member guard reads a right, not just membership."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import environments_router as er
from provisa.api.errors import ApiError

MANAGE = er.MANAGE_CAPABILITY
SWITCH = er.SWITCH_CAPABILITY

_ROLES = {
    "org_admin": {"id": "org_admin", "capabilities": [MANAGE, SWITCH, "user_management"]},
    "developer": {"id": "developer", "capabilities": [MANAGE, SWITCH, "create_model"]},
    "analyst": {"id": "analyst", "capabilities": ["usage", "query_development"]},
    "platform_admin": {"id": "platform_admin", "capabilities": ["admin", "cross_org"]},
}


class _Conn:
    """The membership lookup, recording whether the guard ever got as far as asking."""

    def __init__(self, asked: list):
        self.asked = asked

    async def execute_core(self, _stmt):
        self.asked.append(True)
        return SimpleNamespace(fetchone=lambda: ("acme",))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _Pool:
    def __init__(self, asked: list):
        self.asked = asked

    def acquire(self):
        return _Conn(self.asked)


@pytest.fixture
def wired(monkeypatch):
    asked: list = []
    monkeypatch.setattr(er, "_admin_pool", lambda: _Pool(asked))
    monkeypatch.setattr("provisa.api.app.state", SimpleNamespace(roles=_ROLES))
    return asked


def _request(roles: list[str] | None, user_id: str = "u1"):
    identity = None if roles is None else SimpleNamespace(user_id=user_id, roles=roles)
    return SimpleNamespace(state=SimpleNamespace(identity=identity))


class TestMemberRights:
    async def test_a_developer_passes(self, wired):
        assert await er._member(_request(["developer"]), "acme", MANAGE) == "u1"
        assert wired == [True]  # membership still checked, on top of the right

    async def test_an_org_admin_passes(self, wired):
        assert await er._member(_request(["org_admin"]), "acme", MANAGE) == "u1"

    async def test_an_analyst_is_refused_before_the_membership_lookup(self, wired):
        # The analyst IS a member; that is the point. Membership was the whole of the old guard,
        # and REQ-1573 is what now refuses them — so the lookup must never be reached.
        with pytest.raises(ApiError) as exc:
            await er._member(_request(["analyst"]), "acme", MANAGE)
        assert exc.value.status_code == 403
        assert exc.value.code == "environments.capability_required"
        assert wired == []

    async def test_either_right_opens_a_read_endpoint(self, wired):
        # The listing endpoints accept both: switching is reading which environments exist.
        holder = SimpleNamespace(user_id="u2", roles=["developer"])
        assert (
            await er._member(
                SimpleNamespace(state=SimpleNamespace(identity=holder)), "acme", MANAGE, SWITCH
            )
            == "u2"
        )
        with pytest.raises(ApiError):
            await er._member(_request(["analyst"]), "acme", MANAGE, SWITCH)

    async def test_platform_authority_bypasses_the_right(self, wired):
        # cross_org is authority over any org's lifecycle (REQ-1337); it has never needed the
        # org's own membership row either.
        assert await er._member(_request(["platform_admin"]), "acme", MANAGE) == "u1"
        assert wired == []

    async def test_dev_no_auth_is_exempt(self, wired):
        assert await er._member(_request(None), "acme", MANAGE) is None
        assert wired == []
