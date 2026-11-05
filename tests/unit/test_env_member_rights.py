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
    "org_admin": {
        "id": "org_admin",
        "description": "Test role.",
        "capabilities": [MANAGE, SWITCH, "user_management"],
    },
    "developer": {
        "id": "developer",
        "description": "Test role.",
        "capabilities": [MANAGE, SWITCH, "create_model"],
    },
    "analyst": {
        "id": "analyst",
        "description": "Test role.",
        "capabilities": ["usage", "query_development"],
    },
    "user_admin": {
        "id": "user_admin",
        "description": "Test role.",
        "capabilities": ["user_management", "org_settings"],
    },
    "platform_admin": {
        "id": "platform_admin",
        "description": "Test role.",
        "capabilities": ["platform_settings", "cross_org"],
    },
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


def _request(roles: list[str] | None, user_id: str = "u1", active_org: str | None = None):
    identity = (
        None if roles is None else SimpleNamespace(user_id=user_id, roles=roles, acting_roles=roles)
    )
    return SimpleNamespace(state=SimpleNamespace(identity=identity, active_org_id=active_org))


class TestMemberRights:
    async def test_a_developer_passes(self, wired):
        assert await er._member_rights(_request(["developer"]), "acme", MANAGE) == "u1"
        assert wired == [True]  # membership still checked, on top of the right

    async def test_an_org_admin_passes(self, wired):
        assert await er._member_rights(_request(["org_admin"]), "acme", MANAGE) == "u1"

    async def test_an_analyst_is_refused_before_the_membership_lookup(self, wired):
        # The analyst IS a member; that is the point. Membership was the whole of the old guard,
        # and REQ-1573 is what now refuses them — so the lookup must never be reached.
        with pytest.raises(ApiError) as exc:
            await er._member_rights(_request(["analyst"]), "acme", MANAGE)
        assert exc.value.status_code == 403
        assert exc.value.code == "environments.capability_required"
        assert wired == []

    async def test_either_right_opens_a_read_endpoint(self, wired):
        # The listing endpoints accept both: switching is reading which environments exist.
        holder = SimpleNamespace(user_id="u2", roles=["developer"], acting_roles=["developer"])
        assert (
            await er._member_rights(
                SimpleNamespace(state=SimpleNamespace(identity=holder)), "acme", MANAGE, SWITCH
            )
            == "u2"
        )
        with pytest.raises(ApiError):
            await er._member_rights(_request(["analyst"]), "acme", MANAGE, SWITCH)

    async def test_platform_authority_does_not_reach_an_orgs_branches(self, wired):
        """REQ-1327 (amended 2026-11-05): this test used to assert that cross_org bypassed the
        right and the membership row. cross_org is authority over an org's lifecycle only; a
        platform admin never reads or acts on an org's model branches, so it is refused, by the
        name of the right it lacks, before the membership lookup."""
        with pytest.raises(ApiError) as exc:
            await er._member_rights(_request(["platform_admin"]), "acme", MANAGE)
        assert exc.value.status_code == 403
        assert exc.value.code == "environments.capability_required"
        assert repr(MANAGE) in exc.value.detail
        assert wired == []

    async def test_dev_no_auth_is_exempt(self, wired):
        assert await er._member_rights(_request(None), "acme", MANAGE) is None
        assert wired == []


class TestReadsParent:
    """REQ-1942: showing a parent's real rows needs the right to read them there."""

    async def test_platform_authority_does_not_read_a_parent(self, wired):
        # REQ-1327 (amended 2026-11-05): this used to return early for cross_org. A platform admin
        # never reads an org's model branches, so it needs the usage right like anyone else.
        with pytest.raises(ApiError) as exc:
            await er._reads_parent(_request(["platform_admin"]), "acme", er.PROD)
        assert exc.value.status_code == 403
        assert exc.value.code == "environments.parent_unreadable"
        assert "'usage'" in exc.value.detail

    async def test_a_holder_of_usage_reads_prod(self, wired):
        await er._reads_parent(_request(["analyst"]), "acme", er.PROD)


class TestGuard:
    """REQ-1327 (amended 2026-11-05): protecting, deleting, deciding merges and the repo
    integration are environment management — environment_management in the org being acted in,
    never user_management and never cross_org."""

    @pytest.fixture(autouse=True)
    def _ready(self, monkeypatch):
        async def _noop(_org_id):
            return None

        monkeypatch.setattr(er, "_org_ready_on_this_node", _noop)

    async def test_a_holder_of_environment_management_in_the_org_passes(self, wired):
        assert await er._guard(_request(["developer"], active_org="acme"), "acme") == "u1"
        assert wired == [True]  # membership still checked, on top of the right

    async def test_a_platform_admin_is_refused_by_the_rights_name(self, wired):
        with pytest.raises(ApiError) as exc:
            await er._guard(_request(["platform_admin"], active_org="root"), "acme")
        assert exc.value.status_code == 403
        assert exc.value.code == "environments.capability_required"
        assert repr(MANAGE) in exc.value.detail
        assert wired == []

    async def test_user_management_does_not_stand_in_for_it(self, wired):
        with pytest.raises(ApiError) as exc:
            await er._guard(_request(["user_admin"], active_org="acme"), "acme")
        assert exc.value.status_code == 403
        assert exc.value.code == "environments.capability_required"
        assert repr(MANAGE) in exc.value.detail

    async def test_the_right_is_confined_to_the_org_being_acted_in(self, wired):
        with pytest.raises(ApiError) as exc:
            await er._guard(_request(["developer"], active_org="beta"), "acme")
        assert exc.value.status_code == 403
        assert exc.value.code == "environments.capability_required_in_org"
        assert MANAGE in exc.value.detail

    async def test_dev_no_auth_is_exempt(self, wired):
        assert await er._guard(_request(None), "acme") is None
