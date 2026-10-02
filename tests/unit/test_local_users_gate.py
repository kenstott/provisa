# Copyright (c) 2026 Kenneth Stott
# Canary: b41d9e07-5a2c-4c83-8f16-7e30a9d5c2b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The local-users router is gated on user_management, and platform roles stay with the platform."""

# Requirements: REQ-124, REQ-125, REQ-1308, REQ-1337

from __future__ import annotations

import types

import pytest

import provisa.api.app as appmod
from provisa.api.admin import local_users_router as router
from provisa.api.errors import ApiError

_ROLES = {
    "people_admin": {"capabilities": ["user_management"], "domain_access": ["*"]},
    "nobody": {"capabilities": ["observability"], "domain_access": ["*"]},
    "platform_admin": {
        "capabilities": ["platform_settings", "cross_org"],
        "domain_access": ["*"],
    },
}


def _request(user_id: str, *roles: str):
    identity = types.SimpleNamespace(user_id=user_id, roles=list(roles))
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))


@pytest.fixture(autouse=True)
def _roles(monkeypatch):
    monkeypatch.setattr(appmod.state, "roles", _ROLES, raising=False)


@pytest.fixture
def past_gate(monkeypatch):
    """Both pools raise a marker, so reaching one proves the gate let the caller through."""

    def _stop(_request):
        raise RuntimeError("past the gate")

    monkeypatch.setattr(router, "_admin_pool", _stop)
    monkeypatch.setattr(router, "_pool", _stop)


async def test_a_caller_without_user_management_is_refused_everywhere(past_gate):
    req = _request("alice", "nobody")
    body = router.AssignmentBody(role_id="analyst", domain_id="d")
    calls = [
        router.list_users(req),
        router.get_user("u1", req),
        router.create_user(router.CreateUserBody(username="x", password="y"), req),
        router.update_user("u1", router.UpdateUserBody(email="a@b"), req),
        router.delete_user("u1", req),
        router.change_password("u1", router.ChangePasswordBody(password="p"), req),
        router.list_assignments("u1", req),
        router.add_assignment("u1", body, req),
        router.remove_assignment("u1", 1, req),
    ]
    for call in calls:
        with pytest.raises(ApiError) as err:
            await call
        assert err.value.status_code == 403
        assert err.value.code == "auth.missing_capability"


async def test_user_management_passes_the_gate(past_gate):
    with pytest.raises(RuntimeError, match="past the gate"):
        await router.list_users(_request("alice", "people_admin"))


@pytest.mark.parametrize("role", ["people_admin", "platform_admin"])
async def test_removing_a_user_is_open_to_user_management_and_to_the_cross_org_right(
    past_gate, role
):
    """An org administrator removes a person from their org; the holder of the cross-org right
    deletes the account. Either passes the gate; which of the two happens is the route's."""
    with pytest.raises(RuntimeError, match="past the gate"):
        await router.delete_user("u1", _request("alice", role))


async def test_a_user_may_change_their_own_password_without_user_management(past_gate):
    with pytest.raises(RuntimeError, match="past the gate"):
        await router.change_password(
            "alice", router.ChangePasswordBody(password="p"), _request("alice", "nobody")
        )


async def test_user_management_cannot_grant_a_platform_role(past_gate):
    req = _request("alice", "people_admin")
    with pytest.raises(ApiError) as err:
        await router.add_assignment(
            "u1", router.AssignmentBody(role_id="platform_admin", domain_id="d"), req
        )
    assert err.value.code == "users.platform_role_requires_platform_admin"
    with pytest.raises(ApiError) as err:
        await router.update_user("u1", router.UpdateUserBody(roles=["platform_admin"]), req)
    assert err.value.code == "users.platform_role_requires_platform_admin"
    with pytest.raises(ApiError) as err:
        await router.create_user(
            router.CreateUserBody(username="x", password="y", roles=["platform_admin"]), req
        )
    assert err.value.code == "users.platform_role_requires_platform_admin"


async def test_a_holder_of_the_platform_rights_may_grant_a_platform_role(past_gate):
    # Two questions, two rights: user_management admits the caller to the surface, and the
    # platform rights are what a role carrying them is granted WITH.
    req = _request("root", "platform_admin", "people_admin")
    with pytest.raises(RuntimeError, match="past the gate"):
        await router.add_assignment(
            "u1", router.AssignmentBody(role_id="platform_admin", domain_id="d"), req
        )


async def test_the_platform_rights_alone_do_not_manage_an_orgs_users(past_gate):
    # REQ-1327: managing an org's people is that org's data plane.
    req = _request("root", "platform_admin")
    with pytest.raises(ApiError) as err:
        await router.add_assignment(
            "u1", router.AssignmentBody(role_id="platform_admin", domain_id="d"), req
        )
    assert (err.value.status_code, err.value.code) == (403, "auth.missing_capability")


async def test_the_self_change_refusal_stands_behind_the_gate(past_gate):
    req = _request("alice", "people_admin")
    with pytest.raises(ApiError) as err:
        await router.add_assignment(
            "alice", router.AssignmentBody(role_id="analyst", domain_id="d"), req
        )
    assert err.value.code == "users.self_role_change"
