# Copyright (c) 2026 Kenneth Stott
# Canary: 92a91193-760a-41a3-a660-30608785a96c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2155 (amending REQ-1573): environment_owned_management -- manage the environments a member
owns. A developer holds it: it creates an environment, becoming its owner, and deletes one it owns,
never another's. environment_management, org_admin's alone, manages any environment."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import environments_router as router
from provisa.core.db import STANDARD_ROLE_RIGHTS


class _Refused(Exception):
    pass


@pytest.fixture
def plane(monkeypatch):
    """Ada owns `ada-branch`; Bob owns `bob-branch`. Each holds the given rights."""
    owners = {"ada-branch": "ada", "bob-branch": "bob"}
    held: dict[str, set[str]] = {}

    async def _owns(_admin, _org, name, user_id):
        return owners.get(name) == user_id

    async def _org_admin(_request, _org):
        raise _Refused("org_admin")

    async def _none(*_a, **_k):
        return None

    monkeypatch.setattr(router, "owns_environment", _owns)
    monkeypatch.setattr(router, "_guard", _org_admin)
    monkeypatch.setattr(router, "_confined", _none)
    monkeypatch.setattr(router, "_org_ready_on_this_node", _none)
    monkeypatch.setattr(router, "_admin_pool", lambda: None)
    monkeypatch.setattr(router, "_caller_user_id", lambda request: request.user)
    monkeypatch.setattr(
        router, "_caller_capabilities", lambda request: held.get(request.user, set())
    )

    def request(user: str, *rights: str):
        held[user] = set(rights)
        return SimpleNamespace(user=user)

    return request


async def test_an_owner_manages_its_own_environment(plane):
    request = plane("ada", "environment_owned_management")
    assert await router._guard_owned(request, "acme", "ada-branch") == "ada"


async def test_an_owner_manages_no_one_elses_environment(plane):
    request = plane("ada", "environment_owned_management")
    with pytest.raises(_Refused):
        await router._guard_owned(request, "acme", "bob-branch")


async def test_owning_without_the_right_manages_nothing(plane):
    request = plane("ada")
    with pytest.raises(_Refused):
        await router._guard_owned(request, "acme", "ada-branch")


def test_a_developer_manages_what_it_owns_and_only_org_admin_manages_any():
    assert "environment_owned_management" in STANDARD_ROLE_RIGHTS["developer"]
    assert "environment_management" not in STANDARD_ROLE_RIGHTS["developer"]
    assert "environment_management" in STANDARD_ROLE_RIGHTS["org_admin"]
    assert (
        "environment_owned_management"
        not in STANDARD_ROLE_RIGHTS["sandbox"] - (STANDARD_ROLE_RIGHTS["developer"])
    )


def test_creating_an_environment_admits_either_right():
    assert router.CREATE_RIGHTS == ("environment_management", "environment_owned_management")


def test_a_branch_owner_keeps_managing_its_branch():
    from provisa.core.env_authority import ENVIRONMENT_OWNER_CAPABILITIES

    assert "environment_owned_management" in ENVIRONMENT_OWNER_CAPABILITIES
