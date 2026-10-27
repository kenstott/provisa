# Copyright (c) 2026 Kenneth Stott
# Canary: 811aa5b4-40da-4ae9-bc9e-13571f450c3f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A role's effective capabilities, as its panel draws them (REQ-2161): the lineage from the
standard roles to the role, what each role in it adds, and every capability with the role it
comes from."""

from __future__ import annotations

from provisa.security.effective_rights import capabilities_view

ROWS = [
    {"id": "viewer", "capabilities": ["read"], "parents": []},
    {"id": "dq_steward", "capabilities": ["read", "dq_rw"], "parents": ["viewer"]},
    {"id": "dp_owner", "capabilities": ["data_product_owned_rw"], "parents": ["viewer"]},
    {"id": "dq_lead", "capabilities": ["export"], "parents": ["dq_steward", "dp_owner"]},
]
STANDARD = {"viewer", "dq_steward", "dp_owner"}


def test_each_capability_names_the_role_it_comes_from():
    view = capabilities_view("dq_lead", ROWS, standard=STANDARD)
    assert view["role"] == "dq_lead"
    assert view["standard"] is False
    assert view["chain"] == ["dq_lead", "dq_steward", "viewer", "dp_owner"]
    assert view["capabilities"] == [
        {"right": "data_product_owned_rw", "from": ["dp_owner"]},
        {"right": "dq_rw", "from": ["dq_steward"]},
        {"right": "export", "from": ["dq_lead"]},
        {"right": "read", "from": ["viewer"]},
    ]


def test_the_lineage_has_every_role_s_parents_and_what_it_adds():
    view = capabilities_view("dq_lead", ROWS, standard=STANDARD)
    assert view["parents"] == {
        "dq_lead": ["dq_steward", "dp_owner"],
        "dq_steward": ["viewer"],
        "viewer": [],
        "dp_owner": ["viewer"],
    }
    # A capability is shown where it first enters: dq_steward's own "read" is viewer's.
    assert view["adds"] == {
        "viewer": ["read"],
        "dq_steward": ["dq_rw"],
        "dp_owner": ["data_product_owned_rw"],
        "dq_lead": ["export"],
    }
    assert view["standards"] == ["dq_steward", "viewer", "dp_owner"]


def test_a_right_entering_in_two_branches_comes_from_both():
    diamond = [
        {"id": "base", "capabilities": ["read"], "parents": []},
        {"id": "left", "capabilities": ["relate"], "parents": ["base"]},
        {"id": "right", "capabilities": ["relate", "write"], "parents": ["base"]},
        {"id": "top", "capabilities": [], "parents": ["left", "right"]},
    ]
    view = capabilities_view("top", diamond, standard=set())
    assert {"right": "relate", "from": ["left", "right"]} in view["capabilities"]
    assert view["adds"]["left"] == ["relate"] and view["adds"]["right"] == ["relate", "write"]


def test_a_right_restated_by_a_child_is_added_only_where_it_enters():
    rows = [
        {"id": "base", "capabilities": ["read"], "parents": []},
        {"id": "child", "capabilities": ["read", "export"], "parents": ["base"]},
    ]
    view = capabilities_view("child", rows, standard=set())
    assert view["adds"] == {"child": ["export"], "base": ["read"]}
    assert {"right": "read", "from": ["base"]} in view["capabilities"]


def test_on_the_standard_roster_org_admin_relates_through_developer_and_data_steward():
    from provisa.core.db import STANDARD_ROLES, standard_role_dicts

    view = capabilities_view("org_admin", standard_role_dicts(), standard=STANDARD_ROLES)
    assert {"right": "create_relationship", "from": ["developer", "data_steward"]} in view[
        "capabilities"
    ]


def test_a_meta_role_is_a_child_of_its_members():
    view = capabilities_view("meta:dp_owner+dq_steward", ROWS, standard=STANDARD)
    assert view["parents"]["meta:dp_owner+dq_steward"] == ["dp_owner", "dq_steward"]
    assert view["adds"]["meta:dp_owner+dq_steward"] == []
    assert {c["right"] for c in view["capabilities"]} == {"read", "dq_rw", "data_product_owned_rw"}


# --- GET /admin/roles/{role_id}/effective ----------------------------------------------------------

import pytest  # noqa: E402
from fastapi import Request  # noqa: E402

from provisa.api.admin import roles_router  # noqa: E402
from provisa.api.errors import ApiError  # noqa: E402


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return [type("Row", (), {"_mapping": r})() for r in self._rows]


class _Conn:
    async def execute_core(self, _statement):
        return _Rows(ROWS)


class _Pool:
    def acquire(self):
        class _Ctx:
            async def __aenter__(self):
                return _Conn()

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


def _request() -> Request:
    request = Request({"type": "http", "headers": [], "method": "GET", "path": "/admin/roles/x"})
    request.state.active_org_id = "acme"
    return request


@pytest.fixture
def wired(monkeypatch):
    def wire(visible):
        monkeypatch.setattr(roles_router, "_pool", lambda _request: _Pool())
        monkeypatch.setattr(
            roles_router, "role_definitions_visible", lambda *_a: lambda rid: rid in visible
        )
        monkeypatch.setattr("provisa.core.db.STANDARD_ROLES", dict.fromkeys(STANDARD))

        # The data half is read from the runtime (tests/unit/test_effective_data.py).
        async def data(_request, role_id, parents):
            return [{"role": role_id, "parents": parents[role_id]}]

        monkeypatch.setattr(roles_router, "_data_access", data)

    return wire


@pytest.mark.asyncio
async def test_the_endpoint_answers_a_role_the_caller_may_read(wired):
    wired({"dq_lead"})
    view = await roles_router.effective_rights("dq_lead", _request())
    assert view["chain"] == ["dq_lead", "dq_steward", "viewer", "dp_owner"]
    assert {"right": "export", "from": ["dq_lead"]} in view["capabilities"]
    assert view["data"] == [{"role": "dq_lead", "parents": ["dq_steward", "dp_owner"]}]


@pytest.mark.asyncio
async def test_a_role_the_caller_may_not_read_is_not_found(wired):
    wired({"viewer"})
    with pytest.raises(ApiError) as refused:
        await roles_router.effective_rights("dq_lead", _request())
    assert refused.value.code == "roles.not_found"


@pytest.mark.asyncio
async def test_a_meta_role_is_answered_when_every_member_is_readable(wired):
    wired({"dp_owner", "dq_steward"})
    view = await roles_router.effective_rights("meta:dp_owner+dq_steward", _request())
    assert view["parents"]["meta:dp_owner+dq_steward"] == ["dp_owner", "dq_steward"]
