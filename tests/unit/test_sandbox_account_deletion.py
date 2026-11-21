# Copyright (c) 2026 Kenneth Stott
# Canary: b9d04e67-2a1c-4f35-8d90-5c7e3a1f6b82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1931: leaving the sandbox deletes the visitor's account, and only a sandbox-only account."""

from __future__ import annotations

import types

import pytest
from starlette.requests import Request

from provisa.api import auth_router
from provisa.api.errors import ApiError
from provisa.api.sandbox_org import SANDBOX_ORG_ID


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, plane):
        self._plane = plane

    async def execute_core(self, stmt):
        if str(stmt).lstrip().upper().startswith("SELECT"):
            return _Result([(o,) for o in self._plane.memberships])
        self._plane.calls.append("delete_profile")
        return _Result([])


class _Db:
    def __init__(self, plane):
        self._plane = plane

    def acquire(self):
        conn = _Conn(self._plane)

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()


def _request(user_id="visitor-1") -> Request:
    req = Request({"type": "http", "headers": []})
    req.state.identity = types.SimpleNamespace(user_id=user_id)
    return req


@pytest.fixture
def plane(monkeypatch):
    p = types.SimpleNamespace(memberships=[SANDBOX_ORG_ID], calls=[], idp_error=None)
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(admin_db=_Db(p)), raising=False
    )

    async def _org_model_db(org_id):
        return "model-db"

    def _delete_user(uid):
        p.calls.append(f"idp_delete:{uid}")
        if p.idp_error:
            raise p.idp_error

    async def _remove_from_org(admin_db, model_db, user_id, org_id):
        p.calls.append(f"remove_from_org:{user_id}:{org_id}")

    async def _record_people(org_id, principals):
        p.calls.append(f"record_people:{org_id}:{','.join(principals)}")

    monkeypatch.setattr("provisa.api.admin.orgs_router._org_model_db", _org_model_db)
    monkeypatch.setattr("provisa.auth.providers.firebase.delete_user", _delete_user)
    monkeypatch.setattr("provisa.core.org_membership.remove_from_org", _remove_from_org)
    # REQ-2219: the removed role grants recompute the visitor's effective rows in the sandbox's
    # prod model store, whose runtime _org_model_db built; the stand-in records the recompute.
    monkeypatch.setattr("provisa.api.effective_rows.record_people", _record_people)
    return p


async def test_a_sandbox_only_visitor_is_deleted_identity_provider_first(plane):
    out = await auth_router.delete_sandbox_account(_request())
    assert out == {"deleted": "visitor-1"}
    assert plane.calls == [
        "idp_delete:visitor-1",
        f"remove_from_org:visitor-1:{SANDBOX_ORG_ID}",
        f"record_people:{SANDBOX_ORG_ID}:visitor-1",
        "delete_profile",
    ]


@pytest.mark.parametrize("memberships", [[SANDBOX_ORG_ID, "acme"], ["acme"], []])
async def test_an_account_with_any_other_membership_is_never_deleted(plane, memberships):
    plane.memberships = memberships
    with pytest.raises(ApiError) as err:
        await auth_router.delete_sandbox_account(_request())
    assert err.value.status_code == 409
    assert err.value.code == "auth.not_sandbox_only"
    assert plane.calls == []


async def test_an_identity_provider_failure_leaves_membership_and_profile_in_place(plane):
    plane.idp_error = RuntimeError("iam denied")
    with pytest.raises(RuntimeError):
        await auth_router.delete_sandbox_account(_request())
    assert plane.calls == ["idp_delete:visitor-1"]


async def test_an_anonymous_caller_is_refused(plane):
    with pytest.raises(ApiError) as err:
        await auth_router.delete_sandbox_account(_request("anonymous"))
    assert err.value.status_code == 401
    assert plane.calls == []
