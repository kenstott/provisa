# Copyright (c) 2026 Kenneth Stott
# Canary: 452f4475-5294-42c0-8b5d-c9a023d6c707
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2155: adding a member to an environment takes a list of roles and entitlements in one step.
``POST /admin/users/{id}/access`` grants every role and sets every list in one transaction, or
none of them."""

# Requirements: REQ-2155, REQ-2219

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from provisa.api.admin.local_users_router import router
from provisa.api.errors import ApiError
from provisa.core.schema_org import roles as roles_table
from provisa.core.schema_org import user_role_assignments
from provisa.entitlements import runtime, store
from provisa.security.rights import Capability
from tests.pg_control_plane import ORG
from tests.unit.test_entitlement_lists import _ctx
from tests.unit.test_entitlements_api import ASSIGN, _identity, _role, _state

pytestmark = pytest.mark.asyncio

_PEOPLE = Capability.USER_MANAGEMENT.value


@pytest.fixture
async def api(control_plane, monkeypatch):
    state = _state(control_plane.org())
    state.roles["member_admin"] = _role([_PEOPLE, ASSIGN])
    state.role_chains["member_admin"] = ["member_admin"]
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr(runtime, "_role_context", lambda _state, _role: _ctx())
    caller: dict = {"identity": _identity("admin", "member_admin")}
    async with state.model_db.acquire() as conn:
        for role_id in ("viewer", "analyst"):
            await conn.execute_core(
                pg_insert(roles_table)
                .values(id=role_id, capabilities=[], domain_access=["*"])
                .on_conflict_do_nothing()
            )

    app = FastAPI()
    app.include_router(router)

    @app.exception_handler(ApiError)
    async def _coded(_request, exc):  # pyright: ignore[reportUnusedFunction]
        return JSONResponse({"detail": exc.detail, "code": exc.code}, status_code=exc.status_code)

    @app.middleware("http")
    async def _bind(request, call_next):  # pyright: ignore[reportUnusedFunction]
        identity = caller["identity"]
        request.state.identity = identity
        request.state.active_org_id = ORG
        with runtime.entitlements_scope(identity):
            return await call_next(request)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        yield SimpleNamespace(client=client, state=state, caller=caller, db=state.model_db)


async def _granted(api_, user: str) -> list[tuple[str, str]]:
    async with api_.db.acquire() as conn:
        rows = await conn.execute_core(
            select(user_role_assignments.c.role_id, user_role_assignments.c.domain_id)
            .where(user_role_assignments.c.user_id == user)
            .order_by(user_role_assignments.c.role_id)
        )
        return [(r.role_id, r.domain_id) for r in rows.fetchall()]


def _access(**over) -> dict:
    return {
        "role_ids": ["viewer", "analyst"],
        "domain_id": "*",
        "entitlements": [
            {"role_id": "analyst", "kind": "tables", "identifiers": ["finance.revenue"]}
        ],
        "reason": "joining finance",
        **over,
    }


async def test_one_save_grants_several_roles_and_their_lists(api):
    response = await api.client.post("/admin/users/alice/access", json=_access())
    assert response.status_code == 200, response.text
    assert [a["role_id"] for a in response.json()["assignments"]] == ["analyst", "viewer"]
    assert await _granted(api, "alice") == [("analyst", "*"), ("viewer", "*")]
    async with api.db.acquire() as conn:
        held = await store.load_held(conn)
    assert [(h.role_id, h.kind, h.identifiers) for h in held["alice"].lists] == [
        ("analyst", "tables", ("finance.revenue",))
    ]


async def test_roles_alone_need_no_reason(api):
    response = await api.client.post(
        "/admin/users/alice/access", json=_access(entitlements=[], reason="")
    )
    assert response.status_code == 200, response.text
    assert await _granted(api, "alice") == [("analyst", "*"), ("viewer", "*")]


async def test_a_save_naming_no_role_is_refused(api):
    response = await api.client.post("/admin/users/alice/access", json=_access(role_ids=[]))
    assert (response.status_code, response.json()["code"]) == (422, "users.roles_required")


async def test_a_list_of_a_role_the_save_does_not_grant_is_refused_and_nothing_is_granted(api):
    body = _access(
        role_ids=["viewer"],
        entitlements=[{"role_id": "analyst", "kind": "tables", "identifiers": ["x.y"]}],
    )
    response = await api.client.post("/admin/users/alice/access", json=body)
    assert (response.status_code, response.json()["code"]) == (
        422,
        "users.entitlement_role_not_granted",
    )
    assert await _granted(api, "alice") == []


async def test_lists_need_assign_entitlements_and_nothing_is_granted_without_it(api):
    api.state.roles["people"] = _role([_PEOPLE])
    api.caller["identity"] = _identity("admin", "people")
    response = await api.client.post("/admin/users/alice/access", json=_access())
    assert response.status_code == 403, response.text
    assert await _granted(api, "alice") == []


async def test_lists_need_a_reason(api):
    response = await api.client.post("/admin/users/alice/access", json=_access(reason=" "))
    assert (response.status_code, response.json()["code"]) == (
        422,
        "entitlements.reason_required",
    )
    assert await _granted(api, "alice") == []
