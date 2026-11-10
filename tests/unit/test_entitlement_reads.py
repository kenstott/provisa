# Copyright (c) 2026 Kenneth Stott
# Canary: 2f8d6a41-0c7e-4b95-8e3a-71d9c4b5e062
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219, ONE API, THREE WAYS TO USE IT: the picker's two reads. The roles a person holds, from
Provisa's assignments and from the identity provider, each with its origin; and the kinds the
write accepts, from the definition the write checks, with the names tree a list is picked from."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from provisa.api.admin import entitlements_router
from provisa.api.admin.entitlements_router import router
from provisa.api.errors import ApiError
from provisa.core.schema_org import roles as roles_table
from provisa.core.schema_org import user_role_assignments
from provisa.entitlements import runtime, store
from provisa.entitlements.index import KIND_ORDER, KINDS, SEMANTIC_LISTS
from tests.pg_control_plane import ORG
from tests.unit.test_entitlement_lists import _ctx
from tests.unit.test_entitlements_api import ASSIGN, _identity, _set, _state

pytestmark = pytest.mark.asyncio

_FROM_PROVISA = {"provider": "oidc", "assignments_source": "provisa"}


@pytest.fixture
async def api(control_plane, monkeypatch):
    state = _state(control_plane.org())
    state.auth_config = dict(_FROM_PROVISA)
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr(runtime, "_role_context", lambda _state, _role: _ctx())
    caller: dict = {"identity": _identity()}

    app = FastAPI()
    app.include_router(router)

    @app.exception_handler(ApiError)
    async def _coded(_request, exc):  # pyright: ignore[reportUnusedFunction]
        return JSONResponse(
            {"detail": exc.detail, "code": exc.code, "params": exc.params},
            status_code=exc.status_code,
        )

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


async def _assign(api_, user: str, role: str, domain: str = "*", expires_at=None) -> None:
    async with api_.db.acquire() as conn:
        await conn.execute_core(
            pg_insert(roles_table)
            .values(id=role, capabilities=[], domain_access=["*"])
            .on_conflict_do_nothing()
        )
        await conn.execute_core(
            insert(user_role_assignments).values(
                user_id=user, role_id=role, domain_id=domain, expires_at=expires_at
            )
        )


async def _idp(api_, principal: str, lists: dict) -> None:
    async with api_.db.acquire() as conn:
        await store.mirror_idp(conn, principal, lists)
        api_.state.idp_entitlements = await store.load_idp(conn)


async def _roles(api_, principal: str) -> list[dict]:
    response = await api_.client.get("/admin/entitlements/roles", params={"principal": principal})
    assert response.status_code == 200, response.text
    assert response.json()["principal"] == principal
    return response.json()["roles"]


# -- the person's roles ------------------------------------------------------------------------


async def test_a_role_only_the_identity_provider_speaks_to_is_listed_with_its_origin(api):
    await _assign(api, "alice", "viewer")
    await _idp(api, "alice", {"analyst": {"tables": ("finance.revenue",)}})
    assert await _roles(api, "alice") == [
        {"role_id": "analyst", "origins": ["idp"], "domains": []},
        {"role_id": "viewer", "origins": ["provisa"], "domains": ["*"]},
    ]


async def test_a_role_from_both_sources_is_listed_once_with_each_origin(api):
    await _assign(api, "alice", "viewer", "sales")
    await _assign(api, "alice", "viewer", "finance")
    await _idp(api, "alice", {"viewer": {"tables": ()}})
    assert await _roles(api, "alice") == [
        {"role_id": "viewer", "origins": ["provisa", "idp"], "domains": ["finance", "sales"]},
    ]


async def test_an_expired_assignment_is_not_held(api):
    await _assign(api, "alice", "viewer", expires_at=datetime.now(UTC) - timedelta(hours=1))
    await _assign(api, "alice", "analyst")
    assert [r["role_id"] for r in await _roles(api, "alice")] == ["analyst"]


async def test_another_persons_roles_are_not_listed(api):
    await _assign(api, "bob", "viewer")
    await _idp(api, "bob", {"analyst": {"tables": ()}})
    assert await _roles(api, "alice") == []


async def test_a_person_with_no_assignment_holds_the_default_assignments(api):
    api.state.auth_config = {
        **_FROM_PROVISA,
        "default_assignments": [{"role_id": "viewer", "domain_id": "*"}],
    }
    assert await _roles(api, "carol") == [
        {"role_id": "viewer", "origins": ["default_assignment"], "domains": ["*"]},
    ]
    await _assign(api, "carol", "analyst")
    assert [r["role_id"] for r in await _roles(api, "carol")] == ["analyst"]


async def test_assignments_are_not_held_where_the_deployment_reads_claims(api):
    api.state.auth_config = {"provider": "oidc", "assignments_source": "claims"}
    await _assign(api, "alice", "viewer")
    await _idp(api, "alice", {"analyst": {"tables": ()}})
    assert await _roles(api, "alice") == [
        {"role_id": "analyst", "origins": ["idp"], "domains": []},
    ]


async def test_a_caller_without_assign_entitlements_is_refused_the_roles(api):
    api.caller["identity"] = _identity(role="people")
    response = await api.client.get("/admin/entitlements/roles", params={"principal": "alice"})
    assert response.status_code == 403
    assert response.json()["code"] == "auth.missing_capability"
    assert ASSIGN in response.json()["detail"]


async def test_the_roles_are_those_the_caller_administers(api):
    async with api.db.acquire() as conn:
        await store.set_list(
            conn, "scoped", "scoped_granter", "roles", ["viewer"], set_by="o", reason="r"
        )
        api.state.held_entitlements = await store.load_held(conn)
    api.caller["identity"] = _identity("scoped", "scoped_granter")
    await _assign(api, "alice", "analyst")
    await _assign(api, "alice", "nobody")
    await _idp(api, "alice", {"viewer": {"tables": ()}})
    # analyst has viewer as an ancestor; nobody is outside the caller's roles list
    assert [r["role_id"] for r in await _roles(api, "alice")] == ["analyst", "viewer"]


# -- the kinds and the names -------------------------------------------------------------------


async def _kinds(api_) -> dict:
    response = await api_.client.get("/admin/entitlements/kinds")
    assert response.status_code == 200, response.text
    return response.json()


async def test_the_kinds_are_exactly_those_the_write_accepts(api):
    listed = [k["kind"] for k in (await _kinds(api))["kinds"]]
    assert listed == list(KIND_ORDER)
    assert set(listed) == KINDS
    # The write checks the same definition: each listed kind is taken, any other refused by name.
    for kind in listed:
        response = await api.client.put(
            "/admin/entitlements/lists", json=_set(kind=kind, ids=["*"])
        )
        assert response.json().get("code") != "entitlements.unknown_kind", (kind, response.text)
    refused = await api.client.put("/admin/entitlements/lists", json=_set(kind="planets"))
    assert refused.json()["code"] == "entitlements.unknown_kind"


async def test_the_kinds_are_read_from_the_definition_not_a_copy(api, monkeypatch):
    monkeypatch.setattr(entitlements_router, "KIND_ORDER", ("domains", "tables"))
    assert [k["kind"] for k in (await _kinds(api))["kinds"]] == ["domains", "tables"]


async def test_each_kind_says_whether_an_expiry_ends_it_and_how_deep_its_tree_goes(api):
    kinds = {k["kind"]: k for k in (await _kinds(api))["kinds"]}
    assert {k for k, v in kinds.items() if v["semantic"]} == SEMANTIC_LISTS
    assert {k: v["tree_depth"] for k, v in kinds.items() if v["tree_depth"] is not None} == {
        "domains": 1,
        "tables": 2,
        "columns": 3,
    }


async def test_the_names_tree_is_the_models_domains_tables_and_columns(api):
    names = (await _kinds(api))["names"]

    def ids(nodes):
        return [(n["id"], ids(n["children"])) for n in nodes]

    assert ids(names) == [
        (
            "finance",
            [("finance.revenue", [("finance.revenue.amount", []), ("finance.revenue.id", [])])],
        ),
        ("hr", [("hr.secret", [("hr.secret.id", [])])]),
        (
            "sales",
            [
                ("sales.customers", [("sales.customers.id", [])]),
                (
                    "sales.orders",
                    [("sales.orders.customer_id", []), ("sales.orders.id", [])],
                ),
            ],
        ),
    ]
    assert names[0]["children"][0]["label"] == "revenue"


async def test_before_a_model_is_built_there_are_no_names(api):
    api.state.names_table = None
    assert (await _kinds(api))["names"] is None


async def test_a_caller_without_assign_entitlements_is_refused_the_kinds(api):
    api.caller["identity"] = _identity(role="people")
    response = await api.client.get("/admin/entitlements/kinds")
    assert response.status_code == 403
    assert response.json()["code"] == "auth.missing_capability"
