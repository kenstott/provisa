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
from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from provisa.api.admin import entitlements_router
from provisa.api.admin.entitlements_router import router
from provisa.api.errors import ApiError
from provisa.auth import middleware
from provisa.auth.models import AuthIdentity, RoleAssignment
from provisa.compiler.names_table import NameRow, build_names_table
from provisa.core.schema_org import roles as roles_table
from provisa.core.schema_org import user_idp_roles, user_role_assignments
from provisa.entitlements import roles, runtime, store
from provisa.entitlements.index import (
    KIND_ORDER,
    KINDS,
    SEMANTIC_LISTS,
    build_index,
    matches,
    names_tree,
)
from provisa.security.rights import Capability
from tests.pg_control_plane import ORG
from tests.unit.test_entitlement_lists import _ctx
from tests.unit.test_entitlements_api import ASSIGN, _identity, _set, _state

pytestmark = pytest.mark.asyncio

_FROM_PROVISA = {"provider": "oidc", "assignments_source": "provisa"}
_FROM_CLAIMS = {"provider": "oidc", "assignments_source": "claims"}


@pytest.fixture
async def api(control_plane, monkeypatch):
    state = _state(control_plane.org())
    state.auth_config = dict(_FROM_PROVISA)
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr(runtime, "_role_context", lambda _state, _role: _ctx())
    monkeypatch.setattr(middleware, "_recorded_idp_roles", {})
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


async def _sign_in(api_, principal: str, role_claims: list[str]) -> None:
    """The sign-in hook, as the auth middleware runs it for a token carrying ``role_claims``."""
    identity = AuthIdentity(
        user_id=principal, email=None, display_name=None, roles=role_claims, raw_claims={}
    )
    await middleware._record_idp_roles(api_.db, identity)  # pyright: ignore[reportPrivateUsage]


async def _mirrored(api_, principal: str) -> list[tuple[str, str]]:
    async with api_.db.acquire() as conn:
        rows = await conn.execute_core(
            select(user_idp_roles.c.role_id, user_idp_roles.c.domain_id)
            .where(user_idp_roles.c.user_id == principal)
            .order_by(user_idp_roles.c.role_id, user_idp_roles.c.domain_id)
        )
        return [(r.role_id, r.domain_id) for r in rows.fetchall()]


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


async def test_a_role_the_identity_provider_names_that_the_org_does_not_define_is_left_out(api):
    await _idp(api, "alice", {"ghost": {"tables": ()}, "analyst": {"tables": ()}})
    assert "ghost" not in api.state.roles
    assert await _roles(api, "alice") == [
        {"role_id": "analyst", "origins": ["idp"], "domains": []},
    ]
    # No list can be set on it: the effective view refuses it, so it is not offered.
    response = await api.client.get(
        "/admin/entitlements/effective", params={"principal": "alice", "role_id": "ghost"}
    )
    assert response.status_code == 404
    assert response.json()["code"] == "entitlements.role_not_found"


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


async def test_kinds_names_the_kind_that_scopes_each_capability(api):
    # REQ-2219, THE ENTITLEMENT MODEL: each capability is unscoped (null) or scoped to one kind.
    scopes = (await _kinds(api))["capability_scopes"]
    assert scopes["read"] == "columns"
    assert scopes["traverse"] == "relationships"
    assert scopes["execute"] == "commands"
    assert scopes["consume"] == "data_products"
    assert scopes[Capability.ASSIGN_ENTITLEMENTS.value] == "roles"
    assert scopes[Capability.SOURCE_REGISTRATION.value] == "sources"
    assert scopes[Capability.ENVIRONMENT_MANAGEMENT.value] == "environments"
    assert scopes[Capability.DATA_PRODUCT_RW.value] == "data_products"
    assert scopes[Capability.ORG_SETTINGS.value] is None
    assert {c.value for c in Capability} <= scopes.keys()


async def test_kinds_carries_the_expiring_soon_threshold(api, monkeypatch):
    # REQ-2077, REQ-2219: the one expiring-soon setting, for a grant and its entries.
    assert (await _kinds(api))["expiring_soon_days"] == 7
    monkeypatch.setenv("PROVISA_GRANT_NOTICE_DAYS", "3")
    assert (await _kinds(api))["expiring_soon_days"] == 3


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


def _mixed_case_names():
    def t(oid, name, dom):
        return NameRow(
            "table",
            str(oid),
            "sql",
            name,
            name,
            name,
            domain_id=dom,
            table_id=oid,
            scope=("sql", dom),
        )

    def c(tid, name, dom):
        return NameRow(
            "column",
            f"{tid}:{name}",
            "sql",
            name,
            name,
            name,
            domain_id=dom,
            table_id=tid,
            scope=("sql_column", str(tid)),
        )

    return build_names_table(
        [
            t(1, "Revenue", "Finance"),
            c(1, "AmountUSD", "Finance"),
            c(1, "id", "Finance"),
            t(2, "budget", "Finance"),
            t(3, "Orders", "sales"),
            c(3, "OrderID", "sales"),
        ]
    )


async def test_the_names_tree_shows_the_models_own_spelling():
    tree = names_tree(build_index(_mixed_case_names(), None))

    def spelled(nodes):
        return [(n.id, n.label, spelled(n.children)) for n in nodes]

    # Labels and identifiers as the model spells them, each level ordered ignoring case.
    assert spelled(tree) == [
        (
            "Finance",
            "Finance",
            [
                ("Finance.budget", "budget", []),
                (
                    "Finance.Revenue",
                    "Revenue",
                    [
                        ("Finance.Revenue.AmountUSD", "AmountUSD", []),
                        ("Finance.Revenue.id", "id", []),
                    ],
                ),
            ],
        ),
        ("sales", "sales", [("sales.Orders", "Orders", [("sales.Orders.OrderID", "OrderID", [])])]),
    ]


async def test_matching_ignores_the_case_the_tree_shows():
    index = build_index(_mixed_case_names(), None)
    amount = next(o for o in index.objects if o.oid == "1:AmountUSD")
    assert amount.spelled == ("Finance", "Revenue", "AmountUSD")
    assert amount.paths == (("finance", "revenue", "amountusd"),)
    for identifier in (
        "Finance.Revenue.AmountUSD",
        "finance.revenue.amountusd",
        "FINANCE.REVENUE",
        "fInAnCe",
        "*.revenue.AMOUNTusd",
    ):
        assert matches(identifier, amount.paths), identifier
    assert not matches("Finance.Revenue.id", amount.paths)


# -- IDENTITY PROVIDER ROLES ARE MIRRORED ------------------------------------------------------


async def test_a_sign_in_records_the_tokens_role_claims(api):
    await _sign_in(api, "alice", ["viewer", "analyst:finance"])
    assert await _mirrored(api, "alice") == [("analyst", "finance"), ("viewer", "*")]


async def test_a_later_sign_in_replaces_the_role_claims_of_the_one_before(api):
    await _sign_in(api, "alice", ["viewer", "analyst:finance"])
    await _sign_in(api, "alice", ["analyst:sales"])
    assert await _mirrored(api, "alice") == [("analyst", "sales")]


async def test_a_sign_in_with_no_role_claims_mirrors_none(api):
    await _sign_in(api, "alice", ["viewer"])
    await _sign_in(api, "alice", [])
    assert await _mirrored(api, "alice") == []


async def test_a_role_claim_sent_twice_is_mirrored_once(api):
    await _sign_in(api, "alice", ["viewer", "viewer", "viewer:*"])
    assert await _mirrored(api, "alice") == [("viewer", "*")]


async def test_the_mirror_is_rewritten_only_when_the_claims_changed(api):
    viewer = [RoleAssignment(role_id="viewer", domain_id="*")]
    async with api.db.acquire() as conn:
        assert await roles.mirror_idp_roles(conn, "alice", viewer)
        assert not await roles.mirror_idp_roles(conn, "alice", viewer)
        assert await roles.mirror_idp_roles(conn, "alice", [])


async def test_mirrored_role_claims_are_listed_with_the_identity_provider_as_origin(api):
    api.state.auth_config = dict(_FROM_CLAIMS)
    await _sign_in(api, "alice", ["viewer:sales", "viewer:finance", "analyst"])
    assert await _roles(api, "alice") == [
        {"role_id": "analyst", "origins": ["idp"], "domains": ["*"]},
        {"role_id": "viewer", "origins": ["idp"], "domains": ["finance", "sales"]},
    ]


async def test_a_role_claim_and_an_idp_list_for_one_role_list_it_once(api):
    api.state.auth_config = dict(_FROM_CLAIMS)
    await _sign_in(api, "alice", ["viewer:sales"])
    await _idp(api, "alice", {"viewer": {"tables": ()}})
    assert await _roles(api, "alice") == [
        {"role_id": "viewer", "origins": ["idp"], "domains": ["sales"]},
    ]


async def test_a_claimed_role_the_org_does_not_define_is_left_out(api):
    api.state.auth_config = dict(_FROM_CLAIMS)
    await _sign_in(api, "alice", ["ghost", "viewer"])
    assert "ghost" not in api.state.roles
    assert await _roles(api, "alice") == [
        {"role_id": "viewer", "origins": ["idp"], "domains": ["*"]},
    ]


async def test_another_persons_role_claims_are_not_listed(api):
    api.state.auth_config = dict(_FROM_CLAIMS)
    await _sign_in(api, "bob", ["viewer"])
    assert await _roles(api, "alice") == []


async def test_role_claims_are_not_listed_where_roles_come_from_provisa(api):
    await _sign_in(api, "alice", ["analyst"])
    await _assign(api, "alice", "viewer")
    assert await _roles(api, "alice") == [
        {"role_id": "viewer", "origins": ["provisa"], "domains": ["*"]},
    ]
