# Copyright (c) 2026 Kenneth Stott
# Canary: c2a7e914-5b3d-4f06-8e1a-d94b7f20c6e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219, ONE API, THREE WAYS TO USE IT: the picker finds people through one search by name or
email, over local accounts and known members of the org, returning the principal the sign-in token
carries; a person not yet seen is granted by typed email, shown as pending, and the grant attaches
when a sign-in token's email matches. Where the identity provider sends a list, that list wins."""

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import select

from provisa.api.admin.entitlements_router import router
from provisa.api.errors import ApiError
from provisa.auth import middleware
from provisa.auth.models import AuthIdentity
from provisa.core.schema_admin import local_users, orgs, user_org_memberships, user_profiles
from provisa.core.schema_org import (
    admin_audit_log,
    entitlement_pending,
    entitlements,
    user_directory,
)
from provisa.entitlements import pending, runtime, store
from tests.pg_control_plane import ORG
from tests.unit.test_entitlement_lists import _ctx
from tests.unit.test_entitlements_api import _as, _identity, _set, _state

pytestmark = pytest.mark.asyncio

OTHER_ORG = "elsewhere"


async def _seed(platform, model_db) -> None:
    """Two orgs' people: ours (a member with a profile, a local account that is a member, a local
    account that belongs to no org, one who has signed in here) and the other org's."""
    async with platform.acquire() as conn:
        for org_id in (ORG, OTHER_ORG):
            known = await conn.execute_core(select(orgs.c.id).where(orgs.c.id == org_id))
            if known.fetchone() is None:
                await conn.execute_core(orgs.insert().values(id=org_id, name=org_id))
        await conn.execute_core_many(
            user_profiles.insert(),
            [
                {
                    "user_id": "u-alice",
                    "email": "Alice.Smith@acme.test",
                    "display_name": "Alice Smith",
                },
                {
                    "user_id": "u-mallory",
                    "email": "mallory@rival.test",
                    "display_name": "Alice Rival",
                },
            ],
        )
        await conn.execute_core_many(
            user_org_memberships.insert(),
            [
                {"user_id": "u-alice", "org_id": ORG},
                {"user_id": "l-bob", "org_id": ORG},
                {"user_id": "u-mallory", "org_id": OTHER_ORG},
                {"user_id": "l-eve", "org_id": OTHER_ORG},
            ],
        )
        await conn.execute_core_many(
            local_users.insert(),
            [
                {
                    "id": "l-bob",
                    "username": "bob",
                    "password_hash": "x",
                    "email": "bob@acme.test",
                    "display_name": "Bob Local",
                    "roles": [],
                    "attributes": {},
                },
                {
                    "id": "l-eve",
                    "username": "eve",
                    "password_hash": "x",
                    "email": "eve@rival.test",
                    "display_name": "Alice Eve",
                    "roles": [],
                    "attributes": {},
                },
            ],
        )
    async with model_db.acquire() as conn:
        await conn.execute_core(
            user_directory.insert().values(
                user_id="kc-carol", email="carol@acme.test", display_name="Carol Signed-In"
            )
        )


@pytest.fixture
async def api(control_plane, monkeypatch):
    state = _state(control_plane.org())
    state.admin_db = control_plane.platform
    state.multitenancy = True
    await _seed(state.admin_db, state.model_db)
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr(runtime, "_role_context", lambda _state, _role: _ctx())
    monkeypatch.setattr(middleware, "_recorded_idp_lists", {})
    monkeypatch.setattr(middleware, "_attached_pending", set())
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


def _by_email(email: str, **over) -> dict:
    """A write naming ``email`` instead of a principal."""
    body = _set(**over)
    del body["principal"]
    return {**body, "email": email}


async def _people(api_, q: str) -> list[dict]:
    response = await api_.client.get("/admin/entitlements/people", params={"q": q})
    assert response.status_code == 200, response.text
    return response.json()


async def _sign_in(api_, user: str, email: str, claims: dict | None = None) -> None:
    """The sign-in hook, as the auth middleware runs it for a token carrying ``email``."""
    identity = AuthIdentity(
        user_id=user, email=email, display_name=None, roles=["viewer"], raw_claims=claims or {}
    )
    await middleware._record_idp_entitlements(api_.db, identity)  # pyright: ignore[reportPrivateUsage]


async def _rows(api_, table, **where) -> list:
    async with api_.db.acquire() as conn:
        query = select(table)
        for column, value in where.items():
            query = query.where(table.c[column] == value)
        return (await conn.execute_core(query.order_by(table.c.id))).fetchall()


# -- search ------------------------------------------------------------------------------------


async def test_search_by_name_returns_the_orgs_principals_only(api):
    found = await _people(api, "alice")
    assert found == [
        {"principal": "u-alice", "name": "Alice Smith", "email": "Alice.Smith@acme.test"}
    ]


async def test_search_by_email_returns_the_principal_with_name_and_email(api):
    assert await _people(api, "BOB@acme") == [
        {"principal": "l-bob", "name": "Bob Local", "email": "bob@acme.test"}
    ]


async def test_search_reaches_local_accounts_known_members_and_who_signed_in(api):
    found = {p["principal"] for p in await _people(api, "acme.test")}
    assert found == {"u-alice", "l-bob", "kc-carol"}


async def test_another_orgs_people_are_never_found(api):
    assert await _people(api, "rival") == []


async def test_without_multitenancy_every_local_account_is_the_one_orgs(api):
    api.state.multitenancy = False
    found = {p["principal"] for p in await _people(api, "alice")}
    assert found == {"u-alice", "l-eve"}


async def test_a_pattern_character_in_the_search_is_matched_literally(api):
    assert await _people(api, "%") == []


async def test_search_needs_assign_entitlements(api):
    api.caller["identity"] = _identity("someone", "nobody")
    response = await api.client.get("/admin/entitlements/people", params={"q": "alice"})
    assert response.status_code == 403


# -- a write by email ------------------------------------------------------------------------


async def test_an_email_of_a_known_person_is_that_persons_write(api):
    response = await api.client.put(
        "/admin/entitlements/lists", json=_by_email("alice.smith@ACME.test")
    )
    assert response.status_code == 200, response.text
    assert _as(api, "u-alice") is not None
    assert await _rows(api, entitlement_pending) == []


async def test_an_email_matching_no_one_is_stored_as_a_pending_grant(api):
    response = await api.client.put("/admin/entitlements/lists", json=_by_email(" Dave@New.test "))
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["pending"] is True
    assert body["email"] == "dave@new.test"
    assert await _rows(api, entitlements) == []
    listed = (await api.client.get("/admin/entitlements/pending")).json()
    assert [(g["email"], g["role_id"], g["kind"], g["identifiers"]) for g in listed] == [
        ("dave@new.test", "viewer", "tables", ["finance.revenue"])
    ]
    audit = await _rows(api, admin_audit_log, action="entitlements.set_pending")
    assert [(a.actor_id, a.subject_id) for a in audit] == [("admin", "dave@new.test")]


async def test_a_pending_grant_is_checked_as_every_write_is(api):
    response = await api.client.put(
        "/admin/entitlements/lists", json=_by_email("dave@new.test", reason=" ")
    )
    assert response.status_code == 422
    assert response.json()["code"] == "entitlements.reason_required"


async def test_a_write_names_a_principal_or_an_email_not_both(api):
    neither = _set()
    del neither["principal"]
    for body in (_set(email="dave@new.test"), neither):
        response = await api.client.put("/admin/entitlements/lists", json=body)
        assert response.status_code == 422
        assert response.json()["code"] == "entitlements.principal_or_email"


async def test_a_pending_grant_is_scoped_by_the_roles_list(api):
    api.caller["identity"] = _identity("someone", "nobody")
    response = await api.client.put("/admin/entitlements/lists", json=_by_email("dave@new.test"))
    assert response.status_code == 403


async def test_ending_a_pending_grant_leaves_nothing_to_attach(api):
    await api.client.put("/admin/entitlements/lists", json=_by_email("dave@new.test"))
    response = await api.client.post(
        "/admin/entitlements/lists/end",
        json={
            "email": "dave@new.test",
            "role_id": "viewer",
            "kind": "tables",
            "reason": "not joining",
        },
    )
    assert response.status_code == 200, response.text
    assert (await api.client.get("/admin/entitlements/pending")).json() == []
    await _sign_in(api, "kc-dave", "dave@new.test")
    assert _as(api, "kc-dave") is None


# -- attach on sign-in -----------------------------------------------------------------------


async def test_a_pending_grant_attaches_on_that_emails_first_sign_in(api):
    await api.client.put("/admin/entitlements/lists", json=_by_email("dave@new.test"))
    assert _as(api, "kc-dave") is None
    await _sign_in(api, "kc-dave", "DAVE@new.test")
    mask = _as(api, "kc-dave")
    assert mask is not None
    held = api.state.held_entitlements["kc-dave"].lists
    assert [(h.role_id, h.kind, h.identifiers, h.set_by) for h in held] == [
        ("viewer", "tables", ("finance.revenue",), "admin")
    ]
    assert (await api.client.get("/admin/entitlements/pending")).json() == []
    last = (await _rows(api, entitlement_pending))[-1]
    assert (last.outcome, last.principal) == (pending.ATTACHED, "kc-dave")
    audit = await _rows(api, admin_audit_log, action="entitlements.pending_attached")
    assert [(a.actor_id, a.subject_id) for a in audit] == [("admin", "kc-dave")]


async def test_an_attached_grant_is_the_write_apis_write(api):
    """The same rows a write through the API makes, its expiry included."""
    from datetime import UTC, datetime, timedelta

    expires = (datetime.now(UTC) + timedelta(days=3)).isoformat()
    await api.client.put(
        "/admin/entitlements/lists",
        json=_by_email("dave@new.test", expires_at=expires),
    )
    await _sign_in(api, "kc-dave", "dave@new.test")
    expiries = api.state.held_entitlements["kc-dave"].expiries
    assert [(e.role_id, e.list_kind, e.ends) for e in expiries] == [
        ("viewer", "tables", {"*": ("finance.revenue",)})
    ]


async def test_a_sign_in_with_another_email_attaches_nothing(api):
    await api.client.put("/admin/entitlements/lists", json=_by_email("dave@new.test"))
    await _sign_in(api, "kc-erin", "erin@new.test")
    assert _as(api, "kc-erin") is None
    assert len((await api.client.get("/admin/entitlements/pending")).json()) == 1


async def test_a_pending_grant_the_identity_provider_supersedes_is_refused(api):
    await api.client.put("/admin/entitlements/lists", json=_by_email("dave@new.test"))
    claims = {"groups": ["provisa:viewer:tables:sales.orders"]}
    await _sign_in(api, "kc-dave", "dave@new.test", claims)
    assert "kc-dave" not in api.state.held_entitlements
    last = (await _rows(api, entitlement_pending))[-1]
    assert (last.outcome, last.principal) == (pending.REFUSED, "kc-dave")
    audit = await _rows(api, admin_audit_log, action="entitlements.pending_refused")
    assert [(a.actor_id, a.subject_id) for a in audit] == [("admin", "kc-dave")]


async def test_a_refused_grant_stays_refused(api):
    """Not attached by a later sign-in, and a write through the API for that list is still
    refused by name."""
    await api.client.put("/admin/entitlements/lists", json=_by_email("dave@new.test"))
    claims = {"groups": ["provisa:viewer:tables:sales.orders"]}
    await _sign_in(api, "kc-dave", "dave@new.test", claims)
    middleware._attached_pending.clear()  # pyright: ignore[reportPrivateUsage]
    await _sign_in(api, "kc-dave", "dave@new.test", claims)
    assert "kc-dave" not in api.state.held_entitlements
    assert await _rows(api, entitlements, source="provisa") == []
    response = await api.client.put("/admin/entitlements/lists", json=_set(principal="kc-dave"))
    assert response.status_code == 409
    assert response.json()["code"] == "entitlements.idp_supersedes"


async def test_the_identity_provider_refuses_only_its_role_and_kind(api):
    await api.client.put("/admin/entitlements/lists", json=_by_email("dave@new.test"))
    await api.client.put(
        "/admin/entitlements/lists",
        json=_by_email("dave@new.test", kind="relationships", ids=()),
    )
    claims = {"groups": ["provisa:viewer:tables:sales.orders"]}
    await _sign_in(api, "kc-dave", "dave@new.test", claims)
    held = api.state.held_entitlements["kc-dave"].lists
    assert [(h.role_id, h.kind) for h in held] == [("viewer", "relationships")]


async def test_a_grant_attaches_once(api):
    await api.client.put("/admin/entitlements/lists", json=_by_email("dave@new.test"))
    await _sign_in(api, "kc-dave", "dave@new.test")
    middleware._attached_pending.clear()  # pyright: ignore[reportPrivateUsage]
    await _sign_in(api, "kc-dave", "dave@new.test")
    assert len(await _rows(api, entitlements, source="provisa")) == 1


async def test_attach_reads_the_store_not_this_process(api):
    """Through the store alone, as a sign-in on another worker would."""
    async with api.db.acquire() as conn:
        await pending.set_pending(
            conn,
            "dave@new.test",
            "viewer",
            "tables",
            ["finance.revenue"],
            expires_at=None,
            set_by="admin",
            reason="joining",
        )
        async with conn.transaction():
            outcomes = await pending.attach(conn, "kc-dave", "dave@new.test", {})
        held = await store.load_held(conn)
    assert [o for _g, o in outcomes] == [pending.ATTACHED]
    assert [h.identifiers for h in held["kc-dave"].lists] == [("finance.revenue",)]
