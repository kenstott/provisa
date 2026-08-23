# Copyright (c) 2026 Kenneth Stott
# Canary: 3d97b1e4-52f8-4c6a-8e01-b6a4d29f7c53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1269: auto-join grants a matching member-less identity org membership + the default role.

An org flagged ``auto_join`` with a ``auto_join_role`` admits any authenticated identity whose
email matches its ``email_rule`` on their FIRST request — no invite. The middleware grants
admin-plane membership + the tenant-plane role assignment, and the same request already resolves
active_org_id + mirrored roles. An auto-join org whose email rule excludes the identity grants
nothing. Same sync-DDL / async-request split as test_redeem_invite.
"""

from __future__ import annotations

import os

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, select, text

from provisa.api.auth_router import router as auth_router
from provisa.auth.middleware import AuthMiddleware
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import orgs, user_org_memberships
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles, user_role_assignments
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1269_admin"
_TENANT_SCHEMA = "test_req1269_tenant"


def _seed_org(**overrides):
    values = {"created_by": "super"}
    values.update(overrides)
    return insert(orgs).values(**values)


def _prepare_sync(*, org_rows: list[dict]):
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        for values in org_rows:
            conn.execute(_seed_org(**values))

        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments])
        conn.execute(insert(roles).values(id="analyst"))
    return engine


def _planes(monkeypatch, *org_rows: dict):
    try:
        sync_engine = _prepare_sync(org_rows=list(org_rows))
    except Exception as exc:  # noqa: BLE001 — the suite provisions this PG; a miss is a config fault
        pytest.skip(f"live Postgres not reachable at {_SYNC_URL}: {exc}")

    admin_db = Database(create_engine_from_url(_ASYNC_URL), name="admin", search_path=_ADMIN_SCHEMA)
    tenant_db = Database(
        create_engine_from_url(_ASYNC_URL), name="tenant", search_path=_TENANT_SCHEMA
    )

    from provisa.api.app import state as app_state

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "tenant_db", tenant_db, raising=False)

    # REQ-1269: the middleware binds the auto-join org's runtime (ensure_org_runtime) to grant the
    # tenant-plane role in that org's schema — here the tenant schema IS the org's schema, so
    # resolve the runtime to a stub carrying tenant_db (same seam as test_redeem_invite).
    from types import SimpleNamespace

    async def _org_runtime(_org_id: str):
        return SimpleNamespace(tenant_db=tenant_db)

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _org_runtime, raising=False)
    return admin_db, tenant_db, sync_engine


def _cleanup(sync_engine):
    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


@pytest.fixture
def matching_planes(monkeypatch):
    # alice's provider email is alice@example.com — the rule admits her.
    planes = _planes(
        monkeypatch,
        {
            "id": "acme",
            "name": "Acme",
            "email_rule": r"@example\.com$",
            "auto_join": True,
            "auto_join_role": "analyst",
        },
    )
    yield planes
    _cleanup(planes[2])


@pytest.fixture
def excluding_planes(monkeypatch):
    # Same auto-join flag, but the rule excludes alice@example.com.
    planes = _planes(
        monkeypatch,
        {
            "id": "acme",
            "name": "Acme",
            "email_rule": r"@acme\.com$",
            "auto_join": True,
            "auto_join_role": "analyst",
        },
    )
    yield planes
    _cleanup(planes[2])


def _make_app(admin_db: Database, tenant_db: Database) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseLikeProvider({"tok-alice": "alice"}),
        admin_pool=admin_db,
        db_pool=tenant_db,
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        default_org_id="root",
    )
    app.include_router(auth_router)

    @app.get("/whoami")
    async def _whoami(request: Request):  # noqa: RUF029 — Starlette route must be async
        return {
            "roles": request.state.identity.roles,
            "active_org_id": request.state.active_org_id,
        }

    return app


def _q(sync_engine, schema, stmt):
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema}"))
        return conn.execute(stmt).fetchall()


def test_auto_join_grants_membership_role_and_resolves_org(matching_planes):
    admin_db, tenant_db, sync_engine = matching_planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        # A plain first request — no invite, no explicit join call.
        resp = client.get("/whoami", headers={"Authorization": "Bearer tok-alice"})
    assert resp.status_code == 200, resp.text
    # The SAME request already resolves the auto-joined org + mirrored role.
    assert resp.json() == {"roles": ["analyst"], "active_org_id": "acme"}

    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )
    assert [r[0] for r in membership] == ["acme"]

    assignment = _q(
        sync_engine,
        _TENANT_SCHEMA,
        select(user_role_assignments.c.role_id, user_role_assignments.c.domain_id).where(
            user_role_assignments.c.user_id == "alice"
        ),
    )
    assert [(r[0], r[1]) for r in assignment] == [("analyst", "*")]


def test_auto_join_idempotent_across_requests(matching_planes):
    admin_db, tenant_db, sync_engine = matching_planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        first = client.get("/whoami", headers={"Authorization": "Bearer tok-alice"})
        second = client.get("/whoami", headers={"Authorization": "Bearer tok-alice"})
    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json() == {"roles": ["analyst"], "active_org_id": "acme"}
    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )
    assert [r[0] for r in membership] == ["acme"]  # one row, not two


def test_auto_join_skipped_when_email_rule_excludes(excluding_planes):
    admin_db, tenant_db, sync_engine = excluding_planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        # /auth/me is platform-plane, so a member-less identity gets 200 with no org.
        resp = client.get("/auth/me", headers={"Authorization": "Bearer tok-alice"})
    assert resp.status_code == 200, resp.text
    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )
    assert membership == []
    assignment = _q(
        sync_engine,
        _TENANT_SCHEMA,
        select(user_role_assignments.c.role_id).where(user_role_assignments.c.user_id == "alice"),
    )
    assert assignment == []


# REQ-1568: two orgs claiming one address. Neither is joined at sign-in — the person is shown both
# and picks, or turns both down and goes on to create their own org.


@pytest.fixture
def contested_planes(monkeypatch):
    # Both rules admit alice@example.com. Distinct rules, so REQ-1567's uniqueness gate is silent:
    # the collision is in what they MATCH, which no rule comparison can see.
    planes = _planes(
        monkeypatch,
        {
            "id": "acme",
            "name": "Acme",
            "email_rule": r"@example\.com$",
            "auto_join": True,
            "auto_join_role": "analyst",
        },
        {
            "id": "globex",
            "name": "Globex",
            "email_rule": r"example\.com$",
            "auto_join": True,
            "auto_join_role": "analyst",
        },
    )
    yield planes
    _cleanup(planes[2])


def test_two_claims_join_nothing_and_are_offered_as_a_choice(contested_planes):
    admin_db, tenant_db, sync_engine = contested_planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        me = client.get("/auth/me", headers={"Authorization": "Bearer tok-alice"})
        offers = client.get("/auth/auto-join-offers", headers={"Authorization": "Bearer tok-alice"})
    assert me.status_code == 200, me.text
    assert offers.status_code == 200, offers.text
    assert offers.json() == {
        "offers": [
            {"org_id": "acme", "org_name": "Acme", "role_id": "analyst"},
            {"org_id": "globex", "org_name": "Globex", "role_id": "analyst"},
        ]
    }
    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )
    assert membership == []


def test_the_org_the_user_picks_is_the_only_one_joined(contested_planes):
    admin_db, tenant_db, sync_engine = contested_planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        accepted = client.post(
            "/auth/auto-join",
            json={"org_id": "globex"},
            headers={"Authorization": "Bearer tok-alice"},
        )
        # The next sign-in must not re-offer the one they passed over.
        again = client.get("/auth/auto-join-offers", headers={"Authorization": "Bearer tok-alice"})
    assert accepted.status_code == 200, accepted.text
    assert accepted.json() == {"org_id": "globex", "role_id": "analyst"}
    assert again.json() == {"offers": []}

    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )
    assert [r[0] for r in membership] == ["globex"]
    assignment = _q(
        sync_engine,
        _TENANT_SCHEMA,
        select(user_role_assignments.c.role_id).where(user_role_assignments.c.user_id == "alice"),
    )
    assert [r[0] for r in assignment] == ["analyst"]


def test_an_org_that_never_offered_cannot_be_joined_by_naming_it(contested_planes):
    admin_db, tenant_db, sync_engine = contested_planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = client.post(
            "/auth/auto-join",
            json={"org_id": "root"},
            headers={"Authorization": "Bearer tok-alice"},
        )
    assert resp.status_code == 404, resp.text
    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )
    assert membership == []


def test_declining_every_claim_leaves_the_user_free_to_create_their_own(contested_planes):
    admin_db, tenant_db, sync_engine = contested_planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        declined = client.post(
            "/auth/auto-join/decline", headers={"Authorization": "Bearer tok-alice"}
        )
        again = client.get("/auth/auto-join-offers", headers={"Authorization": "Bearer tok-alice"})
    assert declined.status_code == 200, declined.text
    assert declined.json() == {"declined": ["acme", "globex"]}
    assert again.json() == {"offers": []}
    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )
    assert membership == []
