# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2a91c7-83fd-4b60-9a17-2c6f4d8b0e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1290/REQ-1296: what the first login actually leaves the platform admin holding.

The reported failure: on a freshly wiped multitenant deployment the first Google sign-in claimed
the platform-admin slot (``superadmin_bootstrap`` held the uid, ``/auth/bootstrap-status`` flipped
to claimed) and yet the app rendered "No roles configured" / "You do not have permission to view
this page", with the profile modal showing no roles and no capabilities.

So the claim is not the thing under test — the REQUEST AFTER the claim is. These tests walk the
real sequence a browser walks: status → claim → ``/auth/me`` → a role-carrying data request,
through ``AuthMiddleware`` with ``bootstrap_superadmin`` and ``multitenancy`` on, exactly as the
deployed config has them (``/app/config/provisa-install.yaml``: ``bootstrap_superadmin: true``).

DDL and seeding run on a synchronous psycopg2 engine so the async control-plane engines are only
ever driven inside the TestClient's event loop — same reason as test_redeem_invite.
"""

from __future__ import annotations

import os

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from provisa.auth.middleware import AuthMiddleware
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles, user_role_assignments
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1296_admin"
_TENANT_SCHEMA = "test_req1296_tenant"
_ORG = "default"


def _prepare_sync():
    """A wiped control plane: the bootstrap org registered, no memberships, an unclaimed slot.

    The org row is present because startup seeds it (``init_registry_schema``) before any request
    arrives — the schema exists, it simply has nobody in it. That is precisely the state REQ-1296
    addresses: the deployment is built, and the first person through the door has to be seated in it.

    The tenant plane carries the seeded role catalog every org schema gets. ``platform_admin`` is
    present as a row here, which is what the deployment showed too — the question is never "does the
    role exist" but "is the caller assigned it".
    """
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(
            text("INSERT INTO orgs (id, name, seeded_demo) VALUES (:i, 'Enterprise', true)"),
            {"i": _ORG},
        )

        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments])
        for rid in ("platform_admin", "analyst", "org_admin"):
            conn.execute(text("INSERT INTO roles (id) VALUES (:i)"), {"i": rid})
    return engine


@pytest.fixture
def planes(monkeypatch):
    try:
        sync_engine = _prepare_sync()
    except Exception as exc:  # noqa: BLE001 — the suite provisions this PG; a miss is a config fault
        pytest.skip(f"live Postgres not reachable at {_SYNC_URL}: {exc}")

    admin_db = Database(create_engine_from_url(_ASYNC_URL), name="admin", search_path=_ADMIN_SCHEMA)
    tenant_db = Database(
        create_engine_from_url(_ASYNC_URL), name="tenant", search_path=_TENANT_SCHEMA
    )

    from provisa.api.app import state as app_state

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "tenant_db", tenant_db, raising=False)
    monkeypatch.setattr(app_state, "org_id", _ORG, raising=False)
    # /auth/bootstrap-status and /auth/claim-bootstrap read the SAME dict the middleware resolves
    # its own flag from, so the endpoint can never disagree with the grant it warns about.
    monkeypatch.setattr(
        app_state,
        "auth_config",
        {"provider": "firebase", "assignments_source": "provisa", "bootstrap_superadmin": True},
        raising=False,
    )

    yield admin_db, tenant_db

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


def _make_app(admin_db: Database, tenant_db: Database) -> FastAPI:
    from provisa.api.auth_router import router as auth_router

    app = FastAPI()
    app.include_router(auth_router)  # the router carries its own /auth prefix
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseLikeProvider({"tok-first": "uid-first", "tok-second": "uid-second"}),
        admin_pool=admin_db,
        db_pool=tenant_db,
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        default_org_id=_ORG,
        bootstrap_superadmin=True,
    )

    @app.get("/whoami")
    async def _whoami(request: Request):  # noqa: RUF029 — Starlette route must be async
        return {
            "role": request.state.role,
            "assigned": sorted({a.role_id for a in request.state.assignments}),
            "active_org_id": request.state.active_org_id,
        }

    return app


def _hdr(token: str, role: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {token}"}
    if role is not None:
        headers["X-Provisa-Role"] = role
    return headers


def test_first_login_claims_and_holds_platform_admin(planes):
    """status → claim → /auth/me, in the order the login page performs them.

    ``/auth/me`` is the ONLY thing the UI reads to decide which roles it may put in
    X-Provisa-Role. If it comes back with an empty assignment list the app has no role to select,
    which is the "No roles configured" screen.
    """
    admin_db, tenant_db = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        assert client.get("/auth/bootstrap-status").json() == {"unclaimed": True}

        claim = client.post("/auth/claim-bootstrap", headers=_hdr("tok-first"))
        assert claim.status_code == 200, claim.text
        assert claim.json() == {"claimed": True, "claimed_by": "uid-first", "org_id": _ORG}

        assert client.get("/auth/bootstrap-status").json() == {"unclaimed": False}

        me = client.get("/auth/me", headers=_hdr("tok-first"))
        assert me.status_code == 200, me.text
        body = me.json()

    assert body["dev_mode"] is False
    assert body["assignments"] == [{"role_id": "platform_admin", "domain_id": "*"}]
    assert body["active_org_id"] == _ORG
    # REQ-1296: the claimant is seated in the bootstrap org. Landing with no membership is what left
    # the first admin staring at an empty deployment they could not query.
    assert body["org_memberships"] == [{"org_id": _ORG, "org_name": "Enterprise"}]


def test_claim_seats_the_admin_in_both_planes(planes):
    """REQ-1296: the claim writes rows, not just an in-request grant.

    The middleware can hand the claimant ``platform_admin`` for the duration of one request without
    anything being persisted — and that is exactly what produced "No roles configured" on the next
    request. So the assertions are against the tables: a membership row in the admin plane and an
    assignment row in the tenant plane, both surviving the client's shutdown.
    """
    admin_db, tenant_db = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        claim = client.post("/auth/claim-bootstrap", headers=_hdr("tok-first"))
    assert claim.status_code == 200, claim.text
    # The response names the org the claimant landed in, so the login page can send the next request
    # into a populated org instead of nowhere.
    assert claim.json() == {"claimed": True, "claimed_by": "uid-first", "org_id": _ORG}

    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    try:
        with engine.begin() as conn:
            memberships = conn.execute(
                text(
                    f"SELECT user_id, org_id FROM {_ADMIN_SCHEMA}.user_org_memberships"
                    " ORDER BY user_id"
                )
            ).all()
            assignments = conn.execute(
                text(
                    f"SELECT user_id, role_id, domain_id FROM {_TENANT_SCHEMA}.user_role_assignments"
                    " ORDER BY user_id"
                )
            ).all()
    finally:
        engine.dispose()

    assert memberships == [("uid-first", _ORG)]
    assert assignments == [("uid-first", "platform_admin", "*")]


def test_platform_admin_may_request_the_platform_admin_role(planes):
    """The role /auth/me offered is the role the server honors — no 403 on the next request."""
    admin_db, tenant_db = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        client.post("/auth/claim-bootstrap", headers=_hdr("tok-first"))
        resp = client.get("/whoami", headers=_hdr("tok-first", "platform_admin"))
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "role": "platform_admin",
        "assigned": ["platform_admin"],
        "active_org_id": _ORG,
    }


def test_second_user_is_admitted_without_a_grant(planes):
    """Multitenant bootstrap: a later identity is NOT the platform admin and NOT denied.

    They authenticate with no assignments and no org, which is the state onboarding routes on.
    """
    admin_db, tenant_db = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        client.post("/auth/claim-bootstrap", headers=_hdr("tok-first"))
        me = client.get("/auth/me", headers=_hdr("tok-second"))
    assert me.status_code == 200, me.text
    assert me.json()["assignments"] == []
    assert me.json()["org_memberships"] == []
