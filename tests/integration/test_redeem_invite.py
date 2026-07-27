# Copyright (c) 2026 Kenneth Stott
# Canary: 5f1c9a72-8b3e-4d16-9c04-7ae2f0d3b681
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-516: bearer-user invite redemption end-to-end against the live control planes.

/auth/redeem-invite is the redemption path for an already-authenticated bearer identity
(Firebase/OIDC) that arrives with an ?invite= link — /register only covers the basic
provider. Proven here against real Postgres control planes (the redeem endpoint compares
invite.expires_at against a tz-aware ``now``; only a TIMESTAMPTZ store round-trips that
faithfully). Split plane: membership lands in the platform plane (admin_db), the invite's
role assignment in the tenant plane (tenant_db), and the invite is burned. Idempotent per
(user, org): a second redemption of the burned token is rejected.

DDL, seeding, and row verification run on a SYNCHRONOUS psycopg2 engine so the async
control-plane engines are only ever driven inside the TestClient's event loop (the async
Database connects lazily on first request there — driving it from the pytest-asyncio loop
would bind its asyncpg pool to a different loop). This mirrors _make_admin_db in
test_auth_integration.
"""

from __future__ import annotations

import datetime
import os
from datetime import timezone

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, select, text

from provisa.api.auth_router import router as auth_router
from provisa.auth.middleware import AuthMiddleware
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import org_invites, orgs, user_org_memberships, user_profiles
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles, user_role_assignments
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req516_admin"
_TENANT_SCHEMA = "test_req516_tenant"
_TOKEN = "invite-tok-abc123"


def _prepare_sync():
    """Create both schemas, their tables, and seed the org + invite + role — synchronously."""
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    expires = datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id="acme", name="Acme", created_by="super"))
        conn.execute(
            insert(org_invites).values(
                token=_TOKEN,
                org_id="acme",
                role_id="org_admin",
                created_by="super",
                expires_at=expires,
            )
        )

        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments])
        conn.execute(insert(roles).values(id="org_admin"))
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

    # REQ-1266: redeem-invite/register bind the INVITED org's data-plane runtime (ensure_org_runtime)
    # to obtain a tenant_db search_path-scoped to that org's schema, then grant the role there. The
    # full runtime build is an integration concern the invited org already exercises at provisioning;
    # here the tenant schema IS the org's schema, so resolve the runtime to a stub carrying tenant_db.
    from types import SimpleNamespace

    async def _org_runtime(_org_id: str):
        return SimpleNamespace(tenant_db=tenant_db)

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _org_runtime, raising=False)

    yield admin_db, tenant_db, sync_engine

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


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


def test_redeem_grants_membership_role_and_burns_invite(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = client.post(
            "/auth/redeem-invite",
            json={"token": _TOKEN},
            headers={"Authorization": "Bearer tok-alice"},
        )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"user_id": "alice", "org_id": "acme", "role_id": "org_admin"}

    # Platform plane: membership created.
    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )
    assert [r[0] for r in membership] == ["acme"]

    # Platform plane: invite burned.
    invite = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(org_invites.c.used_at, org_invites.c.used_by).where(org_invites.c.token == _TOKEN),
    )[0]
    assert invite[0] is not None
    assert invite[1] == "alice"

    # Tenant plane: org-confined org_admin assignment, wildcard domain.
    assignment = _q(
        sync_engine,
        _TENANT_SCHEMA,
        select(user_role_assignments.c.role_id, user_role_assignments.c.domain_id).where(
            user_role_assignments.c.user_id == "alice"
        ),
    )
    assert [(r[0], r[1]) for r in assignment] == [("org_admin", "*")]


def test_redeemed_identity_mirrors_org_admin_not_superadmin(planes):
    # After redemption a later request surfaces org_admin via identity.roles (mirror), scoped to
    # acme — NOT the platform "admin"/"superadmin" bypass.
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        client.post(
            "/auth/redeem-invite",
            json={"token": _TOKEN},
            headers={"Authorization": "Bearer tok-alice"},
        )
        resp = client.get("/whoami", headers={"Authorization": "Bearer tok-alice"})
    assert resp.status_code == 200
    assert resp.json() == {"roles": ["org_admin"], "active_org_id": "acme"}


def test_second_redemption_of_burned_token_rejected(planes):
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        first = client.post(
            "/auth/redeem-invite",
            json={"token": _TOKEN},
            headers={"Authorization": "Bearer tok-alice"},
        )
        assert first.status_code == 200
        second = client.post(
            "/auth/redeem-invite",
            json={"token": _TOKEN},
            headers={"Authorization": "Bearer tok-alice"},
        )
    assert second.status_code == 400
    assert second.json()["detail"] == "Invalid or expired invite token"


def test_redeem_requires_authentication(planes):
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = client.post("/auth/redeem-invite", json={"token": _TOKEN})
    assert resp.status_code == 401


def test_profile_update_sets_given_family_name(planes):
    # REQ-1266: user supplies first/last (Firebase/OIDC tokens carry no split). PATCH writes them
    # to user_profiles (platform plane); the IdP mirror (_upsert_profile) never overwrites them.
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = client.patch(
            "/auth/profile",
            json={"given_name": "  Alice  ", "family_name": ""},
            headers={"Authorization": "Bearer tok-alice"},
        )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"user_id": "alice", "given_name": "Alice", "family_name": None}

    prof = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_profiles.c.given_name, user_profiles.c.family_name).where(
            user_profiles.c.user_id == "alice"
        ),
    )[0]
    assert (prof[0], prof[1]) == ("Alice", None)


def test_profile_update_requires_authentication(planes):
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = client.patch("/auth/profile", json={"given_name": "X", "family_name": "Y"})
    assert resp.status_code == 401
