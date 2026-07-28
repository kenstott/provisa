# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7c19e5-42bd-4f08-9c61-5d8e0b4a7f92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1295: the role an org creator may request, proven through the real request path.

A self-service org creator is granted exactly one role in their org's own schema:
``org_admin`` (REQ-1266). Every data request the UI issues carries an ``X-Provisa-Role``
header, and the server honors that header only for roles the caller is actually assigned
(REQ-273, provisa/auth/middleware.py). So a client that asks for ``admin`` — a role the
creator does not hold — gets 403 on every request, and the app renders as an org with no
schema and no tables. That is the failure this file pins down.

Unlike a resolver-level test, these requests go through AuthMiddleware: bearer token →
platform-plane membership → org-scoped ``user_role_assignments`` read in ``org_<id>``'s
schema → role resolution. Nothing about the identity is fabricated; the assignment row in
the tenant plane is the only thing that decides the answer.

DDL and seeding run on a synchronous psycopg2 engine so the async control-plane engines are
only ever driven inside the TestClient's event loop — same reason as test_redeem_invite.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text

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

_ADMIN_SCHEMA = "test_req1295_admin"
_TENANT_SCHEMA = "test_req1295_tenant"
_ORG = "kstott"


def _prepare_sync():
    """Seed the state a completed self-service org creation leaves behind."""
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id=_ORG, name="Kstott", created_by="alice"))
        conn.execute(insert(user_org_memberships).values(user_id="alice", org_id=_ORG))

        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments])
        # The org's schema carries the same seeded role catalog every org gets: `admin` exists as a
        # row, which is precisely why "is this role defined?" is the wrong question — the creator is
        # assigned org_admin and only org_admin.
        conn.execute(insert(roles).values(id="admin"))
        conn.execute(insert(roles).values(id="analyst"))
        conn.execute(insert(roles).values(id="org_admin"))
        conn.execute(
            insert(user_role_assignments).values(
                user_id="alice", role_id="org_admin", domain_id="*"
            )
        )
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

    # REQ-1266: the middleware binds the ACTIVE org's data-plane runtime before reading assignments,
    # so the read lands in that org's schema. Here the tenant schema IS the org's schema; resolve the
    # runtime to a stub carrying it rather than building a full per-org runtime (covered elsewhere).
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

    @app.get("/whoami")
    async def _whoami(request: Request):  # noqa: RUF029 — Starlette route must be async
        return {
            "role": request.state.role,
            "roles": request.state.identity.roles,
            "assigned": sorted({a.role_id for a in request.state.assignments}),
            "active_org_id": request.state.active_org_id,
        }

    return app


def _get(client, role_header: str | None):
    headers = {"Authorization": "Bearer tok-alice"}
    if role_header is not None:
        headers["X-Provisa-Role"] = role_header
    return client.get("/whoami", headers=headers)


def test_creator_holds_org_admin_only(planes):
    """The org creator's assignment set, read through the middleware, is exactly {org_admin}."""
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _get(client, None)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["assigned"] == ["org_admin"]
    assert body["roles"] == ["org_admin"]
    assert body["active_org_id"] == _ORG


def test_requesting_org_admin_resolves_and_serves(planes):
    """The role the creator actually holds is honored — the request is served, not 403'd."""
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _get(client, "org_admin")
    assert resp.status_code == 200, resp.text
    assert resp.json()["role"] == "org_admin"


def test_requesting_admin_is_rejected(planes):
    """Asking for a role the creator does not hold is 403 — the exact on-screen failure.

    This is the server contract the UI must respect: it may only put an ASSIGNED role in
    X-Provisa-Role. A client that defaults to `admin` when its role list comes back empty
    turns one failed roles query into a permanently unusable session.
    """
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _get(client, "admin")
    assert resp.status_code == 403
    assert resp.json()["detail"] == "Role 'admin' is not assigned to this user"
