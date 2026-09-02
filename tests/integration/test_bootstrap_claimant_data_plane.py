# Copyright (c) 2026 Kenneth Stott
# Canary: 6b2e4f70-31a8-4c95-ae63-0d47f9c81b2a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1338: the bootstrap claimant can query the bootstrap org's data.

POST /auth/claim-bootstrap seats the claimant in the bootstrap org with BOTH platform_admin (the
control-plane grant that comes with the slot) and org_admin (the data-plane seat — REQ-1297,
_seat_claimant_in_root). The middleware then short-circuited that identity with a hard-coded
[platform_admin] and returned before ever reading those rows, so the acting role on every data
request was the control-plane role. REQ-1327 builds no data schema for a cross_org role, so
/data/graphql, JSON:API, REST and Flight all answered 400 "No schema available for role
'platform_admin'" — the deployment's own administrator could not read the demo data set.

Proven against the live Postgres control planes because the defect lives in the seam between what
the claim WRITES (two planes, two schemas) and what the middleware READS back on the next request.
Mirrors test_redeem_invite: DDL and row verification on a synchronous psycopg2 engine, the async
control-plane engines driven only inside the TestClient's event loop.
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
from provisa.core.schema_org import roles, user_directory, user_role_assignments
from provisa.security.rights import ORG_ADMIN_ROLE, PLATFORM_ADMIN_ROLE
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1338_admin"
_TENANT_SCHEMA = "test_req1338_tenant"
_ORG = "root"

# What schema.sql seeds, reduced to the two roles this scenario turns on. platform_admin is
# control-plane by the cross_org RIGHT it carries (REQ-1337) — no name is ever tested.
_ROLE_CAPS = {
    PLATFORM_ADMIN_ROLE: ["admin", "superadmin", "platform_settings", "cross_org"],
    ORG_ADMIN_ROLE: ["user_management", "usage", "query_development", "column_grant"],
}


def _prepare_sync():
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id=_ORG, name="Root", created_by="system"))

        # The bootstrap slot starts UNCLAIMED — claiming is the explicit POST under test (REQ-1290).
        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments, user_directory])
        for role_id, caps in _ROLE_CAPS.items():
            conn.execute(insert(roles).values(id=role_id, capabilities=caps))
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
    monkeypatch.setattr(app_state, "auth_config", {"bootstrap_superadmin": True}, raising=False)
    # The roles registry is what turns a role id into its rights; in a real process it comes from
    # the schema.sql seed the DDL above mirrors.
    monkeypatch.setattr(
        app_state,
        "roles",
        {rid: {"id": rid, "capabilities": caps} for rid, caps in _ROLE_CAPS.items()},
        raising=False,
    )

    yield admin_db, tenant_db, sync_engine

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


def _make_app(admin_db: Database, tenant_db: Database) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseLikeProvider({"tok-first": "first", "tok-second": "second"}),
        admin_pool=admin_db,
        db_pool=tenant_db,
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        bootstrap_superadmin=True,
        default_org_id=_ORG,
    )
    app.include_router(auth_router)

    @app.get("/data/probe")
    async def _probe(request: Request):  # noqa: RUF029 — Starlette route must be async
        # Stands in for the data surfaces: they resolve the acting role from request.state.role and
        # look it up in state.schemas (api/data/endpoint.py), which is where the 400 was raised.
        return {
            "roles": request.state.identity.roles,
            "role": request.state.role,
            "active_org_id": request.state.active_org_id,
        }

    return app


def _q(sync_engine, schema, stmt):
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema}"))
        return conn.execute(stmt).fetchall()


def _claim(client) -> dict:
    resp = client.post("/auth/claim-bootstrap", headers={"Authorization": "Bearer tok-first"})
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_claim_seats_the_claimant_in_both_planes(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        assert _claim(client) == {"claimed": True, "claimed_by": "first", "org_id": _ORG}

    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "first"),
    )
    assert [r[0] for r in membership] == [_ORG]

    granted = _q(
        sync_engine,
        _TENANT_SCHEMA,
        select(user_role_assignments.c.role_id).where(user_role_assignments.c.user_id == "first"),
    )
    assert sorted(r[0] for r in granted) == [ORG_ADMIN_ROLE, PLATFORM_ADMIN_ROLE]


def test_claimant_acts_as_org_admin_on_a_data_request(planes):
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        _claim(client)
        resp = client.get("/data/probe", headers={"Authorization": "Bearer tok-first"})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    # The exact failure the user saw: the acting role was platform_admin, for which no data schema
    # is ever built, so the request was refused with a 400 instead of returning the demo data set.
    assert body["role"] == ORG_ADMIN_ROLE
    assert sorted(body["roles"]) == [ORG_ADMIN_ROLE, PLATFORM_ADMIN_ROLE]
    assert body["active_org_id"] == _ORG


def test_claimant_naming_platform_admin_still_acts_as_org_admin(planes):
    # The UI offers every assigned role; platform_admin IS assigned, so the header can name it. The
    # data surfaces resolve the caller's data-plane role rather than refusing the request.
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        _claim(client)
        resp = client.get(
            "/data/probe",
            headers={
                "Authorization": "Bearer tok-first",
                "X-Provisa-Role": PLATFORM_ADMIN_ROLE,
            },
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["role"] == ORG_ADMIN_ROLE


def test_claimant_keeps_the_control_plane_grant(planes):
    # platform_admin must survive alongside org_admin — it is what makes the control plane reachable.
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        _claim(client)
        resp = client.get("/auth/me", headers={"Authorization": "Bearer tok-first"})

    assert resp.status_code == 200, resp.text
    assert sorted(a["role_id"] for a in resp.json()["assignments"]) == [
        ORG_ADMIN_ROLE,
        PLATFORM_ADMIN_ROLE,
    ]


def test_a_second_user_is_not_seated_by_the_first_users_claim(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        _claim(client)
        resp = client.post("/auth/claim-bootstrap", headers={"Authorization": "Bearer tok-second"})

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"claimed": False, "claimed_by": "first", "org_id": None}
    granted = _q(
        sync_engine,
        _TENANT_SCHEMA,
        select(user_role_assignments.c.role_id).where(user_role_assignments.c.user_id == "second"),
    )
    assert granted == []
