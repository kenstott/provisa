# Copyright (c) 2026 Kenneth Stott
# Canary: 4b2e8c19-7a53-4f0d-8c61-2d9f0ab54e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1266: self-service org creation + async provisioning status, against live control planes.

An authenticated member-less user creates an org and immediately owns it (admin-plane membership),
gets a "provisioning" response, and the background task flips the row to "ready". The heavy
data-plane build (per-org runtime + tenant DDL, which needs the federation engine) is stubbed here
so this test isolates the control-plane onboarding contract; the two-org queryability guarantee is
covered separately against a live engine. seeded_demo must persist so a post-restart rebuild knows
whether to reload the demo config.

DDL/verification run on a synchronous psycopg2 engine so the async control-plane engines are only
driven inside the TestClient's event loop (mirrors test_redeem_invite)."""

from __future__ import annotations

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, select, text

import provisa.core.org_membership as org_membership
import provisa.core.org_provisioning as org_provisioning
from provisa.api.admin.orgs_router import router as admin_orgs_router
from provisa.auth.middleware import AuthMiddleware
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES, orgs, user_org_memberships
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import (
    domains,
    registered_tables,
    roles,
    sources,
    table_columns,
    user_role_assignments,
)
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1266_admin"
_TENANT_SCHEMA = "test_req1266_tenant"


def _prepare_sync():
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        # A pre-existing org: proves the server_default keeps older rows "ready".
        conn.execute(insert(orgs).values(id="root", name="Enterprise"))
        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        # REQ-1301: provisioning refreshes the root org's registry view and registers it as a
        # meta-domain table, so the tenant plane needs the dataset catalog too — not just the roles.
        org_metadata.create_all(
            conn,
            tables=[roles, user_role_assignments, sources, domains, registered_tables,
                    table_columns],
        )
        conn.execute(insert(roles).values(id="org_admin"))
        conn.execute(insert(sources).values(id="provisa-admin", type="postgres"))
        conn.execute(insert(domains).values(id="meta"))
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

    # Stub the data-plane build: this test isolates the control-plane onboarding contract. The
    # background task must still flip the row to "ready" and record the grant call.
    grants: list[tuple] = []

    async def _fake_provision_org(*_a, **_k):
        return None

    async def _fake_build_org_runtime(org_id, *, include_demo=False, isolated_engine=False):
        import types as _types

        return _types.SimpleNamespace(tenant_db=admin_db, org_id=org_id)

    async def _fake_grant_org_role(_tenant_db, user_id, role_id):
        grants.append((user_id, role_id))

    monkeypatch.setattr(org_provisioning, "provision_org", _fake_provision_org)
    monkeypatch.setattr("provisa.api.app.build_org_runtime", _fake_build_org_runtime, raising=False)
    monkeypatch.setattr(org_membership, "grant_org_role", _fake_grant_org_role)

    yield admin_db, tenant_db, sync_engine, grants

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


def _make_app(admin_db: Database, tenant_db: Database) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseLikeProvider({"tok-carol": "carol"}),
        admin_pool=admin_db,
        db_pool=tenant_db,
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        default_org_id="root",
    )
    app.include_router(admin_orgs_router)
    return app


def _q(sync_engine, stmt):
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        return conn.execute(stmt).fetchall()


def test_create_org_provisions_and_reaches_ready(planes):
    admin_db, tenant_db, sync_engine, grants = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = client.post(
            "/admin/orgs/",
            json={"id": "carolco", "name": "Carol Co", "include_demo": True},
            headers={"Authorization": "Bearer tok-carol"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["id"] == "carolco"
        assert body["provisioning_state"] == "provisioning"

        # Poll the status endpoint until the background task flips it (bounded).
        final = None
        for _ in range(100):
            st = client.get(
                "/admin/orgs/carolco/status", headers={"Authorization": "Bearer tok-carol"}
            )
            assert st.status_code == 200, st.text
            final = st.json()
            if final["provisioning_state"] != "provisioning":
                break
        assert final is not None
        assert final["provisioning_state"] == "ready", final

    # Membership granted synchronously (admin plane) — creator owns the org at once.
    membership = _q(
        sync_engine,
        select(user_org_memberships.c.org_id).where(
            user_org_memberships.c.user_id == "carol"
        ),
    )
    assert [r[0] for r in membership] == ["carolco"]

    # seeded_demo persisted (include_demo=True) so a post-restart rebuild reloads the demo config.
    row = _q(
        sync_engine,
        select(orgs.c.provisioning_state, orgs.c.seeded_demo).where(orgs.c.id == "carolco"),
    )[0]
    assert row[0] == "ready"
    assert row[1] is True

    # Background task granted the creator org_admin in the tenant plane.
    assert ("carol", "org_admin") in grants


def test_create_org_requires_authenticated_identity(planes):
    # Anonymous is allowed only in dev/no-auth; here auth is configured, so an unauthenticated
    # request is rejected before any org is registered.
    admin_db, tenant_db, sync_engine, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = client.post("/admin/orgs/", json={"id": "nope", "name": "No"})
    assert resp.status_code == 401
    assert _q(sync_engine, select(orgs.c.id).where(orgs.c.id == "nope")) == []


def test_duplicate_org_id_rejected(planes):
    admin_db, tenant_db, sync_engine, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        first = client.post(
            "/admin/orgs/",
            json={"id": "dupe", "name": "First"},
            headers={"Authorization": "Bearer tok-carol"},
        )
        assert first.status_code == 200, first.text
        second = client.post(
            "/admin/orgs/",
            json={"id": "dupe", "name": "Second"},
            headers={"Authorization": "Bearer tok-carol"},
        )
    assert second.status_code == 409
