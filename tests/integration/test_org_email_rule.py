# Copyright (c) 2026 Kenneth Stott
# Canary: 8a41f2d0-6c19-4b7e-9f52-1d0e73a4c9b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1268: an org email rule gates invite redemption against the live control planes.

An org may carry ``email_rule`` (a regex). Redeeming an invite requires the authenticated
identity's email to match, even with a valid, unexpired, unburned invite — the invited address
and the signed-in address can differ. A mismatch is rejected 403 and grants nothing. A NULL rule
imposes no restriction (covered by test_redeem_invite). Same sync-DDL / async-request split as
test_redeem_invite (only TIMESTAMPTZ round-trips the tz-aware expiry comparison).
"""

from __future__ import annotations

import datetime
import os
from datetime import timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, select, text

from provisa.api.auth_router import router as auth_router
from provisa.auth.middleware import AuthMiddleware
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import org_invites, orgs, user_org_memberships
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles, user_role_assignments
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1268_admin"
_TENANT_SCHEMA = "test_req1268_tenant"


def _prepare_sync():
    """Two orgs: one whose email rule excludes the test identity, one whose rule includes it."""
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    expires = datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        # alice's email is alice@example.com (the fake provider). "acme" excludes it, "widget" admits it.
        conn.execute(
            insert(orgs).values(
                id="acme", name="Acme", created_by="super", email_rule=r"@acme\.com$"
            )
        )
        conn.execute(
            insert(orgs).values(
                id="widget", name="Widget", created_by="super", email_rule=r"@example\.com$"
            )
        )
        conn.execute(
            insert(org_invites).values(
                token="tok-acme", org_id="acme", role_id="org_admin",
                created_by="super", expires_at=expires,
            )
        )
        conn.execute(
            insert(org_invites).values(
                token="tok-widget", org_id="widget", role_id="org_admin",
                created_by="super", expires_at=expires,
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
    return app


def _q(sync_engine, schema, stmt):
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema}"))
        return conn.execute(stmt).fetchall()


def test_redeem_rejected_when_email_rule_excludes(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = client.post(
            "/auth/redeem-invite",
            json={"token": "tok-acme"},
            headers={"Authorization": "Bearer tok-alice"},
        )
    assert resp.status_code == 403, resp.text
    # No membership granted on rejection.
    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )
    assert membership == []
    # Invite NOT burned — the rejected redemption must leave it usable by a permitted address.
    invite = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(org_invites.c.used_at).where(org_invites.c.token == "tok-acme"),
    )[0]
    assert invite[0] is None


def test_redeem_allowed_when_email_rule_matches(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = client.post(
            "/auth/redeem-invite",
            json={"token": "tok-widget"},
            headers={"Authorization": "Bearer tok-alice"},
        )
    assert resp.status_code == 200, resp.text
    assert resp.json()["org_id"] == "widget"
    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )
    assert [r[0] for r in membership] == ["widget"]
