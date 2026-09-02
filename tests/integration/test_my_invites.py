# Copyright (c) 2026 Kenneth Stott
# Canary: 5f1c9a72-8b3e-4d16-9c04-7ae2f0d3b681
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1287: GET /auth/my-invites answers "do you have an invitation?" on its own.

Onboarding asks three orthogonal questions — account, invitation, membership. Without this
endpoint the middle one is unanswerable: an invited user who arrives at the front door
without their token is indistinguishable from a stranger, so the UI can only offer "create
an org". The endpoint matches open invitations against the caller's identity email.

Run against live Postgres because the eligibility predicate compares ``expires_at`` to a
tz-aware ``now``; only a TIMESTAMPTZ store round-trips that faithfully. DDL and seeding use
a synchronous psycopg2 engine so the async control-plane engine is only ever driven inside
the TestClient's event loop — same split as test_redeem_invite.
"""

from __future__ import annotations

import datetime
import os
from datetime import timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text

from provisa.api.auth_router import router as auth_router
from provisa.auth.middleware import AuthMiddleware
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import org_invites, orgs
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles, user_directory, user_role_assignments
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1287_admin"
_TENANT_SCHEMA = "test_req1287_tenant"


def _prepare_sync():
    """Create the admin schema and seed one eligible invite plus every ineligible variant."""
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    now = datetime.datetime.now(tz=timezone.utc)
    future = now + datetime.timedelta(days=1)
    past = now - datetime.timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))
        # The middleware resolves role assignments from the tenant plane on every request, so the
        # tenant tables must exist even though this endpoint reads only the platform plane.
        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments, user_directory])
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id="acme", name="Acme", created_by="super"))
        conn.execute(insert(orgs).values(id="globex", name="Globex", created_by="super"))

        rows = [
            # Eligible. Seeded with a differently-cased address to prove the match is
            # case-insensitive on both sides rather than relying on the router lowercasing.
            {
                "token": "tok-eligible",
                "org_id": "acme",
                "role_id": "analyst",
                "email": "ALICE@Example.com",
                "expires_at": future,
            },
            # Addressed to somebody else.
            {
                "token": "tok-other-person",
                "org_id": "globex",
                "role_id": "analyst",
                "email": "bob@example.com",
                "expires_at": future,
            },
            # Expired.
            {
                "token": "tok-expired",
                "org_id": "globex",
                "role_id": "analyst",
                "email": "alice@example.com",
                "expires_at": past,
            },
            # A shareable link invite is addressed to nobody, so it belongs to no inbox.
            {
                "token": "tok-link",
                "org_id": "globex",
                "role_id": "analyst",
                "email": None,
                "expires_at": future,
            },
        ]
        for row in rows:
            conn.execute(insert(org_invites).values(created_by="super", **row))
        # Already redeemed. REQ-1594 counts redemptions, so what makes this invite spent is
        # uses reaching max_uses; used_at/used_by only record who took the last one.
        conn.execute(
            insert(org_invites).values(
                token="tok-used",
                org_id="globex",
                role_id="analyst",
                email="alice@example.com",
                expires_at=future,
                uses=1,
                max_uses=1,
                used_at=now,
                used_by="alice",
                created_by="super",
            )
        )
    return engine


@pytest.fixture
def admin_plane(monkeypatch):
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

    yield admin_db, tenant_db

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


def _make_app(admin_db: Database, tenant_db: Database, tokens: dict[str, str]) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseLikeProvider(tokens),
        admin_pool=admin_db,
        db_pool=tenant_db,
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        default_org_id="root",
    )
    app.include_router(auth_router)
    return app


def _get(planes, token: str):
    admin_db, tenant_db = planes
    app = _make_app(admin_db, tenant_db, {token: token.removeprefix("tok-")})
    with TestClient(app) as client:
        return client.get("/auth/my-invites", headers={"Authorization": f"Bearer {token}"})


def test_returns_only_the_open_invite_addressed_to_the_caller(admin_plane):
    resp = _get(admin_plane, "tok-alice")
    assert resp.status_code == 200, resp.text
    invites = resp.json()["invites"]
    assert [i["token"] for i in invites] == ["tok-eligible"], (
        "another person's invite, an expired one, a redeemed one, and a link invite "
        "addressed to nobody must all stay out of this inbox"
    )
    assert invites[0]["org_id"] == "acme"
    assert invites[0]["org_name"] == "Acme", "the org NAME is what makes the offer legible"
    assert invites[0]["role_id"] == "analyst"
    # ISO-8601 with an offset — the UI renders an expiry the user can act on.
    assert datetime.datetime.fromisoformat(invites[0]["expires_at"]).tzinfo is not None


def test_a_user_with_no_invitations_gets_an_empty_inbox(admin_plane):
    resp = _get(admin_plane, "tok-carol")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"invites": []}, (
        "an empty list is the answer to 'do you have an invitation?' — not an error, "
        "the caller falls through to creating an org"
    )
