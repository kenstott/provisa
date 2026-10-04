# Copyright (c) 2026 Kenneth Stott
# Canary: 91961460-aae0-4b55-87cd-ea7d2c9ad7e3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1308: a user may not gain a role from an invitation they issued themselves.

Issuing an invite is a user_management act; redeeming your own would seat its role on yourself with
no second principal, which is exactly what the self-role-change guard on /admin/users forbids.
/auth/redeem-invite refuses when the redeemer is the invite's creator, before any role is seated."""

# Requirements: REQ-1308

from __future__ import annotations

import datetime
import os
from datetime import timezone

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text

from provisa.api.auth_router import router as auth_router
from provisa.api.errors import ApiError
from provisa.auth.models import AuthIdentity
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import org_invites, orgs

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ADMIN_SCHEMA = "test_invite_self_redeem_admin"
_TOKEN = "invite-tok-selfredeem"
_ISSUER = "user-issuer"


def _prepare():
    engine = create_engine(_URL, pool_pre_ping=True)
    expires = datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id="default", name="Default", created_by=_ISSUER))
        conn.execute(
            insert(org_invites).values(
                token=_TOKEN,
                org_id="default",
                role_id="org_admin",
                env_policy="none",
                created_by=_ISSUER,
                expires_at=expires,
            )
        )
    return engine


@pytest.fixture
def admin_db(monkeypatch):
    try:
        sync = _prepare()
    except Exception as exc:  # noqa: BLE001 — the suite provisions this PG; a miss is a config fault
        pytest.skip(f"live Postgres not reachable at {_URL}: {exc}")
    db = Database(create_engine_from_url(_URL), name="admin", search_path=_ADMIN_SCHEMA)
    from provisa.api.app import state as app_state

    monkeypatch.setattr(app_state, "admin_db", db, raising=False)
    yield db
    with sync.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
    sync.dispose()


def _app(user_id: str) -> FastAPI:
    app = FastAPI()

    @app.exception_handler(ApiError)
    async def _h(_req, exc: ApiError):
        return JSONResponse(
            status_code=exc.status_code, content={"detail": exc.detail, "code": exc.code}
        )

    @app.middleware("http")
    async def _inject(request, call_next):
        request.state.identity = AuthIdentity(
            user_id=user_id, email=None, display_name=None, roles=[], raw_claims={}
        )
        return await call_next(request)

    app.include_router(auth_router)
    return app


def test_the_issuer_cannot_redeem_their_own_invite(admin_db):
    # The creator (_ISSUER) redeeming the invite they issued is refused before any role is seated.
    with TestClient(_app(_ISSUER), raise_server_exceptions=False) as client:
        resp = client.post("/auth/redeem-invite", json={"token": _TOKEN})
    assert resp.status_code == 403, resp.text
    assert resp.json()["code"] == "auth.self_invite_redemption"
