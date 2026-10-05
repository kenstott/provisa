# Copyright (c) 2026 Kenneth Stott
# Canary: 443ff230-1db3-42f0-8024-bbe8afbb836e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1595: a failed environment mint during /register must fail the registration.

Regression coverage for the bug where /register caught redeem_env's exception, logged it, and
let pinned_env stay None -- so an invite whose env provisioning failed still "succeeded" with no
environment bound, and the visitor was served prod instead of the sandbox the invite promised.
The fix removed that except clause so redeem_env failures propagate out of /register. This test
fails if that swallow is ever reintroduced.
"""

from __future__ import annotations

import datetime
import os
import types
from datetime import timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text

from sqlalchemy import select

from provisa.api.auth_router import router as auth_router
from provisa.auth.models import AuthIdentity
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import local_users
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import org_invites, orgs, user_org_memberships
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_register_envfail_admin"
_TENANT_SCHEMA = "test_register_envfail_tenant"
_TOKEN = "invite-tok-envfail"


def _prepare_sync():
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    expires = datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id="sandbox", name="Sandbox", created_by="super"))
        conn.execute(
            insert(org_invites).values(
                token=_TOKEN,
                org_id="sandbox",
                role_id="org_admin",
                env_policy="per_visitor",
                env_ttl_seconds=3600,
                created_by="super",
                expires_at=expires,
            )
        )

        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles])
        conn.execute(insert(roles).values(id="org_admin", origin="admin"))
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

    from provisa.api.org_runtime import OrgRegistry, OrgRuntime

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(
        app_state, "config", types.SimpleNamespace(auth={"provider": "basic"}), raising=False
    )

    # REQ-1266: the invited org's runtime, as ensure_org_runtime builds it before the redemption
    # binds the org; its tenant plane is this test's schema. Every other runtime is the app's own.
    sandbox = OrgRuntime(org_id="sandbox")
    sandbox.tenant_db = tenant_db
    sandbox.model_db = tenant_db
    registry = OrgRegistry()
    for key in app_state.org_registry.all_org_ids():
        rt = app_state.org_registry.get(key)
        assert rt is not None
        registry.set(key, rt)
    registry.set("sandbox", sandbox)
    monkeypatch.setattr(app_state, "org_registry", registry)

    async def _org_runtime(org_id: str, env: str | None = None):
        assert org_id == "sandbox", org_id
        return sandbox

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _org_runtime, raising=False)

    yield admin_db, tenant_db, sync_engine

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


def _install_api_error_handler(app: FastAPI) -> None:
    # Mirror app.py's handler so a raised ApiError serialises its stable `code` (REQ-1350) rather
    # than FastAPI's default {"detail": ...}.
    from fastapi.responses import JSONResponse

    from provisa.api.errors import ApiError

    @app.exception_handler(ApiError)
    async def _h(_req, exc: ApiError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail, "code": exc.code, "params": exc.params},
        )


def _make_app() -> FastAPI:
    app = FastAPI()
    _install_api_error_handler(app)
    app.include_router(auth_router)
    return app


def test_a_failed_environment_mint_fails_registration_instead_of_seating_prod(planes, monkeypatch):
    """A redeem_env failure must surface as a registration failure, not a silent pinned_env=None."""
    admin_db, tenant_db, sync_engine = planes

    async def _boom(invite, user_id):
        raise RuntimeError("environment provisioning failed")

    monkeypatch.setattr("provisa.api.auth_router.redeem_env", _boom)

    with TestClient(_make_app(), raise_server_exceptions=False) as client:
        resp = client.post(
            "/auth/register",
            json={
                "username": "visitor1",
                "password": "correcthorsebatterystaple",
                "invite_token": _TOKEN,
            },
        )

    # The bug this guards against made this a 200 with pinned_env silently left None. Any success
    # response here means the swallow is back.
    assert resp.status_code >= 500, resp.text


# --- REQ-124: a credential-less /auth/register may create an account ONLY with a valid invite, and
# the invite is validated BEFORE any row is written. The router-only app below has no AuthMiddleware,
# so request.state.identity is never set -- exactly the unauthenticated case the middleware forwards.


def _make_app_authed(user_id: str = "existing-user") -> FastAPI:
    """Router plus a shim that marks every request as an already-authenticated caller."""
    app = FastAPI()
    _install_api_error_handler(app)

    @app.middleware("http")
    async def _inject(request, call_next):
        request.state.identity = AuthIdentity(
            user_id=user_id, email=None, display_name=None, roles=[], raw_claims={}
        )
        return await call_next(request)

    app.include_router(auth_router)
    return app


def _usernames(sync_engine) -> set[str]:
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        return {r[0] for r in conn.execute(select(local_users.c.username)).fetchall()}


def test_register_without_an_invite_is_refused_and_writes_nothing(planes):
    _admin_db, _tenant_db, sync_engine = planes
    with TestClient(_make_app(), raise_server_exceptions=False) as client:
        resp = client.post("/auth/register", json={"username": "noinvite", "password": "pw-pw-pw"})
    assert resp.status_code == 401, resp.text
    assert resp.json()["code"] == "auth.invite_required"
    assert "noinvite" not in _usernames(sync_engine)


def test_register_with_an_expired_invite_is_refused_and_writes_nothing(planes):
    _admin_db, _tenant_db, sync_engine = planes
    past = datetime.datetime.now(tz=timezone.utc) - datetime.timedelta(days=1)
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        conn.execute(
            insert(org_invites).values(
                token="invite-tok-expired",
                org_id="sandbox",
                role_id="org_admin",
                env_policy="per_visitor",
                env_ttl_seconds=3600,
                created_by="super",
                expires_at=past,
            )
        )
    with TestClient(_make_app(), raise_server_exceptions=False) as client:
        resp = client.post(
            "/auth/register",
            json={
                "username": "expired",
                "password": "pw-pw-pw",
                "invite_token": "invite-tok-expired",
            },
        )
    assert resp.status_code == 400, resp.text
    assert resp.json()["code"] == "auth.invalid_invite_token"
    assert "expired" not in _usernames(sync_engine)


def test_an_authenticated_caller_may_register_without_an_invite(planes, monkeypatch):
    _admin_db, _tenant_db, sync_engine = planes

    async def _noop_verifier(*_a, **_k):
        return None

    monkeypatch.setattr("provisa.api.auth_router.write_verifier", _noop_verifier)
    with TestClient(_make_app_authed(), raise_server_exceptions=False) as client:
        resp = client.post("/auth/register", json={"username": "byadmin", "password": "pw-pw-pw"})
    assert resp.status_code == 200, resp.text
    assert "byadmin" in _usernames(sync_engine)


def test_credential_less_register_with_a_valid_invite_creates_the_membership(planes, monkeypatch):
    _admin_db, _tenant_db, sync_engine = planes
    from types import SimpleNamespace

    async def _env(_invite, _user_id):
        return SimpleNamespace(name=None)  # pinned to prod (env=None)

    async def _noop(*_a, **_k):
        return None

    monkeypatch.setattr("provisa.api.auth_router.redeem_env", _env)
    monkeypatch.setattr("provisa.api.auth_router.seat_redeemed_roles", _noop)
    monkeypatch.setattr("provisa.api.auth_router.write_verifier", _noop)
    monkeypatch.setattr("provisa.api.sandbox_org.reseat_after_conferral", _noop)
    monkeypatch.setattr("provisa.core.commerce.bind_member_to_org_trial", _noop)

    with TestClient(_make_app(), raise_server_exceptions=False) as client:
        resp = client.post(
            "/auth/register",
            json={"username": "redeemer", "password": "pw-pw-pw", "invite_token": _TOKEN},
        )
    assert resp.status_code == 200, resp.text
    assert "redeemer" in _usernames(sync_engine)
    user_id = resp.json()["user_id"]
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        orgs_joined = {
            r[0]
            for r in conn.execute(
                select(user_org_memberships.c.org_id).where(
                    user_org_memberships.c.user_id == user_id
                )
            ).fetchall()
        }
    assert "sandbox" in orgs_joined
