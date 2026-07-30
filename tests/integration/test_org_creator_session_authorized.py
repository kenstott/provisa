# Copyright (c) 2026 Kenneth Stott
# Canary: 5f21c8a4-3b7e-49d0-92c6-8a1e07b4dd63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1320: the org creator is fully authorized in the SAME session, from the control-plane host.

Reported failure: a second user signed in, created an org, and the app answered "No roles
configured — you do not have permission to view this page." Signing out and back in fixed it.
Nothing in the suite caught that, because every existing onboarding test proves the creator's
``org_admin`` only by acting from the ORG subdomain (``acme.provisa.org``). Production
(``cloud.provisa.dev``) has no subdomain at all: the browser stays on the control-plane host for
the whole flow, so org selection there falls to the sole-membership rule rather than the Host
header, and ``/auth/me`` — the single call the UI trusts for "what may I do" — is answered on
that host too.

So this pins the control-plane-host path specifically, with no re-authentication anywhere: the
same Basic credentials that created the org must, once provisioning reports ``ready``, resolve
the new org, carry ``org_admin``, be reported as such by ``/auth/me``, and be accepted by an
org_admin-gated endpoint.

Like ``test_multi_org_onboarding_flow``, the data-plane build and physical provisioning are
stubbed — this is the control plane. DDL/verification run on a synchronous psycopg2 engine so the
async engines are only ever driven inside the TestClient's event loop.
"""

from __future__ import annotations

import base64
import os
import time
import types

import bcrypt
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text
from sqlalchemy.exc import OperationalError

from provisa.api.admin.invites_router import router as invites_router
from provisa.api.admin.orgs_router import router as orgs_router
from provisa.api.auth_router import router as auth_router
from provisa.auth.middleware import AuthMiddleware
from provisa.auth.providers.basic import BasicAuthProvider
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES, local_users
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1320_admin"
_TENANT_SCHEMA = "test_req1320_tenant"

_ACCOUNTS = {"founder": "pw-founder", "creator": "pw-creator"}
# The control-plane host — no org subdomain. This is what production serves; the whole point.
_CONTROL_HOST = "cloud.provisa.org"
_ORG_ID = "widgets"


def _basic(username: str) -> dict[str, str]:
    raw = f"{username}:{_ACCOUNTS[username]}".encode()
    return {"Authorization": "Basic " + base64.b64encode(raw).decode()}


def _headers(username: str) -> dict[str, str]:
    return {**_basic(username), "host": _CONTROL_HOST}


def _prepare_sync():
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        for username, password in _ACCOUNTS.items():
            pw_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
            conn.execute(
                insert(local_users).values(
                    id=username,
                    username=username,
                    password_hash=pw_hash,
                    email=f"{username}@example.com",
                    display_name=username,
                    is_active=True,
                )
            )

        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn)
        # schema.sql seeds org_admin/analyst per org; provisioning is stubbed here, so the FK
        # targets for user_role_assignments have to exist up front.
        conn.execute(insert(roles).values(id="org_admin"))
        conn.execute(insert(roles).values(id="analyst"))
    return engine


@pytest.fixture
def planes(monkeypatch):
    try:
        sync_engine = _prepare_sync()
    except OperationalError as exc:
        pytest.skip(f"live Postgres not reachable at {_SYNC_URL}: {exc}")

    admin_db = Database(create_engine_from_url(_ASYNC_URL), name="admin", search_path=_ADMIN_SCHEMA)
    tenant_db = Database(
        create_engine_from_url(_ASYNC_URL), name="tenant", search_path=_TENANT_SCHEMA
    )

    from provisa.api.app import state as app_state

    default_rt = app_state.org_registry.get(app_state.org_id)
    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "multitenancy", True, raising=False)
    monkeypatch.setattr(default_rt, "tenant_db", tenant_db)
    monkeypatch.setattr(
        default_rt,
        "roles",
        {
            "platform_admin": {"capabilities": ["admin", "superadmin"]},
            "org_admin": {"capabilities": ["user_management", "source_registration"]},
            "analyst": {"capabilities": ["query_development"]},
        },
    )
    monkeypatch.setattr(
        app_state, "config", types.SimpleNamespace(auth={"provider": "basic"}), raising=False
    )
    monkeypatch.setattr(
        app_state, "auth_config", {"provider": "basic", "bootstrap_superadmin": True}, raising=False
    )

    async def _fake_build(org_id, *, include_demo=False, isolated_engine=False):  # noqa: ARG001
        return types.SimpleNamespace(tenant_db=tenant_db)

    async def _noop_provision(*_args, **_kwargs):
        return None

    monkeypatch.setattr("provisa.api.app.build_org_runtime", _fake_build, raising=False)
    monkeypatch.setattr(
        "provisa.core.org_provisioning.provision_org", _noop_provision, raising=False
    )

    # The org registry is process-wide and has no TTL, so a runtime cached by an earlier test in
    # this module would hand this one a Database bound to that test's (now-closed) event loop —
    # "attached to a different loop". Each test creates the org afresh; evict on both sides.
    app_state.org_registry.invalidate(_ORG_ID)

    yield admin_db, tenant_db, sync_engine

    app_state.org_registry.invalidate(_ORG_ID)
    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


def _make_app(admin_db: Database, tenant_db: Database) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=BasicAuthProvider(admin_db),
        admin_pool=admin_db,
        db_pool=tenant_db,
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        bootstrap_superadmin=True,
        default_org_id="root",
    )
    app.include_router(auth_router)
    app.include_router(orgs_router)
    app.include_router(invites_router)

    @app.get("/whoami")
    async def _whoami(request: Request):  # noqa: RUF029 — Starlette route must be async
        return {
            "roles": request.state.identity.roles,
            "active_org_id": request.state.active_org_id,
        }

    return app


def _poll_ready(client: TestClient, org_id: str, headers: dict[str, str]) -> dict:
    for _ in range(50):
        resp = client.get(f"/admin/orgs/{org_id}/status", headers=headers)
        assert resp.status_code == 200, resp.text
        record = resp.json()
        if record["provisioning_state"] != "provisioning":
            return record
        time.sleep(0.02)
    raise AssertionError("org never left 'provisioning' state")


@pytest.fixture
def created_org(planes):
    """Seat the platform superadmin, then have ``creator`` self-create an org and reach ``ready``.

    The returned client keeps NO session state of its own — every call re-sends the same Basic
    credentials, so "the same session" here means literally the same identity with no re-login."""
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        claim = client.post("/auth/claim-bootstrap", headers=_headers("founder"))
        assert claim.status_code == 200, claim.text

        created = client.post(
            "/admin/orgs/",
            json={"id": _ORG_ID, "name": "Widgets", "include_demo": False},
            headers=_headers("creator"),
        )
        assert created.status_code == 200, created.text
        assert created.json()["provisioning_state"] == "provisioning"

        status = _poll_ready(client, _ORG_ID, _headers("creator"))
        assert status["provisioning_state"] == "ready", status
        assert status["provisioning_error"] is None, status

        yield client, sync_engine


def test_creator_resolves_the_new_org_on_the_control_plane_host(created_org):
    # Sole-membership org selection: with no subdomain to read, the one org the creator belongs to
    # IS the active org. Without this the UI gets a 401 "Org selection required" and renders the
    # permission-denied page even though the org exists and is ready.
    client, _ = created_org
    who = client.get("/whoami", headers=_headers("creator"))
    assert who.status_code == 200, who.text
    assert who.json()["active_org_id"] == _ORG_ID, who.json()


def test_creator_carries_org_admin_in_the_same_session(created_org):
    # No re-authentication between create and this call — the identity resolved for THIS request
    # must already carry the tenant-plane grant the provisioning task wrote.
    client, _ = created_org
    who = client.get("/whoami", headers=_headers("creator"))
    assert who.status_code == 200, who.text
    assert "org_admin" in who.json()["roles"], who.json()


def test_auth_me_reports_the_new_org_and_role(created_org):
    # /auth/me is the only thing the UI consults to decide what to render; "No roles configured"
    # is literally an empty `assignments` here. Membership must list the new org too, or the org
    # switcher has nothing to select.
    client, _ = created_org
    me = client.get("/auth/me", headers=_headers("creator"))
    assert me.status_code == 200, me.text
    body = me.json()
    assert body["active_org_id"] == _ORG_ID, body
    assert "org_admin" in {a["role_id"] for a in body["assignments"]}, body
    assert _ORG_ID in {m["org_id"] for m in body["org_memberships"]}, body


def test_creator_may_immediately_use_an_org_admin_endpoint(created_org):
    # Authorization proven by effect, not by self-report: an org_admin-gated write must succeed
    # from the control-plane host, with no org subdomain and no re-login.
    client, _ = created_org
    invite = client.post(
        "/admin/invites/",
        json={"org_id": _ORG_ID, "role_id": "analyst"},
        headers=_headers("creator"),
    )
    assert invite.status_code == 200, invite.text
    assert invite.json()["role_id"] == "analyst"
