# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7c93f1-64de-4b08-8f21-c05e7bd39a4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1288: GET /auth/bootstrap-status tells the login page a deployment has no administrator yet.

The bootstrap grant (REQ-1266) fires as a side effect of the first successful sign-in, so without
this the first person to authenticate becomes platform admin with no warning. The login page reads
this before any credential exists, which is why the answer must not depend on one.

Runs against live Postgres: "claimed" is the presence of the singleton row, and the same
first-writer-wins upsert the middleware performs is what this reads back.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text

import provisa.core
from provisa.api.auth_router import router as auth_router
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import superadmin_bootstrap

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_SCHEMA = "test_req1288_admin"
# REQ-1296: claiming the slot seats the claimant in the bootstrap org, so the claim path touches the
# tenant plane too. This test owns both planes; the org id is the one state.org_id is pinned to below.
_ORG_ID = "req1288"
_ORG_SCHEMA = f"org_{_ORG_ID}"


def _prepare_sync():
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    schema_sql = (Path(provisa.core.__file__).parent / "schema.sql").read_text()
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
    with engine.begin() as conn:
        # The tenant plane is built with the same schema.sql the runtime runs, so the roles rows the
        # claim path assigns (platform_admin) exist. Built synchronously because an async build would
        # bind its engine to a loop the TestClient does not use.
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ORG_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ORG_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {_ORG_SCHEMA}"))
        conn.execute(text(schema_sql))
    return engine


@pytest.fixture
def admin_plane(monkeypatch):
    try:
        sync_engine = _prepare_sync()
    except Exception as exc:  # noqa: BLE001 — the suite provisions this PG; a miss is a config fault
        pytest.skip(f"live Postgres not reachable at {_SYNC_URL}: {exc}")

    admin_db = Database(create_engine_from_url(_ASYNC_URL), name="admin", search_path=_SCHEMA)

    from provisa.api.app import state as app_state

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    # REQ-1296: the claim seats the claimant in the bootstrap org, which writes the tenant plane.
    # Pinning org_id first makes the AppState shim resolve this test's runtime.
    monkeypatch.setattr(app_state, "org_id", _ORG_ID, raising=False)
    monkeypatch.setattr(
        app_state,
        "tenant_db",
        Database(create_engine_from_url(_ASYNC_URL), name="org", search_path=_ORG_SCHEMA),
        raising=False,
    )

    def set_bootstrap(enabled: bool) -> None:
        # state.auth_config is what wiring.py hands the middleware, and therefore what decides
        # whether signing in grants the bootstrap. The endpoint must read the same dict.
        monkeypatch.setattr(
            app_state, "auth_config", {"bootstrap_superadmin": enabled}, raising=False
        )

    yield sync_engine, set_bootstrap

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ORG_SCHEMA} CASCADE"))
    sync_engine.dispose()


def _make_app() -> FastAPI:
    """Auth router behind a stub that sets ``request.state.identity`` from an X-Test-User header.

    REQ-1290: /auth/claim-bootstrap reads its caller off ``request.state.identity``, which
    AuthMiddleware sets; the stub sets the same attribute so the endpoint's own logic is under test.
    Header-driven so one TestClient (one event loop, one async engine binding) can act as several
    callers — a second TestClient would rebind the pool to a new loop."""
    from starlette.middleware.base import BaseHTTPMiddleware

    from provisa.auth.models import AuthIdentity

    app = FastAPI()
    app.include_router(auth_router)

    async def _inject(request, call_next):
        user_id = request.headers.get("x-test-user")
        if user_id is not None:
            request.state.identity = AuthIdentity(
                user_id=user_id, email=None, display_name=None, roles=[], raw_claims={}
            )
        return await call_next(request)

    app.add_middleware(BaseHTTPMiddleware, dispatch=_inject)
    return app


def _claim(client: TestClient, user_id: str | None):
    """POST /auth/claim-bootstrap as ``user_id`` (None = an unauthenticated caller)."""
    headers = {} if user_id is None else {"X-Test-User": user_id}
    return client.post("/auth/claim-bootstrap", headers=headers)


def _status(client: TestClient) -> dict:
    resp = client.get("/auth/bootstrap-status")
    assert resp.status_code == 200, resp.text
    return resp.json()


def _get() -> dict:
    with TestClient(_make_app()) as client:
        return _status(client)


def test_reports_unclaimed_while_the_admin_slot_is_empty(admin_plane):
    _, set_bootstrap = admin_plane
    set_bootstrap(True)
    assert _get() == {"unclaimed": True}, (
        "an empty singleton table is exactly the state the login page must warn about"
    )


def test_reports_claimed_once_somebody_holds_the_slot(admin_plane):
    sync_engine, set_bootstrap = admin_plane
    set_bootstrap(True)
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {_SCHEMA}"))
        conn.execute(insert(superadmin_bootstrap).values(id=1, user_id="first-arrival"))
    assert _get() == {"unclaimed": False}


def test_a_deployment_without_bootstrap_mode_never_claims_anything(admin_plane):
    _, set_bootstrap = admin_plane
    set_bootstrap(False)
    assert _get() == {"unclaimed": False}, (
        "with bootstrap off nobody is promoted by signing in, so there is nothing to warn about "
        "even though the table is empty"
    )


# REQ-1290 — claiming the slot is an explicit POST, never a side effect of authenticating.


def test_claiming_an_unclaimed_slot_takes_it_and_closes_the_notice(admin_plane):
    _, set_bootstrap = admin_plane
    set_bootstrap(True)
    with TestClient(_make_app()) as client:
        resp = _claim(client, "alice")
        assert resp.status_code == 200, resp.text
        # REQ-1296: the response names the org the claimant was seated in, so the login page can
        # send the next request into a populated org.
        assert resp.json() == {"claimed": True, "claimed_by": "alice", "org_id": _ORG_ID}
        assert _status(client) == {"unclaimed": False}, (
            "the first-login notice must stop showing the moment the slot is taken"
        )


def test_a_second_claimant_loses_the_race_and_the_holder_stands(admin_plane):
    _, set_bootstrap = admin_plane
    set_bootstrap(True)
    with TestClient(_make_app()) as client:
        assert _claim(client, "alice").json()["claimed"] is True
        resp = _claim(client, "bob")
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"claimed": False, "claimed_by": "alice", "org_id": None}, (
            "first writer wins on the singleton row — a later claim reads back the holder, never "
            "displaces them"
        )


def test_reclaiming_by_the_holder_is_idempotent(admin_plane):
    _, set_bootstrap = admin_plane
    set_bootstrap(True)
    with TestClient(_make_app()) as client:
        assert _claim(client, "alice").json()["claimed"] is True
        assert _claim(client, "alice").json() == {
            "claimed": True,
            "claimed_by": "alice",
            "org_id": _ORG_ID,
        }


def test_claiming_is_404_when_the_deployment_has_no_bootstrap_slot(admin_plane):
    _, set_bootstrap = admin_plane
    set_bootstrap(False)
    with TestClient(_make_app()) as client:
        resp = _claim(client, "alice")
    assert resp.status_code == 404, resp.text


def test_claiming_without_a_credential_is_401(admin_plane):
    _, set_bootstrap = admin_plane
    set_bootstrap(True)
    with TestClient(_make_app()) as client:
        resp = _claim(client, None)
        assert resp.status_code == 401, resp.text
        assert _status(client) == {"unclaimed": True}, "a rejected claim must leave the slot open"
