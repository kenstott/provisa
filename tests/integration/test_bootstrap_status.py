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
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text

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


def _prepare_sync():
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
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

    def set_bootstrap(enabled: bool) -> None:
        # state.auth_config is what wiring.py hands the middleware, and therefore what decides
        # whether signing in grants the bootstrap. The endpoint must read the same dict.
        monkeypatch.setattr(
            app_state, "auth_config", {"bootstrap_superadmin": enabled}, raising=False
        )

    yield sync_engine, set_bootstrap

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
    sync_engine.dispose()


def _get() -> dict:
    app = FastAPI()
    app.include_router(auth_router)
    with TestClient(app) as client:
        resp = client.get("/auth/bootstrap-status")
    assert resp.status_code == 200, resp.text
    return resp.json()


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
