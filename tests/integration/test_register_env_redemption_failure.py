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

from provisa.api.auth_router import router as auth_router
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import org_invites, orgs
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

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
    monkeypatch.setattr(
        app_state, "config", types.SimpleNamespace(auth={"provider": "basic"}), raising=False
    )

    from types import SimpleNamespace

    async def _org_runtime(_org_id: str, _env: str | None = None):
        return SimpleNamespace(tenant_db=tenant_db)

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _org_runtime, raising=False)

    yield admin_db, tenant_db, sync_engine

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


def _make_app() -> FastAPI:
    app = FastAPI()
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
