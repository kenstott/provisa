# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7f4e1a-8c3d-4f9e-9a6b-1d5c7e3f0a2b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1602: a per_visitor invite is addressed to whatever the inviter was looking at.

``create_invite`` (provisa/api/admin/invites_router.py) captures ``env_name`` from the
inviter's own ``active_env()`` -- the request's resolved environment (REQ-1487) -- rather
than trusting a client-supplied value. ``active_env()`` reads a ContextVar that, in
production, only ``_OrgRoutingMiddleware`` (provisa/api/app.py) ever sets, by resolving the
``x-provisa-env`` header through ``resolve_selected_env``. The existing invite test coverage
(tests/unit/test_open_invite.py) exercises ``redeem_env`` alone with ``env_name`` injected
directly into a fixture dict, and tests/integration/test_invite_role_authz.py mounts only
``AuthMiddleware`` -- neither proves ``create_invite`` actually captures a non-prod env from
a real request. This test mounts a routing shim that calls the same real
``resolve_selected_env``/``set_current_env`` functions ``_OrgRoutingMiddleware`` calls, so the
binding under test is production code, not a reimplementation of it.

Same harness shape as test_invite_role_authz.py: DDL/seed on a synchronous psycopg2 engine,
the async control planes driven only inside the TestClient's event loop, auth mocked through
test_auth_integration._FirebaseLikeProvider (token -> uid) rather than live Firebase.
"""

from __future__ import annotations

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, select, text

from provisa.api.admin.invites_router import router as invites_router
from provisa.api.env_routing import EnvironmentRightError, EnvironmentSelectionError
from provisa.auth.middleware import AuthMiddleware
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import environments, org_invites, orgs, user_org_memberships
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles, user_role_assignments
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1602_admin"
_TENANT_SCHEMA = "test_req1602_tenant"
_ROOT_ORG = "root"

# org_admin here additionally carries environment_switch, unlike test_invite_role_authz.py's
# seed: this suite is about an inviter who IS on a branch, which requires the right to be
# served by one (REQ-1573).
_SEEDED_ROLE_CAPS: dict[str, list[str]] = {
    "org_admin": [
        "user_management",
        "source_registration",
        "access_config",
        "query_development",
        "environment_switch",
    ],
    # DEFAULT_INVITE_ROLE (provisa/api/admin/invites_router.py) -- resolve_invite_role requires
    # this role to exist in the org whenever a create_invite call doesn't name one explicitly.
    "analyst": ["usage", "query_development"],
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
        conn.execute(insert(orgs).values(id="acme", name="Acme", created_by="alice"))
        conn.execute(insert(orgs).values(id=_ROOT_ORG, name="Root", created_by="bob"))
        conn.execute(insert(user_org_memberships).values(user_id="alice", org_id="acme"))
        # REQ-1487/REQ-1602: the branch alice is looking at when she mints the invite.
        conn.execute(insert(environments).values(org_id="acme", name="qa", created_by="alice"))

        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments])
        for role_id, caps in _SEEDED_ROLE_CAPS.items():
            conn.execute(insert(roles).values(id=role_id, capabilities=caps))
        conn.execute(
            insert(user_role_assignments).values(
                user_id="alice", role_id="org_admin", domain_id="*"
            )
        )
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
    from provisa.api.org_runtime import OrgRegistry, OrgRuntime, runtime_key

    loaded_roles = {
        rid: {"id": rid, "capabilities": caps} for rid, caps in _SEEDED_ROLE_CAPS.items()
    }
    registry = OrgRegistry()
    registry.set(
        _ROOT_ORG, OrgRuntime(org_id=_ROOT_ORG, tenant_db=tenant_db, roles=dict(loaded_roles))
    )
    registry.set("acme", OrgRuntime(org_id="acme", tenant_db=tenant_db, roles=dict(loaded_roles)))
    # REQ-1529: "qa" is a separate runtime slot, keyed by runtime_key -- _active_runtime() refuses
    # to serve a branch out of the base org's runtime, so the fixture must register this slot too.
    registry.set(
        runtime_key("acme", "qa"),
        OrgRuntime(org_id="acme", tenant_db=tenant_db, roles=dict(loaded_roles)),
    )
    monkeypatch.setattr(app_state, "org_registry", registry, raising=False)
    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "org_id", _ROOT_ORG, raising=False)

    from types import SimpleNamespace

    async def _org_runtime(_org_id: str, _env=None):
        return SimpleNamespace(tenant_db=tenant_db)

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _org_runtime, raising=False)
    monkeypatch.setattr(
        "provisa.api.admin.invites_router.ensure_org_runtime", _org_runtime, raising=False
    )

    yield admin_db, tenant_db, sync_engine

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


class _EnvRoutingShim:
    """Test-only stand-in for provisa.api.app._OrgRoutingMiddleware.

    Binds the request's environment the same way production does -- reads the
    ``x-provisa-env`` header, resolves it through the real ``resolve_selected_env`` against
    the caller's ``active_org_id``/``identity`` (set upstream by AuthMiddleware), and sets the
    real ``current_env`` ContextVar via ``set_current_env`` -- so ``active_env()`` inside the
    route handler sees exactly what a live request would bind. Only the ASGI wiring is
    test-local; the resolution and binding are the same functions production calls.
    """

    def __init__(self, app, admin_db):
        self.app = app
        self._admin_db = admin_db

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        from provisa.api.admin.capabilities import env_gate_capabilities
        from provisa.api.app import ensure_org_runtime
        from provisa.api.app import state as app_state
        from provisa.api.env_routing import PROD, env_header_value, resolve_selected_env
        from provisa.api.org_runtime import (
            reset_current_env,
            reset_current_org,
            set_current_env,
            set_current_org,
        )

        request_state = scope.setdefault("state", {})
        active_org = request_state.get("active_org_id")
        identity = request_state.get("identity")
        requested_env = env_header_value(scope.get("headers") or [])
        env_org = active_org or app_state.org_id
        env_caps = env_gate_capabilities(identity, app_state)

        try:
            selected_env = await resolve_selected_env(
                self._admin_db,
                env_org,
                identity,
                requested_env,
                env_caps,
                # REQ-1618: the stand-in for _OrgRoutingMiddleware reads the same published flag.
                is_control_plane=bool(request_state.get("can_cross_org")),
            )
        except EnvironmentSelectionError as exc:
            from starlette.responses import JSONResponse

            await JSONResponse({"error": str(exc)}, status_code=404)(scope, receive, send)
            return
        except EnvironmentRightError as exc:
            from starlette.responses import JSONResponse

            await JSONResponse({"error": str(exc)}, status_code=403)(scope, receive, send)
            return

        if (active_org is None or active_org == app_state.org_id) and selected_env == PROD:
            await self.app(scope, receive, send)
            return

        await ensure_org_runtime(env_org, selected_env)
        org_token = set_current_org(env_org)
        env_token = set_current_env(selected_env)
        try:
            await self.app(scope, receive, send)
        finally:
            reset_current_env(env_token)
            reset_current_org(org_token)


def _make_app(admin_db: Database, tenant_db: Database) -> FastAPI:
    app = FastAPI()
    app.include_router(invites_router)
    # Added first == innermost (closest to the router): runs AFTER AuthMiddleware has set
    # request.state.active_org_id/identity, which is what it reads.
    app.add_middleware(_EnvRoutingShim, admin_db=admin_db)
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseLikeProvider({"tok-alice": "alice"}),
        admin_pool=admin_db,
        db_pool=tenant_db,
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        default_org_id=_ROOT_ORG,
    )
    return app


def _create(client, token: str, body: dict, env: str | None = None):
    headers = {"Authorization": f"Bearer {token}"}
    if env is not None:
        headers["x-provisa-env"] = env
    return client.post("/admin/invites/", json=body, headers=headers)


def _invite_env_names(sync_engine):
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        return [r[0] for r in conn.execute(select(org_invites.c.env_name)).fetchall()]


def test_a_per_visitor_invite_captures_the_branch_the_inviter_is_on(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _create(
            client,
            "tok-alice",
            {"org_id": "acme", "env_policy": "per_visitor", "env_ttl_seconds": 3600},
            env="qa",
        )
    assert resp.status_code == 200, resp.text
    assert resp.json()["env_name"] == "qa"
    assert _invite_env_names(sync_engine) == ["qa"]


def test_a_per_visitor_invite_minted_from_prod_captures_prod(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _create(
            client,
            "tok-alice",
            {"org_id": "acme", "env_policy": "per_visitor", "env_ttl_seconds": 3600},
        )
    assert resp.status_code == 200, resp.text
    assert resp.json()["env_name"] == "prod"
    assert _invite_env_names(sync_engine) == ["prod"]


def test_a_client_supplied_env_name_is_ignored_for_per_visitor(planes):
    # REQ-1602: env_name is captured from active_env(), never trusted off the wire -- a client
    # naming a branch it is not on must not be able to plant that name on the invite.
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _create(
            client,
            "tok-alice",
            {
                "org_id": "acme",
                "env_policy": "per_visitor",
                "env_ttl_seconds": 3600,
                "env_name": "some-other-branch",
            },
            env="qa",
        )
    assert resp.status_code == 200, resp.text
    assert resp.json()["env_name"] == "qa"
    assert _invite_env_names(sync_engine) == ["qa"]
