# Copyright (c) 2026 Kenneth Stott
# Canary: 9c41b7d0-3a86-4e52-8f19-2d6b5ac70e34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1313/REQ-1314: the role an invitation may confer, against the live control planes.

``org_invites.role_id`` is a plain Text column with no foreign key — it names a row in the
per-org ``roles`` table, which lives in the tenant plane — and redemption feeds it straight
into ``grant_org_role``. Untouched, that lets the org_admin of any org name ``platform_admin``
in an invitation and have deployment-wide capabilities land on the redeemer. These tests pin
the two ends of the fix: creation validates the named role against the target org's roles and
refuses ``platform_admin`` outside root, and redemption revalidates rather than trusting the
row (a role can be dropped from the org between the two).

Same harness shape as test_redeem_invite: DDL, seeding and row verification run on a
SYNCHRONOUS psycopg2 engine so the async control-plane engines are only ever driven inside the
TestClient's event loop.
"""

from __future__ import annotations

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select, text

from provisa.api.admin.invites_router import router as invites_router
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

_ADMIN_SCHEMA = "test_req1313_admin"
_TENANT_SCHEMA = "test_req1313_tenant"
_ROOT_ORG = "root"

# REQ-1297 seeds these four as system template roles in every org schema; platform_admin is present
# so the "only into root" rule is proven by the org id, not by a missing row. REQ-1337: the inviter's
# own gate reads the RIGHTS these rows carry — an empty capability list authorizes nothing however
# the row is named — so the capabilities schema.sql seeds are mirrored here.
_SEEDED_ROLE_CAPS: dict[str, list[str]] = {
    "org_admin": ["user_management", "source_registration", "access_config", "query_development"],
    "analyst": ["usage", "query_development"],
    "developer": ["query_development", "create_view", "create_relationship", "write"],
    "platform_admin": ["admin", "superadmin", "platform_settings", "cross_org"],
}


def _prepare_sync():
    """Both schemas, their tables, two orgs, and one org_admin in each — synchronously.

    A single tenant schema stands in for both orgs' role tables: the invite-role rules under test
    are decided from the org id and the roles rows, and every assertion here is about which of
    those is refused, so a second physical schema would add setup without adding a case.
    """
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
        conn.execute(insert(user_org_memberships).values(user_id="bob", org_id=_ROOT_ORG))

        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments])
        for role_id, caps in _SEEDED_ROLE_CAPS.items():
            conn.execute(insert(roles).values(id=role_id, capabilities=caps))
        for user_id in ("alice", "bob"):
            conn.execute(
                insert(user_role_assignments).values(
                    user_id=user_id, role_id="org_admin", domain_id="*"
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
    from provisa.api.org_runtime import OrgRegistry, OrgRuntime

    # AppState routes its per-org maps through the registry, so the default org named by
    # state.org_id must have a runtime registered or every state.roles read asserts.
    # REQ-1337: the runtime's roles registry is where a role id becomes the rights it carries, and
    # the inviter's gate reads those rights. In a real process it comes from the schema.sql seed;
    # here it mirrors the capability lists written into the role rows above.
    loaded_roles = {
        rid: {"id": rid, "capabilities": caps} for rid, caps in _SEEDED_ROLE_CAPS.items()
    }
    registry = OrgRegistry()
    registry.set(
        _ROOT_ORG, OrgRuntime(org_id=_ROOT_ORG, tenant_db=tenant_db, roles=dict(loaded_roles))
    )
    registry.set("acme", OrgRuntime(org_id="acme", tenant_db=tenant_db, roles=dict(loaded_roles)))
    monkeypatch.setattr(app_state, "org_registry", registry, raising=False)
    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    # resolve_invite_role compares the invitation's org against the deployment's root org.
    monkeypatch.setattr(app_state, "org_id", _ROOT_ORG, raising=False)

    from types import SimpleNamespace

    async def _org_runtime(_org_id: str, _env=None):
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
        provider=_FirebaseLikeProvider(
            {"tok-alice": "alice", "tok-bob": "bob", "tok-carol": "carol"}
        ),
        admin_pool=admin_db,
        db_pool=tenant_db,
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        default_org_id=_ROOT_ORG,
    )
    app.include_router(auth_router)
    app.include_router(invites_router)
    return app


def _q(sync_engine, schema, stmt):
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema}"))
        return conn.execute(stmt).fetchall()


def _create(client, token: str, body: dict):
    return client.post("/admin/invites/", json=body, headers={"Authorization": f"Bearer {token}"})


def _invite_rows(sync_engine):
    return _q(sync_engine, _ADMIN_SCHEMA, select(org_invites.c.token, org_invites.c.role_id))


def test_named_role_that_exists_in_the_org_is_stored_unchanged(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _create(client, "tok-alice", {"org_id": "acme", "role_id": "developer"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["role_id"] == "developer"
    assert [r[1] for r in _invite_rows(sync_engine)] == ["developer"]


def test_invitation_naming_no_role_confers_analyst(planes):
    # REQ-1314: resolved at creation, so the stored row always names a concrete role rather than
    # deferring the choice to whatever the redemption path happens to default to.
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _create(client, "tok-alice", {"org_id": "acme"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["role_id"] == "analyst"
        token = resp.json()["token"]

        redeemed = client.post(
            "/auth/redeem-invite",
            json={"token": token},
            headers={"Authorization": "Bearer tok-carol"},
        )
    assert redeemed.status_code == 200, redeemed.text
    assert redeemed.json() == {"user_id": "carol", "org_id": "acme", "role_id": "analyst"}
    assert [r[1] for r in _invite_rows(sync_engine)] == ["analyst"]

    assignment = _q(
        sync_engine,
        _TENANT_SCHEMA,
        select(user_role_assignments.c.role_id).where(user_role_assignments.c.user_id == "carol"),
    )
    assert [r[0] for r in assignment] == ["analyst"]


def test_role_absent_from_the_org_is_refused_and_writes_no_invite(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _create(client, "tok-alice", {"org_id": "acme", "role_id": "wizard"})
    assert resp.status_code == 422, resp.text
    assert "wizard" in resp.json()["detail"]
    assert _invite_rows(sync_engine) == []


def test_org_admin_cannot_confer_platform_admin_in_their_own_org(planes):
    # The escalation this requirement closes: capabilities for platform_admin resolve
    # deployment-wide regardless of which org schema the assignment sits in.
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _create(client, "tok-alice", {"org_id": "acme", "role_id": "platform_admin"})
    assert resp.status_code == 403, resp.text
    assert "root" in resp.json()["detail"]
    assert _invite_rows(sync_engine) == []


def test_platform_admin_may_be_conferred_by_an_invitation_into_root(planes):
    # REQ-1298: a root invitation followed by that assignment is the sole path to a backup
    # platform administrator, so this case must stay open.
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _create(client, "tok-bob", {"org_id": _ROOT_ORG, "role_id": "platform_admin"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["role_id"] == "platform_admin"
    assert [r[1] for r in _invite_rows(sync_engine)] == ["platform_admin"]


def test_redemption_refuses_a_role_dropped_since_the_invitation_was_written(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        created = _create(client, "tok-alice", {"org_id": "acme", "role_id": "developer"})
        assert created.status_code == 200, created.text
        token = created.json()["token"]

        with sync_engine.begin() as conn:
            conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
            conn.execute(delete(roles).where(roles.c.id == "developer"))

        redeemed = client.post(
            "/auth/redeem-invite",
            json={"token": token},
            headers={"Authorization": "Bearer tok-carol"},
        )
    assert redeemed.status_code == 422, redeemed.text
    assert "developer" in redeemed.json()["detail"]

    # No assignment written, and the invitation is not burned by the refused attempt.
    assignment = _q(
        sync_engine,
        _TENANT_SCHEMA,
        select(user_role_assignments.c.role_id).where(user_role_assignments.c.user_id == "carol"),
    )
    assert assignment == []
    used = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(org_invites.c.used_at).where(org_invites.c.token == token),
    )[0]
    assert used[0] is None


def test_org_admin_of_another_org_cannot_invite_into_acme(planes):
    # bob administers root, so the role rules alone would let him name anything acme has —
    # _require_org_admin is what confines him, and it is checked before the role is resolved.
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = _create(client, "tok-bob", {"org_id": "acme", "role_id": "analyst"})
    assert resp.status_code == 403, resp.text
    assert _invite_rows(sync_engine) == []
