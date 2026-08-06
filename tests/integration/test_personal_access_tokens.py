# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Personal access tokens end to end, against a live control plane (REQ-1263).

A PAT is the credential every non-browser protocol accepts, so these run against real Postgres
rather than a mocked store: the guarantees under test — a secret that is never readable back, a
revocation that takes effect, an expiry that is enforced — are all properties of what the
database actually holds.
"""

from __future__ import annotations

import datetime
import os
from datetime import timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text

from provisa.api.pat_router import router as pat_router
from provisa.auth.middleware import AuthMiddleware
from provisa.auth.pat import PersonalAccessTokenStore, hash_token, is_personal_access_token
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import orgs, user_org_memberships
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles, user_role_assignments
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1263_admin"
_TENANT_SCHEMA = "test_req1263_tenant"

_ALICE = "alice"
_BOB = "bob"
_ORG = "acme"


def _prepare_sync():
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments])
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id=_ORG, name="Acme", created_by="super"))
        for user in (_ALICE, _BOB):
            conn.execute(insert(user_org_memberships).values(user_id=user, org_id=_ORG))
    return engine


@pytest.fixture
def planes(monkeypatch):
    sync_engine = _prepare_sync()
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


@pytest.fixture
def store(planes):
    admin_db, _tenant_db = planes
    return PersonalAccessTokenStore(admin_db)


def _client(planes) -> TestClient:
    """The PAT router behind the real auth middleware, authenticated as alice."""
    admin_db, tenant_db = planes
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseLikeProvider({"tok-alice": _ALICE}),
        admin_pool=admin_db,
        db_pool=tenant_db,
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        default_org_id=_ORG,
    )
    app.include_router(pat_router)
    return TestClient(app)


_AS_ALICE = {"Authorization": "Bearer tok-alice"}


# ── the store ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_an_issued_token_resolves_to_its_owners_identity(store):
    secret, row = await store.issue(user_id=_ALICE, org_id=_ORG, name="laptop")

    assert is_personal_access_token(secret), "a PAT must be recognizable by its prefix alone"
    assert "token_hash" not in row, "the issuance record must not echo the stored hash"

    identity = await store.validate(secret)
    assert identity.user_id == _ALICE
    assert identity.active_org_id == _ORG
    assert identity.raw_claims["pat"] is True


@pytest.mark.asyncio
async def test_a_scoped_token_carries_its_own_role(store):
    secret, _ = await store.issue(
        user_id=_ALICE, org_id=_ORG, name="readonly", role_id="analyst", scopes=["read"]
    )

    identity = await store.validate(secret)

    assert identity.roles == ["analyst"], "a narrowed token must not resolve to the owner's role"
    assert identity.raw_claims["scopes"] == ["read"]


@pytest.mark.asyncio
async def test_the_secret_is_not_recoverable_from_the_store(store):
    secret, _ = await store.issue(user_id=_ALICE, org_id=_ORG, name="laptop")

    rows = await store.list_for_user(_ALICE, _ORG)

    assert len(rows) == 1
    serialized = repr(rows)
    assert secret not in serialized, "a listing must never disclose a working credential"
    assert rows[0]["token_hash"] == hash_token(secret)
    assert secret.startswith(rows[0]["prefix"]), "the display prefix identifies the row"


@pytest.mark.asyncio
async def test_an_unknown_token_is_rejected(store):
    with pytest.raises(ValueError):
        await store.validate("provisa_pat_never-issued")


@pytest.mark.asyncio
async def test_a_revoked_token_stops_working(store):
    secret, _ = await store.issue(user_id=_ALICE, org_id=_ORG, name="laptop")
    await store.validate(secret)

    assert await store.revoke(token_hash=hash_token(secret), user_id=_ALICE)

    with pytest.raises(ValueError):
        await store.validate(secret)


@pytest.mark.asyncio
async def test_revocation_is_scoped_to_the_owner(store):
    secret, _ = await store.issue(user_id=_ALICE, org_id=_ORG, name="laptop")

    assert not await store.revoke(token_hash=hash_token(secret), user_id=_BOB), (
        "holding a token's hash must not let another user revoke it"
    )
    await store.validate(secret)  # still live


@pytest.mark.asyncio
async def test_an_expired_token_is_rejected(store, planes):
    from provisa.core.schema_admin import personal_access_tokens

    secret, _ = await store.issue(
        user_id=_ALICE,
        org_id=_ORG,
        name="short-lived",
        expires_at=datetime.datetime.now(timezone.utc) + datetime.timedelta(days=1),
    )
    # Age the row rather than sleeping: the rule under test is the expiry comparison.
    admin_db, _tenant = planes
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            personal_access_tokens.update()
            .where(personal_access_tokens.c.token_hash == hash_token(secret))
            .values(expires_at=datetime.datetime.now(timezone.utc) - datetime.timedelta(seconds=1))
        )

    with pytest.raises(ValueError):
        await store.validate(secret)


@pytest.mark.asyncio
async def test_expiry_in_the_past_is_refused_at_issuance(store):
    with pytest.raises(ValueError):
        await store.issue(
            user_id=_ALICE,
            org_id=_ORG,
            name="already-dead",
            expires_at=datetime.datetime.now(timezone.utc) - datetime.timedelta(days=1),
        )


@pytest.mark.asyncio
async def test_leaving_an_org_revokes_that_orgs_tokens(store, planes):
    from provisa.core.org_membership import remove_from_org

    admin_db, tenant_db = planes
    secret, _ = await store.issue(user_id=_ALICE, org_id=_ORG, name="laptop")
    bob_secret, _ = await store.issue(user_id=_BOB, org_id=_ORG, name="bob-laptop")

    await remove_from_org(admin_db, tenant_db, _ALICE, _ORG)

    with pytest.raises(ValueError):
        await store.validate(secret)
    await store.validate(bob_secret)  # another member's token is untouched


# ── the routes ────────────────────────────────────────────────────────────────


def test_issue_returns_the_secret_exactly_once(planes):
    with _client(planes) as client:
        created = client.post("/auth/tokens", json={"name": "laptop"}, headers=_AS_ALICE)
        assert created.status_code == 200, created.text
        secret = created.json()["token"]
        assert is_personal_access_token(secret)

        listed = client.get("/auth/tokens", headers=_AS_ALICE)

    assert listed.status_code == 200, listed.text
    assert secret not in listed.text, "the secret must exist only in the issuance response"
    assert [t["name"] for t in listed.json()] == ["laptop"]


def test_an_unauthenticated_caller_cannot_issue_a_token(planes):
    with _client(planes) as client:
        resp = client.post("/auth/tokens", json={"name": "laptop"})

    assert resp.status_code == 401


def test_revoking_through_the_route_takes_effect(planes):
    with _client(planes) as client:
        created = client.post("/auth/tokens", json={"name": "laptop"}, headers=_AS_ALICE)
        token_hash = client.get("/auth/tokens", headers=_AS_ALICE).json()[0]["token_hash"]

        first = client.delete(f"/auth/tokens/{token_hash}", headers=_AS_ALICE)
        second = client.delete(f"/auth/tokens/{token_hash}", headers=_AS_ALICE)

    assert created.status_code == 200
    assert first.status_code == 200, first.text
    assert second.status_code == 404, "a second revocation of the same token is not a fresh act"


def test_an_absurd_expiry_is_refused(planes):
    with _client(planes) as client:
        resp = client.post(
            "/auth/tokens", json={"name": "forever", "expires_in_days": 4000}, headers=_AS_ALICE
        )

    # This app mounts the router alone, without create_app's ApiError handler, so the response
    # carries the plain HTTPException body; the i18n code is asserted at the raise site instead.
    assert resp.status_code == 400
    assert "expires_in_days" in resp.json()["detail"]
