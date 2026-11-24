# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6f3c1e-5d4a-4e8b-9f27-1246a1b2c3d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1246: the first verified Firebase sign-in of a person with no local user provisions their
user_profile, from the identity's details, with no local_users/password record; the next sign-in
is the same person (one profile, refreshed), not a second one.

Driven through the auth middleware a Firebase-authenticated request takes, with a fake store at
the store boundary: each pool's connection keeps the rows written to it per table, and an upsert
on a key it already holds updates that row, as the control plane's ON CONFLICT does."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy.sql.dml import Delete, Insert, Update
from sqlalchemy.sql.expression import TableClause

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider


@pytest.fixture(autouse=True)
def _deployment_org_is_root(serve_deployment_org):
    serve_deployment_org("root")


@pytest.fixture(autouse=True)
def _seeded_roles(monkeypatch):
    """The middleware resolves role ids to the rights the loaded roles registry gives them; these
    tests build no app state, so mirror the schema.sql seed (as test_auth_middleware_multitenancy)."""
    seeded = {
        "platform_admin": {
            "id": "platform_admin",
            "capabilities": ["platform_settings", "cross_org"],
        },
        "analyst": {"id": "analyst", "capabilities": ["usage"]},
    }
    monkeypatch.setattr("provisa.auth.middleware._loaded_roles", lambda: seeded)
    monkeypatch.setattr("provisa.auth.middleware._platform_roles", lambda: seeded)
    monkeypatch.setattr("provisa.auth.middleware._loaded_user_attributes", lambda _user_id: {})
    monkeypatch.setattr("provisa.auth.middleware._loaded_derived_roles", lambda: [])


class _FirebaseProvider(AuthProvider):
    """A verified Firebase ID token 'idtok:<sub>' is that Firebase user; anything else is refused."""

    provider_name = "firebase"

    async def validate_token(self, token: str) -> AuthIdentity:
        if not token.startswith("idtok:"):
            raise ValueError("Invalid token")
        sub = token[len("idtok:") :]
        return AuthIdentity(
            user_id=sub,
            email=f"{sub}@example.test",
            email_verified=True,
            display_name=f"Person {sub}",
            roles=[],
            raw_claims={"sub": sub, "email": f"{sub}@example.test", "email_verified": True},
        )


class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, store: "_Store") -> None:
        self._store = store

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def transaction(self):
        return self

    async def advisory_xact_lock(self, key: int) -> None:
        """One request at a time reaches this fake: there is no other writer to wait for."""

    async def execute_core(self, stmt):
        if isinstance(stmt, (Insert, Update, Delete)):
            assert isinstance(stmt.table, TableClause)
            self._store.writes.append(stmt.table.name)
            return _Result([])
        return _Result([])  # an empty store: no local user, no grant, no membership

    async def upsert(self, table, values, *, index_elements, update_columns):
        rows = self._store.rows.setdefault(table.name, {})
        key = tuple(values[c] for c in index_elements)
        self._store.writes.append(table.name)
        if key in rows:
            rows[key].update({c: values[c] for c in update_columns})
        else:
            rows[key] = dict(values)

    async def upsert_returning(self, *a, **k):
        raise AssertionError("a sign-in claims nothing (REQ-1290)")


class _Store:
    def __init__(self) -> None:
        self.rows: dict[str, dict[tuple, dict]] = {}
        self.writes: list[str] = []

    def acquire(self):
        return _Conn(self)


def _app(platform: _Store, tenant: _Store) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseProvider(),
        db_pool=tenant,
        admin_pool=platform,
        assignments_source="provisa",
        default_assignments=[{"role_id": "analyst", "domain_id": "*"}],
    )

    @app.get("/auth/me")
    async def _me(request: Request):
        return {"user_id": request.state.identity.user_id}

    return app


def _signed_in(sub: str) -> dict:
    return {"Authorization": f"Bearer idtok:{sub}"}


def test_first_firebase_sign_in_provisions_a_user_profile_from_the_identity():
    platform, tenant = _Store(), _Store()
    client = TestClient(_app(platform, tenant))

    resp = client.get("/auth/me", headers=_signed_in("fb-ada"))

    assert resp.status_code == 200, resp.text
    profiles = platform.rows["user_profiles"]
    assert list(profiles) == [("fb-ada",)]
    profile = profiles[("fb-ada",)]
    assert profile["user_id"] == "fb-ada"
    assert profile["email"] == "fb-ada@example.test"
    assert profile["email_verified"] is True
    assert profile["display_name"] == "Person fb-ada"
    assert profile["provider"] == "firebase"


def test_firebase_sign_in_creates_no_local_user_or_password():
    platform, tenant = _Store(), _Store()
    client = TestClient(_app(platform, tenant))

    assert client.get("/auth/me", headers=_signed_in("fb-ada")).status_code == 200

    assert "local_users" not in platform.writes
    assert "local_users" not in tenant.writes
    assert "local_users" not in platform.rows


def test_second_firebase_sign_in_is_the_same_profile_not_a_duplicate():
    platform, tenant = _Store(), _Store()
    client = TestClient(_app(platform, tenant))

    assert client.get("/auth/me", headers=_signed_in("fb-ada")).status_code == 200
    first_writes = platform.writes.count("user_profiles")
    resp = client.get("/auth/me", headers=_signed_in("fb-ada"))

    assert resp.status_code == 200, resp.text
    assert resp.json()["user_id"] == "fb-ada"
    # The second sign-in wrote the profile again (last-seen refresh) on the same key: one row.
    assert platform.writes.count("user_profiles") == first_writes + 1
    assert list(platform.rows["user_profiles"]) == [("fb-ada",)]


def test_an_invalid_firebase_token_provisions_nothing():
    platform, tenant = _Store(), _Store()
    client = TestClient(_app(platform, tenant))

    resp = client.get("/auth/me", headers={"Authorization": "Bearer forged"})

    assert resp.status_code == 401
    assert "user_profiles" not in platform.rows
