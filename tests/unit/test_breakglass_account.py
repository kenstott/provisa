# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0f7c2e-8a41-4b9e-b3d6-1e7a2c9f4b80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1264 / REQ-2290: the seeded break-glass platform admin.

One test (or group) per rule: the username is fixed; the password is generated at the platform's
first start and shown once; only its bcrypt hash is stored; it is in no config file; the first
sign-in forces a change; the account holds platform_admin and no org role; it signs in with the
IdP unreachable.
"""

from __future__ import annotations

import base64
import io
import json
import os

import bcrypt
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from provisa.auth import superuser as su
from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider

_SECRET = "unit-test-breakglass-session-secret-padded!"


@pytest.fixture(autouse=True)
def _deployment_org_is_root(serve_deployment_org):
    serve_deployment_org("root")


@pytest.fixture(autouse=True)
def _no_claim_types(monkeypatch):
    monkeypatch.setattr("provisa.auth.middleware._loaded_derived_roles", lambda: [])


@pytest.fixture
def store(monkeypatch):
    """The platform control plane's ``deployment_settings`` rows, in memory."""
    from provisa.core import deployment_settings

    rows: dict[str, object] = {}

    def _create(_db, key, value, *, updated_by):  # noqa: ARG001
        if key in rows:
            return False
        rows[key] = json.loads(json.dumps(value))
        return True

    def _write(_db, values, *, updated_by):  # noqa: ARG001
        for key, value in values.items():
            rows[key] = json.loads(json.dumps(value))

    monkeypatch.setattr(deployment_settings, "get", rows.get)
    monkeypatch.setattr(deployment_settings, "create", _create)
    monkeypatch.setattr(deployment_settings, "write", _write)
    return rows


def _basic(username: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()


def _rotated(store) -> str:
    """Create the account and change its generated password; returns the new password."""
    generated = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    assert generated is not None
    new = "a-new-break-glass-password"
    su.change_breakglass_password(
        None,  # type: ignore[arg-type]
        su.BREAKGLASS_USERNAME,
        generated,
        new,
        store[su.BREAKGLASS_STATE],
    )
    return new


class UnreachableIdP(AuthProvider):
    """An IdP that cannot be reached: every token validation fails on the network."""

    provider_name = "firebase"

    async def validate_token(self, token: str) -> AuthIdentity:
        raise ConnectionError("IdP unreachable")


def _app(provider=None, session_secret=_SECRET) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=provider or UnreachableIdP(),
        superuser=None,  # no configured superuser: only the seeded break-glass account
        superuser_session_secret=session_secret,
    )

    @app.get("/test")
    async def _test(request: Request):
        return {
            "user_id": request.state.identity.user_id,
            "role": request.state.role,
            "assignments": [a.role_id for a in request.state.assignments],
            "held": list(request.state.identity.roles),
        }

    return app


@pytest.fixture
def router_client(monkeypatch, store):  # noqa: ARG001 -- the store backs the endpoints
    from provisa.api import app as app_mod
    from provisa.api.auth_router import router

    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr(
        app_mod.state, "auth_config", {"provider": "firebase", "jwt_secret": _SECRET}, raising=False
    )
    monkeypatch.setattr(app_mod.state, "admin_db", object(), raising=False)
    return TestClient(app)


# --- the username is fixed ---------------------------------------------------------------------


def test_username_is_fixed(store):
    assert su.BREAKGLASS_USERNAME == "platform-admin"
    password = _rotated(store)
    state = store[su.BREAKGLASS_STATE]
    identity = su.check_breakglass("platform-admin", password, state)
    assert identity is not None and identity.user_id == "platform-admin"
    assert su.check_breakglass("admin", password, state) is None
    assert su.check_breakglass("Platform-Admin", password, state) is None


# --- generated at the first start, shown once ---------------------------------------------------


def test_password_is_generated_at_first_start_and_shown_once(store):  # noqa: ARG001
    first = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    assert first is not None and len(first) >= 32
    # A later start (or another worker of the same launch) has nothing to show.
    assert su.ensure_breakglass(db=None) is None  # type: ignore[arg-type]
    console = io.StringIO()
    su.announce_breakglass(first, console)
    shown = console.getvalue()
    assert "platform-admin" in shown and first in shown


def test_each_platform_generates_its_own_password(monkeypatch, store):
    first = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    store.clear()
    second = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    assert first != second


def test_a_worker_that_loses_the_create_race_shows_nothing(monkeypatch, store):  # noqa: ARG001
    from provisa.core import deployment_settings

    monkeypatch.setattr(deployment_settings, "create", lambda *_a, **_k: False)
    assert su.ensure_breakglass(db=None) is None  # type: ignore[arg-type]


def test_the_announcement_never_goes_through_the_logger(store, caplog):  # noqa: ARG001
    password = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    assert password is not None
    su.announce_breakglass(password, io.StringIO())
    assert password not in caplog.text


# --- stored only as a bcrypt hash -----------------------------------------------------------------


def test_only_a_bcrypt_hash_is_stored(store):
    password = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    assert password is not None
    record = store[su.BREAKGLASS_STATE]
    assert set(record) == {"hash", "rotate"}
    assert record["hash"].startswith("$2b$")
    assert bcrypt.checkpw(password.encode(), record["hash"].encode())
    assert password not in json.dumps(store)


def test_a_changed_password_is_stored_only_as_a_bcrypt_hash(store):
    new = _rotated(store)
    record = store[su.BREAKGLASS_STATE]
    assert record["hash"].startswith("$2b$")
    assert bcrypt.checkpw(new.encode(), record["hash"].encode())
    assert new not in json.dumps(store)


# --- absent from every config file ----------------------------------------------------------------


def test_the_password_is_in_no_config_file_and_no_environment(monkeypatch, tmp_path, store):  # noqa: ARG001
    from provisa.api.admin import _config_io

    config = tmp_path / "provisa.yaml"
    config.write_text("auth:\n  provider: firebase\n")
    monkeypatch.setenv("PROVISA_CONFIG", str(config))

    def _refuse(*_a, **_k):
        raise AssertionError("the break-glass account wrote a config file")

    monkeypatch.setattr(_config_io, "write_config", _refuse)
    password = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    assert password is not None
    record_hash = store[su.BREAKGLASS_STATE]["hash"]
    for path in tmp_path.rglob("*"):
        if path.is_file():
            text = path.read_text()
            assert password not in text and record_hash not in text
    assert all(password not in v and record_hash not in v for v in os.environ.values())


# --- the first sign-in forces a change ------------------------------------------------------------


def test_the_generated_password_is_refused_until_changed(store):
    generated = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    assert generated is not None
    with pytest.raises(su.PasswordChangeRequired):
        su.check_breakglass("platform-admin", generated, store[su.BREAKGLASS_STATE])


def test_browser_sign_in_with_the_generated_password_requires_a_change(router_client, store):
    generated = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    resp = router_client.post(
        "/auth/superuser-login", json={"username": "platform-admin", "password": generated}
    )
    assert resp.status_code == 403, resp.text
    assert resp.json()["code"] == su.PASSWORD_CHANGE_REQUIRED
    assert store[su.BREAKGLASS_STATE]["rotate"] is True


def test_basic_sign_in_with_the_generated_password_requires_a_change(store):  # noqa: ARG001
    generated = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    resp = TestClient(_app()).get(
        "/test", headers={"Authorization": _basic("platform-admin", generated)}
    )
    assert resp.status_code == 403, resp.text
    assert resp.json()["code"] == su.PASSWORD_CHANGE_REQUIRED


def test_changing_the_password_signs_in_and_retires_the_generated_one(router_client, store):
    generated = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    resp = router_client.post(
        "/auth/superuser-password",
        json={
            "username": "platform-admin",
            "current_password": generated,
            "new_password": "a-new-break-glass-password",
        },
    )
    assert resp.status_code == 200, resp.text
    token = resp.json()["access_token"]
    assert store[su.BREAKGLASS_STATE]["rotate"] is False
    assert su.validate_breakglass_session(token, store[su.BREAKGLASS_STATE], _SECRET) is not None
    old = router_client.post(
        "/auth/superuser-login", json={"username": "platform-admin", "password": generated}
    )
    assert old.status_code == 401
    new = router_client.post(
        "/auth/superuser-login",
        json={"username": "platform-admin", "password": "a-new-break-glass-password"},
    )
    assert new.status_code == 200, new.text


def test_a_wrong_current_password_changes_nothing(router_client, store):
    su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    before = dict(store[su.BREAKGLASS_STATE])
    resp = router_client.post(
        "/auth/superuser-password",
        json={
            "username": "platform-admin",
            "current_password": "not-the-password",
            "new_password": "a-new-break-glass-password",
        },
    )
    assert resp.status_code == 401
    assert store[su.BREAKGLASS_STATE] == before


@pytest.mark.parametrize(
    ("new", "code"),
    [("short", "auth.password_too_short"), (None, "auth.password_unchanged")],
)
def test_a_weak_or_unchanged_new_password_is_refused(router_client, store, new, code):
    generated = su.ensure_breakglass(db=None)  # type: ignore[arg-type]
    resp = router_client.post(
        "/auth/superuser-password",
        json={
            "username": "platform-admin",
            "current_password": generated,
            "new_password": generated if new is None else new,
        },
    )
    assert resp.status_code == 400, resp.text
    assert resp.json()["code"] == code
    assert store[su.BREAKGLASS_STATE]["rotate"] is True


def test_a_session_issued_before_a_password_change_ends_with_it(store):
    password = _rotated(store)
    token = su.issue_breakglass_session(
        "platform-admin", password, store[su.BREAKGLASS_STATE], _SECRET
    )
    su.change_breakglass_password(
        None,  # type: ignore[arg-type]
        "platform-admin",
        password,
        "another-break-glass-password",
        store[su.BREAKGLASS_STATE],
    )
    assert su.validate_breakglass_session(token, store[su.BREAKGLASS_STATE], _SECRET) is None


# --- platform_admin only, no org role -------------------------------------------------------------


def test_the_account_holds_platform_admin_and_no_org_role(store):
    password = _rotated(store)
    identity = su.check_breakglass("platform-admin", password, store[su.BREAKGLASS_STATE])
    assert identity is not None and identity.roles == ["platform_admin"]


def test_the_middleware_grants_platform_admin_alone(store):
    password = _rotated(store)
    client = TestClient(_app())
    resp = client.get(
        "/test",
        headers={
            "Authorization": _basic("platform-admin", password),
            "X-Provisa-Role": "org_admin",
        },
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["user_id"] == "platform-admin"
    assert data["assignments"] == ["platform_admin"]
    assert data["held"] == ["platform_admin"]
    assert data["role"] == "platform_admin"


# --- signs in with the IdP unreachable ------------------------------------------------------------


def test_basic_sign_in_works_with_the_idp_unreachable(store):
    password = _rotated(store)
    client = TestClient(_app(UnreachableIdP()))
    resp = client.get("/test", headers={"Authorization": _basic("platform-admin", password)})
    assert resp.status_code == 200, resp.text
    assert resp.json()["user_id"] == "platform-admin"


def test_browser_session_works_with_the_idp_unreachable(router_client, store):
    password = _rotated(store)
    login = router_client.post(
        "/auth/superuser-login", json={"username": "platform-admin", "password": password}
    )
    assert login.status_code == 200, login.text
    token = login.json()["access_token"]
    resp = TestClient(_app(UnreachableIdP())).get(
        "/test", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["assignments"] == ["platform_admin"]


def test_a_wrong_password_never_reaches_past_the_account(store):
    _rotated(store)
    resp = TestClient(_app()).get(
        "/test", headers={"Authorization": _basic("platform-admin", "wrong-password")}
    )
    assert resp.status_code == 401
