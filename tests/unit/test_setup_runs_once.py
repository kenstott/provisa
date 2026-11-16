# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0e6f0a-2f0b-4f5e-9a57-1c7d2a9e4b31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""First-run setup runs on a deployment that still needs it, and on no other.

POST /setup/ creates an administrator, writes the auth section and its signing key. The app
offers the wizard only while /setup/status answers needs_setup; the route itself holds to the
same answer, so a caller on a configured deployment -- signed in or not -- is refused and nothing
is created, rotated or changed."""

# Requirements: REQ-120, REQ-124, REQ-539

from __future__ import annotations

import types

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

import provisa.api.setup_router as mod
from provisa.api.errors import ApiError
from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider


class _Result:
    def __init__(self, scalar=None, row=None):
        self._scalar, self._row = scalar, row

    def scalar(self):
        return self._scalar

    def fetchone(self):
        return self._row


class _Pool:
    """Stands in for the platform control plane: local_users with ``existing_users`` accounts,
    recording every account written."""

    def __init__(self, existing_users: int = 0):
        self.existing_users = existing_users
        self.created: list[dict] = []

    def acquire(self):
        pool = self

        class _Conn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute_core(self, stmt):
                if stmt.is_insert:
                    pool.created.append(stmt.compile().params)
                    return _Result()
                return _Result(scalar=pool.existing_users, row=None)

            async def upsert(self, _table, values, **_kw):
                pool.created.append(values)

        return _Conn()


class _SignedIn(AuthProvider):
    """Any ordinary signed-in user: holds no administrator role."""

    provider_name = "test"

    async def validate_token(self, token: str) -> AuthIdentity:
        if token == "valid-token":
            return AuthIdentity(
                user_id="user1",
                email="user1@example.com",
                display_name="User One",
                roles=["analyst"],
                raw_claims={},
            )
        raise ValueError("Invalid token")


@pytest.fixture(autouse=True)
def _deployment_org_is_root(serve_deployment_org, monkeypatch):
    serve_deployment_org("root")
    monkeypatch.setattr("provisa.auth.middleware._loaded_derived_roles", lambda: [])


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    """A deployment whose config file is ``cfg`` and whose control plane is ``pool``, with every
    write setup can make recorded."""
    cfg: dict = {}
    writes: list[dict] = []
    seated: list[str] = []
    stored: list[dict] = []
    verifiers: list[str] = []
    pool = _Pool()

    def _write(_path, new: dict) -> None:
        writes.append(new)

    async def _seat(user_id: str) -> None:
        seated.append(user_id)

    async def _verifier(_db, user_id, *_a) -> None:
        verifiers.append(user_id)

    async def _noop(*_a, **_kw):
        return None

    from provisa.core import settings_registry

    monkeypatch.setattr("provisa.api.admin._config_io.config_path", lambda: tmp_path / "p.yaml")
    monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: cfg)
    monkeypatch.setattr("provisa.api.admin._config_io.read_config_for_setup", lambda: dict(cfg))
    monkeypatch.setattr("provisa.api.admin._config_io.write_config", _write)
    monkeypatch.setattr("provisa.api.app._load_and_build", _noop)
    monkeypatch.setattr("provisa.api.auth_router._seat_claimant_in_root", _seat)
    monkeypatch.setattr(mod, "write_verifier", _verifier)
    monkeypatch.setattr(
        settings_registry, "store", lambda _db, given, **_kw: stored.append(dict(given))
    )
    monkeypatch.setattr(mod, "_is_demo", lambda: False)
    monkeypatch.delenv("PROVISA_IDP", raising=False)
    monkeypatch.delenv("PROVISA_MULTITENANCY", raising=False)
    monkeypatch.delenv("PROVISA_MCP_BRIDGE_COMMAND", raising=False)
    from provisa.api import app as app_mod

    monkeypatch.setattr(app_mod.state, "admin_db", pool, raising=False)
    return types.SimpleNamespace(
        cfg=cfg, pool=pool, writes=writes, seated=seated, stored=stored, verifiers=verifiers
    )


def _client(provider: AuthProvider | None) -> TestClient:
    """The setup route behind the auth middleware, with the app's ApiError wire format
    (provisa.api.app: detail, code, params)."""
    app = FastAPI()
    app.add_middleware(AuthMiddleware, provider=provider, default_role="analyst")
    app.include_router(mod.router)

    @app.exception_handler(ApiError)
    async def _api_error(_req: Request, exc: ApiError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail, "code": exc.code, "params": exc.params},
            headers=exc.headers,
        )

    return TestClient(app)


_BASIC = {"provider": "basic", "mode": "single", "admin_username": "root", "admin_password": "pw"}
_SIGNED_IN = {"Authorization": "Bearer valid-token"}


def _nothing_changed(deployment) -> bool:
    return not (
        deployment.pool.created
        or deployment.writes
        or deployment.seated
        or deployment.stored
        or deployment.verifiers
    )


# --- a deployment that still needs setup ---------------------------------------------------------


def test_an_unauthenticated_caller_sets_up_an_unconfigured_deployment(deployment):
    resp = _client(provider=None).post("/setup/", json=_BASIC)
    assert resp.status_code == 200
    assert resp.json() == {"success": True, "provider": "basic"}
    (account,) = deployment.pool.created
    assert account["username"] == "root"
    assert deployment.seated == [account["id"]]
    (written,) = deployment.writes
    assert written["auth"]["provider"] == "basic" and written["auth"]["jwt_secret"]


async def test_a_basic_deployment_with_no_accounts_still_takes_setup(deployment):
    """The status the app reads calls this state needs_setup (auth declared, nobody to sign in
    as), so setup creates its first administrator."""
    deployment.cfg["auth"] = {"provider": "basic"}
    assert (await mod._setup_state())["needs_setup"] is True
    result = await mod.run_setup(
        mod.SetupRequest(
            provider="basic", mode="single", admin_username="root", admin_password="pw"
        )
    )
    assert result == {"success": True, "provider": "basic"}
    assert len(deployment.pool.created) == 1


# --- a configured deployment ---------------------------------------------------------------------

_CONFIGURED_BASIC = {
    "provider": "basic",
    "assignments_source": "provisa",
    "default_assignments": [{"role_id": "org_admin", "domain_id": "*"}],
    "jwt_secret": "the-original-signing-key",
}

_RERUNS = [
    pytest.param({**_BASIC, "admin_username": "intruder"}, id="a-new-administrator"),
    pytest.param({**_BASIC, "mode": "multi", "admin_username": "intruder"}, id="a-platform-admin"),
    pytest.param({"provider": "none", "mode": "single"}, id="no-sign-in"),
    pytest.param({"provider": "none", "mode": "multi"}, id="no-sign-in-multi"),
    pytest.param(
        {"provider": "firebase", "mode": "single", "firebase_project_id": "theirs"},
        id="another-identity-provider",
    ),
]


def _refused(resp) -> None:
    assert resp.status_code == 409
    body = resp.json()
    assert set(body) == {"detail", "code", "params"}
    assert body["code"] == "setup.already_configured"
    assert body["detail"] and "REQ-" not in body["detail"]


@pytest.mark.parametrize("body", _RERUNS)
def test_a_signed_in_user_cannot_run_setup_on_a_configured_deployment(deployment, body):
    deployment.cfg["auth"] = dict(_CONFIGURED_BASIC)
    deployment.pool.existing_users = 1
    resp = _client(provider=_SignedIn()).post("/setup/", json=body, headers=_SIGNED_IN)
    _refused(resp)
    assert _nothing_changed(deployment)
    assert deployment.cfg["auth"] == _CONFIGURED_BASIC


@pytest.mark.parametrize("body", _RERUNS)
def test_nobody_runs_setup_again_on_a_no_sign_in_deployment(deployment, body):
    """Configured with no sign-in: every caller is the anonymous principal, and setup is as
    finished for them as for anyone."""
    deployment.cfg["auth"] = {"provider": "none"}
    resp = _client(provider=None).post("/setup/", json=body)
    _refused(resp)
    assert _nothing_changed(deployment)
    assert deployment.cfg["auth"] == {"provider": "none"}


@pytest.mark.parametrize("body", _RERUNS)
def test_setup_is_finished_under_an_external_identity_provider(deployment, body):
    deployment.cfg["auth"] = {"provider": "firebase", "bootstrap_superadmin": True}
    resp = _client(provider=_SignedIn()).post("/setup/", json=body, headers=_SIGNED_IN)
    _refused(resp)
    assert _nothing_changed(deployment)


def test_an_unauthenticated_caller_is_refused_on_a_configured_deployment(deployment):
    deployment.cfg["auth"] = dict(_CONFIGURED_BASIC)
    deployment.pool.existing_users = 1
    resp = _client(provider=_SignedIn()).post("/setup/", json=_BASIC)
    assert resp.status_code == 401
    assert _nothing_changed(deployment)
