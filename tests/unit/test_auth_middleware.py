# Copyright (c) 2026 Kenneth Stott
# Canary: 24916eec-da0e-4286-8eda-3f18a26a7e7d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for AuthMiddleware."""

from __future__ import annotations

import base64

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider


class MockProvider(AuthProvider):
    """Test provider that accepts 'valid-token' and rejects everything else."""

    async def validate_token(self, token: str) -> AuthIdentity:
        if token == "valid-token":
            return AuthIdentity(
                user_id="user1",
                email="user1@example.com",
                display_name="User One",
                roles=["editor"],
                raw_claims={"department": "engineering"},
            )
        raise ValueError("Invalid token")


def _make_app(
    provider=None,
    mapping_rules=None,
    default_role="analyst",
    superuser=None,
    superuser_session_secret=None,
    multitenancy=False,
    admin_pool=None,
):
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        mapping_rules=mapping_rules,
        default_role=default_role,
        superuser=superuser,
        superuser_session_secret=superuser_session_secret,
        multitenancy=multitenancy,
        admin_pool=admin_pool,
    )

    @app.get("/health")
    async def _health():
        return {"status": "ok"}

    @app.get("/auth/provider-type")
    async def _provider_type():
        return {"provider": "firebase"}

    @app.get("/auth/bootstrap-status")
    async def _bootstrap_status():
        return {"unclaimed": True}

    @app.get("/setup/status")
    async def _setup_status():
        return {"needs_setup": False}

    @app.get("/test")
    async def _test_route(request: Request):
        return {
            "user_id": request.state.identity.user_id,
            "role": request.state.role,
            "assignments": [a.role_id for a in (getattr(request.state, "assignments", None) or [])],
            "active_org_id": getattr(request.state, "active_org_id", None),
        }

    return app


def test_no_auth_configured_anonymous_org_admin():
    app = _make_app(provider=None)
    client = TestClient(app)
    resp = client.get("/test")
    assert resp.status_code == 200
    data = resp.json()
    # Unsecured: the ANONYMOUS dev principal (engages the documented dev-mode enforcement
    # skip) with the DATA-plane default role. REQ-1327: platform_admin is control-plane
    # only — an unsecured demo need not even define it, so it can never be the default.
    assert data["user_id"] == "anonymous"
    assert data["role"] == "org_admin"


def test_no_auth_configured_header_selects_role():
    """Unsecured + explicit X-Provisa-Role → that role, still the anonymous principal."""
    app = _make_app(provider=None)
    client = TestClient(app)
    resp = client.get("/test", headers={"x-provisa-role": "analyst"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == "anonymous"
    assert data["role"] == "analyst"


def test_valid_token():
    rules = [
        {"type": "exact", "claim": "department", "value": "engineering", "role": "engineer"},
    ]
    app = _make_app(provider=MockProvider(), mapping_rules=rules)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": "Bearer valid-token"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == "user1"
    assert data["role"] == "engineer"


def test_missing_auth_header():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/test")
    assert resp.status_code == 401
    assert "Missing" in resp.json()["detail"]


def test_invalid_token():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": "Bearer bad-token"})
    assert resp.status_code == 401
    assert "Invalid" in resp.json()["detail"]


def test_health_skips_auth():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_provider_type_skips_auth():
    # REQ-1267: the login page fetches /auth/provider-type BEFORE the user has a token,
    # to choose the sign-in UI. It must bypass the bearer gate even when auth is enforced.
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/auth/provider-type")
    assert resp.status_code == 200
    assert resp.json()["provider"] == "firebase"


def test_bootstrap_status_skips_auth():
    # REQ-1288: the login page asks whether the platform-admin slot is unclaimed BEFORE the user
    # picks a provider — the whole point is to warn them before they hold a credential.
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/auth/bootstrap-status")
    assert resp.status_code == 200
    assert resp.json()["unclaimed"] is True


def test_setup_status_skips_auth():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/setup/status")
    assert resp.status_code == 200


def test_malformed_auth_header():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": "Basic abc"})
    assert resp.status_code == 401
    assert "Missing" in resp.json()["detail"] or "invalid" in resp.json()["detail"].lower()


# --- REQ-125: superuser bootstrap short-circuit -----------------------------

_SU = {"username": "root", "password": "s3cr3t"}


def _basic(username: str, password: str) -> str:
    raw = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {raw}"


def test_superuser_basic_grants_both_planes_with_bearer_provider():
    # Works even though a bearer IdP is configured. The break-glass account holds the control
    # plane AND the data plane; the ACTING role is the data-plane one, because REQ-1327 keeps a
    # control-plane role off the data surfaces (they answer "No schema for role platform_admin").
    app = _make_app(provider=MockProvider(), superuser=_SU)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": _basic("root", "s3cr3t")})
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == "root"
    assert data["role"] == "org_admin"  # REQ-1327
    assert data["assignments"] == ["platform_admin", "org_admin"]  # REQ-125/REQ-1297


def test_superuser_role_header_cannot_select_the_control_plane_role():
    # REQ-1327: the acting role stays the data-plane one. The break-glass account holds exactly
    # one role per plane, so no header can put a control-plane role on the data surfaces.
    app = _make_app(provider=MockProvider(), superuser=_SU)
    client = TestClient(app)
    resp = client.get(
        "/test",
        headers={"Authorization": _basic("root", "s3cr3t"), "X-Provisa-Role": "platform_admin"},
    )
    assert resp.status_code == 200
    assert resp.json()["role"] == "org_admin"


def test_superuser_wrong_password_falls_through_to_provider():
    # Non-matching Basic creds are not superuser; the bearer provider rejects Basic.
    app = _make_app(provider=MockProvider(), superuser=_SU)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": _basic("root", "wrong")})
    assert resp.status_code == 401
    # The Basic header doesn't match the Bearer scheme expected by MockProvider,
    # so the middleware returns a missing/invalid header error.
    detail = resp.json()["detail"]
    assert "Missing" in detail or "invalid" in detail.lower() or "Invalid" in detail


def test_bearer_token_still_works_when_superuser_configured():
    app = _make_app(provider=MockProvider(), superuser=_SU)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": "Bearer valid-token"})
    assert resp.status_code == 200
    assert resp.json()["user_id"] == "user1"


# --- REQ-1472: the break-glass BROWSER session ------------------------------

_SU_SECRET = "unit-test-superuser-session-secret-padded!"


def test_superuser_session_token_grants_both_planes_under_an_idp():
    # The browser's presentation of the same account: an IdP is configured and owns the bearer
    # scheme, yet the operator's own session token is accepted ahead of it.
    from provisa.auth.superuser import issue_superuser_session

    token = issue_superuser_session("root", "s3cr3t", _SU, _SU_SECRET)
    app = _make_app(provider=MockProvider(), superuser=_SU, superuser_session_secret=_SU_SECRET)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == "root"
    assert data["role"] == "org_admin"  # REQ-1327
    assert data["assignments"] == ["platform_admin", "org_admin"]


def test_provider_bearer_token_still_works_when_the_session_secret_is_configured():
    # A token this deployment did not mint must reach the provider, not be refused by the
    # superuser branch — otherwise configuring the secret would lock every IdP user out.
    app = _make_app(provider=MockProvider(), superuser=_SU, superuser_session_secret=_SU_SECRET)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": "Bearer valid-token"})
    assert resp.status_code == 200
    assert resp.json()["user_id"] == "user1"


def test_superuser_session_token_signed_with_another_key_is_not_accepted():
    from provisa.auth.superuser import issue_superuser_session

    forged = issue_superuser_session("root", "s3cr3t", _SU, "a-different-key-of-adequate-length!")
    app = _make_app(provider=MockProvider(), superuser=_SU, superuser_session_secret=_SU_SECRET)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": f"Bearer {forged}"})
    assert resp.status_code == 401


def test_a_basic_provider_session_jwt_cannot_pose_as_the_superuser():
    # Same signing key, different issuer: providers/basic.py mints user sessions with
    # auth.jwt_secret too. Only a token carrying the superuser type claim counts.
    import datetime

    import jwt

    now = datetime.datetime.now(datetime.timezone.utc)
    user_session = jwt.encode(
        {"sub": "root", "username": "root", "iat": now, "exp": now + datetime.timedelta(hours=1)},
        _SU_SECRET,
        algorithm="HS256",
    )
    app = _make_app(provider=MockProvider(), superuser=_SU, superuser_session_secret=_SU_SECRET)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": f"Bearer {user_session}"})
    assert resp.status_code == 401


def test_superuser_session_is_refused_when_no_secret_is_configured():
    from provisa.auth.superuser import issue_superuser_session

    token = issue_superuser_session("root", "s3cr3t", _SU, _SU_SECRET)
    app = _make_app(provider=MockProvider(), superuser=_SU)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


def test_superuser_not_configured_no_short_circuit():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": _basic("root", "s3cr3t")})
    assert resp.status_code == 401
    # Without superuser configured, Basic header is not a special case;
    # MockProvider expects Bearer so the middleware rejects as missing/invalid.
    detail = resp.json()["detail"]
    assert "Missing" in detail or "invalid" in detail.lower() or "Invalid" in detail


def test_superuser_password_from_env_secret(monkeypatch):
    # Secrets are resolved once at wiring time via resolve_superuser_config.
    from provisa.auth.superuser import resolve_superuser_config

    monkeypatch.setenv("SU_PASS", "env-pass")
    su = resolve_superuser_config({"username": "root", "password": "${env:SU_PASS}"})
    app = _make_app(provider=MockProvider(), superuser=su)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": _basic("root", "env-pass")})
    assert resp.status_code == 200
    assert resp.json()["role"] == "org_admin"  # REQ-1327


def test_resolve_superuser_config_fails_fast_on_unset_secret(monkeypatch):
    from provisa.auth.superuser import resolve_superuser_config

    monkeypatch.delenv("SU_PASS", raising=False)
    # An unset secret raises at startup rather than silently disabling the superuser.
    with pytest.raises(KeyError) as exc_info:
        resolve_superuser_config({"username": "root", "password": "${env:SU_PASS}"})
    assert "SU_PASS" in str(exc_info.value)


def test_resolve_superuser_config_none_when_unconfigured():
    from provisa.auth.superuser import resolve_superuser_config

    assert resolve_superuser_config(None) is None
    assert resolve_superuser_config({}) is None
    assert resolve_superuser_config({"username": "root"}) is None  # missing password


# --- REQ-1267: lazy resolver re-resolves when auth is reconfigured at runtime -----


def _resolver_settings(provider):
    return {
        "provider": provider,
        "mapping_rules": [],
        "default_role": "analyst",
        "db_pool": None,
        "admin_pool": None,
        "assignments_source": "claims",
        "default_assignments": [],
        "multitenancy": False,
        "default_org_id": "root",
        "superuser": None,
        "superuser_session_secret": None,
        "bootstrap_superadmin": False,
    }


def _make_resolver_app(holder, gen, monkeypatch):
    monkeypatch.setattr(AuthMiddleware, "_current_generation", lambda self: gen["v"])
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware, config_resolver=lambda: _resolver_settings(holder["provider"])
    )

    @app.get("/test")
    async def _test_route(request: Request):
        return {"user_id": request.state.identity.user_id, "role": request.state.role}

    return app


def test_resolver_reresolves_on_generation_bump(monkeypatch):
    """A server that boots unsecured and is later switched to a real provider enforces
    auth on the next request — the cached resolution invalidates when the generation
    advances (REQ-1267 runtime reconfigure / PROVISA_IDP boot deferral)."""
    holder: dict[str, AuthProvider | None] = {"provider": None}
    gen = {"v": 0}
    client = TestClient(_make_resolver_app(holder, gen, monkeypatch))

    # Boots unsecured: the anonymous dev principal (REQ-1327 — org_admin default, not
    # platform_admin; the control-plane role never defaults onto the data plane).
    assert client.get("/test").json()["user_id"] == "anonymous"

    # Reconfigure to a real provider WITHOUT bumping the generation: still cached (unsecured).
    holder["provider"] = MockProvider()
    assert client.get("/test").status_code == 200
    assert client.get("/test").json()["user_id"] == "anonymous"  # still the dev principal

    # Bump the generation: the middleware re-resolves and now enforces the provider.
    gen["v"] += 1
    assert client.get("/test").status_code == 401  # missing token now rejected
    resp = client.get("/test", headers={"Authorization": "Bearer valid-token"})
    assert resp.status_code == 200
    assert resp.json()["user_id"] == "user1"


def test_check_superuser_blank_config_never_matches():
    from provisa.auth.superuser import check_superuser

    # A resolved-but-empty password cannot authenticate, even with empty input.
    assert check_superuser("root", "", {"username": "root", "password": ""}) is None
    assert check_superuser("", "", {"username": "", "password": ""}) is None


class _TwoSchemeProvider(AuthProvider):
    """REQ-124: the shape the basic provider presents — one validator per credential form."""

    @property
    def auth_scheme(self) -> str:
        return "basic"

    @property
    def token_validators(self):
        return {"basic": self.validate_token, "bearer": self.validate_session_token}

    async def validate_token(self, token: str) -> AuthIdentity:
        if token != base64.b64encode(b"alice:pw").decode():
            raise ValueError("Invalid credentials")
        return AuthIdentity(user_id="alice-basic", email=None, display_name=None, roles=[])

    async def validate_session_token(self, token: str) -> AuthIdentity:
        if token != "session-jwt":
            raise ValueError("Invalid credentials")
        return AuthIdentity(user_id="alice-session", email=None, display_name=None, roles=[])


def _two_scheme_client():
    return TestClient(_make_app(provider=_TwoSchemeProvider(), default_role="analyst"))


def test_basic_credential_still_accepted():
    creds = base64.b64encode(b"alice:pw").decode()
    resp = _two_scheme_client().get("/test", headers={"Authorization": f"Basic {creds}"})
    assert resp.status_code == 200
    assert resp.json()["user_id"] == "alice-basic"


def test_session_jwt_accepted_under_bearer():
    resp = _two_scheme_client().get("/test", headers={"Authorization": "Bearer session-jwt"})
    assert resp.status_code == 200
    assert resp.json()["user_id"] == "alice-session"


def test_scheme_selects_exactly_one_validator():
    """A Basic credential presented as Bearer is rejected outright — a failed validation is
    never retried under the other scheme, which would make the header a second guess."""
    creds = base64.b64encode(b"alice:pw").decode()
    resp = _two_scheme_client().get("/test", headers={"Authorization": f"Bearer {creds}"})
    assert resp.status_code == 401


def test_unoffered_scheme_rejected():
    resp = _two_scheme_client().get("/test", headers={"Authorization": "Digest whatever"})
    assert resp.status_code == 401


def test_single_scheme_provider_unchanged():
    """A provider without token_validators still enforces its one declared scheme."""
    client = TestClient(_make_app(provider=MockProvider()))
    assert client.get("/test", headers={"Authorization": "Bearer valid-token"}).status_code == 200
    assert client.get("/test", headers={"Authorization": "Basic valid-token"}).status_code == 401


# --- REQ-1276: the Host subdomain binds the break-glass account too ----------


class _FakeAdminPool:
    """Admin plane holding exactly the orgs in ``org_ids``."""

    def __init__(self, org_ids: set[str]) -> None:
        self._org_ids = org_ids
        self.statements: list[object] = []  # object-ok: SQLAlchemy Select

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                return pool

            async def __aexit__(self, *exc):
                return False

        return _Ctx()

    async def execute_core(self, stmt):
        self.statements.append(stmt)
        compiled = stmt.compile()
        wanted = compiled.params["id_1"]

        class _Result:
            def __init__(self, row):
                self._row = row

            def fetchone(self):
                return self._row

        return _Result((wanted,) if wanted in self._org_ids else None)


def test_superuser_binds_the_org_named_by_the_host():
    """A break-glass call to `<org>.provisa.dev` answers from THAT org, not the default one."""
    pool = _FakeAdminPool({"ks"})
    app = _make_app(provider=MockProvider(), superuser=_SU, multitenancy=True, admin_pool=pool)
    resp = TestClient(app).get(
        "/test", headers={"Authorization": _basic("root", "s3cr3t"), "Host": "ks.provisa.dev"}
    )
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "ks"


def test_superuser_on_an_unknown_org_host_is_refused_not_defaulted():
    """An org the admin plane does not hold is a 404 — never a quiet fall back to the default."""
    pool = _FakeAdminPool(set())
    app = _make_app(provider=MockProvider(), superuser=_SU, multitenancy=True, admin_pool=pool)
    resp = TestClient(app).get(
        "/test", headers={"Authorization": _basic("root", "s3cr3t"), "Host": "nope.provisa.dev"}
    )
    assert resp.status_code == 404


def test_superuser_on_the_control_plane_host_binds_the_default_org():
    """`cloud.*` carries no org subdomain, so the break-glass account acts in the default org."""
    pool = _FakeAdminPool({"ks"})
    app = _make_app(provider=MockProvider(), superuser=_SU, multitenancy=True, admin_pool=pool)
    resp = TestClient(app).get(
        "/test", headers={"Authorization": _basic("root", "s3cr3t"), "Host": "cloud.provisa.dev"}
    )
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "root"
    assert pool.statements == []


def test_superuser_single_org_deployment_ignores_the_host():
    """Without multitenancy there is one org; the subdomain names nothing to bind."""
    pool = _FakeAdminPool({"ks"})
    app = _make_app(provider=MockProvider(), superuser=_SU, multitenancy=False, admin_pool=pool)
    resp = TestClient(app).get(
        "/test", headers={"Authorization": _basic("root", "s3cr3t"), "Host": "ks.provisa.dev"}
    )
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "root"
    assert pool.statements == []
