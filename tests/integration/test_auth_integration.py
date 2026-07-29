# Copyright (c) 2026 Kenneth Stott
# Canary: c7e2f1b4-d9a3-4e87-b0c6-5a8d2f3e1c9b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: Authentication & Identity (Section 2 requirements).

Covers:
  REQ-120 — pluggable auth provider interface (AuthProvider → AuthIdentity)
  REQ-121 — Firebase auth provider (token validation interface)
  REQ-122 — Keycloak OIDC provider (token validation interface)
  REQ-123 — Generic OIDC provider (token validation interface)
  REQ-124 — Simple username/password auth (bcrypt + JWT, not for production)
  REQ-125 — Superuser bootstrap access (HTTP Basic → admin role)
  REQ-551 — role mapping: identity claims → Provisa role_id + domain assignments

Auth tests that need real IdP connectivity are skipped (no live IdP in CI).
The middleware chain and token-validation interface are exercised with mocked
providers so the logic path — header extraction, validate_token dispatch,
role mapping, X-Provisa-Role header enforcement — is covered without a real IdP.
"""

from __future__ import annotations

import base64
import datetime

import bcrypt
import jwt
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider, RoleAssignment
from provisa.auth.role_mapping import resolve_assignments, resolve_role
from provisa.auth.superuser import check_superuser, resolve_superuser_config
from provisa.auth.providers.simple import SimpleAuthProvider
from provisa.security.rights import PLATFORM_ADMIN_ROLE

pytestmark = [pytest.mark.integration]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_JWT_SECRET = "test-secret-for-integration-tests-padded"

ROLE_ANALYST = "analyst"
ROLE_ADMIN = "admin"
ROLE_VIEWER = "viewer"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _hash(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _make_simple_provider(users: list[dict] | None = None) -> SimpleAuthProvider:
    users = users or [
        {"username": "alice", "password_hash": _hash("s3cr3t"), "roles": [ROLE_ANALYST]},
        {"username": "bob", "password_hash": _hash("passw0rd"), "roles": [ROLE_ADMIN]},
    ]
    return SimpleAuthProvider(users=users, jwt_secret=_JWT_SECRET)


def _echo_identity(request: Request) -> JSONResponse:
    """Test endpoint — echoes resolved identity fields."""
    return JSONResponse(
        {
            "user_id": getattr(request.state, "user_id", None)
            or getattr(request.state.identity, "user_id", None),
            "role": getattr(request.state, "role", None),
        }
    )


def _make_test_app(
    provider: AuthProvider | None,
    mapping_rules: list[dict] | None = None,
    default_role: str = ROLE_ANALYST,
    superuser: dict | None = None,
) -> Starlette:
    routes = [Route("/echo", _echo_identity)]
    app = Starlette(routes=routes)
    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        mapping_rules=mapping_rules or [],
        default_role=default_role,
        superuser=superuser,
    )
    return app


class _MockProvider(AuthProvider):
    """Controllable mock auth provider."""

    def __init__(self, identity: AuthIdentity | None, raise_on_validate: bool = False) -> None:
        self._identity = identity
        self._raise = raise_on_validate

    async def validate_token(self, token: str) -> AuthIdentity:
        del token
        if self._raise:
            raise ValueError("Invalid token")
        assert self._identity is not None
        return self._identity


# ---------------------------------------------------------------------------
# REQ-120 — Pluggable auth provider interface
# ---------------------------------------------------------------------------


class TestAuthProviderInterface:
    def test_auth_identity_carries_required_fields(self):
        # REQ-120: AuthIdentity must carry user_id, email, roles, raw_claims.
        identity = AuthIdentity(
            user_id="u123",
            email="u@example.com",
            display_name="User",
            roles=[ROLE_ANALYST],
            raw_claims={"sub": "u123"},
        )
        assert identity.user_id == "u123"
        assert identity.email == "u@example.com"
        assert ROLE_ANALYST in identity.roles
        assert identity.raw_claims["sub"] == "u123"

    def test_role_assignment_domain_wildcard(self):
        # REQ-120: RoleAssignment with domain_id='*' means all-domain access.
        ra = RoleAssignment(role_id=ROLE_ANALYST, domain_id="*")
        assert ra.role_id == ROLE_ANALYST
        assert ra.domain_id == "*"

    def test_role_assignment_domain_scoped(self):
        # REQ-120: RoleAssignment with specific domain_id scopes the role.
        ra = RoleAssignment(role_id="steward", domain_id="trading")
        assert ra.domain_id == "trading"

    def test_auth_provider_is_abstract(self):
        # REQ-120: AuthProvider is abstract — cannot be instantiated directly.
        with pytest.raises(TypeError):
            AuthProvider()  # type: ignore[abstract]

    def test_mock_provider_returns_identity(self):
        # REQ-120: a concrete AuthProvider subclass must return an AuthIdentity.
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=[ROLE_ANALYST],
            raw_claims={},
        )
        provider = _MockProvider(identity)
        # validate_token is async; check it is callable via the interface contract.
        import asyncio

        result = asyncio.run(provider.validate_token("any-token"))
        assert result.user_id == "u1"

    def test_auth_scheme_default_is_bearer(self):
        # REQ-120: AuthProvider.auth_scheme defaults to "bearer".
        provider = _MockProvider(identity=None)
        assert provider.auth_scheme == "bearer"


# ---------------------------------------------------------------------------
# REQ-124 — Simple username/password auth
# ---------------------------------------------------------------------------


class TestSimpleAuthProvider:
    def test_login_valid_credentials_returns_jwt(self):
        # REQ-124: valid credentials must return a signed JWT.
        provider = _make_simple_provider()
        token = provider.login("alice", "s3cr3t")
        assert isinstance(token, str)
        decoded = jwt.decode(token, _JWT_SECRET, algorithms=["HS256"])
        assert decoded["sub"] == "alice"

    def test_login_invalid_password_raises(self):
        # REQ-124: wrong password must raise ValueError.
        provider = _make_simple_provider()
        with pytest.raises(ValueError, match="Invalid credentials"):
            provider.login("alice", "wrong-password")

    def test_login_unknown_user_raises(self):
        # REQ-124: unknown username must raise ValueError.
        provider = _make_simple_provider()
        with pytest.raises(ValueError, match="Invalid credentials"):
            provider.login("nobody", "any")

    def test_validate_token_returns_identity(self):
        # REQ-124: validate_token must decode JWT and return AuthIdentity.
        import asyncio

        provider = _make_simple_provider()
        token = provider.login("alice", "s3cr3t")
        identity = asyncio.run(provider.validate_token(token))
        assert identity.user_id == "alice"
        assert ROLE_ANALYST in identity.roles

    def test_validate_token_expired_raises(self):
        # REQ-124: expired token must raise (jwt.ExpiredSignatureError).
        import asyncio

        provider = _make_simple_provider()
        now = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            "sub": "alice",
            "roles": [ROLE_ANALYST],
            "iat": now - datetime.timedelta(hours=2),
            "exp": now - datetime.timedelta(hours=1),
        }
        expired_token = jwt.encode(payload, _JWT_SECRET, algorithm="HS256")
        with pytest.raises(jwt.ExpiredSignatureError):
            asyncio.run(provider.validate_token(expired_token))

    def test_jwt_contains_role_claims(self):
        # REQ-124: JWT payload must contain roles claim.
        provider = _make_simple_provider()
        token = provider.login("bob", "passw0rd")
        decoded = jwt.decode(token, _JWT_SECRET, algorithms=["HS256"])
        assert ROLE_ADMIN in decoded["roles"]

    def test_token_has_short_expiry(self):
        # REQ-124: JWT must have a short expiry (30 min or less for simple/test use).
        provider = _make_simple_provider()
        token = provider.login("alice", "s3cr3t")
        decoded = jwt.decode(token, _JWT_SECRET, algorithms=["HS256"])
        expiry = datetime.datetime.fromtimestamp(decoded["exp"], tz=datetime.timezone.utc)
        issued = datetime.datetime.fromtimestamp(decoded["iat"], tz=datetime.timezone.utc)
        lifetime = expiry - issued
        assert lifetime <= datetime.timedelta(hours=1), (
            "REQ-124: simple auth tokens must have short lifetimes"
        )


# ---------------------------------------------------------------------------
# REQ-125 — Superuser bootstrap access
# ---------------------------------------------------------------------------


class TestSuperuserBootstrap:
    def test_check_superuser_correct_credentials_returns_identity(self):
        # REQ-125: valid superuser credentials must return admin AuthIdentity.
        config = {"username": "su_admin", "password": "su_secret"}
        identity = check_superuser("su_admin", "su_secret", config)
        assert identity is not None
        assert identity.user_id == "su_admin"
        assert PLATFORM_ADMIN_ROLE in identity.roles
        assert identity.raw_claims.get("superuser") is True

    def test_check_superuser_wrong_password_returns_none(self):
        # REQ-125: wrong password must return None, not raise.
        config = {"username": "su_admin", "password": "su_secret"}
        assert check_superuser("su_admin", "wrong", config) is None

    def test_check_superuser_wrong_username_returns_none(self):
        # REQ-125: wrong username must return None.
        config = {"username": "su_admin", "password": "su_secret"}
        assert check_superuser("other", "su_secret", config) is None

    def test_check_superuser_blank_password_never_matches(self):
        # REQ-125: blank configured password must never match (security guard).
        config = {"username": "su_admin", "password": ""}
        assert check_superuser("su_admin", "", config) is None

    def test_check_superuser_blank_username_never_matches(self):
        # REQ-125: blank configured username must never match.
        config = {"username": "", "password": "su_secret"}
        assert check_superuser("", "su_secret", config) is None

    def test_superuser_identity_has_admin_role(self):
        # REQ-125: superuser must always resolve to admin role regardless of provider.
        config = {"username": "bootstrap", "password": "bootstrap_pw"}
        identity = check_superuser("bootstrap", "bootstrap_pw", config)
        assert identity is not None
        assert identity.roles == [PLATFORM_ADMIN_ROLE]

    def test_resolve_superuser_config_returns_none_when_empty(self):
        # REQ-125: resolve_superuser_config returns None when config is None.
        assert resolve_superuser_config(None) is None
        assert resolve_superuser_config({}) is None

    def test_resolve_superuser_config_passes_through_credentials(self):
        # REQ-125: resolve_superuser_config must return dict with username+password.
        config = {"username": "su", "password": "pw"}
        resolved = resolve_superuser_config(config)
        assert resolved is not None
        assert resolved["username"] == "su"
        assert resolved["password"] == "pw"

    def test_superuser_http_basic_short_circuits_auth_middleware(self):
        # REQ-125: HTTP Basic superuser credentials short-circuit the middleware chain.
        identity = AuthIdentity(
            user_id="regular",
            email=None,
            display_name=None,
            roles=[ROLE_ANALYST],
            raw_claims={},
        )
        provider = _MockProvider(identity)
        superuser = {"username": "su", "password": "su_pw"}
        app = _make_test_app(provider=provider, superuser=superuser)
        client = TestClient(app, raise_server_exceptions=True)

        creds = base64.b64encode(b"su:su_pw").decode("ascii")
        resp = client.get("/echo", headers={"Authorization": f"Basic {creds}"})
        assert resp.status_code == 200
        assert resp.json()["role"] == PLATFORM_ADMIN_ROLE, (
            "REQ-125/REQ-1297: superuser Basic auth must resolve to the platform admin role"
        )


# ---------------------------------------------------------------------------
# REQ-120 — AuthMiddleware: token extraction and validation pipeline
# ---------------------------------------------------------------------------


class TestAuthMiddlewarePipeline:
    def test_missing_authorization_header_returns_401(self):
        # REQ-120: missing Authorization header must return 401.
        identity = AuthIdentity(
            user_id="u1", email=None, display_name=None, roles=[ROLE_ANALYST], raw_claims={}
        )
        app = _make_test_app(provider=_MockProvider(identity))
        client = TestClient(app, raise_server_exceptions=True)
        resp = client.get("/echo")
        assert resp.status_code == 401

    def test_invalid_token_returns_401(self):
        # REQ-120: invalid or expired token must return 401.
        app = _make_test_app(provider=_MockProvider(identity=None, raise_on_validate=True))
        client = TestClient(app, raise_server_exceptions=True)
        resp = client.get("/echo", headers={"Authorization": "Bearer bad-token"})
        assert resp.status_code == 401

    def test_valid_token_resolves_role_via_provider(self):
        # REQ-120: valid token must have validate_token called; role resolved from identity.
        identity = AuthIdentity(
            user_id="alice",
            email="alice@example.com",
            display_name="Alice",
            roles=[ROLE_ANALYST],
            raw_claims={},
        )
        mapping_rules = [{"type": "exact", "claim": "sub", "value": "alice", "role": ROLE_ANALYST}]
        app = _make_test_app(provider=_MockProvider(identity), mapping_rules=mapping_rules)
        client = TestClient(app, raise_server_exceptions=True)
        resp = client.get("/echo", headers={"Authorization": "Bearer any-valid-token"})
        assert resp.status_code == 200

    def test_no_provider_honors_x_provisa_role_header(self):
        # REQ-120 / REQ-273: when no provider configured (no auth), X-Provisa-Role is honored.
        app = _make_test_app(provider=None, default_role="admin")
        client = TestClient(app, raise_server_exceptions=True)
        resp = client.get("/echo", headers={"X-Provisa-Role": ROLE_ANALYST})
        assert resp.status_code == 200
        data = resp.json()
        assert data["role"] == ROLE_ANALYST

    def test_no_provider_defaults_to_admin(self):
        # REQ-120 / REQ-273: when no provider configured and no role header, default is admin.
        app = _make_test_app(provider=None)
        client = TestClient(app, raise_server_exceptions=True)
        resp = client.get("/echo")
        assert resp.status_code == 200
        assert resp.json()["role"] == PLATFORM_ADMIN_ROLE

    def test_x_provisa_role_not_assigned_returns_403(self):
        # REQ-273: client-supplied role via X-Provisa-Role is rejected if not assigned.
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=[ROLE_ANALYST],
            raw_claims={},
        )
        app = _make_test_app(provider=_MockProvider(identity), default_role=ROLE_ANALYST)
        client = TestClient(app, raise_server_exceptions=True)
        resp = client.get(
            "/echo",
            headers={
                "Authorization": "Bearer any",
                "X-Provisa-Role": "superadmin",
            },
        )
        assert resp.status_code == 403, "REQ-273: unassigned role in X-Provisa-Role must return 403"

    def test_health_path_skipped_by_middleware(self):
        # REQ-120: /health must be exempt from auth checks.
        from starlette.routing import Route as _Route

        def health(_: Request) -> JSONResponse:
            return JSONResponse({"ok": True})

        routes = [_Route("/health", health), Route("/echo", _echo_identity)]
        app = Starlette(routes=routes)
        app.add_middleware(
            AuthMiddleware,
            provider=_MockProvider(identity=None, raise_on_validate=True),
        )
        client = TestClient(app, raise_server_exceptions=True)
        resp = client.get("/health")
        assert resp.status_code == 200, "REQ-120: /health must bypass auth middleware"


# ---------------------------------------------------------------------------
# REQ-551 — Role mapping: claims → Provisa role_id + domain assignments
# ---------------------------------------------------------------------------


class TestRoleMapping:
    def test_resolve_role_exact_match(self):
        # REQ-551: exact-match mapping rule maps claim value to Provisa role_id.
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=[],
            raw_claims={"department": "engineering"},
        )
        rules = [
            {"type": "exact", "claim": "department", "value": "engineering", "role": ROLE_ANALYST}
        ]
        role = resolve_role(identity, rules, "viewer")
        assert role == ROLE_ANALYST

    def test_resolve_role_contains_match(self):
        # REQ-551: contains-match mapping maps a list claim value to a Provisa role.
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=[],
            raw_claims={"groups": ["data-platform", "engineering"]},
        )
        rules = [
            {
                "type": "contains",
                "claim": "groups",
                "value": "data-platform",
                "role": ROLE_ANALYST,
            }
        ]
        role = resolve_role(identity, rules, "viewer")
        assert role == ROLE_ANALYST

    def test_resolve_role_falls_back_to_default(self):
        # REQ-551: no matching rule must fall back to default_role.
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=[],
            raw_claims={"department": "hr"},
        )
        rules = [
            {"type": "exact", "claim": "department", "value": "engineering", "role": ROLE_ANALYST}
        ]
        role = resolve_role(identity, rules, ROLE_VIEWER)
        assert role == ROLE_VIEWER

    def test_resolve_role_first_matching_rule_wins(self):
        # REQ-551: first matching rule in the list determines the role.
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=[],
            raw_claims={"level": "senior"},
        )
        rules = [
            {"type": "exact", "claim": "level", "value": "senior", "role": ROLE_ANALYST},
            {"type": "exact", "claim": "level", "value": "senior", "role": ROLE_ADMIN},
        ]
        role = resolve_role(identity, rules, ROLE_VIEWER)
        assert role == ROLE_ANALYST, "REQ-551: first matching rule must win"

    def test_resolve_assignments_plain_role_is_global(self):
        # REQ-551: plain role claim (no colon) → RoleAssignment(role_id, "*").
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=[ROLE_ANALYST],
            raw_claims={},
        )
        assignments = resolve_assignments(identity)
        assert len(assignments) == 1
        assert assignments[0].role_id == ROLE_ANALYST
        assert assignments[0].domain_id == "*", (
            "REQ-551: plain role claim must resolve to domain_id='*'"
        )

    def test_resolve_assignments_structured_role_parses_domain(self):
        # REQ-551: "role_id:domain_id" claim → RoleAssignment(role_id, domain_id).
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=["analyst:trading_ops"],
            raw_claims={},
        )
        assignments = resolve_assignments(identity)
        assert len(assignments) == 1
        assert assignments[0].role_id == "analyst"
        assert assignments[0].domain_id == "trading_ops"

    def test_resolve_assignments_multiple_claims(self):
        # REQ-551: enterprise IdPs emit multiple role claims — all must be parsed.
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=["analyst:trading_ops", "steward:trading_risk", "viewer"],
            raw_claims={},
        )
        assignments = resolve_assignments(identity)
        assert len(assignments) == 3
        domains = {a.domain_id for a in assignments}
        assert "trading_ops" in domains
        assert "trading_risk" in domains
        assert "*" in domains

    def test_resolve_assignments_empty_claims_excluded(self):
        # REQ-551: blank/empty claims in roles list must be ignored.
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=["analyst", "", "   "],
            raw_claims={},
        )
        assignments = resolve_assignments(identity)
        assert len(assignments) == 1, (
            "REQ-551: blank role claims must not produce RoleAssignment entries"
        )
        assert assignments[0].role_id == "analyst"

    def test_resolve_assignments_strips_whitespace(self):
        # REQ-551: claims with surrounding whitespace must be trimmed before parsing.
        identity = AuthIdentity(
            user_id="u1",
            email=None,
            display_name=None,
            roles=[" analyst : trading_ops "],
            raw_claims={},
        )
        assignments = resolve_assignments(identity)
        assert assignments[0].role_id == "analyst"
        assert assignments[0].domain_id == "trading_ops"


# ---------------------------------------------------------------------------
# REQ-1266 — Single-administrator bootstrap (limited Firebase/IdP mode)
# ---------------------------------------------------------------------------


class _FirebaseLikeProvider(AuthProvider):
    """Provider that maps opaque tokens → user_ids, standing in for Firebase."""

    provider_name = "firebase"

    def __init__(self, token_to_user: dict[str, str]) -> None:
        self._map = token_to_user

    async def validate_token(self, token: str) -> AuthIdentity:
        user_id = self._map.get(token)
        if user_id is None:
            raise ValueError("Invalid token")
        return AuthIdentity(
            user_id=user_id,
            email=f"{user_id}@example.com",
            display_name=user_id,
            roles=[],
            raw_claims={"sub": user_id},
        )


def _make_admin_db(db_file: str):
    """Build a real platform control-plane Database over a temp SQLite file.

    The registry schema is created with a synchronous engine so DDL never touches
    the async engine's event loop; the async Database then connects lazily inside
    the TestClient's loop, avoiding cross-loop engine binding."""
    from sqlalchemy import create_engine as _sa_sync_engine

    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import REGISTRY_TABLES, metadata

    sync_engine = _sa_sync_engine(f"sqlite:///{db_file}")
    with sync_engine.begin() as conn:
        metadata.create_all(conn, tables=REGISTRY_TABLES)
    sync_engine.dispose()

    engine = create_engine_from_url(f"sqlite+aiosqlite:///{db_file}")
    return Database(engine, name="admin")


def _make_bootstrap_app(provider: AuthProvider, admin_db) -> Starlette:
    routes = [Route("/echo", _echo_identity)]
    app = Starlette(routes=routes)
    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        admin_pool=admin_db,
        assignments_source="provisa",
        default_assignments=[],
        bootstrap_superadmin=True,
        multitenancy=False,
        default_org_id="root",
    )
    return app


def _claim_slot(db_file: str, user_id: str) -> None:
    """Record ``user_id`` as the holder of the sole platform-admin slot.

    REQ-1290: the middleware reads this row and never writes it — claiming is the explicit
    POST /auth/claim-bootstrap the first-login page makes after the user picks a provider. These
    tests therefore seed the claim rather than expecting a request to produce one."""
    import sqlite3

    conn = sqlite3.connect(db_file)
    conn.execute("INSERT INTO superadmin_bootstrap (id, user_id) VALUES (1, ?)", (user_id,))
    conn.commit()
    conn.close()


class TestSingleAdminBootstrap:
    def test_claimant_resolves_to_admin(self, tmp_path):
        # REQ-1266: whoever holds the sole admin slot resolves to the admin role on every request.
        db_file = str(tmp_path / "admin.db")
        admin_db = _make_admin_db(db_file)
        _claim_slot(db_file, "alice")
        provider = _FirebaseLikeProvider({"tok-alice": "alice"})
        app = _make_bootstrap_app(provider, admin_db)
        client = TestClient(app, raise_server_exceptions=True)

        resp = client.get("/echo", headers={"Authorization": "Bearer tok-alice"})
        assert resp.status_code == 200
        assert resp.json()["role"] == PLATFORM_ADMIN_ROLE, (
            "REQ-1266: the claimant must resolve to admin role"
        )

    def test_second_different_user_is_denied(self, tmp_path):
        # REQ-1266: once the slot is claimed, a different user is denied (403).
        db_file = str(tmp_path / "admin.db")
        admin_db = _make_admin_db(db_file)
        _claim_slot(db_file, "alice")
        provider = _FirebaseLikeProvider({"tok-alice": "alice", "tok-bob": "bob"})
        app = _make_bootstrap_app(provider, admin_db)
        client = TestClient(app, raise_server_exceptions=True)

        first = client.get("/echo", headers={"Authorization": "Bearer tok-alice"})
        assert first.status_code == 200

        second = client.get("/echo", headers={"Authorization": "Bearer tok-bob"})
        assert second.status_code == 403, "REQ-1266: second user must be denied"
        assert "single administrator" in second.json()["detail"]

    def test_claimant_reauth_still_admin(self, tmp_path):
        # REQ-1266: the claiming user keeps admin on every subsequent login.
        db_file = str(tmp_path / "admin.db")
        admin_db = _make_admin_db(db_file)
        _claim_slot(db_file, "alice")
        provider = _FirebaseLikeProvider({"tok-alice": "alice", "tok-bob": "bob"})
        app = _make_bootstrap_app(provider, admin_db)
        client = TestClient(app, raise_server_exceptions=True)

        assert client.get("/echo", headers={"Authorization": "Bearer tok-alice"}).status_code == 200
        # A different user is rejected between the claimant's logins.
        assert client.get("/echo", headers={"Authorization": "Bearer tok-bob"}).status_code == 403
        again = client.get("/echo", headers={"Authorization": "Bearer tok-alice"})
        assert again.status_code == 200
        assert again.json()["role"] == PLATFORM_ADMIN_ROLE

    def test_authenticating_never_claims_the_slot(self, tmp_path):
        # REQ-1290: this is the defect that reached production. Claiming inside the middleware made
        # platform admin a side effect of holding a token, so a browser refresh took the slot before
        # the first-login disclosure (REQ-1288) could render.
        import sqlite3

        db_file = str(tmp_path / "admin.db")
        admin_db = _make_admin_db(db_file)
        provider = _FirebaseLikeProvider({"tok-alice": "alice"})
        client = TestClient(_make_bootstrap_app(provider, admin_db), raise_server_exceptions=True)

        resp = client.get("/echo", headers={"Authorization": "Bearer tok-alice"})
        assert resp.status_code == 200, (
            "an unclaimed slot is not 'registration closed' — POST /auth/claim-bootstrap has to be "
            "reachable by an authenticated caller or the slot could never be claimed"
        )
        assert resp.json()["role"] != "admin", "authenticating alone must not confer admin"
        rows = sqlite3.connect(db_file).execute("SELECT user_id FROM superadmin_bootstrap").fetchall()
        assert rows == [], "the middleware must never write the slot"


class TestFirebaseBootstrapRealWiring:
    """Exercise the REAL wiring path — wire_auth → build_auth_provider →
    the concrete FirebaseAuthProvider → the bootstrap gate — rather than a
    hand-built middleware with a mock provider. Only the firebase-admin SDK's
    network call (verify_id_token) is patched; the token→identity→gate flow and
    the config→middleware wiring are real."""

    def _patch_firebase(self, monkeypatch, token_claims: dict) -> None:
        pytest.importorskip("firebase_admin")
        import provisa.auth.providers.firebase as fb

        if not getattr(fb, "_HAS_FIREBASE", False):
            pytest.skip("firebase-admin not installed")
        # A truthy _apps makes FirebaseAuthProvider.__init__ skip initialize_app
        # (no service-account key / ADC lookup needed for the test).
        monkeypatch.setattr(fb.firebase_admin, "_apps", {"default": object()}, raising=False)

        def _verify(token: str) -> dict:
            if token not in token_claims:
                raise ValueError("Invalid token")
            return token_claims[token]

        monkeypatch.setattr(fb.firebase_auth, "verify_id_token", _verify, raising=False)

    def _wire(self, admin_db):
        from fastapi import FastAPI

        from provisa.auth.wiring import wire_auth

        app = FastAPI()
        app.add_route("/echo", _echo_identity)

        # REQ-1266 config exactly as _auto_configure_idp / run_setup write it for firebase.
        auth_config = {
            "provider": "firebase",
            "assignments_source": "provisa",
            "default_assignments": [],
            "bootstrap_superadmin": True,
            "firebase": {"project_id": "demo-project"},
        }
        wire_auth(app, auth_config, db_pool=None, admin_pool=admin_db)
        return app

    def test_real_provider_first_admin_second_denied(self, monkeypatch, tmp_path):
        self._patch_firebase(
            monkeypatch,
            {
                "tok-alice": {"uid": "alice", "email": "alice@example.com", "name": "Alice"},
                "tok-bob": {"uid": "bob", "email": "bob@example.com", "name": "Bob"},
            },
        )
        db_file = str(tmp_path / "admin.db")
        admin_db = _make_admin_db(db_file)
        _claim_slot(db_file, "alice")  # REQ-1290: the claim is explicit, never a request side effect
        client = TestClient(self._wire(admin_db), raise_server_exceptions=True)

        first = client.get("/echo", headers={"Authorization": "Bearer tok-alice"})
        assert first.status_code == 200
        # REQ-1297: the bootstrap claimant resolves to the platform admin role.
        assert first.json() == {"user_id": "alice", "role": PLATFORM_ADMIN_ROLE}

        second = client.get("/echo", headers={"Authorization": "Bearer tok-bob"})
        assert second.status_code == 403
        assert "single administrator" in second.json()["detail"]

    def test_real_provider_invalid_token_401(self, monkeypatch, tmp_path):
        self._patch_firebase(monkeypatch, {"tok-alice": {"uid": "alice"}})
        admin_db = _make_admin_db(str(tmp_path / "admin.db"))
        client = TestClient(self._wire(admin_db), raise_server_exceptions=True)
        resp = client.get("/echo", headers={"Authorization": "Bearer garbage"})
        assert resp.status_code == 401

    def test_real_provider_records_profile(self, monkeypatch, tmp_path):
        # The claimant is persisted to user_profiles with provider="firebase".
        self._patch_firebase(
            monkeypatch, {"tok-alice": {"uid": "alice", "email": "alice@example.com", "name": "Alice"}}
        )
        db_file = str(tmp_path / "admin.db")
        admin_db = _make_admin_db(db_file)
        _claim_slot(db_file, "alice")  # REQ-1290: the claim is explicit, never a request side effect
        client = TestClient(self._wire(admin_db), raise_server_exceptions=True)
        assert client.get("/echo", headers={"Authorization": "Bearer tok-alice"}).status_code == 200

        import sqlite3

        rows = sqlite3.connect(db_file).execute(
            "SELECT user_id, provider FROM user_profiles"
        ).fetchall()
        assert ("alice", "firebase") in rows


# ---------------------------------------------------------------------------
# REQ-121, REQ-122, REQ-123 — Provider interface contracts (no live IdP)
# ---------------------------------------------------------------------------


class TestAuthProviderContracts:
    def test_firebase_provider_is_importable(self):
        # REQ-121: FirebaseAuthProvider must be importable and conform to AuthProvider.
        from provisa.auth.providers.firebase import FirebaseAuthProvider

        assert issubclass(FirebaseAuthProvider, AuthProvider)

    def test_keycloak_provider_is_importable(self):
        # REQ-122: KeycloakAuthProvider must be importable and conform to AuthProvider.
        from provisa.auth.providers.keycloak import KeycloakAuthProvider

        assert issubclass(KeycloakAuthProvider, AuthProvider)

    def test_oauth_provider_is_importable(self):
        # REQ-123: OAuthProvider must be importable and conform to AuthProvider.
        from provisa.auth.providers.oauth import OAuthProvider

        assert issubclass(OAuthProvider, AuthProvider)

    def test_basic_provider_is_importable(self):
        # REQ-120: BasicAuthProvider (if present) must conform to AuthProvider.
        from provisa.auth.providers.basic import BasicAuthProvider

        assert issubclass(BasicAuthProvider, AuthProvider)

    def test_simple_provider_inherits_auth_provider(self):
        # REQ-124: SimpleAuthProvider must be a concrete AuthProvider.
        assert issubclass(SimpleAuthProvider, AuthProvider)

    def test_all_providers_have_validate_token(self):
        # REQ-120: every provider must implement validate_token.
        from provisa.auth.providers.firebase import FirebaseAuthProvider
        from provisa.auth.providers.keycloak import KeycloakAuthProvider
        from provisa.auth.providers.oauth import OAuthProvider

        for cls in (
            FirebaseAuthProvider,
            KeycloakAuthProvider,
            OAuthProvider,
            SimpleAuthProvider,
        ):
            assert hasattr(cls, "validate_token"), (
                f"REQ-120: {cls.__name__} must implement validate_token"
            )
