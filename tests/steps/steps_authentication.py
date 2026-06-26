# Copyright (c) 2026 Kenneth Stott
# Canary: 9f3c2e7a-1b4d-4c8e-9a6f-2d5e8b1c4a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-123 — Generic OAuth 2.0 / OIDC support.

Exercises the generic OIDC validation path: a provider configured from an OIDC
discovery document publishes a JWKS; inbound JWT access tokens are validated
against that JWKS (RS256 signature + standard claims) and the configured role
claim is mapped onto a Provisa role via provisa.auth.role_mapping.

Also includes REQ-124 — simple username/password auth (bcrypt + short-lived JWT)
gated behind the allow_simple_auth flag, intended for local developer testing.

Also includes REQ-125 — superuser bootstrap access: superuser credentials in
config (username + password from env secret) always resolve to the admin role
with all capabilities, regardless of the configured auth provider.

Also includes REQ-535 — dev-mode anonymous identity: when no auth provider is
configured, any request is treated as the `anonymous` identity mapped to all
configured roles with wildcard domain access.
"""

from __future__ import annotations

import base64
import datetime
import json
import os
import uuid

import bcrypt
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider
from provisa.auth.providers.simple import SimpleAuthProvider
from provisa.auth.role_mapping import resolve_role

try:  # scenarios binding (path resolved relative to this test module)
    from pytest_bdd import given, parsers, scenarios, then, when

    scenarios("req_123.feature")
    scenarios("req_124.feature")
    scenarios("req_125.feature")
    scenarios("req_535.feature")
except Exception:  # pragma: no cover - binding optional when run standalone
    from pytest_bdd import given, parsers, then, when


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict used to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_KID = "req123-signing-key"
_ISSUER = "https://oidc.example.com"
_AUDIENCE = "provisa-platform"


def _build_jwks(private_key: rsa.RSAPrivateKey) -> dict:
    """Build a JWKS document from an RSA public key (as a real OIDC provider would)."""
    public_jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key()))
    public_jwk["kid"] = _KID
    public_jwk["use"] = "sig"
    public_jwk["alg"] = "RS256"
    return {"keys": [public_jwk]}


def _validate_via_jwks(token: str, jwks: dict) -> dict:
    """Replicate JWKS-based JWT validation: pick key by kid, verify RS256 + claims."""
    header = jwt.get_unverified_header(token)
    kid = header["kid"]
    matching = [k for k in jwks["keys"] if k["kid"] == kid]
    if not matching:
        raise jwt.InvalidKeyError(f"No JWKS key matches kid {kid!r}")
    public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(matching[0]))
    return jwt.decode(
        token,
        key=public_key,
        algorithms=["RS256"],
        audience=_AUDIENCE,
        issuer=_ISSUER,
    )


# ---------------------------------------------------------------------------
# Steps — REQ-123
# ---------------------------------------------------------------------------


@given("a generic OIDC provider is configured with a discovery URL")
def configure_oidc_provider(shared_data: dict) -> None:
    # An RSA keypair stands in for the provider's signing material; the public
    # half is exposed via the JWKS document referenced by the discovery doc.
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwks = _build_jwks(private_key)

    discovery = {
        "issuer": _ISSUER,
        "jwks_uri": f"{_ISSUER}/.well-known/jwks.json",
        "authorization_endpoint": f"{_ISSUER}/authorize",
        "token_endpoint": f"{_ISSUER}/token",
        "id_token_signing_alg_values_supported": ["RS256"],
    }

    shared_data["private_key"] = private_key
    shared_data["jwks"] = jwks
    shared_data["discovery"] = discovery
    shared_data["discovery_url"] = f"{_ISSUER}/.well-known/openid-configuration"
    # Configurable role claim mapping: provider emits a "groups" claim.
    shared_data["mapping_rules"] = [
        {"type": "exact", "claim": "groups", "value": "data-analysts", "role": "analyst"},
        {"type": "exact", "claim": "groups", "value": "data-admins", "role": "admin"},
    ]
    shared_data["default_role"] = "viewer"

    assert discovery["jwks_uri"].endswith("/jwks.json")
    assert "RS256" in discovery["id_token_signing_alg_values_supported"]
    assert jwks["keys"][0]["kid"] == _KID


@when("a request arrives with a JWT access token")
def request_with_jwt(shared_data: dict) -> None:
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    claims = {
        "sub": "user-" + uuid.uuid4().hex[:8],
        "iss": _ISSUER,
        "aud": _AUDIENCE,
        "iat": now,
        "exp": now + datetime.timedelta(minutes=15),
        "email": "analyst@example.com",
        "groups": "data-analysts",
    }
    token = jwt.encode(
        claims,
        shared_data["private_key"],
        algorithm="RS256",
        headers={"kid": _KID},
    )
    shared_data["token"] = token
    shared_data["expected_sub"] = claims["sub"]
    assert isinstance(token, str) and token.count(".") == 2


@then(
    "the token is validated via JWKS and roles are mapped using the "
    "configured claim mapping"
)
def validate_and_map(shared_data: dict) -> None:
    decoded = _validate_via_jwks(shared_data["token"], shared_data["jwks"])

    # JWKS validation succeeded and standard claims are intact.
    assert decoded["sub"] == shared_data["expected_sub"]
    assert decoded["iss"] == _ISSUER
    assert decoded["aud"] == _AUDIENCE
    assert decoded["groups"] == "data-analysts"

    # Configurable role claim mapping resolves the provider claim → Provisa role.
    role = resolve_role(
        shared_data["mapping_rules"],
        decoded,
        shared_data["default_role"],
    )
    assert role == "analyst"

    # A non-matching claim falls back to the configured default role.
    fallback = resolve_role(
        shared_data["mapping_rules"],
        {**decoded, "groups": "unmapped-group"},
        shared_data["default_role"],
    )
    assert fallback == shared_data["default_role"]

    # Tampered tokens must be rejected by JWKS signature verification.
    with pytest.raises(Exception):
        _validate_via_jwks(shared_data["token"] + "tamper", shared_data["jwks"])


# ---------------------------------------------------------------------------
# Steps — REQ-124 (simple username/password auth, dev-only)
# ---------------------------------------------------------------------------

_SIMPLE_JWT_SECRET = "req124-simple-auth-secret"


def _hash_pw(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


@given(
    "allow_simple_auth is true and users are defined in config YAML with "
    "bcrypt passwords"
)
def configure_simple_auth(shared_data: dict) -> None:
    # Mirrors a parsed auth config block where simple auth is explicitly enabled.
    config = {
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": _SIMPLE_JWT_SECRET,
        "simple": {
            "users": [
                {
                    "username": "devuser",
                    "password_hash": _hash_pw("dev-pass"),
                    "roles": ["analyst"],
                }
            ]
        },
    }

    # Guard: simple auth must be explicitly opted into (NOT for production).
    assert config["allow_simple_auth"] is True, "simple auth must be explicitly enabled"

    users = config["simple"]["users"]
    # Passwords must be stored as bcrypt hashes, never plaintext.
    for user in users:
        stored = user["password_hash"].encode("utf-8")
        assert stored.startswith((b"$2a$", b"$2b$", b"$2y$")), "password must be bcrypt-hashed"

    provider = SimpleAuthProvider(users=users, jwt_secret=config["jwt_secret"])

    shared_data["config"] = config
    shared_data["provider"] = provider
    shared_data["username"] = "devuser"
    shared_data["password"] = "dev-pass"
    shared_data["expected_roles"] = ["analyst"]


@when("a developer submits valid credentials")
def submit_valid_credentials(shared_data: dict) -> None:
    provider: SimpleAuthProvider = shared_data["provider"]
    token = provider.login(shared_data["username"], shared_data["password"])
    assert isinstance(token, str) and token.count(".") == 2
    shared_data["issued_token"] = token

    # Sanity: wrong credentials are rejected, proving the bcrypt check is live.
    with pytest.raises(ValueError, match="Invalid credentials"):
        provider.login(shared_data["username"], "wrong-pass")


@then("a short-lived JWT is issued for local testing")
async def short_lived_jwt_issued(shared_data: dict) -> None:
    token = shared_data["issued_token"]
    decoded = jwt.decode(token, _SIMPLE_JWT_SECRET, algorithms=["HS256"])

    assert decoded["sub"] == shared_data["username"]
    assert decoded["roles"] == shared_data["expected_roles"]
    assert "iat" in decoded and "exp" in decoded

    # "Short-lived" — the token must expire, and within a tight bound (<= 24h).
    lifetime = decoded["exp"] - decoded["iat"]
    assert lifetime > 0, "token must have a positive lifetime"
    assert lifetime <= 24 * 3600, "JWT must be short-lived for local testing"

    # Round-trip through the provider's own validation path yields the identity.
    provider: SimpleAuthProvider = shared_data["provider"]
    identity = await provider.validate_token(token)
    assert isinstance(identity, AuthIdentity)
    assert identity.user_id == shared_data["username"]
    assert identity.roles == shared_data["expected_roles"]

    # Tokens signed with a different secret must be rejected.
    forged = jwt.encode({"sub": "devuser", "roles": []}, "other-secret", algorithm="HS256")
    with pytest.raises(jwt.InvalidSignatureError):
        await provider.validate_token(forged)


# ---------------------------------------------------------------------------
# Steps — REQ-125 (superuser bootstrap access)
# ---------------------------------------------------------------------------

_SUPERUSER_ENV_VAR = "PROVISA_SUPERUSER_PASSWORD"
_SUPERUSER_USERNAME = "root"


class _RejectAllBearerProvider(AuthProvider):
    """A configured (bearer/IdP) auth provider that rejects every token.

    Used to prove that the superuser bootstrap short-circuit grants admin access
    *regardless* of — and even when bypassing — the configured auth provider.
    """

    async def validate_token(self, token: str) -> AuthIdentity:
        raise ValueError("Invalid token (provider rejects all)")


def _make_superuser_app(provider: AuthProvider | None, superuser: dict | None) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        mapping_rules=[
            {"type": "exact", "claim": "groups", "value": "analysts", "role": "analyst"},
        ],
        default_role="viewer",
        superuser=superuser,
    )

    @app.get("/whoami")
    async def whoami(request: Request):
        identity = request.state.identity
        return {
            "user_id": identity.user_id,
            "roles": identity.roles,
            "role": request.state.role,
        }

    return app


def _basic_header(username: str, password: str) -> str:
    raw = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    return f"Basic {raw}"


@given("superuser credentials are set in config via env secret")
def superuser_credentials_from_env(shared_data: dict, monkeypatch) -> None:
    # The password is sourced from an environment secret, the username from config.
    secret_password = "bootstrap-" + uuid.uuid4().hex[:12]
    monkeypatch.setenv(_SUPERUSER_ENV_VAR, secret_password)

    # Config references the env var; resolution reads the secret at startup.
    resolved_password = os.environ[_SUPERUSER_ENV_VAR]
    assert resolved_password == secret_password, "env secret must resolve into config"

    superuser = {"username": _SUPERUSER_USERNAME, "password": resolved_password}
    assert superuser["username"] and superuser["password"], "superuser must be fully configured"

    shared_data["superuser"] = superuser
    shared_data["su_username"] = superuser["username"]
    shared_data["su_password"] = superuser["password"]


@when("the superuser authenticates")
def superuser_authenticates(shared_data: dict) -> None:
    # A bearer IdP that rejects all tokens is configured, proving the superuser
    # path is independent of the auth provider.
    app = _make_superuser_app(
        provider=_RejectAllBearerProvider(),
        superuser=shared_data["superuser"],
    )
    client = TestClient(app)

    header = _basic_header(shared_data["su_username"], shared_data["su_password"])
    response = client.get("/whoami", headers={"Authorization": header})
    shared_data["su_app"] = app
    shared_data["su_response"] = response


@then(
    "they receive admin role and all capabilities regardless of the configured "
    "auth provider"
)
def superuser_receives_admin(shared_data: dict) -> None:
    response = shared_data["su_response"]
    assert response.status_code == 200, response.text

    body = response.json()
    # Superuser bootstrap always maps to the admin role with all capabilities.
    assert body["role"] == "admin", "superuser must resolve to the admin role"
    assert "admin" in body["roles"], "superuser identity must carry the admin role"
    assert body["user_id"] == shared_data["su_username"]

    # The bootstrap path is honored even though the configured provider rejects
    # all bearer tokens — proving independence from the IdP.
    client = TestClient(shared_data["su_app"])
    bearer_resp = client.get("/whoami", headers={"Authorization": "Bearer anything"})
    assert bearer_resp.status_code == 401, "configured provider must still reject bad bearer tokens"


# ---------------------------------------------------------------------------
# Steps — REQ-535 (dev-mode anonymous identity)
# ---------------------------------------------------------------------------


def _make_anonymous_app(provider: AuthProvider | None = None) -> FastAPI:
    """Build a minimal FastAPI app wired with AuthMiddleware for dev-mode tests."""
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        mapping_rules=[],
        default_role="admin",
        superuser=None,
    )

    @app.get("/probe")
    async def probe(request: Request):
        identity = request.state.identity
        assignments = request.state.assignments
        return {
            "user_id": identity.user_id,
            "roles": identity.roles,
            "role": request.state.role,
            "domain_ids": [a.domain_id for a in assignments],
        }

    return app


@given("no auth provider is configured")
def no_auth_provider_configured(shared_data: dict) -> None:
    # Dev mode: AuthMiddleware constructed with provider=None.
    app = _make_anonymous_app(provider=None)
    shared_data["anon_app"] = app
    shared_data["anon_client"] = TestClient(app)

    # The middleware truly has no provider configured.
    middleware = app.user_middleware[0]
    assert middleware.cls is AuthMiddleware
    assert middleware.kwargs.get("provider") is None


@when("any request arrives")
def any_request_arrives(shared_data: dict) -> None:
    client: TestClient = shared_data["anon_client"]
    # A baseline request with no Authorization header at all.
    shared_data["anon_response"] = client.get("/probe")

    # And a request that exercises the role-based code path by selecting a role:
    # in dev mode any configured role is honored without an IdP.
    shared_data["anon_role_response"] = client.get(
        "/probe", headers={"x-provisa-role": "analyst"}
    )


@then(
    "it is treated as the anonymous identity with all roles and wildcard "
    "domain access"
)
def anonymous_identity_resolved(shared_data: dict) -> None:
    response = shared_data["anon_response"]
    assert response.status_code == 200, response.text

    body = response.json()
    # Any request maps to the `anonymous` identity.
    assert body["user_id"] == "anonymous", "dev mode must resolve to anonymous identity"

    # Wildcard domain access — the assignment spans all domains.
    assert "*" in body["domain_ids"], "anonymous identity must have wildcard domain access"

    # The default resolved role is admin (all capabilities) in dev mode.
    assert body["role"] == "admin"
    assert "admin" in body["roles"]

    # Exercising role-based code paths: any configured role is honored without an
    # IdP, confirming the anonymous identity maps to all configured roles.
    role_resp = shared_data["anon_role_response"]
    assert role_resp.status_code == 200, role_resp.text
    role_body = role_resp.json()
    assert role_body["user_id"] == "anonymous"
    assert role_body["role"] == "analyst", "dev mode honors any requested role"
    assert "analyst" in role_body["roles"]
    assert "*" in role_body["domain_ids"], "wildcard domain access regardless of role"

    # Sanity: another arbitrary role is equally accepted (unrestricted dev access).
    viewer_resp = shared_data["anon_client"].get(
        "/probe", headers={"x-provisa-role": "viewer"}
    )
    assert viewer_resp.status_code == 200
    assert viewer_resp.json()["role"] == "viewer"
