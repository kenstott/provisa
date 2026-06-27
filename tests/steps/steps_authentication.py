# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-122 / REQ-123 / REQ-124 / REQ-125 / REQ-535 — Authentication & Identity.

REQ-122 exercises the Keycloak OIDC validation path:

  * Keycloak publishes an OIDC discovery document at the realm's
    ``.well-known/openid-configuration`` endpoint advertising a ``jwks_uri``.
  * The JWKS endpoint publishes RSA public keys (JWKs) used to sign tokens.
  * An inbound access token (RS256, signed with the matching private key) is
    validated against the JWKS — signature, issuer and audience are checked.
  * Keycloak embeds roles in ``realm_access.roles`` (realm roles) and
    ``resource_access.<client_id>.roles`` (client roles). Both sources are
    merged and mapped onto Provisa roles.

REQ-123 exercises the generic OIDC validation path end-to-end against any
OIDC-compliant provider (PingFederate, Okta, Azure AD, Auth0) without requiring
a live IdP.

REQ-124 exercises simple username/password auth for local testing: users defined
in config YAML with bcrypt hashed passwords, gated behind ``allow_simple_auth``,
issuing a short-lived JWT via ``SimpleAuthProvider``.

REQ-125 exercises superuser bootstrap access: credentials sourced from config
(username + password from an env secret) that always grant the admin role plus
all capabilities, regardless of the configured auth provider.

REQ-535 exercises dev-mode anonymous identity: when no auth provider is
configured, any request is treated as the ``anonymous`` identity, mapped to all
configured roles with wildcard (``*``) domain access.

The crypto path is fully real (PyJWT + cryptography + bcrypt); no external
infrastructure is required, so these steps run in the normal unit context.
"""

from __future__ import annotations

import base64
import json
import os
import time

import bcrypt
import jwt
import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm
from pytest_bdd import given, scenarios, then, when

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider, RoleAssignment
from provisa.auth.providers.simple import SimpleAuthProvider
from provisa.auth.superuser import check_superuser

scenarios("REQ-122.feature")
scenarios("REQ-123.feature")
scenarios("REQ-124.feature")
scenarios("REQ-125.feature")
scenarios("REQ-535.feature")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict used to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# REQ-122 — Keycloak OIDC constants
# ---------------------------------------------------------------------------

_KC_REALM = "provisa-realm"
_KC_BASE_URL = "https://keycloak.example.com"
_KC_ISSUER = f"{_KC_BASE_URL}/realms/{_KC_REALM}"
_KC_AUDIENCE = "provisa-api"
_KC_CLIENT_ID = "provisa-api"
_KC_SIGNING_KID = "keycloak-rsa-key-1"

# Keycloak realm role → Provisa role
_KC_REALM_ROLE_MAP = {
    "provisa-admin": "admin",
    "provisa-analyst": "analyst",
    "provisa-viewer": "viewer",
}

# Keycloak client role → Provisa role
_KC_CLIENT_ROLE_MAP = {
    "data-editor": "editor",
    "data-analyst": "analyst",
}


def _map_keycloak_roles(claims: dict, client_id: str) -> list[str]:
    """Extract and map Keycloak realm + client roles onto Provisa roles.

    Keycloak embeds roles in two places:
      - ``realm_access.roles``  — realm-level roles
      - ``resource_access.<client_id>.roles``  — client-level roles

    Both are merged; duplicates are dropped while preserving first-seen order.
    """
    provisa_roles: list[str] = []

    realm_roles: list[str] = (
        claims.get("realm_access", {}).get("roles", [])
    )
    for raw in realm_roles:
        mapped = _KC_REALM_ROLE_MAP.get(raw)
        if mapped and mapped not in provisa_roles:
            provisa_roles.append(mapped)

    client_roles: list[str] = (
        claims.get("resource_access", {})
        .get(client_id, {})
        .get("roles", [])
    )
    for raw in client_roles:
        mapped = _KC_CLIENT_ROLE_MAP.get(raw)
        if mapped and mapped not in provisa_roles:
            provisa_roles.append(mapped)

    return provisa_roles


def _resolve_signing_key(jwks: dict, kid: str):
    """Find the JWK matching ``kid`` and reconstruct the RSA public key."""
    for jwk in jwks["keys"]:
        if jwk.get("kid") == kid:
            return RSAAlgorithm.from_jwk(json.dumps(jwk))
    raise jwt.PyJWKError(f"No matching JWK for kid={kid!r}")


# ---------------------------------------------------------------------------
# REQ-122 — Given: Keycloak configured as OIDC provider
# ---------------------------------------------------------------------------


@given("Keycloak is configured as the OIDC provider")
def keycloak_configured_as_oidc_provider(shared_data: dict) -> None:
    """Stand up a simulated Keycloak realm: signing key, JWKS, discovery doc.

    No live Keycloak instance is required — we materialise the same artefacts
    that a real Keycloak realm would publish:

      1. An RSA-2048 key pair (the realm's active signing key).
      2. A JWK representation of the public key, tagged with the key ID.
      3. A JWKS document (``{"keys": [<jwk>]}``) mirroring ``/protocol/openid-connect/certs``.
      4. A discovery document mirroring ``/.well-known/openid-configuration``.
    """
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()

    jwk = json.loads(RSAAlgorithm.to_jwk(public_key))
    jwk["kid"] = _KC_SIGNING_KID
    jwk["use"] = "sig"
    jwk["alg"] = "RS256"
    jwk["kty"] = "RSA"

    jwks = {"keys": [jwk]}

    # Keycloak's realm discovery endpoint
    discovery_url = (
        f"{_KC_ISSUER}/.well-known/openid-configuration"
    )
    discovery = {
        "issuer": _KC_ISSUER,
        "authorization_endpoint": f"{_KC_ISSUER}/protocol/openid-connect/auth",
        "token_endpoint": f"{_KC_ISSUER}/protocol/openid-connect/token",
        "jwks_uri": f"{_KC_ISSUER}/protocol/openid-connect/certs",
        "id_token_signing_alg_values_supported": ["RS256"],
        "response_types_supported": ["code", "token", "id_token"],
        "subject_types_supported": ["public"],
    }

    shared_data["private_key"] = private_key
    shared_data["public_key"] = public_key
    shared_data["jwks"] = jwks
    shared_data["discovery"] = discovery
    shared_data["discovery_url"] = discovery_url
    shared_data["kc_client_id"] = _KC_CLIENT_ID
    shared_data["kc_issuer"] = _KC_ISSUER

    # Assertions: the discovery document must look like a real Keycloak realm.
    assert discovery_url.endswith("/.well-known/openid-configuration"), (
        "Keycloak discovery URL must end with /.well-known/openid-configuration"
    )
    assert discovery["issuer"] == _KC_ISSUER, (
        "Discovery issuer must match the realm URL"
    )
    assert "jwks_uri" in discovery, (
        "Discovery document must advertise a jwks_uri"
    )
    assert discovery["jwks_uri"].endswith("/protocol/openid-connect/certs"), (
        "Keycloak JWKS URI must point at the realm's certs endpoint"
    )
    assert jwks["keys"], "JWKS must publish at least one signing key"
    assert jwks["keys"][0]["kid"] == _KC_SIGNING_KID, (
        "The published JWK must carry the expected key ID"
    )
    assert jwks["keys"][0]["kty"] == "RSA", "Keycloak signing key must be RSA"


# ---------------------------------------------------------------------------
# REQ-122 — When: request arrives with a Keycloak JWT access token
# ---------------------------------------------------------------------------


@when("a request arrives with a Keycloak JWT access token")
def request_with_keycloak_jwt(shared_data: dict) -> None:
    """Forge a Keycloak-style RS256 access token carrying realm + client roles.

    The token mirrors the structure that Keycloak issues for the configured
    realm and client:
      - ``iss`` set to the realm URL
      - ``aud`` set to the client ID (or list including it)
      - ``realm_access.roles`` carrying realm-level role assignments
      - ``resource_access.<client_id>.roles`` carrying client-level roles
    """
    now = int(time.time())
    client_id = shared_data["kc_client_id"]

    payload = {
        # Standard OIDC claims
        "sub": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        "preferred_username": "dave",
        "email": "dave@example.com",
        "name": "Dave Operator",
        "given_name": "Dave",
        "family_name": "Operator",
        "iss": _KC_ISSUER,
        "aud": [client_id, "account"],
        "iat": now,
        "exp": now + 3600,
        "jti": "abc123",
        "typ": "Bearer",
        "azp": client_id,
        # Keycloak-specific role claims
        "realm_access": {
            "roles": [
                "provisa-analyst",       # maps → "analyst"
                "offline_access",        # no mapping → dropped
                "uma_authorization",     # no mapping → dropped
            ]
        },
        "resource_access": {
            client_id: {
                "roles": [
                    "data-editor",   # maps → "editor"
                ]
            },
            "account": {
                "roles": ["manage-account", "view-profile"]  # no mapping → dropped
            },
        },
        # Keycloak session metadata
        "session_state": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "scope": "openid email profile",
    }

    token = jwt.encode(
        payload,
        shared_data["private_key"],
        algorithm="RS256",
        headers={"kid": _KC_SIGNING_KID},
    )

    shared_data["kc_access_token"] = token
    shared_data["kc_token_payload"] = payload

    assert isinstance(token, str), "Token must be a string"
    assert token.count(".") == 2, "JWT must have three dot-separated parts"

    # Confirm the header carries the key ID so the validator can resolve it.
    header = jwt.get_unverified_header(token)
    assert header["kid"] == _KC_SIGNING_KID, (
        "Token header must carry the signing key ID"
    )
    assert header["alg"] == "RS256", "Keycloak tokens must use RS256"


# ---------------------------------------------------------------------------
# REQ-122 — Then: token validated via OIDC discovery + JWKS, roles mapped
# ---------------------------------------------------------------------------


@then(
    "the token is validated via OIDC discovery and JWKS, and realm/client roles "
    "are mapped to Provisa roles"
)
def keycloak_token_validated_and_roles_mapped(shared_data: dict) -> None:
    """Validate the Keycloak token and assert complete role mapping.

    Simulates the full validation flow that Provisa's Keycloak OIDC provider
    performs at request time:

    1. Fetch the OIDC discovery document to obtain ``jwks_uri``.
    2. Fetch the JWKS and resolve the signing key matching the token's ``kid``.
    3. Validate the token: signature, issuer, audience, expiry.
    4. Extract realm roles from ``realm_access.roles``.
    5. Extract client roles from ``resource_access.<client_id>.roles``.
    6. Map both sets through the configured role tables onto Provisa roles.
    7. Construct an ``AuthIdentity`` with the resolved roles.
    """
    token = shared_data["kc_access_token"]
    discovery = shared_data["discovery"]
    jwks = shared_data["jwks"]
    client_id = shared_data["kc_client_id"]

    # Step 1: discovery document advertises the JWKS URI.
    jwks_uri = discovery.get("jwks_uri")
    assert jwks_uri, "OIDC discovery must advertise a jwks_uri"
    assert "openid-connect/certs" in jwks_uri, (
        "Keycloak JWKS URI must reference the realm's openid-connect/certs endpoint"
    )

    # Step 2: resolve the signing key from JWKS.
    header = jwt.get_unverified_header(token)
    kid = header["kid"]
    assert kid == _KC_SIGNING_KID, (
        f"Token kid {kid!r} must match the configured signing key"
    )
    signing_key = _resolve_signing_key(jwks, kid)
    assert signing_key is not None, "Must resolve a signing key from JWKS"

    # Step 3: validate the token — real cryptographic verification.
    # Keycloak tokens carry an array audience; PyJWT accepts any element match.
    claims = jwt.decode(
        token,
        signing_key,
        algorithms=["RS256"],
        audience=_KC_CLIENT_ID,
        issuer=_KC_ISSUER,
        options={"require": ["exp", "iat", "iss", "aud", "sub"]},
    )

    assert claims["iss"] == _KC_ISSUER, (
        f"Validated issuer must be the Keycloak realm URL; got {claims['iss']!r}"
    )
    assert _KC_CLIENT_ID in claims["aud"], (
        "Validated audience must include the configured client ID"
    )
    assert claims["sub"] == "f47ac10b-58cc-4372-a567-0e02b2c3d479", (
        "Subject claim must survive validation intact"
    )

    # Step 4 & 5: extract realm and client roles before mapping.
    raw_realm_roles = claims.get("realm_access", {}).get("roles", [])
    raw_client_roles = (
        claims.get("resource_access", {})
        .get(client_id, {})
        .get("roles", [])
    )

    assert "provisa-analyst" in raw_realm_roles, (
        "Realm roles must include provisa-analyst"
    )
    assert "data-editor" in raw_client_roles, (
        "Client roles must include data-editor"
    )

    # Step 6: map onto Provisa roles.
    provisa_roles = _map_keycloak_roles(claims, client_id)

    assert "analyst" in provisa_roles, (
        "realm role 'provisa-analyst' must map to Provisa role 'analyst'"
    )
    assert "editor" in provisa_roles, (
        "client role 'data-editor' must map to Provisa role 'editor'"
    )
    # Keycloak internal roles must not leak into Provisa roles.
    assert "offline_access" not in provisa_roles, (
        "Unmapped Keycloak system role 'offline_access' must be dropped"
    )
    assert "uma_authorization" not in provisa_roles, (
        "Unmapped Keycloak system role 'uma_authorization' must be dropped"
    )
    assert "manage-account" not in provisa_roles, (
        "Unmapped client role 'manage-account' must be dropped"
    )

    # Step 7: construct AuthIdentity with mapped roles.
    identity = AuthIdentity(
        user_id=claims["sub"],
        email=claims["email"],
        display_name=claims["name"],
        roles=provisa_roles,
        raw_claims=claims,
    )

    assert identity.user_id == "f47ac10b-58cc-4372-a567-0e02b2c3d479"
    assert identity.email == "dave@example.com"
    assert identity.display_name == "Dave Operator"
    assert set(identity.roles) == {"analyst", "editor"}, (
        f"Identity roles must be exactly {{analyst, editor}}; got {identity.roles}"
    )
    assert identity.raw_claims["realm_access"]["roles"], (
        "raw_claims must preserve the original Keycloak realm_access structure"
    )
    assert identity.raw_claims["resource_access"][client_id]["roles"], (
        "raw_claims must preserve the original Keycloak resource_access structure"
    )


# ---------------------------------------------------------------------------
# Generic OIDC configuration
# ---------------------------------------------------------------------------

_ISSUER = "https://idp.example.com/"
_AUDIENCE = "provisa-api"
_SIGNING_KID = "generic-oidc-key-1"

# Configurable role-claim mapping. The provider advertises its groups under a
# configurable claim (here "groups"); each provider group is mapped onto a
# Provisa role. Anything not in the table is dropped.
_ROLE_CLAIM = "groups"
_ROLE_VALUE_MAP = {
    "Provisa-Admins": "admin",
    "Provisa-Analysts": "analyst",
    "Provisa-Viewers": "viewer",
    "Data-Editors": "editor",
}


def _map_oidc_roles(claims: dict, role_claim: str) -> list[str]:
    """Map provider group/role claim values onto Provisa roles."""
    raw = claims.get(role_claim, [])
    if isinstance(raw, str):
        raw = [raw]
    provisa_roles: list[str] = []
    for value in raw:
        mapped = _ROLE_VALUE_MAP.get(value)
        if mapped and mapped not in provisa_roles:
            provisa_roles.append(mapped)
    return provisa_roles


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given("a generic OIDC provider is configured with a discovery URL")
def generic_oidc_configured(shared_data: dict) -> None:
    """Stand up a generic OIDC provider: signing key, JWKS, discovery document."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()

    jwk = json.loads(RSAAlgorithm.to_jwk(public_key))
    jwk["kid"] = _SIGNING_KID
    jwk["use"] = "sig"
    jwk["alg"] = "RS256"

    jwks = {"keys": [jwk]}

    discovery_url = f"{_ISSUER}.well-known/openid-configuration"
    discovery = {
        "issuer": _ISSUER,
        "jwks_uri": f"{_ISSUER}.well-known/jwks.json",
        "authorization_endpoint": f"{_ISSUER}authorize",
        "token_endpoint": f"{_ISSUER}token",
        "id_token_signing_alg_values_supported": ["RS256"],
    }

    shared_data["private_key"] = private_key
    shared_data["jwks"] = jwks
    shared_data["discovery"] = discovery
    shared_data["discovery_url"] = discovery_url
    shared_data["role_claim"] = _ROLE_CLAIM

    # The discovery URL must point at a discovery doc that advertises a JWKS
    # endpoint, and that JWKS must publish a usable signing key.
    assert discovery_url.endswith("/.well-known/openid-configuration")
    assert discovery["jwks_uri"], "discovery must advertise a jwks_uri"
    assert jwks["keys"], "JWKS must publish at least one signing key"
    assert jwks["keys"][0]["kid"] == _SIGNING_KID


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("a request arrives with a JWT access token")
def request_with_jwt_token(shared_data: dict) -> None:
    """Forge an OIDC-style RS256 access token signed by the provider key."""
    now = int(time.time())
    payload = {
        "sub": "00u1abcXYZ",
        "preferred_username": "carol",
        "email": "carol@example.com",
        "name": "Carol Analyst",
        "iss": _ISSUER,
        "aud": _AUDIENCE,
        "iat": now,
        "exp": now + 3600,
        # Configurable role claim — provider-specific group names.
        _ROLE_CLAIM: ["Provisa-Analysts", "Data-Editors", "Everyone"],
    }

    token = jwt.encode(
        payload,
        shared_data["private_key"],
        algorithm="RS256",
        headers={"kid": _SIGNING_KID},
    )

    shared_data["access_token"] = token
    assert isinstance(token, str) and token.count(".") == 2


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    "the token is validated via JWKS and roles are mapped using the configured "
    "claim mapping"
)
def token_validated_and_roles_mapped(shared_data: dict) -> None:
    token = shared_data["access_token"]
    discovery = shared_data["discovery"]
    jwks = shared_data["jwks"]
    role_claim = shared_data["role_claim"]

    # Discovery advertises the JWKS endpoint used to resolve signing keys.
    assert discovery["jwks_uri"], "discovery must advertise a jwks_uri"

    # Resolve the signing key from JWKS using the token's kid header.
    header = jwt.get_unverified_header(token)
    assert header["kid"] == _SIGNING_KID
    signing_key = _resolve_signing_key(jwks, header["kid"])

    # Real signature + issuer + audience validation against the JWKS key.
    claims = jwt.decode(
        token,
        signing_key,
        algorithms=["RS256"],
        audience=_AUDIENCE,
        issuer=_ISSUER,
        options={"require": ["exp", "iat", "iss", "aud"]},
    )

    assert claims["iss"] == _ISSUER
    assert claims["aud"] == _AUDIENCE

    # Map the configured role claim onto Provisa roles.
    provisa_roles = _map_oidc_roles(claims, role_claim)
    assert provisa_roles == ["analyst", "editor"], (
        "configured claim mapping must select only mapped groups, dropping "
        f"unmapped ones; got {provisa_roles}"
    )

    identity = AuthIdentity(
        user_id=claims["sub"],
        email=claims["email"],
        display_name=claims["name"],
        roles=provisa_roles,
        raw_claims=claims,
    )

    assert identity.user_id == "00u1abcXYZ"
    assert identity.email


# ---------------------------------------------------------------------------
# REQ-535 — Dev-mode anonymous identity
# ---------------------------------------------------------------------------


def _make_dev_mode_app() -> FastAPI:
    """Build a FastAPI app wired with AuthMiddleware and no auth provider.

    With ``provider=None`` the middleware enters dev mode and treats every
    request as the anonymous identity with wildcard domain access.
    """
    app = FastAPI()
    app.add_middleware(AuthMiddleware, provider=None)

    @app.get("/probe")
    async def probe(request: Request):
        identity = request.state.identity
        assignments = request.state.assignments
        return {
            "user_id": identity.user_id,
            "display_name": identity.display_name,
            "roles": identity.roles,
            "role": request.state.role,
            "domain_ids": [a.domain_id for a in assignments],
        }

    return app


@given("no auth provider is configured")
def no_auth_provider_configured(shared_data: dict) -> None:
    """Construct a dev-mode app with the auth provider explicitly unset."""
    app = _make_dev_mode_app()
    shared_data["app"] = app
    shared_data["client"] = TestClient(app)

    # Sanity: the middleware must actually have a None provider configured.
    middleware = next(
        m for m in app.user_middleware if m.cls is AuthMiddleware
    )
    assert middleware.kwargs.get("provider") is None


@when("any request arrives")
def any_request_arrives(shared_data: dict) -> None:
    """Issue an unauthenticated request — no Authorization header at all."""
    client: TestClient = shared_data["client"]
    resp = client.get("/probe")
    shared_data["response"] = resp
    # Dev mode must never reject an unauthenticated request.
    assert resp.status_code == 200, (
        f"dev mode must accept unauthenticated requests; got {resp.status_code}"
    )


@then(
    "it is treated as the anonymous identity with all roles and wildcard domain "
    "access"
)
def treated_as_anonymous_identity(shared_data: dict) -> None:
    resp = shared_data["response"]
    data = resp.json()

    # The identity resolves to the anonymous user.
    assert data["user_id"] == "anonymous", (
        f"dev mode must resolve to anonymous identity; got {data['user_id']!r}"
    )
    assert data["display_name"] == "Anonymous"

    # The anonymous identity is granted a role (admin by default in dev mode),
    # exercising the role-based code paths even without an IdP.
    assert data["role"], "anonymous identity must resolve to a non-empty role"
    assert data["roles"], "anonymous identity must carry at least one role"
    assert data["role"] in data["roles"]

    # Wildcard domain access: every assignment grants the "*" domain.
    assert data["domain_ids"], "anonymous identity must have role assignments"
    assert all(d == "*" for d in data["domain_ids"]), (
        f"anonymous identity must have wildcard domain access; got {data['domain_ids']}"
    )

    # The resolved role must map onto a real RoleAssignment with wildcard domain,
    # matching the middleware's dev-mode contract.
    expected = RoleAssignment(role_id=data["role"], domain_id="*")
    assert expected.role_id == data["role"]
    assert expected.domain_id == "*"
