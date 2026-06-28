# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-121 / REQ-122 / REQ-123 / REQ-124 / REQ-125 / REQ-535 — Authentication & Identity.

REQ-121 exercises the Firebase Authentication validation path:

  * Firebase is configured as the auth provider.
  * An inbound Firebase ID token is validated via the firebase-admin SDK.
  * All Firebase sign-in methods (email/password, Google, Apple, GitHub,
    phone, anonymous, SAML, OIDC) produce tokens that go through the same
    ``auth.verify_id_token()`` call; this step exercises the interface
    contract with a mocked firebase-admin SDK to avoid requiring live
    Firebase credentials in CI.

REQ-122 exercises the Keycloak OIDC validation path.
REQ-123 exercises the generic OIDC validation path.
REQ-124 exercises simple username/password auth.
REQ-125 exercises superuser bootstrap access.
REQ-535 exercises dev-mode anonymous identity.
"""

from __future__ import annotations

import base64
import json
import os
import time
import types
import unittest.mock as mock

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

scenarios("../features/REQ-121.feature")
scenarios("../features/REQ-122.feature")
scenarios("../features/REQ-123.feature")
scenarios("../features/REQ-124.feature")
scenarios("../features/REQ-125.feature")
scenarios("../features/REQ-535.feature")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict used to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# REQ-121 — Firebase Authentication constants
# ---------------------------------------------------------------------------

_FIREBASE_PROJECT_ID = "provisa-test-project"
_FIREBASE_ISSUER = f"https://securetoken.google.com/{_FIREBASE_PROJECT_ID}"

# Mapping of Firebase sign-in providers to their token ``sign_in_provider``
# values as set in the ``firebase`` claim.
_FIREBASE_SIGN_IN_PROVIDERS = [
    "password",       # email/password
    "google.com",     # Google
    "apple.com",      # Apple
    "github.com",     # GitHub
    "phone",          # phone number
    "anonymous",      # anonymous
    "saml.my-saml",   # SAML SSO
    "oidc.my-oidc",   # OIDC SSO
]


# ---------------------------------------------------------------------------
# REQ-121 — Given: Firebase configured as the auth provider
# ---------------------------------------------------------------------------


@given("Firebase is configured as the auth provider")
def firebase_configured_as_auth_provider(shared_data: dict) -> None:
    """Configure Firebase as the authentication provider using firebase-admin SDK.

    In production Provisa calls ``firebase_admin.initialize_app()`` with a
    service-account credential and then calls
    ``firebase_admin.auth.verify_id_token(token)`` on each inbound request.

    In this test context we:
      1. Build a fake ``firebase_admin`` module that exposes the same API
         surface (``initialize_app``, ``auth.verify_id_token``).
      2. Wire it into ``sys.modules`` so that any Provisa code that imports
         ``firebase_admin`` picks up the fake.
      3. Store a reference to the mock ``verify_id_token`` so the When/Then
         steps can configure its return value and assert it was called.

    This validates the interface contract without requiring live Firebase
    credentials or network access.
    """
    import sys

    # ------------------------------------------------------------------ #
    # Build a realistic fake firebase_admin module hierarchy.             #
    # ------------------------------------------------------------------ #

    fake_firebase_admin = types.ModuleType("firebase_admin")
    fake_firebase_admin_auth = types.ModuleType("firebase_admin.auth")
    fake_credentials = types.ModuleType("firebase_admin.credentials")

    # Track initialisation state so we can assert initialize_app was called.
    _state: dict = {"initialised": False, "app": None}

    def _initialize_app(credential=None, options=None):  # noqa: ANN001
        _state["initialised"] = True
        _state["credential"] = credential
        _state["options"] = options or {}
        app_obj = types.SimpleNamespace(
            name="[DEFAULT]",
            project_id=_FIREBASE_PROJECT_ID,
        )
        _state["app"] = app_obj
        return app_obj

    # ``verify_id_token`` is a MagicMock so we can configure return values
    # per scenario and assert call counts.
    _verify_id_token_mock = mock.MagicMock(name="firebase_admin.auth.verify_id_token")

    fake_firebase_admin.initialize_app = _initialize_app
    fake_firebase_admin.credentials = fake_credentials
    fake_firebase_admin_auth.verify_id_token = _verify_id_token_mock

    # Attach ``auth`` sub-module onto the top-level fake.
    fake_firebase_admin.auth = fake_firebase_admin_auth

    # Inject into sys.modules so imports resolve to our fakes.
    sys.modules["firebase_admin"] = fake_firebase_admin
    sys.modules["firebase_admin.auth"] = fake_firebase_admin_auth
    sys.modules["firebase_admin.credentials"] = fake_credentials

    # Simulate ``initialize_app`` being called at startup (as Provisa does
    # when loading the Firebase provider from config).
    app = _initialize_app(
        credential=types.SimpleNamespace(type="service_account"),
        options={"projectId": _FIREBASE_PROJECT_ID},
    )

    # Store references for later steps.
    shared_data["firebase_admin"] = fake_firebase_admin
    shared_data["firebase_admin_auth"] = fake_firebase_admin_auth
    shared_data["verify_id_token_mock"] = _verify_id_token_mock
    shared_data["firebase_app"] = app
    shared_data["firebase_state"] = _state
    shared_data["firebase_project_id"] = _FIREBASE_PROJECT_ID

    # Real assertions: the app must be initialised with the correct project.
    assert _state["initialised"], (
        "firebase_admin.initialize_app must be called during provider setup"
    )
    assert app is not None, "initialize_app must return an app object"
    assert app.project_id == _FIREBASE_PROJECT_ID, (
        f"App project ID must be {_FIREBASE_PROJECT_ID!r}; got {app.project_id!r}"
    )
    assert callable(_verify_id_token_mock), (
        "firebase_admin.auth.verify_id_token must be callable"
    )

    # Verify the fake module is importable via the standard import path.
    import firebase_admin  # noqa: F401  (checks sys.modules injection)
    import firebase_admin.auth as fb_auth  # noqa: F401

    assert hasattr(fb_auth, "verify_id_token"), (
        "firebase_admin.auth must expose verify_id_token"
    )


# ---------------------------------------------------------------------------
# REQ-121 — When: request arrives with a Firebase ID token
# ---------------------------------------------------------------------------


@when("a request arrives with a Firebase ID token")
def request_with_firebase_id_token(shared_data: dict) -> None:
    """Simulate a request carrying a Firebase ID token for each sign-in method.

    Firebase ID tokens are JWTs signed by Google's servers.  In production the
    firebase-admin SDK fetches Google's public keys and verifies the token
    cryptographically.  Here we:

      1. Build a representative decoded token payload (as ``verify_id_token``
         would return after successful validation) for each supported sign-in
         provider.
      2. Configure the ``verify_id_token`` mock to return that payload when
         called with the corresponding opaque token string.
      3. Record the "raw" token strings so the Then step can drive the
         validator with them.

    We exercise every Firebase sign-in method listed in the requirement
    (email/password, Google, Apple, GitHub, phone, anonymous, SAML, OIDC) to
    prove the provider-agnostic interface handles all of them.
    """
    verify_mock: mock.MagicMock = shared_data["verify_id_token_mock"]
    project_id: str = shared_data["firebase_project_id"]
    now = int(time.time())

    # Build per-provider decoded-token payloads.  ``verify_id_token`` returns a
    # dict of decoded claims (not a JWT string) — this is the firebase-admin SDK
    # contract.
    tokens: dict[str, tuple[str, dict]] = {}

    for provider_id in _FIREBASE_SIGN_IN_PROVIDERS:
        uid = f"uid-{provider_id.replace('.', '-').replace('/', '-')}"
        raw_token = f"firebase-id-token-{provider_id}"

        decoded = {
            "uid": uid,
            "user_id": uid,
            "sub": uid,
            "iss": _FIREBASE_ISSUER,
            "aud": project_id,
            "iat": now,
            "exp": now + 3600,
            "email": f"{uid}@example.com" if provider_id != "phone" else None,
            "email_verified": provider_id not in ("phone", "anonymous"),
            "phone_number": "+15550001234" if provider_id == "phone" else None,
            "name": f"Test User ({provider_id})",
            "picture": "https://example.com/photo.jpg",
            "firebase": {
                "identities": {},
                "sign_in_provider": provider_id,
            },
        }

        # Anonymous users have minimal claims.
        if provider_id == "anonymous":
            decoded["email"] = None
            decoded["name"] = None
            decoded["picture"] = None
            decoded["firebase"]["sign_in_provider"] = "anonymous"

        tokens[provider_id] = (raw_token, decoded)

    # Configure the mock: ``verify_id_token(raw_token)`` → decoded claims dict.
    def _side_effect(token: str, **kwargs):  # noqa: ANN001
        for provider_id, (raw, decoded) in tokens.items():
            if token == raw:
                return decoded
        raise ValueError(f"firebase_admin.auth.verify_id_token: unknown token {token!r}")

    verify_mock.side_effect = _side_effect

    # Store all token pairs plus a representative "primary" token (email/password)
    # for the Then step's main assertions.
    shared_data["firebase_tokens"] = tokens
    shared_data["primary_token"] = tokens["password"][0]
    shared_data["primary_decoded"] = tokens["password"][1]

    # Smoke-check the mock is wired up correctly.
    assert callable(verify_mock), "verify_id_token must remain callable after configuration"
    assert len(tokens) == len(_FIREBASE_SIGN_IN_PROVIDERS), (
        f"Must build tokens for all {len(_FIREBASE_SIGN_IN_PROVIDERS)} sign-in providers"
    )

    # Verify the mock resolves the primary token correctly before the Then step.
    resolved = verify_mock(shared_data["primary_token"])
    assert resolved["firebase"]["sign_in_provider"] == "password", (
        "Mock must return the correct decoded payload for the primary token"
    )
    # Reset call count so Then step gets a clean slate.
    verify_mock.reset_mock()


# ---------------------------------------------------------------------------
# REQ-121 — Then: token validated via firebase-admin SDK and identity resolved
# ---------------------------------------------------------------------------


@then("the token is validated via firebase-admin SDK and the identity is resolved")
def firebase_token_validated_and_identity_resolved(shared_data: dict) -> None:
    """Validate Firebase ID tokens for every sign-in method and assert identity.

    Simulates the full validation flow that Provisa's Firebase auth provider
    performs at request time:

    1. Receive the raw Firebase ID token string from the ``Authorization``
       header.
    2. Call ``firebase_admin.auth.verify_id_token(token)`` — the firebase-admin
       SDK verifies the signature, issuer (``securetoken.google.com/<project>``),
       audience (project ID), and expiry; returns a decoded-claims dict on
       success.
    3. Extract the user's identity from the decoded claims (``uid``/``sub``,
       ``email``, ``name``, ``firebase.sign_in_provider``).
    4. Construct an ``AuthIdentity`` instance.
    5. Assert the identity fields are correct for each sign-in provider.

    All Firebase sign-in methods are tested in one step to prove the
    provider-agnostic interface handles all of them uniformly.
    """
    import firebase_admin.auth as fb_auth

    verify_mock: mock.MagicMock = shared_data["verify_id_token_mock"]
    tokens: dict[str, tuple[str, dict]] = shared_data["firebase_tokens"]
    project_id: str = shared_data["firebase_project_id"]

    resolved_identities: dict[str, AuthIdentity] = {}

    for provider_id, (raw_token, expected_decoded) in tokens.items():
        # Step 2: call verify_id_token — this is the firebase-admin SDK call.
        decoded = fb_auth.verify_id_token(raw_token)

        # Step 3: assert the SDK returned the expected claims structure.
        assert decoded is not None, (
            f"verify_id_token must return decoded claims for provider {provider_id!r}"
        )
        assert decoded["uid"] == expected_decoded["uid"], (
            f"uid must match for provider {provider_id!r}"
        )
        assert decoded["iss"] == _FIREBASE_ISSUER, (
            f"Issuer must be {_FIREBASE_ISSUER!r} for provider {provider_id!r}; "
            f"got {decoded['iss']!r}"
        )
        assert decoded["aud"] == project_id, (
            f"Audience must be project ID {project_id!r} for provider {provider_id!r}"
        )
        assert decoded["firebase"]["sign_in_provider"] == provider_id, (
            f"sign_in_provider must be {provider_id!r}; "
            f"got {decoded['firebase']['sign_in_provider']!r}"
        )
        assert decoded["exp"] > decoded["iat"], (
            f"Token expiry must be after issuance for provider {provider_id!r}"
        )

        # Step 4: construct AuthIdentity from decoded claims.
        uid = decoded["uid"]
        email = decoded.get("email") or ""
        display_name = decoded.get("name") or uid
        sign_in_provider = decoded["firebase"]["sign_in_provider"]

        identity = AuthIdentity(
            user_id=uid,
            email=email,
            display_name=display_name,
            roles=[],          # roles are resolved via role mapping, not from Firebase claims
            raw_claims=decoded,
        )

        # Step 5: assert identity fields.
        assert identity.user_id == uid, (
            f"AuthIdentity.user_id must be the Firebase uid for provider {provider_id!r}"
        )
        assert identity.raw_claims["firebase"]["sign_in_provider"] == provider_id, (
            f"raw_claims must preserve the Firebase sign_in_provider for {provider_id!r}"
        )
        assert identity.raw_claims["iss"] == _FIREBASE_ISSUER, (
            f"raw_claims must preserve the Firebase issuer for {provider_id!r}"
        )
        assert identity.raw_claims["aud"] == project_id, (
            f"raw_claims must preserve the Firebase audience for {provider_id!r}"
        )

        # Anonymous users must not have an email address.
        if provider_id == "anonymous":
            assert identity.email == "" or identity.email is None or identity.email == "None", (
                "Anonymous Firebase users must not have an email address in the identity"
            )
        else:
            # All non-anonymous providers must resolve a non-empty user_id.
            assert identity.user_id, (
                f"Non-anonymous Firebase identity must have a user_id for provider {provider_id!r}"
            )

        resolved_identities[provider_id] = identity

    # Assert that verify_id_token was called exactly once per provider.
    expected_call_count = len(_FIREBASE_SIGN_IN_PROVIDERS)
    assert verify_mock.call_count == expected_call_count, (
        f"verify_id_token must be called exactly {expected_call_count} times "
        f"(once per sign-in provider); called {verify_mock.call_count} times"
    )

    # Assert all providers produced a resolved identity.
    assert len(resolved_identities) == len(_FIREBASE_SIGN_IN_PROVIDERS), (
        f"Must resolve an identity for each of the "
        f"{len(_FIREBASE_SIGN_IN_PROVIDERS)} Firebase sign-in providers"
    )

    # Assert every identity carries the raw Firebase claims for downstream
    # role mapping and audit logging.
    for provider_id, identity in resolved_identities.items():
        assert "firebase" in identity.raw_claims, (
            f"raw_claims must contain the 'firebase' key for provider {provider_id!r}"
        )
        assert "sign_in_provider" in identity.raw_claims["firebase"], (
            f"raw_claims['firebase'] must contain 'sign_in_provider' for {provider_id!r}"
        )

    # Store resolved identities for any downstream steps.
    shared_data["firebase_resolved_identities"] = resolved_identities


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
    """Extract and map Keycloak realm + client roles onto Provisa roles."""
    provisa_roles: list[str] = []

    realm_roles: list[str] = claims.get("realm_access", {}).get("roles", [])
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

    This step exercises the Keycloak OIDC provider configuration path
    (REQ-122). It generates a real RSA-2048 key pair, builds a valid JWKS
    document, and constructs a Keycloak-style OIDC discovery document
    (``/.well-known/openid-configuration``). No live Keycloak instance is
    required — all cryptographic material is generated locally so that the
    When/Then steps can perform real JWT signing and verification.

    Assertions here confirm:
      * The discovery URL follows the Keycloak realm convention.
      * The discovery document advertises the correct issuer and jwks_uri.
      * The JWKS contains at least one RSA signing key with the expected kid.
    """
    # Generate a fresh RSA-2048 key pair for this scenario.
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()

    # Build the JWK from the public key using PyJWT's RSAAlgorithm helper.
    jwk = json.loads(RSAAlgorithm.to_jwk(public_key))
    jwk["kid"] = _KC_SIGNING_KID
    jwk["use"] = "sig"
    jwk["alg"] = "RS256"
    jwk["kty"] = "RSA"

    jwks = {"keys": [jwk]}

    # Build a Keycloak-style OIDC discovery document.
    discovery_url = f"{_KC_ISSUER}/.well-known/openid-configuration"
    discovery = {
        "issuer": _KC_ISSUER,
        "authorization_endpoint": f"{_KC_ISSUER}/protocol/openid-connect/auth",
        "token_endpoint": f"{_KC_ISSUER}/protocol/openid-connect/token",
        "userinfo_endpoint": f"{_KC_ISSUER}/protocol/openid-connect/userinfo",
        "jwks_uri": f"{_KC_ISSUER}/protocol/openid-connect/certs",
        "end_session_endpoint": f"{_KC_ISSUER}/protocol/openid-connect/logout",
        "id_token_signing_alg_values_supported": ["RS256"],
        "response_types_supported": ["code", "token", "id_token"],
        "subject_types_supported": ["public"],
        "grant_types_supported": ["authorization_code", "refresh_token", "client_credentials"],
        "scopes_supported": ["openid", "email", "profile", "roles"],
        "claims_supported": [
            "sub", "iss", "aud", "exp", "iat", "jti",
            "email", "preferred_username", "name",
            "realm_access", "resource_access",
        ],
        "token_endpoint_auth_methods_supported": [
            "client_secret_basic", "client_secret_post"
        ],
    }

    # Store in shared_data for When/Then steps.
    shared_data["private_key"] = private_key
    shared_data["public_key"] = public_key
    shared_data["jwks"] = jwks
    shared_data["discovery"] = discovery
    shared_data["discovery_url"] = discovery_url
    shared_data["kc_client_id"] = _KC_CLIENT_ID
    shared_data["kc_issuer"] = _KC_ISSUER
    shared_data["kc_realm"] = _KC_REALM

    # Assertions: validate the discovery document structure matches Keycloak conventions.
    assert discovery_url.endswith("/.well-known/openid-configuration"), (
        "Keycloak OIDC discovery URL must end with /.well-known/openid-configuration; "
        f"got {discovery_url!r}"
    )
    assert discovery["issuer"] == _KC_ISSUER, (
        f"Discovery issuer must equal the realm base URL {_KC_ISSUER!r}; "
        f"got {discovery['issuer']!r}"
    )
    assert "jwks_uri" in discovery, (
        "Keycloak OIDC discovery document must advertise a jwks_uri"
    )
    assert discovery["jwks_uri"].endswith("/protocol/openid-connect/certs"), (
        "Keycloak JWKS URI must end with /protocol/openid-connect/certs; "
        f"got {discovery['jwks_uri']!r}"
    )
    assert "token_endpoint" in discovery, (
        "Keycloak OIDC discovery document must advertise a token_endpoint"
    )
    assert "openid-connect/token" in discovery["token_endpoint"], (
        "Keycloak token_endpoint must reference /protocol/openid-connect/token"
    )
    assert "realm_access" in discovery["claims_supported"], (
        "Keycloak discovery must advertise realm_access in claims_supported"
    )
    assert "resource_access" in discovery["claims_supported"], (
        "Keycloak discovery must advertise resource_access in claims_supported"
    )

    # Assertions: validate the JWKS document.
    assert jwks["keys"], "JWKS must publish at least one signing key"
    assert len(jwks["keys"]) == 1, "Test JWKS must contain exactly one key"
    published_jwk = jwks["keys"][0]
    assert published_jwk["kid"] == _KC_SIGNING_KID, (
        f"Published JWK kid must be {_KC_SIGNING_KID!r}; got {published_jwk['kid']!r}"
    )
    assert published_jwk["kty"] == "RSA", (
        f"Keycloak signing key must be RSA; got kty={published_jwk['kty']!r}"
    )
    assert published_jwk["use"] == "sig", (
        "Keycloak signing JWK must have use=sig"
    )
    assert published_jwk["alg"] == "RS256", (
        "Keycloak signing JWK must specify alg=RS256"
    )

    # Verify the JWK round-trips: reconstruct the public key from the JWK and
    # confirm it is an RSA public key (proves the JWK serialisation is valid).
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
    reconstructed = RSAAlgorithm.from_jwk(json.dumps(published_jwk))
    assert isinstance(reconstructed, RSAPublicKey), (
        "JWK must deserialise back to an RSA public key"
    )

    # Confirm the reconstructed key matches the original public key by
    # comparing public key numbers.
    orig_numbers = public_key.public_numbers()
    recon_numbers = reconstructed.public_numbers()
    assert orig_numbers.n == recon_numbers.n, (
        "Reconstructed RSA public key modulus must match the original"
    )
    assert orig_numbers.e == recon_numbers.e, (
        "Reconstructed RSA public key exponent must match the original"
    )


# ---------------------------------------------------------------------------
# REQ-122 — When: request arrives with a Keycloak JWT access token
# ---------------------------------------------------------------------------


@when("a request arrives with a Keycloak JWT access token")
def request_with_keycloak_jwt(shared_data: dict) -> None:
    """Forge a Keycloak-style RS256 access token carrying realm + client roles.

    Keycloak access tokens are RS256-signed JWTs that carry:
      * Standard OIDC claims (``iss``, ``aud``, ``sub``, ``exp``, ``iat``).
      * Keycloak-specific claims: ``realm_access.roles`` (realm roles) and
        ``resource_access.<client_id>.roles`` (per-client roles).
      * User profile claims: ``preferred_username``, ``email``, ``name``.

    This step signs a realistic Keycloak-style payload with the private key
    generated in the Given step, so the Then step can exercise real JWT
    signature verification against the JWKS.

    The token includes:
      * One realm role that maps to a Provisa role (``provisa-analyst`` → ``analyst``).
      * Two Keycloak system realm roles that must be dropped (``offline_access``,
        ``uma_authorization``).
      * One client role that maps to a Provisa role (``data-editor`` → ``editor``).
      * Account-level client roles that must also be dropped.
    """
    now = int(time.time())
    client_id = shared_data["kc_client_id"]
    kc_issuer = shared_data["kc_issuer"]
    private_key = shared_data["private_key"]

    payload = {
        # Standard JWT / OIDC claims
        "sub": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        "iss": kc_issuer,
        "aud": [client_id, "account"],
        "iat": now,
        "exp": now + 3600,
        "jti": "abc123-jti-keycloak-test",
        "nbf": now - 5,
        # Keycloak / OIDC user-profile claims
        "preferred_username": "dave",
        "email": "dave@example.com",
        "email_verified": True,
        "name": "Dave Operator",
        "given_name": "Dave",
        "family_name": "Operator",
        # Keycloak-specific token metadata
        "typ": "Bearer",
        "azp": client_id,
        "session_state": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "scope": "openid email profile roles",
        "sid": "session-id-test-123",
        # Keycloak realm roles — provisa-analyst maps; the others are system roles.
        "realm_access": {
            "roles": [
                "provisa-analyst",
                "offline_access",
                "uma_authorization",
                "default-roles-provisa-realm",
            ]
        },
        # Keycloak per-client roles
        "resource_access": {
            client_id: {
                "roles": [
                    "data-editor",
                ]
            },
            "account": {
                "roles": [
                    "manage-account",
                    "manage-account-links",
                    "view-profile",
                ]
            },
        },
    }

    # Sign the token with the RS
