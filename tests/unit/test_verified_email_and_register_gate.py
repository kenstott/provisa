# Copyright (c) 2026 Kenneth Stott
# Canary: 5c0b7f4e-2d1a-4f6b-9a3e-7b8c1d2e3f40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two rules of the account paths.

REQ-1261: an identity is matched by its email address -- to an invitation, an organization's
email rule, a grant waiting on an address -- only when its provider verified the address. A local
(basic provider) account's address is whatever was typed when it was made, so it is never
verified; an OIDC, Keycloak or Firebase token's is verified when the token's ``email_verified``
claim says so.

REQ-124: a signed-in caller of ``/auth/register`` is creating an account for someone else, which
needs user_management. Being signed in alone creates nothing.
"""

from __future__ import annotations

import types

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from provisa.api.auth_router import router as auth_router
from provisa.api.errors import ApiError
from provisa.auth.models import AuthIdentity, verified_email


def _identity(email: str | None, *, email_verified: bool) -> AuthIdentity:
    return AuthIdentity(
        user_id="u",
        email=email,
        email_verified=email_verified,
        display_name=None,
        roles=[],
    )


# -- the address an identity is matched by ----------------------------------------------------


async def test_an_identity_says_nothing_of_its_address_unless_its_provider_does():
    identity = AuthIdentity(user_id="u", email="a@b.test", display_name=None, roles=[])
    assert identity.email_verified is False


async def test_only_a_verified_address_is_matched():
    assert verified_email(_identity("a@b.test", email_verified=True)) == "a@b.test"
    assert verified_email(_identity("a@b.test", email_verified=False)) is None
    assert verified_email(_identity(None, email_verified=True)) is None
    assert verified_email(None) is None


async def test_a_local_accounts_typed_address_is_not_verified():
    from provisa.auth.providers.basic import BasicAuthProvider

    identity = BasicAuthProvider._identity(  # pyright: ignore[reportPrivateUsage]
        {
            "id": "local-1",
            "username": "mallory",
            "email": "mallory@victim-corp.test",
            "display_name": None,
            # An attribute an administrator records is not the provider's word either.
            "attributes": {"email_verified": True},
        }
    )
    assert identity.email == "mallory@victim-corp.test"
    assert identity.email_verified is False
    assert verified_email(identity) is None


class _Key:
    key = "k"


class _JwksClient:
    def get_signing_key_from_jwt(self, token):
        return _Key()


def _oidc(monkeypatch, decoded: dict):
    from provisa.auth.providers import oauth as oa_mod
    from provisa.auth.providers.oauth import OAuthProvider

    provider = OAuthProvider(
        discovery_url="https://idp.example/.well-known/openid-configuration", client_id="provisa"
    )
    monkeypatch.setattr(provider, "_get_jwks_client", lambda: _JwksClient())
    monkeypatch.setattr(provider, "_discovery", lambda: {"issuer": "https://idp.example"})
    monkeypatch.setattr(oa_mod.jwt, "decode", lambda *a, **k: decoded)
    return provider


def _keycloak(monkeypatch, decoded: dict):
    from provisa.auth.providers import keycloak as kc_mod
    from provisa.auth.providers.keycloak import KeycloakAuthProvider

    provider = KeycloakAuthProvider(server_url="https://kc.example", realm="r", client_id="api")
    monkeypatch.setattr(provider, "_get_jwks_client", lambda: _JwksClient())
    monkeypatch.setattr(kc_mod.jwt, "decode", lambda *a, **k: decoded)
    return provider


def _firebase(monkeypatch, decoded: dict):
    from provisa.auth.providers import firebase as fb_mod
    from provisa.auth.providers.firebase import FirebaseAuthProvider

    class _Auth:
        def verify_id_token(self, token):
            return decoded

    # Bypass __init__ (which needs firebase-admin); validate_token only uses the module-level
    # firebase_auth handle.
    provider = FirebaseAuthProvider.__new__(FirebaseAuthProvider)
    provider._entitlements_claim = None  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(fb_mod, "firebase_auth", _Auth())
    return provider


_PROVIDERS = {
    "oidc": (_oidc, "sub"),
    "keycloak": (_keycloak, "sub"),
    "firebase": (_firebase, "uid"),
}


@pytest.mark.parametrize("name", sorted(_PROVIDERS))
@pytest.mark.parametrize(
    ("claim", "matched"),
    [
        ({"email_verified": True}, "carol@corp.test"),
        ({"email_verified": False}, None),
        # No claim is no statement: the issuer has not said the address is the person's.
        ({}, None),
        # Only the issuer's boolean counts, not a string that reads like one.
        ({"email_verified": "true"}, None),
    ],
)
async def test_a_tokens_address_is_matched_only_when_its_issuer_verified_it(
    monkeypatch, name, claim, matched
):
    build, subject = _PROVIDERS[name]
    provider = build(monkeypatch, {subject: "carol", "email": "carol@corp.test", **claim})
    identity = await provider.validate_token("tok")
    assert identity.email == "carol@corp.test"
    assert verified_email(identity) == matched


# -- a signed-in register needs user_management ---------------------------------------------


_ROLE_RIGHTS = {
    "org_admin": {"capabilities": ["user_management"]},
    "analyst": {"capabilities": ["query_development"]},
}


class _ReachedTheControlPlane(Exception):
    pass


class _ControlPlane:
    """The platform control plane, which a refused register neither reads nor writes."""

    def acquire(self):
        raise _ReachedTheControlPlane


def _app(role: str) -> FastAPI:
    app = FastAPI()

    @app.exception_handler(ApiError)
    async def _api_error(_request, exc: ApiError):
        return JSONResponse(
            status_code=exc.status_code, content={"detail": exc.detail, "code": exc.code}
        )

    @app.middleware("http")
    async def _signed_in(request, call_next):
        request.state.identity = AuthIdentity(
            user_id="existing-user",
            email=None,
            display_name=None,
            roles=[role],
            acting_roles=[role],
        )
        return await call_next(request)

    app.include_router(auth_router)
    return app


@pytest.fixture
def basic_deployment(monkeypatch):
    from provisa.api.app import state

    monkeypatch.setattr(
        state, "config", types.SimpleNamespace(auth={"provider": "basic"}), raising=False
    )
    monkeypatch.setattr(state, "roles", _ROLE_RIGHTS, raising=False)
    monkeypatch.setattr(state, "admin_db", _ControlPlane(), raising=False)
    monkeypatch.setattr(state, "multitenancy", False, raising=False)
    return state


@pytest.mark.parametrize(
    "body",
    [
        {"username": "second", "password": "pw-pw-pw"},
        {"username": "second", "password": "pw-pw-pw", "email": "second@victim-corp.test"},
        {"username": "second", "password": "pw-pw-pw", "invite_token": "any-token"},
    ],
)
def test_a_signed_in_caller_without_user_management_is_refused_before_anything_is_written(
    basic_deployment, body
):
    with TestClient(_app("analyst")) as client:
        response = client.post("/auth/register", json=body)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == "auth.missing_capability"


def test_a_signed_in_user_manager_passes_the_gate(basic_deployment):
    """With user_management the register goes on to create the account (the account itself is
    created against a real control plane in tests/integration/test_register_env_redemption_failure.py)."""
    with TestClient(_app("org_admin")) as client, pytest.raises(_ReachedTheControlPlane):
        client.post("/auth/register", json={"username": "second", "password": "pw-pw-pw"})


def test_on_a_deployment_of_many_orgs_a_register_naming_no_org_holds_the_right_nowhere(
    basic_deployment, monkeypatch
):
    """A right is held in an org. A signed-in register that names none acts in none, so even a
    caller who administers some org is refused there, as any missing right is."""
    from provisa.core.request_context import current_org

    monkeypatch.setattr(basic_deployment, "multitenancy", True)
    # The unit suite binds an org for every test; this request names none.
    unbound = current_org.set(None)
    try:
        with TestClient(_app("org_admin")) as client:
            response = client.post(
                "/auth/register", json={"username": "second", "password": "pw-pw-pw"}
            )
    finally:
        current_org.reset(unbound)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == "auth.missing_capability"
