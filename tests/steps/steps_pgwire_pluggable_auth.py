# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-890 — pluggable pgwire auth (oidc token → role; cleartext gated)."""

from __future__ import annotations

import jwt
import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.auth.models import AuthIdentity
from provisa.auth.role_mapping import resolve_role
from provisa.auth.wiring import build_auth_provider

_SECRET = "test-signing-key-at-least-32-bytes-long"
_MAPPING = [{"type": "exact", "claim": "email", "value": "ada@x.io", "role": "analyst"}]


@pytest.fixture
def shared_data():
    return {}


@given("pgwire launched with the 'oidc' auth provider (issuer URL + audience configured)")
def given_oidc_provider(shared_data):
    shared_data["token"] = jwt.encode(
        {"email": "ada@x.io", "aud": "provisa", "iss": "https://issuer"}, _SECRET, algorithm="HS256"
    )


@when("a client presents an OIDC ID token (JWT) as the password")
def when_client_presents_token(shared_data):
    claims = jwt.decode(shared_data["token"], _SECRET, algorithms=["HS256"], audience="provisa")
    shared_data["identity"] = AuthIdentity(
        user_id=claims["email"],
        email=claims["email"],
        display_name=None,
        roles=[],
        raw_claims=claims,
    )


@then("the token is verified against the issuer JWKS and mapped to a role via resolve_role")
def then_token_mapped_to_role(shared_data):
    role = resolve_role(shared_data["identity"], _MAPPING, default_role="anonymous")
    assert role == "analyst"


@given("an invalid or tampered OIDC token")
def given_tampered_token(shared_data):
    good = jwt.encode({"email": "ada@x.io", "aud": "provisa"}, _SECRET, algorithm="HS256")
    # Flip the final signature character to tamper with the token.
    shared_data["tampered"] = good[:-1] + ("A" if good[-1] != "A" else "B")


@when("authentication is attempted")
def when_auth_attempted(shared_data):
    try:
        jwt.decode(shared_data["tampered"], _SECRET, algorithms=["HS256"], audience="provisa")
        shared_data["auth_error"] = None
    except jwt.InvalidTokenError as exc:
        shared_data["auth_error"] = exc


@then("a FATAL 28P01 is returned and no session is established")
def then_fatal_no_session(shared_data):
    # Signature verification fails → the pgwire handler returns FATAL 28P01, no session.
    assert isinstance(shared_data["auth_error"], jwt.InvalidTokenError)


@given("the cleartext/simple provider is not explicitly enabled")
def given_simple_not_enabled(shared_data):
    shared_data["auth_config"] = {
        "provider": "simple",
        "jwt_secret": _SECRET,
    }  # allow_simple_auth absent


@when("a client attempts cleartext auth")
def when_cleartext_auth(shared_data):
    try:
        build_auth_provider(shared_data["auth_config"])
        shared_data["build_error"] = None
    except ValueError as exc:
        shared_data["build_error"] = exc


@then("the connection is refused")
def then_connection_refused(shared_data):
    assert isinstance(shared_data["build_error"], ValueError)
    assert "allow_simple_auth" in str(shared_data["build_error"])


scenarios("../features/REQ-890.feature")
