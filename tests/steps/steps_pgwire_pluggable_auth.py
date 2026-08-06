# Copyright (c) 2026 Kenneth Stott
# Canary: 5d99b844-8f75-499f-b38e-77c9adf9c5d1
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-890 — pluggable pgwire auth.

Every step drives ``ProvisaHandler.handle_md5_password`` — the real startup-packet path, through
``_build_provider`` → ``_validate_credential`` → ``_complete_auth`` — and asserts on what the
handler put on the wire. Only the identity provider and the socket are stood in for; the
presentation decision, the role mapping and the FATAL bytes are the code under test.
"""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from typing import Any, cast

import bcrypt
import jwt
import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.auth.models import AuthIdentity

_SECRET = "test-signing-key-at-least-32-bytes-long"


@pytest.fixture
def pgwire_loop():
    """A running main loop for the auth path to submit validators to.

    pgwire authenticates on a socketserver worker thread and drives every validator on the main
    event loop, so exercising the handler needs a real loop running off this thread.
    """
    import provisa.pgwire.server as pg_server

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    previous = pg_server._loop
    pg_server._loop = loop
    try:
        yield loop
    finally:
        pg_server._loop = previous
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()


@pytest.fixture
def shared_data(pgwire_loop):
    del pgwire_loop
    return {"errors": [], "provider": None}


class _Session:
    """A session with no role until the handler grants one — so 'no session' is checkable."""

    org_id = None


class _Ctx:
    def __init__(self, user: str):
        self.params = {"user": user}
        self.session = _Session()


def _handler(shared_data):
    """A ProvisaHandler with the socket replaced, so the wire bytes land in shared_data."""
    from provisa.pgwire.server import ProvisaHandler

    handler = object.__new__(ProvisaHandler)
    errors = shared_data["errors"]
    admitted = shared_data.setdefault("admitted", [])

    handler._send_pg_error = lambda severity, sqlstate, message: errors.append(
        (severity, sqlstate, message)
    )
    handler.send_authentication_ok = lambda: admitted.append(True)
    handler.handle_post_auth = lambda ctx: None  # noqa: ARG005 — signature match
    return handler


def _authenticate(shared_data, *, user: str = "alice") -> None:
    """Run the real startup-packet auth path with the pending config and secret."""
    from unittest.mock import patch

    # The feature is one scenario of six independent attempts; each starts from a clean wire.
    shared_data["errors"] = []
    shared_data["admitted"] = []
    state = SimpleNamespace(
        auth_config=shared_data["auth_config"],
        auth_middleware_active=True,
        multitenancy=False,
        admin_db=None,
    )
    handler, ctx = _handler(shared_data), cast(Any, _Ctx(user))
    shared_data["ctx"] = ctx

    patches = [patch("provisa.pgwire.server.state", state)]
    if shared_data["provider"] is not None:
        patches.append(
            patch("provisa.auth.wiring.build_auth_provider", return_value=shared_data["provider"])
        )
    payload = shared_data["password"].encode("utf-8") + b"\x00"
    try:
        with patches[0]:
            if len(patches) > 1:
                with patches[1]:
                    handler.handle_md5_password(ctx, payload)
            else:
                handler.handle_md5_password(ctx, payload)
        shared_data["raised"] = None
    except RuntimeError as exc:
        shared_data["raised"] = exc


def _simple_config(**overrides) -> dict:
    config = {
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": _SECRET,
        "simple": {
            "users": [
                {
                    "username": "alice",
                    "password_hash": bcrypt.hashpw(b"s3cret", bcrypt.gensalt()).decode(),
                    "roles": ["steward"],
                }
            ]
        },
        "role_mapping": [],
        "default_role": "analyst",
    }
    config.update(overrides)
    return config


class _OidcProvider:
    """Stands in for the issuer: it is the JWKS verification the handler delegates to."""

    auth_scheme = "bearer"

    def __init__(self, *, seen: dict):
        self._seen = seen

    async def validate_token(self, token: str) -> AuthIdentity:
        self._seen["token"] = token
        claims = jwt.decode(token, _SECRET, algorithms=["HS256"], audience="provisa")
        return AuthIdentity(
            user_id=claims["email"],
            email=claims["email"],
            display_name=None,
            roles=[],
            raw_claims=claims,
        )


def _oidc_config(**overrides) -> dict:
    config = {
        "provider": "oidc",
        "oidc": {
            "discovery_url": "https://issuer/.well-known/openid-configuration",
            "client_id": "provisa",
            "audience": "provisa",
        },
        "role_mapping": [{"type": "exact", "claim": "email", "value": "ada@x.io", "role": "analyst"}],
        "default_role": "viewer",
    }
    config.update(overrides)
    return config


# --- oidc token → role ------------------------------------------------------------------------


@given("pgwire launched with the 'oidc' auth provider (issuer URL + audience configured)")
def given_oidc_provider(shared_data):
    shared_data["auth_config"] = _oidc_config()
    shared_data["seen"] = {}
    shared_data["provider"] = _OidcProvider(seen=shared_data["seen"])


@when("a client presents an OIDC ID token (JWT) as the password")
def when_client_presents_token(shared_data):
    shared_data["password"] = jwt.encode(
        {"email": "ada@x.io", "aud": "provisa", "iss": "https://issuer"}, _SECRET, algorithm="HS256"
    )
    _authenticate(shared_data, user="ada")


@then("the token is verified against the issuer JWKS and mapped to a role via resolve_role")
def then_token_mapped_to_role(shared_data):
    # An oidc provider takes the password field as a bearer token — the handler sent it verbatim,
    # not base64 user:password — and the mapping rule, not the startup username, chose the role.
    assert shared_data["seen"]["token"] == shared_data["password"]
    assert shared_data["errors"] == []
    assert shared_data["ctx"].session.role_id == "analyst"


# --- tampered token → FATAL 28P01 -------------------------------------------------------------


@given("an invalid or tampered OIDC token")
def given_tampered_token(shared_data):
    shared_data["auth_config"] = _oidc_config()
    shared_data["provider"] = _OidcProvider(seen={})
    good = jwt.encode({"email": "ada@x.io", "aud": "provisa"}, _SECRET, algorithm="HS256")
    # Flip the final signature character to tamper with the token.
    shared_data["password"] = good[:-1] + ("A" if good[-1] != "A" else "B")


@when("authentication is attempted")
def when_auth_attempted(shared_data):
    _authenticate(shared_data)


@then("a FATAL 28P01 is returned and no session is established")
def then_fatal_no_session(shared_data):
    assert shared_data["errors"] == [
        ("FATAL", "28P01", 'password authentication failed for user "alice"')
    ]
    assert shared_data.get("admitted", []) == []
    assert not hasattr(shared_data["ctx"].session, "role_id")


# --- simple provider not enabled → refused ------------------------------------------------------


@given("the cleartext/simple provider is not explicitly enabled")
def given_simple_not_enabled(shared_data):
    # allow_simple_auth absent: the provider cannot be constructed, so it authenticates nobody.
    shared_data["auth_config"] = {"provider": "simple", "jwt_secret": _SECRET}
    shared_data["provider"] = None


@when("a client attempts cleartext auth")
def when_cleartext_auth(shared_data):
    shared_data["password"] = "s3cret"
    _authenticate(shared_data)


@then("the connection is refused")
def then_connection_refused(shared_data):
    assert len(shared_data["errors"]) == 1
    severity, sqlstate, message = shared_data["errors"][0]
    assert (severity, sqlstate) == ("FATAL", "28P01")
    assert "allow_simple_auth" in message
    assert shared_data.get("admitted", []) == []
    assert not hasattr(shared_data["ctx"].session, "role_id")


# --- ordinary password → basic presentation, role from the identity -----------------------------


@given("any configured auth provider")
def given_any_provider(shared_data):
    # The startup username is 'alice'; the mapping keys off a claim the provider returned, so a
    # role named 'alice' could never be the answer.
    shared_data["auth_config"] = _simple_config(
        role_mapping=[{"type": "contains", "claim": "roles", "value": "steward", "role": "steward"}]
    )
    shared_data["provider"] = None


@when("a client presents an ordinary password in the startup packet")
def when_password_presented(shared_data):
    shared_data["password"] = "s3cret"
    _authenticate(shared_data, user="alice")


@then(
    "it is validated through the provider's basic presentation and the role comes from the "
    "validated identity, not the supplied username"
)
def then_basic_presentation_role_from_identity(shared_data):
    assert shared_data["errors"] == []
    role = shared_data["ctx"].session.role_id
    assert role == "steward"
    assert role != shared_data["ctx"].params["user"]


# --- PAT → bearer presentation ------------------------------------------------------------------


@given("a client presents a personal access token as the password")
def given_pat_presented(shared_data):
    from provisa.auth.pat import TOKEN_PREFIX

    seen: dict = {}

    class _PatProvider:
        auth_scheme = "basic"

        @property
        def token_validators(self):
            return {"basic": self._basic, "bearer": self._bearer}

        async def _basic(self, _token):
            raise AssertionError("a PAT must not be presented as basic")

        async def _bearer(self, token):
            seen["token"] = token
            return AuthIdentity(
                user_id="u-1",
                email=None,
                display_name="Alice",
                roles=["steward"],
                raw_claims={"sub": "u-1"},
            )

    shared_data["auth_config"] = _simple_config()
    shared_data["provider"] = _PatProvider()
    shared_data["seen"] = seen
    shared_data["password"] = TOKEN_PREFIX + "abc123"


@then(
    "the token prefix selects the bearer presentation and the token validates as an ordinary "
    "bearer credential"
)
def then_pat_uses_bearer(shared_data):
    # The basic validator raises if it is reached at all, so arriving here proves the prefix
    # decided the presentation rather than the provider's declared auth_scheme.
    assert shared_data["seen"]["token"] == shared_data["password"]
    assert shared_data["errors"] == []
    assert shared_data["ctx"].session.role_id == "analyst"


# --- no default_role → the connection fails -----------------------------------------------------


@given("auth.default_role is not configured")
def given_no_default_role(shared_data):
    config = _simple_config()
    del config["default_role"]
    shared_data["auth_config"] = config
    shared_data["provider"] = None


@when("an authenticated identity matches no role-mapping rule")
def when_identity_matches_no_rule(shared_data):
    # The credential is good — role_mapping is empty, so nothing but a default could admit it.
    assert shared_data["auth_config"]["role_mapping"] == []
    shared_data["password"] = "s3cret"
    _authenticate(shared_data)


@then("the connection fails rather than landing on an arbitrary role")
def then_connection_fails(shared_data):
    assert isinstance(shared_data["raised"], RuntimeError)
    assert "default_role" in str(shared_data["raised"])
    assert shared_data.get("admitted", []) == []
    assert not hasattr(shared_data["ctx"].session, "role_id")


scenarios("../features/REQ-890.feature")
