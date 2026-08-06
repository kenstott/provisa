# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2f0a61-58d4-4b93-9f10-1d6b4a8e2c33
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cross-surface auth conformance (REQ-124, REQ-890, REQ-1263).

Every client-facing surface authenticates through the same provider contract, so the same
credential must work on all of them wherever the protocol can carry it, and the same bad
credential must be refused on all of them. One matrix asserts that directly: surface × credential
presentation, driven through each surface's real validation entry point.

The matrix is the gate. A surface added without an entry in ``_SURFACES`` fails
:func:`test_every_authenticating_surface_is_in_the_matrix`, so "all surfaces support all methods"
cannot quietly become "all but the newest one".
"""

from __future__ import annotations

import asyncio
import base64
import threading
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable, cast
from unittest.mock import patch

import bcrypt
import jwt
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity
from provisa.auth.wiring import build_auth_provider

_JWT_SECRET = "conformance-signing-key-at-least-32-bytes"
_PASSWORD = "s3cret"
_USERNAME = "alice"
_PAT = "provisa_pat_conformance"
_PAT_USER = "pat-user"

# A stand-in for the platform control plane. Its only job is to be non-None so
# ``build_auth_provider`` attaches a PAT store; the store itself is faked below.
_ADMIN_POOL = object()


class _Rejected(Exception):
    """A surface refused the credential — however that surface says so on its wire."""


class _FakePatStore:
    """The PAT store without a database: one issued token, everything else unknown."""

    def __init__(self, pool) -> None:
        del pool

    async def validate(self, token: str) -> AuthIdentity:
        if token != _PAT:
            raise ValueError("unknown personal access token")
        return AuthIdentity(
            user_id=_PAT_USER,
            email=None,
            display_name="PAT holder",
            roles=["steward"],
            raw_claims={"sub": _PAT_USER, "roles": ["steward"]},
        )


def _auth_config() -> dict:
    """A ``simple`` provider: it accepts a password AND a token, so one config drives both
    presentations without an issuer to reach over the network."""
    return {
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": _JWT_SECRET,
        "simple": {
            "users": [
                {
                    "username": _USERNAME,
                    "password_hash": bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode(),
                    "roles": ["steward"],
                }
            ]
        },
        "role_mapping": [],
        "default_role": "analyst",
    }


def _state() -> SimpleNamespace:
    return SimpleNamespace(
        auth_config=_auth_config(),
        auth_middleware_active=True,
        multitenancy=False,
        admin_db=_ADMIN_POOL,
        roles={},
        contexts={"analyst": object(), "steward": object()},
    )


@pytest.fixture(autouse=True)
def fake_pat_store():
    """Every surface builds its own provider, so the store is faked where it is constructed."""
    with patch("provisa.auth.pat.PersonalAccessTokenStore", _FakePatStore):
        yield


@pytest.fixture
def pgwire_loop():
    """A running main loop for pgwire's auth path to submit validators to (it authenticates on a
    socketserver worker thread and drives every validator on the main loop)."""
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


# --- credentials --------------------------------------------------------------------------------


def _provider_token() -> str:
    provider = build_auth_provider(_auth_config(), admin_pool=_ADMIN_POOL)
    return provider.login(_USERNAME, _PASSWORD)  # type: ignore[attr-defined]


def _tampered_token() -> str:
    """A token whose signature no longer matches its payload.

    The edit is to the first signature character, not the last: a 32-byte HMAC encodes to 43
    base64url characters, so the final one carries only four significant bits and three of its
    sixty-four values decode to the byte its neighbours do. Flipping it leaves a token that still
    verifies, and the surface under test rightly accepts it.
    """
    head, _, signature = _provider_token().rpartition(".")
    return f"{head}.{'A' if signature[0] != 'A' else 'B'}{signature[1:]}"


@dataclass(frozen=True)
class Credential:
    """One credential presentation: what it is, and what the surface must decide about it."""

    kind: str
    scheme: str  # the presentation the protocol carries it under
    principal: str
    good: Callable[[], str]
    bad: Callable[[], str]
    user_id: str


_PASSWORD_CRED = Credential(
    kind="password",
    scheme="basic",
    principal=_USERNAME,
    good=lambda: _PASSWORD,
    bad=lambda: "wrong-password",
    user_id=_USERNAME,
)
_TOKEN_CRED = Credential(
    kind="provider-token",
    scheme="bearer",
    principal=_USERNAME,
    good=_provider_token,
    bad=_tampered_token,
    user_id=_USERNAME,
)
_PAT_CRED = Credential(
    kind="personal-access-token",
    scheme="bearer",
    principal=_USERNAME,
    good=lambda: _PAT,
    bad=lambda: "provisa_pat_revoked",
    user_id=_PAT_USER,
)


# --- surface adapters ---------------------------------------------------------------------------
#
# Each returns the authenticated user_id, or raises _Rejected. Nothing is stubbed inside a surface:
# the adapter presents a credential the way the protocol does and reads back what the surface
# decided.


def _http(cred: Credential, secret: str) -> str:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=build_auth_provider(_auth_config(), admin_pool=_ADMIN_POOL),
        mapping_rules=[],
        default_role="analyst",
    )

    @app.get("/conformance")
    async def _route(request: Request):
        return {"user_id": request.state.identity.user_id}

    if cred.scheme == "basic":
        header = "Basic " + base64.b64encode(f"{cred.principal}:{secret}".encode()).decode()
    else:
        header = f"Bearer {secret}"
    resp = TestClient(app).get("/conformance", headers={"Authorization": header})
    if resp.status_code != 200:
        raise _Rejected(f"HTTP {resp.status_code}")
    return resp.json()["user_id"]


def _bolt(cred: Credential, secret: str) -> str:
    from provisa.bolt.session import BoltSession

    identity = asyncio.run(
        BoltSession._authenticate(_state(), cred.scheme, cred.principal, secret)
    )
    if identity is None:
        raise _Rejected("bolt refused the credential")
    return identity.user_id


def _pgwire(cred: Credential, secret: str) -> str:
    """pgwire carries a username and one cleartext secret; what the secret IS picks the
    presentation, so the adapter simply hands it over as the startup packet does."""
    from provisa.pgwire.server import ProvisaHandler

    errors: list[tuple] = []
    handler = object.__new__(ProvisaHandler)
    handler._send_pg_error = lambda severity, sqlstate, message: errors.append(
        (severity, sqlstate, message)
    )
    handler.send_authentication_ok = lambda: None
    handler.handle_post_auth = lambda ctx: None  # noqa: ARG005 — signature match

    session = SimpleNamespace()
    ctx = cast(Any, SimpleNamespace(params={"user": cred.principal}, session=session))
    with patch("provisa.pgwire.server.state", _state()):
        handler.handle_md5_password(ctx, secret.encode("utf-8") + b"\x00")
    if errors:
        raise _Rejected(str(errors[0]))
    # pgwire admits a ROLE, not an identity — the identity never reaches the session. The role is
    # the mapped default here, which is what proves the credential validated.
    return cred.user_id if session.role_id == "analyst" else "unmapped"


def _mcp(cred: Credential, secret: str) -> str:
    from provisa.api.mcp.server import _validate_mcp_token

    del cred
    try:
        identity = asyncio.run(_validate_mcp_token(secret, _state()))
    except (ValueError, PermissionError, jwt.PyJWTError) as exc:
        raise _Rejected(str(exc)) from exc
    return identity.user_id


def _flight(cred: Credential, secret: str) -> str:
    from provisa.api.flight.server import _validate_flight_credential

    del cred
    try:
        identity = asyncio.run(_validate_flight_credential(_state(), secret))
    except (ValueError, PermissionError, jwt.PyJWTError) as exc:
        raise _Rejected(str(exc)) from exc
    return identity.user_id


def _grpc(cred: Credential, secret: str) -> str:
    from provisa.grpc.auth import validate_grpc_credential

    del cred
    try:
        identity = asyncio.run(validate_grpc_credential(_state(), secret))
    except (ValueError, PermissionError, jwt.PyJWTError) as exc:
        raise _Rejected(str(exc)) from exc
    return identity.user_id


@dataclass(frozen=True)
class Surface:
    name: str
    authenticate: Callable[[Credential, str], str]
    credentials: tuple[Credential, ...]
    needs_pgwire_loop: bool = False


# A bearer JWT is absent from pgwire's row on purpose: the startup packet has one secret field and
# no scheme, so under a non-OIDC provider that field is a password. An OIDC deployment turns the
# same field into a bearer token — covered by tests/steps/steps_pgwire_pluggable_auth.py.
_SURFACES = (
    Surface("http", _http, (_PASSWORD_CRED, _TOKEN_CRED, _PAT_CRED)),
    Surface("bolt", _bolt, (_PASSWORD_CRED, _TOKEN_CRED, _PAT_CRED)),
    Surface("pgwire", _pgwire, (_PASSWORD_CRED, _PAT_CRED), needs_pgwire_loop=True),
    Surface("mcp", _mcp, (_TOKEN_CRED, _PAT_CRED)),
    Surface("flight", _flight, (_TOKEN_CRED, _PAT_CRED)),
    Surface("grpc", _grpc, (_TOKEN_CRED, _PAT_CRED)),
)

_CASES = [
    pytest.param(surface, cred, id=f"{surface.name}-{cred.kind}")
    for surface in _SURFACES
    for cred in surface.credentials
]


@pytest.fixture
def maybe_pgwire_loop(request):
    """pgwire is the one surface that authenticates off the main loop; the rest need no loop."""
    surface = request.getfixturevalue("surface") if "surface" in request.fixturenames else None
    if surface is not None and surface.needs_pgwire_loop:
        yield request.getfixturevalue("pgwire_loop")
    else:
        yield None


@pytest.mark.parametrize(("surface", "credential"), _CASES)
def test_a_surface_accepts_every_credential_its_protocol_can_carry(
    surface, credential, maybe_pgwire_loop
):
    del maybe_pgwire_loop

    assert surface.authenticate(credential, credential.good()) == credential.user_id


@pytest.mark.parametrize(("surface", "credential"), _CASES)
def test_a_surface_refuses_a_bad_credential_of_every_kind(surface, credential, maybe_pgwire_loop):
    del maybe_pgwire_loop

    with pytest.raises(_Rejected):
        surface.authenticate(credential, credential.bad())


@pytest.mark.parametrize("surface", _SURFACES, ids=lambda s: s.name)
def test_the_personal_access_token_works_on_every_surface(surface, maybe_pgwire_loop):
    """REQ-1263: one credential a user can issue for themselves, good on every protocol."""
    del maybe_pgwire_loop

    assert surface.authenticate(_PAT_CRED, _PAT) == _PAT_USER


def test_every_authenticating_surface_is_in_the_matrix():
    """The gate: a surface that validates credentials must be conformance-tested.

    ``validator_for_scheme`` is how a non-HTTP surface picks its validator, so every module
    calling it is a client-facing surface — and every one of them must appear above. HTTP selects
    its validator inside AuthMiddleware and is listed separately.
    """
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[2] / "provisa"
    callers = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*.py")
        if re.search(r"\bvalidator_for_scheme\(", path.read_text(encoding="utf-8"))
        and path.name != "models.py"
    }

    covered = {
        "grpc/auth.py": "grpc",
        "bolt/session.py": "bolt",
        "api/flight/server.py": "flight",
        "api/mcp/server.py": "mcp",
        "pgwire/server.py": "pgwire",
    }
    assert callers == set(covered), (
        f"surfaces validating credentials but absent from the conformance matrix: "
        f"{sorted(callers - set(covered))}"
    )
    assert {s.name for s in _SURFACES} == set(covered.values()) | {"http"}
