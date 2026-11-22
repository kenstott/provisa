# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1c9a47-2b86-4f03-9e75-a81c3f6d0b24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A personal access token's scopes are the roles it may use (REQ-1263, REQ-2076).

A token carries a non-empty list of role names. Each must be a role its issuer holds when it is
issued; naming one that is not held is refused and recorded as a security event. On every
transport the token acts only as a scope its owner still holds: it never reaches another role the
owner holds, and a scope the owner has lost is dead. Both where roles are read from the sign-in's
claims and where Provisa holds the assignments."""

# Requirements: REQ-1263, REQ-2076

from __future__ import annotations

import asyncio
import io
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import insert, select

from provisa.auth import held_assignments
from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider, RoleAssignment
from provisa.auth.pat import PersonalAccessTokenStore
from provisa.auth.wiring import build_auth_provider
from provisa.core import schema_admin
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import orgs, personal_access_tokens
from provisa.security import role_probe

_OWNER = "ada"
_ORG = "default"
_SOURCES = ("claims", "provisa")


class _Refused(Exception):
    """A transport refused the call, however it says so on its wire."""


@pytest.fixture
def plane(tmp_path) -> Database:
    """The platform control plane: where tokens are kept."""
    db = Database(create_engine_from_url(f"sqlite+pysqlite:///{tmp_path}/admin.db"), name="pat")
    with db.engine.begin() as conn:
        schema_admin.metadata.create_all(conn)

    async def _seed() -> None:
        async with db.acquire() as conn:
            await conn.execute_core(insert(orgs).values(id=_ORG, name=_ORG))

    asyncio.run(_seed())
    return db


@pytest.fixture
def holdings(monkeypatch) -> dict[str, list[str]]:
    """The roles each user holds now, whichever place the deployment reads them from: Provisa's
    assignments, or what the user's identity provider last sent."""
    held: dict[str, list[str]] = {_OWNER: ["analyst", "org_admin"]}

    async def _assigned(db_pool, identity, default_assignments):  # noqa: ARG001  # the reader's
        return [RoleAssignment(r, "*") for r in held.get(identity.user_id, [])]

    async def _mirrored(db_pool, user_id):  # noqa: ARG001  # the reader's
        return [RoleAssignment(r, "*") for r in held.get(user_id, [])]

    monkeypatch.setattr(held_assignments, "read_assignments", _assigned)
    monkeypatch.setattr(held_assignments, "mirrored_claims", _mirrored, raising=False)
    return held


@pytest.fixture
def events(monkeypatch, _role_probe_events) -> list[role_probe.Probe]:
    """The security events recorded (REQ-2076), as the unit suite collects them
    (tests/unit/conftest.py); no principal's token access is suspended."""

    async def _not_suspended(identity, org_id, transport):  # noqa: ARG001  # role_probe's
        return False

    monkeypatch.setattr(role_probe, "suspended", _not_suspended)
    return _role_probe_events


@pytest.fixture(autouse=True)
def _bare_runtime(monkeypatch):
    """The HTTP middleware runs with no deployment loaded: no role definitions, attributes or
    derived roles."""

    async def _no_roles(org_id):  # noqa: ARG001  # the reader's
        return {}

    monkeypatch.setattr("provisa.auth.middleware._loaded_derived_roles", lambda: [])
    monkeypatch.setattr("provisa.auth.middleware._loaded_user_attributes", lambda user_id: {})
    monkeypatch.setattr("provisa.auth.middleware._loaded_roles", lambda: {})
    monkeypatch.setattr("provisa.auth.middleware._org_roles", _no_roles)
    # REQ-2155: an owner holding several roles acts as their meta-role, built by id here: no
    # deployment is loaded to build it from.
    monkeypatch.setattr(
        "provisa.security.meta_role.ensure_meta_role",
        lambda _state, members, qualifiers=(): "meta:" + "+".join(sorted({*members, *qualifiers})),
    )


def _auth_config(source: str) -> dict:
    return {
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": "scopes-signing-key-at-least-32-bytes-long",
        "simple": {"users": []},
        "role_mapping": [],
        "default_role": "analyst",
        "assignments_source": source,
        "default_assignments": [],
    }


def _state(plane: Database, source: str) -> SimpleNamespace:
    return SimpleNamespace(
        auth_config=_auth_config(source),
        auth_middleware_active=True,
        multitenancy=False,
        org_id=_ORG,
        admin_db=plane,
        roles={},
        platform_roles={},
        contexts={"analyst": object(), "org_admin": object(), "steward": object()},
        user_attributes={},
        settings_overrides={"derived_roles": []},
        meta_roles={},
        meta_qualifiers={},
    )


def _issue(plane: Database, scopes: list[str]) -> str:
    """A token of the owner's, kept as the store keeps one."""
    secret, _row = asyncio.run(
        PersonalAccessTokenStore(plane).issue(
            user_id=_OWNER, org_id=_ORG, name="ci", env_name="prod", scopes=scopes
        )
    )
    return secret


# --- issue --------------------------------------------------------------------------------------


class _SignedIn(AuthProvider):
    """A provider whose one bearer token signs the owner in, with the role claims given."""

    provider_name = "oauth"

    def __init__(self, claims: list[str]) -> None:
        self._claims = claims

    async def validate_token(self, token: str) -> AuthIdentity:
        if token != "session":
            raise ValueError("unknown token")
        return AuthIdentity(user_id=_OWNER, email=None, display_name=None, roles=list(self._claims))


class _OrgPlane:
    """An org control plane that is bound; the readers that use it are the fixture's."""


def _token_api(plane: Database, source: str, holdings: dict[str, list[str]]) -> TestClient:
    """POST /auth/tokens behind the real auth middleware, signed in as the owner."""
    from provisa.api import pat_router
    from provisa.api.errors import ApiError

    app = FastAPI()
    app.include_router(pat_router.router)

    @app.exception_handler(ApiError)
    async def _coded(request: Request, exc: ApiError):  # noqa: ARG001  # the handler's signature
        from fastapi.responses import JSONResponse

        return JSONResponse({"code": exc.code, "detail": exc.detail}, status_code=exc.status_code)

    app.add_middleware(
        AuthMiddleware,
        provider=_SignedIn(holdings[_OWNER] if source == "claims" else []),
        mapping_rules=[],
        default_role="analyst",
        db_pool=_OrgPlane(),
        assignments_source=source,
        default_org_id=_ORG,
    )
    client = TestClient(app)
    client.headers["Authorization"] = "Bearer session"
    return client


@pytest.fixture
def issuing(monkeypatch, plane):
    """The router's store is the test's control plane, and a token is issued into prod."""
    from provisa.api import pat_router

    async def _prod(request, org_id, named):  # noqa: ARG001  # the router's
        return "prod"

    monkeypatch.setattr(pat_router, "_store", lambda: PersonalAccessTokenStore(plane))
    monkeypatch.setattr(pat_router, "_issued_into", _prod)


def _kept(plane: Database) -> list[dict]:
    async def _rows() -> list[dict]:
        async with plane.acquire() as conn:
            found = await conn.execute_core(select(personal_access_tokens))
            return [dict(r._mapping) for r in found.fetchall()]

    return asyncio.run(_rows())


@pytest.mark.parametrize("source", _SOURCES)
def test_a_token_is_issued_for_roles_its_issuer_holds(source, plane, holdings, events, issuing):
    api = _token_api(plane, source, holdings)
    answered = api.post("/auth/tokens", json={"name": "ci", "scopes": ["analyst"]})
    assert answered.status_code == 200, answered.text
    assert answered.json()["scopes"] == ["analyst"]
    assert "role_id" not in answered.json()
    [row] = _kept(plane)
    assert row["scopes"] == ["analyst"] and "role_id" not in row
    assert events == []


@pytest.mark.parametrize("source", _SOURCES)
def test_a_scope_the_issuer_does_not_hold_is_refused_and_recorded(
    source, plane, holdings, events, issuing
):
    """H1: a user minted a token for platform_admin or org_admin they did not hold."""
    holdings[_OWNER] = ["analyst"]
    api = _token_api(plane, source, holdings)
    answered = api.post(
        "/auth/tokens", json={"name": "ci", "scopes": ["analyst", "platform_admin"]}
    )
    assert answered.status_code == 403, answered.text
    assert answered.json()["code"] == "auth.token_scope_not_held"
    assert _kept(plane) == []
    [event] = events
    assert (event.requested, event.user_id, event.transport, event.endpoint) == (
        "platform_admin",
        _OWNER,
        "http",
        "POST /auth/tokens",
    )
    assert event.held == ("analyst",)


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("body", [{"name": "ci"}, {"name": "ci", "scopes": []}])
def test_a_token_with_no_scopes_is_not_issued(source, body, plane, holdings, events, issuing):
    api = _token_api(plane, source, holdings)
    answered = api.post("/auth/tokens", json=body)
    assert answered.status_code == 400, answered.text
    assert answered.json()["code"] == "auth.token_scopes_required"
    assert _kept(plane) == []


def test_the_single_role_field_is_gone(plane):
    assert "role_id" not in personal_access_tokens.c
    with pytest.raises(TypeError):
        asyncio.run(
            PersonalAccessTokenStore(plane).issue(  # type: ignore[call-arg]  # the field is gone
                user_id=_OWNER, org_id=_ORG, name="ci", env_name="prod", role_id="analyst"
            )
        )
    with pytest.raises(ValueError, match="at least one role"):
        asyncio.run(
            PersonalAccessTokenStore(plane).issue(
                user_id=_OWNER, org_id=_ORG, name="ci", env_name="prod", scopes=[]
            )
        )


# --- use, on every transport --------------------------------------------------------------------
#
# Each adapter presents the token as the protocol carries it, names the role the way the protocol
# names one (or none), and answers the role the call acts as, or raises _Refused.


def _http(state: Any, secret: str, named: str | None) -> str:
    app = FastAPI()
    auth = state.auth_config
    app.add_middleware(
        AuthMiddleware,
        provider=build_auth_provider(auth, admin_pool=state.admin_db),
        mapping_rules=[],
        default_role=auth["default_role"],
        db_pool=_OrgPlane(),
        assignments_source=auth["assignments_source"],
        default_org_id=_ORG,
    )

    @app.get("/acting")
    async def _route(request: Request):
        return {"role": request.state.role, "roles": request.state.identity.roles}

    headers = {"Authorization": f"Bearer {secret}"}
    if named:
        headers["X-Provisa-Role"] = named
    answered = TestClient(app).get("/acting", headers=headers)
    if answered.status_code != 200:
        raise _Refused(f"HTTP {answered.status_code}: {answered.text}")
    return answered.json()["role"]


def _pgwire(state: Any, secret: str, named: str | None) -> str:
    from provisa.core.connection_loop import connection_loop
    from provisa.core.egress import CountingWriter
    from provisa.pgwire.server import ProvisaHandler, ProvisaSession

    errors: list[tuple] = []
    handler = object.__new__(ProvisaHandler)
    handler._meter = CountingWriter(io.BytesIO(), None)
    handler.wfile = handler._meter
    handler._send_pg_error = lambda severity, sqlstate, message: errors.append(
        (severity, sqlstate, message)
    )
    handler.send_authentication_ok = lambda: None
    handler.handle_post_auth = lambda ctx: None  # noqa: ARG005 — signature match
    session = ProvisaSession()
    params = {"user": _OWNER}
    if named:
        params["provisa.role"] = named
    ctx = cast(Any, SimpleNamespace(params=params, session=session))
    with connection_loop(), patch("provisa.pgwire.server.state", state):
        handler.handle_md5_password(ctx, secret.encode("utf-8") + b"\x00")
    if errors:
        raise _Refused(str(errors[0]))
    assert session.role_id is not None
    return session.role_id


def _bolt(state: Any, secret: str, named: str | None) -> str:
    from provisa.bolt.session import BoltSession

    async def _sign_in() -> list[str]:
        identity = await BoltSession._authenticate(state, "bearer", "", secret)
        assert identity is not None
        return await BoltSession._selectable_roles(state, identity)

    try:
        roles = asyncio.run(_sign_in())
    except PermissionError as exc:
        raise _Refused(str(exc)) from exc
    if not roles:
        raise _Refused("bolt: no role to act as")
    if named is None:
        return roles[0]
    # Bolt names a role as a database; the session answers one it does not hold with the event.
    session = BoltSession.__new__(BoltSession)
    session.roles = roles
    try:
        resolved = session._resolve_db(f"provisa_{named}")
    except PermissionError as exc:
        raise _Refused(str(exc)) from exc
    assert resolved is not None
    return resolved[0]


def _bearer_transport(transport: str):
    def _call(state: Any, secret: str, named: str | None) -> str:
        from provisa.auth.bearer import authorize_role_or_probe, validate_bearer_credential

        identity = asyncio.run(validate_bearer_credential(state, secret, transport))
        try:
            return authorize_role_or_probe(
                state, identity, named, transport=transport, endpoint=transport, run=asyncio.run
            )
        except PermissionError as exc:
            raise _Refused(str(exc)) from exc

    return _call


def _flight(state: Any, secret: str, named: str | None) -> str:
    import pyarrow.flight as flight

    from provisa.api.flight.server import ProvisaFlightServer, _validate_flight_credential

    identity = asyncio.run(_validate_flight_credential(state, secret))
    server = ProvisaFlightServer.__new__(ProvisaFlightServer)
    server._state = state
    server._run_on_loop = asyncio.run  # type: ignore[method-assign]  # the RPC's loop, here
    try:
        return server._authorize_role(identity, {"role": named})
    except flight.FlightUnauthorizedError as exc:  # pyright: ignore[reportPrivateImportUsage]
        raise _Refused(str(exc)) from exc


def _mcp(state: Any, secret: str, named: str | None) -> str:
    from provisa.api.mcp import server as mcp_server

    acted: list[str] = []
    refused: list[BaseException] = []

    async def _tool(scope, receive, send):  # noqa: ARG001  # the ASGI signature
        try:
            if named is None:
                role = mcp_server._request_role.get()
                assert role is not None
                acted.append(role)
            else:
                acted.append(mcp_server.named_role(named, state))
        except PermissionError as exc:
            refused.append(exc)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    sent: list[dict] = []

    async def _receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def _send(message):
        sent.append(message)

    app = mcp_server._wrap_role_auth(_tool, state, require_token=True)
    scope = {"type": "http", "headers": [(b"authorization", f"Bearer {secret}".encode())]}
    asyncio.run(app(scope, _receive, _send))
    status = next(m["status"] for m in sent if m["type"] == "http.response.start")
    if status != 200:
        raise _Refused(f"MCP {status}")
    if refused:
        raise _Refused(str(refused[0]))
    return acted[0]


_TRANSPORTS = {
    "http": _http,
    "pgwire": _pgwire,
    "bolt": _bolt,
    "flight": _flight,
    "grpc": _bearer_transport("grpc"),
    "airport": _bearer_transport("airport"),
    "mcp": _mcp,
}
_EVERYWHERE = [
    pytest.param(source, transport, id=f"{transport}-{source}")
    for transport in _TRANSPORTS
    for source in _SOURCES
]


@pytest.fixture(autouse=True)
def _mcp_runtime(monkeypatch):
    """Remote MCP builds the org's runtime before a call; there is none to build here."""

    async def _built(org_id, env=None):  # noqa: ARG001  # the builder's
        return None

    monkeypatch.setattr("provisa.api.app.ensure_serving_runtime", _built)


@pytest.mark.parametrize(("source", "transport"), _EVERYWHERE)
def test_a_token_acts_as_its_scope(source, transport, plane, holdings, events):
    secret = _issue(plane, ["analyst"])
    state = _state(plane, source)
    assert _TRANSPORTS[transport](state, secret, None) == "analyst"
    assert _TRANSPORTS[transport](state, secret, "analyst") == "analyst"
    assert events == []


@pytest.mark.parametrize(("source", "transport"), _EVERYWHERE)
def test_a_token_acts_as_its_scope_whatever_the_default_role_is(
    source, transport, plane, holdings, events
):
    """Naming no role, a token acts as a scope, never as the deployment's default role."""
    secret = _issue(plane, ["org_admin"])
    state = _state(plane, source)
    assert _TRANSPORTS[transport](state, secret, None) == "org_admin"


@pytest.mark.parametrize(("source", "transport"), _EVERYWHERE)
def test_a_token_cannot_act_as_another_role_its_owner_holds(
    source, transport, plane, holdings, events
):
    """M2: a token scoped to analyst reached org_admin, which its owner holds."""
    secret = _issue(plane, ["analyst"])
    state = _state(plane, source)
    with pytest.raises(_Refused):
        _TRANSPORTS[transport](state, secret, "org_admin")
    # Bolt's event is recorded by the session's RUN and MCP's by its tool wrapper, both past the
    # entry point driven here (tests/unit/test_role_probe.py covers them).
    if transport not in ("bolt", "mcp"):
        assert [e.requested for e in events] == ["org_admin"]
        assert events[0].credential == "pat"


@pytest.mark.parametrize(("source", "transport"), _EVERYWHERE)
def test_a_scope_its_owner_no_longer_holds_is_dead(source, transport, plane, holdings, events):
    secret = _issue(plane, ["analyst"])
    state = _state(plane, source)
    holdings[_OWNER] = ["org_admin"]  # the owner lost analyst
    with pytest.raises(_Refused):
        _TRANSPORTS[transport](state, secret, None)
    with pytest.raises(_Refused):
        _TRANSPORTS[transport](state, secret, "analyst")


@pytest.mark.parametrize(("source", "transport"), _EVERYWHERE)
def test_a_token_of_several_scopes_keeps_the_ones_still_held(
    source, transport, plane, holdings, events
):
    secret = _issue(plane, ["analyst", "org_admin"])
    state = _state(plane, source)
    holdings[_OWNER] = ["org_admin"]
    assert _TRANSPORTS[transport](state, secret, None) == "org_admin"
    assert _TRANSPORTS[transport](state, secret, "org_admin") == "org_admin"
    with pytest.raises(_Refused):
        _TRANSPORTS[transport](state, secret, "analyst")


# --- remote MCP reads assignments, for every credential (L4) -------------------------------------


def _mcp_as(state: Any, monkeypatch, identity: AuthIdentity, named: str | None) -> str:
    from provisa.api.mcp import server as mcp_server

    async def _validated(token, state):  # noqa: ARG001  # the validator's
        return identity

    monkeypatch.setattr(mcp_server, "_validate_mcp_token", _validated)
    return _mcp(state, "session", named)


def test_remote_mcp_refuses_a_role_removed_in_the_control_plane(
    plane, holdings, events, monkeypatch
):
    """L4: the role came from the credential's own claims, so a role removed in the control plane
    was still accepted."""
    state = _state(plane, "provisa")
    signed_in = AuthIdentity(
        user_id=_OWNER, email=None, display_name=None, roles=["analyst", "org_admin"]
    )
    assert _mcp_as(state, monkeypatch, signed_in, "org_admin") == "org_admin"
    holdings[_OWNER] = ["analyst"]  # org_admin is removed
    with pytest.raises(_Refused):
        _mcp_as(state, monkeypatch, signed_in, "org_admin")


def test_remote_mcp_refuses_a_default_role_the_caller_does_not_hold(
    plane, holdings, events, monkeypatch
):
    """L4: a caller acted as, and could name, the deployment's default role without holding it."""
    state = _state(plane, "provisa")
    holdings[_OWNER] = ["org_admin"]  # the default role is analyst; the caller does not hold it
    signed_in = AuthIdentity(user_id=_OWNER, email=None, display_name=None, roles=[])
    assert _mcp_as(state, monkeypatch, signed_in, None) == "org_admin"
    with pytest.raises(_Refused):
        _mcp_as(state, monkeypatch, signed_in, "analyst")
