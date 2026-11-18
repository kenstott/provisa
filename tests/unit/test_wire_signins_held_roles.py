# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e1f52-6c94-4d07-a2e5-91f7c0d48b6a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every sign-in, on every transport, acts only as a role its identity holds (REQ-120, amended
2026-11-01; REQ-802), and the cross-org right is judged from what the principal holds, not from
the role claims its credential carries (REQ-1337).

Where Provisa holds the assignments, a provider sign-in (not a personal access token) over
pgwire, Flight SQL, gRPC or Airport that names no role is read against the identity's assignments:
it acts as a held role, never the deployment's default role nobody granted it, and holding none
it is refused by name."""

# Requirements: REQ-120, REQ-802, REQ-1337

from __future__ import annotations

import asyncio
import io
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest

from provisa.auth import held_assignments
from provisa.auth.models import AuthIdentity, AuthProvider, RoleAssignment
from provisa.security import role_probe

_USER = "grace"
_ORG = "default"
_SESSION = "signed-in-session"


class _Refused(Exception):
    """A transport refused the sign-in, however it says so on its wire."""


class _Provider(AuthProvider):
    """An identity provider whose one bearer token signs the user in with the role claims given."""

    provider_name = "oidc"

    def __init__(self, claims: list[str]) -> None:
        self._claims = claims

    async def validate_token(self, token: str) -> AuthIdentity:
        if token != _SESSION:
            raise ValueError("unknown token")
        return AuthIdentity(user_id=_USER, email=None, display_name=None, roles=list(self._claims))


@pytest.fixture
def claims() -> list[str]:
    """The role claims the user's sign-in carries."""
    return []


@pytest.fixture
def reads() -> list[str]:
    """Each read of a user's assignments in the control plane, by user."""
    return []


@pytest.fixture
def holdings(monkeypatch, claims, reads) -> dict[str, list[str]]:
    """The roles the user is assigned in the control plane."""
    held: dict[str, list[str]] = {_USER: []}

    async def _assigned(db_pool, identity, default_assignments):  # noqa: ARG001  # the reader's
        reads.append(identity.user_id)
        return [RoleAssignment(r, "*") for r in held.get(identity.user_id, [])]

    monkeypatch.setattr(held_assignments, "read_assignments", _assigned)
    monkeypatch.setattr(
        "provisa.auth.wiring.build_auth_provider",
        lambda auth_config, admin_pool=None: _Provider(claims),  # noqa: ARG005  # the builder's
    )
    return held


@pytest.fixture(autouse=True)
def _not_suspended(monkeypatch, _role_probe_events):
    async def _no(identity, org_id, transport):  # noqa: ARG001  # role_probe's
        return False

    monkeypatch.setattr(role_probe, "suspended", _no)


def _state(source: str, *, multitenancy: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        auth_config={
            "provider": "oidc",
            "role_mapping": [],
            "default_role": "analyst",
            "assignments_source": source,
            "default_assignments": [],
        },
        auth_middleware_active=True,
        multitenancy=multitenancy,
        org_id=_ORG,
        admin_db=None,
        roles={},
        platform_roles={"platform_admin": {"capabilities": ["cross_org"]}},
        contexts={"analyst": object(), "org_admin": object(), "steward": object()},
        user_attributes={},
        settings_overrides={"derived_roles": []},
        meta_roles={},
        meta_qualifiers={},
    )


# --- the role a provider sign-in acts as, naming none ---------------------------------------------


def _pgwire(state: Any) -> str:
    from provisa.core.connection_loop import connection_loop
    from provisa.core.egress import CountingWriter
    from provisa.pgwire.server import ProvisaHandler, ProvisaSession

    errors: list[tuple] = []
    handler = object.__new__(ProvisaHandler)
    handler._meter = CountingWriter(io.BytesIO(), None)
    handler.wfile = handler._meter
    handler._send_pg_error = lambda severity, sqlstate, message, detail=None: errors.append(  # noqa: ARG005
        (severity, sqlstate, message)
    )
    handler.send_authentication_ok = lambda: None
    handler.handle_post_auth = lambda ctx: None  # noqa: ARG005 — signature match
    session = ProvisaSession()
    ctx = cast(Any, SimpleNamespace(params={"user": _USER}, session=session))
    with connection_loop(), patch("provisa.pgwire.server.state", state):
        handler.handle_md5_password(ctx, _SESSION.encode("utf-8") + b"\x00")
    if errors:
        raise _Refused(str(errors[0]))
    assert session.role_id is not None
    return session.role_id


def _flight(state: Any) -> str:
    import pyarrow.flight as flight

    from provisa.api.flight.server import ProvisaFlightServer, _validate_flight_credential

    identity = asyncio.run(_validate_flight_credential(state, _SESSION))
    server = ProvisaFlightServer.__new__(ProvisaFlightServer)
    server._state = state
    server._run_on_loop = asyncio.run  # type: ignore[method-assign]  # the RPC's loop, here
    try:
        return server._authorize_role(identity, {"role": None})
    except flight.FlightUnauthorizedError as exc:  # pyright: ignore[reportPrivateImportUsage]
        raise _Refused(str(exc)) from exc


def _bearer_transport(transport: str):
    def _call(state: Any) -> str:
        from provisa.auth.bearer import authorize_role_or_probe, validate_bearer_credential

        identity = asyncio.run(validate_bearer_credential(state, _SESSION, transport))
        try:
            return authorize_role_or_probe(
                state, identity, None, transport=transport, endpoint=transport, run=asyncio.run
            )
        except PermissionError as exc:
            raise _Refused(str(exc)) from exc

    return _call


_TRANSPORTS = {
    "pgwire": _pgwire,
    "flight": _flight,
    "grpc": _bearer_transport("grpc"),
    "airport": _bearer_transport("airport"),
}


@pytest.mark.parametrize("transport", _TRANSPORTS)
def test_a_sign_in_naming_no_role_acts_as_a_role_it_holds(transport, holdings):
    """The default role is analyst; the user is assigned org_admin alone. It acted as analyst."""
    holdings[_USER] = ["org_admin"]
    assert _TRANSPORTS[transport](_state("provisa")) == "org_admin"


@pytest.mark.parametrize("transport", _TRANSPORTS)
def test_a_sign_in_keeps_the_default_role_when_it_holds_it(transport, holdings):
    holdings[_USER] = ["steward", "analyst"]
    assert _TRANSPORTS[transport](_state("provisa")) == "analyst"


@pytest.mark.parametrize("transport", _TRANSPORTS)
def test_a_sign_in_holding_no_role_is_refused_by_name(transport, holdings):
    """Assigned nothing, and no default assignments: it acted as the default role analyst."""
    holdings[_USER] = []
    with pytest.raises(_Refused, match=f"'{_USER}' holds no role in this organization"):
        _TRANSPORTS[transport](_state("provisa"))


@pytest.mark.parametrize("transport", _TRANSPORTS)
@pytest.mark.parametrize("claims", [["steward"]])
def test_where_roles_are_claims_a_sign_in_is_answered_from_them_with_nothing_read(
    transport, claims, holdings, reads
):
    """Claims mode is unchanged: the mapped role, and no control-plane read."""
    assert _TRANSPORTS[transport](_state("claims")) == "analyst"
    assert reads == []


def test_the_refusal_is_catalogued():
    refused = held_assignments.NoRoleHeld(_USER)
    assert isinstance(refused, PermissionError)
    assert (refused.code, refused.params) == ("auth.no_role_held", {"user": _USER})


# --- the cross-org right, judged from what is held ------------------------------------------------


@pytest.fixture
def judged(monkeypatch) -> list[bool]:
    """What each transport's org resolution was told about the cross-org right."""
    told: list[bool] = []

    async def _resolve(state, *, can_act_any_org=False, **kw):  # noqa: ARG001  # the resolver's
        told.append(can_act_any_org)
        return _ORG

    async def _built(org_id, env=None):  # noqa: ARG001  # the builder's
        return None

    monkeypatch.setattr("provisa.api.org_resolve.resolve_session_org", _resolve)
    monkeypatch.setattr("provisa.api.app.ensure_serving_runtime", _built)
    return told


def _identity(roles: list[str]) -> AuthIdentity:
    return AuthIdentity(user_id=_USER, email=None, display_name=None, roles=roles)


def _mcp_org(state: Any, identity: AuthIdentity) -> None:
    from provisa.api.mcp.server import _org_for_identity

    asyncio.run(_org_for_identity(identity, state))


def _grpc_org(state: Any, identity: AuthIdentity) -> None:
    from provisa.grpc.auth import principal_org

    asyncio.run(principal_org(state, identity, None))


def _flight_org(state: Any, identity: AuthIdentity) -> None:
    from provisa.api.flight.server import _resolve_identity_org

    asyncio.run(_resolve_identity_org(state, identity, {}))


def _pgwire_org(state: Any, identity: AuthIdentity) -> None:
    from provisa.pgwire.server import _resolve_and_build_org

    asyncio.run(_resolve_and_build_org(state, identity, None))


_ORG_RESOLVERS = {
    "mcp": _mcp_org,
    "grpc": _grpc_org,
    "flight": _flight_org,
    "pgwire": _pgwire_org,
}


@pytest.mark.parametrize("transport", _ORG_RESOLVERS)
def test_a_cross_org_claim_the_principal_does_not_hold_crosses_no_org(transport, holdings, judged):
    """The credential claims platform_admin; the control plane assigns analyst alone."""
    holdings[_USER] = ["analyst"]
    _ORG_RESOLVERS[transport](_state("provisa", multitenancy=True), _identity(["platform_admin"]))
    assert judged == [False]


@pytest.mark.parametrize("transport", _ORG_RESOLVERS)
def test_a_held_cross_org_role_crosses_orgs_whatever_the_credential_claims(
    transport, holdings, judged
):
    holdings[_USER] = ["platform_admin"]
    _ORG_RESOLVERS[transport](_state("provisa", multitenancy=True), _identity([]))
    assert judged == [True]


@pytest.mark.parametrize("transport", _ORG_RESOLVERS)
def test_where_roles_are_claims_the_claims_are_what_is_held(transport, holdings, judged):
    _ORG_RESOLVERS[transport](_state("claims", multitenancy=True), _identity(["platform_admin"]))
    assert judged == [True]
