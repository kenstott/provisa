# Copyright (c) 2026 Kenneth Stott
# Canary: 9e2b7d46-1a83-4c5f-b6d0-4f7a3c8e1b92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The airport Flight service authenticates its callers (REQ-1263, REQ-273, REQ-1120).

The call's ``authorization: Bearer`` header used to BE the role: whatever name it carried was the
role the call read and wrote as. With authentication on it is a credential — validated like every
other transport's — and the role comes from the validated identity; a role the identity does not
hold is refused. A deployment with no auth provider keeps the bearer-names-the-role behaviour.
"""

# Requirements: REQ-1263, REQ-273, REQ-1120

from __future__ import annotations

import types

import pyarrow.flight as flight
import pytest

from provisa.api.airport.server import ProvisaAirportServer
from provisa.auth.models import AuthIdentity

_AUTH_CONFIG = {"provider": "oidc", "default_role": "seller", "role_mapping": []}
_ROLES = ("seller", "hr_reader", "org_admin")


class _Call:
    """A call's context: the headers it arrived with, as the server's middleware keeps them."""

    def __init__(self, bearer: str | None = None, role: str | None = None):
        headers: dict[str, list[str]] = {}
        if bearer is not None:
            headers["authorization"] = [f"Bearer {bearer}"]
        if role is not None:
            headers["x-provisa-role"] = [role]
        self._headers = headers

    def get_middleware(self, key: str):
        assert key == "headers"
        return types.SimpleNamespace(headers=self._headers)


def _identity(roles: list[str]) -> AuthIdentity:
    return AuthIdentity(
        user_id="u-1",
        email=None,
        display_name=None,
        roles=roles,
        raw_claims={},
        active_org_id=None,
    )


def _server(monkeypatch, *, auth: bool, middleware_only: bool = False) -> ProvisaAirportServer:
    srv = ProvisaAirportServer.__new__(ProvisaAirportServer)
    srv._state = types.SimpleNamespace(
        auth_config=_AUTH_CONFIG if auth and not middleware_only else None,
        auth_middleware_active=auth,
        admin_db=None,
        contexts={r: object() for r in _ROLES},
        roles={r: {"id": r} for r in _ROLES},
    )
    validated: list[str] = []

    async def _validate(state, token):  # noqa: ARG001  # signature mirrors the real validator
        validated.append(token)
        if token == "sam-token":
            return _identity(["seller"])
        if token == "both-token":
            return _identity(["seller", "hr_reader"])
        raise ValueError("no such credential")

    monkeypatch.setattr("provisa.grpc.auth.validate_grpc_credential", _validate)
    monkeypatch.setattr(
        "provisa.core.connection_loop.run_on_connection_loop",
        lambda coro, **kwargs: __import__("asyncio").run(coro),
    )
    monkeypatch.setattr(
        "provisa.security.meta_role.ensure_meta_role",
        lambda state, members: "meta:" + "+".join(sorted(set(members))),
    )
    srv._validated = validated  # type: ignore[attr-defined]
    return srv


# --- authentication on -----------------------------------------------------------------------------


def test_a_role_name_is_not_a_credential(monkeypatch):
    srv = _server(monkeypatch, auth=True)
    for named in _ROLES:
        with pytest.raises(flight.FlightUnauthenticatedError, match="credential rejected"):
            srv._role(_Call(bearer=named))
    assert srv._validated == list(_ROLES), "each bearer went to the credential validator"


def test_a_call_without_a_credential_is_refused_whatever_the_default_role(monkeypatch):
    monkeypatch.setenv("PROVISA_AIRPORT_DEFAULT_ROLE", "org_admin")
    srv = _server(monkeypatch, auth=True)
    with pytest.raises(flight.FlightUnauthenticatedError, match="credential is required"):
        srv._role(_Call())
    with pytest.raises(flight.FlightUnauthenticatedError, match="credential is required"):
        srv._role(_Call(role="org_admin"))


def test_the_role_comes_from_the_validated_identity(monkeypatch):
    srv = _server(monkeypatch, auth=True)
    assert srv._role(_Call(bearer="sam-token")) == "seller"
    assert srv._role(_Call(bearer="both-token", role="hr_reader")) == "hr_reader"
    # A set of held roles acts as its meta-role; it has no surface in this fixture, which is
    # the ordinary answer for a role with none.
    with pytest.raises(flight.FlightServerError, match="meta:hr_reader\\+seller.*no data surface"):
        srv._role(_Call(bearer="both-token", role="seller,hr_reader"))


def test_a_role_the_identity_does_not_hold_is_refused_by_name(monkeypatch):
    srv = _server(monkeypatch, auth=True)
    for requested in ("org_admin", "seller,org_admin", "meta:hr_reader+seller"):
        with pytest.raises(flight.FlightUnauthenticatedError) as refused:
            srv._role(_Call(bearer="sam-token", role=requested))
        assert "org_admin" in str(refused.value) or "is not a role" in str(refused.value)


def test_a_live_auth_layer_with_no_config_fails_closed(monkeypatch):
    srv = _server(monkeypatch, auth=True, middleware_only=True)
    with pytest.raises(flight.FlightServerError, match="auth_config not configured"):
        srv._role(_Call(bearer="org_admin"))


# --- no auth provider: as before ------------------------------------------------------------------


def test_with_no_auth_provider_the_bearer_names_the_role(monkeypatch):
    monkeypatch.delenv("PROVISA_AIRPORT_DEFAULT_ROLE", raising=False)
    srv = _server(monkeypatch, auth=False)
    assert srv._role(_Call(bearer="hr_reader")) == "hr_reader"
    assert srv._validated == [], "there is no credential to validate"
    with pytest.raises(flight.FlightServerError, match="unknown role"):
        srv._role(_Call(bearer="ghost"))
    with pytest.raises(flight.FlightServerError, match="no role"):
        srv._role(_Call())
    monkeypatch.setenv("PROVISA_AIRPORT_DEFAULT_ROLE", "seller")
    assert srv._role(_Call()) == "seller"
