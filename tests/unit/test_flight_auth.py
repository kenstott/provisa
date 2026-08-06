# Copyright (c) 2026 Kenneth Stott
# Canary: 410873c7-9dcf-44c5-89e2-1cceafb01fca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrow Flight authenticates its clients (REQ-1263).

Flight used to echo back whatever role the client asked for and validate no credential at all,
so any process that could open the port could read any role's data. These tests pin the replacement:
a bearer credential is required, the role comes from the validated identity, and a role the
identity does not hold is refused rather than granted.
"""

from __future__ import annotations

import asyncio
import json
import threading

import pyarrow.flight as flight
import pytest

from provisa.api.flight import server as flight_server
from provisa.api.flight.server import ProvisaFlightServer
from provisa.auth.models import AuthIdentity


class FakeState:
    """Minimal AppState substitute carrying only what the auth path reads."""

    def __init__(self, auth_config=None, auth_middleware_active=False):
        self.auth_config = auth_config
        self.auth_middleware_active = auth_middleware_active
        self.multitenancy = False
        self.admin_db = None
        self.roles = {}


_AUTH_CONFIG = {
    "provider": "oidc",
    "default_role": "analyst",
    "role_mapping": [
        {"claim": "groups", "type": "contains", "value": "data-eng", "role": "steward"}
    ],
}


def _identity(**claims) -> AuthIdentity:
    return AuthIdentity(
        user_id="u-1",
        email="alice@acme.test",
        display_name="Alice",
        roles=claims.pop("roles", []),
        raw_claims=claims,
        active_org_id=None,
    )


@pytest.fixture(scope="module")
def loop():
    """A live event loop on another thread — what _run_on_loop dispatches credential checks to."""
    made = asyncio.new_event_loop()
    thread = threading.Thread(target=made.run_forever, daemon=True)
    thread.start()
    yield made
    made.call_soon_threadsafe(made.stop)
    thread.join(timeout=5)


def _server(state, loop) -> ProvisaFlightServer:
    """A server instance without binding a port — __init__ would open a listener."""
    srv = ProvisaFlightServer.__new__(ProvisaFlightServer)
    srv._state = state
    srv._main_loop = loop
    return srv


@pytest.fixture()
def secured(loop, monkeypatch):
    srv = _server(FakeState(auth_config=_AUTH_CONFIG, auth_middleware_active=True), loop)
    seen: dict = {}

    async def _validate(state, token):  # noqa: ARG001  # signature mirrors the real validator
        if token == "good-token":
            return _identity(groups=["data-eng"], roles=["steward:trading", "auditor"])
        if token == "plain-token":
            return _identity(groups=["everyone"])
        raise ValueError("no such credential")

    monkeypatch.setattr(flight_server, "_validate_flight_credential", _validate)
    monkeypatch.setattr(srv, "_do_get_inner", lambda request, ticket: seen.update(request) or "ok")
    srv._seen = seen
    return srv


def _ticket(**body) -> flight.Ticket:
    return flight.Ticket(json.dumps(body).encode())


class TestDoGetAuthentication:
    def test_a_ticket_without_a_credential_is_refused(self, secured):
        with pytest.raises(
            flight.FlightUnauthenticatedError, match="bearer credential is required"
        ):
            secured.do_get(None, _ticket(query="SELECT 1", role="admin"))

    def test_a_rejected_credential_says_nothing_about_why(self, secured):
        with pytest.raises(flight.FlightUnauthenticatedError, match="credential rejected"):
            secured.do_get(None, _ticket(query="SELECT 1", token="bogus"))

    def test_the_role_comes_from_the_identity_not_the_ticket(self, secured):
        secured.do_get(None, _ticket(query="SELECT 1", token="plain-token"))
        assert secured._seen["role"] == "analyst", (
            "an identity matching no mapping rule runs as the configured default"
        )

    def test_a_mapped_claim_selects_the_role(self, secured):
        secured.do_get(None, _ticket(query="SELECT 1", token="plain-token", role=None))
        assert secured._seen["role"] == "analyst"
        secured._seen.clear()
        secured.do_get(None, _ticket(query="SELECT 1", token="good-token"))
        assert secured._seen["role"] == "steward"

    def test_a_requested_role_the_identity_holds_is_honored(self, secured):
        secured.do_get(None, _ticket(query="SELECT 1", token="good-token", role="auditor"))
        assert secured._seen["role"] == "auditor"

    def test_a_requested_role_the_identity_lacks_is_refused(self, secured):
        with pytest.raises(
            flight.FlightUnauthenticatedError, match="not assigned to this identity"
        ):
            secured.do_get(None, _ticket(query="SELECT 1", token="good-token", role="admin"))

    def test_the_ticket_cannot_assert_a_role_it_merely_names(self, secured):
        """The old echo behavior: `role` alone granted that role. It must not, even with a token."""
        with pytest.raises(flight.FlightUnauthenticatedError):
            secured.do_get(None, _ticket(query="SELECT 1", token="plain-token", role="steward"))


class TestHandshake:
    def test_the_handshake_validates_the_credential(self, secured):
        with pytest.raises(flight.FlightUnauthenticatedError):
            list(secured.do_handshake(None, [json.dumps({"role": "admin"}).encode()]))

    def test_the_handshake_answers_with_the_authorized_role(self, secured):
        token, _ = secured.do_handshake(
            None, [json.dumps({"token": "good-token", "role": "auditor"}).encode()]
        )
        assert json.loads(token.decode()) == {"role": "auditor"}

    def test_the_handshake_will_not_confirm_an_unheld_role(self, secured):
        with pytest.raises(flight.FlightUnauthenticatedError):
            secured.do_handshake(
                None, [json.dumps({"token": "good-token", "role": "admin"}).encode()]
            )


class TestHighSecurityMode:
    """REQ-693: Flight keeps serving, but a ticket returning rows must prove client-side decrypt.

    These call ``_do_get_inner`` directly — the gate lives there, and the ``secured`` fixture
    replaces it to keep the authentication tests off the execution path.
    """

    @pytest.fixture()
    def high(self, loop):
        srv = _server(FakeState(), loop)
        srv._state.security_high = True
        return srv

    def test_a_query_ticket_without_a_kms_key_is_refused(self, high):
        with pytest.raises(flight.FlightServerError, match="high-security mode"):
            high._do_get_inner({"query": "SELECT 1", "role": "analyst"}, _ticket())

    def test_a_query_ticket_carrying_the_kms_key_reaches_execution(self, high, monkeypatch):
        monkeypatch.setattr(type(high), "_execute_query", lambda self, request: "rows")
        request = {"query": "SELECT 1", "role": "analyst", "kms_key": "arn:kms:key"}
        assert high._do_get_inner(request, _ticket()) == "rows"

    def test_a_query_ticket_is_ungated_in_standard_mode(self, loop, monkeypatch):
        srv = _server(FakeState(), loop)
        monkeypatch.setattr(type(srv), "_execute_query", lambda self, request: "rows")
        assert srv._do_get_inner({"query": "SELECT 1", "role": "analyst"}, _ticket()) == "rows"


class TestUnsecuredDeployment:
    def test_no_auth_config_leaves_the_ticket_role_alone(self, loop):
        srv = _server(FakeState(), loop)
        seen: dict = {}
        srv._do_get_inner = lambda request, ticket: seen.update(request) or "ok"
        srv.do_get(None, _ticket(query="SELECT 1", role="admin"))
        assert seen["role"] == "admin"

    def test_a_live_middleware_without_config_fails_closed(self, loop):
        """A secured server whose config went missing must refuse, never degrade to trust mode."""
        srv = _server(FakeState(auth_middleware_active=True), loop)
        with pytest.raises(flight.FlightServerError, match="auth_config not configured"):
            srv.do_get(None, _ticket(query="SELECT 1", role="admin"))
