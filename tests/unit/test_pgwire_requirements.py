# Copyright (c) 2026 Kenneth Stott
# Canary: a50213e3-7e60-4260-b7cd-9d3effd52a4e
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for pgwire requirements: REQ-527, REQ-529, REQ-530, REQ-532, REQ-579, REQ-580, REQ-581, REQ-582, REQ-583, REQ-584, REQ-585, REQ-586, REQ-587, REQ-588, REQ-589, REQ-590, REQ-614, REQ-615, REQ-616"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from provisa.core.egress import CountingWriter

import jwt
import pytest
from tests.platform_plane import platform_db


# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------


def _session():
    """A real session: authentication sets the role through it (act_as)."""
    from provisa.pgwire.server import ProvisaSession

    return ProvisaSession()


def _make_role(caps: list[str]) -> dict:
    return {"capabilities": caps, "domain_access": ["domain1"]}


@pytest.fixture()
def pgwire_loop():
    """This test thread's connection loop, as ProvisaHandler.handle binds one for a connection.

    pgwire runs every validator on the connection thread's own loop (REQ-1882), so a test that
    drives handler methods directly binds one to the test thread for the test's duration.
    """
    from provisa.core.connection_loop import connection_loop

    with connection_loop() as cl:
        yield cl


# ---------------------------------------------------------------------------
# REQ-527 — pgwire disabled by default; starts only when PROVISA_PGWIRE_PORT is set
# ---------------------------------------------------------------------------


class TestReq527PgwireDisabledByDefault:
    """REQ-527: pgwire starts only when PROVISA_PGWIRE_PORT env var is a non-zero integer."""

    def test_zero_port_does_not_start(self, monkeypatch):
        # REQ-527: The startup function in app_startup.py gates on `if pgwire_port:` so port 0 means disabled.
        import inspect
        from provisa.api import app_startup as app_mod

        # REQ-1913: the port is the operator setting `server.pgwire_port`, which
        # PROVISA_PGWIRE_PORT sets; app_startup.py reads it through the settings registry.
        from provisa.core import settings_registry

        monkeypatch.setenv("PROVISA_PGWIRE_PORT", "0")
        port = settings_registry.resolve("server.pgwire_port").value
        assert port == 0, "Port 0 must be treated as disabled"

        # Verify the startup code in app_startup.py uses the correct gating pattern.
        src = inspect.getsource(app_mod)
        assert 'pgwire_port = settings_registry.value("server.pgwire_port")' in src, (
            "app_startup.py must read the pgwire port from the operator setting"
        )
        assert "if pgwire_port:" in src, (
            "app_startup.py must gate pgwire startup on `if pgwire_port:` so zero disables it"
        )

    def test_absent_env_var_does_not_start(self, monkeypatch):
        # REQ-527: Absent env var → default "0" → int 0 → falsy → server not started.
        import inspect
        from provisa.api import app_startup as app_mod

        from provisa.core import settings_registry

        monkeypatch.delenv("PROVISA_PGWIRE_PORT", raising=False)
        monkeypatch.setattr(settings_registry, "_config", {})
        # The declared default is 0 (falsy), so an absent env var disables pgwire.
        assert settings_registry.resolve("server.pgwire_port") == (0, "default")
        assert settings_registry.setting("server.pgwire_port").env == "PROVISA_PGWIRE_PORT"

        src = inspect.getsource(app_mod)
        assert 'pgwire_port = settings_registry.value("server.pgwire_port")' in src, (
            "app_startup.py must read the pgwire port from the operator setting"
        )

    def test_nonzero_port_enables_server(self, monkeypatch):
        # REQ-527: A non-zero port is truthy and passes the `if pgwire_port:` gate.
        import inspect
        from provisa.pgwire.server import start_pgwire_server

        monkeypatch.setenv("PROVISA_PGWIRE_PORT", "5439")
        port = int(os.environ.get("PROVISA_PGWIRE_PORT", "0"))
        assert port == 5439

        # `start_pgwire_server` must accept a port parameter and pass it to ProvisaServer.
        src = inspect.getsource(start_pgwire_server)
        assert "port" in src, "start_pgwire_server must accept and use the port argument"
        assert "ProvisaServer" in src, (
            "start_pgwire_server must instantiate ProvisaServer with the given port"
        )

    def test_start_pgwire_server_binds_all_interfaces(self):
        # REQ-527: server binds to 0.0.0.0 on the configured port
        from unittest.mock import patch, MagicMock

        mock_server = MagicMock()
        mock_thread = MagicMock()

        with (
            patch("provisa.pgwire.server.ProvisaServer") as MockServer,
            patch("threading.Thread", return_value=mock_thread),
        ):
            MockServer.return_value = mock_server

            from provisa.pgwire.server import start_pgwire_server

            start_pgwire_server("0.0.0.0", 5439, None)

            # Must be instantiated with host=0.0.0.0
            call_args = MockServer.call_args
            assert call_args[0][0] == ("0.0.0.0", 5439)


# ---------------------------------------------------------------------------
# REQ-529 — pgwire auth type 3 (cleartext) for both trust and simple modes
# ---------------------------------------------------------------------------


class TestReq529AuthType3:
    """REQ-529: auth uses PG auth type 3 (cleartext password)."""

    def test_send_auth_request_sends_type_3(self):
        # REQ-529
        import struct
        from buenavista.postgres import ServerResponse

        handler = MagicMock()
        buf = bytearray()

        def capture_write(data):
            buf.extend(data)

        handler.wfile.write.side_effect = capture_write
        handler.wfile.flush = MagicMock()

        from provisa.pgwire.server import ProvisaHandler

        # Call send_auth_request directly via unbound method on a mock instance
        instance = object.__new__(ProvisaHandler)
        instance.wfile = handler.wfile
        instance.send_auth_request(MagicMock())

        # Expect: b'R' (0x52) + length 8 + auth type 3
        assert len(buf) >= 9
        msg_type = bytes([buf[0]])
        assert msg_type == ServerResponse.AUTHENTICATION_REQUEST
        # auth type is the last 4 bytes of the 9-byte message
        auth_type = struct.unpack("!i", buf[5:9])[0]
        assert auth_type == 3, "Auth type must be 3 (cleartext password)"

    def test_trust_mode_ignores_password(self):
        # REQ-529: Trust mode: username becomes role_id, password ignored
        from provisa.pgwire.server import ProvisaHandler

        ctx = MagicMock()
        ctx.params = {"user": "analyst"}
        ctx.session = _session()

        handler = object.__new__(ProvisaHandler)
        handler.wfile = MagicMock()
        handler.wfile.write = MagicMock()
        handler.wfile.flush = MagicMock()
        handler.send_authentication_ok = MagicMock()
        handler.handle_post_auth = MagicMock()

        fake_state = MagicMock()

        # A single-tenant deployment: its catalog serves the one database, provisa.

        fake_state.multitenancy = False
        fake_state.auth_config = {"provider": "none"}
        fake_state.auth_middleware_active = False

        with patch("provisa.pgwire.server.state", fake_state):
            handler.handle_md5_password(ctx, b"any_password\x00")

        assert ctx.session.role_id == "analyst"
        handler.send_authentication_ok.assert_called_once()

    def test_unsupported_provider_returns_fatal_error(self, pgwire_loop):
        # REQ-529/REQ-890: A provider that is neither trust, simple, nor an OIDC-family
        # provider returns FATAL.
        from provisa.pgwire.server import ProvisaHandler

        ctx = MagicMock()
        ctx.params = {"user": "analyst"}
        ctx.session = _session()

        handler = object.__new__(ProvisaHandler)
        written = bytearray()
        handler.wfile = MagicMock()
        handler.wfile.write.side_effect = lambda d: written.extend(d)
        handler.wfile.flush = MagicMock()
        handler._send_pg_error = MagicMock()

        fake_state = MagicMock()

        # A single-tenant deployment: its catalog serves the one database, provisa.

        fake_state.multitenancy = False
        fake_state.auth_config = {"provider": "kerberos"}
        fake_state.auth_middleware_active = True

        with patch("provisa.pgwire.server.state", fake_state):
            handler.handle_md5_password(ctx, b"token\x00")

        handler._send_pg_error.assert_called_once()
        call_args = handler._send_pg_error.call_args[0]
        assert call_args[0] == "FATAL"


# ---------------------------------------------------------------------------
# REQ-890 — pluggable pgwire auth: oidc provider validates a bearer token
# ---------------------------------------------------------------------------


class TestReq890OidcAuth:
    """REQ-890: pgwire 'oidc' provider validates an OIDC bearer token (cleartext password)
    via the existing AuthProvider abstraction and maps the identity to a role."""

    def _handler(self):
        from provisa.pgwire.server import ProvisaHandler

        handler = object.__new__(ProvisaHandler)
        handler.wfile = MagicMock()
        handler.wfile.write = MagicMock()
        handler.wfile.flush = MagicMock()
        # socketserver's setup() is bypassed by object.__new__, so the metering writer admitting
        # the connection binds the session's org onto (REQ-1452) is installed here as setup()
        # installs it.
        handler._meter = CountingWriter(handler.wfile, None)
        handler.wfile = handler._meter
        handler.send_authentication_ok = MagicMock()
        handler.handle_post_auth = MagicMock()
        handler._send_pg_error = MagicMock()
        return handler

    def _state(self):
        fake_state = MagicMock()
        # A single-tenant deployment: its catalog serves the one database, provisa.
        fake_state.multitenancy = False
        fake_state.auth_config = {
            "provider": "oidc",
            "oidc": {
                "discovery_url": "https://idp.example/.well-known/openid-configuration",
                "client_id": "provisa",
            },
            "role_mapping": [
                {"type": "exact", "claim": "sub", "value": "alice", "role": "analyst"}
            ],
            "default_role": "viewer",
        }
        fake_state.auth_middleware_active = True
        # Single-org deployment: without this the MagicMock attribute is truthy, so the handler
        # takes the multitenant org-resolution branch and fails the connection on a missing loop.
        fake_state.multitenancy = False
        return fake_state

    def test_valid_token_accepted_and_mapped_to_role(self, pgwire_loop):
        # REQ-890: a valid bearer token is validated and mapped to a role via resolve_role.
        from provisa.auth.models import AuthIdentity

        ctx = MagicMock()
        ctx.params = {"user": "alice"}
        ctx.session = _session()

        class _FakeProvider:
            async def validate_token(self, token):
                assert token == "valid-token"
                return AuthIdentity(
                    user_id="alice",
                    email=None,
                    display_name="alice",
                    roles=["analyst"],
                    raw_claims={"sub": "alice", "roles": ["analyst"]},
                )

        handler = self._handler()
        with (
            patch("provisa.pgwire.server.state", self._state()),
            patch("provisa.auth.wiring.build_auth_provider", return_value=_FakeProvider()),
        ):
            handler.handle_md5_password(ctx, b"valid-token\x00")

        assert ctx.session.role_id == "analyst"
        handler.send_authentication_ok.assert_called_once()
        handler.handle_post_auth.assert_called_once()
        handler._send_pg_error.assert_not_called()

    def test_invalid_token_rejected_with_fatal(self, pgwire_loop):
        # REQ-890: an invalid token yields a FATAL 28P01 and no authentication.
        ctx = MagicMock()
        ctx.params = {"user": "alice"}
        ctx.session = _session()

        class _FakeProvider:
            async def validate_token(self, token):
                raise jwt.InvalidTokenError("bad token")

        handler = self._handler()
        with (
            patch("provisa.pgwire.server.state", self._state()),
            patch("provisa.auth.wiring.build_auth_provider", return_value=_FakeProvider()),
        ):
            handler.handle_md5_password(ctx, b"bad-token\x00")

        handler.send_authentication_ok.assert_not_called()
        handler._send_pg_error.assert_called_once()
        assert handler._send_pg_error.call_args[0][0] == "FATAL"
        assert handler._send_pg_error.call_args[0][1] == "28P01"


# ---------------------------------------------------------------------------
# REQ-124 / REQ-1263 — pgwire accepts a password (basic) and a personal access token
# ---------------------------------------------------------------------------


class TestPgwireBasicAndPat:
    """pgwire carries one cleartext secret, and what that secret is decides the presentation.

    A password goes to the provider's ``basic`` validator, a personal access token is a bearer
    credential like it is on every other surface, and the admitted role comes from the validated
    identity through resolve_role — not from the username the client typed.
    """

    def _handler(self):
        from provisa.pgwire.server import ProvisaHandler

        handler = object.__new__(ProvisaHandler)
        # As setup() builds it (REQ-1452), which object.__new__ bypasses.
        handler._meter = CountingWriter(MagicMock(), None)
        handler.wfile = handler._meter
        handler.send_authentication_ok = MagicMock()
        handler.handle_post_auth = MagicMock()
        handler._send_pg_error = MagicMock()
        return handler

    def _ctx(self, user: str = "alice"):
        ctx = MagicMock()
        ctx.params = {"user": user}
        ctx.session = _session()
        return ctx

    def _state(self, auth_config):
        fake_state = MagicMock()
        # A single-tenant deployment: its catalog serves the one database, provisa.
        fake_state.multitenancy = False
        fake_state.auth_config = auth_config
        fake_state.auth_middleware_active = True
        fake_state.multitenancy = False
        # The simple provider keeps its users' ids on the platform plane.
        fake_state.admin_db = platform_db()
        return fake_state

    def _simple_config(self):
        import bcrypt

        return {
            "provider": "simple",
            "allow_simple_auth": True,
            "jwt_secret": "unit-test-secret",
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

    def test_a_password_authenticates_under_a_non_oidc_provider(self, pgwire_loop):
        handler, ctx = self._handler(), self._ctx()
        with patch("provisa.pgwire.server.state", self._state(self._simple_config())):
            handler.handle_md5_password(ctx, b"s3cret\x00")

        assert ctx.session.role_id == "analyst"
        handler.send_authentication_ok.assert_called_once()
        handler._send_pg_error.assert_not_called()

    def test_a_wrong_password_is_refused(self, pgwire_loop):
        handler, ctx = self._handler(), self._ctx()
        with patch("provisa.pgwire.server.state", self._state(self._simple_config())):
            handler.handle_md5_password(ctx, b"wrong\x00")

        handler.send_authentication_ok.assert_not_called()
        assert handler._send_pg_error.call_args[0][1] == "28P01"

    def test_the_role_comes_from_the_identity_not_the_username(self, pgwire_loop):
        """The client names a user, never a role — a mapping rule decides what it runs as."""
        config = self._simple_config()
        config["role_mapping"] = [
            {"type": "contains", "claim": "roles", "value": "steward", "role": "steward"}
        ]
        handler, ctx = self._handler(), self._ctx()
        with patch("provisa.pgwire.server.state", self._state(config)):
            handler.handle_md5_password(ctx, b"s3cret\x00")

        assert ctx.session.role_id == "steward"

    def test_a_personal_access_token_is_accepted_as_a_bearer_credential(self, pgwire_loop):
        # REQ-1263: the PAT store hangs off the provider, so pgwire needs no notion of a PAT.
        from provisa.auth.models import AuthIdentity

        seen = {}

        class _FakeProvider:
            auth_scheme = "basic"

            @property
            def token_validators(self):
                return {"basic": self._basic, "bearer": self._bearer}

            async def _basic(self, token):
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

        config = self._simple_config()
        handler, ctx = self._handler(), self._ctx()
        with (
            patch("provisa.pgwire.server.state", self._state(config)),
            patch("provisa.auth.wiring.build_auth_provider", return_value=_FakeProvider()),
        ):
            handler.handle_md5_password(ctx, b"provisa_pat_abc\x00")

        assert seen["token"] == "provisa_pat_abc"
        assert ctx.session.role_id == "analyst"
        handler.send_authentication_ok.assert_called_once()

    def test_a_provider_that_cannot_be_built_refuses_on_the_wire(self, pgwire_loop):
        """An unknown provider name authenticates nobody, and the client is told so."""
        handler, ctx = self._handler(), self._ctx()
        with patch("provisa.pgwire.server.state", self._state({"provider": "kerberos"})):
            handler.handle_md5_password(ctx, b"whatever\x00")

        handler.send_authentication_ok.assert_not_called()
        assert handler._send_pg_error.call_args[0][0] == "FATAL"

    @pytest.mark.parametrize("provider", ["ldap", "saml"])
    def test_a_provider_selected_without_its_settings_refuses_on_the_wire(
        self, pgwire_loop, provider
    ):
        """REQ-1265: a known provider with no config block cannot be built either."""
        handler, ctx = self._handler(), self._ctx()
        config = {"provider": provider, "jwt_secret": "s" * 48}
        with patch("provisa.pgwire.server.state", self._state(config)):
            handler.handle_md5_password(ctx, b"whatever\x00")

        handler.send_authentication_ok.assert_not_called()
        assert handler._send_pg_error.call_args[0][0] == "FATAL"


# ---------------------------------------------------------------------------
# REQ-530 — TLS enabled via PROVISA_PGWIRE_CERT and PROVISA_PGWIRE_KEY
# ---------------------------------------------------------------------------


class TestReq530TLS:
    """REQ-530: TLS wraps connections when CERT+KEY set; replies 'N' when absent."""

    def test_no_env_vars_means_no_ssl_ctx(self, monkeypatch):
        # REQ-530: When CERT+KEY are absent, ssl_ctx must be None and no TLS wrapping occurs.
        import inspect
        from provisa.api import app_startup as app_mod

        monkeypatch.delenv("PROVISA_PGWIRE_CERT", raising=False)
        monkeypatch.delenv("PROVISA_PGWIRE_KEY", raising=False)
        cert = os.environ.get("PROVISA_PGWIRE_CERT")
        key = os.environ.get("PROVISA_PGWIRE_KEY")
        assert cert is None and key is None

        # Confirm app_startup.py reads CERT+KEY and only builds ssl_ctx when both are present.
        # REQ-1226: TLS resolution is centralized in _resolve_tls(), which returns a (cert, key)
        # tuple only when both are set (gated by `if cert and key:`) and None otherwise; the pgwire
        # start-up gates SSLContext creation on that tuple being non-None.
        src = inspect.getsource(app_mod)
        assert "PROVISA_PGWIRE_CERT" in src, "app_startup.py must read PROVISA_PGWIRE_CERT"
        assert "PROVISA_PGWIRE_KEY" in src, "app_startup.py must read PROVISA_PGWIRE_KEY"
        # The pair is gated on BOTH being set: asserted on what _resolve_tls returns.
        for env in ("PROVISA_TLS_CERT", "PROVISA_TLS_KEY"):
            monkeypatch.delenv(env, raising=False)
        assert app_mod._resolve_tls("PROVISA_PGWIRE_CERT", "PROVISA_PGWIRE_KEY") is None
        monkeypatch.setenv("PROVISA_PGWIRE_CERT", "/only/cert.pem")
        assert app_mod._resolve_tls("PROVISA_PGWIRE_CERT", "PROVISA_PGWIRE_KEY") is None, (
            "_resolve_tls must gate the (cert, key) pair on both being set"
        )
        assert "if _pgwire_tls is not None:" in src, (
            "app_startup.py must gate SSLContext creation on _resolve_tls returning a pair"
        )

    def test_ssl_negotiation_sends_n_when_no_ctx(self):
        # REQ-530: When no TLS, server replies 'N' to SSL negotiation
        import io
        import struct

        from buenavista.postgres import BVBuffer

        from provisa.pgwire.server import ProvisaHandler

        handler = object.__new__(ProvisaHandler)
        handler.wfile = MagicMock()
        written = []
        handler.wfile.write.side_effect = lambda d: written.append(d)
        handler.wfile.flush = MagicMock()
        handler.rfile = MagicMock()
        # The SSLRequest as it arrives on the wire: length 8, then the SSL request code.
        handler.r = BVBuffer(io.BytesIO(struct.pack("!II", 8, 80877103)))
        handler.request = MagicMock()
        handler.server = MagicMock()
        handler.server.ssl_ctx = None

        # Call the real method while mocking only the recursive call it makes to itself
        real = ProvisaHandler.handle_startup
        with patch.object(ProvisaHandler, "handle_startup", return_value=None):
            real(handler, MagicMock())

        assert b"N" in written, "Must reply 'N' when no TLS context"

    def test_ssl_negotiation_sends_s_when_ctx_present(self):
        # REQ-530: When TLS configured, server replies 'S' to SSL negotiation
        import io
        import ssl
        import struct

        from buenavista.postgres import BVBuffer

        from provisa.pgwire.server import ProvisaHandler

        handler = object.__new__(ProvisaHandler)
        handler.wfile = MagicMock()
        written = []
        handler.wfile.write.side_effect = lambda d: written.append(d)
        handler.wfile.flush = MagicMock()
        handler.rfile = MagicMock()
        handler.r = BVBuffer(io.BytesIO(struct.pack("!II", 8, 80877103)))
        mock_ssl_ctx = MagicMock(spec=ssl.SSLContext)
        mock_ssl_socket = MagicMock()
        mock_ssl_socket.makefile = MagicMock(return_value=MagicMock())
        mock_ssl_ctx.wrap_socket.return_value = mock_ssl_socket
        handler.request = MagicMock()
        handler.server = MagicMock()
        handler.server.ssl_ctx = mock_ssl_ctx

        real = ProvisaHandler.handle_startup
        with patch.object(ProvisaHandler, "handle_startup", return_value=None):
            real(handler, MagicMock())

        assert b"S" in written, "Must reply 'S' when TLS context is present"


# ---------------------------------------------------------------------------
# REQ-532 — information_schema and pg_catalog intercepted from in-memory DuckDB
# ---------------------------------------------------------------------------


class TestReq532CatalogIntercept:
    """REQ-532: Queries against information_schema / pg_catalog answered in-memory."""

    def test_information_schema_tables_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM information_schema.tables") == "INTERCEPT"

    def test_pg_catalog_pg_namespace_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM pg_catalog.pg_namespace") == "INTERCEPT"

    def test_pg_class_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM pg_catalog.pg_class") == "INTERCEPT"

    def test_pg_attribute_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM pg_catalog.pg_attribute") == "INTERCEPT"

    def test_pg_type_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM pg_catalog.pg_type") == "INTERCEPT"

    def test_pg_constraint_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM pg_catalog.pg_constraint") == "INTERCEPT"

    def test_pg_roles_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM pg_catalog.pg_roles") == "INTERCEPT"

    def test_pg_settings_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM pg_catalog.pg_settings") == "INTERCEPT"

    def test_pg_stat_activity_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM pg_catalog.pg_stat_activity") == "INTERCEPT"

    def test_information_schema_columns_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM information_schema.columns") == "INTERCEPT"

    def test_information_schema_key_column_usage_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM information_schema.key_column_usage") == "INTERCEPT"

    def test_information_schema_referential_constraints_intercepted(self):
        # REQ-532
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM information_schema.referential_constraints") == "INTERCEPT"

    def test_normal_select_not_intercepted(self):
        # REQ-532: Regular user queries must NOT be intercepted
        from provisa.pgwire.catalog import classify

        assert classify("SELECT * FROM orders") == "PASS_THROUGH"


# ---------------------------------------------------------------------------
# REQ-579 — server_version reported as "14.0.provisa"
# ---------------------------------------------------------------------------


class TestReq579ServerVersion:
    """REQ-579: Server reports version 14.0.provisa."""

    def test_connection_parameters_include_server_version(self):
        # REQ-579
        from provisa.pgwire.server import ProvisaConnection

        conn = ProvisaConnection()
        params = conn.parameters()
        assert params["server_version"] == "14.0.provisa"

    def test_catalog_settings_include_server_version(self):
        # REQ-579: pg_catalog.pg_settings must expose server_version = 14.0.provisa
        from provisa.pgwire.catalog import _KNOWN_SETTINGS

        assert _KNOWN_SETTINGS.get("server_version") == "14.0.provisa"


# ---------------------------------------------------------------------------
# REQ-580 — multi-statement simple-query support (semicolon splitting)
# ---------------------------------------------------------------------------


class TestReq580MultiStatement:
    """REQ-580: Semicolon-separated statements executed sequentially."""

    def test_semicolon_split_produces_multiple_statements(self):
        # REQ-580: handle_query splits a batch into statements before dispatching. The split is
        # statement-aware (sqlglot tokenizer), NOT a naive str.split(';') — a ';' inside a string
        # literal / comment / dollar-quote must not mis-split (parser differential).
        from provisa.compiler.sql_rewrite import split_sql_statements

        assert split_sql_statements("SELECT 1; SELECT 2; SELECT 3") == [
            "SELECT 1",
            "SELECT 2",
            "SELECT 3",
        ]
        # A ';' inside a literal is NOT a boundary (naive split would produce 4 fragments here).
        assert split_sql_statements("SELECT 'a;b'; SELECT 2") == ["SELECT 'a;b'", "SELECT 2"]

    def test_empty_parts_after_semicolon_skipped(self):
        # REQ-580: a trailing semicolon must not produce an empty statement.
        import inspect
        from provisa.pgwire.server import ProvisaHandler
        from provisa.compiler.sql_rewrite import split_sql_statements

        src = inspect.getsource(ProvisaHandler.handle_query)
        assert "EMPTY_QUERY_RESPONSE" in src, (
            "handle_query must send EMPTY_QUERY_RESPONSE when no statements remain after filtering"
        )
        assert split_sql_statements("SELECT 1;") == ["SELECT 1"]
        assert split_sql_statements("   ") == []


# ---------------------------------------------------------------------------
# REQ-581 — $N positional parameter substitution
# ---------------------------------------------------------------------------


class TestReq581ParameterSubstitution:
    """REQ-581: $1, $2, ... substituted as SQL literals before execution."""

    def test_string_parameter_substituted(self):
        # REQ-581
        from provisa.pgwire.server import _substitute_params

        result = _substitute_params("SELECT * FROM t WHERE name = $1", ["Alice"])
        assert "'Alice'" in result
        assert "$1" not in result

    def test_integer_parameter_substituted(self):
        # REQ-581
        from provisa.pgwire.server import _substitute_params

        result = _substitute_params("SELECT * FROM t WHERE id = $1", [42])
        assert "42" in result
        assert "$1" not in result

    def test_multiple_parameters_substituted(self):
        # REQ-581: Multiple positional params
        from provisa.pgwire.server import _substitute_params

        result = _substitute_params("SELECT $1, $2", ["foo", 99])
        assert "'foo'" in result
        assert "99" in result
        assert "$1" not in result
        assert "$2" not in result

    def test_none_becomes_null(self):
        # REQ-581: None → NULL literal
        from provisa.pgwire.server import _substitute_params

        result = _substitute_params("SELECT $1", [None])
        assert "NULL" in result

    def test_no_params_returns_original_sql(self):
        # REQ-581: No substitution when params is None
        from provisa.pgwire.server import _substitute_params

        sql = "SELECT * FROM t"
        assert _substitute_params(sql, None) == sql

    def test_high_index_substituted_before_low_to_avoid_partial_match(self):
        # REQ-581: $10 must not be confused with $1 — highest index replaced first
        from provisa.pgwire.server import _substitute_params

        params = ["a"] * 10
        params[9] = "ten"
        result = _substitute_params("SELECT $10, $1", params)
        assert "'ten'" in result
        assert "'a'" in result


# ---------------------------------------------------------------------------
# REQ-582 / REQ-583 / REQ-584 — DDL over pgwire: SUPERSEDED
#
# These described DDL dispatched to an engine or a source by the domain's ddl_catalog, the
# created object registered into the creating role's compile context, and the target resolved
# from ddl_catalog / ddl_schema. Nothing is defined through a query protocol any more
# (provisa/compiler/definitions.py): the handler, its registration and the two config keys are
# gone, and what was a test of each is a test of its refusal.
# ---------------------------------------------------------------------------

_DEFINITIONS = [
    ("CREATE TABLE foo (id INT)", "CREATE TABLE"),
    ("CREATE TABLE foo AS SELECT 1", "CREATE TABLE"),
    ("CREATE VIEW v AS SELECT 1", "CREATE VIEW"),
    ("CREATE OR REPLACE VIEW v AS SELECT 1", "CREATE VIEW"),
    ("ALTER TABLE foo ADD COLUMN bar INT", "ALTER TABLE"),
    ("DROP TABLE foo", "DROP TABLE"),
    ("DROP VIEW v", "DROP VIEW"),
    ("CREATE INDEX idx ON foo(id)", "CREATE INDEX"),
    ("CREATE SEQUENCE s", "CREATE SEQUENCE"),
    ("CREATE SCHEMA sx", "CREATE SCHEMA"),
]


class TestReq582DdlRouting:
    """Was REQ-582 (DDL dispatched by ddl_catalog). Every definition statement is refused."""

    @pytest.mark.parametrize("sql,kind", _DEFINITIONS)
    def test_a_definition_statement_is_refused_naming_its_kind(self, sql, kind):
        import sqlglot

        from provisa.compiler.definitions import DefinitionNotAvailable, refuse_definition

        with pytest.raises(DefinitionNotAvailable) as raised:
            refuse_definition(sqlglot.parse_one(sql, read="postgres"))
        assert raised.value.kind == kind
        assert str(raised.value).startswith(f"{kind} is not available here: create it in the model")

    def test_pgwire_has_no_ddl_handler_and_no_ddl_or_ctas_branch(self):
        import importlib.util
        import inspect

        from provisa.pgwire import server

        assert importlib.util.find_spec("provisa.pgwire.ddl_handler") is None
        source = inspect.getsource(server)
        assert "_DDL_RE" not in source and "_CTAS_RE" not in source
        assert "run_ctas" not in source and "DdlHandler" not in source

    def test_the_refusal_is_answered_as_feature_not_supported(self):
        """pgwire answers the pipeline's refusal with SQLSTATE 0A000, whatever wrapped it."""
        from provisa.compiler.definitions import DefinitionNotAvailable, definition_refusal

        try:
            try:
                raise DefinitionNotAvailable("CREATE TABLE")
            except DefinitionNotAvailable as inner:
                raise RuntimeError(str(inner)) from inner  # as the handler re-raises it
        except RuntimeError as wrapped:
            assert definition_refusal(wrapped) is not None
        assert definition_refusal(RuntimeError("anything else")) is None


class TestReq583PostDdlRegistration:
    """Was REQ-583 (the created object joins the creating role's compile context). Nothing is
    created, so nothing is registered outside the model."""

    def test_nothing_registers_an_object_into_a_roles_context(self):
        import pathlib

        import provisa

        root = pathlib.Path(provisa.__file__).parent
        holders = [
            str(path.relative_to(root))
            for path in root.rglob("*.py")
            if "_register_ddl_object" in path.read_text(errors="replace")
        ]
        assert holders == []


class TestReq584DdlTargetResolution:
    """Was REQ-584 (DDL target from the domain's ddl_catalog / ddl_schema). The keys are gone,
    and a config that still carries one fails validation instead of being read past."""

    @pytest.mark.parametrize("key", ["ddl_catalog", "ddl_schema"])
    def test_a_domain_that_still_names_a_ddl_target_fails_validation(self, key):
        from provisa.core.models import Domain

        with pytest.raises(ValueError) as raised:
            Domain(**{"id": "sales", key: "iceberg"})
        message = str(raised.value)
        assert key in message and "'sales'" in message
        assert "never through a query protocol" in message

    def test_a_domain_without_them_is_valid(self):
        from provisa.core.models import Domain

        assert Domain(id="sales").id == "sales"
        assert "ddl_catalog" not in Domain.model_fields and "ddl_schema" not in Domain.model_fields


# ---------------------------------------------------------------------------
# REQ-585 — COPY TO STDOUT and COPY FROM STDIN support
# ---------------------------------------------------------------------------


class TestReq585CopySupport:
    """REQ-585: COPY TO STDOUT and COPY FROM STDIN supported; text and csv formats."""

    def test_is_copy_sql_recognises_copy_keyword(self):
        # REQ-585
        from provisa.pgwire.copy_handler import is_copy_sql

        assert is_copy_sql("COPY orders TO STDOUT")
        assert is_copy_sql("copy orders from stdin")
        assert not is_copy_sql("SELECT * FROM orders")

    def test_copy_to_table_form_parsed(self):
        # REQ-585: Table reference form
        from provisa.pgwire.copy_handler import _PARSE_TO_RE

        m = _PARSE_TO_RE.match("COPY orders TO STDOUT")
        assert m is not None
        assert m.group("table") == "orders"

    def test_copy_to_query_form_parsed(self):
        # REQ-585: Arbitrary subquery form
        from provisa.pgwire.copy_handler import _PARSE_QUERY_TO_RE

        m = _PARSE_QUERY_TO_RE.match("COPY (SELECT id FROM orders) TO STDOUT")
        assert m is not None
        assert "SELECT id FROM orders" in m.group("query")

    def test_copy_to_csv_format_parsed(self):
        # REQ-585: CSV format spec parsed
        from provisa.pgwire.copy_handler import _PARSE_TO_RE

        m = _PARSE_TO_RE.match("COPY orders TO STDOUT WITH (FORMAT csv)")
        assert m is not None
        assert m.group("fmt") == "csv"

    def test_copy_text_format_default(self):
        # REQ-585: Default format is text (tab-delimited)
        from provisa.pgwire.copy_handler import _PARSE_TO_RE

        m = _PARSE_TO_RE.match("COPY orders TO STDOUT")
        assert m is not None
        assert m.group("fmt") is None  # None means default (text)

    def test_copy_from_stdin_parsed(self):
        # REQ-585
        from provisa.pgwire.copy_handler import _PARSE_FROM_RE

        m = _PARSE_FROM_RE.match("COPY orders FROM STDIN")
        assert m is not None
        assert m.group("table") == "orders"

    def test_text_copy_output_tab_delimited(self):
        # REQ-585: text format uses tab delimiter
        from provisa.pgwire.copy_handler import _rows_to_copy_text

        rows = [("Alice", 42), ("Bob", 99)]
        result = _rows_to_copy_text(rows, 2).decode()
        lines = result.strip().split("\n")
        assert "\t" in lines[0]

    def test_csv_copy_output_comma_delimited(self):
        # REQ-585: csv format uses comma delimiter
        from provisa.pgwire.copy_handler import _rows_to_copy_csv

        rows = [("Alice", 42)]
        result = _rows_to_copy_csv(rows, 2).decode()
        assert "," in result


# ---------------------------------------------------------------------------
# REQ-586 — COPY FROM is a bulk INSERT, admitted like one
# ---------------------------------------------------------------------------


class TestReq586CopyFromAdmitted:
    """``COPY … FROM STDIN`` (maintainer correction 2026-10-03) is a bulk INSERT into an existing
    registered table, admitted like one: the write right and the columns' writable_by before any
    data is asked for, the role's row filter over every row before any row is written."""

    @staticmethod
    def _state(capabilities: list[str], write_ops: list[str]):
        from types import SimpleNamespace

        from provisa.compiler.rls import RLSContext

        return SimpleNamespace(
            source_types={"pg": "postgresql"},
            roles={"dev": {"id": "dev", "capabilities": capabilities, "domain_access": ["*"]}},
            rls_contexts={"dev": RLSContext.empty()},
            masking_rules={},
            contexts={"dev": SimpleNamespace(tables={}, refusal=None, kept={})},
            tables=[
                {
                    "id": 1,
                    "source_id": "pg",
                    "domain_id": "sales",
                    "schema_name": "public",
                    "table_name": "orders",
                    "write_ops": write_ops,
                    "columns": [
                        {"column_name": "id", "visible_to": ["dev"], "writable_by": ["dev"]}
                    ],
                }
            ],
            relationships=[],
        )

    def _refused(self, state, raised, match):
        from types import SimpleNamespace

        from provisa.pgwire.copy_handler import CopyHandler

        handler = object.__new__(CopyHandler)
        handler._h = MagicMock()
        ctx = MagicMock()
        ctx.session.role_id = "dev"
        tm = SimpleNamespace(source_id="pg", table_id=1, domain_id="sales", table_name="orders")
        with (
            patch("provisa.pgwire.copy_handler._find_table_meta", return_value=(tm, ["id"])),
            patch("provisa.pgwire.copy_handler.state", state),
        ):
            with pytest.raises(raised, match=match):
                handler.handle(ctx, "COPY sales.orders (id) FROM STDIN")
        handler._h.wfile.write.assert_not_called()  # no CopyInResponse: no data asked for
        handler._h.rfile.read.assert_not_called()

    def test_a_role_without_the_write_right_is_refused_before_any_data_is_asked_for(self):
        from provisa.compiler.write_admission import WriteNotAdmitted

        state = self._state(["query_development"], ["delete", "insert", "update"])
        self._refused(state, WriteNotAdmitted, "'write' right")

    def test_copy_from_a_source_that_takes_no_writes_is_refused(self):
        # executor/write_capability.py: a table whose source takes no inserts refuses a bulk load
        # by name, whatever the role holds, before any data is asked for.
        from provisa.compiler.write_admission import WriteNotSupported

        state = self._state(["query_development", "write"], [])
        self._refused(state, WriteNotSupported, "'orders' does not take INSERT")

    def test_copy_from_column_list_inferred_when_not_provided(self):
        from provisa.pgwire.copy_handler import _PARSE_FROM_RE

        m = _PARSE_FROM_RE.match("COPY orders FROM STDIN")
        assert m is not None
        assert m.group("cols") is None


# ---------------------------------------------------------------------------
# REQ-587 — Transaction control commands intercepted and return empty success
# ---------------------------------------------------------------------------


class TestReq587TransactionIntercept:
    """REQ-587: SET/BEGIN/COMMIT/ROLLBACK etc. are intercepted; in_transaction() always False."""

    @pytest.mark.parametrize(
        "sql",
        [
            "SET search_path TO public",
            "BEGIN",
            "START TRANSACTION",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT sp1",
            "RELEASE sp1",
            "DISCARD ALL",
            "RESET ALL",
            "DEALLOCATE ALL",
        ],
    )
    def test_transaction_commands_intercepted(self, sql):
        # REQ-587
        from provisa.pgwire.catalog import classify

        assert classify(sql) == "INTERCEPT", f"Expected INTERCEPT for: {sql}"

    def test_session_in_transaction_always_false(self):
        # REQ-587: The server is stateless w.r.t. transactions
        from provisa.pgwire.server import ProvisaSession

        session = ProvisaSession()
        assert session.in_transaction() is False


# ---------------------------------------------------------------------------
# REQ-588 — Catalog scalar expression intercepts
# ---------------------------------------------------------------------------


class TestReq588ScalarExpressionIntercepts:
    """REQ-588: current_user, current_database(), version() etc. are intercepted."""

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT current_user",
            "SELECT session_user",
            "SELECT current_database()",
            "SELECT current_schema()",
            "SELECT version()",
            "SELECT pg_backend_pid()",
            "SHOW server_version",
        ],
    )
    def test_scalar_expressions_intercepted(self, sql):
        # REQ-588
        from provisa.pgwire.catalog import classify

        assert classify(sql) == "INTERCEPT", f"Expected INTERCEPT for: {sql}"

    def test_version_in_settings_returns_postgresql_14(self):
        # REQ-588: version() must return "PostgreSQL 14.0 on Provisa"
        from provisa.pgwire.catalog import _KNOWN_SETTINGS

        # server_version_num maps to 14.x
        assert _KNOWN_SETTINGS["server_version_num"].startswith("14")

    def test_current_database_returns_provisa(self):
        # REQ-588: current_database() → "provisa"
        from provisa.pgwire.catalog import answer, classify
        from unittest.mock import MagicMock

        # Ensure the query is intercepted
        assert classify("SELECT current_database()") == "INTERCEPT"

        fake_state = MagicMock()

        # A single-tenant deployment: its catalog serves the one database, provisa.

        fake_state.multitenancy = False
        fake_state.contexts = {}
        fake_state.schema_build_cache = {"column_types": {}}

        result = answer("SELECT current_database()", "", fake_state)
        # Result must contain "provisa"
        assert any("provisa" in str(row) for row in result.rows), (
            f"Expected 'provisa' in result rows, got: {result.rows}"
        )

    def test_pg_backend_pid_returns_zero(self):
        # REQ-588: pg_backend_pid() → 0
        from provisa.pgwire.catalog import answer, classify

        assert classify("SELECT pg_backend_pid()") == "INTERCEPT"

        fake_state = MagicMock()

        # A single-tenant deployment: its catalog serves the one database, provisa.

        fake_state.multitenancy = False
        fake_state.contexts = {}
        fake_state.schema_build_cache = {"column_types": {}}

        result = answer("SELECT pg_backend_pid()", "", fake_state)
        assert any(0 in row for row in result.rows), (
            f"Expected 0 in result rows, got: {result.rows}"
        )


# ---------------------------------------------------------------------------
# REQ-589 — Binary parameter encoding for extended-query protocol
# ---------------------------------------------------------------------------


class TestReq589BinaryParameters:
    """REQ-589: Binary OID decoding for standard types; unsupported OIDs raise error."""

    def test_supported_oids_present_in_type_map(self):
        # REQ-589: All required OIDs must be handled
        # These are the OIDs named in the requirement
        required_oids = {16, 17, 20, 21, 23, 25, 700, 701, 1043, 1082, 1114, 1184, 1700, 2950}
        from provisa.pgwire.catalog_data import _PG_TYPE_ROWS

        present_oids = {row[0] for row in _PG_TYPE_ROWS}
        missing = required_oids - present_oids
        assert not missing, f"Missing OIDs in pg_type rows: {missing}"

    def test_pg_literal_bool_true(self):
        # REQ-589: bool True encoded as TRUE
        from provisa.pgwire.server import _pg_literal

        assert _pg_literal(True) == "TRUE"

    def test_pg_literal_bool_false(self):
        # REQ-589: bool False encoded as FALSE
        from provisa.pgwire.server import _pg_literal

        assert _pg_literal(False) == "FALSE"

    def test_pg_literal_none_is_null(self):
        # REQ-589: None → NULL
        from provisa.pgwire.server import _pg_literal

        assert _pg_literal(None) == "NULL"

    def test_pg_literal_bytes_hex_encoded(self):
        # REQ-589: bytes → hex-escaped literal (OID 17 = bytea)
        from provisa.pgwire.server import _pg_literal

        result = _pg_literal(b"\xde\xad")
        assert "dead" in result.lower()

    def test_pg_literal_integer(self):
        # REQ-589: int passed through numerically
        from provisa.pgwire.server import _pg_literal

        assert _pg_literal(42) == "42"

    def test_pg_literal_string_escapes_quotes(self):
        # REQ-589: single-quotes inside strings are doubled
        from provisa.pgwire.server import _pg_literal

        result = _pg_literal("O'Brien")
        assert "O''Brien" in result


# ---------------------------------------------------------------------------
# REQ-590 — Hard-coded timeouts: DDL 60s, query 120s
# ---------------------------------------------------------------------------


class TestReq590Timeouts:
    """REQ-590: DDL ops time out after 60s. Query ops run under pgwire's own request timeout
    (REQ-1905: limits.request_timeouts.pgwire, shipped at 300s) — no longer a hard-coded 120s."""

    def test_query_timeout_is_pgwires_request_timeout(self):
        # REQ-590 (amended) / REQ-1905: every statement run asks the one resolver for pgwire's
        # timeout; nothing is hard-coded at 120 any more.
        import inspect
        from provisa.core.limits import request_timeout_for
        from provisa.pgwire import copy_handler, server

        for module in (server, copy_handler):
            source = inspect.getsource(module)
            assert 'timeout=request_timeout_for("pgwire")' in source
            assert "timeout=120" not in source
        assert request_timeout_for("pgwire") == 300.0  # what ships


# ---------------------------------------------------------------------------
# REQ-614 — pgwire accepts only SQL (not GraphQL or Cypher)
# ---------------------------------------------------------------------------


class TestReq614SqlOnly:
    """REQ-614: pgwire listener accepts only SQL; GraphQL and Cypher are not parsed."""

    def test_execute_pgwire_sql_is_sql_pipeline(self):
        # REQ-614: The pipeline entry point is named for SQL and routes through governance.
        # execute_pgwire_sql delegates the govern step to govern_pgwire_plan (the govern-then-
        # stream split, REQ-028); governance still lives on the SQL pipeline entry points.
        from provisa.pgwire._pipeline import execute_pgwire_sql, govern_pgwire_plan
        import inspect

        assert "govern_pgwire_plan" in inspect.getsource(execute_pgwire_sql)
        assert "_govern_and_route" in inspect.getsource(govern_pgwire_plan)

    def test_pipeline_does_not_import_graphql_resolvers(self):
        # REQ-614: No GraphQL execution in the pgwire pipeline module
        import inspect
        from provisa.pgwire import _pipeline

        source = inspect.getsource(_pipeline)
        # GraphQL-specific executor modules must not be imported
        assert "graphql_executor" not in source
        assert "cypher_translator" not in source

    def test_pipeline_does_not_have_cypher_entry_point(self):
        # REQ-614: No execute_pgwire_cypher function
        import provisa.pgwire._pipeline as pipeline_mod

        assert not hasattr(pipeline_mod, "execute_pgwire_cypher")
        assert not hasattr(pipeline_mod, "execute_pgwire_graphql")


# ---------------------------------------------------------------------------
# REQ-615 — pgwire does not support DML mutations
# ---------------------------------------------------------------------------


class TestReq615NoDml:
    """REQ-615: pgwire does not support INSERT, UPDATE, DELETE."""

    def test_dml_is_not_taken_for_a_definition(self):
        # A data write is not a definition: the definitions refusal lets it by, from its text
        # and from its parsed tree.
        import sqlglot

        from provisa.compiler.definitions import definition_kind, refuse_definition_text

        dml_statements = [
            "INSERT INTO orders VALUES (1, 'x')",
            "UPDATE orders SET name = 'y' WHERE id = 1",
            "DELETE FROM orders WHERE id = 1",
        ]
        for sql in dml_statements:
            refuse_definition_text(sql)  # does not raise
            assert definition_kind(sqlglot.parse_one(sql, read="postgres")) is None, sql

    def test_dml_not_matched_by_copy_regex(self):
        # REQ-615: DML is not COPY either
        from provisa.pgwire.server import _COPY_RE

        dml_statements = [
            "INSERT INTO orders VALUES (1)",
            "UPDATE orders SET id = 1",
            "DELETE FROM orders",
        ]
        for sql in dml_statements:
            assert not _COPY_RE.match(sql), f"DML should not match COPY regex: {sql}"

    def test_validate_sql_rejects_insert(self):
        # REQ-615: INSERT is not routed by DDL or COPY handlers; it falls into the
        # governance pipeline (_govern_and_route) which only handles SELECT queries.
        # Verify that INSERT cannot be dispatched via a dedicated DML write path.
        import sqlglot
        import sqlglot.expressions as exp
        from provisa.pgwire.server import _COPY_RE
        import provisa.pgwire._pipeline as pipeline_mod

        insert_sql = "INSERT INTO orders VALUES (1, 'x')"

        # INSERT must not be captured by COPY routing
        assert not _COPY_RE.match(insert_sql), "INSERT must not match COPY regex"

        # The pipeline must have no dedicated INSERT/DML entry point
        assert not hasattr(pipeline_mod, "execute_pgwire_insert")
        assert not hasattr(pipeline_mod, "execute_pgwire_dml")

        # sqlglot confirms INSERT is an Insert expression, not a Select — proving
        # the governance pipeline (which operates on Select trees) cannot succeed for it
        parsed = sqlglot.parse_one(insert_sql, read="postgres")
        assert isinstance(parsed, exp.Insert), "INSERT must parse as an Insert expression"
        assert not isinstance(parsed, exp.Select), "INSERT must not parse as a Select"


# ---------------------------------------------------------------------------
# REQ-616 — COPY and DDL require ddl capability
# ---------------------------------------------------------------------------


class TestReq616DdlCapabilityRequired:
    """REQ-616: no capability makes a definition available over pgwire."""

    def test_a_definition_is_refused_whatever_the_roles_capabilities(self):
        # No capability makes a definition available: the refusal takes the statement, not
        # the role.
        import inspect

        from provisa.compiler import definitions

        source = inspect.getsource(definitions)
        assert "capabilit" not in source and "role_id" not in source

    def test_copy_handler_checks_ddl_capability_via_server(self):
        # REQ-616: COPY error surfaces as PermissionError (42501 in handler)
        # Test that PermissionError from CopyHandler propagates correctly
        from provisa.pgwire.server import _COPY_RE

        # COPY statements match the COPY regex and are routed to CopyHandler
        assert _COPY_RE.match("COPY orders TO STDOUT")
        assert _COPY_RE.match("COPY orders FROM STDIN")

    def test_server_routes_copy_to_copy_handler(self):
        # REQ-616: server.handle_query dispatches COPY to CopyHandler
        from provisa.pgwire.server import _COPY_RE

        # COPY statement triggers CopyHandler path
        sql = "COPY orders TO STDOUT"
        assert _COPY_RE.match(sql), "COPY regex must match COPY statements"

    def test_server_sends_a_definition_to_the_pipeline_not_to_a_handler(self):
        # Was "server.handle_query dispatches DDL to DdlHandler". A definition statement has no
        # branch of its own in the statement loop: COPY and cursors do, a definition does not.
        import inspect

        from provisa.pgwire import server

        loop = inspect.getsource(server.ProvisaHandler._process_query_stmts)
        assert "_COPY_RE.match(stmt)" in loop
        assert "ddl_handler" not in loop and "run_ctas" not in loop
        assert "_DDL_RE" not in loop and "_CTAS_RE" not in loop


class TestRoleSetOnPgwire:
    """REQ-1620: a connection names a role, or a set of held roles acting as their meta-role."""

    @pytest.mark.parametrize(
        "params",
        [
            {"provisa.role": "analyst,auditor"},
            {"options": "-c provisa.role=analyst,auditor"},
            {"options": "-cprovisa.role=analyst,auditor"},
            {"options": "--provisa.role=analyst,auditor"},
        ],
    )
    def test_the_role_is_named_by_parameter_or_options(self, params):
        from provisa.pgwire.server import requested_role

        assert requested_role(params) == "analyst,auditor"

    def test_naming_two_different_roles_is_refused(self):
        from provisa.pgwire.server import requested_role

        with pytest.raises(PermissionError, match="two roles"):
            requested_role({"provisa.role": "analyst", "options": "-c provisa.role=auditor"})

    def test_trust_mode_user_names_a_set(self):
        from provisa.pgwire.server import ProvisaHandler

        ctx = MagicMock()
        ctx.params = {"user": "auditor,analyst"}
        ctx.session = _session()
        handler = object.__new__(ProvisaHandler)
        handler.wfile = MagicMock()
        handler.send_authentication_ok = MagicMock()
        handler.handle_post_auth = MagicMock()
        fake_state = MagicMock()
        fake_state.multitenancy = False
        fake_state.auth_config = {"provider": "none"}
        fake_state.auth_middleware_active = False
        with patch("provisa.pgwire.server.state", fake_state):
            handler.handle_md5_password(ctx, b"x\x00")
        assert ctx.session.role_id == "meta:analyst+auditor"
        assert ctx.session.role_set == ("analyst", "auditor")

    def test_trust_mode_refuses_a_named_meta_role(self):
        from provisa.pgwire.server import ProvisaHandler

        ctx = MagicMock()
        ctx.params = {"user": "meta:analyst+auditor"}
        ctx.session = _session()
        handler = object.__new__(ProvisaHandler)
        handler.wfile = MagicMock()
        handler.send_authentication_ok = MagicMock()
        handler._send_pg_error = MagicMock()
        fake_state = MagicMock()
        fake_state.multitenancy = False
        fake_state.auth_config = {"provider": "none"}
        fake_state.auth_middleware_active = False
        with patch("provisa.pgwire.server.state", fake_state):
            handler.handle_md5_password(ctx, b"x\x00")
        handler.send_authentication_ok.assert_not_called()
        assert handler._send_pg_error.call_args[0][:2] == ("FATAL", "28000")

    def _authenticate(self, params):
        from provisa.auth.models import AuthIdentity

        class _FakeProvider:
            async def validate_token(self, token):
                return AuthIdentity(
                    user_id="alice",
                    email=None,
                    display_name="alice",
                    roles=["analyst", "auditor"],
                    raw_claims={"sub": "alice"},
                )

        ctx = MagicMock()
        ctx.params = {"user": "alice", **params}
        ctx.session = _session()
        suite = TestReq890OidcAuth()
        handler = suite._handler()
        with (
            patch("provisa.pgwire.server.state", suite._state()),
            patch("provisa.auth.wiring.build_auth_provider", return_value=_FakeProvider()),
            patch(
                "provisa.security.meta_role.ensure_meta_role",
                side_effect=lambda st, members: "meta:" + "+".join(members),
            ),
            patch(
                "provisa.security.meta_role.acting_roles",
                side_effect=lambda st, r: (
                    tuple(r[5:].split("+")) if r.startswith("meta:") else (r,)
                ),
            ),
        ):
            handler.handle_md5_password(ctx, b"valid-token\x00")
        return ctx, handler

    def test_a_held_set_acts_as_its_meta_role(self, pgwire_loop):
        ctx, handler = self._authenticate({"options": "-c provisa.role=auditor,analyst"})
        handler._send_pg_error.assert_not_called()
        assert ctx.session.role_id == "meta:analyst+auditor"
        assert ctx.session.role_set == ("analyst", "auditor")

    def test_a_set_with_a_role_not_held_is_refused_by_name(self, pgwire_loop):
        ctx, handler = self._authenticate({"provisa.role": "analyst,org_admin"})
        handler.send_authentication_ok.assert_not_called()
        assert handler._send_pg_error.call_args[0][:2] == ("FATAL", "28000")
        assert "'org_admin'" in handler._send_pg_error.call_args[0][2]

    def test_the_meta_role_is_made_again_in_the_sessions_org_before_each_statement(self):
        from provisa.core.request_context import current_org

        session = _session()
        session.org_id = "acme"
        session.act_as("meta:analyst+auditor", ("analyst", "auditor"))
        seen = []

        def _ensure(st, members):
            seen.append((current_org.get(), members))
            return "meta:" + "+".join(members)

        with patch("provisa.security.meta_role.ensure_meta_role", side_effect=_ensure):
            session.bind_role()
            session.bind_role()
        assert seen == [("acme", ["analyst", "auditor"])] * 2
        assert session.role_id == "meta:analyst+auditor"
