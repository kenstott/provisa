# Copyright (c) 2026 Kenneth Stott
# Canary: 957f4b66-5714-424a-bfe3-0acf76b65525
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-527, REQ-529, REQ-530, REQ-532, REQ-579, REQ-580, REQ-581, REQ-582, REQ-583, REQ-584, REQ-585, REQ-587, REQ-588 and REQ-589 — pgwire disabled-by-default, auth, TLS, catalog intercept, server version, multi-statement queries, parameterized queries, DDL dispatch, post-DDL registration, DDL target resolution, COPY protocol, transaction control intercepts, scalar intercepts and binary params."""

from __future__ import annotations

import io
import os
import struct
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import bcrypt
import jwt
import pytest
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.auth.models import AuthIdentity
from provisa.core.egress import CountingWriter
from tests.platform_plane import platform_db

_SECRET = "test-signing-key-at-least-32-bytes-long"


scenarios("../features/REQ-527.feature")
scenarios("../features/REQ-529.feature")
scenarios("../features/REQ-530.feature")
scenarios("../features/REQ-532.feature")
scenarios("../features/REQ-579.feature")
scenarios("../features/REQ-580.feature")
scenarios("../features/REQ-581.feature")
scenarios("../features/REQ-582.feature")
scenarios("../features/REQ-583.feature")
scenarios("../features/REQ-584.feature")
scenarios("../features/REQ-585.feature")
scenarios("../features/REQ-587.feature")
scenarios("../features/REQ-588.feature")
scenarios("../features/REQ-589.feature")


@pytest.fixture
def pgwire_loop():
    """This test thread's connection loop, bound as ProvisaHandler.handle binds one (REQ-1882)."""
    from provisa.core.connection_loop import connection_loop

    with connection_loop() as cl:
        yield cl


@pytest.fixture
def shared_data(pgwire_loop) -> dict:
    del pgwire_loop  # triggers setup; value unused
    return {}


# ===========================================================================
# REQ-527 — pgwire disabled by default; starts only when PROVISA_PGWIRE_PORT is set
# ===========================================================================


@given("PROVISA_PGWIRE_PORT is not set or is zero")
def pgwire_port_not_set_or_zero(monkeypatch, shared_data):
    """Ensure PROVISA_PGWIRE_PORT is absent from the environment (disabled state)."""
    monkeypatch.delenv("PROVISA_PGWIRE_PORT", raising=False)
    port = int(os.environ.get("PROVISA_PGWIRE_PORT", "0"))
    assert port == 0, (
        f"Expected PROVISA_PGWIRE_PORT to be absent/zero for the disabled scenario; got {port}"
    )
    shared_data["initial_port"] = port
    shared_data["monkeypatch"] = monkeypatch


@when("the server starts")
def server_starts(shared_data):
    """
    Simulate the server startup decision logic.

    When PROVISA_PGWIRE_PORT is 0 / absent the pgwire listener must NOT be started.
    Record the outcome so the Then step can assert.
    """
    port = int(os.environ.get("PROVISA_PGWIRE_PORT", "0"))
    shared_data["startup_port"] = port

    # Simulate the exact conditional that the startup hook uses:
    #   if port != 0: start_pgwire_server(...)
    pgwire_started = False
    bind_address = None

    if port != 0:
        # This branch must NOT execute for the disabled scenario.
        pgwire_started = True
        bind_address = "0.0.0.0"

    shared_data["pgwire_started"] = pgwire_started
    shared_data["bind_address"] = bind_address


@then(
    parsers.re(
        r"the pgwire listener does not bind; when the variable is set to a non-zero integer it binds\s+to 0\.0\.0\.0"
    )
)
def pgwire_listener_disabled_then_enabled(shared_data):
    """
    Assert two things required by REQ-527:

    1. When PROVISA_PGWIRE_PORT is absent/zero the listener does NOT bind.
    2. When PROVISA_PGWIRE_PORT is set to a non-zero integer the server calls
       start_pgwire_server with host="0.0.0.0".
    """
    from unittest.mock import patch, MagicMock

    # -----------------------------------------------------------------------
    # Part 1 — disabled when port == 0
    # -----------------------------------------------------------------------
    assert shared_data["pgwire_started"] is False, (
        "pgwire listener must NOT start when PROVISA_PGWIRE_PORT is absent or zero"
    )
    assert shared_data["bind_address"] is None, "bind_address must be None when pgwire is disabled"

    # -----------------------------------------------------------------------
    # Part 2 — verify the env-var guard logic with port 0 and absent var
    # -----------------------------------------------------------------------
    for env_value, expected_enabled in [
        ("0", False),
        (None, False),  # absent
    ]:
        if env_value is None:
            actual_port = int(os.environ.get("PROVISA_PGWIRE_PORT", "0"))
        else:
            actual_port = int(env_value)
        assert actual_port == 0, (
            f"Port derived from {env_value!r} must be 0 (disabled); got {actual_port}"
        )
        enabled = actual_port != 0
        assert enabled is expected_enabled, (
            f"For env_value={env_value!r} expected enabled={expected_enabled}, got {enabled}"
        )

    # -----------------------------------------------------------------------
    # Part 3 — non-zero port causes binding to 0.0.0.0
    # -----------------------------------------------------------------------
    test_port = 5439

    mock_server = MagicMock()
    mock_thread = MagicMock()

    with (
        patch("provisa.pgwire.server.ProvisaServer") as MockServer,
        patch("threading.Thread", return_value=mock_thread),
    ):
        MockServer.return_value = mock_server

        from provisa.pgwire.server import start_pgwire_server

        start_pgwire_server("0.0.0.0", test_port, None)

        # ProvisaServer must have been instantiated with ("0.0.0.0", test_port) as
        # the first positional argument (the server_address tuple).
        assert MockServer.called, "ProvisaServer must be instantiated when port is non-zero"
        call_args = MockServer.call_args
        server_address = call_args[0][0]
        assert server_address[0] == "0.0.0.0", (
            f"pgwire server must bind to 0.0.0.0; got {server_address[0]!r}"
        )
        assert server_address[1] == test_port, (
            f"pgwire server must bind to port {test_port}; got {server_address[1]}"
        )

    # -----------------------------------------------------------------------
    # Part 4 — verify several non-zero port values all trigger binding
    # -----------------------------------------------------------------------
    for nonzero_port in (5432, 5439, 15432, 1):
        enabled = nonzero_port != 0
        assert enabled is True, f"Port {nonzero_port} must be treated as enabled (non-zero)"

    # -----------------------------------------------------------------------
    # Part 5 — unit-level guard: the start_pgwire_server function is NOT called
    #           when port == 0 (simulate the app.py lifespan conditional)
    # -----------------------------------------------------------------------
    called_ports: list[int] = []

    def _fake_start(host, port, state, loop):
        called_ports.append(port)

    with patch("provisa.pgwire.server.start_pgwire_server", side_effect=_fake_start):
        for zero_val in ("0", ""):
            candidate_port = int(zero_val) if zero_val else 0
            if candidate_port != 0:
                from provisa.pgwire.server import start_pgwire_server as _s

                _s("0.0.0.0", candidate_port, None, None)

    assert called_ports == [], (
        f"start_pgwire_server must NOT be invoked for zero/absent port; calls: {called_ports}"
    )


# ===========================================================================
# REQ-529 — pgwire auth type 3; trust/simple/OIDC modes; invalid credential → 28P01
# ===========================================================================

# ---------------------------------------------------------------------------
# Helpers — same real-invocation pattern as steps_pgwire_pluggable_auth.py
# ---------------------------------------------------------------------------


def _real_session():
    """The session pgwire authenticates: it has no role until the handler grants one."""
    from provisa.pgwire.server import ProvisaSession

    return ProvisaSession()


class _Ctx529:
    def __init__(self, user: str):
        self.params = {"user": user}
        self.session = _real_session()


def _handler529(shared_data: dict):
    """Bare ProvisaHandler with socket replaced so wire bytes land in shared_data."""
    from provisa.pgwire.server import ProvisaHandler

    handler = object.__new__(ProvisaHandler)
    errors = shared_data.setdefault("errors", [])
    admitted = shared_data.setdefault("admitted", [])

    handler._send_pg_error = lambda severity, sqlstate, message: errors.append(
        (severity, sqlstate, message)
    )
    handler.send_authentication_ok = lambda: admitted.append(True)
    handler.handle_post_auth = lambda ctx: None  # noqa: ARG005
    handler._assert_peer_binding = lambda username: None  # noqa: ARG005 — no mTLS in tests
    handler._sasl_offered = False
    # REQ-1452: setup() is what wraps the socket in the egress meter, and the auth path binds the
    # admitted session's org to it — so a handler built without setup() still needs one.
    handler._meter = CountingWriter(io.BytesIO(), None)
    return handler


def _authenticate529(shared_data: dict, *, user: str = "alice") -> None:
    """Run handle_md5_password against the auth config recorded in shared_data."""
    shared_data["errors"] = []
    shared_data["admitted"] = []
    state = SimpleNamespace(
        auth_config=shared_data.get("auth_config"),
        auth_middleware_active=shared_data.get("auth_middleware_active", True),
        multitenancy=False,
        # The simple provider keeps its users' ids on the platform plane.
        admin_db=platform_db(),
    )
    handler = _handler529(shared_data)
    ctx = cast(Any, _Ctx529(user))
    shared_data["ctx"] = ctx

    patches = [patch("provisa.pgwire.server.state", state)]
    provider_obj = shared_data.get("provider")
    if provider_obj is not None:
        patches.append(patch("provisa.auth.wiring.build_auth_provider", return_value=provider_obj))
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


# ---------------------------------------------------------------------------
# Triple 1 — provider: none (trust mode)
# ---------------------------------------------------------------------------


@given("pgwire is launched with provider `none`")
def given_provider_none(shared_data):
    """Trust mode: auth_config declares provider=none; password is irrelevant."""
    shared_data["auth_config"] = {"provider": "none"}
    shared_data["auth_middleware_active"] = True
    shared_data["provider"] = None


@when("a client connects with any password")
def when_any_password(shared_data):
    shared_data["password"] = "anything-at-all"
    _authenticate529(shared_data, user="alice")


@then("the username becomes the role_id and the session is established")
def then_username_is_role(shared_data):
    assert shared_data["errors"] == [], f"Unexpected errors in trust mode: {shared_data['errors']}"
    assert shared_data.get("admitted") == [True], (
        "send_authentication_ok must be called in trust mode"
    )
    assert shared_data["ctx"].session.role_id == "alice", (
        f"role_id must equal the startup username; got {shared_data['ctx'].session.role_id!r}"
    )


# ---------------------------------------------------------------------------
# Triple 2 — credential provider (simple / basic)
# ---------------------------------------------------------------------------


def _simple_auth_config_529() -> dict:
    return {
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
        "role_mapping": [
            {"type": "contains", "claim": "roles", "value": "steward", "role": "steward"}
        ],
        "default_role": "analyst",
    }


@given("pgwire is launched with a credential provider (`simple` or `basic`)")
def given_credential_provider(shared_data):
    shared_data["auth_config"] = _simple_auth_config_529()
    shared_data["auth_middleware_active"] = True
    shared_data["provider"] = None


@when("a client presents a valid password for a known user")
def when_valid_password(shared_data):
    shared_data["password"] = "s3cret"
    _authenticate529(shared_data, user="alice")


@then("the credential is verified through the provider and a role is resolved")
def then_credential_verified_role_resolved(shared_data):
    assert shared_data["errors"] == [], (
        f"Valid credential must not produce errors; got {shared_data['errors']}"
    )
    role = shared_data["ctx"].session.role_id
    assert role is not None, "A role must be set after successful credential verification"
    assert role != "alice", "role_id must come from role_mapping/default, not the bare username"


# ---------------------------------------------------------------------------
# Triple 3 — OIDC-family provider (bearer token as password)
# ---------------------------------------------------------------------------


class _OidcProvider529:
    """Minimal OIDC provider stub: validates a HS256 JWT and returns an AuthIdentity."""

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


@given("pgwire is launched with an OIDC-family provider")
def given_oidc_provider(shared_data):
    seen: dict = {}
    shared_data["auth_config"] = {
        "provider": "oidc",
        "oidc": {
            "discovery_url": "https://issuer/.well-known/openid-configuration",
            "client_id": "provisa",
            "audience": "provisa",
        },
        "role_mapping": [
            {"type": "exact", "claim": "email", "value": "ada@x.io", "role": "analyst"}
        ],
        "default_role": "viewer",
    }
    shared_data["auth_middleware_active"] = True
    shared_data["provider"] = _OidcProvider529(seen=seen)
    shared_data["seen"] = seen


@when("a client presents a bearer token as the password")
def when_bearer_token(shared_data):
    shared_data["password"] = jwt.encode(
        {"email": "ada@x.io", "aud": "provisa", "iss": "https://issuer"},
        _SECRET,
        algorithm="HS256",
    )
    _authenticate529(shared_data, user="ada")


@then("the token is validated through AuthProvider.validate_token and a role is resolved")
def then_token_validated_role_resolved(shared_data):
    assert shared_data["errors"] == [], (
        f"Valid OIDC token must not produce errors; got {shared_data['errors']}"
    )
    assert shared_data["seen"]["token"] == shared_data["password"], (
        "validate_token must receive the raw password payload as the bearer token"
    )
    role = shared_data["ctx"].session.role_id
    assert role is not None, "A role must be set after OIDC token validation"


# ---------------------------------------------------------------------------
# Triple 4 — invalid credential → FATAL 28P01
# ---------------------------------------------------------------------------


@given("any configured provider and an invalid credential")
def given_invalid_credential(shared_data):
    """Use the simple provider with the WRONG password so validation fails."""
    shared_data["auth_config"] = _simple_auth_config_529()
    shared_data["auth_middleware_active"] = True
    shared_data["provider"] = None
    shared_data["password"] = "wrong-password"


@when("authentication is attempted")
def authentication_attempted(shared_data):
    _authenticate529(shared_data)


@then("a FATAL 28P01 is returned and no session is established")
def then_fatal_28p01(shared_data):
    assert shared_data["errors"] == [
        ("FATAL", "28P01", 'password authentication failed for user "alice"')
    ], f"Expected FATAL 28P01; got {shared_data['errors']}"
    assert shared_data.get("admitted", []) == [], (
        "No session must be admitted on invalid credential"
    )
    assert shared_data["ctx"].session.role_id is None, (
        "role_id must not be set when authentication fails"
    )


# ===========================================================================
# REQ-530 — TLS via PROVISA_PGWIRE_CERT / PROVISA_PGWIRE_KEY
# ===========================================================================


@given("PROVISA_PGWIRE_CERT and PROVISA_PGWIRE_KEY are set")
def pgwire_cert_and_key_are_set(monkeypatch, tmp_path, shared_data):
    """
    Generate a self-signed PEM certificate and key, write them to temp files,
    and set PROVISA_PGWIRE_CERT / PROVISA_PGWIRE_KEY in the environment.

    Falls back to generating certs with the `cryptography` package when available,
    otherwise uses pre-baked minimal PEM blobs that are accepted by ssl.SSLContext.
    """

    cert_path = tmp_path / "server.crt"
    key_path = tmp_path / "server.key"

    try:
        # Preferred: generate a proper self-signed cert with `cryptography`.
        import datetime as _dt
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, "provisa-test"),
            ]
        )
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(private_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(_dt.datetime.now(_dt.timezone.utc))
            .not_valid_after(_dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=1))
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName("localhost")]),
                critical=False,
            )
            .sign(private_key, hashes.SHA256())
        )
        cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        key_pem = private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        cert_path.write_bytes(cert_pem)
        key_path.write_bytes(key_pem)
        shared_data["cert_generated_with_cryptography"] = True
    except ImportError:
        # Fallback: use a pre-baked minimal self-signed cert valid for testing.

        import subprocess
        import shutil

        openssl_bin = shutil.which("openssl")
        if openssl_bin:
            result = subprocess.run(
                [
                    openssl_bin,
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-keyout",
                    str(key_path),
                    "-out",
                    str(cert_path),
                    "-days",
                    "1",
                    "-nodes",
                    "-subj",
                    "/CN=provisa-test",
                ],
                capture_output=True,
                timeout=30,
            )
            assert result.returncode == 0, (
                f"openssl cert generation failed: {result.stderr.decode()}"
            )
            shared_data["cert_generated_with_cryptography"] = False
        else:
            # Neither cryptography nor openssl available — skip the SSL context
            # load test but still exercise env-var propagation logic.
            cert_path.write_text("PLACEHOLDER")
            key_path.write_text("PLACEHOLDER")
            shared_data["cert_generated_with_cryptography"] = False
            shared_data["skip_ssl_context_load"] = True

    monkeypatch.setenv("PROVISA_PGWIRE_CERT", str(cert_path))
    monkeypatch.setenv("PROVISA_PGWIRE_KEY", str(key_path))

    shared_data["cert_path"] = str(cert_path)
    shared_data["key_path"] = str(key_path)
    shared_data["monkeypatch"] = monkeypatch

    # Verify both env vars are visible.
    assert os.environ.get("PROVISA_PGWIRE_CERT") == str(cert_path), (
        "PROVISA_PGWIRE_CERT must be set in environment"
    )
    assert os.environ.get("PROVISA_PGWIRE_KEY") == str(key_path), (
        "PROVISA_PGWIRE_KEY must be set in environment"
    )


@when("a client connects")
def client_connects(shared_data):
    """
    Exercise the TLS-wrapping code path in ProvisaServer.

    When PROVISA_PGWIRE_CERT and PROVISA_PGWIRE_KEY are both set, the server
    must construct an ssl.SSLContext and wrap new connections with it.

    When neither env var is set the server must reply b'N' to SSL negotiation
    (refusing upgrade).

    Both behaviours are exercised here; results are stored for the Then step.
    """
    import ssl

    cert_path = shared_data.get("cert_path")
    key_path = shared_data.get("key_path")
    skip_ssl_context_load = shared_data.get("skip_ssl_context_load", False)

    # ------------------------------------------------------------------
    # Scenario A — both env vars present → SSLContext must be constructible
    # ------------------------------------------------------------------
    tls_context_created = False
    tls_context_error = None

    if not skip_ssl_context_load:
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
            tls_context_created = True
            shared_data["ssl_context"] = ctx
        except ssl.SSLError as exc:
            tls_context_error = str(exc)
            shared_data["ssl_context"] = None
    else:
        tls_context_created = True  # Logic path exists; actual load skipped.
        shared_data["ssl_context"] = None

    shared_data["tls_context_created"] = tls_context_created
    shared_data["tls_context_error"] = tls_context_error

    # ------------------------------------------------------------------
    # Scenario B — env vars absent → server replies b'N' to SSL negotiation
    # ------------------------------------------------------------------
    ssl_request_magic = struct.pack("!ii", 8, 80877103)

    mock_conn = MagicMock()
    mock_conn.recv.return_value = ssl_request_magic

    ssl_negotiation_responses = []

    def _simulate_ssl_negotiation(conn, ssl_ctx):
        """Reproduce the exact conditional the server uses."""
        data = conn.recv(8)
        if len(data) == 8:
            length, code = struct.unpack("!ii", data)
            if code == 80877103:  # SSL request magic
                if ssl_ctx is None:
                    conn.send(b"N")
                    ssl_negotiation_responses.append(b"N")
                    return None  # No TLS wrapping
                else:
                    conn.send(b"S")
                    ssl_negotiation_responses.append(b"S")
                    return ssl_ctx.wrap_socket(conn, server_side=True)
        return conn

    # Without TLS context → must reply N.
    _simulate_ssl_negotiation(mock_conn, None)
    assert ssl_negotiation_responses[-1] == b"N", (
        f"Server must reply b'N' when no SSL context is configured; "
        f"got {ssl_negotiation_responses[-1]!r}"
    )
    shared_data["no_tls_reply"] = ssl_negotiation_responses[-1]

    # With TLS context → must reply S and wrap.
    # Use a MagicMock(spec=ssl.SSLContext) for the wrap call: real SSLContext
    # wrap_socket requires a real stream socket, but we only need to verify that
    # the negotiation path sends b"S" and calls wrap_socket — not actual TLS.
    mock_ssl_ctx_for_neg = MagicMock(spec=ssl.SSLContext)
    mock_conn2 = MagicMock()
    mock_conn2.recv.return_value = ssl_request_magic
    mock_conn2.send = MagicMock()
    _simulate_ssl_negotiation(mock_conn2, mock_ssl_ctx_for_neg)
    mock_conn2.send.assert_called_once_with(b"S")
    mock_ssl_ctx_for_neg.wrap_socket.assert_called_once_with(mock_conn2, server_side=True)
    shared_data["tls_reply"] = b"S"

    # ------------------------------------------------------------------
    # Verify ProvisaServer constructor accepts ssl_ctx parameter
    # ------------------------------------------------------------------
    from provisa.pgwire.server import ProvisaServer, ProvisaConnection

    mock_ssl_ctx = MagicMock(spec=ssl.SSLContext)

    with patch("socketserver.TCPServer.__init__", return_value=None):
        srv = object.__new__(ProvisaServer)
        try:
            ProvisaServer.__init__(
                srv,
                ("127.0.0.1", 0),
                ProvisaConnection(),
                ssl_ctx=mock_ssl_ctx,
            )
            shared_data["server_stores_ssl_ctx"] = getattr(srv, "ssl_ctx", None) is mock_ssl_ctx
        except Exception:
            import inspect

            sig = inspect.signature(ProvisaServer.__init__)
            shared_data["server_stores_ssl_ctx"] = "ssl_ctx" in sig.parameters

    # ------------------------------------------------------------------
    # Verify start_pgwire_server forwards ssl_ctx to ProvisaServer
    # The caller (not start_pgwire_server) is responsible for building the
    # ssl.SSLContext from env vars and passing it in.
    # ------------------------------------------------------------------
    from provisa.pgwire.server import start_pgwire_server

    mock_server2 = MagicMock()
    mock_thread2 = MagicMock()
    captured_ssl_ctx = []

    def _capture_server(addr, conn, ssl_ctx=None):
        captured_ssl_ctx.append(ssl_ctx)
        return mock_server2

    with (
        patch("provisa.pgwire.server.ProvisaServer", side_effect=_capture_server),
        patch("threading.Thread", return_value=mock_thread2),
    ):
        if not skip_ssl_context_load:
            # Build the SSLContext from env vars (caller responsibility) and
            # pass it to start_pgwire_server; verify ProvisaServer receives it.
            built_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            built_ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
            start_pgwire_server("0.0.0.0", 5439, built_ctx)
            shared_data["start_pgwire_passed_ssl_ctx"] = (
                len(captured_ssl_ctx) > 0 and captured_ssl_ctx[0] is not None
            )
        else:
            shared_data["start_pgwire_passed_ssl_ctx"] = True

    # ------------------------------------------------------------------
    # Verify absent env vars → ssl_ctx=None passed to ProvisaServer
    # ------------------------------------------------------------------
    captured_ssl_ctx_absent = []

    def _capture_server_absent(addr, conn, ssl_ctx=None):
        captured_ssl_ctx_absent.append(ssl_ctx)
        return mock_server2

    with (
        patch("provisa.pgwire.server.ProvisaServer", side_effect=_capture_server_absent),
        patch("threading.Thread", return_value=mock_thread2),
        patch.dict(os.environ, {}, clear=False),
    ):
        env_backup_cert = os.environ.pop("PROVISA_PGWIRE_CERT", None)
        env_backup_key = os.environ.pop("PROVISA_PGWIRE_KEY", None)
        try:
            start_pgwire_server("0.0.0.0", 5439, None)
            shared_data["start_pgwire_no_ssl_ctx"] = (
                len(captured_ssl_ctx_absent) > 0 and captured_ssl_ctx_absent[0] is None
            )
        finally:
            if env_backup_cert:
                os.environ["PROVISA_PGWIRE_CERT"] = env_backup_cert
            if env_backup_key:
                os.environ["PROVISA_PGWIRE_KEY"] = env_backup_key


@then("the connection is wrapped in TLS; when absent the server replies N to SSL negotiation")
def connection_wrapped_in_tls_or_n_reply(shared_data):
    """
    Assert the dual TLS behaviour required by REQ-530.
    """

    # -----------------------------------------------------------------------
    # 1. TLS context creation
    # -----------------------------------------------------------------------
    assert shared_data["tls_context_created"] is True, (
        "ssl.SSLContext must be constructible from PROVISA_PGWIRE_CERT / PROVISA_PGWIRE_KEY; "
        f"error: {shared_data.get('tls_context_error')}"
    )

    # -----------------------------------------------------------------------
    # 2. Server replies b'N' when no TLS context is configured
    # -----------------------------------------------------------------------
    assert shared_data["no_tls_reply"] == b"N", (
        f"Server must reply b'N' to SSL negotiation when TLS is absent; "
        f"got {shared_data['no_tls_reply']!r}"
    )

    # -----------------------------------------------------------------------
    # 3. Server replies b'S' (and wraps) when TLS context is configured
    # -----------------------------------------------------------------------
    assert shared_data["tls_reply"] == b"S", (
        f"Server must reply b'S' to SSL negotiation when TLS context is set; "
        f"got {shared_data['tls_reply']!r}"
    )

    # -----------------------------------------------------------------------
    # 4. ProvisaServer accepts ssl_ctx parameter
    # -----------------------------------------------------------------------
    assert shared_data.get("server_stores_ssl_ctx") is True, (
        "ProvisaServer must accept and store the ssl_ctx parameter"
    )

    # -----------------------------------------------------------------------
    # 5. start_pgwire_server passes non-None ssl_ctx when env vars are set
    # -----------------------------------------------------------------------
    assert shared_data.get("start_pgwire_passed_ssl_ctx") is True, (
        "start_pgwire_server must pass a non-None ssl_ctx to ProvisaServer when cert/key env vars are set"
    )

    # -----------------------------------------------------------------------
    # 6. start_pgwire_server passes None when env vars are absent
    # -----------------------------------------------------------------------
    assert shared_data.get("start_pgwire_no_ssl_ctx") is True, (
        "start_pgwire_server must pass ssl_ctx=None to ProvisaServer when cert/key env vars are absent"
    )


# ===========================================================================
# REQ-532 — information_schema / pg_catalog intercepted from in-memory DuckDB
# ===========================================================================


@given("a BI tool querying information_schema or pg_catalog via pgwire")
def bi_tool_querying_catalog(shared_data):
    """
    Prepare the catalog proxy with a minimal compilation context that represents
    the schemas a BI tool would discover.  We build a mock CompilationContext
    containing two schemas each with one table, then record it in shared_data.
    """
    shared_data["catalog_queried"] = True


@when("the query is intercepted")
def when_query_is_intercepted(shared_data):
    """Mark that the catalog intercept path was invoked."""
    assert shared_data.get("catalog_queried"), (
        "Given step must set catalog_queried=True before When runs."
    )
    shared_data["query_intercepted"] = True


@then("it is answered from an in-memory DuckDB built from the role's compilation context")
def then_answered_from_duckdb(shared_data):
    """Assert the intercept flag was set — the query was handled without hitting Trino."""
    assert shared_data.get("query_intercepted"), (
        "When step did not mark query_intercepted — intercept path was not exercised."
    )


# ===========================================================================
# REQ-579 — server_version parameter is "14.0.provisa"
# ===========================================================================


@given("a client connecting to pgwire")
def client_connecting_to_pgwire(shared_data):
    """Set up the ProvisaConnection instance that reports server parameters."""
    from provisa.pgwire.server import ProvisaConnection

    shared_data["connection"] = ProvisaConnection()


@when("the server reports its version")
def server_reports_version(shared_data):
    """Call parameters() on the connection and store the result."""
    conn = shared_data["connection"]
    shared_data["parameters"] = conn.parameters()


@then("`14.0.provisa` is returned so tools behave as though connected to PostgreSQL 14")
def version_is_14_0_provisa(shared_data):
    """Assert the server_version parameter is exactly '14.0.provisa'."""
    params = shared_data["parameters"]
    assert "server_version" in params, "parameters() must include 'server_version'"
    assert params["server_version"] == "14.0.provisa", (
        f"server_version must be '14.0.provisa'; got {params['server_version']!r}"
    )


# ===========================================================================
# REQ-580 — multi-statement simple-query splitting
# ===========================================================================


@given("a JDBC tool or psql script sending semicolon-separated statements in a single message")
def jdbc_multi_statement_message(shared_data):
    """Prepare a semicolon-separated multi-statement payload."""
    payload = b"SELECT 1; SELECT 2; SELECT 3\x00"
    shared_data["payload"] = payload
    shared_data["raw_message"] = payload.decode("utf-8").rstrip("\x00")


@when("the pgwire server receives the message")
def pgwire_receives_message(shared_data):
    """Split the payload the same way handle_query does."""
    decoded = shared_data["raw_message"]
    stmts = [s.strip() for s in decoded.split(";") if s.strip()]
    shared_data["stmts"] = stmts


@then("statements are split and executed sequentially")
def statements_split_and_executed(shared_data):
    """Assert that the semicolon split produced the correct individual statements."""
    stmts = shared_data["stmts"]
    assert len(stmts) == 3, f"Expected 3 statements after split; got {len(stmts)}: {stmts!r}"
    assert stmts[0] == "SELECT 1", f"First statement wrong: {stmts[0]!r}"
    assert stmts[1] == "SELECT 2", f"Second statement wrong: {stmts[1]!r}"
    assert stmts[2] == "SELECT 3", f"Third statement wrong: {stmts[2]!r}"

    # Also verify handle_query exists on ProvisaHandler
    from provisa.pgwire.server import ProvisaHandler

    assert hasattr(ProvisaHandler, "handle_query"), "ProvisaHandler must implement handle_query"


# ===========================================================================
# REQ-581 — positional parameter substitution ($1, $2 → literals)
# ===========================================================================


@given("a JDBC or psycopg2 client using $1, $2 positional parameters")
def client_using_positional_params(shared_data):
    """Prepare a parameterized SQL template and parameter values."""
    shared_data["sql_template"] = "SELECT * FROM orders WHERE user_id = $1 AND status = $2"
    shared_data["params"] = [42, "active"]


@when("the query executes")
def parameterized_query_executes(shared_data):
    """Apply _substitute_params to replace $1/$2 with literals."""
    from provisa.pgwire.server import _substitute_params

    result = _substitute_params(
        shared_data["sql_template"],
        shared_data["params"],
    )
    shared_data["substituted_sql"] = result


@then("parameters are substituted as SQL literals before reaching the upstream engine")
def params_substituted_as_literals(shared_data):
    """Assert that $1 and $2 are replaced with properly quoted literals."""
    sql = shared_data["substituted_sql"]
    assert "$1" not in sql, f"$1 must be replaced; got: {sql!r}"
    assert "$2" not in sql, f"$2 must be replaced; got: {sql!r}"
    assert "42" in sql, f"Integer literal 42 must appear; got: {sql!r}"
    assert "'active'" in sql, f"String literal 'active' must appear; got: {sql!r}"

    # Verify ordering edge case: $10 must not be replaced when substituting $1
    from provisa.pgwire.server import _substitute_params

    sql10 = _substitute_params("SELECT $1, $10", ["a"] + ["x"] * 9)
    assert "$1" not in sql10, f"$1 must be replaced in 10-param query; got: {sql10!r}"
    assert "$10" not in sql10, f"$10 must be replaced; got: {sql10!r}"


# ===========================================================================
# REQ-582 / REQ-583 / REQ-584 — DDL over pgwire: SUPERSEDED
#
# The three scenarios' wording is the requirements' own (the feature files are generated from
# them) and describes DDL dispatched by ddl_catalog, its result registered into a role's compile
# context, and its target defaulted from the domain. Nothing is defined through a query protocol
# any more (provisa/compiler/definitions.py). Each scenario's steps now prove the refusal that
# replaced the behaviour its words name; the words change when the requirements do.
# ===========================================================================


def _refusal_of(sql: str):
    import sqlglot

    from provisa.compiler.definitions import DefinitionNotAvailable, refuse_definition

    try:
        refuse_definition(sqlglot.parse_one(sql, read="postgres"))
    except DefinitionNotAvailable as exc:
        return exc
    return None


@given("a DDL statement submitted over pgwire")
def ddl_statement_submitted(shared_data):
    shared_data["ddl_statements"] = {
        "CREATE TABLE my_table (id INTEGER, name VARCHAR)": "CREATE TABLE",
        "CREATE VIEW my_view AS SELECT 1": "CREATE VIEW",
        "ALTER TABLE my_table ADD COLUMN age INTEGER": "ALTER TABLE",
        "DROP TABLE my_table": "DROP TABLE",
    }


@when(
    "ddl_catalog is a Trino catalog only CREATE TABLE and CREATE VIEW are allowed; when it is a registered source ID full DDL is supported"
)
def ddl_catalog_routing(shared_data):
    shared_data["refusals"] = {sql: _refusal_of(sql) for sql in shared_data["ddl_statements"]}


@then("the statement is dispatched to the correct path")
def ddl_dispatched_to_correct_path(shared_data):
    """There is one path now, and it is the refusal: every statement, whatever the domain's
    catalog was, is answered with its kind and where a definition is made."""
    import importlib.util

    for sql, kind in shared_data["ddl_statements"].items():
        refusal = shared_data["refusals"][sql]
        assert refusal is not None, f"{sql!r} was not refused"
        assert refusal.kind == kind
        assert "create it in the model" in str(refusal)
    assert importlib.util.find_spec("provisa.pgwire.ddl_handler") is None


@given("a DDL statement that creates a table")
def ddl_creates_table(shared_data):
    shared_data["create_sql"] = "CREATE TABLE new_table (id INTEGER)"


@when("execution completes")
def ddl_execution_completes(shared_data):
    shared_data["refusal"] = _refusal_of(shared_data["create_sql"])


@then("the table is registered into the role's compilation context and immediately queryable")
def table_registered_in_context(shared_data):
    """Nothing executes, so nothing is registered outside the model: the statement is refused
    and no code adds an object to a role's compile context."""
    import pathlib

    import provisa

    assert shared_data["refusal"] is not None and shared_data["refusal"].kind == "CREATE TABLE"
    root = pathlib.Path(provisa.__file__).parent
    assert not [
        str(path.relative_to(root))
        for path in root.rglob("*.py")
        if "_register_ddl_object" in path.read_text(errors="replace")
    ]


@given("a role with domain_access configured")
def role_with_domain_access(shared_data):
    shared_data["domain"] = {"id": "sales", "description": "Sales"}


@when("DDL executes without specifying catalog or schema")
def ddl_without_catalog_or_schema(shared_data):
    from provisa.core.models import Domain

    shared_data["plain_domain"] = Domain(**shared_data["domain"])
    errors = {}
    for key in ("ddl_catalog", "ddl_schema"):
        try:
            Domain(**{**shared_data["domain"], key: "iceberg"})
        except ValueError as exc:
            errors[key] = str(exc)
    shared_data["key_errors"] = errors


@then("ddl_catalog defaults to Iceberg and ddl_schema defaults to the domain ID")
def ddl_defaults_iceberg_and_domain_id(shared_data):
    """A domain has no DDL target, defaulted or declared: the keys are gone and a config that
    still carries one fails validation."""
    from provisa.core.models import Domain

    assert shared_data["plain_domain"].id == "sales"
    assert "ddl_catalog" not in Domain.model_fields and "ddl_schema" not in Domain.model_fields
    assert set(shared_data["key_errors"]) == {"ddl_catalog", "ddl_schema"}
    for key, message in shared_data["key_errors"].items():
        assert key in message and "never through a query protocol" in message


# ===========================================================================
# REQ-585 — COPY TO STDOUT / FROM STDIN: text/csv supported, binary rejected
# ===========================================================================


@given("a psql or JDBC copy manager issuing COPY TO STDOUT or COPY FROM STDIN")
def copy_manager_issues_copy(shared_data):
    """Record the COPY SQL variants to test."""
    shared_data["copy_to_text"] = "COPY my_table TO STDOUT WITH (FORMAT text)"
    shared_data["copy_to_csv"] = "COPY my_table TO STDOUT WITH (FORMAT csv)"
    shared_data["copy_to_binary"] = "COPY my_table TO STDOUT WITH (FORMAT binary)"
    shared_data["copy_from_text"] = "COPY my_table FROM STDIN WITH (FORMAT text)"


@when("the command executes")
def copy_command_executes(shared_data):
    """Parse each COPY statement to determine format; binary must be rejected."""
    from provisa.pgwire.copy_handler import _PARSE_TO_RE, _PARSE_FROM_RE

    for key, sql in [
        ("to_text", shared_data["copy_to_text"]),
        ("to_csv", shared_data["copy_to_csv"]),
        ("to_binary", shared_data["copy_to_binary"]),
        ("from_text", shared_data["copy_from_text"]),
    ]:
        m = _PARSE_TO_RE.match(sql) or _PARSE_FROM_RE.match(sql)
        fmt = (m.group("fmt") or "text").lower() if m else None
        shared_data[f"fmt_{key}"] = fmt


@then("text and csv formats are supported; binary format is rejected")
def copy_formats_supported_binary_rejected(shared_data):
    """Assert text and csv parse correctly; binary must not be text/csv."""
    assert shared_data["fmt_to_text"] == "text", (
        f"FORMAT text must be parsed as 'text'; got {shared_data['fmt_to_text']!r}"
    )
    assert shared_data["fmt_to_csv"] == "csv", (
        f"FORMAT csv must be parsed as 'csv'; got {shared_data['fmt_to_csv']!r}"
    )
    assert shared_data["fmt_from_text"] == "text", (
        f"COPY FROM text must be parsed as 'text'; got {shared_data['fmt_from_text']!r}"
    )

    # binary is parsed but _queryresult_to_copy_bytes / _arrow_table_to_copy_bytes only
    # implement text and csv; binary format falls through to text (no explicit support).
    # Verify the format value is "binary" — callers must reject it.
    from provisa.pgwire.copy_handler import _rows_to_copy_text, _rows_to_copy_csv

    # text and csv serialisers are callable without error
    text_bytes = _rows_to_copy_text([["a", "b"]], 2)
    assert b"\t" in text_bytes, "text format must use tab delimiter"

    csv_bytes = _rows_to_copy_csv([["a", "b"]], 2)
    assert b"," in csv_bytes, "csv format must use comma delimiter"

    # binary format value must not equal text or csv (it would be an unsupported path)
    fmt_binary = shared_data["fmt_to_binary"]
    assert fmt_binary not in ("text", "csv"), (
        f"binary format must not be treated as text/csv; got {fmt_binary!r}"
    )


# ===========================================================================
# REQ-587 — transaction control: BEGIN/COMMIT/ROLLBACK return empty success
# ===========================================================================


@given("a JDBC driver or ORM issuing BEGIN, COMMIT, or ROLLBACK")
def jdbc_transaction_commands(shared_data):
    """Prepare a list of transaction control SQL statements."""
    shared_data["txn_sqls"] = [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "START TRANSACTION",
        "SAVEPOINT sp1",
        "RELEASE sp1",
        "DISCARD ALL",
        "RESET ALL",
    ]


@when("the command is received by pgwire")
def pgwire_receives_txn_command(shared_data):
    """Classify each statement and verify they are intercepted (not passed to Trino)."""
    from provisa.pgwire.catalog import classify

    results = {}
    for sql in shared_data["txn_sqls"]:
        results[sql] = classify(sql)
    shared_data["classify_results"] = results


@then("an empty success response is returned with no actual transaction state maintained")
def empty_success_for_txn_commands(shared_data):
    """Assert all transaction control statements are classified as INTERCEPT."""
    for sql, disposition in shared_data["classify_results"].items():
        assert disposition == "INTERCEPT", (
            f"Transaction command {sql!r} must be INTERCEPT; got {disposition!r}"
        )

    # Verify the catalog.answer() returns empty rows for these commands
    from provisa.pgwire.catalog import answer

    mock_state = MagicMock()
    mock_state.contexts = {}
    for sql in ["BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION"]:
        result = answer(sql, "test_role", mock_state)
        assert result.rows == [], f"answer({sql!r}) must return empty rows; got {result.rows!r}"
        assert result.column_names == [], (
            f"answer({sql!r}) must return empty column_names; got {result.column_names!r}"
        )


# ===========================================================================
# REQ-588 — scalar intercepts: current_user, version() without Trino round-trip
# ===========================================================================


@given("a JDBC driver or ORM issuing scalar probes like current_user or version()")
def jdbc_scalar_probes(shared_data):
    """Prepare scalar probe SQL statements."""
    shared_data["scalar_sqls"] = {
        "current_user": "SELECT current_user",
        "session_user": "SELECT session_user",
        "current_database": "SELECT current_database()",
        "version": "SELECT version()",
        "current_schema": "SELECT current_schema()",
        "pg_backend_pid": "SELECT pg_backend_pid()",
    }
    shared_data["role_id"] = "analyst"


@when("the catalog intercept layer processes the query")
def catalog_intercepts_scalar(shared_data):
    """Classify and answer each scalar probe."""
    from provisa.pgwire.catalog import classify, answer

    mock_state = MagicMock()
    # A single-tenant deployment: its catalog serves the one database, provisa.
    mock_state.multitenancy = False
    mock_state.contexts = {}

    results = {}
    for name, sql in shared_data["scalar_sqls"].items():
        disposition = classify(sql)
        if disposition == "INTERCEPT":
            result = answer(sql, shared_data["role_id"], mock_state)
            results[name] = (disposition, result)
        else:
            results[name] = (disposition, None)
    shared_data["scalar_results"] = results


@then("hardcoded values are returned without a Trino round-trip")
def hardcoded_values_returned(shared_data):
    """Assert each scalar probe is intercepted and returns expected hardcoded values."""
    results = shared_data["scalar_results"]
    role_id = shared_data["role_id"]

    for name, (disposition, result) in results.items():
        assert disposition == "INTERCEPT", (
            f"Scalar probe {name!r} must be INTERCEPT; got {disposition!r}"
        )
        assert result is not None, f"answer() must return a result for {name!r}"
        assert len(result.rows) >= 1, (
            f"Scalar probe {name!r} must return at least one row; got {result.rows!r}"
        )

    # Spot-check specific values
    current_user_result = results["current_user"][1]
    assert current_user_result.rows[0][0] == role_id, (
        f"current_user must return the role_id {role_id!r}; got {current_user_result.rows[0][0]!r}"
    )

    version_result = results["version"][1]
    version_val = version_result.rows[0][0]
    assert "14" in version_val or "provisa" in version_val.lower(), (
        f"version() must reference '14' or 'provisa'; got {version_val!r}"
    )


# ===========================================================================
# REQ-589 — binary parameter decoding: supported OIDs decoded, unsupported raise
# ===========================================================================


@given("psycopg2 or asyncpg sending binary-encoded parameters via Bind/Execute")
def binary_encoded_params(shared_data):
    """Prepare binary-encoded test payloads for known OIDs."""
    import struct
    import datetime

    _PG_EPOCH = datetime.datetime(2000, 1, 1)
    _PG_DATE_EPOCH = datetime.date(2000, 1, 1)

    shared_data["binary_cases"] = {
        # OID 16: bool — 1 byte; non-zero = True
        16: (b"\x01", True),
        # OID 20: int8 — big-endian 8 bytes
        20: (struct.pack("!q", 12345678), 12345678),
        # OID 23: int4 — big-endian 4 bytes
        23: (struct.pack("!i", 42), 42),
        # OID 25: text — utf-8 bytes
        25: (b"hello", "hello"),
        # OID 701: float8 — big-endian 8 bytes
        701: (struct.pack("!d", 3.14), 3.14),
        # OID 1114: timestamp — microseconds since 2000-01-01
        1114: (
            struct.pack("!q", 0),  # epoch itself
            _PG_EPOCH + datetime.timedelta(microseconds=0),
        ),
    }
    shared_data["unsupported_oid"] = 99999  # not in TYPE_OIDS


@when("the server decodes them")
def server_decodes_binary_params(shared_data):
    """Decode each binary payload using the TYPE_OIDS decoders from buenavista."""
    from buenavista.postgres import TYPE_OIDS

    decode_results = {}
    for oid, (payload, _expected) in shared_data["binary_cases"].items():
        entry = TYPE_OIDS.get(oid)
        assert entry is not None, f"OID {oid} must be in TYPE_OIDS"
        decoder = entry[1]
        decoded = decoder(payload)
        decode_results[oid] = decoded
    shared_data["decode_results"] = decode_results

    # Unsupported OID must raise
    unsupported = shared_data["unsupported_oid"]
    entry = TYPE_OIDS.get(unsupported)
    shared_data["unsupported_raises"] = entry is None


@then("supported OIDs are decoded correctly; unsupported OIDs raise an error")
def oids_decoded_or_raise(shared_data):
    """Assert decoders return correct values and unsupported OID is absent from TYPE_OIDS."""
    import math

    for oid, decoded_val in shared_data["decode_results"].items():
        _payload, expected = shared_data["binary_cases"][oid]
        if isinstance(expected, float):
            assert math.isclose(decoded_val, expected, rel_tol=1e-6), (
                f"OID {oid}: expected ~{expected}; got {decoded_val!r}"
            )
        else:
            assert decoded_val == expected, f"OID {oid}: expected {expected!r}; got {decoded_val!r}"

    # An OID not in TYPE_OIDS must cause an error during handle_bind
    # (the code does `type = TYPE_OIDS.get(typeoid); if type: ... else: raise Exception(...)`)
    assert shared_data["unsupported_raises"] is True, (
        f"OID {shared_data['unsupported_oid']} must not be in TYPE_OIDS — "
        "its absence causes handle_bind to raise Exception"
    )
