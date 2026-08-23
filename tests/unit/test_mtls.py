# Copyright (c) 2026 Kenneth Stott
# Canary: 1045acb6-9450-48db-b6d9-c0b9105807a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1228 — client-certificate verification (mutual TLS) on the wire servers.

Three things are asserted here. That the policy is read from the environment exactly as configured,
with every ambiguous configuration raising rather than being guessed at. That the policy reaches the
transport — an ``ssl.SSLContext`` that really refuses a client presenting no certificate, and the
gRPC and Flight spellings carrying the same decision. And that principal binding, when switched on,
refuses a connection whose certificate names someone other than the authenticating user.

The handshake tests run a real TLS exchange over a loopback socket against a CA generated in the
fixture, because a test that only inspects ``verify_mode`` would pass on a context whose CA never
loaded.
"""

from __future__ import annotations

import datetime
import socket
import ssl
import threading

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from provisa.security.mtls import (
    ClientAuth,
    apply_to_context,
    assert_principal_binding,
    flight_tls_kwargs,
    peer_common_name,
    resolve_client_auth,
)

_ENV_VARS = (
    "PROVISA_MTLS_CLIENT_CA",
    "PROVISA_MTLS_MODE",
    "PROVISA_MTLS_BIND_PRINCIPAL",
    "PROVISA_PGWIRE_CLIENT_CA",
    "PROVISA_PGWIRE_MTLS_MODE",
    "PROVISA_PGWIRE_MTLS_BIND_PRINCIPAL",
)

_PGWIRE_ENVS = (
    "PROVISA_PGWIRE_CLIENT_CA",
    "PROVISA_PGWIRE_MTLS_MODE",
    "PROVISA_PGWIRE_MTLS_BIND_PRINCIPAL",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """No mTLS variable leaks in from the developer's shell or a neighbouring test."""
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


# ── Certificate authority ─────────────────────────────────────────────────────


def _key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _pem(cert, key) -> tuple[bytes, bytes]:
    return (
        cert.public_bytes(serialization.Encoding.PEM),
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ),
    )


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def _builder(subject: x509.Name, issuer: x509.Name, public_key):
    now = datetime.datetime.now(datetime.timezone.utc)
    return (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
    )


class _Authority:
    """A CA plus the certificates it signs, written to disk for the ssl module to read."""

    def __init__(self, tmp_path):
        self.key = _key()
        self.cert = (
            _builder(_name("provisa-test-ca"), _name("provisa-test-ca"), self.key.public_key())
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .sign(self.key, hashes.SHA256())
        )
        self.ca_path = str(tmp_path / "ca.pem")
        with open(self.ca_path, "wb") as handle:
            handle.write(self.cert.public_bytes(serialization.Encoding.PEM))
        self._tmp_path = tmp_path

    def issue(self, common_name: str, *, server: bool = False) -> tuple[str, str]:
        """Sign a certificate for ``common_name``; return the (cert, key) paths."""
        key = _key()
        builder = _builder(_name(common_name), self.cert.subject, key.public_key())
        if server:
            builder = builder.add_extension(
                x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False
            )
        cert = builder.sign(self.key, hashes.SHA256())
        cert_pem, key_pem = _pem(cert, key)
        cert_path = str(self._tmp_path / f"{common_name}-cert.pem")
        key_path = str(self._tmp_path / f"{common_name}-key.pem")
        with open(cert_path, "wb") as handle:
            handle.write(cert_pem)
        with open(key_path, "wb") as handle:
            handle.write(key_pem)
        return cert_path, key_path


@pytest.fixture
def ca(tmp_path) -> _Authority:
    return _Authority(tmp_path)


# ── Configuration ─────────────────────────────────────────────────────────────


def test_no_configuration_means_no_client_certificates():
    assert resolve_client_auth(*_PGWIRE_ENVS) is None


def test_naming_a_ca_and_no_mode_means_required(ca, monkeypatch):
    """Naming a trust anchor is the act of deciding client certificates matter."""
    monkeypatch.setenv("PROVISA_PGWIRE_CLIENT_CA", ca.ca_path)

    auth = resolve_client_auth(*_PGWIRE_ENVS)

    assert auth == ClientAuth(ca_path=ca.ca_path, required=True, bind_principal=False)


def test_optional_mode_does_not_demand_a_certificate(ca, monkeypatch):
    monkeypatch.setenv("PROVISA_PGWIRE_CLIENT_CA", ca.ca_path)
    monkeypatch.setenv("PROVISA_PGWIRE_MTLS_MODE", "optional")

    auth = resolve_client_auth(*_PGWIRE_ENVS)

    assert auth is not None
    assert auth.required is False


def test_the_node_wide_setting_applies_where_a_protocol_names_none(ca, monkeypatch):
    monkeypatch.setenv("PROVISA_MTLS_CLIENT_CA", ca.ca_path)
    monkeypatch.setenv("PROVISA_MTLS_MODE", "optional")

    auth = resolve_client_auth(*_PGWIRE_ENVS)

    assert auth is not None
    assert (auth.ca_path, auth.required) == (ca.ca_path, False)


def test_a_protocol_override_wins_over_the_node_wide_setting(ca, monkeypatch):
    monkeypatch.setenv("PROVISA_MTLS_CLIENT_CA", ca.ca_path)
    monkeypatch.setenv("PROVISA_MTLS_MODE", "optional")
    monkeypatch.setenv("PROVISA_PGWIRE_MTLS_MODE", "required")

    auth = resolve_client_auth(*_PGWIRE_ENVS)

    assert auth is not None
    assert auth.required is True


def test_a_mode_with_no_ca_refuses_to_start(monkeypatch):
    """A deployment that believes it verifies client certificates and does not is the worst case."""
    monkeypatch.setenv("PROVISA_PGWIRE_MTLS_MODE", "required")

    with pytest.raises(ValueError, match="no client CA is configured"):
        resolve_client_auth(*_PGWIRE_ENVS)


def test_a_ca_bundle_that_does_not_exist_refuses_to_start(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_PGWIRE_CLIENT_CA", str(tmp_path / "absent.pem"))

    with pytest.raises(ValueError, match="does not exist"):
        resolve_client_auth(*_PGWIRE_ENVS)


def test_an_unrecognized_mode_refuses_to_start(ca, monkeypatch):
    """Not read as the nearest safe neighbour — a typo must be visible, not silently interpreted."""
    monkeypatch.setenv("PROVISA_PGWIRE_CLIENT_CA", ca.ca_path)
    monkeypatch.setenv("PROVISA_PGWIRE_MTLS_MODE", "requird")

    with pytest.raises(ValueError, match="not one of 'required', 'optional'"):
        resolve_client_auth(*_PGWIRE_ENVS)


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_principal_binding_reads_the_usual_truths(ca, monkeypatch, value):
    monkeypatch.setenv("PROVISA_PGWIRE_CLIENT_CA", ca.ca_path)
    monkeypatch.setenv("PROVISA_PGWIRE_MTLS_BIND_PRINCIPAL", value)

    auth = resolve_client_auth(*_PGWIRE_ENVS)

    assert auth is not None
    assert auth.bind_principal is True


def test_principal_binding_is_off_by_default(ca, monkeypatch):
    monkeypatch.setenv("PROVISA_PGWIRE_CLIENT_CA", ca.ca_path)

    auth = resolve_client_auth(*_PGWIRE_ENVS)

    assert auth is not None
    assert auth.bind_principal is False


# ── The stdlib transport: pgwire and Bolt ─────────────────────────────────────


def test_no_policy_leaves_the_context_alone():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    apply_to_context(ctx, None)

    assert ctx.verify_mode is ssl.CERT_NONE


def test_required_puts_cert_required_on_the_context(ca):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    apply_to_context(ctx, ClientAuth(ca.ca_path, required=True, bind_principal=False))

    assert ctx.verify_mode is ssl.CERT_REQUIRED
    assert ctx.get_ca_certs(), "the CA bundle was not loaded onto the context"


def test_optional_puts_cert_optional_on_the_context(ca):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    apply_to_context(ctx, ClientAuth(ca.ca_path, required=False, bind_principal=False))

    assert ctx.verify_mode is ssl.CERT_OPTIONAL
    assert ctx.get_ca_certs(), "the CA bundle was not loaded onto the context"


# ── A real handshake ──────────────────────────────────────────────────────────


def _server_context(ca: _Authority, auth: ClientAuth | None) -> ssl.SSLContext:
    cert_path, key_path = ca.issue("localhost", server=True)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert_path, key_path)
    apply_to_context(ctx, auth)
    return ctx


def _client_context(ca: _Authority, client_cn: str | None) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=ca.ca_path)
    if client_cn is not None:
        ctx.load_cert_chain(*ca.issue(client_cn))
    return ctx


def _handshake(server_ctx: ssl.SSLContext, client_ctx: ssl.SSLContext) -> dict:
    """Run one TLS handshake over loopback and report what the server made of it.

    Returns ``{"peer_cert": ...}`` when the server accepted the connection and ``{"error": ...}``
    when it refused. The verdict is read from the server rather than the client because under TLS
    1.3 the client's certificate arrives after it has finished its own handshake — the client sees
    success and only the server sees the rejection.
    """
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    outcome: dict = {}

    def serve() -> None:
        raw, _ = listener.accept()
        try:
            with server_ctx.wrap_socket(raw, server_side=True) as wrapped:
                outcome["peer_cert"] = wrapped.getpeercert()
        except ssl.SSLError as exc:
            outcome["error"] = exc
        finally:
            raw.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=10) as raw:
            try:
                with client_ctx.wrap_socket(raw, server_hostname="localhost"):
                    pass
            except ssl.SSLError as exc:
                outcome.setdefault("error", exc)
    finally:
        thread.join(timeout=10)
        listener.close()
    return outcome


def test_required_refuses_a_client_that_presents_no_certificate(ca):
    """The point of the whole feature — the refusal happens at the handshake, before any credential."""
    server_ctx = _server_context(ca, ClientAuth(ca.ca_path, required=True, bind_principal=False))

    outcome = _handshake(server_ctx, _client_context(ca, client_cn=None))

    assert isinstance(outcome.get("error"), ssl.SSLError)
    assert "peer_cert" not in outcome


def test_required_admits_a_client_the_ca_signed(ca):
    server_ctx = _server_context(ca, ClientAuth(ca.ca_path, required=True, bind_principal=False))

    outcome = _handshake(server_ctx, _client_context(ca, client_cn="alice"))

    assert "error" not in outcome
    assert peer_common_name(outcome["peer_cert"]) == "alice"


def test_optional_admits_a_client_that_presents_no_certificate(ca):
    server_ctx = _server_context(ca, ClientAuth(ca.ca_path, required=False, bind_principal=False))

    outcome = _handshake(server_ctx, _client_context(ca, client_cn=None))

    assert "error" not in outcome
    assert not outcome["peer_cert"]
    assert peer_common_name(outcome["peer_cert"]) is None


def test_a_certificate_from_another_ca_is_refused_even_when_optional(ca, tmp_path):
    """Optional means a certificate may be absent, never that an unsigned one is accepted."""
    other = tmp_path / "other"
    other.mkdir()
    stranger = _Authority(other)
    server_ctx = _server_context(ca, ClientAuth(ca.ca_path, required=False, bind_principal=False))
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client_ctx.load_verify_locations(cafile=ca.ca_path)
    client_ctx.load_cert_chain(*stranger.issue("mallory"))

    outcome = _handshake(server_ctx, client_ctx)

    assert isinstance(outcome.get("error"), ssl.SSLError)
    assert "peer_cert" not in outcome


# ── Principal binding ─────────────────────────────────────────────────────────


def test_no_binding_configured_accepts_any_signed_certificate(ca):
    auth = ClientAuth(ca.ca_path, required=True, bind_principal=False)

    assert_principal_binding(auth, {"subject": ((("commonName", "svc-etl"),),)}, "alice")


def test_binding_accepts_a_certificate_that_names_the_user(ca):
    auth = ClientAuth(ca.ca_path, required=True, bind_principal=True)

    assert_principal_binding(auth, {"subject": ((("commonName", "alice"),),)}, "alice")


def test_binding_refuses_a_certificate_that_names_someone_else(ca):
    """A stolen password is useless without that user's certificate."""
    auth = ClientAuth(ca.ca_path, required=True, bind_principal=True)

    with pytest.raises(PermissionError, match="cannot authenticate as 'alice'"):
        assert_principal_binding(auth, {"subject": ((("commonName", "carol"),),)}, "alice")


def test_binding_refuses_a_connection_with_no_certificate(ca):
    auth = ClientAuth(ca.ca_path, required=False, bind_principal=True)

    with pytest.raises(PermissionError, match="no common name"):
        assert_principal_binding(auth, {}, "alice")


def test_the_common_name_is_found_among_other_subject_fields():
    peer_cert = {
        "subject": (
            (("countryName", "US"),),
            (("organizationName", "Provisa"),),
            (("commonName", "alice"),),
        )
    }

    assert peer_common_name(peer_cert) == "alice"


def test_a_pre_handshake_certificate_has_no_name():
    assert peer_common_name(None) is None


# ── The binding reaches the surfaces ──────────────────────────────────────────


class _Writer:
    """A stand-in for Bolt's asyncio StreamWriter, carrying one peer certificate."""

    def __init__(self, common_name: str | None) -> None:
        self._ssl_object = _PeerSocket(common_name) if common_name else None

    def get_extra_info(self, name: str):
        assert name == "ssl_object"
        return self._ssl_object


class _PeerSocket:
    def __init__(self, common_name: str) -> None:
        self._common_name = common_name

    def getpeercert(self) -> dict:
        return {"subject": ((("commonName", self._common_name),),)}


@pytest.mark.asyncio
async def test_bolt_refuses_a_logon_whose_certificate_names_someone_else(ca, monkeypatch):
    """The binding is checked before the password, so a mismatch is not a credential answer."""
    from provisa.bolt.session import BoltSession

    monkeypatch.setenv("PROVISA_BOLT_CLIENT_CA", ca.ca_path)
    monkeypatch.setenv("PROVISA_BOLT_MTLS_BIND_PRINCIPAL", "true")
    session = BoltSession(_Writer("carol"), (5, 4))  # type: ignore[arg-type]
    failures: list[tuple[str, str]] = []
    session.send_failure = lambda code, message: failures.append((code, message))  # type: ignore[method-assign]

    async def _never(*args, **kwargs):
        raise AssertionError("the credential layer must not be reached")

    session._resolve_user = _never  # type: ignore[method-assign]

    await session.handle_logon([{"principal": "alice", "credentials": "s3cret"}])

    assert failures[0][0] == "Neo.ClientError.Security.Unauthorized"
    assert "cannot authenticate as 'alice'" in failures[0][1]


@pytest.mark.asyncio
async def test_bolt_admits_a_logon_whose_certificate_names_the_user(ca, monkeypatch):
    from provisa.bolt.session import BoltSession

    monkeypatch.setenv("PROVISA_BOLT_CLIENT_CA", ca.ca_path)
    monkeypatch.setenv("PROVISA_BOLT_MTLS_BIND_PRINCIPAL", "true")
    session = BoltSession(_Writer("alice"), (5, 4))  # type: ignore[arg-type]
    session.send_failure = lambda code, message: None  # type: ignore[method-assign]
    session.send_success = lambda meta: None  # type: ignore[method-assign]

    async def _resolve(scheme, principal, credentials):
        return (principal, ["analyst"])

    session._resolve_user = _resolve  # type: ignore[method-assign]

    await session.handle_logon([{"principal": "alice", "credentials": "s3cret"}])

    assert session.user_id == "alice"


def test_pgwire_refuses_a_startup_user_the_certificate_does_not_name(ca, monkeypatch):
    from provisa.pgwire.server import ProvisaHandler

    monkeypatch.setenv("PROVISA_PGWIRE_CLIENT_CA", ca.ca_path)
    monkeypatch.setenv("PROVISA_PGWIRE_MTLS_BIND_PRINCIPAL", "true")
    handler = object.__new__(ProvisaHandler)
    handler.request = _WrappedSocket("carol")

    with pytest.raises(PermissionError, match="cannot authenticate as 'alice'"):
        handler._assert_peer_binding("alice")


def test_pgwire_admits_a_startup_user_the_certificate_names(ca, monkeypatch):
    from provisa.pgwire.server import ProvisaHandler

    monkeypatch.setenv("PROVISA_PGWIRE_CLIENT_CA", ca.ca_path)
    monkeypatch.setenv("PROVISA_PGWIRE_MTLS_BIND_PRINCIPAL", "true")
    handler = object.__new__(ProvisaHandler)
    handler.request = _WrappedSocket("alice")

    handler._assert_peer_binding("alice")


class _WrappedSocket(ssl.SSLSocket):
    """An SSLSocket the isinstance check accepts, without a handshake behind it."""

    def __init__(self, common_name: str) -> None:  # noqa: D107 — deliberately not SSLSocket.__init__
        self._common_name = common_name

    def getpeercert(self, binary_form: bool = False):  # type: ignore[override]
        return {"subject": ((("commonName", self._common_name),),)}


# ── The gRPC and Flight spellings ─────────────────────────────────────────────


def test_flight_asks_for_nothing_when_mtls_is_off():
    assert flight_tls_kwargs(None) == {}


def test_flight_carries_the_roots_and_the_demand(ca):
    kwargs = flight_tls_kwargs(ClientAuth(ca.ca_path, required=True, bind_principal=False))

    assert kwargs["verify_client"] is True
    assert b"BEGIN CERTIFICATE" in kwargs["root_certificates"]


def test_flight_supplies_roots_without_demanding_when_optional(ca):
    kwargs = flight_tls_kwargs(ClientAuth(ca.ca_path, required=False, bind_principal=False))

    assert kwargs["verify_client"] is False
    assert b"BEGIN CERTIFICATE" in kwargs["root_certificates"]


def test_grpc_credentials_carry_the_policy(ca, monkeypatch):
    """grpc's builder is opaque, so the arguments it receives are what can be asserted."""
    import provisa.security.mtls as mtls_module

    captured: dict = {}

    class _FakeGrpc:
        @staticmethod
        def ssl_server_credentials(pairs, **kwargs):
            captured["pairs"] = pairs
            captured.update(kwargs)
            return "credentials"

    monkeypatch.setitem(__import__("sys").modules, "grpc", _FakeGrpc)

    result = mtls_module.grpc_server_credentials(
        b"cert", b"key", ClientAuth(ca.ca_path, required=True, bind_principal=False)
    )

    assert result == "credentials"
    assert captured["pairs"] == [(b"key", b"cert")]
    assert captured["require_client_auth"] is True
    assert b"BEGIN CERTIFICATE" in captured["root_certificates"]


def test_grpc_credentials_ask_for_no_client_certificate_when_mtls_is_off(monkeypatch):
    import provisa.security.mtls as mtls_module

    captured: dict = {}

    class _FakeGrpc:
        @staticmethod
        def ssl_server_credentials(pairs, **kwargs):
            captured["pairs"] = pairs
            captured.update(kwargs)
            return "credentials"

    monkeypatch.setitem(__import__("sys").modules, "grpc", _FakeGrpc)

    mtls_module.grpc_server_credentials(b"cert", b"key", None)

    assert captured["pairs"] == [(b"key", b"cert")]
    assert "root_certificates" not in captured
