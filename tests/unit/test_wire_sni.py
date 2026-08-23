# Copyright (c) 2026 Kenneth Stott
# Canary: 6f9c2a41-0d7b-4e58-93a1-c4f8e07b6d2a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1234 — the org selector on a wire protocol.

Two things are proved. The hostname rule is one rule: the labels of ``acme.provisa.dev`` mean the
same thing whether they arrived in an HTTP ``Host`` header or a TLS ClientHello, so the HTTP
middleware and the wire servers are checked against the same table of cases.

Then the capture itself is exercised over a real TLS handshake rather than by calling the callback
directly, because what matters is that the name survives from the ClientHello to the object the
protocol handler holds afterwards — pgwire reads it off the wrapped socket, Bolt off the transport's
``ssl_object``, and only a handshake produces those.
"""

from __future__ import annotations

import socket
import ssl
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.security.sni import (
    indicated_host,
    install,
    is_control_plane_host,
    org_from_host,
)

# Requirements: REQ-1234, REQ-1276


# ── The hostname rule ─────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "host,expected",
    [
        ("acme.provisa.dev", "acme"),
        ("ACME.Provisa.Dev", "acme"),  # DNS is case-insensitive; the org id is lowercase
        ("acme.provisa.dev:5439", "acme"),  # a Host header carries the port; SNI does not
        ("beta.internal.example.com", "beta"),
        ("provisa.dev", None),  # apex — the leftmost label is the domain, not an org
        ("localhost", None),
        ("127.0.0.1", None),  # dialed by address, which sends no SNI at all
        ("cloud.provisa.dev", None),  # the control plane names no org by its hostname
        ("", None),
        (None, None),
    ],
)
def test_the_org_a_hostname_addresses(host, expected):
    assert org_from_host(host) == expected


def test_the_control_plane_host_is_recognized_with_its_port_and_case():
    assert is_control_plane_host("Cloud.Provisa.Dev:443")
    assert not is_control_plane_host("cloudy.provisa.dev")
    assert not is_control_plane_host("acme.provisa.dev")


def test_the_http_middleware_reads_hostnames_the_same_way():
    """One rule, one place — the middleware must not grow a second reading of the same string."""
    from provisa.auth.middleware import _requested_org_from_host

    def requested(host: str, **headers: str) -> str | None:
        request = cast(Any, SimpleNamespace(headers={"host": host, **headers}))
        return _requested_org_from_host(request)

    assert requested("acme.provisa.dev") == "acme"
    assert requested("provisa.dev") is None
    assert requested("localhost:8000") is None
    # The control-plane host is the one place an explicit header still names the org.
    assert requested("cloud.provisa.dev", **{"x-org-provisa": "beta"}) == "beta"
    assert requested("cloud.provisa.dev") is None


# ── The capture, over a real handshake ────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def tls_pair(tmp_path_factory) -> tuple[str, str]:
    """A self-signed certificate. The name in it is irrelevant — the client does not verify it."""
    directory: Path = tmp_path_factory.mktemp("sni-tls")
    cert, key = directory / "server.pem", directory / "server.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=provisa.test",
        ],
        check=True,
        capture_output=True,
    )
    return str(cert), str(key)


def _server_context(tls_pair: tuple[str, str], *, with_capture: bool) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(*tls_pair)
    if with_capture:
        install(context)
    return context


def _client_context() -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def _handshake(context: ssl.SSLContext, server_hostname: str | None) -> str | None:
    """Dial the listener under ``server_hostname`` and return what the server captured."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    captured: dict[str, str | None] = {}

    def accept() -> None:
        raw, _ = listener.accept()
        wrapped = context.wrap_socket(raw, server_side=True)
        captured["host"] = indicated_host(wrapped)
        wrapped.close()

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    client = _client_context().wrap_socket(
        socket.create_connection(listener.getsockname()), server_hostname=server_hostname
    )
    client.do_handshake()
    client.close()
    thread.join(timeout=10)
    listener.close()
    return captured["host"]


def test_the_indicated_hostname_survives_the_handshake(tls_pair):
    assert _handshake(_server_context(tls_pair, with_capture=True), "acme.provisa.dev") == (
        "acme.provisa.dev"
    )


def test_a_client_that_indicates_nothing_leaves_nothing_behind(tls_pair):
    """An IP-address dial sends no servername extension, and must not be read as one."""
    assert _handshake(_server_context(tls_pair, with_capture=True), None) is None


def test_a_listener_with_no_capture_installed_reports_nothing(tls_pair):
    """The stash is opt-in: a context nobody installed the callback on holds no hostname."""
    assert _handshake(_server_context(tls_pair, with_capture=False), "acme.provisa.dev") is None


def test_one_connections_hostname_does_not_leak_into_the_next(tls_pair):
    """The name lives on the connection, so a reused address cannot inherit the previous org."""
    context = _server_context(tls_pair, with_capture=True)
    assert _handshake(context, "acme.provisa.dev") == "acme.provisa.dev"
    assert _handshake(context, "beta.provisa.dev") == "beta.provisa.dev"
    assert _handshake(context, None) is None


def test_a_plaintext_connection_indicates_nothing():
    assert indicated_host(socket.socket()) is None
    assert indicated_host(None) is None


# ── What the protocol handlers do with it ─────────────────────────────────────────────────────


def test_pgwire_reads_the_org_off_its_wrapped_socket():
    from provisa.pgwire.server import ProvisaHandler

    handler = cast(Any, object.__new__(ProvisaHandler))
    handler.request = socket.socket()
    assert handler._requested_org() is None  # plaintext — no org requested

    # handle_startup replaces self.request with the wrapped socket during the SSLRequest exchange,
    # and that is what carries the stash. A raw socket.socket cannot stand in for it — it defines
    # __slots__ and rejects the attribute the callback sets.
    handler.request = SimpleNamespace(_provisa_sni_host="acme.provisa.dev")
    assert handler._requested_org() == "acme"

    handler.request = SimpleNamespace(_provisa_sni_host="provisa.dev")
    assert handler._requested_org() is None  # apex — a TLS connection that named no org


def test_bolt_reads_the_org_off_its_transport():
    from provisa.bolt.session import BoltSession

    session = cast(Any, object.__new__(BoltSession))
    ssl_object = SimpleNamespace(_provisa_sni_host="beta.provisa.dev")
    session.writer = SimpleNamespace(
        get_extra_info=lambda name: ssl_object if name == "ssl_object" else None
    )
    assert session._requested_org() == "beta"

    # A WebSocket or plaintext writer exposes no ssl_object, and one that does not answer
    # get_extra_info at all is the same story.
    session.writer = SimpleNamespace(get_extra_info=lambda name: None)  # noqa: ARG005
    assert session._requested_org() is None
    session.writer = SimpleNamespace()
    assert session._requested_org() is None
