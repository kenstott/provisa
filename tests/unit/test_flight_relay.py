# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6d9b12-7a85-4c0e-9d41-b8e2c5a7f063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every worker serves Arrow Flight on the ONE advertised port (REQ-1900).

pyarrow's Flight server cannot share a port between processes, so `--workers N` ran N servers on N
ports and the advertised one reached a single worker. Each worker now binds the advertised port
itself (SO_REUSEPORT) and relays the bytes to its own Flight server on a loopback port."""

# Requirements: REQ-1900

from __future__ import annotations

import datetime
import socket
import threading

import pyarrow as pa
import pyarrow.flight as fl
import pytest

from provisa.api.flight.relay import FlightRelay
from tests.port_lease import lease_port


class _Echo(fl.FlightServerBase):
    """Answers do_get with one row naming this server."""

    def __init__(self, name: str, location: str = "grpc://127.0.0.1:0", **kwargs) -> None:
        super().__init__(location, **kwargs)
        self._name = name

    def do_get(self, context, ticket):  # noqa: ARG002 - Flight override signature
        table = pa.table({"server": [self._name], "ticket": [ticket.ticket.decode()]})
        return fl.RecordBatchStream(table)


@pytest.fixture
def started():
    """Start servers/relays and stop every one of them afterwards."""
    opened: list = []

    def _start(thing):
        opened.append(thing)
        if isinstance(thing, fl.FlightServerBase):
            threading.Thread(target=thing.serve, daemon=True).start()
        return thing

    yield _start
    for thing in reversed(opened):
        if isinstance(thing, FlightRelay):
            thing.close()
        else:
            thing.shutdown()


def _get(port: int, ticket: str = "t", **client_kwargs) -> dict:
    scheme = "grpc+tls" if client_kwargs else "grpc"
    client = fl.connect(f"{scheme}://127.0.0.1:{port}", **client_kwargs)
    try:
        reader = client.do_get(fl.Ticket(ticket.encode()), fl.FlightCallOptions(timeout=20))
        return reader.read_all().to_pylist()[0]
    finally:
        client.close()


def test_a_call_on_the_advertised_port_reaches_the_workers_own_server(started):
    server = started(_Echo("w1"))
    port = lease_port()
    started(FlightRelay("127.0.0.1", port, server.port))
    assert _get(port, "hello") == {"server": "w1", "ticket": "hello"}


def test_a_large_stream_arrives_whole_through_the_relay(started):
    class _Big(fl.FlightServerBase):
        def do_get(self, context, ticket):  # noqa: ARG002 - Flight override signature
            return fl.RecordBatchStream(pa.table({"n": pa.array(range(3_000_000), pa.int64())}))

    server = started(_Big("grpc://127.0.0.1:0"))
    port = lease_port()
    started(FlightRelay("127.0.0.1", port, server.port))
    client = fl.connect(f"grpc://127.0.0.1:{port}")
    try:
        table = client.do_get(fl.Ticket(b"x"), fl.FlightCallOptions(timeout=60)).read_all()
    finally:
        client.close()
    assert table.num_rows == 3_000_000
    assert table["n"][2_999_999].as_py() == 2_999_999


def test_two_workers_bind_the_same_advertised_port(started):
    port = lease_port()
    first, second = started(_Echo("w1")), started(_Echo("w2"))
    started(FlightRelay("127.0.0.1", port, first.port))
    started(FlightRelay("127.0.0.1", port, second.port))  # no "address already in use"
    served = {_get(port)["server"] for _ in range(20)}
    # Which listener the kernel hands a connection to is the kernel's choice (Linux spreads
    # them; darwin gives them all to one), but every call is answered by one of the two.
    assert served and served <= {"w1", "w2"}


def _self_signed() -> tuple[bytes, bytes]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    import ipaddress

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    return (
        cert.public_bytes(serialization.Encoding.PEM),
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )


def test_tls_is_terminated_by_the_flight_server_not_the_relay(started):
    cert, key = _self_signed()
    server = started(_Echo("tls", "grpc+tls://127.0.0.1:0", tls_certificates=[(cert, key)]))
    port = lease_port()
    started(FlightRelay("127.0.0.1", port, server.port))
    # The client verifies the SERVER's certificate through the relay: the relay holds no key.
    assert _get(port, "secure", tls_root_certs=cert) == {"server": "tls", "ticket": "secure"}


def test_closing_the_relay_stops_listening(started):
    server = started(_Echo("w1"))
    port = lease_port()
    relay = FlightRelay("127.0.0.1", port, server.port)
    assert _get(port)["server"] == "w1"
    relay.close()
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=2).close()


def test_a_connection_whose_server_is_gone_is_closed_not_left_open():
    port, dead = lease_port(), lease_port()
    relay = FlightRelay("127.0.0.1", port, dead)  # nothing listens on `dead`
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=5) as client:
            client.settimeout(5)
            assert client.recv(1) == b""  # closed by the relay
    finally:
        relay.close()


def test_closing_the_relay_ends_its_accept_thread_before_its_listener_is_closed(started):
    """A descriptor closed under a thread still blocked in accept() is reused by the next socket
    the process opens, and that accept then takes the new socket's connections: another server's
    clients were refused or reset. close() returns only once the accept thread has ended."""
    server = started(_Echo("w1"))
    port = lease_port()
    relay = FlightRelay("127.0.0.1", port, server.port)
    assert _get(port)["server"] == "w1"
    relay.close()
    assert not relay._acceptor.is_alive()  # noqa: SLF001 - the property under test
    assert relay._listener.fileno() == -1  # noqa: SLF001 - closed only after the thread ended
