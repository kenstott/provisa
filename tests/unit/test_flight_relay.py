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


def test_a_relay_closes_while_another_listener_shares_its_port(started):
    """Every worker binds the advertised port (SO_REUSEPORT), and on Linux the kernel gives a new
    connection to any one of the port's listeners. close() used to wake its accept thread with a
    connection to the port; when that connection went to the other listener the thread stayed in
    accept() and close() never returned -- a worker's shutdown hung (the e2e lane: seven module
    teardowns, 15 minutes each). Closing does not depend on which listener a connection reaches."""
    server = started(_Echo("w1"))
    port = lease_port()
    other = started(FlightRelay("127.0.0.1", port, server.port))  # stays open throughout
    for _ in range(8):  # one in two of these hung on Linux when the wake-up went to `other`
        relay = FlightRelay("127.0.0.1", port, server.port)
        closing = threading.Thread(target=relay.close, daemon=True)
        closing.start()
        closing.join(timeout=10)
        assert not closing.is_alive(), "close() did not return while another listener held the port"
        assert not relay._acceptor.is_alive()  # noqa: SLF001 - the property under test
    assert _get(port)["server"] == "w1"  # the listener that stayed still serves
    del other


# --- a host with no buffer space at this moment (ENOBUFS) ----------------------------------------


class _ShortOfBufferSpace:
    """A socket whose host refuses some sends for want of buffer space, and takes only part of
    what it is given when it does accept."""

    def __init__(self, refusals: list[int], takes: int) -> None:
        self.refusals = refusals  # the send calls (by number) refused with ENOBUFS
        self.takes = takes
        self.calls = 0
        self.received = bytearray()

    def send(self, data) -> int:
        import errno

        self.calls += 1
        if self.calls in self.refusals:
            raise OSError(errno.ENOBUFS, "No buffer space available")
        taken = bytes(data[: self.takes])
        self.received += taken
        return len(taken)


def test_a_host_short_of_buffer_space_is_waited_out_and_every_byte_arrives_once(monkeypatch):
    """Twelve large streams relayed at once on macOS exhausted the host's network buffers:
    ``send`` failed with ENOBUFS, the relay took that for the connection ending, dropped a
    healthy connection, and the client saw "Socket closed". It is waited out, and what was not
    yet sent is sent -- no byte twice, none skipped."""
    from provisa.api.flight import relay

    monkeypatch.setattr(relay, "_NO_BUFFER_SPACE_WAIT", 0.0)
    payload = bytes(range(256)) * 40  # 10,240 bytes, every position distinguishable
    sink = _ShortOfBufferSpace(refusals=[1, 4, 5, 9], takes=1000)
    relay._send_all(sink, memoryview(payload))  # noqa: SLF001
    assert bytes(sink.received) == payload
    assert sink.calls == 11 + 4  # eleven partial sends, four refusals waited out


def test_any_other_socket_error_still_ends_the_copy():
    import errno

    from provisa.api.flight import relay

    class _Reset:
        def send(self, data) -> int:
            raise ConnectionResetError(errno.ECONNRESET, "Connection reset by peer")

    with pytest.raises(ConnectionResetError):
        relay._send_all(_Reset(), memoryview(b"abc"))  # noqa: SLF001


_H2_FRAMES = {0: "DATA", 1: "HEADERS", 2: "PRIORITY", 3: "RST_STREAM", 4: "SETTINGS", 5: "PUSH_PROMISE",
              6: "PING", 7: "GOAWAY", 8: "WINDOW_UPDATE", 9: "CONTINUATION"}  # fmt: skip


def _as_http2(raw: bytes) -> str:
    """What a server-to-client byte stream holds, read as HTTP/2 frames from its first byte:
    the frames by kind, the DATA bytes per stream, and the first place it stops being frames."""
    offset, kinds, data, notes = 0, {}, {}, []
    while offset + 9 <= len(raw):
        length, kind = int.from_bytes(raw[offset : offset + 3], "big"), raw[offset + 3]
        stream = int.from_bytes(raw[offset + 5 : offset + 9], "big") & 0x7FFFFFFF
        if kind not in _H2_FRAMES:
            notes.append(
                f"not a frame at byte {offset}: length={length} type={kind} stream={stream} "
                f"bytes={raw[offset : offset + 16].hex()}"
            )
            break
        name = _H2_FRAMES[kind]
        kinds[name] = kinds.get(name, 0) + 1
        if name == "DATA":
            data[stream] = data.get(stream, 0) + length
        elif name in ("RST_STREAM", "GOAWAY"):
            at = offset + 9 + (4 if name == "GOAWAY" else 0)
            notes.append(f"{name} error={int.from_bytes(raw[at : at + 4], 'big')} at byte {offset}")
        offset += 9 + length
    return f"{len(raw)} bytes; frames {kinds}; DATA bytes by stream {data}; {'; '.join(notes)}"


def test_many_large_streams_at_once_each_arrive_whole_through_the_relay(started, monkeypatch):
    """Twelve 24 MB streams relayed at once, twice over. Each must arrive whole -- every row, in
    order -- and the relay must have carried the same number of bytes towards each client.

    On macOS, while the whole machine was under load, relayed streams have failed at the client
    with "frame of size N overflows local window of M". It has not been reproduced on a quiet
    machine in any arrangement, and what the relay does to bytes has been verified separately,
    so where the stream goes wrong is not yet known. This test runs on Linux in CI, where the
    product runs; and when a stream fails it says what the relay carried to each client, read as
    HTTP/2 frames, so the failure names the place."""
    import zlib

    import pyarrow.compute as pc

    from provisa.api.flight import relay

    rows = 3_000_000
    # One record per copy: what the relay carried in that direction of that connection.
    copies: list[dict] = []
    copies_lock = threading.Lock()
    this_copy = threading.local()
    real_copy, real_send_all = relay._copy, relay._send_all  # noqa: SLF001

    def _copy(source, sink) -> None:
        this_copy.record = {"bytes": 0, "packer": zlib.compressobj(1), "packed": []}
        with copies_lock:
            copies.append(this_copy.record)
        real_copy(source, sink)

    def _send_all(sink, data) -> None:
        real_send_all(sink, data)
        record = this_copy.record
        record["bytes"] += len(data)
        record["packed"].append(record["packer"].compress(data))  # sequential integers: small

    monkeypatch.setattr(relay, "_copy", _copy)
    monkeypatch.setattr(relay, "_send_all", _send_all)

    class _Big(fl.FlightServerBase):
        def do_get(self, context, ticket):  # noqa: ARG002 - Flight override signature
            return fl.RecordBatchStream(pa.table({"n": pa.array(range(rows), pa.int64())}))

    server = started(_Big("grpc://127.0.0.1:0"))
    port = lease_port()
    started(FlightRelay("127.0.0.1", port, server.port))
    failures: list[str] = []

    def _read() -> None:
        client = fl.connect(f"grpc://127.0.0.1:{port}")
        try:
            table = client.do_get(fl.Ticket(b"x"), fl.FlightCallOptions(timeout=120)).read_all()
            column = table["n"]
            if table.num_rows != rows or pc.sum(column).as_py() != rows * (rows - 1) // 2:
                failures.append(f"a stream arrived changed: {table.num_rows} rows")
            elif column[0].as_py() != 0 or column[rows - 1].as_py() != rows - 1:
                failures.append("a stream arrived out of order")
        except Exception as exc:  # noqa: BLE001 - every failure is reported, by its text
            failures.append(str(exc).splitlines()[0][:200])
        finally:
            client.close()

    for _ in range(2):
        readers = [threading.Thread(target=_read) for _ in range(12)]
        for reader in readers:
            reader.start()
        for reader in readers:
            reader.join()

    # Towards the client each connection carries one whole stream: the same bytes, give or take
    # HTTP/2's own framing. (The other direction of each connection is a few kilobytes.)
    towards_clients = sorted(
        (c for c in copies if c["bytes"] > 1_000_000), key=lambda c: c["bytes"]
    )
    carried = [c["bytes"] for c in towards_clients]
    whole = carried[len(carried) // 2] if carried else 0
    odd = [
        _as_http2(zlib.decompress(b"".join(c["packed"]) + c["packer"].flush()))
        for c in towards_clients
        if abs(c["bytes"] - whole) > 8192
    ]
    assert failures == [], {"failures": failures, "carried": carried, "streams that differ": odd}
    assert len(carried) == 24 and odd == [], {"carried": carried, "streams that differ": odd}
