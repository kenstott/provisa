# Copyright (c) 2026 Kenneth Stott
# Canary: dfb06e7d-e994-4b19-b5a1-675033ac5a80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The front end of every built image, Node or Python (REQ-2098, REQ-2100).

    <python> front.py <port> <state directory> <names file> <worker argv ...>

It serves the image's one port, 7443, over TLS, and runs the code's worker beside it: the
language's host program (Python's host.py, Node's host.mjs), started once with a connected socket
in place of the ``{fd}`` of its argv (``fd:N``), serving every code source's protocol
(provisa/codesource/worker.py). Each Provisa process keeps a connection of its own; their calls
share the one worker, each call's id mapped to one of the worker's own.

PAIRING. On its first start the front prints a one-time pairing code: its certificate's SHA-256
fingerprint and a secret. Until it is paired it takes one message on a connection, ``{op: "pair",
secret, certificate}``: with the code's secret, it keeps the certificate as the one Provisa
presents, answers ``{op: "paired"}`` and from then requires that certificate of every connection.
Provisa, given the code, checks the front's certificate against the fingerprint before it sends
anything. Another secret is refused, and the front stays unpaired.

HELLO. Each connection is told the worker's hello, with the code's name tables (REQ-2100,
provisa/codesource/names.py) from the names file baked into the image.

It serves no reads of its own: a read of a code table is a call of its function, its rows landed
by Provisa, which runs the statement over them (REQ-2100, codesource.proxy).

Its imports beside the standard library are cryptography's, in the image beside this program; the
code's own environment is the code's.
"""

# Requirements: REQ-2098, REQ-2100
from __future__ import annotations

import datetime
import hashlib
import itertools
import json
import os
import secrets
import socket
import socketserver
import ssl
import struct
import subprocess  # nosec B404 - argv-list invocation of the image's own worker
import sys
import threading
from pathlib import Path
from typing import Any

_HEAD = struct.Struct(">I")
_ID = struct.Struct(">Q")
_JSON, _ARROW = b"J", b"A"
KEY = "host.key"
CERTIFICATE = "host.crt"
#: The certificate of the Provisa the front is paired to.
TRUSTED = "provisa.crt"
#: The pairing secret, kept until the front is paired.
SECRET = "pairing.secret"


# -- frames -------------------------------------------------------------------------------------


def frame(message: dict) -> bytes:
    body = _JSON + json.dumps(message, separators=(",", ":")).encode()
    return _HEAD.pack(len(body)) + body


def rows_frame(call_id: int, ipc: bytes) -> bytes:
    body = _ARROW + _ID.pack(call_id) + ipc
    return _HEAD.pack(len(body)) + body


def _read_exact(conn: Any, size: int) -> bytes | None:  # a socket or a TLS socket
    chunks, left = [], size
    while left:
        chunk = conn.recv(min(left, 1 << 20))
        if not chunk:
            return None
        chunks.append(chunk)
        left -= len(chunk)
    return b"".join(chunks)


def read_body(conn: Any) -> bytes | None:
    """The next frame's body (its kind byte first), or None when the connection has closed."""
    head = _read_exact(conn, _HEAD.size)
    if head is None:
        return None
    return _read_exact(conn, _HEAD.unpack(head)[0])


# -- identity and pairing -----------------------------------------------------------------------


def fingerprint(der: bytes) -> str:
    """A certificate's SHA-256 fingerprint, as a pairing code carries it."""
    return hashlib.sha256(der).hexdigest()


def pairing_code(certificate_der: bytes, secret: str) -> str:
    """The code Provisa is given to pair with the front: its fingerprint and the secret."""
    return f"{fingerprint(certificate_der)}.{secret}"


def make_identity(state: Path) -> None:
    """The front's key and self-signed certificate, made once."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "provisa remote host")])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .sign(key, hashes.SHA256())
    )
    (state / KEY).write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    (state / KEY).chmod(0o600)
    (state / CERTIFICATE).write_bytes(certificate.public_bytes(serialization.Encoding.PEM))


class Identity:
    """The front's identity, its TLS context and its pairing."""

    def __init__(self, state: Path) -> None:
        self.state = state
        if not (state / KEY).is_file():
            make_identity(state)
        if not (state / TRUSTED).is_file() and not (state / SECRET).is_file():
            (state / SECRET).write_text(secrets.token_urlsafe(18))
            (state / SECRET).chmod(0o600)
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.minimum_version = ssl.TLSVersion.TLSv1_3
        self.context.load_cert_chain(state / CERTIFICATE, state / KEY)
        self._lock = threading.Lock()
        if self.paired:
            self.context.verify_mode = ssl.CERT_OPTIONAL
            self.context.load_verify_locations(state / TRUSTED)
        else:
            # Before pairing there is no certificate to check against, and an unpaired front
            # takes nothing but a pair message.
            self.context.verify_mode = ssl.CERT_NONE

    @property
    def paired(self) -> bool:
        return (self.state / TRUSTED).is_file()

    def code(self) -> str | None:
        """The pairing code, while the front is not paired."""
        if self.paired:
            return None
        der = ssl.PEM_cert_to_DER_cert((self.state / CERTIFICATE).read_text())
        return pairing_code(der, (self.state / SECRET).read_text())

    def pair(self, conn: ssl.SSLSocket) -> None:
        """Take one pair message on ``conn``: with the pairing secret, keep its certificate."""
        body = read_body(conn)
        message = json.loads(body[1:]) if body and body[:1] == _JSON else {}
        with self._lock:
            if self.paired:
                conn.sendall(frame({"op": "refused", "message": "this host is paired already"}))
                return
            held = (self.state / SECRET).read_text()
            if message.get("op") != "pair" or not secrets.compare_digest(
                str(message.get("secret", "")), held
            ):
                conn.sendall(frame({"op": "refused", "message": "not this host's pairing code"}))
                return
            certificate = str(message["certificate"])
            ssl.PEM_cert_to_DER_cert(certificate)  # a certificate, or refused here
            (self.state / TRUSTED).write_text(certificate)
            (self.state / SECRET).unlink()
            self.context.load_verify_locations(self.state / TRUSTED)
            self.context.verify_mode = ssl.CERT_OPTIONAL
        conn.sendall(frame({"op": "paired"}))
        sys.stderr.write("paired with Provisa\n")


# -- the worker, shared ---------------------------------------------------------------------------


class Connection:
    """One Provisa connection's side of the worker: what it is sent, under its own call ids."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn
        self._lock = threading.Lock()
        self.open = True

    def send(self, data: bytes) -> None:
        with self._lock:
            if not self.open:
                return
            try:
                self.conn.sendall(data)
            except OSError:
                self.open = False


class _Call:
    def __init__(self, connection: Connection, call_id: int) -> None:
        self.connection = connection
        self.call_id = call_id


class Worker:
    """The code's worker, started once, its calls shared by every connection."""

    def __init__(self, argv: list[str]) -> None:
        mine, theirs = socket.socketpair()
        self._sock = mine
        argv = [a.replace("{fd}", f"fd:{theirs.fileno()}") for a in argv]
        self.process = subprocess.Popen(argv, pass_fds=(theirs.fileno(),))  # noqa: S603  # nosec B603
        theirs.close()
        body = read_body(mine)
        if body is None or body[:1] != _JSON:
            raise RuntimeError("the code's worker ended before it said hello")
        self.hello = json.loads(body[1:])
        self._ids = itertools.count(1)
        self._calls: dict[int, _Call] = {}
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        threading.Thread(target=self._read, name="worker-reader", daemon=True).start()

    def _send(self, message: dict) -> None:
        with self._send_lock:
            self._sock.sendall(frame(message))

    def call(self, connection: Connection, message: dict) -> None:
        """A connection's call, sent to the worker under an id of the worker's own."""
        own = next(self._ids)
        with self._lock:
            self._calls[own] = _Call(connection, message["id"])
        self._send({**message, "id": own})

    def forward(self, connection: Connection, message: dict) -> None:
        """A connection's cancel or ping, for the call it names."""
        if message["op"] == "ping":
            connection.send(frame({"op": "pong", "id": message["id"]}))
            return
        with self._lock:
            own = next(
                (
                    k
                    for k, c in self._calls.items()
                    if c.connection is connection and c.call_id == message["id"]
                ),
                None,
            )
        if own is not None:
            self._send({**message, "id": own})

    def closed(self, connection: Connection) -> None:
        """A connection has ended: its calls still running are cancelled."""
        with self._lock:
            ended = [k for k, c in self._calls.items() if c.connection is connection]
            for k in ended:
                del self._calls[k]
        for k in ended:
            self._send({"op": "cancel", "id": k})

    def _read(self) -> None:
        while True:
            body = read_body(self._sock)
            if body is None:
                sys.stderr.write("the code's worker ended\n")
                os._exit(3)  # the container's restart policy starts it again
            if body[:1] == _ARROW:
                own = _ID.unpack(body[1 : 1 + _ID.size])[0]
                with self._lock:
                    call = self._calls.get(own)
                if call is None:
                    continue  # its connection ended: the call was cancelled
                call.connection.send(rows_frame(call.call_id, body[1 + _ID.size :]))
                continue
            message = json.loads(body[1:])
            own = message.get("id")
            with self._lock:
                call = self._calls.get(own)
                if call is not None and message["op"] in ("end", "error", "value"):
                    if message["op"] != "value":
                        del self._calls[own]
            if call is None:
                continue
            call.connection.send(frame({**message, "id": call.call_id}))


# -- the listener ---------------------------------------------------------------------------------


class Front(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, port: int, identity: Identity, worker: Worker, names: dict) -> None:
        self.identity = identity
        self.worker = worker
        self.names = names
        super().__init__(("", port), _Handler)


class _Handler(socketserver.BaseRequestHandler):
    @property
    def front(self) -> Front:
        assert isinstance(self.server, Front)
        return self.server

    def handle(self) -> None:
        raw: socket.socket = self.request
        try:
            conn = self.front.identity.context.wrap_socket(raw, server_side=True)
            if not self.front.identity.paired:
                self.front.identity.pair(conn)
                return
            if not conn.getpeercert():
                return  # no certificate, or not the paired Provisa's: the TLS check refused it
            self._serve(conn)
        except (ConnectionError, ssl.SSLError, OSError):
            pass  # the connection ended: Provisa redials

    def _serve(self, conn: ssl.SSLSocket) -> None:
        worker = self.front.worker
        connection = Connection(conn)
        connection.send(frame({**worker.hello, "names": self.front.names}))
        try:
            while (body := read_body(conn)) is not None:
                message = json.loads(body[1:])
                op = message.get("op")
                if op == "call":
                    worker.call(connection, message)
                elif op in ("cancel", "ping"):
                    worker.forward(connection, message)
                elif op == "refused":
                    sys.stderr.write(f"provisa refused this host: {message.get('message')}\n")
                    return
        finally:
            connection.open = False
            worker.closed(connection)


def main(argv: list[str]) -> int:
    port, state, names_file, *worker_argv = argv
    os.dup2(2, 1)  # what is printed goes to the container's log, never to a connection
    Path(state).mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    # The image's own libraries for the front end, apart from the code's (image_build.py); run
    # beside Provisa's source, the interpreter's.
    lib = os.environ.get("PROVISA_FRONT_LIB")
    if lib:
        sys.path.insert(1, lib)
    identity = Identity(Path(state))
    names = json.loads(Path(names_file).read_text())
    worker = Worker(worker_argv)
    pairing = identity.code()
    if pairing is not None:
        sys.stderr.write(f"pairing code: {pairing}\n")
    server = Front(int(port), identity, worker, names)
    sys.stderr.write(f"remote host listening on {server.server_address[1]}\n")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
