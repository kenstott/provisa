# Copyright (c) 2026 Kenneth Stott
# Canary: b7afdebf-7265-4ccc-8e0b-4df8bb8b06a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source that runs on a remote host (REQ-2098): Provisa's side of the connection.

The host is a container serving the source's code on one port over mutual TLS
(provisa/python/remote_host.py). Provisa dials it -- each Provisa process its own connection, held
in its worker pool as a local worker is -- and checks its hello as it checks a local host's.

Trust both ways is set up once, by pairing: the operator gives the source the code the container
printed on its first start, ``<fingerprint>.<secret>``. Provisa connects, checks that the host's
certificate is the one the code names before it sends anything, and sends the secret with the
certificate Provisa presents from then on (:class:`Identity`). The fingerprint is kept as the
host's pin: every later connection is to that certificate and no other.
"""

# Requirements: REQ-2098
from __future__ import annotations

import datetime
import hashlib
import secrets
import socket
import ssl
import tempfile
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from provisa.codesource.worker import (
    START_TIMEOUT_S,
    Worker,
    WorkerFailed,
    _frame,
    _read_frame,
    refusal,
)

#: The port a remote host's container listens on, fixed in its image (REQ-2098).
HOST_PORT = 7443


@dataclass(frozen=True)
class Identity:
    """The certificate and key Provisa presents to a remote host, PEM-encoded."""

    certificate: str
    key: str


def new_identity() -> Identity:
    """A key and a self-signed certificate for Provisa to present to remote hosts."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "provisa")])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=3650))
        # A trust anchor of its own: the host keeps it as the one certificate it accepts.
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return Identity(
        certificate=certificate.public_bytes(serialization.Encoding.PEM).decode(),
        key=key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode(),
    )


class PairingRefused(ValueError):
    """The pairing code is not the host's, or the host refused it: named, with why."""


def parse_pairing_code(code: str) -> tuple[str, str]:
    """A pairing code's host fingerprint and secret."""
    fingerprint, _, secret = code.strip().partition(".")
    if (
        len(fingerprint) != 64
        or not secret
        or any(c not in "0123456789abcdef" for c in fingerprint)
    ):
        raise PairingRefused(
            "a pairing code is the line the remote host printed on its first start, "
            "<fingerprint>.<secret>"
        )
    return fingerprint, secret


def _context(identity: Identity | None) -> ssl.SSLContext:
    """A client context that checks the host by its pinned fingerprint, not by a CA: a remote host
    has a self-signed certificate of its own."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE  # the pin is checked on the certificate itself (_dial)
    if identity is not None:
        # load_cert_chain reads files only: the identity is written to a private directory for
        # the call and removed at once.
        with tempfile.TemporaryDirectory(prefix="provisa-remote-") as private:
            cert, key = Path(private) / "c.pem", Path(private) / "k.pem"
            cert.write_text(identity.certificate)
            key.write_text(identity.key)
            key.chmod(0o600)
            context.load_cert_chain(cert, key)
    return context


def _dial(
    address: str, port: int, pin: str, identity: Identity | None, timeout: float
) -> ssl.SSLSocket:
    """A TLS connection to the host whose certificate's fingerprint is ``pin``, refused by name
    when it cannot be reached or is another host."""
    where = f"{address}:{port}"
    try:
        raw = socket.create_connection((address, port), timeout=timeout)
    except OSError as exc:
        raise ConnectionError(f"remote host {where} could not be reached: {exc}") from exc
    try:
        conn = _context(identity).wrap_socket(raw)
    except (OSError, ssl.SSLError) as exc:
        raw.close()
        raise ConnectionError(f"remote host {where} refused the TLS connection: {exc}") from exc
    der = conn.getpeercert(binary_form=True)
    found = hashlib.sha256(der or b"").hexdigest()
    if not secrets.compare_digest(found, pin):
        conn.close()
        raise ConnectionError(
            f"remote host {where} presented certificate {found}, not the paired {pin}"
        )
    return conn


def pair(address: str, port: int, code: str, identity: Identity, timeout: float = 10.0) -> str:
    """Pair with the remote host at ``address:port`` using its pairing ``code``: it keeps
    ``identity``'s certificate as Provisa's. The host's fingerprint, to pin."""
    fingerprint, secret = parse_pairing_code(code)
    try:
        conn = _dial(address, port, fingerprint, None, timeout)
    except ConnectionError as exc:
        raise PairingRefused(str(exc)) from exc
    with conn:
        conn.sendall(_frame({"op": "pair", "secret": secret, "certificate": identity.certificate}))
        answer = _read_frame(conn)
    if answer is None or answer.get("op") != "paired":
        said = (answer or {}).get("message", "it closed the connection")
        raise PairingRefused(f"remote host {address}:{port} refused the pairing: {said}")
    return fingerprint


class RemoteWorker(Worker):
    """A connection to a remote host, in place of a local host process: the same calls, cancels
    and failures, over TLS. Its exports are checked against the ones the source pinned."""

    def __init__(
        self,
        source_id: str,
        content_hash: str,
        *,
        address: str,
        port: int,
        pin: str,
        identity: Identity,
        exports: Sequence[str] | None = None,
    ) -> None:
        # No process to start: the connection is dialled (start).
        super().__init__(source_id, content_hash, command=None)  # pyright: ignore[reportArgumentType]
        self.address = address
        self.port = port
        self._pin = pin
        self._identity = identity
        self._pinned_exports = None if exports is None else sorted(exports)

    @property
    def where(self) -> str:
        return f"{self.address}:{self.port}"

    def start(self, timeout: float = START_TIMEOUT_S) -> None:
        try:
            conn = _dial(self.address, self.port, self._pin, self._identity, timeout)
        except ConnectionError as exc:
            raise WorkerFailed(self.source_id, str(exc)) from None
        conn.settimeout(timeout)
        try:
            hello = _read_frame(conn)
        except (TimeoutError, OSError):
            hello = None
        if hello is None or hello.get("op") != "hello":
            conn.close()
            raise WorkerFailed(self.source_id, f"remote host {self.where} did not say it was ready")
        refused = refusal(hello, self.content_hash) or self._other_exports(hello)
        if refused is not None:
            try:
                conn.sendall(_frame({"op": "refused", "message": refused}))
            except OSError:
                pass  # the host has gone already: the refusal is still raised by name below
            conn.close()
            raise WorkerFailed(self.source_id, f"remote host {self.where}: {refused}")
        conn.settimeout(None)
        self._conn = conn
        self.exports = list(hello.get("exports") or [])
        threading.Thread(
            target=self._read, name=f"code-remote-{self.source_id}", daemon=True
        ).start()

    def _other_exports(self, hello: dict) -> str | None:
        if self._pinned_exports is None:
            return None
        offered = sorted(hello.get("exports") or [])
        if offered == self._pinned_exports:
            return None
        return (
            f"its host offers the exports {offered}, not the {self._pinned_exports} the "
            "source pinned"
        )

    def alive(self) -> bool:
        return self._ended is None and self._conn is not None

    def stop(self) -> None:
        self._end("the worker was stopped")
        if self._conn is not None:
            try:
                self._conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # already closed by the host: nothing left to shut
            self._conn.close()

    def _said(self) -> str:
        return f" (remote host {self.where})"
