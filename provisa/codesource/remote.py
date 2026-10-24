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

The host is a container serving the source's code on one port over mutual TLS, through the front
end every built image runs (provisa/codehost/front.py). Provisa dials it -- each Provisa process
its own connections, :data:`~provisa.codesource.worker.CONNECTIONS` of them, each serving one
call at a time, held in its worker pool as a local worker is -- and checks the hello on each as
it checks a local host's.

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
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from provisa.codesource.worker import (
    CONNECTIONS,
    START_TIMEOUT_S,
    Worker,
    WorkerFailed,
    _frame,
    _read_frame,
    duckdb_bounds,
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


def host_certificate(
    address: str, port: int, pin: str, identity: Identity, timeout: float = 10.0
) -> str:
    """REQ-2100: the paired host's certificate (PEM), checked against its ``pin``: what an engine
    attaching the host verifies it by (sslmode=verify-ca), as Provisa's own dials do by the pin."""
    with _dial(address, port, pin, identity, timeout) as conn:
        der = conn.getpeercert(binary_form=True)
    assert der is not None  # _dial checked it against the pin
    return ssl.DER_cert_to_PEM_cert(der)


def pair(
    address: str,
    port: int,
    code: str,
    identity: Identity,
    duckdb: dict[str, int],
    timeout: float = 10.0,
) -> str:
    """Pair with the remote host at ``address:port`` using its pairing ``code``: it keeps
    ``identity``'s certificate as Provisa's, and ``duckdb``, the bounds of its DuckDB
    (worker.duckdb_bounds), until a call gives others. The host's fingerprint, to pin."""
    fingerprint, secret = parse_pairing_code(code)
    try:
        conn = _dial(address, port, fingerprint, None, timeout)
    except ConnectionError as exc:
        raise PairingRefused(str(exc)) from exc
    with conn:
        conn.sendall(
            _frame(
                {
                    "op": "pair",
                    "secret": secret,
                    "certificate": identity.certificate,
                    "duckdb": duckdb,
                }
            )
        )
        answer = _read_frame(conn)
    if answer is None or answer.get("op") != "paired":
        said = (answer or {}).get("message", "it closed the connection")
        raise PairingRefused(f"remote host {address}:{port} refused the pairing: {said}")
    return fingerprint


class RemoteWorker(Worker):
    """A connection to a remote host, in place of a local host process: the same calls, cancels
    and failures, over TLS. Its exports are checked against the ones the source pinned."""

    where_kind = "remote"  # REQ-2106: on a call's stage

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
        conns: list[ssl.SSLSocket] = []
        hello: dict = {}
        try:
            for _ in range(CONNECTIONS):
                try:
                    conn = _dial(self.address, self.port, self._pin, self._identity, timeout)
                except ConnectionError as exc:
                    raise WorkerFailed(self.source_id, str(exc)) from None
                conns.append(conn)
                hello = self._hello_of(conn, timeout)
        except WorkerFailed:
            for conn in conns:
                conn.close()
            raise
        self._attach(list(conns), hello, f"code-remote-{self.source_id}")
        #: REQ-2100: the code's name tables, as the image carries them (provisa/codesource/names.py).
        self.names = dict(hello.get("names") or {})

    def _hello_of(self, conn: ssl.SSLSocket, timeout: float) -> dict:
        """The host's hello on ``conn``, checked: refused by name when it is not this source's."""
        conn.settimeout(timeout)
        try:
            hello = _read_frame(conn)
        except (TimeoutError, OSError):
            hello = None
        if hello is None or hello.get("op") != "hello":
            raise WorkerFailed(self.source_id, f"remote host {self.where} did not say it was ready")
        refused = refusal(hello, self.content_hash) or self._other_exports(hello)
        if refused is not None:
            try:
                conn.sendall(_frame({"op": "refused", "message": refused}))
            except OSError:
                pass  # the host has gone already: the refusal is still raised by name below
            raise WorkerFailed(self.source_id, f"remote host {self.where}: {refused}")
        conn.settimeout(None)
        return hello

    def _call_frame(self, *args: Any, **kwargs: Any) -> dict:
        """REQ-2100: every call to a host carries the operator's DuckDB bounds, which its front
        end applies (and keeps) before the call."""
        return {**super()._call_frame(*args, **kwargs), "duckdb": duckdb_bounds()}

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
        return self._ended is None and bool(self._lanes)

    def stop(self) -> None:
        self._end("the worker was stopped")
        for lane in self._lanes:
            lane.shut()

    def _said(self) -> str:
        return f" (remote host {self.where})"
