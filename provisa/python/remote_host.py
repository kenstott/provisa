# Copyright (c) 2026 Kenneth Stott
# Canary: 27759125-0f7e-401c-a8a7-d7e587d2bb47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python source's remote host (REQ-2098): the host program (``host.py``) served over mutual
TLS on one port, in the image Provisa builds for the source.

    <python> remote_host.py <port> <state directory> <code directory> <entry module>

Provisa dials it: every Provisa process keeps a connection of its own, and each is served as the
local worker's socket is (host.serve), with the code's hello. The host's identity is a key and a
self-signed certificate made on its first start and kept in the state directory, with the
certificate of the Provisa it is paired to.

PAIRING. On its first start the host prints a one-time pairing code: its certificate's SHA-256
fingerprint and a secret. Until it is paired it takes one message on a connection, ``{op: "pair",
secret, certificate}``: with the code's secret, it keeps the certificate as the one Provisa
presents, answers ``{op: "paired"}`` and from then requires that certificate of every connection.
Provisa, given the code, checks the host's certificate against the fingerprint before it sends
anything. Another secret is refused, and the host is not paired.

Its imports beside the standard library are ``cryptography``'s, installed in the image beside this
program; the code's own environment is the code's.
"""

# Requirements: REQ-2098
from __future__ import annotations

import asyncio
import datetime
import hashlib
import importlib
import json
import os
import secrets
import ssl
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
# The host program beside this one, by name: in the image they are two files in one directory.
host = importlib.import_module("host")

_HEAD = struct.Struct(">I")
KEY = "host.key"
CERTIFICATE = "host.crt"
#: The certificate of the Provisa the host is paired to.
TRUSTED = "provisa.crt"
#: The pairing secret, kept until the host is paired.
SECRET = "pairing.secret"


def fingerprint(der: bytes) -> str:
    """A certificate's SHA-256 fingerprint, as a pairing code carries it."""
    return hashlib.sha256(der).hexdigest()


def pairing_code(certificate_der: bytes, secret: str) -> str:
    """The code Provisa is given to pair with the host: its fingerprint and the secret."""
    return f"{fingerprint(certificate_der)}.{secret}"


def make_identity(state: Path) -> None:
    """The host's key and self-signed certificate, made once."""
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


def _der(pem_path: Path) -> bytes:
    return ssl.PEM_cert_to_DER_cert(pem_path.read_text())


class RemoteHost:
    """The listener, its identity and its pairing."""

    def __init__(self, state: Path, module: object) -> None:
        self.state = state
        self.module = module
        if not (state / KEY).is_file():
            make_identity(state)
        if not (state / TRUSTED).is_file() and not (state / SECRET).is_file():
            (state / SECRET).write_text(secrets.token_urlsafe(18))
            (state / SECRET).chmod(0o600)
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.minimum_version = ssl.TLSVersion.TLSv1_3
        self.context.load_cert_chain(state / CERTIFICATE, state / KEY)
        # A connection may present no certificate only to pair; once paired, every one is
        # checked against the paired Provisa's (serve).
        self.context.verify_mode = ssl.CERT_OPTIONAL
        if self.paired:
            self.context.load_verify_locations(state / TRUSTED)
        else:
            # Before pairing there is no certificate to check against, and an unpaired host
            # takes nothing but a pair message.
            self.context.verify_mode = ssl.CERT_NONE

    @property
    def paired(self) -> bool:
        return (self.state / TRUSTED).is_file()

    def code(self) -> str | None:
        """The pairing code, while the host is not paired."""
        if self.paired:
            return None
        return pairing_code(_der(self.state / CERTIFICATE), (self.state / SECRET).read_text())

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            if not self.paired:
                await self._pair(reader, writer)
                return
            if not writer.get_extra_info("peercert"):
                return  # no certificate, or not the paired Provisa's: the TLS check refused it
            await host.serve(self.module, reader, writer)
        except host.Refused as refused:
            sys.stderr.write(f"provisa refused this host: {refused}\n")
        except (ConnectionError, asyncio.IncompleteReadError, ssl.SSLError):
            pass  # the connection ended: Provisa redials
        finally:
            writer.close()

    async def _pair(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await reader.readexactly(_HEAD.size)
        message = json.loads(await reader.readexactly(_HEAD.unpack(head)[0]))
        held = (self.state / SECRET).read_text()
        if message.get("op") != "pair" or not secrets.compare_digest(
            str(message.get("secret", "")), held
        ):
            await _send(writer, {"op": "refused", "message": "not this host's pairing code"})
            return
        certificate = str(message["certificate"])
        ssl.PEM_cert_to_DER_cert(certificate)  # a certificate, or refused here
        (self.state / TRUSTED).write_text(certificate)
        (self.state / SECRET).unlink()
        self.context.load_verify_locations(self.state / TRUSTED)
        self.context.verify_mode = ssl.CERT_OPTIONAL
        await _send(writer, {"op": "paired"})
        sys.stderr.write("paired with Provisa\n")


async def _send(writer: asyncio.StreamWriter, message: dict) -> None:
    body = json.dumps(message, separators=(",", ":")).encode()
    writer.write(_HEAD.pack(len(body)) + body)
    await writer.drain()


async def run(port: int, state: Path, code: str, entry: str) -> None:
    remote = RemoteHost(state, host.load(code, entry))
    pairing = remote.code()
    if pairing is not None:
        sys.stderr.write(f"pairing code: {pairing}\n")
    server = await asyncio.start_server(remote.serve, port=port, ssl=remote.context)
    sys.stderr.write(f"remote host listening on {port}\n")
    async with server:
        await server.serve_forever()


def main(argv: list[str]) -> int:
    port, state, code, entry = argv
    os.dup2(2, 1)  # what the code prints goes to the container's log, never to a connection
    Path(state).mkdir(parents=True, exist_ok=True)
    asyncio.run(run(int(port), Path(state), code, entry))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
