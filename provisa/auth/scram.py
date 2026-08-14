# Copyright (c) 2026 Kenneth Stott
# Canary: 9b9e7177-1f92-4393-ab7e-755253d4c743
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SCRAM-SHA-256 — RFC 5802, the mechanism every PostgreSQL client speaks (REQ-1394).

pgwire's other credential paths send the password to the server in the clear and trust TLS to hide
it. SCRAM does not: the client proves it knows the password without transmitting it, the server
proves it knows the verifier without being able to replay the client's proof elsewhere, and a
stolen copy of the stored verifier cannot be used to log in.

The verifier is what a deployment stores instead of a password. It is PostgreSQL's own format,

    SCRAM-SHA-256$<iterations>:<b64 salt>$<b64 StoredKey>:<b64 ServerKey>

so an operator can read it with the same eyes they read pg_authid with. Deriving it needs the
plaintext password, which exists only at the moment a user sets one — a bcrypt hash cannot be
converted, so a deployment turning SCRAM on asks its local users to set their passwords again.

This module is the mechanism alone: the exchange and the arithmetic, with no notion of where a
verifier is stored or which wire carries the messages. :class:`ScramExchange` is a two-step state
machine — ``server_first`` answers the client's first message, ``server_final`` verifies its proof
— and refuses to run out of order.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from typing import NamedTuple

# Requirements: REQ-1394

MECHANISM = "SCRAM-SHA-256"

# RFC 7677 names 4096 as the minimum for SCRAM-SHA-256; PostgreSQL uses exactly that, and matching
# it keeps a verifier this code writes interchangeable with one pg_authid holds.
DEFAULT_ITERATIONS = 4096

_DIGEST = "sha256"
_SALT_BYTES = 16
_NONCE_BYTES = 18


class ScramVerifier(NamedTuple):  # REQ-1394
    """What the server stores in place of a password."""

    salt: bytes
    iterations: int
    stored_key: bytes
    server_key: bytes

    def serialize(self) -> str:
        """PostgreSQL's pg_authid spelling of this verifier."""
        return (
            f"{MECHANISM}${self.iterations}:{_b64(self.salt)}"
            f"${_b64(self.stored_key)}:{_b64(self.server_key)}"
        )


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.b64decode(text)


def _hmac(key: bytes, message: bytes) -> bytes:
    return hmac.new(key, message, hashlib.sha256).digest()


def _xor(left: bytes, right: bytes) -> bytes:
    return bytes(a ^ b for a, b in zip(left, right))


def make_verifier(
    password: str, *, salt: bytes | None = None, iterations: int = DEFAULT_ITERATIONS
) -> ScramVerifier:  # REQ-1394
    """Derive a verifier from a plaintext password. Callable only where the plaintext exists."""
    salt = salt if salt is not None else os.urandom(_SALT_BYTES)
    salted = hashlib.pbkdf2_hmac(_DIGEST, password.encode("utf-8"), salt, iterations)
    client_key = _hmac(salted, b"Client Key")
    return ScramVerifier(
        salt=salt,
        iterations=iterations,
        stored_key=hashlib.sha256(client_key).digest(),
        server_key=_hmac(salted, b"Server Key"),
    )


def parse_verifier(serialized: str) -> ScramVerifier:  # REQ-1394
    """Read back a stored verifier. Raises ``ValueError`` on anything this code did not write."""
    prefix, _, body = serialized.partition("$")
    if prefix != MECHANISM:
        raise ValueError(f"not a {MECHANISM} verifier: {prefix!r}")
    params, _, keys = body.partition("$")
    iterations, _, salt = params.partition(":")
    stored_key, _, server_key = keys.partition(":")
    if not (iterations and salt and stored_key and server_key):
        raise ValueError("malformed SCRAM verifier")
    return ScramVerifier(
        salt=_unb64(salt),
        iterations=int(iterations),
        stored_key=_unb64(stored_key),
        server_key=_unb64(server_key),
    )


def mock_verifier(username: str, seed: bytes) -> ScramVerifier:  # REQ-1394
    """A verifier for a user who has none, so an unknown name is indistinguishable from a wrong one.

    PostgreSQL calls this mock authentication. Without it the server would have to answer the
    client's first message differently for a user it has never heard of, turning the handshake into
    a name oracle. The salt is derived from the name and a per-process seed, so it is stable within
    a connection's lifetime and unguessable across deployments; the password is random, so no proof
    can satisfy it.
    """
    salt = _hmac(seed, username.encode("utf-8"))[:_SALT_BYTES]
    return make_verifier(secrets.token_urlsafe(32), salt=salt, iterations=DEFAULT_ITERATIONS)


class ScramError(ValueError):  # REQ-1394
    """A SCRAM exchange that cannot continue. The surface answers; it never retries."""


def _fields(message: str) -> dict[str, str]:
    """The comma-separated ``k=v`` attributes of one SCRAM message, first occurrence winning."""
    parsed: dict[str, str] = {}
    for part in message.split(","):
        key, sep, value = part.partition("=")
        if sep and key not in parsed:
            parsed[key] = value
    return parsed


class ScramExchange:  # REQ-1394
    """The server half of one SCRAM-SHA-256 conversation.

    One exchange authenticates one connection. It holds the nonce and the verifier between the two
    round trips, which is why it cannot be a pair of free functions: the proof is computed over the
    exact bytes of both earlier messages, so those bytes must survive between them.
    """

    def __init__(self, verifier: ScramVerifier) -> None:
        self._verifier = verifier
        self._client_first_bare: str | None = None
        self._server_first: str | None = None
        self._gs2_header: str | None = None

    def server_first(self, client_first: str) -> str:
        """Answer the client's first message with the salt, iteration count and combined nonce."""
        if self._client_first_bare is not None:
            raise ScramError("SCRAM client-first received twice")

        gs2_flag, _, rest = client_first.partition(",")
        authzid, _, bare = rest.partition(",")
        if gs2_flag.startswith("p"):
            # Channel binding was requested, but only SCRAM-SHA-256 was advertised, never the -PLUS
            # variant. Proceeding would let the client believe its channel was bound when it is not.
            raise ScramError("channel binding is not supported; SCRAM-SHA-256-PLUS is not offered")
        if gs2_flag not in ("n", "y"):
            raise ScramError(f"unsupported SCRAM gs2 flag {gs2_flag!r}")

        fields = _fields(bare)
        client_nonce = fields.get("r")
        if not client_nonce:
            raise ScramError("SCRAM client-first carries no nonce")

        self._gs2_header = f"{gs2_flag},{authzid},"
        self._client_first_bare = bare
        nonce = client_nonce + secrets.token_urlsafe(_NONCE_BYTES)
        self._server_first = (
            f"r={nonce},s={_b64(self._verifier.salt)},i={self._verifier.iterations}"
        )
        return self._server_first

    def server_final(self, client_final: str) -> str:
        """Verify the client's proof and return the server's own signature.

        Raises :class:`ScramError` when the proof does not match — the caller reports that as a
        failed password, because under SCRAM that is precisely what it is.
        """
        if self._client_first_bare is None or self._server_first is None:
            raise ScramError("SCRAM client-final received before client-first")

        without_proof, _, proof_part = client_final.rpartition(",")
        fields = _fields(client_final)
        proof = fields.get("p")
        if not proof or not proof_part.startswith("p="):
            raise ScramError("SCRAM client-final carries no proof")

        channel_binding = fields.get("c")
        if channel_binding is None:
            raise ScramError("SCRAM client-final carries no channel-binding field")
        assert self._gs2_header is not None  # set together with _client_first_bare
        if _unb64(channel_binding) != self._gs2_header.encode("utf-8"):
            # The gs2 header is echoed under the proof so a man in the middle cannot strip the
            # client's channel-binding request on the way through. A mismatch is that attack.
            raise ScramError("SCRAM channel-binding data does not match the client's first message")

        expected_nonce = _fields(self._server_first)["r"]
        if fields.get("r") != expected_nonce:
            raise ScramError("SCRAM nonce does not match the one the server issued")

        auth_message = f"{self._client_first_bare},{self._server_first},{without_proof}"
        client_signature = _hmac(self._verifier.stored_key, auth_message.encode("utf-8"))
        client_proof = _unb64(proof)
        if len(client_proof) != len(client_signature):
            raise ScramError("SCRAM client proof is the wrong length")
        client_key = _xor(client_proof, client_signature)
        if not hmac.compare_digest(hashlib.sha256(client_key).digest(), self._verifier.stored_key):
            raise ScramError("SCRAM authentication failed")

        server_signature = _hmac(self._verifier.server_key, auth_message.encode("utf-8"))
        return f"v={_b64(server_signature)}"
