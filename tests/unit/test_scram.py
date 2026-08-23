# Copyright (c) 2026 Kenneth Stott
# Canary: 9d53c6a4-3a64-4e6d-9a45-8a7104b95ff8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1394 — SCRAM-SHA-256 as pgwire speaks it.

The arithmetic is checked against a client written here from RFC 5802 rather than against the
server's own helpers, because a test that computes the proof the way the server verifies it would
pass on a server that got the formula wrong in both places. The client below builds ClientProof
from the password alone, which is what a real driver has.

What is asserted: a correct password authenticates and a wrong one does not; the stored verifier
survives PostgreSQL's pg_authid spelling; every way a client can send a malformed or hostile
message is refused rather than accommodated; and a user with no verifier is answered the same way
as a user with one, so the handshake cannot be used to enumerate names.
"""

from __future__ import annotations

import base64
import hashlib
import hmac

import pytest

from provisa.auth.scram import (
    DEFAULT_ITERATIONS,
    MECHANISM,
    ScramError,
    ScramExchange,
    ScramVerifier,
    make_verifier,
    mock_verifier,
    parse_verifier,
)

# Requirements: REQ-1394


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


class _Client:
    """The client half of RFC 5802, written from the RFC rather than from the server's helpers."""

    def __init__(self, username: str, password: str, *, nonce: str = "clientnonce") -> None:
        self.password = password
        self.first_bare = f"n={username},r={nonce}"
        self.gs2_header = "n,,"

    @property
    def first(self) -> str:
        return self.gs2_header + self.first_bare

    def final(self, server_first: str, *, password: str | None = None) -> str:
        """The client-final message, optionally proving a password other than the real one."""
        fields = dict(part.split("=", 1) for part in server_first.split(","))
        salt = base64.b64decode(fields["s"])
        iterations = int(fields["i"])
        salted = hashlib.pbkdf2_hmac(
            "sha256", (password or self.password).encode("utf-8"), salt, iterations
        )
        client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
        stored_key = hashlib.sha256(client_key).digest()

        without_proof = f"c={_b64(self.gs2_header.encode())},r={fields['r']}"
        auth_message = f"{self.first_bare},{server_first},{without_proof}"
        signature = hmac.new(stored_key, auth_message.encode("utf-8"), hashlib.sha256).digest()
        proof = bytes(a ^ b for a, b in zip(client_key, signature))
        self.auth_message = auth_message
        self.salted = salted
        return f"{without_proof},p={_b64(proof)}"

    def check_server_signature(self, server_final: str) -> bool:
        """Whether the server proved it holds the verifier — mutual authentication's other half."""
        server_key = hmac.new(self.salted, b"Server Key", hashlib.sha256).digest()
        expected = hmac.new(server_key, self.auth_message.encode("utf-8"), hashlib.sha256).digest()
        return server_final == f"v={_b64(expected)}"


@pytest.fixture
def verifier() -> ScramVerifier:
    return make_verifier("correct horse battery staple")


# ── The verifier ──────────────────────────────────────────────────────────────────────────────


def test_the_verifier_is_derived_the_way_rfc_5802_says(verifier):
    salted = hashlib.pbkdf2_hmac(
        "sha256", b"correct horse battery staple", verifier.salt, verifier.iterations
    )
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    assert verifier.stored_key == hashlib.sha256(client_key).digest()
    assert verifier.server_key == hmac.new(salted, b"Server Key", hashlib.sha256).digest()


def test_the_verifier_uses_postgres_iteration_count(verifier):
    assert verifier.iterations == DEFAULT_ITERATIONS == 4096


def test_two_verifiers_for_one_password_differ(verifier):
    """A per-user salt, so one rainbow table does not answer for the whole deployment."""
    assert make_verifier("correct horse battery staple").salt != verifier.salt


def test_the_verifier_survives_the_pg_authid_spelling(verifier):
    assert parse_verifier(verifier.serialize()) == verifier


def test_the_serialized_verifier_is_the_format_pg_authid_holds(verifier):
    serialized = verifier.serialize()
    assert serialized.startswith(f"{MECHANISM}${verifier.iterations}:")
    prefix, params, keys = serialized.split("$")
    assert prefix == MECHANISM
    assert base64.b64decode(params.split(":", 1)[1]) == verifier.salt
    stored, server = keys.split(":")
    assert (base64.b64decode(stored), base64.b64decode(server)) == (
        verifier.stored_key,
        verifier.server_key,
    )


@pytest.mark.parametrize(
    "serialized",
    [
        "MD5$4096:c2FsdA==$c3RvcmVk:c2VydmVy",
        "SCRAM-SHA-256$4096:c2FsdA==",
        "SCRAM-SHA-256$:c2FsdA==$c3RvcmVk:c2VydmVy",
        "SCRAM-SHA-256$4096:$c3RvcmVk:c2VydmVy",
        "",
    ],
)
def test_a_verifier_this_code_did_not_write_is_refused(serialized):
    with pytest.raises(ValueError):
        parse_verifier(serialized)


# ── The exchange ──────────────────────────────────────────────────────────────────────────────


def test_the_right_password_authenticates(verifier):
    client = _Client("alice", "correct horse battery staple")
    exchange = ScramExchange(verifier)
    server_final = exchange.server_final(client.final(exchange.server_first(client.first)))
    assert client.check_server_signature(server_final)


def test_the_wrong_password_does_not(verifier):
    client = _Client("alice", "correct horse battery staple")
    exchange = ScramExchange(verifier)
    server_first = exchange.server_first(client.first)
    with pytest.raises(ScramError, match="authentication failed"):
        exchange.server_final(client.final(server_first, password="Tr0ub4dor&3"))


def test_the_server_first_message_carries_the_salt_and_iteration_count(verifier):
    exchange = ScramExchange(verifier)
    fields = dict(
        part.split("=", 1)
        for part in exchange.server_first(_Client("alice", "pw").first).split(",")
    )
    assert base64.b64decode(fields["s"]) == verifier.salt
    assert int(fields["i"]) == verifier.iterations


def test_the_server_extends_the_client_nonce_rather_than_replacing_it(verifier):
    """The client checks this to know the server is live rather than replaying a recorded run."""
    exchange = ScramExchange(verifier)
    server_first = exchange.server_first(_Client("alice", "pw", nonce="abc123").first)
    nonce = dict(part.split("=", 1) for part in server_first.split(","))["r"]
    assert nonce.startswith("abc123")
    assert len(nonce) > len("abc123")


def test_two_exchanges_do_not_reuse_a_nonce(verifier):
    def nonce_of(exchange):
        first = exchange.server_first(_Client("alice", "pw").first)
        return dict(part.split("=", 1) for part in first.split(","))["r"]

    assert nonce_of(ScramExchange(verifier)) != nonce_of(ScramExchange(verifier))


def test_a_proof_from_another_exchange_is_refused(verifier):
    """A recorded proof replayed against a fresh exchange, which the server nonce is there to stop."""
    client = _Client("alice", "correct horse battery staple")
    recorded = ScramExchange(verifier)
    stale = client.final(recorded.server_first(client.first))

    fresh = ScramExchange(verifier)
    fresh.server_first(client.first)
    with pytest.raises(ScramError, match="nonce"):
        fresh.server_final(stale)


def test_channel_binding_is_refused_rather_than_pretended(verifier):
    """SCRAM-SHA-256-PLUS is not advertised, so a client asking to bind must not be told it did."""
    exchange = ScramExchange(verifier)
    with pytest.raises(ScramError, match="channel binding"):
        exchange.server_first("p=tls-server-end-point,,n=alice,r=clientnonce")


def test_an_unknown_gs2_flag_is_refused(verifier):
    with pytest.raises(ScramError, match="gs2 flag"):
        ScramExchange(verifier).server_first("x,,n=alice,r=clientnonce")


def test_a_client_first_with_no_nonce_is_refused(verifier):
    with pytest.raises(ScramError, match="no nonce"):
        ScramExchange(verifier).server_first("n,,n=alice")


def test_a_stripped_channel_binding_request_is_caught(verifier):
    """The gs2 header is signed, so a proxy downgrading `y` to `n` cannot go unnoticed."""
    client = _Client("alice", "correct horse battery staple")
    client.gs2_header = "y,,"
    exchange = ScramExchange(verifier)
    server_first = exchange.server_first("n,," + client.first_bare)
    with pytest.raises(ScramError, match="channel-binding data"):
        exchange.server_final(client.final(server_first))


def test_a_client_final_with_no_channel_binding_field_is_refused(verifier):
    client = _Client("alice", "pw")
    exchange = ScramExchange(verifier)
    server_first = exchange.server_first(client.first)
    nonce = dict(part.split("=", 1) for part in server_first.split(","))["r"]
    with pytest.raises(ScramError, match="channel-binding field"):
        exchange.server_final(f"r={nonce},p={_b64(b'x' * 32)}")


def test_a_client_final_with_no_proof_is_refused(verifier):
    client = _Client("alice", "pw")
    exchange = ScramExchange(verifier)
    server_first = exchange.server_first(client.first)
    without_proof = client.final(server_first).rsplit(",p=", 1)[0]
    with pytest.raises(ScramError, match="no proof"):
        exchange.server_final(without_proof)


def test_a_proof_of_the_wrong_length_is_refused(verifier):
    """Refused as a malformed message, never fed to a comparison that would truncate it."""
    client = _Client("alice", "pw")
    exchange = ScramExchange(verifier)
    server_first = exchange.server_first(client.first)
    final = client.final(server_first).rsplit(",p=", 1)[0]
    with pytest.raises(ScramError, match="wrong length"):
        exchange.server_final(f"{final},p={_b64(b'short')}")


def test_client_first_cannot_be_sent_twice(verifier):
    """Restarting the exchange would reissue a nonce and let a client hunt for a favourable one."""
    exchange = ScramExchange(verifier)
    exchange.server_first(_Client("alice", "pw").first)
    with pytest.raises(ScramError, match="twice"):
        exchange.server_first(_Client("alice", "pw").first)


def test_client_final_before_client_first_is_refused(verifier):
    with pytest.raises(ScramError, match="before client-first"):
        ScramExchange(verifier).server_final("c=biws,r=x,p=" + _b64(b"y" * 32))


# ── Mock authentication ───────────────────────────────────────────────────────────────────────


def test_a_user_with_no_verifier_gets_an_exchange_that_looks_the_same():
    """The shape of the answer must not say whether the account exists."""
    seed = b"\x01" * 32
    real = ScramExchange(make_verifier("pw")).server_first(_Client("alice", "pw").first)
    mock = ScramExchange(mock_verifier("nobody", seed)).server_first(_Client("nobody", "pw").first)
    assert sorted(f.split("=", 1)[0] for f in real.split(",")) == sorted(
        f.split("=", 1)[0] for f in mock.split(",")
    )
    real_fields = dict(part.split("=", 1) for part in real.split(","))
    mock_fields = dict(part.split("=", 1) for part in mock.split(","))
    assert mock_fields["i"] == real_fields["i"]
    assert len(base64.b64decode(mock_fields["s"])) == len(base64.b64decode(real_fields["s"]))


def test_no_password_satisfies_a_mock_verifier():
    exchange = ScramExchange(mock_verifier("nobody", b"\x02" * 32))
    client = _Client("nobody", "")
    server_first = exchange.server_first(client.first)
    for guess in ("", "password", "admin", "nobody"):
        with pytest.raises(ScramError, match="authentication failed"):
            exchange.server_final(client.final(server_first, password=guess))


def test_a_mock_salt_is_stable_for_one_name_and_seed():
    """Stable so a repeated attempt on the same name does not look different from a real account."""
    seed = b"\x03" * 32
    assert mock_verifier("nobody", seed).salt == mock_verifier("nobody", seed).salt


def test_mock_salts_differ_by_name_and_by_deployment():
    assert mock_verifier("a", b"\x04" * 32).salt != mock_verifier("b", b"\x04" * 32).salt
    assert mock_verifier("a", b"\x04" * 32).salt != mock_verifier("a", b"\x05" * 32).salt
