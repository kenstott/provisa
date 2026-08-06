# Copyright (c) 2026 Kenneth Stott
# Canary: 8e788d30-2239-4eb5-8c8b-89c6973b7a99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1394 — SCRAM as it reaches the pgwire socket.

The mechanism itself is proved in ``test_scram.py``. What is proved here is everything around it:
that the server advertises SASL only where SCRAM can actually authenticate someone, that the two
round trips are framed the way libpq expects to read them, that a proof turns into a session by
re-reading the account rather than by trusting the exchange, and that a failed proof lands in the
same lockout counter a wrong password on any other surface lands in (REQ-1393).

The exchange is driven through ``ProvisaHandler`` with a client that speaks the wire messages, so
a change to the framing fails here rather than in a driver.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import struct
import threading
from types import SimpleNamespace
from typing import Any, cast

import bcrypt
import pytest

from provisa.auth.scram import MECHANISM, make_verifier
from provisa.auth.throttle import login_throttle, reset_login_throttle, subject_key

# Requirements: REQ-1394, REQ-1393

_USERNAME = "alice"
_PASSWORD = "correct horse battery staple"

_AUTHENTICATION_REQUEST = b"R"
_SASL = 10
_SASL_CONTINUE = 11
_SASL_FINAL = 12


@pytest.fixture(autouse=True)
def clean_throttle():
    """No lockout carries between tests — REQ-1393's counter is process-wide."""
    reset_login_throttle()
    yield
    reset_login_throttle()


@pytest.fixture
def pgwire_loop():
    """pgwire authenticates on a worker thread and submits its coroutines to the main loop."""
    import provisa.pgwire.server as pg_server

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    previous = pg_server._loop
    pg_server._loop = loop
    try:
        yield loop
    finally:
        pg_server._loop = previous
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()


# ── The stand-in control plane ────────────────────────────────────────────────────────────────


class _Pool:
    """The platform control plane, holding one user row and one verifier row.

    Both ``read_verifier`` and the basic provider's account lookup go through the same pool, so
    the connection answers by which table the statement names.
    """

    def __init__(self, *, verifier: str | None, user_row: dict | None) -> None:
        self.verifier = verifier
        self.user_row = user_row

    def acquire(self):
        pool = self

        class _Result:
            def __init__(self, value) -> None:
                self._value = value

            def fetchone(self):
                return self._value

        class _Conn:
            async def execute_core(self, statement):
                text = str(statement)
                if "scram_credentials" in text:
                    return _Result(None if pool.verifier is None else (pool.verifier,))
                assert "local_users" in text, text
                return _Result(
                    None
                    if pool.user_row is None
                    else SimpleNamespace(_mapping=pool.user_row)  # type: ignore[arg-type]
                )

        class _Ctx:
            async def __aenter__(self):
                return _Conn()

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


def _user_row() -> dict:
    return {
        "id": "user-1",
        "username": _USERNAME,
        "password_hash": bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt(rounds=4)).decode(),
        "email": "alice@example.invalid",
        "display_name": "Alice",
        "attributes": {"team": "data"},
    }


def _auth_config(**overrides) -> dict:
    config = {
        "provider": "basic",
        "scram": True,
        "jwt_secret": "s" * 40,
        "role_mapping": [],
        "default_role": "analyst",
    }
    config.update(overrides)
    return config


def _state(auth_config: dict, pool: _Pool | None) -> SimpleNamespace:
    return SimpleNamespace(
        auth_config=auth_config,
        auth_middleware_active=True,
        multitenancy=False,
        admin_db=pool,
        tenant_db=None,
    )


# ── The handler under test ────────────────────────────────────────────────────────────────────


class _Handler:
    """A ProvisaHandler wired to buffers instead of a socket."""

    def __init__(self, state) -> None:
        from provisa.pgwire.server import ProvisaHandler

        self.written = bytearray()
        self.errors: list[tuple[str, str, str]] = []
        self.authenticated = False

        handler = cast(Any, object.__new__(ProvisaHandler))
        handler.wfile = SimpleNamespace(
            write=self.written.extend, flush=lambda: None
        )
        handler._send_pg_error = lambda severity, sqlstate, message: self.errors.append(
            (severity, sqlstate, message)
        )
        handler._complete_auth = self._complete
        handler.send_authentication_ok = lambda: None
        handler.handle_post_auth = lambda ctx: None  # noqa: ARG005 — signature match
        self.handler = handler
        self.state = state
        self.identity = None
        self.ctx = cast(
            Any,
            SimpleNamespace(
                params={"user": _USERNAME}, session=SimpleNamespace(org_id=None, role_id=None)
            ),
        )

    def _complete(self, ctx, identity, auth_config):  # noqa: ARG002 — signature match
        self.authenticated = True
        self.identity = identity

    def messages(self) -> list[tuple[bytes, bytes]]:
        """The AuthenticationRequest frames written so far, as (subcode, body)."""
        out: list[tuple[bytes, bytes]] = []
        buf = memoryview(bytes(self.written))
        while buf:
            assert bytes(buf[:1]) == _AUTHENTICATION_REQUEST
            (length,) = struct.unpack("!i", buf[1:5])
            out.append((bytes(buf[5:9]), bytes(buf[9 : 1 + length])))
            buf = buf[1 + length :]
        return out

    def send_auth_request(self):
        from unittest.mock import patch

        with patch("provisa.pgwire.server.state", self.state):
            self.handler.send_auth_request(self.ctx)

    def password_message(self, payload: bytes):
        from unittest.mock import patch

        with patch("provisa.pgwire.server.state", self.state):
            self.handler.handle_md5_password(self.ctx, payload)


def _subcode(raw: bytes) -> int:
    return struct.unpack("!i", raw)[0]


class _Client:
    """The client half of the exchange, framed the way libpq frames it."""

    def __init__(self, password: str, *, nonce: str = "clientnonce") -> None:
        self.password = password
        self.gs2_header = "n,,"
        self.first_bare = f"n=,r={nonce}"

    def initial_response(self) -> bytes:
        client_first = (self.gs2_header + self.first_bare).encode("utf-8")
        return MECHANISM.encode("ascii") + b"\x00" + struct.pack("!i", len(client_first)) + client_first

    def final(self, server_first: str, *, password: str | None = None) -> bytes:
        fields = dict(part.split("=", 1) for part in server_first.split(","))
        salted = hashlib.pbkdf2_hmac(
            "sha256",
            (password or self.password).encode("utf-8"),
            base64.b64decode(fields["s"]),
            int(fields["i"]),
        )
        client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
        stored_key = hashlib.sha256(client_key).digest()
        without_proof = (
            f"c={base64.b64encode(self.gs2_header.encode()).decode()},r={fields['r']}"
        )
        auth_message = f"{self.first_bare},{server_first},{without_proof}"
        signature = hmac.new(stored_key, auth_message.encode("utf-8"), hashlib.sha256).digest()
        proof = bytes(a ^ b for a, b in zip(client_key, signature))
        return f"{without_proof},p={base64.b64encode(proof).decode()}".encode("utf-8")


def _negotiate(handler: _Handler, client: _Client, *, password: str | None = None) -> None:
    handler.send_auth_request()
    handler.password_message(client.initial_response())
    frames = handler.messages()
    if _subcode(frames[-1][0]) != _SASL_CONTINUE:
        return
    server_first = frames[-1][1].decode("utf-8")
    handler.password_message(client.final(server_first, password=password))


# ── What is advertised ────────────────────────────────────────────────────────────────────────


def test_scram_is_advertised_when_the_deployment_turns_it_on():
    handler = _Handler(_state(_auth_config(), _Pool(verifier=None, user_row=None)))
    handler.send_auth_request()
    (subcode, body), = handler.messages()
    assert _subcode(subcode) == _SASL
    # NUL-terminated mechanism names ended by an empty one — the list libpq parses.
    assert body == MECHANISM.encode("ascii") + b"\x00\x00"


def test_the_cleartext_request_is_sent_when_scram_is_off():
    handler = _Handler(_state(_auth_config(scram=False), _Pool(verifier=None, user_row=None)))
    handler.send_auth_request()
    (subcode, body), = handler.messages()
    assert (_subcode(subcode), body) == (3, b"")


def test_a_bearer_provider_is_not_offered_scram():
    """A PAT or an OIDC token is an opaque secret; SCRAM has no way to carry one."""
    handler = _Handler(_state(_auth_config(provider="oidc"), _Pool(verifier=None, user_row=None)))
    handler.send_auth_request()
    assert _subcode(handler.messages()[0][0]) == 3


def test_trust_mode_is_not_offered_scram():
    state = SimpleNamespace(
        auth_config=_auth_config(), auth_middleware_active=False, admin_db=None
    )
    handler = _Handler(state)
    handler.send_auth_request()
    assert _subcode(handler.messages()[0][0]) == 3


# ── The exchange over the wire ────────────────────────────────────────────────────────────────


def test_the_right_password_authenticates_over_the_wire(pgwire_loop):
    pool = _Pool(verifier=make_verifier(_PASSWORD).serialize(), user_row=_user_row())
    handler = _Handler(_state(_auth_config(), pool))
    _negotiate(handler, _Client(_PASSWORD))

    subcodes = [_subcode(code) for code, _ in handler.messages()]
    assert subcodes == [_SASL, _SASL_CONTINUE, _SASL_FINAL]
    assert handler.errors == []
    assert handler.authenticated
    assert handler.identity is not None
    assert handler.identity.user_id == "user-1"


def test_the_server_proves_itself_in_the_final_message(pgwire_loop):
    """Mutual authentication: the client checks v= to know it did not talk to an impostor."""
    verifier = make_verifier(_PASSWORD)
    pool = _Pool(verifier=verifier.serialize(), user_row=_user_row())
    handler = _Handler(_state(_auth_config(), pool))
    client = _Client(_PASSWORD)

    handler.send_auth_request()
    handler.password_message(client.initial_response())
    server_first = handler.messages()[-1][1].decode("utf-8")
    handler.password_message(client.final(server_first))

    final = handler.messages()[-1][1].decode("utf-8")
    without_proof = (
        f"c=biws,r={dict(p.split('=', 1) for p in server_first.split(','))['r']}"
    )
    auth_message = f"{client.first_bare},{server_first},{without_proof}"
    expected = hmac.new(
        verifier.server_key, auth_message.encode("utf-8"), hashlib.sha256
    ).digest()
    assert final == "v=" + base64.b64encode(expected).decode()


def test_a_wrong_password_is_refused_over_the_wire(pgwire_loop):
    pool = _Pool(verifier=make_verifier(_PASSWORD).serialize(), user_row=_user_row())
    handler = _Handler(_state(_auth_config(), pool))
    _negotiate(handler, _Client(_PASSWORD), password="Tr0ub4dor&3")

    assert not handler.authenticated
    assert handler.errors == [
        ("FATAL", "28P01", f'password authentication failed for user "{_USERNAME}"')
    ]


def test_a_user_with_no_verifier_is_answered_like_one_with(pgwire_loop):
    """Mock authentication: the absence of a verifier must not be visible on the wire."""
    handler = _Handler(_state(_auth_config(), _Pool(verifier=None, user_row=None)))
    _negotiate(handler, _Client(_PASSWORD))

    subcodes = [_subcode(code) for code, _ in handler.messages()]
    assert subcodes == [_SASL, _SASL_CONTINUE]
    assert not handler.authenticated
    assert handler.errors == [
        ("FATAL", "28P01", f'password authentication failed for user "{_USERNAME}"')
    ]


def test_the_server_first_message_looks_the_same_for_an_unknown_user(pgwire_loop):
    """The salt differs, the shape does not — which is the whole point of the mock verifier."""

    def server_first(pool: _Pool) -> dict:
        handler = _Handler(_state(_auth_config(), pool))
        handler.send_auth_request()
        handler.password_message(_Client(_PASSWORD).initial_response())
        body = handler.messages()[-1][1].decode("utf-8")
        return dict(part.split("=", 1) for part in body.split(","))

    known = server_first(_Pool(verifier=make_verifier(_PASSWORD).serialize(), user_row=_user_row()))
    unknown = server_first(_Pool(verifier=None, user_row=None))
    assert sorted(known) == sorted(unknown)
    assert known["i"] == unknown["i"]
    assert len(base64.b64decode(known["s"])) == len(base64.b64decode(unknown["s"]))


def test_a_deactivated_account_is_refused_after_a_valid_proof(pgwire_loop):
    """The proof says the password is right; the account read says whether it still counts."""
    pool = _Pool(verifier=make_verifier(_PASSWORD).serialize(), user_row=None)
    handler = _Handler(_state(_auth_config(), pool))
    _negotiate(handler, _Client(_PASSWORD))

    assert not handler.authenticated
    assert handler.errors == [
        ("FATAL", "28P01", f'password authentication failed for user "{_USERNAME}"')
    ]


def test_a_mechanism_this_server_did_not_offer_is_refused(pgwire_loop):
    handler = _Handler(_state(_auth_config(), _Pool(verifier=None, user_row=None)))
    handler.send_auth_request()
    handler.password_message(b"SCRAM-SHA-256-PLUS\x00" + struct.pack("!i", 1) + b"n")

    assert handler.errors == [
        ("FATAL", "28000", "unsupported SASL mechanism: 'SCRAM-SHA-256-PLUS'")
    ]


def test_an_absent_initial_response_is_refused(pgwire_loop):
    handler = _Handler(_state(_auth_config(), _Pool(verifier=None, user_row=None)))
    handler.send_auth_request()
    handler.password_message(MECHANISM.encode("ascii") + b"\x00" + struct.pack("!i", -1))

    assert handler.errors == [("FATAL", "28000", "SASL initial response is required")]


def test_a_client_asking_for_channel_binding_is_refused(pgwire_loop):
    handler = _Handler(_state(_auth_config(), _Pool(verifier=None, user_row=None)))
    client = _Client(_PASSWORD)
    client.gs2_header = "p=tls-server-end-point,,"
    handler.send_auth_request()
    handler.password_message(client.initial_response())

    assert handler.errors[0][:2] == ("FATAL", "28000")
    assert "channel binding" in handler.errors[0][2]


# ── The lockout ───────────────────────────────────────────────────────────────────────────────


def test_a_failed_proof_counts_against_the_account(pgwire_loop):
    pool = _Pool(verifier=make_verifier(_PASSWORD).serialize(), user_row=_user_row())
    for _ in range(login_throttle().max_attempts):
        _negotiate(_Handler(_state(_auth_config(), pool)), _Client(_PASSWORD), password="wrong")

    from provisa.auth.throttle import LockedOut

    with pytest.raises(LockedOut):
        login_throttle().check(subject_key(_USERNAME, ""))


def test_a_locked_out_account_is_refused_before_the_exchange_starts(pgwire_loop):
    throttle = login_throttle()
    for _ in range(throttle.max_attempts):
        throttle.record_failure(subject_key(_USERNAME, ""))

    pool = _Pool(verifier=make_verifier(_PASSWORD).serialize(), user_row=_user_row())
    handler = _Handler(_state(_auth_config(), pool))
    handler.send_auth_request()
    handler.password_message(_Client(_PASSWORD).initial_response())

    assert [_subcode(code) for code, _ in handler.messages()] == [_SASL]
    assert handler.errors[0][:2] == ("FATAL", "28000")
    assert "too many failed authentication attempts" in handler.errors[0][2]


def test_a_valid_proof_clears_the_accounts_history(pgwire_loop):
    throttle = login_throttle()
    throttle.record_failure(subject_key(_USERNAME, ""))

    pool = _Pool(verifier=make_verifier(_PASSWORD).serialize(), user_row=_user_row())
    handler = _Handler(_state(_auth_config(), pool))
    _negotiate(handler, _Client(_PASSWORD))
    assert handler.authenticated

    for _ in range(throttle.max_attempts - 1):
        throttle.record_failure(subject_key(_USERNAME, ""))
    throttle.check(subject_key(_USERNAME, ""))  # still under the limit, so the history was cleared
