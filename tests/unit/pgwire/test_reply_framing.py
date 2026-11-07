# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1f0e7a-93d2-4b8e-a6f1-2d7b40c9e815
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A reply leaves pgwire as PostgreSQL's does (REQ-2203): its messages are buffered and sent
together when the server next waits on the client, not one send per message.

A driver waiting on the socket wakes once per send that reaches it: a reply sent message by
message (RowDescription, DataRow, CommandComplete, ReadyForQuery, each its own segment, as the
connection is TCP_NODELAY) wakes it once per message. psycopg's own
test_waiting::test_wait_no_busy_loop counts those wakes, and failed or passed by how the segments
happened to coalesce."""

# Requirements: REQ-2203

from __future__ import annotations

import base64
import select
import socket
import threading
import time
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from provisa.pgwire.server import ProvisaConnection, ProvisaServer


class _RecordingSocket(socket.socket):
    """The server's end of a connection, recording each send it makes."""

    sends: list[bytes]

    def send(self, data, *args):  # type: ignore[override]
        sent = super().send(data, *args)
        self.sends.append(bytes(data[:sent]))
        return sent

    def sendall(self, data, *args):  # type: ignore[override]
        self.sends.append(bytes(data))
        return super().sendall(data, *args)


def _provider():
    from provisa.auth.models import AuthIdentity

    class _Stub:
        auth_scheme = "basic"

        @property
        def token_validators(self):
            return {"basic": self._validate}

        async def _validate(self, token: str) -> AuthIdentity:
            username, password = base64.b64decode(token).decode().split(":", 1)
            if username != "alice" or password != "secret":
                raise ValueError("Invalid credentials")
            return AuthIdentity(
                user_id=username,
                email=None,
                display_name=username,
                roles=["analyst"],
                raw_claims={"sub": username},
            )

    return _Stub()


@pytest.fixture
def recorded():
    """A pgwire server whose accepted connections record their sends: yields (port, sockets)."""
    sockets: list[_RecordingSocket] = []

    class _Server(ProvisaServer):
        # A connection a failed test left open must not hold the teardown on its thread.
        block_on_close = False

        def get_request(self):
            sock, address = self.socket.accept()
            recording = _RecordingSocket(sock.family, sock.type, sock.proto, fileno=sock.detach())
            recording.sends = []
            sockets.append(recording)
            return recording, address

    server = _Server(("127.0.0.1", 0), ProvisaConnection(), ssl_ctx=None)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.contexts = {"analyst": ctx}
    state.roles = {"analyst": {"id": "analyst", "capabilities": ["query_development"]}}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = {"provider": "simple", "default_role": "analyst", "role_mapping": []}
    state.auth_middleware_active = True
    state.multitenancy = False
    state.admin_db = None
    with (
        patch("provisa.auth.wiring.build_auth_provider", return_value=_provider()),
        patch("provisa.api.app.state", state),
    ):
        yield server.server_address[1], sockets
    server.shutdown()
    server.server_close()


def _drain(pgconn, timeout: float = 10) -> list[psycopg.pq.abc.PGresult]:
    """Read the reply's results, so the connection closes idle.

    The wait is select()'s, which lets the GIL go: libpq's blocking PQgetResult holds it, and the
    server writing the reply is a thread of this process, so a get_result() that waits on the
    reply waits for ever. get_result() is called only once the result is in (``is_busy()`` false)."""
    deadline = time.monotonic() + timeout
    results = []
    while True:
        while pgconn.is_busy():
            left = deadline - time.monotonic()
            assert left > 0, f"no whole reply in {timeout}s: {results!r}"
            select.select([pgconn.socket], [], [], left)
            pgconn.consume_input()
        result = pgconn.get_result()
        if result is None:
            return results
        results.append(result)


def _idle_sends(sockets: list[_RecordingSocket]) -> int:
    """How many sends the connection has made once it is idle: its last ends with ReadyForQuery.

    A send is recorded once it returns, after its bytes have left: the client can have read the
    ReadyForQuery that ends connecting before its send is recorded, and a count read then puts that
    send in the next reply's."""
    return len(_reply_sends(sockets, 0))


def _reply_sends(sockets: list[_RecordingSocket], before: int) -> list[bytes]:
    """The sends the connection made after the first ``before``, once its ReadyForQuery is out."""
    (sock,) = sockets
    deadline = time.monotonic() + 5
    while not b"".join(sock.sends[before:]).endswith(b"Z\x00\x00\x00\x05I"):
        assert time.monotonic() < deadline, f"no ReadyForQuery sent: {sock.sends[before:]!r}"
        time.sleep(0.01)
    return sock.sends[before:]


def test_a_simple_query_reply_is_one_send(recorded):
    port, sockets = recorded
    with psycopg.connect(
        host="127.0.0.1", port=port, user="alice", password="secret", dbname="provisa",
        connect_timeout=10, autocommit=True,
    ) as conn:  # fmt: skip
        before = _idle_sends(sockets)
        conn.pgconn.send_query(b"SELECT pg_sleep(0.01)")
        sends = _reply_sends(sockets, before)
        _drain(conn.pgconn)
    reply = b"".join(sends)
    assert [reply[:1], reply[-6:]] == [b"T", b"Z\x00\x00\x00\x05I"]
    assert len(sends) == 1, [s[:1] for s in sends]


def test_an_extended_query_reply_is_one_send_at_sync(recorded):
    port, sockets = recorded
    with psycopg.connect(
        host="127.0.0.1", port=port, user="alice", password="secret", dbname="provisa",
        connect_timeout=10, autocommit=True,
    ) as conn:  # fmt: skip
        before = _idle_sends(sockets)
        conn.pgconn.send_query_params(b"SELECT pg_sleep(0.01)", [], result_format=0)
        sends = _reply_sends(sockets, before)
        _drain(conn.pgconn)
    reply = b"".join(sends)
    assert [reply[:1], reply[-6:]] == [b"1", b"Z\x00\x00\x00\x05I"]
    assert len(sends) == 1, [s[:1] for s in sends]


def test_a_reply_read_while_the_server_is_on_it_arrives_whole(recorded):
    port, _ = recorded
    with psycopg.connect(
        host="127.0.0.1", port=port, user="alice", password="secret", dbname="provisa",
        connect_timeout=10, autocommit=True,
    ) as conn:  # fmt: skip
        conn.pgconn.send_query_params(b"SELECT pg_sleep(0.2)", [], result_format=0)
        results = _drain(conn.pgconn)
    assert [r.status for r in results] == [psycopg.pq.ExecStatus.TUPLES_OK]
