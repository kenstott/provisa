# Copyright (c) 2026 Kenneth Stott
# Canary: bbed1f5e-9dfa-4ade-a71c-a0775a82b699
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bolt request whose records are never pulled ends with its connection (REQ-1905).

A RUN holds its request's deadline until the records are pulled (PULL, DISCARD or RESET end it).
A connection that ends first — GOODBYE, the client gone, the session defunct — ends that request
too: its deadline stops and is no longer a live request the worker's shutdown counts."""

# Requirements: REQ-1905

from __future__ import annotations

from unittest.mock import patch

import pytest

from provisa.core import request_deadline

pytestmark = pytest.mark.asyncio


async def _run_unpulled():
    from tests.unit.test_bolt_show_databases import _make_session

    session, _writer = _make_session(["analyst"])

    async def execute(*_args, **_kwargs):
        return ["id"], [[1]], None

    with (
        patch.object(session, "_resolve_db", return_value=("analyst", False)),
        patch("provisa.bolt.session._execute_cypher", execute),
    ):
        await session.handle_run(["MATCH (p:Pets) RETURN p.id AS id", {}, {}])
    return session


async def test_closing_the_session_ends_its_unpulled_request():
    session = await _run_unpulled()
    try:
        assert session._deadline is not None  # held for the PULL that never comes
    finally:
        session.close()
    assert session._deadline is None
    assert request_deadline.expire_all("the server is shutting down") == 0


@pytest.mark.parametrize("ending", [None, ConnectionResetError("the client went away")])
async def test_the_server_closes_the_session_however_its_connection_ends(monkeypatch, ending):
    """Once negotiated, the message loop ending — normally or by the client vanishing — closes
    the session."""
    import asyncio

    from provisa.bolt import server
    from provisa.bolt.messages import MAGIC

    closed: list[object] = []
    monkeypatch.setattr(server.BoltSession, "close", lambda self: closed.append(self))

    async def _loop(_session, _reader, _writer):
        if ending is not None:
            raise ending

    monkeypatch.setattr(server, "_serve", _loop)

    class _Writer:
        def write(self, _data: bytes) -> None:
            pass

        async def drain(self) -> None:
            pass

    reader = asyncio.StreamReader()
    reader.feed_data(MAGIC + bytes([0, 0, 4, 5]) + bytes(12))  # proposes Bolt 5.4
    if ending is None:
        await server._bolt_handshake_and_serve(reader, _Writer())  # type: ignore[arg-type]
    else:
        with pytest.raises(ConnectionResetError):
            await server._bolt_handshake_and_serve(reader, _Writer())  # type: ignore[arg-type]
    assert len(closed) == 1
