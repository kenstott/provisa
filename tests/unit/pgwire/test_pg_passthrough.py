# Copyright (c) 2026 Kenneth Stott
# Canary: bd0ce2d3-ff49-4901-9b8d-2103dc40643b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1863: PassthroughCursor forwards raw DataRow message bytes byte-for-byte, unmodified —
verified over a real socketpair against a scripted fake Postgres server (Parse/Bind/Execute/Sync
exchange), never a live Postgres. Also verifies the fallback triggers: an ErrorResponse and a
column-count mismatch both raise PassthroughError rather than corrupting output."""

from __future__ import annotations

import asyncio
import socket
import struct

import pytest

from provisa.pgwire.pg_passthrough import PassthroughCursor, PassthroughError, RawPgConnection

_INT32 = struct.Struct("!i")
_INT16 = struct.Struct("!h")


def _msg(tag: bytes, payload: bytes = b"") -> bytes:
    return tag + _INT32.pack(4 + len(payload)) + payload


def _data_row(ncols: int, values: list[bytes | None]) -> bytes:
    body = bytearray(_INT16.pack(ncols))
    for v in values:
        if v is None:
            body += b"\xff\xff\xff\xff"
        else:
            body += _INT32.pack(len(v)) + v
    return _msg(b"D", bytes(body))


def _make_raw_connection() -> tuple[RawPgConnection, socket.socket]:
    """A connected socketpair standing in for the dedicated raw connection's transport — one end
    driven by PassthroughCursor via the event loop, the other end played by the test as the fake
    Postgres server."""
    client_sock, server_sock = socket.socketpair()
    client_sock.setblocking(False)
    server_sock.setblocking(False)
    raw = RawPgConnection(_asyncpg_conn=None, _loop=asyncio.get_event_loop(), _sock=client_sock)
    return raw, server_sock


async def _server_recv_parse_and_bind(server_sock: socket.socket) -> None:
    """Drain the Parse+Bind messages the cursor sends on its first fetch() — enough bytes to
    unblock the test without fully parsing them (this test asserts on OUTPUT, not on-wire input)."""
    loop = asyncio.get_event_loop()
    # Parse and Bind together are comfortably under 4KB for these small test queries.
    await loop.sock_recv(server_sock, 65536)


async def test_fetch_forwards_data_row_bytes_unmodified():
    raw, server_sock = _make_raw_connection()
    try:
        cursor = PassthroughCursor(raw, "SELECT a, b FROM t", [], [0, 0], expected_column_count=2)

        row1 = _data_row(2, [b"1", b"hello"])
        row2 = _data_row(2, [b"2", None])
        server_script = (
            _msg(b"1") + _msg(b"2") + row1 + row2 + _msg(b"C", b"SELECT 2\x00") + _msg(b"Z", b"I")
        )

        async def _server() -> None:
            await _server_recv_parse_and_bind(server_sock)
            loop = asyncio.get_event_loop()
            await loop.sock_sendall(server_sock, server_script)

        server_task = asyncio.ensure_future(_server())
        rows = await cursor.fetch(10_000)
        await server_task

        assert rows == [row1, row2]
        # A second fetch after CommandComplete must not re-send Parse/Bind or hang.
        assert await cursor.fetch(10_000) == []
    finally:
        server_sock.close()
        raw._sock.close()


async def test_fetch_respects_portal_suspend_across_two_batches():
    raw, server_sock = _make_raw_connection()
    try:
        cursor = PassthroughCursor(raw, "SELECT a FROM t", [], [0], expected_column_count=1)
        row1 = _data_row(1, [b"1"])
        row2 = _data_row(1, [b"2"])

        async def _server() -> None:
            loop = asyncio.get_event_loop()
            await _server_recv_parse_and_bind(server_sock)
            await loop.sock_sendall(
                server_sock, _msg(b"1") + _msg(b"2") + row1 + _msg(b"s") + _msg(b"Z", b"I")
            )
            await loop.sock_recv(server_sock, 65536)  # second Execute+Sync
            await loop.sock_sendall(
                server_sock, row2 + _msg(b"C", b"SELECT 2\x00") + _msg(b"Z", b"I")
            )

        server_task = asyncio.ensure_future(_server())
        first_batch = await cursor.fetch(1)
        second_batch = await cursor.fetch(1)
        await server_task

        assert first_batch == [row1]
        assert second_batch == [row2]
    finally:
        server_sock.close()
        raw._sock.close()


async def test_error_response_raises_passthrough_error():
    raw, server_sock = _make_raw_connection()
    try:
        cursor = PassthroughCursor(raw, "SELECT 1/0", [], [0], expected_column_count=1)

        async def _server() -> None:
            loop = asyncio.get_event_loop()
            await _server_recv_parse_and_bind(server_sock)
            await loop.sock_sendall(
                server_sock,
                _msg(b"1")
                + _msg(b"2")
                + _msg(b"E", b"SDIVISION_BY_ZERO\x00Mdivision by zero\x00\x00")
                + _msg(b"Z", b"I"),
            )

        server_task = asyncio.ensure_future(_server())
        with pytest.raises(PassthroughError, match="division by zero"):
            await cursor.fetch(10_000)
        await server_task
    finally:
        server_sock.close()
        raw._sock.close()


async def test_column_count_mismatch_raises_passthrough_error():
    raw, server_sock = _make_raw_connection()
    try:
        # Client was told to expect 2 columns; the source's own DataRow says 3 — must not be
        # silently forwarded (the downstream client already committed to a 2-column RowDescription).
        cursor = PassthroughCursor(raw, "SELECT * FROM t", [], [0, 0], expected_column_count=2)
        bad_row = _data_row(3, [b"1", b"2", b"3"])

        async def _server() -> None:
            loop = asyncio.get_event_loop()
            await _server_recv_parse_and_bind(server_sock)
            await loop.sock_sendall(
                server_sock,
                _msg(b"1") + _msg(b"2") + bad_row + _msg(b"C", b"SELECT 1\x00") + _msg(b"Z", b"I"),
            )

        server_task = asyncio.ensure_future(_server())
        with pytest.raises(PassthroughError, match="column count mismatch"):
            await cursor.fetch(10_000)
        await server_task
    finally:
        server_sock.close()
        raw._sock.close()
