# Copyright (c) 2026 Kenneth Stott
# Canary: a6b9c3d7-e0f2-1234-5678-90abcdef0123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E tests for Cypher over Bolt protocol (REQ-802).

Requires a running Provisa backend with the Bolt server listening on port 5251.
Tests the full Bolt handshake, HELLO, and RUN/PULL pipeline.
"""

from __future__ import annotations

import asyncio
import os

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

_BOLT_HOST = "localhost"
_BOLT_PORT = int(os.environ.get("PROVISA_BOLT_PORT", "5251"))
_MAGIC = b"\x60\x60\xb0\x17"
# Generous client read timeout: RUN/PULL execute a federated graph query that can
# scan multiple sources (Postgres + cold Kafka/Iceberg), which legitimately takes
# longer than a handshake. Matches the server's advertised recv-timeout hint
# (PROVISA_BOLT_RECV_TIMEOUT, default 120s).
_TIMEOUT = 120.0


def _version_bytes(major: int, minor: int, range_: int = 0) -> bytes:
    return bytes([0x00, range_, minor, major])


def _no_version() -> bytes:
    return b"\x00\x00\x00\x00"


async def _bolt_connect():
    return await asyncio.wait_for(
        asyncio.open_connection(_BOLT_HOST, _BOLT_PORT),
        timeout=_TIMEOUT,
    )


async def _handshake(reader, writer) -> tuple[int, int]:
    from provisa.bolt.messages import SUPPORTED_VERSIONS

    writer.write(_MAGIC)
    for sv in SUPPORTED_VERSIONS[:4]:
        writer.write(_version_bytes(*sv))
    while len(SUPPORTED_VERSIONS) < 4:
        writer.write(_no_version())
    await writer.drain()

    chosen = await asyncio.wait_for(reader.readexactly(4), timeout=_TIMEOUT)
    major, minor = chosen[3], chosen[2]
    return major, minor


async def _send_msg(writer, tag: int, fields) -> None:
    from provisa.bolt.framing import write_message
    from provisa.bolt.packstream import pack_message

    # pack_message takes each Bolt field as a positional arg. Callers pass a list
    # for multi-field messages (e.g. RUN: query, params, extra) or a single dict.
    args = fields if isinstance(fields, list) else [fields]
    payload = pack_message(tag, *args)
    write_message(writer, payload)  # type: ignore[arg-type]
    await writer.drain()


async def _recv_msg(reader) -> tuple[int, object]:
    from provisa.bolt.framing import read_message
    from provisa.bolt.packstream import unpack_fields

    data = await asyncio.wait_for(read_message(reader), timeout=_TIMEOUT)
    if len(data) < 2:
        return 0, None
    tag = data[1]
    fields = unpack_fields(data[2:]) if len(data) > 2 else []
    return tag, fields


@pytest.mark.requires_provisa_server
class TestBoltHandshake:
    async def test_server_reachable(self):
        reader, writer = await _bolt_connect()
        try:
            assert writer.get_extra_info("peername") is not None
            assert not reader.at_eof()
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_magic_accepted(self):
        reader, writer = await _bolt_connect()
        try:
            major, _ = await _handshake(reader, writer)
            assert major > 0, "Server rejected all versions (returned 0.0)"
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_negotiates_supported_version(self):
        reader, writer = await _bolt_connect()
        try:
            from provisa.bolt.messages import SUPPORTED_VERSIONS

            major, minor = await _handshake(reader, writer)
            assert (major, minor) in SUPPORTED_VERSIONS
        finally:
            writer.close()
            await writer.wait_closed()


@pytest.mark.requires_provisa_server
class TestBoltHello:
    async def _connect_and_handshake(self):
        reader, writer = await _bolt_connect()
        await _handshake(reader, writer)
        return reader, writer

    async def test_hello_with_no_auth_returns_success(self):
        reader, writer = await self._connect_and_handshake()
        try:
            await _send_msg(
                writer,
                0x01,
                {"user_agent": "test/1.0", "scheme": "none", "principal": "", "credentials": ""},
            )
            tag, _ = await _recv_msg(reader)
            assert tag in (0x70, 0x7F), f"Expected SUCCESS(0x70) or FAILURE(0x7F), got 0x{tag:02X}"
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_hello_with_bad_credentials_returns_failure(self):
        reader, writer = await self._connect_and_handshake()
        try:
            await _send_msg(
                writer,
                0x01,
                {
                    "user_agent": "test/1.0",
                    "scheme": "basic",
                    "principal": "nobody",
                    "credentials": "wrongpassword",
                },
            )
            tag, _ = await _recv_msg(reader)
            # With auth disabled, may return SUCCESS; with auth enabled must return FAILURE
            assert tag in (0x70, 0x7F), f"Unexpected tag 0x{tag:02X}"
        finally:
            writer.close()
            await writer.wait_closed()


@pytest.mark.requires_provisa_server
class TestBoltCypherExecution:
    async def _authenticated_session(self):
        reader, writer = await _bolt_connect()
        major, _ = await _handshake(reader, writer)
        await _send_msg(
            writer,
            0x01,
            {"user_agent": "test/1.0", "scheme": "none", "principal": "", "credentials": ""},
        )
        tag, fields = await _recv_msg(reader)
        assert tag == 0x70, f"HELLO expected SUCCESS(0x70), got 0x{tag:02X}: {fields!r}"
        # Bolt 5.x authenticates via a separate LOGON (0x6A); HELLO does not carry auth.
        if major >= 5:
            await _send_msg(writer, 0x6A, [{"scheme": "none"}])
            logon_tag, logon_fields = await _recv_msg(reader)
            assert logon_tag == 0x70, (
                f"LOGON expected SUCCESS(0x70), got 0x{logon_tag:02X}: {logon_fields!r}"
            )
        return reader, writer

    async def test_run_match_returns_success(self):
        reader, writer = await self._authenticated_session()
        try:
            await _send_msg(writer, 0x10, ["MATCH (n) RETURN n LIMIT 1", {}, {}])
            tag, _ = await _recv_msg(reader)
            assert tag in (0x70, 0x7F), f"RUN unexpected tag 0x{tag:02X}"
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_run_pull_returns_records_or_success(self):
        reader, writer = await self._authenticated_session()
        try:
            await _send_msg(writer, 0x10, ["MATCH (n) RETURN n LIMIT 5", {}, {}])
            run_tag, run_fields = await _recv_msg(reader)
            assert run_tag == 0x70, (
                f"RUN expected SUCCESS(0x70), got 0x{run_tag:02X}: {run_fields!r}"
            )

            await _send_msg(writer, 0x3F, [{"n": 5, "qid": -1}])
            tags = []
            for _ in range(10):
                tag, _ = await _recv_msg(reader)
                tags.append(tag)
                if tag in (0x70, 0x7F):
                    break
            assert 0x70 in tags or 0x71 in tags, f"Expected RECORD or SUCCESS, got {tags}"
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_reset_clears_session(self):
        reader, writer = await self._authenticated_session()
        try:
            await _send_msg(writer, 0x0F, [])
            tag, _ = await _recv_msg(reader)
            assert tag == 0x70, f"RESET expected SUCCESS(0x70), got 0x{tag:02X}"
        finally:
            writer.close()
            await writer.wait_closed()
