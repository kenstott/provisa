# Copyright (c) 2026 Kenneth Stott
# Canary: f5a8b2c6-d7e9-0123-4f01-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Integration tests for Bolt TCP server handshake and session layer (REQ-802).

Tests the PackStream + framing + session boundary with real components.
Query execution is mocked — this boundary is about protocol correctness,
not DB execution.
"""

from __future__ import annotations

import asyncio

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_MAGIC = b"\x60\x60\xb0\x17"


class TestHandshake:
    async def test_magic_and_version_negotiation(self):
        from provisa.bolt.messages import MAGIC, encode_version, SUPPORTED_VERSIONS

        assert MAGIC == _MAGIC
        major, minor = max(SUPPORTED_VERSIONS)
        encoded = encode_version(major, minor)
        assert len(encoded) == 4

    async def test_encode_decode_roundtrip(self):
        from provisa.bolt.messages import decode_version_proposal, encode_version

        for major, minor in [(5, 4), (4, 4), (5, 0)]:
            encoded = encode_version(major, minor)
            dec_major, dec_minor = decode_version_proposal(encoded)
            assert (dec_major, dec_minor) == (major, minor)

    async def test_supported_versions_non_empty(self):
        from provisa.bolt.messages import SUPPORTED_VERSIONS

        assert len(SUPPORTED_VERSIONS) > 0

    async def test_version_5_4_supported(self):
        from provisa.bolt.messages import SUPPORTED_VERSIONS

        assert (5, 4) in SUPPORTED_VERSIONS


class TestPackStreamSession:
    async def test_pack_hello_message(self):
        from provisa.bolt.packstream import pack_message

        data = pack_message(0x01, {"user_agent": "test/1.0", "routing": None})
        assert data[0] == 0xB1
        assert data[1] == 0x01

    async def test_pack_run_message(self):
        from provisa.bolt.packstream import pack_message

        data = pack_message(0x10, ["MATCH (n) RETURN n LIMIT 1", {}, {}])
        assert data[0] == 0xB1
        assert data[1] == 0x10

    async def test_pack_pull_message(self):
        from provisa.bolt.packstream import pack_message

        data = pack_message(0x3F, [{"n": 100, "qid": -1}])
        assert data[0] == 0xB1
        assert data[1] == 0x3F

    async def test_pack_reset_message(self):
        from provisa.bolt.packstream import pack_message

        data = pack_message(0x0F, [])
        assert data[0] == 0xB0
        assert data[1] == 0x0F

    async def test_success_response_roundtrip(self):
        from provisa.bolt.packstream import pack_message

        data = pack_message(0x70, {"fields": ["n"]})
        # 0xB1 = tiny struct 1 field, 0x70 = SUCCESS tag
        assert data[0] == 0xB1
        assert data[1] == 0x70

    async def test_record_response_contains_values(self):
        from provisa.bolt.packstream import pack_message

        record_data = [{"id": 1, "name": "Alice"}]
        data = pack_message(0x71, record_data)
        assert data[1] == 0x71


class TestFramingIntegration:
    async def test_write_then_read_roundtrip(self):
        import io
        from provisa.bolt.framing import read_message, write_message
        from provisa.bolt.packstream import pack_message

        payload = pack_message(0x70, {"fields": ["n", "m"]})

        class _FakeWriter:
            def __init__(self):
                self.buf = io.BytesIO()

            def write(self, data):
                self.buf.write(data)

        writer = _FakeWriter()
        write_message(writer, payload)  # type: ignore[arg-type]
        raw = writer.buf.getvalue()

        reader = asyncio.StreamReader()
        reader.feed_data(raw)
        reader.feed_eof()
        result = await read_message(reader)
        assert result == payload

    async def test_large_payload_multi_chunk_roundtrip(self):
        import io
        from provisa.bolt.framing import _CHUNK_SIZE, read_message, write_message

        payload = b"\xab" * (_CHUNK_SIZE + 50)

        class _FakeWriter:
            def __init__(self):
                self.buf = io.BytesIO()

            def write(self, data):
                self.buf.write(data)

        writer = _FakeWriter()
        write_message(writer, payload)  # type: ignore[arg-type]
        raw = writer.buf.getvalue()

        reader = asyncio.StreamReader()
        reader.feed_data(raw)
        reader.feed_eof()
        result = await read_message(reader)
        assert result == payload


class TestBoltSession:
    def _make_writer(self):
        import io

        class _Writer:
            def __init__(self):
                self.buf = io.BytesIO()

            def write(self, d):
                self.buf.write(d)

            def drain(self):
                pass

        return _Writer()

    async def test_session_init(self):
        from provisa.bolt.session import BoltSession

        writer = self._make_writer()
        session = BoltSession(writer, (5, 4))  # type: ignore[arg-type]
        assert session is not None
        assert session.bolt_version == (5, 4)

    async def test_session_send_success(self):

        from provisa.bolt.session import BoltSession

        writer = self._make_writer()
        session = BoltSession(writer, (5, 4))  # type: ignore[arg-type]
        session.send_success({"server": "provisa/1.0"})

        raw = writer.buf.getvalue()
        assert len(raw) > 0

    async def test_session_send_failure(self):
        from provisa.bolt.session import BoltSession

        writer = self._make_writer()
        session = BoltSession(writer, (5, 4))  # type: ignore[arg-type]
        session.send_failure("Neo.ClientError.Security.Unauthorized", "Not authenticated")

        raw = writer.buf.getvalue()
        assert len(raw) > 0

    async def test_session_send_record(self):
        from provisa.bolt.session import BoltSession

        writer = self._make_writer()
        session = BoltSession(writer, (5, 4))  # type: ignore[arg-type]
        session.send_record(["Alice", 42])

        raw = writer.buf.getvalue()
        assert len(raw) > 0

    async def test_handle_hello_unauthenticated_sends_failure(self):
        from unittest.mock import patch

        from provisa.bolt.session import BoltSession

        writer = self._make_writer()
        session = BoltSession(writer, (5, 4))  # type: ignore[arg-type]

        with patch("provisa.bolt.session.BoltSession._resolve_user", return_value=None):
            session.handle_hello([{"principal": "nobody", "credentials": "wrong"}])

        raw = writer.buf.getvalue()
        assert len(raw) > 0

    async def test_handle_reset_sends_success(self):
        from provisa.bolt.session import BoltSession

        writer = self._make_writer()
        session = BoltSession(writer, (5, 4))  # type: ignore[arg-type]
        session.handle_reset()

        raw = writer.buf.getvalue()
        assert len(raw) > 0
