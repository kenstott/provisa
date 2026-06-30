# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f1a2e-4c8b-4e9f-a5d2-1b6c3e8f2a4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for Bolt PackStream codec and framing."""

import asyncio
import struct


from provisa.bolt.framing import read_message, write_message
from provisa.bolt.packstream import pack, pack_message, unpack


# ── PackStream scalar encoding ─────────────────────────────────────────────────


def test_pack_none():
    assert pack(None) == b"\xc0"


def test_pack_true():
    assert pack(True) == b"\xc3"


def test_pack_false():
    assert pack(False) == b"\xc2"


def test_pack_int_small():
    data = pack(42)
    assert unpack(data) == 42


def test_pack_int_negative():
    data = pack(-1)
    assert unpack(data) == -1


def test_pack_float():
    data = pack(3.14)
    assert abs(unpack(data) - 3.14) < 1e-10


def test_pack_string():
    data = pack("hello")
    assert unpack(data) == "hello"


def test_pack_empty_string():
    data = pack("")
    assert unpack(data) == ""


def test_pack_list():
    data = pack([1, 2, 3])
    # List header 0x93 (fixarray of 3) + three ints
    assert data[0] == 0x93


def test_pack_empty_list():
    data = pack([])
    assert data == b"\x90"


def test_pack_dict():
    data = pack({"a": 1})
    # fixmap of 1
    assert data[0] == 0xA1


def test_pack_nested():
    data = pack({"key": [1, None, "x"]})
    assert isinstance(data, bytes)
    assert len(data) > 0


# ── Node encoding ──────────────────────────────────────────────────────────────


def test_pack_node_dict_produces_struct():
    node = {"id": 7, "label": "Person", "properties": {"name": "Alice"}}
    data = pack(node)
    # Tiny struct with 4 fields: 0xB4, then tag 0x4E ('N')
    assert data[0] == 0xB4
    assert data[1] == 0x4E


def test_pack_node_dict_string_id_uses_zero():
    # String IDs (not yet resolved by register_node_ids) fall back to 0
    node = {"id": "Person|1", "label": "Person", "properties": {}}
    data = pack(node)
    assert data[0] == 0xB4
    assert data[1] == 0x4E


# ── Relationship encoding ──────────────────────────────────────────────────────


def test_pack_rel_dict_produces_struct():
    rel = {
        "identity": 5,
        "type": "KNOWS",
        "properties": {},
        "startNode": {"id": 1, "label": "Person", "properties": {}},
        "endNode": {"id": 2, "label": "Person", "properties": {}},
    }
    data = pack(rel)
    # Tiny struct with 8 fields: 0xB8, then tag 0x52 ('R')
    assert data[0] == 0xB8
    assert data[1] == 0x52


# ── pack_message ───────────────────────────────────────────────────────────────


def test_pack_message_success():
    data = pack_message(0x70, {"fields": ["n", "m"]})
    # tiny struct: 0xB1 (1 field), tag 0x70
    assert data[0] == 0xB1
    assert data[1] == 0x70


def test_pack_message_record_two_values():
    data = pack_message(0x71, [1, "x"])
    assert data[0] == 0xB1
    assert data[1] == 0x71


# ── Framing ────────────────────────────────────────────────────────────────────


def _make_chunked(data: bytes) -> bytes:
    """Build a valid chunked stream from raw message bytes."""
    return struct.pack("!H", len(data)) + data + b"\x00\x00"


async def _read_from_bytes(raw: bytes) -> bytes:
    reader = asyncio.StreamReader()
    reader.feed_data(raw)
    reader.feed_eof()
    return await read_message(reader)


def test_framing_roundtrip_small():
    payload = b"\xb1\x70\xa0"  # SUCCESS {}
    chunked = _make_chunked(payload)
    result = asyncio.run(_read_from_bytes(chunked))
    assert result == payload


def test_framing_roundtrip_empty():
    chunked = b"\x00\x00"
    result = asyncio.run(_read_from_bytes(chunked))
    assert result == b""


def test_framing_write_produces_valid_chunks():
    import io

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

    payload = b"hello world"
    writer = _FakeWriter()
    write_message(writer, payload)  # type: ignore[arg-type]
    out = writer.buf.getvalue()
    # First 2 bytes: chunk size
    size = struct.unpack("!H", out[:2])[0]
    assert out[2 : 2 + size] == payload
    # Last 2 bytes: end marker
    assert out[-2:] == b"\x00\x00"


def test_framing_multi_chunk():
    from provisa.bolt.framing import _CHUNK_SIZE
    import io

    payload = b"x" * (_CHUNK_SIZE + 100)

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

    writer = _FakeWriter()
    write_message(writer, payload)  # type: ignore[arg-type]
    out = writer.buf.getvalue()

    # Reconstruct: parse chunks
    reconstructed = b""
    offset = 0
    while offset < len(out):
        size = struct.unpack("!H", out[offset : offset + 2])[0]
        offset += 2
        if size == 0:
            break
        reconstructed += out[offset : offset + size]
        offset += size

    assert reconstructed == payload


# ── Version negotiation ────────────────────────────────────────────────────────


def test_encode_decode_version():
    from provisa.bolt.messages import decode_version_proposal, encode_version

    encoded = encode_version(5, 4)
    assert len(encoded) == 4
    major, minor = decode_version_proposal(encoded)
    assert major == 5
    assert minor == 4


def test_supported_versions_includes_5_4():
    from provisa.bolt.messages import SUPPORTED_VERSIONS

    assert (5, 4) in SUPPORTED_VERSIONS
