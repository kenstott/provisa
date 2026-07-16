# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f1a2e-4c8b-4e9f-a5d2-1b6c3e8f2a4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""WebSocket transport layer for Bolt.

Neo4j Browser connects via WebSocket (ws://) rather than raw TCP.
This module detects the protocol from the first 4 bytes and wraps
the reader/writer so the Bolt handshake layer sees a uniform interface.

Protocol detection:
  - Raw Bolt:  first byte is 0x60 (Bolt magic prefix)
  - WebSocket: first 4 bytes are b"GET " (HTTP upgrade request)
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import struct
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BoltReader(Protocol):
    async def readexactly(self, n: int) -> bytes: ...
    async def read(self, n: int = -1) -> bytes: ...


@runtime_checkable
class BoltWriter(Protocol):
    def write(self, data: bytes) -> None: ...
    async def drain(self) -> None: ...
    def get_extra_info(self, key: str, default: Any = None) -> Any: ...
    def close(self) -> None: ...
    async def wait_closed(self) -> None: ...


_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# WebSocket frame fields (RFC 6455 §5.2). The 7-bit payload-length field carries either the length
# directly (<= 125), or a sentinel (126 → real length in the next 2 bytes, 127 → next 8 bytes).
_WS_OPCODE_CLOSE = 0x8
_WS_LEN_7BIT_MAX = 125
_WS_LEN_16BIT = 126
_WS_LEN_64BIT = 127
_WS_LEN_16BIT_MAX = 65535


async def detect_and_upgrade(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> tuple:
    """Return (reader_like, writer_like) ready for _bolt_handshake_and_serve."""
    import logging as _logging

    _dbg = _logging.getLogger("uvicorn.error")
    first4 = await reader.readexactly(4)
    _dbg.warning("[BOLT] first4=%r", first4)

    if first4[0:1] == b"\x60":
        return _PrefixReader(reader, first4), writer

    if first4 == b"GET ":
        return await _do_ws_upgrade(reader, writer, first4)

    raise ValueError(f"Unknown protocol, first bytes: {first4!r}")


async def _do_ws_upgrade(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    already_read: bytes,
) -> tuple:
    buf = bytearray(already_read)
    while b"\r\n\r\n" not in buf:
        chunk = await reader.read(4096)
        if not chunk:
            raise ConnectionError("EOF during WebSocket handshake")
        buf.extend(chunk)

    sep = buf.index(b"\r\n\r\n")
    header_section = buf[:sep]
    leftover = bytes(buf[sep + 4 :])

    headers: dict[str, str] = {}
    for line in header_section.decode("latin-1").split("\r\n")[1:]:
        if ": " in line:
            k, v = line.split(": ", 1)
            headers[k.lower()] = v

    key = headers.get("sec-websocket-key", "")
    # RFC 6455 mandates SHA-1 for the Sec-WebSocket-Accept handshake — a protocol digest, not a
    # security hash; usedforsecurity=False documents that (and clears the weak-hash warning).
    accept = base64.b64encode(
        hashlib.sha1((key + _WS_GUID).encode(), usedforsecurity=False).digest()
    ).decode()

    proto = headers.get("sec-websocket-protocol", "")
    proto_line = f"Sec-WebSocket-Protocol: {proto}\r\n" if proto else ""

    response = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n"
        f"{proto_line}"
        "\r\n"
    )
    writer.write(response.encode())
    await writer.drain()

    return _WsReader(reader, leftover), _WsWriter(writer)


class _PrefixReader:
    """StreamReader-like that prepends already-consumed bytes."""

    def __init__(self, reader: asyncio.StreamReader, prefix: bytes) -> None:
        self._reader = reader
        self._buf = bytearray(prefix)

    def get_extra_info(self, key: str, default=None):  # noqa: ANN001
        return None

    async def readexactly(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = await self._reader.read(n - len(self._buf))
            if not chunk:
                raise asyncio.IncompleteReadError(bytes(self._buf), n)
            self._buf.extend(chunk)
        result = bytes(self._buf[:n])
        del self._buf[:n]
        return result

    async def read(self, n: int = -1) -> bytes:
        if self._buf:
            take = len(self._buf) if n < 0 else min(n, len(self._buf))
            result = bytes(self._buf[:take])
            del self._buf[:take]
            return result
        return await self._reader.read(n)


class _WsReader:
    """StreamReader-like that unpacks WebSocket binary frames."""

    def __init__(self, reader: asyncio.StreamReader, leftover: bytes = b"") -> None:
        self._reader = reader
        self._buf = bytearray(leftover)

    async def _read_raw(self, n: int) -> bytes:
        data = bytearray()
        while len(data) < n:
            chunk = await self._reader.read(n - len(data))
            if not chunk:
                raise asyncio.IncompleteReadError(bytes(data), n)
            data.extend(chunk)
        return bytes(data)

    async def _read_ws_frame(self) -> bytes:
        header = await self._read_raw(2)
        opcode = header[0] & 0x0F
        masked = (header[1] & 0x80) != 0
        payload_len = header[1] & 0x7F

        if payload_len == _WS_LEN_16BIT:
            payload_len = struct.unpack("!H", await self._read_raw(2))[0]
        elif payload_len == _WS_LEN_64BIT:
            payload_len = struct.unpack("!Q", await self._read_raw(8))[0]

        mask_key = await self._read_raw(4) if masked else b""
        payload = bytearray(await self._read_raw(payload_len))

        if masked:
            for i in range(len(payload)):
                payload[i] ^= mask_key[i % 4]

        if opcode == _WS_OPCODE_CLOSE:
            raise asyncio.IncompleteReadError(b"", 0)
        if opcode in (0x9, 0xA):
            return b""

        return bytes(payload)

    async def _fill(self, n: int) -> None:
        while len(self._buf) < n:
            frame = await self._read_ws_frame()
            if frame:
                self._buf.extend(frame)

    async def readexactly(self, n: int) -> bytes:
        await self._fill(n)
        result = bytes(self._buf[:n])
        del self._buf[:n]
        return result

    async def read(self, n: int = -1) -> bytes:
        if not self._buf:
            frame = await self._read_ws_frame()
            self._buf.extend(frame)
        take = len(self._buf) if n < 0 else min(n, len(self._buf))
        result = bytes(self._buf[:take])
        del self._buf[:take]
        return result


class _WsWriter:
    """StreamWriter-like that buffers writes and flushes as WS binary frames on drain()."""

    def __init__(self, writer: asyncio.StreamWriter) -> None:
        self._writer = writer
        self._pending = bytearray()

    def get_extra_info(self, key: str, default=None):  # noqa: ANN001
        return self._writer.get_extra_info(key, default)

    def write(self, data: bytes) -> None:
        self._pending.extend(data)

    def _flush_pending(self) -> None:
        if not self._pending:
            return
        data = bytes(self._pending)
        self._pending.clear()
        length = len(data)
        if length <= _WS_LEN_7BIT_MAX:
            header = bytes([0x82, length])
        elif length <= _WS_LEN_16BIT_MAX:
            header = struct.pack("!BBH", 0x82, _WS_LEN_16BIT, length)
        else:
            header = struct.pack("!BBQ", 0x82, _WS_LEN_64BIT, length)
        self._writer.write(header + data)

    async def drain(self) -> None:
        self._flush_pending()
        await self._writer.drain()

    def close(self) -> None:
        self._writer.close()

    async def wait_closed(self) -> None:
        await self._writer.wait_closed()
