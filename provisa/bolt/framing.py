# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f1a2e-4c8b-4e9f-a5d2-1b6c3e8f2a4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bolt message framing (chunking).

Every Bolt message on the wire is split into chunks:
  [2-byte big-endian chunk size][chunk data] ... [0x00 0x00 end marker]

read_message() reassembles chunks into a single message bytes object.
write_message() splits and writes a message with the end marker.
"""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.bolt.websocket import BoltReader, BoltWriter

_CHUNK_SIZE = 16384


async def read_message(reader: BoltReader) -> bytes:
    """Read one complete Bolt message from the stream."""
    parts: list[bytes] = []
    while True:
        size_bytes = await reader.readexactly(2)
        size = struct.unpack("!H", size_bytes)[0]
        if size == 0:
            break
        chunk = await reader.readexactly(size)
        parts.append(chunk)
    return b"".join(parts)


def write_message(writer: BoltWriter, data: bytes) -> None:
    """Write one complete Bolt message as chunks followed by end marker."""
    offset = 0
    while offset < len(data):
        chunk = data[offset : offset + _CHUNK_SIZE]
        writer.write(struct.pack("!H", len(chunk)))
        writer.write(chunk)
        offset += len(chunk)
    writer.write(b"\x00\x00")
