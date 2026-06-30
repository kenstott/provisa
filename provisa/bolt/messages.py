# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f1a2e-4c8b-4e9f-a5d2-1b6c3e8f2a4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bolt message signature constants and decoded message dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ── Request signatures (client → server) ──────────────────────────────────────

HELLO = 0x01
GOODBYE = 0x02
LOGON = 0x6A
LOGOFF = 0x6B
RESET = 0x0F
BEGIN = 0x11
COMMIT = 0x12
ROLLBACK = 0x13
RUN = 0x10
PULL = 0x3F
DISCARD = 0x2F
ROUTE = 0x66
TELEMETRY = 0x54

# ── Response signatures (server → client) ─────────────────────────────────────

SUCCESS = 0x70
FAILURE = 0x7F
IGNORED = 0x7E
RECORD = 0x71

# ── Bolt handshake ─────────────────────────────────────────────────────────────

MAGIC = b"\x60\x60\xb0\x17"

# Version proposals we advertise (big-endian 4-byte each):
# 0x00000504 = Bolt 5.4, 0x00000404 = Bolt 4.4, zeros = not supported
SUPPORTED_VERSIONS = [
    (5, 4),
    (5, 3),
    (5, 1),
    (4, 4),
]


def encode_version(major: int, minor: int) -> bytes:
    # Bolt wire format: [0x00, 0x00, minor, major] (big-endian: major is LSB)
    import struct

    return struct.pack("!BBBB", 0, 0, minor, major)


def decode_version_proposal(b: bytes) -> tuple[int, int]:
    """Decode a 4-byte version proposal → (major, minor)."""
    minor, major = b[2], b[3]
    return (major, minor)


# ── Decoded message types ──────────────────────────────────────────────────────


@dataclass
class BoltMessage:
    tag: int
    fields: list[Any] = field(default_factory=list)

    @property
    def first(self) -> Any:
        return self.fields[0] if self.fields else {}

    @property
    def meta(self) -> dict:
        return self.fields[-1] if self.fields and isinstance(self.fields[-1], dict) else {}
