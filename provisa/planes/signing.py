# Copyright (c) 2026 Kenneth Stott
# Canary: 647a36ad-779e-4dfb-b38e-7592383103b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What crosses the plane socket is sealed under a key every node shares (REQ-1916).

The key is derived from the deployment master key (REQ-684,
``encryption.providers.derived_deployment_key``), which every node of a deployment already holds,
so there is no second secret to distribute. A sealed message names its kind (a plan request, a
plan, an audit report, ...), the time it was sealed and an HMAC-SHA256 over all of it: a message
of one kind is never accepted as another, a message altered or sealed under another key is refused
as forged, and one older than its kind allows is refused as stale. A message is opened -- its
payload unpickled -- only after its seal verifies, so only a holder of the key can put an object
into another node.
"""

# Requirements: REQ-1916, REQ-684

from __future__ import annotations

import hashlib
import hmac
import struct
import time
from functools import cache

_MAGIC = b"PVPL1"
_HEADER = struct.Struct(">5sB d")  # magic, kind length, sealed-at (epoch seconds)
_MAC_BYTES = 32
_PURPOSE = "provisa/plane-seal/v1"

#: How long a sealed message stays good: a plan is run the moment it arrives, and a request is
#: answered the moment it is received, so a minute only absorbs clock difference between hosts.
MAX_AGE_S = 60.0


class PlanForged(PermissionError):
    """A sealed message whose seal does not verify under the deployment key (REQ-1916)."""

    code = "plane.forged"

    def __init__(self, kind: str, why: str) -> None:
        self.kind = kind
        super().__init__(f"plane {kind} refused as forged: {why}")


class PlanStale(PermissionError):
    """A sealed message older than its kind allows (REQ-1916)."""

    code = "plane.stale"

    def __init__(self, kind: str, age_s: float) -> None:
        self.kind = kind
        self.age_s = age_s
        super().__init__(
            f"plane {kind} refused as stale: sealed {age_s:.1f}s ago, older than {MAX_AGE_S:.0f}s"
        )


@cache
def _key() -> bytes:
    from provisa.encryption.providers import derived_deployment_key

    return derived_deployment_key(_PURPOSE)


def _mac(key: bytes, signed: bytes) -> bytes:
    return hmac.new(key, signed, hashlib.sha256).digest()


def seal(kind: str, payload: bytes, *, now: float | None = None, key: bytes | None = None) -> bytes:
    """``payload`` sealed as a message of ``kind``."""
    tag = kind.encode("ascii")
    head = _HEADER.pack(_MAGIC, len(tag), time.time() if now is None else now) + tag
    return head + _mac(key or _key(), head + payload) + payload


def unseal(
    kind: str,
    sealed: bytes,
    *,
    now: float | None = None,
    key: bytes | None = None,
    max_age_s: float = MAX_AGE_S,
) -> bytes:
    """The payload of ``sealed``, a message of ``kind``; refused by name when it is forged or
    stale."""
    if len(sealed) < _HEADER.size + _MAC_BYTES:
        raise PlanForged(kind, "too short to carry a seal")
    magic, tag_len, sealed_at = _HEADER.unpack_from(sealed)
    if magic != _MAGIC:
        raise PlanForged(kind, "not a plane message")
    tag_end = _HEADER.size + tag_len
    tag = sealed[_HEADER.size : tag_end]
    mac = sealed[tag_end : tag_end + _MAC_BYTES]
    payload = sealed[tag_end + _MAC_BYTES :]
    if not hmac.compare_digest(mac, _mac(key or _key(), sealed[:tag_end] + payload)):
        raise PlanForged(kind, "its seal does not verify under the deployment key")
    if tag != kind.encode("ascii"):
        raise PlanForged(kind, f"it is a {tag.decode('ascii', 'replace')}, not a {kind}")
    age = (time.time() if now is None else now) - sealed_at
    if abs(age) > max_age_s:
        raise PlanStale(kind, age)
    return payload
