# Copyright (c) 2026 Kenneth Stott
# Canary: 876c523d-670c-4b14-9df8-0e313b8170e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The keyed digest every fake is computed from (REQ-1494), and the platform key it is keyed by.

The digest of a value is the first eight bytes of HMAC-SHA256 under the platform key of the
value's text, read as a signed big-endian 64-bit integer: ``provisa_digest(fingerprint, text)``
in every engine. It is one published definition, so an engine computing it in another language
agrees. Without the key it cannot be turned back into the value.

A statement names the key by its fingerprint -- the first sixteen hex digits of the SHA-256 of the
key's hex text -- which cannot be turned back into the key; the engine holds the key itself. So
platforms sharing an engine (test stacks, control planes) each reach their own key.
"""

# Requirements: REQ-1494

from __future__ import annotations

import hashlib
import hmac


class FakeKeyMissing(RuntimeError):
    """A fake was computed before the platform key was loaded, or under a key not held."""


_key: bytes | None = None


def set_key(key: bytes) -> None:
    """Hold the platform key for this process's embedded engine."""
    global _key
    _key = key


def platform_key() -> bytes:
    if _key is None:
        raise FakeKeyMissing("the platform fake key is not loaded; fakes cannot be computed")
    return _key


def fingerprint(key: bytes) -> str:
    """The name a statement gives ``key``: not the key, and not turned back into it."""
    return hashlib.sha256(key.hex().encode("ascii")).hexdigest()[:16]


def digest(key: bytes, text: str) -> int:
    """The keyed digest of ``text``: a signed 64-bit integer."""
    mac = hmac.new(key, text.encode("utf-8"), hashlib.sha256).digest()
    return int.from_bytes(mac[:8], "big", signed=True)
