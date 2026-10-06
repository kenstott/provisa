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
import json


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


def tag(value_digest: int) -> str:
    """A digest as a short tag -- base 36 of its unsigned value -- the part a fake that must stay
    distinct carries (REQ-1494: email, phone and identifier fakes)."""
    n = value_digest % (1 << 64)
    out = ""
    while True:
        n, r = divmod(n, 36)
        out = "0123456789abcdefghijklmnopqrstuvwxyz"[r] + out
        if n == 0:
            return out


def digest(key: bytes, text: str) -> int:
    """The keyed digest of ``text``: a signed 64-bit integer."""
    mac = hmac.new(key, text.encode("utf-8"), hashlib.sha256).digest()
    return int.from_bytes(mac[:8], "big", signed=True)


_M = (1 << 64) - 1


_INTEGER = {"tinyint", "smallint", "integer", "int", "bigint", "int2", "int4", "int8", "long"}
_TEXT = {"varchar", "char", "text", "string", "character varying", "character", "uuid"}
_FLOAT = {"real", "double", "double precision", "float", "float4", "float8"}
_DECIMAL = {"decimal", "numeric"}
_TIMESTAMP = {"timestamp", "timestamp without time zone", "datetime"}
_TIMESTAMPTZ = {"timestamptz", "timestamp with time zone"}


def canonical_type(data_type: str) -> str:
    """A column type as a fake's definition names it (REQ-1494): integer types are ``integer`` and
    text types ``text``, so a join between int and bigint keys, or varchar and text, still agrees;
    a decimal keeps its precision and scale (``decimal(p,s)``); timestamp and timestamptz stay
    apart; floating types are ``float``; any other type is its name, lowered."""
    lowered = " ".join(data_type.lower().split())
    base, _, rest = lowered.partition("(")
    base = base.strip()
    if base in _INTEGER:
        return "integer"
    if base in _TEXT:
        return "text"
    if base in _FLOAT:
        return "float"
    if base in _DECIMAL:
        if not rest:
            return "decimal"
        parts = [p.strip() for p in rest.rstrip(")").split(",")]
        return f"decimal({parts[0]},{parts[1] if len(parts) > 1 else '0'})"
    if base in ("bool", "boolean"):
        return "boolean"
    if base in _TIMESTAMP:
        return "timestamp"
    if base in _TIMESTAMPTZ:
        return "timestamptz"
    return lowered


def definition_hash(method: str, args: dict, version: int | None, data_type: str) -> int:
    """The published hash of a fake method's canonical definition -- its name, its arguments,
    for a stable fake the portable definition version it is pinned to, and the column's type as
    :func:`canonical_type` names it -- as a signed 64-bit integer: the first 8 bytes, big-endian,
    of the SHA-256 of ``{"args":{..},"method":"..","type":"..","version":N|null}`` (keys sorted, no
    spaces). REQ-1494: a fake is a function of the value's digest and this definition alone, mixed
    into each value's seed, so two columns faked from one value differ, and a changed definition
    gives other values, the same on every engine."""
    text = json.dumps(
        {"args": args, "method": method, "type": canonical_type(data_type), "version": version},
        sort_keys=True,
        separators=(",", ":"),
    )
    return int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big", signed=True)


def seed(value_digest: int, def_hash: int) -> int:
    """A method's seed for one value: splitmix64's mix of the digest XOR the definition hash, as a
    signed 64-bit integer. The Trino plugin's FakeFunctions.seed is the same arithmetic."""
    z = (((value_digest ^ def_hash) & _M) + 0x9E3779B97F4A7C15) & _M
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _M
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _M
    z ^= z >> 31
    return z - (1 << 64) if z >> 63 else z
