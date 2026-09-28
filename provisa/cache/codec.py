# Copyright (c) 2026 Kenneth Stott
# Canary: 28daafb0-328d-4d71-a425-672e1376ff33
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Typed binary encoding for response-cache values (REQ-1896).

Replaces ``json.dumps(..., default=str)``, which is lossy: a ``Decimal`` becomes a string
(precision recoverable only by re-parsing, if the reader remembers to), ``bytes`` becomes its
``repr()`` (unrecoverable), and integer width/type identity is lost entirely. Every query
transport now shares ONE response cache (REQ-1897), so a value written by one transport must be
readable, byte-for-byte, by any other — round-tripping through JSON silently degrades that
contract for exactly the value types query results are full of.

``encode_cache_payload``/``decode_cache_payload`` msgpack-encode an arbitrary JSON-like payload
dict, with ``Decimal``/``date``/``datetime``/``time`` preserved via msgpack ext types (``bytes``
is msgpack's native bin type already). This is an internal-only wire format for cache values this
process itself wrote — never exposed to untrusted deserialization.
"""

from __future__ import annotations

import datetime as _dt
import decimal
from typing import Any

import msgpack

_EXT_DECIMAL = 1
_EXT_DATETIME = 2
_EXT_DATE = 3
_EXT_TIME = 4


def _default(obj: Any) -> msgpack.ExtType:
    if isinstance(obj, decimal.Decimal):
        return msgpack.ExtType(_EXT_DECIMAL, str(obj).encode("utf-8"))
    # datetime is a subclass of date -- check it first.
    if isinstance(obj, _dt.datetime):
        return msgpack.ExtType(_EXT_DATETIME, obj.isoformat().encode("utf-8"))
    if isinstance(obj, _dt.date):
        return msgpack.ExtType(_EXT_DATE, obj.isoformat().encode("utf-8"))
    if isinstance(obj, _dt.time):
        return msgpack.ExtType(_EXT_TIME, obj.isoformat().encode("utf-8"))
    raise TypeError(f"cache codec: unsupported type {type(obj)!r} for value {obj!r}")


def _ext_hook(code: int, data: bytes) -> Any:
    if code == _EXT_DECIMAL:
        return decimal.Decimal(data.decode("utf-8"))
    if code == _EXT_DATETIME:
        return _dt.datetime.fromisoformat(data.decode("utf-8"))
    if code == _EXT_DATE:
        return _dt.date.fromisoformat(data.decode("utf-8"))
    if code == _EXT_TIME:
        return _dt.time.fromisoformat(data.decode("utf-8"))
    return msgpack.ExtType(code, data)


def encode_cache_payload(payload: dict) -> bytes:  # REQ-1896
    """Encode a JSON-like payload dict to msgpack bytes, losslessly."""
    encoded = msgpack.packb(payload, default=_default, use_bin_type=True)
    assert encoded is not None  # packb only returns None when writing to a caller-supplied stream
    return encoded


def decode_cache_payload(data: bytes) -> dict:  # REQ-1896
    """Decode bytes produced by :func:`encode_cache_payload` back to the original payload dict."""
    return msgpack.unpackb(data, ext_hook=_ext_hook, raw=False, strict_map_key=False)
