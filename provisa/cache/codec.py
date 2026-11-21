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
dict, with ``Decimal``/``date``/``datetime``/``time``, an IR interval and PostgreSQL's infinite
date or timestamp (``ir_types.infinite_date``) preserved via msgpack ext types (``bytes`` is
msgpack's native bin type already): a hit reads back the values its miss answered (REQ-1867). This is an internal-only wire format for cache values this
process itself wrote — never exposed to untrusted deserialization.
"""

from __future__ import annotations

import datetime as _dt
import decimal
from typing import Any

import msgpack
from dateutil.relativedelta import relativedelta

from provisa.core.ir_types import (
    infinite_date,
    infinite_timestamp,
    infinity_of,
    interval_parts,
    interval_value,
)

_EXT_DECIMAL = 1
_EXT_DATETIME = 2
_EXT_DATE = 3
_EXT_TIME = 4
_EXT_INFINITE_DATE = 5  # b"1" / b"-1"
_EXT_INFINITE_TIMESTAMP = 6  # b"1" / b"-1", then b"z" when timezone-aware
_EXT_INTERVAL = 7  # b"months,days,microseconds"


def _default(obj: Any) -> msgpack.ExtType:
    sign = infinity_of(obj)
    if sign:
        if isinstance(obj, _dt.datetime):
            tag = f"{sign}{'z' if obj.tzinfo is not None else ''}"
            return msgpack.ExtType(_EXT_INFINITE_TIMESTAMP, tag.encode("ascii"))
        return msgpack.ExtType(_EXT_INFINITE_DATE, str(sign).encode("ascii"))
    if isinstance(obj, (_dt.timedelta, relativedelta)):
        parts = ",".join(str(p) for p in interval_parts(obj))
        return msgpack.ExtType(_EXT_INTERVAL, parts.encode("ascii"))
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
    if code == _EXT_INFINITE_DATE:
        return infinite_date(int(data))
    if code == _EXT_INFINITE_TIMESTAMP:
        tag = data.decode("ascii")
        return infinite_timestamp(int(tag.rstrip("z")), aware=tag.endswith("z"))
    if code == _EXT_INTERVAL:
        months, days, micros = (int(p) for p in data.decode("ascii").split(","))
        return interval_value(months, days, micros)
    return msgpack.ExtType(code, data)


def encode_cache_payload(payload: dict) -> bytes:  # REQ-1896
    """Encode a JSON-like payload dict to msgpack bytes, losslessly."""
    encoded = msgpack.packb(payload, default=_default, use_bin_type=True)
    assert encoded is not None  # packb only returns None when writing to a caller-supplied stream
    return encoded


def decode_cache_payload(data: bytes) -> dict:  # REQ-1896
    """Decode bytes produced by :func:`encode_cache_payload` back to the original payload dict."""
    return msgpack.unpackb(data, ext_hook=_ext_hook, raw=False, strict_map_key=False)
