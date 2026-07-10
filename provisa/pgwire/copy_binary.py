# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2a9c14-6b30-4e85-9f01-2c8a0d4f7b53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL binary COPY wire encoding for the pgwire server (REQ-883).

DuckDB's postgres extension bulk-reads via COPY ... TO STDOUT (FORMAT binary); this
produces that stream — signature/flags header, length-prefixed per-field bodies keyed
by the column's advertised type, and the -1 trailer. Split from copy_handler to keep it
within its size budget.
"""

from __future__ import annotations

import struct
from collections.abc import Sequence
from typing import cast

_COPY_BINARY_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"  # 11-byte file signature
_COPY_BINARY_NULL = struct.pack("!i", -1)  # field length -1 = SQL NULL


def _encode_binary_field(v: object, tag: str) -> bytes:  # object-ok: opaque query-result cell
    """Encode one value in PostgreSQL binary COPY wire format (length-prefixed).

    ``v`` is validated at runtime to match ``tag`` by the column's advertised type; the
    casts reflect that contract so the numeric conversions are type-checked precisely.
    """
    if v is None:
        return _COPY_BINARY_NULL
    if tag == "bool":
        body = b"\x01" if v else b"\x00"
    elif tag == "int2":
        body = struct.pack("!h", int(cast("int", v)))
    elif tag == "int4":
        body = struct.pack("!i", int(cast("int", v)))
    elif tag == "int8":
        body = struct.pack("!q", int(cast("int", v)))
    elif tag == "float4":
        body = struct.pack("!f", float(cast("float", v)))
    elif tag == "float8":
        body = struct.pack("!d", float(cast("float", v)))
    elif tag == "bytea":
        body = bytes(cast("bytes", v))
    else:  # text/varchar/unknown → UTF-8 text, matching the advertised text OID
        body = str(v).encode("utf-8")
    return struct.pack("!i", len(body)) + body


def _rows_to_copy_binary(rows: Sequence[Sequence[object]], tags: list[str]) -> bytes:
    """Serialize rows to the PostgreSQL binary COPY stream (header, rows, trailer)."""
    n = len(tags)
    out = [_COPY_BINARY_SIGNATURE, struct.pack("!i", 0), struct.pack("!i", 0)]  # sig, flags, ext
    for row in rows:
        out.append(struct.pack("!h", n))  # field count
        for i in range(n):
            out.append(_encode_binary_field(row[i] if i < len(row) else None, tags[i]))
    out.append(struct.pack("!h", -1))  # trailer
    return b"".join(out)


_DUCKDB_BINARY_TAG = {
    # DuckDB / engine type names
    "SMALLINT": "int2",
    "TINYINT": "int2",
    "INTEGER": "int4",
    "BIGINT": "int8",
    "HUGEINT": "int8",
    "FLOAT": "float4",
    "REAL": "float4",
    "DOUBLE": "float8",
    "BOOLEAN": "bool",
    "BLOB": "bytea",
    "BYTEA": "bytea",
    # PostgreSQL result-type names (as reported by a DIRECT source's driver, REQ-883)
    "INT2": "int2",
    "INT4": "int4",
    "INT8": "int8",
    "FLOAT4": "float4",
    "FLOAT8": "float8",
    "BOOL": "bool",
}


def _duckdb_binary_tag(type_str: str | None) -> str:
    return _DUCKDB_BINARY_TAG.get((type_str or "").upper(), "text")


def _arrow_binary_tag(arrow_type) -> str:
    import pyarrow as _pa

    if _pa.types.is_boolean(arrow_type):
        return "bool"
    if _pa.types.is_int16(arrow_type) or _pa.types.is_int8(arrow_type):
        return "int2"
    if _pa.types.is_int32(arrow_type):
        return "int4"
    if _pa.types.is_int64(arrow_type):
        return "int8"
    if _pa.types.is_float32(arrow_type):
        return "float4"
    if _pa.types.is_float64(arrow_type):
        return "float8"
    if _pa.types.is_binary(arrow_type) or _pa.types.is_large_binary(arrow_type):
        return "bytea"
    return "text"
