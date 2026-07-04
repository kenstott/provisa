# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8d2c60-7a19-4d53-9e02-1c7a0d6f8b41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-883: PostgreSQL binary COPY output (DuckDB postgres-extension bulk read)."""

from __future__ import annotations

import struct

from provisa.executor.trino import QueryResult
from provisa.pgwire.copy_binary import (
    _COPY_BINARY_SIGNATURE,
    _duckdb_binary_tag,
    _encode_binary_field,
    _rows_to_copy_binary,
)
from provisa.pgwire.copy_handler import _queryresult_to_copy_bytes

_NULL = struct.pack("!i", -1)


# ---- per-field encoding -----------------------------------------------------


def test_null_is_length_minus_one():
    assert _encode_binary_field(None, "int4") == _NULL


def test_int4_encoding():
    assert _encode_binary_field(42, "int4") == struct.pack("!i", 4) + struct.pack("!i", 42)


def test_int8_encoding():
    assert _encode_binary_field(42, "int8") == struct.pack("!i", 8) + struct.pack("!q", 42)


def test_bool_encoding():
    assert _encode_binary_field(True, "bool") == struct.pack("!i", 1) + b"\x01"
    assert _encode_binary_field(False, "bool") == struct.pack("!i", 1) + b"\x00"


def test_float8_encoding():
    assert _encode_binary_field(1.5, "float8") == struct.pack("!i", 8) + struct.pack("!d", 1.5)


def test_text_encoding_utf8():
    body = "aé".encode("utf-8")
    assert _encode_binary_field("aé", "text") == struct.pack("!i", len(body)) + body


def test_bytea_encoding_raw():
    assert _encode_binary_field(b"\x00\x01", "bytea") == struct.pack("!i", 2) + b"\x00\x01"


# ---- stream framing ---------------------------------------------------------


def test_stream_header_and_trailer():
    out = _rows_to_copy_binary([], ["int4"])
    # signature + flags(0) + header-ext-length(0) + trailer(-1)
    assert out == _COPY_BINARY_SIGNATURE + struct.pack("!i", 0) + struct.pack(
        "!i", 0
    ) + struct.pack("!h", -1)


def test_stream_one_row_two_columns():
    out = _rows_to_copy_binary([(7, "hi")], ["int4", "text"])
    expected = (
        _COPY_BINARY_SIGNATURE
        + struct.pack("!i", 0)
        + struct.pack("!i", 0)
        + struct.pack("!h", 2)  # field count
        + struct.pack("!i", 4)
        + struct.pack("!i", 7)
        + struct.pack("!i", 2)
        + b"hi"
        + struct.pack("!h", -1)  # trailer
    )
    assert out == expected


def test_null_field_in_row():
    out = _rows_to_copy_binary([(None,)], ["int4"])
    assert struct.pack("!h", 1) + _NULL in out  # one field, NULL


# ---- type-tag mapping -------------------------------------------------------


def test_duckdb_type_tags():
    assert _duckdb_binary_tag("INTEGER") == "int4"
    assert _duckdb_binary_tag("BIGINT") == "int8"
    assert _duckdb_binary_tag("DOUBLE") == "float8"
    assert _duckdb_binary_tag("BOOLEAN") == "bool"
    assert _duckdb_binary_tag("VARCHAR") == "text"
    assert _duckdb_binary_tag(None) == "text"  # unknown → text


# ---- QueryResult dispatch ---------------------------------------------------


def test_queryresult_binary_uses_column_types():
    qr = QueryResult(
        rows=[(1, "a"), (2, "b")],
        column_names=["id", "name"],
        column_types=["INTEGER", "VARCHAR"],
    )
    out = _queryresult_to_copy_bytes(qr, "binary")
    assert out.startswith(_COPY_BINARY_SIGNATURE)
    assert out.endswith(struct.pack("!h", -1))
    # id column encoded as int4 (4-byte body), not text
    assert struct.pack("!i", 4) + struct.pack("!i", 1) in out


def test_queryresult_text_unchanged():
    qr = QueryResult(rows=[(1,)], column_names=["id"], column_types=["INTEGER"])
    assert _queryresult_to_copy_bytes(qr, "text") == b"1\n"
