# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""COPY FROM STDIN into a session's temporary table: text, csv and binary fields, each read as
its column's type (REQ-615; pgspec TestCopy)."""

from __future__ import annotations

import datetime
import struct
from decimal import Decimal

import pytest

from provisa.pgwire.copy_handler import _parse_copy_binary, _parse_copy_data
from provisa.pgwire.temp_exec import _from_copy_binary, _from_copy_text

_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"


def _binary(rows: list[list[bytes | None]]) -> bytes:
    out = _SIGNATURE + struct.pack("!ii", 0, 0)
    for row in rows:
        out += struct.pack("!h", len(row))
        for field in row:
            out += struct.pack("!i", -1) if field is None else struct.pack("!i", len(field)) + field
    return out + struct.pack("!h", -1)


def test_a_binary_stream_reads_as_its_rows_of_fields():
    data = _binary([[struct.pack("!i", 42), b"x"], [None, b""]])
    assert _parse_copy_binary(data) == [[struct.pack("!i", 42), b"x"], [None, b""]]


def test_a_stream_without_the_signature_is_refused():
    with pytest.raises(ValueError, match="signature"):
        _parse_copy_binary(b"1\tAlice\n")


def test_text_stops_at_the_end_marker_and_reads_nulls():
    assert _parse_copy_data(b"1\tAlice\n2\t\\N\n\\.\n", "text") == [["1", "Alice"], ["2", None]]


@pytest.mark.parametrize(
    ("landed", "text", "value"),
    [
        ("bigint", "42", 42),
        ("double", "1.5", 1.5),
        ("boolean", "t", True),
        ("boolean", "false", False),
        ("date", "2024-01-02", datetime.date(2024, 1, 2)),
        ("numeric(10,2)", "12.34", Decimal("12.34")),
        ("bytea", "\\xdeadbeef", b"\xde\xad\xbe\xef"),
        ("text", "Alice", "Alice"),
        ("bigint", None, None),
    ],
)
def test_a_text_field_reads_as_its_columns_type(landed, text, value):
    assert _from_copy_text("c", landed, text) == value


def test_a_text_field_its_type_does_not_read_is_refused_naming_the_column():
    with pytest.raises(ValueError, match="column 'id' is bigint"):
        _from_copy_text("id", "bigint", "PGCOPY")


@pytest.mark.parametrize(
    ("landed", "raw", "value"),
    [
        ("bigint", struct.pack("!i", 7), 7),
        ("bigint", struct.pack("!q", -9), -9),
        ("double", struct.pack("!d", 2.5), 2.5),
        ("boolean", b"\x01", True),
        ("text", "é".encode(), "é"),
        ("date", struct.pack("!i", 1), datetime.date(2000, 1, 2)),
        ("bigint", None, None),
    ],
)
def test_a_binary_field_reads_as_its_columns_type(landed, raw, value):
    assert _from_copy_binary("c", landed, raw) == value


def test_copy_out_sends_one_copydata_per_row_in_text_and_csv():
    from provisa.pgwire.copy_handler import _copy_out_messages

    rows = [[1, "Alice"], [2, "line\nbreak"]]
    assert _copy_out_messages(rows, 2, "text", []) == [b"1\tAlice\n", b"2\tline\\nbreak\n"]
    assert _copy_out_messages(rows, 2, "csv", []) == [b"1,Alice\n", b'2,"line\nbreak"\n']
    assert len(_copy_out_messages(rows, 2, "binary", ["int4", "text"])) == 1
