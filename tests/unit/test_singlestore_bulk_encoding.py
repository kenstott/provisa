# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-990: the row encoding for SingleStore's streaming LOAD DATA bulk path. The default
tab-separated, backslash-escaped text format must round-trip a row byte-for-byte: NULL is ``\\N``
and stays distinct from an empty string, and the format's special characters (backslash, tab,
newline, carriage return) are escaped. Pure-function tests; the live round-trip is a
requires_warehouse integration test."""

# Requirements: REQ-990

from __future__ import annotations

import decimal

from provisa.core.database import _singlestore_field, _singlestore_tsv_chunks


def test_null_is_backslash_N_and_distinct_from_empty_string():
    assert _singlestore_field(None, None) == "\\N"
    assert _singlestore_field("", None) == ""  # empty field = empty string, NOT null


def test_special_characters_are_escaped():
    assert _singlestore_field("a\\b", None) == "a\\\\b"  # backslash doubled
    assert _singlestore_field("a\tb", None) == "a\\tb"  # tab (the field terminator)
    assert _singlestore_field("a\nb", None) == "a\\nb"  # newline (the line terminator)
    assert _singlestore_field("a\rb", None) == "a\\rb"
    assert _singlestore_field("a\\t", None) == "a\\\\t"  # a literal backslash-t, not a tab


def test_processor_runs_before_escaping():
    # The column type's bind processor renders the value first (e.g. a JSON serialiser), then the
    # format escaping applies to its text.
    assert _singlestore_field({"a": 1}, lambda v: __import__("json").dumps(v)) == '{"a": 1}'
    assert _singlestore_field(decimal.Decimal("12.340"), str) == "12.340"  # exact, no float round


def test_tsv_chunks_lines_and_null_vs_empty_in_a_row():
    rows = [
        {"a": "x", "b": None, "c": ""},
        {"a": "t\tb", "b": "n\nl", "c": "z"},
    ]
    cols = ["a", "b", "c"]
    data = b"".join(_singlestore_tsv_chunks(rows, cols, [None, None, None]))
    assert data == b"x\t\\N\t\nt\\tb\tn\\nl\tz\n"
    # first row: a=x, b=NULL (\N), c=empty — three tab-separated fields, trailing empty is ""


def test_tsv_chunks_flush_boundary():
    rows = [{"a": "v" * 100} for _ in range(50)]
    chunks = list(_singlestore_tsv_chunks(rows, ["a"], [None], chunk_bytes=256))
    assert len(chunks) > 1  # flushed in several bounded chunks, not one buffer
    assert b"".join(chunks).count(b"\n") == 50  # every row present exactly once
