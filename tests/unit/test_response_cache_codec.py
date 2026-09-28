# Copyright (c) 2026 Kenneth Stott
# Canary: 0d5e1d13-711c-4fa7-bc9e-e1caa4a9bae3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the typed binary response-cache codec (REQ-1896).

Covers the lossy-JSON gap the maintainer flagged in GraphQL's own ``store_result``:
``Decimal`` -> string, ``bytes`` -> repr(), integer width lost. Every type below must round-trip
byte-for-byte through :func:`encode_cache_payload`/:func:`decode_cache_payload`.
"""

from __future__ import annotations

import datetime as dt
import decimal

import pytest

from provisa.cache.codec import decode_cache_payload, encode_cache_payload


def test_decimal_round_trips_exactly():
    payload = {"rows": [[decimal.Decimal("19.99")]]}
    out = decode_cache_payload(encode_cache_payload(payload))
    value = out["rows"][0][0]
    assert value == decimal.Decimal("19.99")
    assert isinstance(value, decimal.Decimal)


def test_decimal_precision_not_collapsed_by_float_rounding():
    # A value JSON's default=str would have preserved as a *string* (fine) but a naive
    # float-based codec would mangle. Assert real Decimal precision survives.
    payload = {"rows": [[decimal.Decimal("0.1"), decimal.Decimal("100000000000000000.123456789")]]}
    out = decode_cache_payload(encode_cache_payload(payload))
    assert out["rows"][0][0] == decimal.Decimal("0.1")
    assert out["rows"][0][1] == decimal.Decimal("100000000000000000.123456789")


def test_bytes_round_trip_not_repr_string():
    payload = {"rows": [[b"\x00\x01\xff binary"]]}
    out = decode_cache_payload(encode_cache_payload(payload))
    value = out["rows"][0][0]
    assert value == b"\x00\x01\xff binary"
    assert isinstance(value, bytes)


def test_datetime_round_trips():
    ts = dt.datetime(2026, 9, 28, 13, 45, 0, 123456)
    payload = {"rows": [[ts]]}
    out = decode_cache_payload(encode_cache_payload(payload))
    assert out["rows"][0][0] == ts


def test_date_round_trips():
    d = dt.date(2026, 9, 28)
    payload = {"rows": [[d]]}
    out = decode_cache_payload(encode_cache_payload(payload))
    assert out["rows"][0][0] == d


def test_date_and_datetime_do_not_collide():
    # datetime is a subclass of date -- confirm the encoder distinguishes them rather than
    # collapsing datetime down to date on decode.
    payload = {"rows": [[dt.date(2026, 1, 1), dt.datetime(2026, 1, 1, 0, 0, 0)]]}
    out = decode_cache_payload(encode_cache_payload(payload))
    assert type(out["rows"][0][0]) is dt.date
    assert type(out["rows"][0][1]) is dt.datetime


def test_time_round_trips():
    t = dt.time(23, 59, 59)
    payload = {"rows": [[t]]}
    out = decode_cache_payload(encode_cache_payload(payload))
    assert out["rows"][0][0] == t


def test_int_width_preserved_as_int_not_string():
    payload = {"rows": [[2**53 + 1, -1]]}
    out = decode_cache_payload(encode_cache_payload(payload))
    assert out["rows"][0][0] == 2**53 + 1
    assert isinstance(out["rows"][0][0], int)


def test_none_round_trips():
    payload = {"rows": [[None, "x"]]}
    out = decode_cache_payload(encode_cache_payload(payload))
    assert out["rows"][0][0] is None


def test_column_types_travel_alongside_rows():
    payload = {
        "rows": [[decimal.Decimal("1"), b"x"]],
        "column_names": ["price", "blob"],
        "column_types": ["numeric", "bytea"],
    }
    out = decode_cache_payload(encode_cache_payload(payload))
    assert out["column_names"] == ["price", "blob"]
    assert out["column_types"] == ["numeric", "bytea"]


def test_unsupported_type_raises_instead_of_silently_stringifying():
    class Unsupported:
        pass

    with pytest.raises(TypeError):
        encode_cache_payload({"rows": [[Unsupported()]]})
