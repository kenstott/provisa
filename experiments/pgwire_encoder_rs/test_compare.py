# Copyright (c) 2026 Kenneth Stott
# Canary: ca69753f-71e8-489e-a5c6-a98d4d9e7b86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Differential test: the Rust DataRow encoder against the product's Python encode path, over every
type the port covers, NULLs and edge values, in text and binary formats. Every cell's bytes are the
same, except a float4/float8 cell (or array element), whose decoded value is the same within one part
in 10^14. A timestamp in a named zone takes its local time from zoneinfo over the tzdata package (the
release chrono-tz is built from), not from pytz (see RESULTS.md).

Lives outside tests/ so the project's suites never collect it. Run (from the worktree root):
    experiments/pgwire_encoder_rs/build.sh
    PYTHONPATH=.:vendor/buenavista:experiments/pgwire_encoder_rs/build:experiments/pgwire_encoder_rs \
        .venv/bin/python -m pytest -p no:cacheprovider experiments/pgwire_encoder_rs/test_compare.py
"""

from __future__ import annotations

import datetime as dt
import functools
import importlib.resources
import math
import random
import re
import struct
import sys
import uuid
import zoneinfo
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pyarrow as pa
import pytest
import tzdata

import pgwire_encoder_rs
from python_ref import named_zones, python_encode

UTF8_SPACES = [chr(c) for c in range(sys.maxunicode + 1) if chr(c).isspace()]

US = 1_000_000
DAY_US = 86_400 * US
I32_MAX, I64_MAX = 2**31 - 1, 2**63 - 1


def _ts(*args: int) -> int:
    """Microseconds since 1970-01-01 of a naive datetime."""
    return (dt.datetime(*args) - dt.datetime(1970, 1, 1)) // dt.timedelta(microseconds=1)


def _date(*args: int) -> int:
    return (dt.date(*args) - dt.date(1970, 1, 1)).days


TS_US = [
    0,
    1,
    -1,
    _ts(2000, 1, 1),
    _ts(1999, 12, 31, 23, 59, 59, 999999),
    _ts(1, 1, 1),
    _ts(9999, 12, 31, 23, 59, 59, 999999),
    _ts(2024, 2, 29, 12, 30, 0, 500000),
    _ts(1900, 3, 1, 0, 0, 0, 120),
    _ts(123, 4, 5, 6, 7, 8, 9),
    None,
    I64_MAX,
    -I64_MAX,
]

MDN = [
    (0, 0, 0),
    (14, 3, (4 * 3600 + 5 * 60 + 6) * 10**9 + 5 * 10**8),
    (-14, -3, -(4 * 3600) * 10**9),
    (1, -2, 3 * 10**9),
    (-1, 2, -3 * 10**9),
    (0, 1, -5000),
    (0, -1, 5000),
    (0, 0, -5000),
    (0, 1, 25 * 3600 * 10**9),
    (12, 0, 0),
    (13, 0, 0),
    (-25, 0, 0),
    (0, 0, 1500),
    (0, 0, -1500),
    (0, 500_000, (DAY_US - 1) * 1000),
    (0, 500_000, (DAY_US - 9_000) * 1000),
    (0, 999_999_999, (DAY_US - 1) * 1000),
    (0, -999_999_999, -(DAY_US - 1) * 1000),
    (0, 999_999_999, 0),
    (0, 2**31 - 1, 0),
    (5, 2**31 - 1, 7),
    None,
]

STRINGS = [
    "",
    "plain",
    "NULL",
    "null",
    "Null",
    "a b",
    "a,b",
    'q"uote',
    "back\\slash",
    "{",
    "}",
    "héllo wörld",
    "日本語",
    "emoji 🎉",
    "tab\there",
    "nl\nhere",
    "\x00nul",
    "x" * 5000,
    None,
] + UTF8_SPACES


def _f32(bits: int) -> float:
    return struct.unpack("!f", struct.pack("!I", bits))[0]


FLOATS = [
    0.0,
    -0.0,
    float("nan"),
    float("inf"),
    float("-inf"),
    1.0,
    -2.5,
    0.1,
    1 / 3,
    1e15,
    1e16,
    123456789012345.6,
    1234567890123456.7,
    1e-4,
    1e-5,
    0.00012345,
    5e-324,
    2.2250738585072014e-308,
    1.7976931348623157e308,
    -1.7976931348623157e308,
    100.0,
    1e22,
    9007199254740993.0,
    None,
]

FLOAT32S = [
    0.0,
    -0.0,
    float("nan"),
    float("inf"),
    float("-inf"),
    1.0,
    0.1,
    1 / 3,
    100000.0,
    1000000.0,
    123456.7,
    1e-4,
    1e-5,
    _f32(1),
    _f32(0x7F7FFFFF),
    _f32(0x00800000),
    16777217.0,
    None,
]

UUIDS = [
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
    "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
    "urn:uuid:a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    "a0eebc999c0b4ef8bb6d6bb9bd380a11",
    "00000000-0000-0000-0000-000000000000",
    None,
]

NS = 1000
DAY_NS = DAY_US * NS
I64_MIN = -(2**63)


def _ms_of_date(*args: int) -> int:
    return _date(*args) * 86_400_000


TS_NS = [
    0,
    1,
    -1,
    999,
    -999,
    1000,
    -1001,
    _ts(2000, 1, 1) * NS + 123,
    _ts(1999, 12, 31, 23, 59, 59, 999999) * NS + 999,
    _ts(2024, 2, 29, 12, 30, 0, 500000) * NS - 1,
    I64_MAX,
    -I64_MAX,
    None,
]

# Spellings uuid.UUID reads: hyphens anywhere, braces at the ends, urn:/uuid: anywhere, then 32
# characters int(hex, 16) takes (whitespace around, a +, 0x and one _ after it, single _ between
# digits, Unicode decimal digits and spaces).
UUID_SPELLINGS = [
    " a0eebc999c0b4ef8bb6d6bb9bd380a1",
    "a0eebc999c0b4ef8bb6d6bb9bd380a1\t",
    "\u3000" + "0eebc999c0b4ef8bb6d6bb9bd380a11",
    "\x85" + "eebc999c0b4ef8bb6d6bb9bd380a11 ",
    "a0ee_bc99_9c0b_4ef8_bb6d_6bb9bd3",
    "0xa0eebc999c0b4ef8bb6d6bb9bd380a",
    "0Xa0eebc999c0b4ef8bb6d6bb9bd380a",
    "0x_a0eebc999c0b4ef8bb6d6bb9bd380",
    "+0eebc999c0b4ef8bb6d6bb9bd380a11",
    "+0x_eebc999c0b4ef8bb6d6bb9bd380a",
    " +0x0ebc999c0b4ef8bb6d6bb9bd380 ",
    "\u0663\u0660" + "eebc999c0b4ef8bb6d6bb9bd380a11",
    "\uff11\U0001d7ce\U0001e950" + "ebc999c0b4ef8bb6d6bb9bd380a11",
    "{a0eebc99-9c0b-4ef8_bb6d-6bb9bd380a1}",
    "urn:uuid:{0x_a0eebc999c0b4ef8bb6d6bb9bd380}",
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11-",
    "00000000000000000000000000000000",
    "ffffffffffffffffffffffffffffffff",
    None,
]
# Spellings it refuses, each its own case (a raise refuses the whole batch).
UUID_REFUSED = [
    " a0eebc999c0b4ef8bb6d6bb9bd380a11",
    "a0ee__bc999c0b4ef8bb6d6bb9bd380a",
    "_a0eebc999c0b4ef8bb6d6bb9bd380a1",
    "a0eebc999c0b4ef8bb6d6bb9bd380a1_",
    "0x__a0eebc999c0b4ef8bb6d6bb9bd38",
    "+_a0eebc999c0b4ef8bb6d6bb9bd380a",
    "++a0eebc999c0b4ef8bb6d6bb9bd380a",
    "a0eebc999c0b 4ef8bb6d6bb9bd380a1",
    "\x1ca0eebc999c0b4ef8bb6d6bb9bd380a",
    "\u00b2a0eebc999c0b4ef8bb6d6bb9bd380a",
    "\u0663a0eebc999c0b4ef8bb6d6bb9bd380",
    "0x                              ",
    "                                ",
    "g0eebc999c0b4ef8bb6d6bb9bd380a11",
    "a0eebc999c0b4ef8bb6d6bb9bd380a1",
]

# (case id, Arrow array, the type name the engine reports for the column)
CASES: list[tuple[str, Any, str]] = [
    ("bool", pa.array([True, False, None]), "BOOLEAN"),
    ("int8", pa.array([-128, 0, 127, None], pa.int8()), "TINYINT"),
    ("int16", pa.array([-32768, 0, 32767, None], pa.int16()), "SMALLINT"),
    ("int32", pa.array([-(2**31), -1, 0, 2**31 - 1, None], pa.int32()), "INTEGER"),
    ("int64", pa.array([-(2**63), -1, 0, 2**63 - 1, None], pa.int64()), "BIGINT"),
    ("uint8", pa.array([0, 255, None], pa.uint8()), "UTINYINT"),
    ("uint32", pa.array([0, 2**32 - 1, None], pa.uint32()), "UINTEGER"),
    ("uint64_fits", pa.array([0, 2**63 - 1, None], pa.uint64()), "UBIGINT"),
    ("uint64_overflows", pa.array([2**64 - 1], pa.uint64()), "UBIGINT"),
    ("int64_as_integer_fits", pa.array([5, -(2**31), None], pa.int64()), "INTEGER"),
    ("int64_as_integer_overflows", pa.array([2**31], pa.int64()), "INTEGER"),
    ("int64_as_smallint_overflows", pa.array([40000], pa.int64()), "SMALLINT"),
    ("oid", pa.array([0, 2**32 - 1, None], pa.int64()), "OID"),
    ("oid_negative", pa.array([-1], pa.int64()), "OID"),
    ("float64", pa.array(FLOATS, pa.float64()), "DOUBLE"),
    ("float32_as_double", pa.array(FLOAT32S, pa.float32()), "DOUBLE"),
    ("float32", pa.array(FLOAT32S, pa.float32()), "FLOAT"),
    ("float64_as_real", pa.array([0.1, 1 / 3, 3.4028234663852886e38, 1e-46, None]), "REAL"),
    ("float64_as_real_overflows", pa.array([1e39]), "REAL"),
    ("int_as_double", pa.array([0, 2**53 + 1, -(2**63), None], pa.int64()), "DOUBLE"),
    (
        "decimal_38_10",
        pa.array(
            [
                Decimal("0"),
                Decimal("-1.5"),
                Decimal("1E-10"),
                Decimal("0.0000001"),
                Decimal("1234567890123456789012345678.9999999999"),
                Decimal("-9999999999999999999999999999.9999999999"),
                None,
            ],
            pa.decimal128(38, 10),
        ),
        "DECIMAL(38,10)",
    ),
    (
        "decimal_18_2",
        pa.array(
            [Decimal("0.00"), Decimal("10000.01"), Decimal("-0.05"), Decimal("12345678.90"), None],
            pa.decimal128(18, 2),
        ),
        "DECIMAL(18,2)",
    ),
    (
        "decimal_38_0",
        pa.array(
            [Decimal("0"), Decimal(10**38 - 1), Decimal(-(10**37)), Decimal(10000), None],
            pa.decimal128(38, 0),
        ),
        "DECIMAL(38,0)",
    ),
    (
        "decimal_6_6",
        pa.array([Decimal("0.000001"), Decimal("0.100000"), Decimal("0")], pa.decimal128(6, 6)),
        "DECIMAL(6,6)",
    ),
    (
        "decimal256",
        pa.array(
            [Decimal("1" * 56 + "." + "2" * 20), Decimal("-0.00000000000000000001"), None],
            pa.decimal256(76, 20),
        ),
        "DECIMAL(76,20)",
    ),
    (
        "decimal_negative_scale",
        pa.array([Decimal("1.23E+4"), Decimal("0E+2"), None], pa.decimal128(5, -2)),
        "DECIMAL",
    ),
    (
        # binary numeric goes through abs(), which rounds past 28 digits (half to even)
        "decimal_context_rounding",
        pa.array(
            [
                Decimal(10**38 - 1),
                Decimal("12345678901234567890123456785"),
                Decimal("12345678901234567890123456795"),
                Decimal("123456789012345678901234567850001"),
                Decimal("-99999999999999999999999999995"),
                Decimal(10**28 - 1),
            ],
            pa.decimal128(38, 0),
        ),
        "DECIMAL(38,0)",
    ),
    (
        "decimal_context_rounding_scaled",
        pa.array(
            [Decimal("9999999999999999999999999999.9999999999"), Decimal("1.5")],
            pa.decimal128(38, 10),
        ),
        "DECIMAL",
    ),
    (
        "decimal_context_rounding_fraction",
        pa.array(
            [
                Decimal("0.12345678901234567890123456785000000000"),
                Decimal("0.99999999999999999999999999999999999999"),
            ],
            pa.decimal128(38, 38),
        ),
        "DECIMAL",
    ),
    ("int_as_decimal", pa.array([0, -7, 10**18, None], pa.int64()), "DECIMAL"),
    ("text", pa.array(STRINGS, pa.string()), "VARCHAR"),
    ("large_text", pa.array(STRINGS, pa.large_string()), "VARCHAR"),
    ("dictionary_text", pa.array(["a", "b", "a", None]).dictionary_encode(), "VARCHAR"),
    ("all_null", pa.array([None, None], pa.null()), "VARCHAR"),
    ("blob", pa.array([b"", b"\x00\xff\x10", b"abc" * 100, None], pa.binary()), "BLOB"),
    ("fixed_blob", pa.array([b"\x01\x02", b"\xff\xfe"], pa.binary(2)), "BLOB"),
    (
        "date",
        pa.array(
            [
                _date(2000, 1, 1),
                _date(1, 1, 1),
                _date(9999, 12, 31),
                _date(1999, 12, 31),
                _date(1970, 1, 1),
                _date(222, 3, 4),
                None,
                I32_MAX,
                -I32_MAX,
            ],
            pa.date32(),
        ),
        "DATE",
    ),
    ("date_out_of_range", pa.array([_date(9999, 12, 31) + 1], pa.date32()), "DATE"),
    (
        "date64",
        pa.array(
            [
                0,
                -1,
                3 * 86_400_000 + 5,
                -86_400_000 - 1,
                _ms_of_date(1, 1, 1),
                _ms_of_date(9999, 12, 31) + 86_399_999,
                _ms_of_date(2000, 1, 1),
                None,
            ],
            pa.date64(),
        ),
        "DATE",
    ),
    (
        "date64_out_of_range",
        pa.array([_ms_of_date(9999, 12, 31) + 86_400_000], pa.date64()),
        "DATE",
    ),
    # the date32 infinity marker is a plain (out of range) day count in a date64
    ("date64_int32_max_days", pa.array([(2**31 - 1) * 86_400_000], pa.date64()), "DATE"),
    ("date64_before_year_1", pa.array([_ms_of_date(1, 1, 1) - 1], pa.date64()), "DATE"),
    ("timestamp", pa.array(TS_US, pa.timestamp("us")), "TIMESTAMP"),
    (
        "timestamp_ms",
        pa.array([0, -1, 1_700_000_000_123, None], pa.timestamp("ms")),
        "TIMESTAMP_MS",
    ),
    ("timestamp_s", pa.array([0, -1, 1_700_000_000, None], pa.timestamp("s")), "TIMESTAMP_S"),
    ("timestamp_ns", pa.array(TS_NS, pa.timestamp("ns")), "TIMESTAMP_NS"),
    ("timestamp_ns_nat", pa.array([I64_MIN], pa.timestamp("ns")), "TIMESTAMP_NS"),
    (
        "timestamptz_ns_utc",
        pa.array(TS_NS, pa.timestamp("ns", tz="UTC")),
        "TIMESTAMP WITH TIME ZONE",
    ),
    (
        "timestamptz_ns_plus0530",
        pa.array(TS_NS, pa.timestamp("ns", tz="+05:30")),
        "TIMESTAMP WITH TIME ZONE",
    ),
    (
        "timestamptz_ns_new_york",
        pa.array(TS_NS, pa.timestamp("ns", tz="America/New_York")),
        "TIMESTAMP WITH TIME ZONE",
    ),
    ("timestamp_ns_naive_as_tz", pa.array([0], pa.timestamp("ns")), "TIMESTAMP WITH TIME ZONE"),
    (
        "timestamptz_ms_kolkata",
        pa.array([0, -1, 1_700_000_000_123, None], pa.timestamp("ms", tz="Asia/Kolkata")),
        "TIMESTAMP WITH TIME ZONE",
    ),
    (
        "timestamptz_s_lord_howe",
        pa.array([0, -1, 1_700_000_000, None], pa.timestamp("s", tz="Australia/Lord_Howe")),
        "TIMESTAMP WITH TIME ZONE",
    ),
    (
        "timestamptz_unknown_zone",
        pa.array([0], pa.timestamp("us", tz="Mars/Olympus_Mons")),
        "TIMESTAMP WITH TIME ZONE",
    ),
    (
        "timestamptz_utc",
        pa.array(TS_US[:-4] + [None, I64_MAX, -I64_MAX], pa.timestamp("us", tz="UTC")),
        "TIMESTAMP WITH TIME ZONE",
    ),
    (
        "timestamptz_plus0530",
        pa.array(TS_US[:5] + [None], pa.timestamp("us", tz="+05:30")),
        "TIMESTAMP WITH TIME ZONE",
    ),
    (
        "timestamptz_minus0300",
        pa.array(TS_US[:5] + [None], pa.timestamp("us", tz="-03:00")),
        "TIMESTAMP WITH TIME ZONE",
    ),
    (
        "timestamptz_naive_column",
        pa.array([0, I64_MAX], pa.timestamp("us")),
        "TIMESTAMP WITH TIME ZONE",
    ),
    (
        "timestamp_aware_column",
        pa.array([0, _ts(2020, 5, 6, 7, 8, 9)], pa.timestamp("us", tz="UTC")),
        "TIMESTAMP",
    ),
    (
        "time",
        pa.array(
            [0, 1, 500_000, DAY_US - 1, 12 * 3600 * US + 34 * 60 * US + 56 * US + 789, None],
            pa.time64("us"),
        ),
        "TIME",
    ),
    ("time_ms", pa.array([0, 86_399_999, None], pa.time32("ms")), "TIME"),
    ("time_s", pa.array([0, 86_399, None], pa.time32("s")), "TIME"),
    (
        "time_ns",
        pa.array(
            [
                0,
                1,
                999,
                1000,
                1001,
                DAY_NS - 1,
                DAY_NS,
                2 * DAY_NS + 3600 * 10**9 + 7001,
                -1,
                -DAY_NS - 1,
                I64_MAX,
                I64_MIN + 1,
                None,
            ],
            pa.time64("ns"),
        ),
        "TIME",
    ),
    ("time_ns_nat", pa.array([I64_MIN], pa.time64("ns")), "TIME"),
    ("interval", pa.array(MDN, pa.month_day_nano_interval()), "INTERVAL"),
    (
        "duration",
        pa.array(
            [
                0,
                1,
                -1,
                DAY_US,
                -DAY_US - 1,
                100_000 * DAY_US + DAY_US - 1,
                I64_MAX,
                -I64_MAX - 1,
                None,
            ],
            pa.duration("us"),
        ),
        "INTERVAL",
    ),
    ("duration_overflows", pa.array([1_000_000_000 * 86_400], pa.duration("s")), "INTERVAL"),
    (
        "duration_s_large",
        pa.array(
            [999_999_999 * 86_400 + 86_399, -999_999_999 * 86_400, 500_000 * 86_400 + 86_399],
            pa.duration("s"),
        ),
        "INTERVAL",
    ),
    ("duration_s", pa.array([0, 90061, -90061, None], pa.duration("s")), "INTERVAL"),
    (
        "duration_ms",
        pa.array([0, 1, -1, 90_061_001, -90_061_001, None], pa.duration("ms")),
        "INTERVAL",
    ),
    (
        "duration_ns",
        pa.array(
            [
                0,
                1,
                -1,
                999,
                -999,
                1001,
                -1001,
                DAY_NS,
                -DAY_NS - 1,
                100_000 * DAY_NS + DAY_NS - 1,
                I64_MAX,
                -I64_MAX,
                None,
            ],
            pa.duration("ns"),
        ),
        "INTERVAL",
    ),
    ("duration_ns_nat", pa.array([I64_MIN], pa.duration("ns")), "INTERVAL"),
    ("json_text", pa.array(['{"a": 1}', "[1,2]", '"s"', "null", "", "  {}  ", None]), "JSON"),
    (
        "json_struct",
        pa.array(
            [
                {
                    "a": 1,
                    "b": 'xé"\\\n\t\x01\x7f🎉',
                    "c": [1.5, None, float("nan")],
                    "d": {"e": True, "f": None},
                },
                {"a": None, "b": "", "c": [], "d": None},
                None,
            ],
            pa.struct(
                [
                    ("a", pa.int64()),
                    ("b", pa.string()),
                    ("c", pa.list_(pa.float64())),
                    ("d", pa.struct([("e", pa.bool_()), ("f", pa.float32())])),
                ]
            ),
        ),
        "JSON",
    ),
    (
        "json_map",
        pa.array([[("k", 1), ("v", None)], [], None], pa.map_(pa.string(), pa.int64())),
        "MAP(VARCHAR, BIGINT)",
    ),
    (
        "json_list",
        pa.array([[1e16, 1e-5, 0.1, -0.0, 1e300, float("inf")], [], None], pa.list_(pa.float64())),
        "JSON",
    ),
    ("json_float32", pa.array([[0.1, 1e-7, 3.4e38]], pa.list_(pa.float32())), "JSON"),
    ("jsonb_text", pa.array(['{"a": 1}', None]), "JSONB"),
    ("jsonb_struct", pa.array([{"a": 1}, None], pa.struct([("a", pa.int64())])), "JSONB"),
    ("uuid", pa.array(UUIDS), "UUID"),
    ("uuid_spellings", pa.array(UUID_SPELLINGS), "UUID"),
    *((f"uuid_refused_{k}", pa.array([u]), "UUID") for k, u in enumerate(UUID_REFUSED)),
    ("uuid_large_string", pa.array(UUID_SPELLINGS, pa.large_string()), "UUID"),
    (
        "uuid_extension",
        pa.ExtensionArray.from_storage(
            pa.uuid(),
            pa.array(
                [uuid.UUID(u).bytes if u else None for u in UUIDS[:1] + UUIDS[4:]], pa.binary(16)
            ),
        ),
        "UUID",
    ),
    ("uuid_bad", pa.array(["not-a-uuid"]), "UUID"),
    (
        "int_array",
        pa.array([[1, None, -3], [], None, [2**31 - 1]], pa.list_(pa.int32())),
        "INTEGER[]",
    ),
    ("int_array_overflows", pa.array([[2**31]], pa.list_(pa.int64())), "INTEGER[]"),
    ("bigint_array", pa.array([[2**63 - 1, -(2**63)], [None]], pa.list_(pa.int64())), "BIGINT[]"),
    ("smallint_array", pa.array([[1, -2]], pa.list_(pa.int16())), "SMALLINT[]"),
    ("text_array", pa.array([STRINGS, [], None, [None, ""]], pa.list_(pa.string())), "VARCHAR[]"),
    ("double_array", pa.array([FLOATS, []], pa.list_(pa.float64())), "DOUBLE[]"),
    ("real_array", pa.array([FLOAT32S], pa.list_(pa.float32())), "FLOAT[]"),
    ("bool_array", pa.array([[True, False, None]], pa.list_(pa.bool_())), "BOOLEAN[]"),
    (
        "decimal_array",
        pa.array([[Decimal("1.50"), None, Decimal("-0.01")]], pa.list_(pa.decimal128(10, 2))),
        "DECIMAL(10,2)[]",
    ),
    ("large_list_array", pa.array([[1, 2]], pa.large_list(pa.int32())), "INTEGER[]"),
    (
        "int_array_2d",
        pa.array([[[1, 2], [3, None]], [[]], [], [None, [1]]], pa.list_(pa.list_(pa.int32()))),
        "INTEGER[][]",
    ),
    (
        "int_array_2d_ragged",
        pa.array([[[1, 2], [3]]], pa.list_(pa.list_(pa.int32()))),
        "INTEGER[][]",
    ),
    (
        "int_array_2d_null_row",
        pa.array([[[1], None]], pa.list_(pa.list_(pa.int32()))),
        "INTEGER[][]",
    ),
    (
        "text_array_2d",
        pa.array([[["a", "b c"], ["{", None]]], pa.list_(pa.list_(pa.string()))),
        "VARCHAR[][]",
    ),
]


@functools.cache
def _zone(name: str) -> zoneinfo.ZoneInfo:
    """A zone from the tzdata package, whatever the system's zoneinfo holds."""
    path = importlib.resources.files("tzdata.zoneinfo").joinpath(*name.split("/"))
    with path.open("rb") as f:
        return zoneinfo.ZoneInfo.from_file(f, key=name)


def _python(
    batch: pa.RecordBatch, types: list[str], formats: list[int] | None
) -> tuple[Any, list[str]]:
    with named_zones(_zone):
        try:
            return python_encode(batch.schema, [batch], types, formats)
        except Exception as e:  # noqa: BLE001 -- a raise is an outcome to compare
            # The encoder raised mid-stream; the types are known before any row is encoded.
            from python_ref import bvtype_names, query_result

            return e, bvtype_names(query_result(batch.schema, [], types, formats))


def _rust(batch: pa.RecordBatch, bvtypes: list[str], formats: list[int] | None) -> Any:
    try:
        return pgwire_encoder_rs.encode_batch(batch, bvtypes, formats)
    except Exception as e:  # noqa: BLE001
        return e


def _batch(arrays: list[Any]) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(arrays, names=[f"c{i}" for i in range(len(arrays))])


def _both(
    arrays: list[Any], types: list[str], formats: list[int] | None
) -> tuple[Any, Any, list[str]]:
    batch = _batch(arrays)
    py, bvtypes = _python(batch, types, formats)
    return py, _rust(batch, bvtypes, formats), bvtypes


def _check(
    arrays: list[Any], types: list[str], formats: list[int] | None, *, encodes: bool = False
) -> None:
    """Python and Rust agree on the batch; with ``encodes``, the product encodes it (no raise)."""
    batch = _batch(arrays)
    py, bvtypes = _python(batch, types, formats)
    assert not (encodes and isinstance(py, Exception)), f"python raised {py!r:.300}"
    rs = _rust(batch, bvtypes, formats)
    # A float4 text cell is held to the float the product sends in binary for that value: its text
    # of a double described as REAL can name another float (RESULTS.md, product defect 4).
    real_text = [
        c
        for c, t in enumerate(bvtypes)
        if FLOAT_WIDTH.get(t) == 4 and not (formats and c < len(formats) and formats[c] == 1)
    ]
    sent = None
    if real_text and not isinstance(py, Exception):
        binary = [1 if c in real_text else f for c, f in enumerate(formats or [0] * len(bvtypes))]
        py_binary, _ = _python(batch, types, binary)
        sent = {c: [row[c] for row in _data_rows(py_binary)] for c in real_text}
    _assert_same(py, rs, bvtypes, formats, sent)


# --- comparison: bytes, except a float cell by its value ---

REL_TOL = 1e-14
FLOAT_WIDTH = {"REAL": 4, "FLOAT": 8, "REALARRAY": 4, "FLOATARRAY": 8}


def _data_rows(b: bytes) -> list[list[bytes | None]]:
    rows = []
    at = 0
    while at < len(b):
        assert b[at : at + 1] == b"D", f"not a DataRow at {at}"
        (length,) = struct.unpack_from("!i", b, at + 1)
        (ncols,) = struct.unpack_from("!h", b, at + 5)
        p = at + 7
        cells: list[bytes | None] = []
        for _ in range(ncols):
            (n,) = struct.unpack_from("!i", b, p)
            p += 4
            cells.append(None if n < 0 else b[p : p + n])
            p += max(n, 0)
        assert p == at + 1 + length, f"DataRow at {at} says {length} bytes"
        rows.append(cells)
        at = p
    return rows


def _f32_step(f: float, up: bool) -> float:
    """The next float (single precision) above or below the float ``f``."""
    (bits,) = struct.unpack("!i", struct.pack("!f", f))
    if f == 0.0:
        bits = 1 if up else -(2**31) + 1
    elif (f > 0) == up:
        bits += 1
    else:
        bits -= 1
    return struct.unpack("!f", struct.pack("!i", bits))[0]


def _f32_of_text(s: str) -> float:
    """The float (single precision) a decimal text reads as, correctly rounded (strtof): the
    double's rounding to float, except where the double sits on a midpoint between two floats."""
    x = float(s)
    try:
        f = struct.unpack("!f", struct.pack("!f", x))[0]
    except OverflowError:
        return math.copysign(math.inf, x)
    if f == x or not math.isfinite(x):
        return f
    g = _f32_step(f, up=x > f)
    if x != (f + g) / 2:
        return f
    exact = Fraction(Decimal(s))
    if exact == Fraction(x):
        return f
    return max(f, g) if exact > Fraction(x) else min(f, g)


def _float_of(text: str, width: int) -> float:
    return _f32_of_text(text) if width == 4 else float(text)


def _same_float(a: float, b: float) -> bool:
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    if math.copysign(1.0, a) != math.copysign(1.0, b):
        return False
    return a == b or math.isclose(a, b, rel_tol=REL_TOL, abs_tol=0.0)


_ARRAY_TOKEN = re.compile(r"[{},]")


def _float_cell_parts(cell: bytes, width: int, binary: bool, array: bool) -> tuple[bytes, list]:
    """A float cell as (the bytes that must match exactly, the float values it holds)."""
    if not array:
        if binary:
            return b"", [struct.unpack("!f" if width == 4 else "!d", cell)[0]]
        return b"", [_float_of(cell.decode(), width)]
    if not binary:
        text = cell.decode()
        tokens = [t for t in _ARRAY_TOKEN.split(text) if t and t != "NULL"]
        # Float elements are never quoted: the braces, commas and NULLs must match.
        skeleton = re.sub(r"[^{},]+", lambda m: "NULL" if m.group() == "NULL" else "#", text)
        return skeleton.encode(), [_float_of(t, width) for t in tokens]
    ndim = struct.unpack_from("!i", cell, 0)[0]
    p = 12 + 8 * ndim
    exact, values = [cell[:p]], []
    while p < len(cell):
        (n,) = struct.unpack_from("!i", cell, p)
        exact.append(cell[p : p + 4])
        p += 4
        if n >= 0:
            values.append(struct.unpack("!f" if width == 4 else "!d", cell[p : p + n])[0])
            p += n
    return b"".join(exact), values


def _assert_same(
    py: Any,
    rs: Any,
    bvtypes: list[str],
    formats: list[int] | None = None,
    sent: dict[int, list[bytes | None]] | None = None,
) -> None:
    """Python's and Rust's DataRow bytes (or raises) are the same, a float cell's by value. ``sent``
    holds the product's binary cells of a float4 text column, the values its text cells are held
    to."""
    if isinstance(py, Exception) or isinstance(rs, Exception):
        assert isinstance(py, Exception) and isinstance(rs, Exception), (
            f"only one raised: python={py!r:.300} rust={rs!r:.300}"
        )
        return
    if py == rs and not sent:
        return
    py_rows, rs_rows = _data_rows(py), _data_rows(rs)
    assert len(py_rows) == len(rs_rows), f"python {len(py_rows)} rows, rust {len(rs_rows)}"
    for r, (a_row, b_row) in enumerate(zip(py_rows, rs_rows, strict=True)):
        assert len(a_row) == len(b_row), f"row {r}: python {len(a_row)} cells, rust {len(b_row)}"
        for c, (a, b) in enumerate(zip(a_row, b_row, strict=True)):
            where = f"row {r} column {c} ({bvtypes[c]})"
            binary = bool(formats and c < len(formats) and formats[c] == 1)
            array = bvtypes[c].endswith("ARRAY")
            if sent and c in sent:
                a, a_binary = sent[c][r], True
            else:
                a_binary = binary
            if a == b:
                continue
            width = FLOAT_WIDTH.get(bvtypes[c])
            assert width is not None and a is not None and b is not None, (
                f"{where}: python {a!r:.200} rust {b!r:.200}"
            )
            a_exact, a_vals = _float_cell_parts(a, width, a_binary, array)
            b_exact, b_vals = _float_cell_parts(b, width, binary, array)
            if a_binary != binary:
                # a text cell against the product's binary one: only the values compare
                a_exact = b_exact = b""
            assert a_exact == b_exact and len(a_vals) == len(b_vals), (
                f"{where}: python {a!r:.200} rust {b!r:.200}"
            )
            for x, y in zip(a_vals, b_vals, strict=True):
                assert _same_float(x, y), (
                    f"{where}: python {a!r:.200} ({x!r}) rust {b!r:.200} ({y!r})"
                )


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
@pytest.mark.parametrize("case", CASES, ids=[c[0] for c in CASES])
def test_column(case: tuple[str, Any, str], fmt: int) -> None:
    _, array, type_name = case
    _check([array], [type_name], [fmt])


def test_default_format_is_text() -> None:
    _check([pa.array([1, None]), pa.array(["a", None])], ["BIGINT", "VARCHAR"], None)


def _mixed(n: int, seed: int) -> tuple[list[Any], list[str]]:
    rng = random.Random(seed)
    nullable = lambda v: None if rng.random() < 0.1 else v  # noqa: E731
    arrays = [
        pa.array([rng.randint(-(2**63), 2**63 - 1) for _ in range(n)], pa.int64()),
        pa.array([nullable(rng.randint(-(2**31), 2**31 - 1)) for _ in range(n)], pa.int32()),
        pa.array([nullable(rng.uniform(-1e6, 1e6)) for _ in range(n)], pa.float64()),
        pa.array(
            [nullable(Decimal(rng.randint(-(10**15), 10**15)).scaleb(-4)) for _ in range(n)],
            pa.decimal128(20, 4),
        ),
        pa.array(
            [
                nullable("".join(rng.choice('abc é,{}"') for _ in range(rng.randint(0, 12))))
                for _ in range(n)
            ]
        ),
        pa.array(
            [nullable(rng.randint(_ts(1900, 1, 1), _ts(2100, 1, 1))) for _ in range(n)],
            pa.timestamp("us", tz="UTC"),
        ),
        pa.array(
            [nullable(rng.randint(_date(1, 1, 1), _date(9999, 12, 31))) for _ in range(n)],
            pa.date32(),
        ),
        pa.array([nullable(rng.random() < 0.5) for _ in range(n)]),
        pa.array([nullable(f'{{"k": {rng.randint(0, 99)}}}') for _ in range(n)]),
        pa.array(
            [
                nullable(
                    (rng.randint(-30, 30), rng.randint(-40, 40), rng.randint(-(10**14), 10**14))
                )
                for _ in range(n)
            ],
            pa.month_day_nano_interval(),
        ),
    ]
    types = [
        "BIGINT",
        "INTEGER",
        "DOUBLE",
        "DECIMAL(20,4)",
        "VARCHAR",
        "TIMESTAMP WITH TIME ZONE",
        "DATE",
        "BOOLEAN",
        "JSON",
        "INTERVAL",
    ]
    return arrays, types


@pytest.mark.parametrize(
    "formats", [None, [0] * 10, [1] * 10, [0, 1] * 5], ids=["none", "text", "binary", "alternating"]
)
def test_mixed_batch(formats: list[int] | None) -> None:
    arrays, types = _mixed(3000, seed=7)
    _check(arrays, types, formats)


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_random_float64_bit_patterns(fmt: int) -> None:
    rng = random.Random(64)
    values = [
        struct.unpack("!d", rng.getrandbits(64).to_bytes(8, "big"))[0] for _ in range(200_000)
    ]
    values += [rng.uniform(-1e3, 1e3) for _ in range(100_000)]
    _check([pa.array(values, pa.float64())], ["DOUBLE"], [fmt])


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_random_float32_bit_patterns(fmt: int) -> None:
    rng = random.Random(32)
    values = [_f32(rng.getrandbits(32)) for _ in range(200_000)]
    values += [
        _f32(struct.unpack("!I", struct.pack("!f", rng.uniform(-1e3, 1e3)))[0])
        for _ in range(100_000)
    ]
    _check([pa.array(values, pa.float32())], ["FLOAT"], [fmt])


def test_random_float64_as_real() -> None:
    """A double column described as float4: the text search over digits, in a double's values."""
    rng = random.Random(4)
    values = [rng.uniform(-1e30, 1e30) * 10 ** rng.randint(-40, 0) for _ in range(100_000)]
    _check([pa.array(values, pa.float64())], ["REAL"], [0])


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_random_decimals(fmt: int) -> None:
    rng = random.Random(10)
    for scale in (0, 1, 4, 9, 10, 20, 37, 38):
        values = [
            Decimal(rng.randint(-(10**38) + 1, 10**38 - 1)).scaleb(-scale) for _ in range(5000)
        ]
        values += [Decimal(rng.randint(-999, 999)).scaleb(-scale) for _ in range(5000)]
        _check([pa.array(values, pa.decimal128(38, scale))], [f"DECIMAL(38,{scale})"], [fmt])


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_random_intervals(fmt: int) -> None:
    rng = random.Random(11)
    values = []
    for _ in range(50_000):
        days = rng.choice(
            [
                0,
                rng.randint(-40, 40),
                rng.randint(-(2**31), 2**31 - 1),
                rng.randint(-999_999_999, 999_999_999),
            ]
        )
        months = rng.choice([0, 0, rng.randint(-(2**31), 2**31 - 1)])
        nanos = rng.choice(
            [
                0,
                rng.randint(-(10**15), 10**15),
                rng.randint(0, DAY_US * 1000 - 1),
                -rng.randint(0, DAY_US * 1000 - 1),
            ]
        )
        values.append((months, days, nanos))
    _check([pa.array(values, pa.month_day_nano_interval())], ["INTERVAL"], [fmt])


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_random_timestamps(fmt: int) -> None:
    rng = random.Random(12)
    lo, hi = _ts(1, 1, 1), _ts(9999, 12, 31, 23, 59, 59, 999999)
    values = [rng.randint(lo, hi) for _ in range(50_000)]
    for tz in (None, "UTC", "+09:00", "-11:30"):
        safe = [v for v in values if lo + DAY_US < v < hi - DAY_US]
        _check(
            [pa.array(safe, pa.timestamp("us", tz=tz))],
            ["TIMESTAMP WITH TIME ZONE" if tz else "TIMESTAMP"],
            [fmt],
        )


def test_many_batches_chunk_boundaries() -> None:
    """send_data_rows writes in 500-row chunks; the bytes are the concatenation either way."""
    arrays, types = _mixed(1234, seed=3)
    batch = pa.RecordBatch.from_arrays(arrays, names=[f"c{i}" for i in range(len(arrays))])
    batches = [batch.slice(0, 1), batch.slice(1, 700), batch.slice(701)]
    py, bvtypes = python_encode(batch.schema, batches, types, [1] * 10)
    rs = b"".join(pgwire_encoder_rs.encode_batch(b, bvtypes, [1] * 10) for b in batches)
    _assert_same(py, rs, bvtypes, [1] * 10)


def test_uuid_fixed_size_binary() -> None:
    """A UUID column of 16 bytes, with or without its arrow.uuid extension, is sent as the UUID it
    holds: what the product sends for the arrow.uuid column. (The product writes str(bytes) for
    a plain fixed_size_binary(16) column: RESULTS.md, product defect 5.)"""
    values = [uuid.UUID(u).bytes if u else None for u in UUIDS[:1] + UUIDS[4:]]
    storage = pa.array(values, pa.binary(16))
    extension = pa.ExtensionArray.from_storage(pa.uuid(), storage)
    for fmt in (0, 1):
        py, bvtypes = _python(_batch([extension]), ["UUID"], [fmt])
        for arr in (storage, extension):
            _assert_same(py, _rust(_batch([arr]), bvtypes, [fmt]), bvtypes, [fmt])


def test_random_json_floats() -> None:
    """json.dumps writes a float as repr; the encoder writes Ryu's shortest digits. Byte for byte."""
    rng = random.Random(5)
    values = [
        struct.unpack("!d", rng.getrandbits(64).to_bytes(8, "big"))[0] for _ in range(200_000)
    ]
    values += [rng.uniform(-1e3, 1e3) for _ in range(100_000)]
    values += [-738171652909232.25, 5e-324, 1e16, 1e-7, 123456789012345680.0]
    rows = [values[k : k + 100] for k in range(0, len(values), 100)]
    py, rs, bvtypes = _both([pa.array(rows, pa.list_(pa.float64()))], ["JSON"], [0])
    assert not isinstance(py, Exception) and py == rs, "JSON float text differs"


# --- named time zones: chrono-tz (and the zone's POSIX rule from 2100) against zoneinfo ---

ZONE_NAMES = importlib.resources.files("tzdata").joinpath("zones").read_text().split()
# Factory (the placeholder zone of tzdata's `factory` file) is not one chrono-tz builds.
ZONES = [z for z in ZONE_NAMES if z != "Factory"]
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
SEC_MIN = _ts(1, 1, 1) // US
SEC_MAX = _ts(9999, 12, 31, 23, 59, 59) // US
NS_SEC_MIN, NS_SEC_MAX = -(2**63) // 10**9 + 1, (2**63 - 1) // 10**9 - 1
# Years whose transitions come from the zone's rule rather than its tables: zoneinfo applies the
# POSIX rule after a zone's last explicit transition (2037 at most), chrono-tz holds rule transitions
# to 2099, the encoder applies the rule from 2100.
RULE_YEARS = [2037, 2038, 2050, 2099, 2100, 2101, 2400, 5000, 9998, 9999]


def test_tz_version() -> None:
    """chrono-tz's tables, the encoder's POSIX rules and the tzdata package are one IANA release."""
    assert pgwire_encoder_rs.TZDB_VERSION == tzdata.IANA_VERSION
    assert pgwire_encoder_rs.TZ_RULES_VERSION == tzdata.IANA_VERSION


def _tzif_transitions(name: str) -> list[int]:
    """The transition instants (seconds) of a zone's TZif file: its version-2 (64-bit) data."""
    data = importlib.resources.files("tzdata.zoneinfo").joinpath(*name.split("/")).read_bytes()
    isut, isstd, leap, timecnt, typecnt, charcnt = struct.unpack_from(">6l", data, 20)
    v2 = 44 + timecnt * 5 + typecnt * 6 + charcnt + leap * 8 + isstd + isut
    assert data[v2 : v2 + 4] == b"TZif", f"{name}: no version-2 data"
    timecnt = struct.unpack_from(">6l", data, v2 + 20)[3]
    return list(struct.unpack_from(f">{timecnt}q", data, v2 + 44))


def _offset(z: dt.tzinfo, secs: int) -> dt.timedelta | None:
    return (EPOCH + dt.timedelta(seconds=secs)).astimezone(z).utcoffset()


def _rule_transitions(z: dt.tzinfo) -> list[int]:
    """The instants (seconds) zoneinfo changes the zone's offset in RULE_YEARS: a day scan, then a
    bisection to the second."""
    found = []
    for year in RULE_YEARS:
        start = _ts(year, 1, 1) // US
        # two days short of the end of year 9999: zoneinfo cannot hold a local time past it
        end = _ts(year + 1, 1, 1) // US if year < 9999 else SEC_MAX - 2 * 86_400
        prev = start
        for t in range(start + 86_400, end + 86_400, 86_400):
            t = min(t, end)
            if _offset(z, t) != _offset(z, prev):
                lo, hi = prev, t
                while hi - lo > 1:
                    mid = (lo + hi) // 2
                    if _offset(z, mid) == _offset(z, lo):
                        lo = mid
                    else:
                        hi = mid
                found.append(hi)
            prev = t
    return found


@functools.cache
def _zone_instants(name: str) -> tuple[list[int | None], list[int]]:
    """Microsecond instants for a zone: either side of every transition (its tables' and its
    rule's), years 1, 2, 9998 and 9999, 200 random ones, ±infinity and NULL; and nanosecond
    instants either side of every transition in the nanosecond range."""
    z = _zone(name)
    lo, hi = SEC_MIN + 2 * 86_400, SEC_MAX - 2 * 86_400
    transitions = sorted(set(_tzif_transitions(name) + _rule_transitions(z)))
    micros: list[int | None] = []
    for t in transitions:
        if lo <= t <= hi:
            micros += [t * US - 1, t * US, t * US + 1]
    for year in (1, 2, 9998, 9999):
        for month in (1, 6, 12):
            t = _ts(year, month, 15, 12) // US
            if lo <= t <= hi:
                micros.append(t * US + 123_456)
    rng = random.Random(name)
    micros += [rng.randint(lo * US, hi * US) for _ in range(200)]
    micros += [I64_MAX, -I64_MAX, None]
    nanos = []
    for t in transitions:
        if NS_SEC_MIN <= t <= NS_SEC_MAX:
            nanos += [t * 10**9 - 1, t * 10**9, t * 10**9 + 1]
    return micros, nanos


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
@pytest.mark.parametrize("zone", ZONES)
def test_named_zone(zone: str, fmt: int) -> None:
    micros, nanos = _zone_instants(zone)
    tz_type = "TIMESTAMP WITH TIME ZONE"
    _check([pa.array(micros, pa.timestamp("us", tz=zone))], [tz_type], [fmt], encodes=True)
    if nanos:
        _check([pa.array(nanos, pa.timestamp("ns", tz=zone))], [tz_type], [fmt], encodes=True)


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_named_zone_year_limits(fmt: int) -> None:
    """The first and last instants of years 1..9999: a zone whose local time leaves those years
    is refused by both, else written."""
    for zone in ZONES:
        for v in (SEC_MIN * US, SEC_MAX * US + 999_999):
            _check(
                [pa.array([v], pa.timestamp("us", tz=zone))], ["TIMESTAMP WITH TIME ZONE"], [fmt]
            )
