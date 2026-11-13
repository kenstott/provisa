# Copyright (c) 2026 Kenneth Stott
# Canary: ca69753f-71e8-489e-a5c6-a98d4d9e7b86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Differential test: what pgwire sends -- the product's send path through the Rust encoder,
straight from the engine's Arrow batches (REQ-2194) -- against PostgreSQL 17's answer, over every
type pgwire serves, NULLs and edge values, in text and binary formats. The reference is the
product's former Python writers (oracle.py) with each of their defects corrected to PostgreSQL's
answer (pg_reference.pg17, held to a live server by rust/pgwire/scripts/pg_types_check.py). Every
cell's bytes are the same, except a float4/float8 cell (or array element), whose decoded value is
the same within one part in 10^14. A timestamp in a named zone takes its local time from zoneinfo
over the tzdata package (the release chrono-tz is built from), not from pytz (see
rust/pgwire/RESULTS.md).

Run (from the worktree root):
    rust/build-local.sh
    PYTHONPATH=.:vendor/buenavista \
        .venv/bin/python -m pytest -p no:cacheprovider tests/unit/pgwire_encoder/test_differential.py
"""

from __future__ import annotations

import datetime as dt
import functools
import importlib.resources
import ipaddress
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
import pyarrow.compute  # noqa: F401 -- pa.compute
import pytest
import tzdata

from provisa._encoders import pgwire as pgwire_encoder
from buenavista.core import BVType
from buenavista.postgres import BuenaVistaHandler

import provisa.pgwire.cast_types as cast_types
import provisa.pgwire.server as server
from tests.unit.pgwire_encoder.pg_reference import DEFECTS, pg17
from provisa.federation.duckdb_pg_xml import xml_out
from provisa.pgwire.cast_types import XML_ARRAY
from provisa.transpiler.pg_forms_json import jsonb_out
from tests.unit.pgwire_encoder.python_ref import (
    bvtype_names,
    coercer_names,
    has_row_form,
    named_zones,
    python_encode,
    query_result,
    rows_wired_encode,
    wired_encode,
)

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
    batch: pa.RecordBatch, types: list[str], formats: list[int] | None, **described: Any
) -> tuple[Any, list[str], list[list[str]]]:
    """The reference's DataRow bytes (or its raise), the BVTypes and the coercers: the product's
    path, with PostgreSQL's answer where the product's is not it (pg_reference.pg17)."""
    with named_zones(_zone), pg17():
        try:
            return python_encode(batch.schema, [batch], types, formats, **described)
        except Exception as e:  # noqa: BLE001 -- a raise is an outcome to compare
            # The encoder raised mid-stream; the types are known before any row is encoded.
            qr = query_result(batch.schema, [], types, formats, **described)
            return e, bvtype_names(qr), coercer_names(qr)


def _former(
    batch: pa.RecordBatch, types: list[str], formats: list[int] | None, **described: Any
) -> Any:
    """What the product's former Python writers sent (or their raise), with no correction."""
    with named_zones(_zone):
        try:
            return python_encode(batch.schema, [batch], types, formats, **described)[0]
        except Exception as e:  # noqa: BLE001
            return e


def _rust(
    batch: pa.RecordBatch,
    bvtypes: list[str],
    formats: list[int] | None,
    coercers: list[list[str]] | None = None,
) -> Any:
    """The encoder module's own bytes (or its raise) for wire types and coercers given outright."""
    try:
        return pgwire_encoder.encode_batch(batch, bvtypes, formats, coercers)
    except Exception as e:  # noqa: BLE001
        return e


def _wired(
    batch: pa.RecordBatch,
    types: list[str],
    formats: list[int] | None,
    bvtypes: list[str] | None = None,
    **described: Any,
) -> Any:
    """What pgwire sends (or its raise): the product's send path through the Rust encoder. With
    ``bvtypes``, the wire types it chose are checked to be the reference's."""
    try:
        out, chosen, _ = wired_encode(batch.schema, [batch], types, formats, **described)
    except Exception as e:  # noqa: BLE001
        _same_from_rows(batch, types, formats, e, **described)
        return e
    assert bvtypes is None or chosen == bvtypes, (chosen, bvtypes)
    _same_from_rows(batch, types, formats, out, **described)
    return out


def _same_from_rows(
    batch: pa.RecordBatch, types: list[str], formats: list[int] | None, sent: Any, **described: Any
) -> None:
    """REQ-2195: the same result as a row result is encoded from its rows to the same bytes, or
    refused as the Arrow form is."""
    if not has_row_form(batch.schema):
        return
    try:
        from_rows = rows_wired_encode(batch.schema, [batch], types, formats, **described)
    except Exception as e:  # noqa: BLE001
        assert isinstance(sent, Exception), f"the rows form raised {e!r:.300}; the Arrow form sent"
        return
    assert not isinstance(sent, Exception), f"the Arrow form raised {sent!r:.300}; rows sent"
    assert from_rows == sent, "the rows form answers otherwise"


def _batch(arrays: list[Any]) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(arrays, names=[f"c{i}" for i in range(len(arrays))])


def _both(
    arrays: list[Any], types: list[str], formats: list[int] | None, **described: Any
) -> tuple[Any, Any, list[str]]:
    batch = _batch(arrays)
    py, bvtypes, _ = _python(batch, types, formats, **described)
    return py, _wired(batch, types, formats, bvtypes, **described), bvtypes


def _check(
    arrays: list[Any],
    types: list[str],
    formats: list[int] | None,
    *,
    encodes: bool = False,
    **described: Any,
) -> None:
    """The reference (PostgreSQL 17's answer) and the wired path agree on the batch; with
    ``encodes``, the reference encodes it (no raise). ``described``: the statement's sql, shape or
    read_shape (ProvisaQueryResult's)."""
    batch = _batch(arrays)
    py, bvtypes, _ = _python(batch, types, formats, **described)
    assert not (encodes and isinstance(py, Exception)), f"python raised {py!r:.300}"
    rs = _wired(batch, types, formats, bvtypes, **described)
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
        py_binary, _, _ = _python(batch, types, binary, **described)
        if not isinstance(py_binary, Exception):  # a value no binary float4 holds is not one
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
    with pg17():
        py, bvtypes, _ = python_encode(batch.schema, batches, types, [1] * 10)
    rs, wired_types, _ = wired_encode(batch.schema, batches, types, [1] * 10)
    assert wired_types == bvtypes
    _assert_same(py, rs, bvtypes, [1] * 10)


def test_uuid_fixed_size_binary() -> None:
    """A UUID column of 16 bytes, with or without its arrow.uuid extension, is sent as the UUID it
    holds: what the product sends for the arrow.uuid column. (The product writes str(bytes) for
    a plain fixed_size_binary(16) column: RESULTS.md, product defect 5.)"""
    values = [uuid.UUID(u).bytes if u else None for u in UUIDS[:1] + UUIDS[4:]]
    storage = pa.array(values, pa.binary(16))
    extension = pa.ExtensionArray.from_storage(pa.uuid(), storage)
    for fmt in (0, 1):
        py, bvtypes, _ = _python(_batch([extension]), ["UUID"], [fmt])
        for arr in (storage, extension):
            _assert_same(py, _wired(_batch([arr]), ["UUID"], [fmt], bvtypes), bvtypes, [fmt])


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
# Factory (the placeholder zone of tzdata's `factory` file) included: the tables build that file too.
ZONES = ZONE_NAMES
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
    assert pgwire_encoder.TZDB_VERSION == tzdata.IANA_VERSION
    assert pgwire_encoder.TZ_RULES_VERSION == tzdata.IANA_VERSION


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


# float8out / float4out as PostgreSQL 17 writes them (pg_float_check.py holds the encoder to a live
# server over 300,000 random values of each): NaN, ±Infinity, -0, fixed form for a decimal exponent
# in [-4, 15) or [-4, 6), else e±dd, and a decimal on the rounding interval's boundary never taken.
PG_FLOAT8 = [
    (0.0, "0"),
    (-0.0, "-0"),
    (float("nan"), "NaN"),
    (float("inf"), "Infinity"),
    (float("-inf"), "-Infinity"),
    (1.0, "1"),
    (0.1, "0.1"),
    (100.0, "100"),
    (1e14, "100000000000000"),
    (1e15, "1e+15"),
    (123456789012345.6, "123456789012345.6"),
    (1234567890123456.7, "1.2345678901234568e+15"),
    (1e-4, "0.0001"),
    (1e-5, "1e-05"),
    (0.00012345, "0.00012345"),
    (1 / 3, "0.3333333333333333"),
    (5e-324, "5e-324"),
    (2.2250738585072014e-308, "2.2250738585072014e-308"),
    (1.7976931348623157e308, "1.7976931348623157e+308"),
    (1e100, "1e+100"),
    (-738171652909232.25, "-738171652909232.2"),
    (1.218865206211834e17, "1.2188652062118339e+17"),
    (-4.079636726800334e16, "-4.0796367268003344e+16"),
]
PG_FLOAT4 = [
    (0.0, "0"),
    (-0.0, "-0"),
    (1.0, "1"),
    (0.1, "0.1"),
    (1 / 3, "0.33333334"),
    (100000.0, "100000"),
    (1e6, "1e+06"),
    (123456.7, "123456.7"),
    (1e-4, "0.0001"),
    (1e-5, "1e-05"),
    (_f32(1), "1e-45"),
    (_f32(0x00800000), "1.1754944e-38"),
    (_f32(0x7F7FFFFF), "3.4028235e+38"),
    (16777217.0, "1.6777216e+07"),
]


@pytest.mark.parametrize(
    ("bvtype", "arrow_type", "cases"),
    [("FLOAT", pa.float64(), PG_FLOAT8), ("REAL", pa.float32(), PG_FLOAT4)],
    ids=["float8", "float4"],
)
def test_postgres_float_text(bvtype: str, arrow_type: pa.DataType, cases: list) -> None:
    batch = _batch([pa.array([v for v, _ in cases], arrow_type)])
    rows = _data_rows(pgwire_encoder.encode_batch(batch, [bvtype], [0]))
    assert [r[0] for r in rows] == [text.encode() for _, text in cases]


# --- every BVType, from every Arrow type a column may reach it as ---


#: Every wire type send_data_rows knows, and BVType.UNKNOWN (any type it does not list).
ALL_TYPES: list[Any] = [*BVType, XML_ARRAY]


def _type_name(t: Any) -> str:
    return getattr(t, "name", t)


def _forced(arrays: list[Any], types: list[Any], formats: list[int], **described: Any) -> None:
    """_check with each column's BVType set outright."""
    _check(arrays, ["VARCHAR"] * len(arrays), formats, force_types=types, **described)


_DUCKDB_INET = pa.struct(
    [("ip_type", pa.uint8()), ("address", pa.decimal128(38, 0)), ("mask", pa.uint16())]
)


def _duckdb_inet(ips: list[str | None]) -> pa.StructArray:
    """DuckDB's INET column: each value's type, its address (an IPv6 one with its top bit flipped,
    a HUGEINT, which can pass decimal128(38, 0)'s 38 digits) and its mask, as DuckDB's Arrow
    carries it. Built from its buffers: pyarrow refuses a 39-digit value from Python."""
    kinds, addresses, masks = [], [], []
    for ip in ips:
        iface = ipaddress.ip_interface(ip or "0.0.0.0")
        flipped = int(iface.ip) if iface.version == 4 else int(iface.ip) - (1 << 127)
        kinds.append(1 if iface.version == 4 else 2)
        addresses.append(flipped.to_bytes(16, "little", signed=True))
        masks.append(iface.network.prefixlen)
    address = pa.Array.from_buffers(
        pa.decimal128(38, 0), len(ips), [None, pa.py_buffer(b"".join(addresses))]
    )
    valid = pa.array([ip is not None for ip in ips])
    return pa.StructArray.from_arrays(
        [pa.array(kinds, pa.uint8()), address, pa.array(masks, pa.uint16())],
        fields=list(_DUCKDB_INET),
        mask=pa.compute.invert(valid),
    )


#: A column of each Arrow type a result may hold, NULLs among its values.
POOL: list[tuple[str, Any]] = [
    ("null", pa.array([None, None], pa.null())),
    ("bool", pa.array([True, False, None])),
    ("int8", pa.array([0, -1, 127, None], pa.int8())),
    ("int64", pa.array([0, 1, -1, 2**40, 26, 23, None], pa.int64())),
    ("uint64", pa.array([2**64 - 1, None], pa.uint64())),
    ("float64", pa.array([0.0, -0.0, 0.1, 1e16, 2.5, -1e-7, None])),
    ("float64_special", pa.array([float("nan"), float("inf"), float("-inf")])),
    ("float32", pa.array([0.1, 3.0, None], pa.float32())),
    (
        "decimal_2",
        pa.array([Decimal("1.50"), Decimal("-0.01"), Decimal("0.00"), None], pa.decimal128(10, 2)),
    ),
    (
        "decimal_10",
        pa.array(
            [Decimal("1E-10"), Decimal("12345678901234567890.1234567891")], pa.decimal128(38, 10)
        ),
    ),
    ("decimal256", pa.array([Decimal("-1.5"), None], pa.decimal256(40, 1))),
    ("binary", pa.array([b"", b"a'\x00\xff\"", None], pa.binary())),
    ("fsb16", pa.array([uuid.UUID(UUIDS[0]).bytes, None], pa.binary(16))),
    (
        "uuid_ext",
        pa.ExtensionArray.from_storage(
            pa.uuid(), pa.array([uuid.UUID(UUIDS[0]).bytes], pa.binary(16))
        ),
    ),
    ("date32", pa.array([_date(2024, 2, 29), _date(1, 1, 1), None], pa.date32())),
    ("date32_infinity", pa.array([I32_MAX, -I32_MAX], pa.date32())),
    ("date64", pa.array([_ms_of_date(1999, 12, 31) + 5], pa.date64())),
    ("ts_us", pa.array([_ts(2000, 1, 1, 12, 30, 0, 500), _ts(1, 1, 1), None], pa.timestamp("us"))),
    ("ts_us_infinity", pa.array([I64_MAX, -I64_MAX], pa.timestamp("us"))),
    ("ts_us_utc", pa.array([_ts(2024, 7, 1, 1, 2, 3), None], pa.timestamp("us", "UTC"))),
    ("ts_us_utc_infinity", pa.array([I64_MAX], pa.timestamp("us", "UTC"))),
    ("ts_s_offset", pa.array([_ts(2024, 7, 1) // US], pa.timestamp("s", "+05:30"))),
    ("ts_us_named", pa.array([_ts(2024, 7, 1, 12)], pa.timestamp("us", "America/New_York"))),
    ("ts_ns", pa.array([1, 1500, None], pa.timestamp("ns"))),
    ("ts_ns_utc", pa.array([1], pa.timestamp("ns", "UTC"))),
    ("time_us", pa.array([0, 3_600_000_001, None], pa.time64("us"))),
    ("time_s", pa.array([45296], pa.time32("s"))),
    ("time_ns", pa.array([5_000], pa.time64("ns"))),
    ("duration_us", pa.array([1, -1, DAY_US + 1, None], pa.duration("us"))),
    ("duration_ns", pa.array([1, -1], pa.duration("ns"))),
    (
        "mdn",
        pa.array(
            [(1, 2, 3000), (0, 5, 0), (0, 1, 25 * 3600 * 10**9)], pa.month_day_nano_interval()
        ),
    ),
    ("list_int", pa.array([[1, None, 3], [], None], pa.list_(pa.int32()))),
    ("list_str", pa.array([["a b", "", None, "x'y", 'q"'], []], pa.list_(pa.string()))),
    (
        "list_list",
        pa.array([[[1, 2], [3, 4]], [[], []], [[1], None]], pa.list_(pa.list_(pa.int32()))),
    ),
    ("list_list_str", pa.array([[[], []], [None, ["a"]]], pa.list_(pa.list_(pa.string())))),
    ("list_float", pa.array([[0.5, float("nan"), None]], pa.list_(pa.float64()))),
    ("list_decimal", pa.array([[Decimal("1.5"), None]], pa.list_(pa.decimal128(5, 1)))),
    ("list_date", pa.array([[_date(2020, 1, 2)]], pa.list_(pa.date32()))),
    ("list_ts", pa.array([[_ts(2020, 1, 2, 3, 4, 5, 6)]], pa.list_(pa.timestamp("us")))),
    ("list_ts_utc", pa.array([[_ts(2020, 1, 2)]], pa.list_(pa.timestamp("us", "UTC")))),
    ("list_time", pa.array([[1]], pa.list_(pa.time64("us")))),
    ("list_duration", pa.array([[DAY_US + 5]], pa.list_(pa.duration("us")))),
    ("list_bytes", pa.array([[b"x\n"]], pa.list_(pa.binary()))),
    ("list_bool", pa.array([[True, None]], pa.list_(pa.bool_()))),
    (
        "struct",
        pa.array([{"a": 1, "b": "x y"}, None], pa.struct([("a", pa.int64()), ("b", pa.string())])),
    ),
    (
        "struct_reg",
        pa.array(
            [{"oid": 1259, "name": "pg_class"}],
            pa.struct([("oid", pa.int64()), ("name", pa.string())]),
        ),
    ),
    (
        "struct_nested",
        pa.array(
            [{"a": [1, 2], "b": {"c": None}}],
            pa.struct([("a", pa.list_(pa.int64())), ("b", pa.struct([("c", pa.float64())]))]),
        ),
    ),
    ("duckdb_inet", _duckdb_inet(["1.2.3.4/24", "::ffff:1.2.3.4/128", "2001:db8::1/64", None])),
    ("map", pa.array([[("k", 1), ("v", None)], []], pa.map_(pa.string(), pa.int64()))),
    ("dictionary", pa.array(["a", "b", "a", None]).dictionary_encode()),
    ("large_string", pa.array(["x", None], pa.large_string())),
    ("strings", pa.array(["", "abc", "t", "NULL", "日本語", "tab\there", None])),
]

#: Texts each a column of its own (one bad row raises for the column): the PostgreSQL text forms of
#: the types held as text, and what int(), float() and Decimal() make of a str.
TEXTS = [
    "42",
    "  -17 ",
    "1_000",
    "١٢٣",
    "1.5",
    "-0.0",
    "1e300",
    "inf",
    "-Infinity",
    "NaN",
    "0x1F",
    "4294967296",
    "(1,2)",
    "(1.5,-2e-05)",
    "{1,-1,0}",
    "[(0,0),(1,1)]",
    "((0,0),(1,1),(2,0))",
    "(2,3),(0,0)",
    "<(1,2),3>",
    "[1,10)",
    "empty",
    '["2020-01-01 00:00:00","2020-01-02 00:00:00")',
    '["2020-01-01 00:00:00+00","2020-01-02 00:00:00+05:30")',
    "[2020-01-01,2020-02-01)",
    "{[1,3), [5,7)}",
    "{}",
    "'a':1A,2 'b''c' 'd\\\\e':3C",
    "'a' & 'b' | !'c' <-> 'd':*A",
    "192.168.0.1/24",
    "10.0.0.0/8",
    "2001:db8::1",
    "::ffff:1.2.3.4",
    "08:00:2b:01:02:03",
    "08-00-2b-01-02-03-04-05",
    "101",
    "10101010101",
    "(1,2)",
    "10:20:12,15",
    "16/B374D848",
    '<?xml version="1.1" standalone="yes"?><a/>',
    '<?xml version="1.0"?>\n<a/>',
    "$1,234.50",
    "-$0.01",
    "{1,2,3}",
    "{{a,b},{c,NULL}}",
    '{"a": 1, "b": [1, 2]}',
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    "2024-02-29",
    "12:34:56.789+05:30",
    "12:34:56-08",
    "1 day 02:03:04",
    "$.a",
    "=r/postgres",
    "x",
    "é",
    "",
]

MATRIX = [(name, arr) for name, arr in POOL] + [
    (f"text_{k}", pa.array([t])) for k, t in enumerate(TEXTS)
]


#: Every BVType in each format: a tsquery in binary is held to PostgreSQL's own bytes instead
#: (test_tsquery_binary), since the product refuses it.
TYPE_FORMATS = [(t, f) for t in ALL_TYPES for f in (0, 1) if not (t == BVType.TSQUERY and f == 1)]


@pytest.mark.parametrize(
    ("bvtype", "fmt"),
    TYPE_FORMATS,
    ids=[f"{_type_name(t)}-{'binary' if f else 'text'}" for t, f in TYPE_FORMATS],
)
def test_every_type_from_every_arrow_type(bvtype: Any, fmt: int) -> None:
    """Each BVType's writer, over every column of the pool: the same bytes, or both raise."""
    failures = []
    for name, arr in MATRIX:
        try:
            _forced([arr], [bvtype], [fmt])
        except AssertionError as e:
            failures.append(f"{name}: {e}"[:400])
    assert not failures, "\n".join(failures)


# --- every coercer ProvisaQueryResult applies, over every column of the pool ---


COERCED_TEXTS = [
    "[1, 2.5, null]",
    "[[1, 2], [3, 4]]",
    '["a", null, "b c"]',
    "[]",
    "null",
    '{"a": 1}',
    '{"b": 1, "aa": [1.50, 1e2, -0.0, 0E+2, 1E400], "a": "\\u00e9\\n\\u0001", "a": true}',
    '"x"',
    "NaN",
    "[1, 2",
    '<?xml version="1.0" encoding="UTF-8"?>\n<a/>',
    '<?xml  version = "1.0"  standalone="no" ?><a/>',
    '<?xml version="1.0"?><a/>',
    "<?xmlx?>",
    "abcdefgh",
    "ab",
    "é日本",
    # interval_out's text (cast_types.interval_of_text), and texts it is not
    "1 day 02:03:04",
    "1 year 2 mons -3 days +04:05:06.5",
    "-1 days -00:00:01.000001",
    "26:00:00",
    "1 day",
    "1 days 2",
    "1:2:3",
    "٣ days",
    "",
]
COERCER_POOL = (
    MATRIX
    + [(f"coerced_text_{k}", pa.array([t])) for k, t in enumerate(COERCED_TEXTS)]
    + [
        ("clock_texts", pa.array(["01:02:03", "23:59:59.999999", "24:00:00", "1:2:3", "", None])),
        ("byte_texts", pa.array(["\x00\x01", "abc", "é", "", None])),
        (
            "clock_bytes",
            pa.array(
                [(3_600_000_001).to_bytes(8, "little", signed=True), b"", b"\xff"], pa.binary()
            ),
        ),
    ]
)

T = BVType
#: (coercer, the BVTypes it brings values to)
COERCIONS: list[tuple[Any, list[Any]]] = [
    (int, [T.INTEGER, T.BIGINT, T.SMALLINT, T.OID]),
    (float, [T.FLOAT, T.REAL]),
    (server._to_decimal, [T.DECIMAL, T.MONEY]),
    (server._to_date, [T.DATE]),
    (server._to_timestamp, [T.TIMESTAMP, T.TIMESTAMPTZ]),
    (
        server._to_list,
        [
            T.INTEGERARRAY,
            T.STRINGARRAY,
            T.FLOATARRAY,
            T.DECIMALARRAY,
            T.BOOLARRAY,
            T.MONEYARRAY,
            T.INTERVALARRAY,
            XML_ARRAY,
            T.OIDARRAY,
            T.REALARRAY,
        ],
    ),
    (server._null_alone, [T.TEXT]),
    (server._duckdb_inet, [T.INET, T.CIDR, T.TEXT]),
    (xml_out, [T.XML]),
    (jsonb_out, [T.JSONB]),
    (cast_types._to_int, [T.INTEGER, T.BIGINT]),
    (cast_types._to_float, [T.FLOAT, T.REAL]),
    (cast_types._to_decimal, [T.DECIMAL]),
    (cast_types._to_bool, [T.BOOL]),
    (cast_types.record_text, [T.TEXT]),
    (cast_types._clock_of, [T.TIME]),
    (cast_types._of_no_zone, [T.TIMESTAMP]),
    (cast_types._padded(5), [T.TEXT]),
    (cast_types.interval_of_text, [T.INTERVAL]),
    (cast_types.bytes_of_text, [T.BYTES]),
    (cast_types.clock_of_text, [T.TIME]),
]
COERCER_CASES = [(c, t) for c, types in COERCIONS for t in types]


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
@pytest.mark.parametrize(
    "case",
    COERCER_CASES,
    ids=[f"{getattr(c, '__qualname__', c)}-{_type_name(t)}" for c, t in COERCER_CASES],
)
def test_every_coercer(case: tuple[Any, Any], fmt: int) -> None:
    """The coercer, then the type's writer, over every column of the pool: the same bytes, or
    both raise."""
    coercer, bvtype = case
    failures = []
    for name, arr in COERCER_POOL:
        try:
            _forced([arr], [bvtype], [fmt], force_coerce=[(0, coercer)])
        except AssertionError as e:
            failures.append(f"{name}: {e}"[:400])
    assert not failures, "\n".join(failures)


# --- columns as the product describes them: engine types, shapes, casts ---

POOL_BY_NAME = dict(POOL)
_TS_INF = pa.array([I64_MAX, _ts(2024, 1, 2, 3, 4, 5, 6), -I64_MAX, None], pa.timestamp("us"))
_TSTZ_INF = pa.array([I64_MAX, _ts(2024, 1, 2, 3, 4, 5, 6), None], pa.timestamp("us", "UTC"))
_DATES_INF = pa.array([I32_MAX, _date(2024, 1, 2), -I32_MAX, None], pa.date32())

#: (id, column, engine type, how it is described, the coercers the product applies)
DESCRIBED: list[tuple[str, Any, str, dict[str, Any], list[str]]] = [
    ("duckdb_inet", POOL_BY_NAME["duckdb_inet"], "INET", {}, ["duckdb_inet"]),
    (
        "shape_int_of_double",
        pa.array([1.9, -2.5, 1e18, None]),
        "DOUBLE",
        {"shape": [("c0", "INTEGER")]},
        ["int"],
    ),
    (
        "shape_bigint_of_nan",
        pa.array([float("nan")]),
        "DOUBLE",
        {"shape": [("c0", "BIGINT")]},
        ["int"],
    ),
    (
        "shape_double_of_bigint",
        pa.array([2**53 + 1, -3, None]),
        "BIGINT",
        {"shape": [("c0", "DOUBLE")]},
        ["float"],
    ),
    (
        "shape_decimal_of_double",
        pa.array([0.1, 1e16, 1e-7, -0.0, float("nan"), float("inf"), None]),
        "DOUBLE",
        {"shape": [("c0", "DECIMAL")]},
        ["decimal"],
    ),
    ("shape_date_of_timestamp", _TS_INF, "TIMESTAMP", {"shape": [("c0", "DATE")]}, ["date"]),
    (
        "shape_timestamp_of_date",
        _DATES_INF,
        "DATE",
        {"shape": [("c0", "TIMESTAMP")]},
        ["timestamp"],
    ),
    (
        "shape_timestamptz_of_date",
        _DATES_INF,
        "DATE",
        {"shape": [("c0", "TIMESTAMPTZ")]},
        ["timestamp"],
    ),
    (
        "shape_int_array_of_json",
        pa.array(["[1, null, 3]", "[[1, 2], [3, 4]]", "[]", "null", None]),
        "VARCHAR",
        {"shape": [("c0", "INTEGER[]")]},
        ["list"],
    ),
    (
        "shape_text_array_of_json",
        pa.array(['["a", "b c", null]', "[[], []]"]),
        "VARCHAR",
        {"shape": [("c0", "VARCHAR[]")]},
        ["list"],
    ),
    (
        "read_shape_xml",
        pa.array(['<?xml version="1.0" encoding="UTF-8"?>\n<a/>', "<b/>", None]),
        "VARCHAR",
        {"read_shape": [("c0", "xml")]},
        ["xml_out"],
    ),
    (
        "cast_char",
        pa.array(["ab", "abcdefg", "", None]),
        "VARCHAR",
        {"sql": "SELECT c0::char(5) FROM t"},
        ["pad:5"],
    ),
    ("cast_bool", pa.array([1, 0, 7, None]), "INTEGER", {"sql": "SELECT c0 = 1 FROM t"}, ["bool"]),
    (
        "cast_jsonb",
        pa.array(['{"b": 1, "a": [1.50, 2]}', "", None]),
        "JSON",
        {"sql": "SELECT c0::jsonb FROM t"},
        ["jsonb_out"],
    ),
    (
        "cast_time_of_bytes",
        POOL_BY_NAME["binary"],
        "BLOB",
        {"sql": "SELECT c0::time FROM t"},
        ["clock"],
    ),
    (
        "cast_timestamp_of_tz",
        _TSTZ_INF,
        "TIMESTAMP",
        {"sql": "SELECT c0::timestamp FROM t"},
        ["no_zone"],
    ),
    (
        "cast_money",
        pa.array([Decimal("1234.5"), Decimal("-0.005"), None], pa.decimal128(21, 4)),
        "DECIMAL(21,4)",
        {"sql": "SELECT c0::money FROM t"},
        [],
    ),
    (
        "cast_xml",
        pa.array(['<?xml version="1.1"?><a/>']),
        "VARCHAR",
        {"sql": "SELECT c0::xml FROM t"},
        ["xml_out"],
    ),
    *(
        (f"read_shape_{name}", pa.array(texts), "VARCHAR", {"read_shape": [("c0", name)]}, [])
        for name, texts in [
            ("inet", ["192.168.0.1/24", "::ffff:1.2.3.4", None]),
            ("cidr", ["10.0.0.0/8"]),
            ("macaddr", ["08:00:2b:01:02:03"]),
            ("macaddr8", ["08:00:2b:01:02:03:04:05"]),
            ("money", ["$1,234.50", "-$0.01"]),
            ("point", ["(1,2)", "(NaN,-Infinity)"]),
            ("line", ["{1,-1,0}"]),
            ("lseg", ["[(0,0),(1,1)]"]),
            ("box", ["(2,3),(0,0)"]),
            ("path", ["((0,0),(1,1))", "[(0,0)]"]),
            ("polygon", ["((0,0),(1,1),(2,0))"]),
            ("circle", ["<(1,2),3>"]),
            ("int4range", ["[1,10)", "empty"]),
            ("daterange", ["[2020-01-01,infinity)"]),
            ("tsrange", ['["2000-01-01 00:00:00 BC",)']),
            ("tsvector", ["'a':1A,2 'b'"]),
            ("tsquery", ["'a' & 'b'"]),
            ("bit", ["101"]),
            ("pg_lsn", ["16/B374D848"]),
            ("jsonpath", ['$."a"']),
            ("txid_snapshot", ["10:20:12,15"]),
            ("geometry", ["POINT(1 2)"]),
        ]
    ),
]


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
@pytest.mark.parametrize("case", DESCRIBED, ids=[c[0] for c in DESCRIBED])
def test_described(case: tuple[str, Any, str, dict[str, Any], list[str]], fmt: int) -> None:
    """A column as the product describes it (its engine type, the statement's shape or cast) and
    the coercers it applies, which the Rust encoder is told by name."""
    _, column, engine_type, described, coercers = case
    batch = _batch([column])
    py, bvtypes, said = _python(batch, [engine_type], [fmt], **described)
    assert said == [coercers]
    if bvtypes == ["TSQUERY"] and fmt == 1:
        # the product refuses a binary tsquery: PostgreSQL's own bytes
        assert isinstance(py, ValueError)
        rows = _data_rows(_wired(batch, [engine_type], [fmt], bvtypes, **described))
        want = [bytes.fromhex(dict(TSQUERY_SEND)[t]) for t in column.to_pylist()]
        assert [r[0] for r in rows] == want
        return
    _check([column], [engine_type], [fmt], **described)


# --- PostgreSQL's own bytes (PostgreSQL 18.4, pg_types_check.py) ---

#: tsquery text -> tsquerysend(text::tsquery)
TSQUERY_SEND = [
    ("'a' & 'b' & 'c'", "0000000502020100006300020201000062000100006100"),
    ("( 'a' | 'b' ) & 'c'", "0000000502020100006300020301000062000100006100"),
    ("'a' <-> ( 'b' <-> 'c' )", "000000050204000102040001010000630001000062000100006100"),
    ("'a' <-> 'b' <-> 'c'", "000000050204000101000063000204000101000062000100006100"),
    ("'a' <3> 'b'", "000000030204000301000062000100006100"),
    ("!( 'a' & 'b' )", "000000040201020201000062000100006100"),
    ("!!'a'", "00000003020102010100006100"),
    ("'a':AB & 'b':*", "0000000302020100016200010c006100"),
    ("'x y' & 'o''k'", "0000000302020100006f276b0001000078207900"),
    ("'a\\\\b'", "00000001010000615c6200"),
    ("( 'a' | 'b' ) <-> 'c'", "00000005020400010100006300020301000062000100006100"),
    ("'a' <0> 'b'", "000000030204000001000062000100006100"),
    (
        "'a' & !'b' | 'c' & 'd':*BCD",
        "0000000802030202010701640001000063000202020101000062000100006100",
    ),
    (
        "'fat' & ( 'rat' | 'cat' ) <2> !'dog'",
        "000000080202020400020201010000646f67000203010000636174000100007261740001000066617400",
    ),
    ("'é' | 'ü'", "000000030203010000c3bc00010000c3a900"),
    ("'a' & 'b'", "00000003020201000062000100006100"),
]


def test_tsquery_binary() -> None:
    """tsquery in binary is tsquerysend of tsquery_in(text): its operators left-associative, each
    stored before its right operand, then its left. The product refuses a binary tsquery."""
    texts = [t for t, _ in TSQUERY_SEND] + ["", "   "]
    out = pgwire_encoder.encode_batch(_batch([pa.array(texts)]), ["TSQUERY"], [1])
    cells = [row[0] for row in _data_rows(out)]
    assert cells == [bytes.fromhex(h) for _, h in TSQUERY_SEND] + [b"\x00\x00\x00\x00"] * 2
    former = _former(_batch([pa.array(texts[:1])]), ["VARCHAR"], [1], force_types=[BVType.TSQUERY])
    assert isinstance(former, ValueError)


def test_tsquery_malformed() -> None:
    for text in ["'a' &", "( 'a'", "''", "'a' <x> 'b'", "'unterminated"]:
        with pytest.raises(ValueError):
            pgwire_encoder.encode_batch(_batch([pa.array([text])]), ["TSQUERY"], [1])


# --- RawDataRowBytes passthrough ---


def test_forward_raw() -> None:
    """Rows a PostgreSQL source sent whole are forwarded verbatim, in send_data_rows' chunks of 500
    rows: the wired path writes what the former writer wrote."""
    from buenavista.core import RawDataRowBytes, ResultData

    from tests.unit.pgwire_encoder import oracle
    from tests.unit.pgwire_encoder.python_ref import _Sink

    rows = [
        b"D" + struct.pack("!ih", 4 + 2 + 4 + len(v), 1) + struct.pack("!i", len(v)) + v
        for v in (str(k).encode() for k in range(1234))
    ] + [b"D\x00\x00\x00\x0a\x00\x01\xff\xff\xff\xff"]

    class _Raw:
        result_format = None

        def column_count(self) -> int:
            return 1

        def column(self, i: int) -> tuple[str, Any]:
            return ("c", BVType.TEXT)

        def data(self) -> ResultData:
            return ResultData(iter([[RawDataRowBytes(r) for r in rows]]), raw=True)

        def coercers(self) -> None:
            return None

    py_writes: list[bytes] = []
    writer = type("W", (), {"write": staticmethod(py_writes.append)})()
    py_count = oracle.send_data_rows(writer, _Raw(), (RawDataRowBytes(r) for r in rows))
    rs_writes: list[bytes] = []
    rs_count = pgwire_encoder.forward_raw(iter(rows), rs_writes.append)
    wired: list[bytes] = []
    wired_count = BuenaVistaHandler.send_data_rows(
        _Sink(type("W", (), {"write": staticmethod(wired.append)})()),  # type: ignore[arg-type]
        _Raw(),
    )
    assert (rs_count, rs_writes) == (py_count, py_writes) == (wired_count, wired)
    assert [len(w) for w in rs_writes][:2] == [sum(len(r) for r in rows[:500])] * 1 + [
        sum(len(r) for r in rows[500:1000])
    ]
    for bad in [
        b"",
        b"X\x00\x00\x00\x06\x00\x00",
        b"D\x00\x00\x00\x07\x00\x00",
        b"D\x00\x00\x00\x0a\x00\x01\x00\x00\x00\x09",
    ]:
        with pytest.raises(ValueError):
            pgwire_encoder.forward_raw([bad], rs_writes.append)


def test_encode_stream_coerced() -> None:
    """encode_stream takes each column's coercers as encode_batch does."""
    column = pa.array([1.9, None, -2.5])
    batch = _batch([column])
    py, bvtypes, coercers = _python(batch, ["DOUBLE"], [1], shape=[("c0", "INTEGER")])
    writes: list[bytes] = []
    n = pgwire_encoder.encode_stream(
        [batch, batch.slice(0, 0), batch], bvtypes, writes.append, [1], coercers
    )
    assert n == 6 and b"".join(writes) == py * 2


# --- each product defect pg_reference corrects: the product and the reference differ on a witness,
# and the encoder gives the reference's (PostgreSQL's) answer ---

WITNESSES: dict[str, tuple[list[Any], list[Any], list[int], dict[str, Any]]] = {
    "numeric_text": ([pa.array([Decimal("1E-7")], pa.decimal128(10, 7))], [T.DECIMAL], [0], {}),
    # main's 68bdf0150f kept every digit past 28 before the wiring; Infinity still raised.
    "numeric_binary": (
        [pa.array(["1234567890123456789012345678.9012345678", "Infinity", "-Infinity"])],
        [T.DECIMAL],
        [1],
        {},
    ),
    "empty_array": (
        [pa.array([[[], []]], pa.list_(pa.list_(pa.int32())))],
        [T.INTEGERARRAY],
        [1],
        {},
    ),
    "array_element_list": (
        [pa.array([[None, ["a"]]], pa.list_(pa.list_(pa.string())))],
        [T.STRINGARRAY],
        [1],
        {},
    ),
    "vector_lower_bound": ([pa.array([[1, 2]], pa.list_(pa.int64()))], [T.OIDVECTOR], [1], {}),
    "inet_text": (
        [_duckdb_inet(["::1.2.3.4/128", "::ffff:1.2.3.4/128"])],
        [T.INET],
        [0],
        {"force_coerce": [(0, server._duckdb_inet)]},
    ),
    "duckdb_inet": (
        [POOL_BY_NAME["duckdb_inet"]],
        [T.INET],
        [0],
        {"force_coerce": [(0, server._duckdb_inet)]},
    ),
    "range_bounds": ([pa.array(["[2020-01-01,infinity)"])], [T.DATERANGE], [1], {}),
    "tsquery_binary": ([pa.array(["'a' & 'b'"])], [T.TSQUERY], [1], {}),
    "money_range": (
        [pa.array([Decimal("100000000000000000")], pa.decimal128(24, 4))],
        [T.MONEY],
        [0],
        {},
    ),
    "interval_days": (
        [pa.array([(0, 999_999_999, (DAY_US - 1) * 1000)], pa.month_day_nano_interval())],
        [T.INTERVAL],
        [0],
        {},
    ),
    "coerced_infinity": ([_TS_INF], [T.DATE], [0], {"force_coerce": [(0, server._to_date)]}),
    "uuid_bytes": ([POOL_BY_NAME["fsb16"]], [T.UUID], [0], {}),
}


#: Defects main corrected in the Python writers themselves before they were replaced (68bdf0150f,
#: the integer interval day split; ed563ffbe6, a UUID's 16 plain bytes): the witness shows the
#: former writers and PostgreSQL agree.
FIXED_BEFORE_WIRING = {"interval_days", "uuid_bytes"}


@pytest.mark.parametrize("defect", sorted(DEFECTS))
def test_product_defect(defect: str) -> None:
    """Each defect of the product's former Python writers, on a witness: what they sent differs
    from PostgreSQL 17's answer, and pgwire now sends PostgreSQL's (REQ-2194: the wired path is
    the contract; the former answer is not)."""
    arrays, types, formats, described = WITNESSES[defect]
    batch = _batch(arrays)
    names = ["VARCHAR"] * len(arrays)
    former = _former(batch, names, formats, force_types=types, **described)
    reference, bvtypes, _ = _python(batch, names, formats, force_types=types, **described)
    wired = _wired(batch, names, formats, bvtypes, force_types=types, **described)
    if defect == "tsquery_binary":
        # no reference writes it: PostgreSQL's own bytes
        assert isinstance(former, ValueError)
        assert _data_rows(wired)[0][0] == bytes.fromhex(dict(TSQUERY_SEND)["'a' & 'b'"])
        return
    if defect == "inet_text":
        # the former text of the address _duckdb_inet makes (given an int, as DuckDB's own
        # Python rows carry it): ipaddress's hextets for an IPv4-compatible address, in every
        # Python (an IPv4-mapped one is dotted from CPython 3.12.16 on, hextets before)
        from tests.unit.pgwire_encoder import oracle

        assert oracle._inet_text(ipaddress.ip_interface("::1.2.3.4/128")) == "::102:304"
        assert [r[0] for r in _data_rows(wired)] == [b"::1.2.3.4", b"::ffff:1.2.3.4"]
    if defect in FIXED_BEFORE_WIRING:
        assert repr(former) == repr(reference), f"{defect}: the former writers differ again"
    else:
        assert repr(former) != repr(reference), f"{defect}: the former writers answer as PostgreSQL"
    _assert_same(reference, wired, bvtypes, formats)


def test_unicode_version() -> None:
    """repr's table of printable characters is this interpreter's (gen_unicode.py)."""
    import unicodedata

    assert pgwire_encoder.UNICODE_VERSION == unicodedata.unidata_version


def test_no_type_is_unported() -> None:
    """Every BVType, from every column of the pool, encodes or raises what the product's writer
    raises: never NotImplementedError."""
    for bvtype in ALL_TYPES:
        for fmt in (0, 1):
            for _, arr in MATRIX:
                out = _rust(_batch([arr]), [_type_name(bvtype)], [fmt], [[]])
                assert not isinstance(out, NotImplementedError), (bvtype, arr.type, out)
