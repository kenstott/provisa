"""Differential test: the Rust DataRow encoder against the product's Python encode path, byte for
byte, over every type the port covers, NULLs and edge values, in text and binary formats.

Lives outside tests/ so the project's suites never collect it. Run (from the worktree root):
    experiments/pgwire_encoder_rs/build.sh
    PYTHONPATH=.:vendor/buenavista:experiments/pgwire_encoder_rs/build:experiments/pgwire_encoder_rs \
        .venv/bin/python -m pytest -p no:cacheprovider experiments/pgwire_encoder_rs/test_compare.py
"""

from __future__ import annotations

import datetime as dt
import random
import struct
import sys
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

import pgwire_encoder_rs
from python_ref import python_encode

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
    ("timestamp", pa.array(TS_US, pa.timestamp("us")), "TIMESTAMP"),
    (
        "timestamp_ms",
        pa.array([0, -1, 1_700_000_000_123, None], pa.timestamp("ms")),
        "TIMESTAMP_MS",
    ),
    ("timestamp_s", pa.array([0, -1, 1_700_000_000, None], pa.timestamp("s")), "TIMESTAMP_S"),
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


def _both(
    arrays: list[Any], types: list[str], formats: list[int] | None
) -> tuple[Any, Any, list[str] | None]:
    batch = pa.RecordBatch.from_arrays(arrays, names=[f"c{i}" for i in range(len(arrays))])
    try:
        py, bvtypes = python_encode(batch.schema, [batch], types, formats)
    except Exception as e:  # noqa: BLE001 -- a raise is an outcome to compare
        py, bvtypes = e, None
    if bvtypes is None:
        # The encoder raised mid-stream; the types are known before any row is encoded.
        from python_ref import bvtype_names, query_result

        bvtypes = bvtype_names(query_result(batch.schema, [], types, formats))
    try:
        rs = pgwire_encoder_rs.encode_batch(batch, bvtypes, formats)
    except Exception as e:  # noqa: BLE001
        rs = e
    return py, rs, bvtypes


def _assert_same(py: Any, rs: Any) -> None:
    if isinstance(py, Exception) or isinstance(rs, Exception):
        assert isinstance(py, Exception) and isinstance(rs, Exception), (
            f"only one raised: python={py!r:.300} rust={rs!r:.300}"
        )
        return
    if py != rs:
        at = next(
            i
            for i in range(min(len(py), len(rs)) + 1)
            if i >= len(py) or i >= len(rs) or py[i] != rs[i]
        )
        raise AssertionError(
            f"bytes differ at {at} (python {len(py)} bytes, rust {len(rs)} bytes)\n"
            f"python: {py[max(0, at - 40) : at + 60]!r}\nrust:   {rs[max(0, at - 40) : at + 60]!r}"
        )


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
@pytest.mark.parametrize("case", CASES, ids=[c[0] for c in CASES])
def test_column(case: tuple[str, Any, str], fmt: int) -> None:
    _, array, type_name = case
    py, rs, _ = _both([array], [type_name], [fmt])
    _assert_same(py, rs)


def test_default_format_is_text() -> None:
    py, rs, _ = _both([pa.array([1, None]), pa.array(["a", None])], ["BIGINT", "VARCHAR"], None)
    _assert_same(py, rs)


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
    py, rs, _ = _both(arrays, types, formats)
    _assert_same(py, rs)


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_random_float64_bit_patterns(fmt: int) -> None:
    rng = random.Random(64)
    values = [
        struct.unpack("!d", rng.getrandbits(64).to_bytes(8, "big"))[0] for _ in range(200_000)
    ]
    values += [rng.uniform(-1e3, 1e3) for _ in range(100_000)]
    py, rs, _ = _both([pa.array(values, pa.float64())], ["DOUBLE"], [fmt])
    _assert_same(py, rs)


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_random_float32_bit_patterns(fmt: int) -> None:
    rng = random.Random(32)
    values = [_f32(rng.getrandbits(32)) for _ in range(200_000)]
    values += [
        _f32(struct.unpack("!I", struct.pack("!f", rng.uniform(-1e3, 1e3)))[0])
        for _ in range(100_000)
    ]
    py, rs, _ = _both([pa.array(values, pa.float32())], ["FLOAT"], [fmt])
    _assert_same(py, rs)


def test_random_float64_as_real() -> None:
    """A double column described as float4: the text search over digits, in a double's values."""
    rng = random.Random(4)
    values = [rng.uniform(-1e30, 1e30) * 10 ** rng.randint(-40, 0) for _ in range(100_000)]
    py, rs, _ = _both([pa.array(values, pa.float64())], ["REAL"], [0])
    _assert_same(py, rs)


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_random_decimals(fmt: int) -> None:
    rng = random.Random(10)
    for scale in (0, 1, 4, 9, 10, 20, 37, 38):
        values = [
            Decimal(rng.randint(-(10**38) + 1, 10**38 - 1)).scaleb(-scale) for _ in range(5000)
        ]
        values += [Decimal(rng.randint(-999, 999)).scaleb(-scale) for _ in range(5000)]
        py, rs, _ = _both(
            [pa.array(values, pa.decimal128(38, scale))], [f"DECIMAL(38,{scale})"], [fmt]
        )
        _assert_same(py, rs)


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
    py, rs, _ = _both([pa.array(values, pa.month_day_nano_interval())], ["INTERVAL"], [fmt])
    _assert_same(py, rs)


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_random_timestamps(fmt: int) -> None:
    rng = random.Random(12)
    lo, hi = _ts(1, 1, 1), _ts(9999, 12, 31, 23, 59, 59, 999999)
    values = [rng.randint(lo, hi) for _ in range(50_000)]
    for tz in (None, "UTC", "+09:00", "-11:30"):
        safe = [v for v in values if lo + DAY_US < v < hi - DAY_US]
        py, rs, _ = _both(
            [pa.array(safe, pa.timestamp("us", tz=tz))],
            ["TIMESTAMP WITH TIME ZONE" if tz else "TIMESTAMP"],
            [fmt],
        )
        _assert_same(py, rs)


def test_many_batches_chunk_boundaries() -> None:
    """send_data_rows writes in 500-row chunks; the bytes are the concatenation either way."""
    arrays, types = _mixed(1234, seed=3)
    batch = pa.RecordBatch.from_arrays(arrays, names=[f"c{i}" for i in range(len(arrays))])
    batches = [batch.slice(0, 1), batch.slice(1, 700), batch.slice(701)]
    py, bvtypes = python_encode(batch.schema, batches, types, [1] * 10)
    rs = b"".join(pgwire_encoder_rs.encode_batch(b, bvtypes, [1] * 10) for b in batches)
    _assert_same(py, rs)
