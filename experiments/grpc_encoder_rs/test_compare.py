# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0e8a37-1c6f-4b29-a7e4-93f2b8c06d15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Differential test: the product's Python gRPC encode path against the Rust encoder.

Both encode the same Arrow batches with the same compiled service. Each pair of messages is decoded
with one descriptor and compared field by field: float and double values within a relative
tolerance of 1e-14, everything else exactly. Where Python raises, Rust must raise too. The bytes
are also compared; every case asserts they are identical, which is stronger than the decoded
comparison but is not what the decoded comparison relies on.

Not collected by CI (it lives under experiments/). Run from the worktree root:

    experiments/grpc_encoder_rs/build.sh
    PYTHONPATH=.:experiments/grpc_encoder_rs/build:experiments/grpc_encoder_rs \\
        ~/.provisa-agent/heavy.sh .venv/bin/python -m pytest -p no:cacheprovider -q \\
        experiments/grpc_encoder_rs/test_compare.py
"""

from __future__ import annotations

import datetime as dt
import math
import struct
from decimal import Decimal
from typing import Any

import numpy as np
import pyarrow as pa
import pytest
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.unknown_fields import UnknownFieldSet

from python_ref import Service, python_encode, rust_encode

REL_TOL = 1e-14
FLOATS = (FieldDescriptor.TYPE_DOUBLE, FieldDescriptor.TYPE_FLOAT)


@pytest.fixture(scope="module")
def svc() -> Service:
    return Service()


# ---- comparison -------------------------------------------------------------------------------


def _same_float(a: float, b: float) -> bool:
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    if a == b:
        return math.copysign(1.0, a) == math.copysign(1.0, b)
    return math.isclose(a, b, rel_tol=REL_TOL, abs_tol=0.0)


def _same_value(field: Any, a: Any, b: Any, path: str) -> None:
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        assert_same_message(a, b, path)
    elif field.type in FLOATS:
        assert _same_float(a, b), f"{path}: {a!r} != {b!r}"
    else:
        assert a == b and type(a) is type(b), f"{path}: {a!r} != {b!r}"


def assert_same_message(a: Any, b: Any, path: str = "") -> None:
    """Field by field: the same fields present, each value equal (floats within REL_TOL)."""
    fa, fb = dict(a.ListFields()), dict(b.ListFields())
    names_a = sorted(f.name for f in fa)
    names_b = sorted(f.name for f in fb)
    assert names_a == names_b, f"{path}: fields {names_a} != {names_b}"
    for field, va in fa.items():
        vb = fb[field]
        where = f"{path}.{field.name}"
        if field.is_repeated:
            assert len(va) == len(vb), f"{where}: {len(va)} != {len(vb)} items"
            for k, (x, y) in enumerate(zip(va, vb, strict=True)):
                _same_value(field, x, y, f"{where}[{k}]")
        else:
            _same_value(field, va, vb, where)
    unknown_a = [(u.field_number, u.wire_type, u.data) for u in UnknownFieldSet(a)]
    unknown_b = [(u.field_number, u.wire_type, u.data) for u in UnknownFieldSet(b)]
    assert unknown_a == unknown_b, f"{path}: unknown fields {unknown_a} != {unknown_b}"


STATS = {"cases": 0, "messages": 0, "identical": 0, "both_raised": 0}


def compare(
    svc: Service,
    type_name: str,
    batches: list[pa.RecordBatch] | pa.RecordBatch,
    *,
    mode: str = "row",
    batch_rows: int = 0,
    decode_as: Any = None,
) -> list[bytes]:
    """Encodes through both paths and compares; returns the messages. ``decode_as`` is the
    service the messages are decoded with (the wire's by default)."""
    if isinstance(batches, pa.RecordBatch):
        batches = [batches]
    schema = batches[0].schema
    STATS["cases"] += 1
    try:
        py = python_encode(svc.wire, type_name, schema, batches, mode=mode, batch_rows=batch_rows)
    except Exception as py_exc:  # noqa: BLE001  # allow-blind-except: whatever Python raises, Rust must raise too
        with pytest.raises(Exception) as rs_exc:  # noqa: B017, PT011
            rust_encode(svc, type_name, schema, batches, mode=mode, batch_rows=batch_rows)
        assert not isinstance(rs_exc.value, NotImplementedError), (
            f"Python raised {py_exc!r}; Rust refused instead: {rs_exc.value!r}"
        )
        STATS["both_raised"] += 1
        return []
    rs = rust_encode(svc, type_name, schema, batches, mode=mode, batch_rows=batch_rows)
    assert len(py) == len(rs), f"{len(py)} Python messages, {len(rs)} Rust"
    name = f"{type_name}Batch" if batch_rows else type_name
    cls = getattr(decode_as or svc.wire, name)
    for k, (p, r) in enumerate(zip(py, rs, strict=True)):
        assert_same_message(cls.FromString(p), cls.FromString(r), f"{name}[{k}]")
    STATS["messages"] += len(py)
    STATS["identical"] += sum(p == r for p, r in zip(py, rs, strict=True))
    assert py == rs, "decoded equal, but the bytes differ"
    return py


def refused(svc: Service, type_name: str, batch: pa.RecordBatch, mode: str = "row") -> None:
    """A case the port does not cover: Python encodes it, Rust raises NotImplementedError."""
    python_encode(svc.wire, type_name, batch.schema, [batch], mode=mode)
    with pytest.raises(NotImplementedError):
        rust_encode(svc, type_name, batch.schema, [batch], mode=mode)


def one(name: str, array: pa.Array) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays([array], names=[name])


# ---- integer fields ---------------------------------------------------------------------------

I32 = (-(2**31), 2**31 - 1)
INT_TYPES = [
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.uint64(),
]


@pytest.mark.parametrize("arrow_type", INT_TYPES, ids=str)
@pytest.mark.parametrize("column", ["id", "qty", "small"])  # int64, int32, int32
def test_ints(svc, column, arrow_type):
    info = np.iinfo(arrow_type.to_pandas_dtype())
    edges = [info.min, info.max, 0, 1, -1 if info.min < 0 else 2, None]
    rng = np.random.default_rng(1)
    rand = rng.integers(info.min, info.max, 2000, dtype=arrow_type.to_pandas_dtype(), endpoint=True)
    values = [v for v in edges] + [int(v) for v in rand]
    in_range = (
        [v for v in values if v is None or I32[0] <= v <= I32[1]] if column != "id" else values
    )
    in_range = [v for v in in_range if v is None or v < 2**63]
    compare(svc, "Orders", one(column, pa.array(in_range, arrow_type)))


@pytest.mark.parametrize(
    "column,array",
    [
        ("qty", pa.array([2**31], pa.int64())),  # int32 field
        ("qty", pa.array([-(2**31) - 1], pa.int64())),
        ("small", pa.array([2**32], pa.uint64())),
        ("id", pa.array([2**63], pa.uint64())),  # int64 field
        ("id", pa.array([2**64 - 1], pa.uint64())),
    ],
    ids=str,
)
def test_int_out_of_range_raises(svc, column, array):
    compare(svc, "Orders", one(column, array))


def test_ints_from_bool_float_decimal_date_timestamp(svc):
    compare(svc, "Orders", one("id", pa.array([True, False, None])))
    floats = [0.0, -0.0, 1.9, -1.9, 1e18, -1e18, 2.0**62, 123456.75, None]
    compare(svc, "Orders", one("id", pa.array(floats)))
    compare(svc, "Orders", one("qty", pa.array([1.5, -2.5, 2147483647.9])))
    for bad in (float("nan"), float("inf"), -float("inf"), 1e19, 1e300):
        compare(svc, "Orders", one("id", pa.array([bad])))
    decimals = [
        Decimal("12.99"),
        Decimal("-12.99"),
        Decimal("0.00"),
        Decimal("9999999999999999.99"),
        None,
    ]
    compare(svc, "Orders", one("id", pa.array(decimals, pa.decimal128(18, 2))))
    compare(svc, "Orders", one("qty", pa.array([Decimal("3E+2")], pa.decimal128(5, -2))))
    compare(
        svc, "Orders", one("qty", pa.array([Decimal("3E+9")], pa.decimal128(5, -9)))
    )  # out of range
    days = [0, -1, 19000, -719162, 2932896, 2**31 - 1, -(2**31 - 1), None]  # toordinal; ± infinity
    compare(svc, "Orders", one("id", pa.array(days, pa.date32())))
    ts = [0, -1, 1_700_000_000_123_456, 2**63 - 1, -(2**63 - 1), None]
    compare(svc, "Orders", one("id", pa.array(ts, pa.timestamp("us"))))
    compare(
        svc,
        "Orders",
        one("id", pa.array([86_399_999_999, 86_400_000_000], pa.timestamp("us", "+05:30"))),
    )


def test_int_from_time_or_interval_raises(svc):
    compare(svc, "Orders", one("id", pa.array([dt.time(1, 2)], pa.time64("us"))))
    compare(svc, "Orders", one("id", pa.array([dt.timedelta(1)], pa.duration("us"))))


def test_int_from_text_is_not_ported(svc):
    refused(svc, "Orders", one("id", pa.array(["12"])))


# ---- floating fields --------------------------------------------------------------------------


def _float_bits(rng: np.random.Generator, n: int) -> np.ndarray:
    return rng.integers(0, 2**64, n, dtype=np.uint64).view(np.float64)


SPECIAL = [
    0.0,
    -0.0,
    float("nan"),
    float("inf"),
    -float("inf"),
    5e-324,
    -5e-324,
    1.7976931348623157e308,
    2.2250738585072014e-308,
    0.1,
    1 / 3,
    3.4028234663852886e38,
    3.4028235677973366e38,
    1e300,
    -1e300,
    None,
]


@pytest.mark.parametrize("column", ["price", "ratio"])  # double, float
def test_floats(svc, column):
    rng = np.random.default_rng(7)
    compare(svc, "Orders", one(column, pa.array(SPECIAL)))
    compare(svc, "Orders", one(column, pa.array(_float_bits(rng, 100_000))))
    compare(svc, "Orders", one(column, pa.array(rng.random(10_000, dtype=np.float32))))
    compare(svc, "Orders", one(column, pa.array(_float_bits(rng, 50_000).astype(np.float32))))
    compare(svc, "Orders", one(column, pa.array([True, False, None])))
    compare(
        svc, "Orders", one(column, pa.array([0, -1, 2**53 + 1, -(2**63), 2**63 - 1], pa.int64()))
    )
    compare(svc, "Orders", one(column, pa.array([2**64 - 1, 2**53 + 1], pa.uint64())))


@pytest.mark.parametrize("column", ["price", "ratio", "amount"])
@pytest.mark.parametrize("precision,scale", [(18, 2), (38, 10), (38, 0), (38, 38), (5, -3)])
def test_floats_from_decimal(svc, column, precision, scale):
    rng = np.random.default_rng(precision * 100 + scale)
    digits = rng.integers(1, precision + 1, 20_000)
    values: list[Decimal | None] = []
    for d in digits:
        unscaled = int("".join(str(x) for x in rng.integers(0, 10, d)))
        sign = -1 if rng.random() < 0.5 else 1
        values.append(Decimal(sign * unscaled).scaleb(-scale))
    values += [Decimal(0).scaleb(-scale), None]
    compare(svc, "Orders", one(column, pa.array(values, pa.decimal128(precision, scale))))


def test_float_from_temporal_raises(svc):
    compare(svc, "Orders", one("price", pa.array([0], pa.date32())))
    compare(svc, "Orders", one("price", pa.array([0], pa.timestamp("us"))))
    compare(svc, "Orders", one("price", pa.array([dt.timedelta(1)], pa.duration("us"))))


def test_float_from_text_is_not_ported(svc):
    refused(svc, "Orders", one("price", pa.array(["1.5"])))


# ---- bool -------------------------------------------------------------------------------------


def test_bools(svc):
    compare(svc, "Orders", one("active", pa.array([True, False, None])))
    compare(svc, "Orders", one("active", pa.array([0, 1, -1, None], pa.int64())))
    compare(svc, "Orders", one("active", pa.array([0.0, -0.0, float("nan"), 0.5])))
    compare(
        svc,
        "Orders",
        one("active", pa.array([Decimal("0.00"), Decimal("0.01")], pa.decimal128(5, 2))),
    )
    compare(svc, "Orders", one("active", pa.array(["", "false", "0", None])))
    compare(svc, "Orders", one("active", pa.array([b"", b"\x00"])))
    compare(svc, "Orders", one("active", pa.array([0, -719162], pa.date32())))
    compare(svc, "Orders", one("active", pa.array([0], pa.timestamp("us"))))
    compare(svc, "Orders", one("active", pa.array([dt.time(0)], pa.time64("us"))))


# ---- strings ----------------------------------------------------------------------------------

TEXT = [
    "",
    "plain",
    "héllo wörld",
    "日本語",
    "emoji \U0001f600",
    "nul \x00 inside",
    "x" * 5000,
    " \t\n",
    None,
]


def test_strings(svc):
    for column in ("name", "payload", "tag", "secret"):
        compare(svc, "Orders", one(column, pa.array(TEXT)))
    compare(svc, "Orders", one("name", pa.array(TEXT, pa.large_string())))
    compare(svc, "Orders", one("name", pa.array(TEXT, pa.string_view())))
    compare(svc, "Orders", one("name", pa.array(TEXT).dictionary_encode()))
    compare(svc, "Orders", one("name", pa.array([None, None], pa.string())))
    compare(svc, "Orders", one("name", pa.nulls(3)))
    compare(svc, "Orders", one("payload", pa.array(['{"k": 1, "v": [1, 2]}', "[]", "null"])))
    compare(svc, "Orders", one("tag", pa.array(["5f0c3a8e-4b5e-4d0c-9a2b-1c3d4e5f6a7b"])))


def test_str_of_numbers(svc):
    rng = np.random.default_rng(11)
    compare(svc, "Orders", one("name", pa.array([0, -1, 2**63 - 1, -(2**63), None], pa.int64())))
    compare(svc, "Orders", one("name", pa.array([2**64 - 1], pa.uint64())))
    compare(svc, "Orders", one("name", pa.array([True, False])))
    compare(svc, "Orders", one("name", pa.array(SPECIAL)))
    compare(svc, "Orders", one("name", pa.array(_float_bits(rng, 100_000))))
    compare(svc, "Orders", one("name", pa.array(_float_bits(rng, 20_000).astype(np.float32))))
    compare(svc, "Orders", one("name", pa.array(10.0 ** np.arange(-30, 30))))


@pytest.mark.parametrize(
    "precision,scale", [(18, 2), (38, 10), (38, 0), (38, 38), (5, -3), (10, 9)]
)
def test_str_of_decimals(svc, precision, scale):
    rng = np.random.default_rng(precision + scale + 99)
    values: list[Decimal | None] = []
    for d in rng.integers(1, precision + 1, 10_000):
        unscaled = int("".join(str(x) for x in rng.integers(0, 10, d)))
        values.append(Decimal(-unscaled if rng.random() < 0.5 else unscaled).scaleb(-scale))
    values += [Decimal(0).scaleb(-scale), None]
    compare(svc, "Orders", one("name", pa.array(values, pa.decimal128(precision, scale))))


def test_str_of_dates(svc):
    rng = np.random.default_rng(3)
    days = [0, -1, -719162, 2932896, 2**31 - 1, -(2**31 - 1), None] + rng.integers(
        -719162, 2932897, 20_000
    ).tolist()
    compare(svc, "Orders", one("day", pa.array(days, pa.date32())))
    for bad in (2932897, -719163, -(2**31)):
        compare(svc, "Orders", one("day", pa.array([bad], pa.date32())))


TS_EDGES_US = [
    0,
    -1,
    1,
    999_999,
    -86_400_000_000,
    253_402_300_799_999_999,
    -62_135_596_800_000_000,
    2**63 - 1,
    -(2**63 - 1),
    None,
]


@pytest.mark.parametrize("tz", [None, "UTC", "+00:00", "+05:30", "-03:00", "+14:00", "-12:45"])
@pytest.mark.parametrize("unit", ["s", "ms", "us"])
def test_timestamps_as_text_and_as_timestamp(svc, unit, tz):
    rng = np.random.default_rng(5)
    per = {"s": 1_000_000, "ms": 1_000, "us": 1}[unit]
    lo, hi = (
        -62_135_596_800_000_000 // per + 86_400 * 1_000_000 // per,
        253_402_300_799_999_999 // per - 86_400 * 1_000_000 // per,
    )
    values = rng.integers(lo, hi, 20_000).tolist()
    edges = [v if v is None or unit == "us" else v // per for v in TS_EDGES_US]
    arrow_type = pa.timestamp(unit, tz)
    for column in ("name", "created", "created_tz"):
        compare(svc, "Orders", one(column, pa.array(values, arrow_type)))
        for v in edges:  # one at a time: an out-of-range edge raises for its own batch only
            compare(svc, "Orders", one(column, pa.array([v], arrow_type)))


def test_timestamp_from_other_types(svc):
    compare(
        svc, "Orders", one("created", pa.array([0], pa.date32()))
    )  # fromisoformat(date): TypeError
    compare(svc, "Orders", one("created", pa.array([1], pa.int64())))
    compare(svc, "Orders", one("created", pa.array([None], pa.date32())))


def test_timestamp_from_text_is_not_ported(svc):
    refused(svc, "Orders", one("created", pa.array(["2024-01-02T03:04:05"])))


def test_timestamp_in_a_named_zone_is_not_ported(svc):
    refused(svc, "Orders", one("created", pa.array([0], pa.timestamp("us", "America/New_York"))))


def test_times(svc):
    rng = np.random.default_rng(9)
    us = [0, 1, 86_399_999_999, None] + rng.integers(0, 86_400_000_000, 10_000).tolist()
    compare(svc, "Orders", one("at", pa.array(us, pa.time64("us"))))
    compare(svc, "Orders", one("at", pa.array([0, 1, 86_399_999, None], pa.time32("ms"))))
    compare(svc, "Orders", one("at", pa.array([0, 1, 86_399, None], pa.time32("s"))))


def test_intervals(svc):
    rng = np.random.default_rng(13)
    n = 30_000
    months = rng.integers(-(2**31), 2**31, n)
    days = rng.integers(-(2**31), 2**31, n)
    nanos = rng.integers(-(2**63), 2**63, n, dtype=np.int64)
    small = rng.integers(-100, 100, (n, 3))
    values = [(int(m), int(d), int(ns)) for m, d, ns in zip(months, days, nanos, strict=True)]
    values += [(int(m), int(d), int(us) * 1_000_000_123) for m, d, us in small]
    values += [
        (0, 0, 0),
        (0, 0, -1),
        (0, 0, -999),
        (0, 0, -1000),
        (0, 1, -1000),
        (-1, 0, 0),
        (12, 0, 0),
        (-13, 2, 3_600_000_000_000),
        (0, 999_999_999, 86_399_999_999_000),
        None,
    ]
    random_part = values[:n] + [None]  # every one has months: no timedelta range to leave
    compare(svc, "Orders", one("span", pa.array(random_part, pa.month_day_nano_interval())))
    for v in values[n:]:  # one at a time: a month-less one past timedelta's range raises
        compare(svc, "Orders", one("span", pa.array([v], pa.month_day_nano_interval())))
    for edge in ((0, 999_999_999, 86_400_000_000_000), (0, -999_999_999, -1000)):
        compare(svc, "Orders", one("span", pa.array([edge], pa.month_day_nano_interval())))


@pytest.mark.parametrize("unit", ["s", "ms", "us"])
def test_durations(svc, unit):
    rng = np.random.default_rng(17)
    per = {"s": 1_000_000, "ms": 1_000, "us": 1}[unit]
    limit = 86_399_999_999_999_999_999 // per  # past timedelta.max for s and ms
    values = [0, -1, 1, None] + rng.integers(-(10**15), 10**15, 10_000).tolist()
    compare(svc, "Orders", one("span", pa.array(values, pa.duration(unit))))
    for edge in (
        999_999_999 * 86_400_000_000 // per,
        -999_999_999 * 86_400_000_000 // per,
        min(limit, 2**63 - 1),
        -(2**63),
    ):
        edge = max(min(edge, 2**63 - 1), -(2**63))  # an int64 holds no more
        compare(svc, "Orders", one("span", pa.array([edge], pa.duration(unit))))


def test_str_of_bytes_is_not_ported(svc):
    refused(svc, "Orders", one("name", pa.array([b"ab"])))


def test_native_array_into_json_string_is_not_ported(svc):
    """The product writes str(list), a Python repr, where REQ-1965 says JSON (a defect)."""
    refused(svc, "Orders", one("payload", pa.array([["a", None], []], pa.list_(pa.string()))))
    refused(svc, "Orders", one("payload", pa.array([{"k": 1.0}], pa.struct([("k", pa.float64())]))))


# ---- bytes ------------------------------------------------------------------------------------


def test_bytes(svc):
    raw = [b"", b"\x00\xff\x80", bytes(range(256)) * 20, None]
    compare(svc, "Orders", one("blob", pa.array(raw)))
    compare(svc, "Orders", one("blob", pa.array(raw, pa.large_binary())))
    compare(
        svc, "Orders", one("blob", pa.array([b"abcd", None, b"\x00\x00\x00\x01"], pa.binary(4)))
    )
    compare(svc, "Orders", one("blob", pa.array(TEXT)))  # str(...).encode()
    compare(svc, "Orders", one("blob", pa.array([1, -2], pa.int64())))
    compare(svc, "Orders", one("blob", pa.array([0.1, float("nan")])))
    compare(svc, "Orders", one("blob", pa.array([0], pa.date32())))
    compare(
        svc, "Orders", one("blob", pa.array([1_700_000_000_000_000], pa.timestamp("us", "UTC")))
    )


def test_bytes_of_interval_is_not_ported(svc):
    refused(svc, "Orders", one("blob", pa.array([dt.timedelta(1)], pa.duration("us"))))


# ---- NULLs, absent fields, redacted fields, column names --------------------------------------


def test_all_null_and_null_typed_columns_are_absent(svc):
    cols = {
        c: pa.nulls(4) for c in ("id", "qty", "price", "ratio", "name", "created", "active", "blob")
    }
    msgs = compare(svc, "Orders", pa.RecordBatch.from_pydict(cols))
    assert msgs == [b""] * 4


def test_zero_values_are_present(svc):
    """optional scalars: a zero is written, a NULL is not."""
    batch = pa.RecordBatch.from_pydict(
        {
            "id": pa.array([0, None], pa.int64()),
            "price": [0.0, None],
            "name": ["", None],
            "active": [False, None],
            "blob": [b"", None],
            "created": pa.array([0, None], pa.timestamp("us")),
        }
    )
    msgs = compare(svc, "Orders", batch)
    assert msgs[1] == b"" and len(msgs[0]) > 0


def test_redacted_field_decodes_alike_with_the_roles_service(svc):
    """The wire service carries ``secret``; the role's does not, and reads the same bytes with the
    secret as an unknown field."""
    assert "secret" in svc.wire.Orders.DESCRIPTOR.fields_by_name
    assert "secret" not in svc.role.Orders.DESCRIPTOR.fields_by_name
    batch = pa.RecordBatch.from_pydict(
        {"id": pa.array([1, 2], pa.int64()), "secret": ["s1", None], "name": ["a", "b"]}
    )
    compare(svc, "Orders", batch, decode_as=svc.role)
    compare(svc, "Orders", batch)


def test_masked_values(svc):
    """A masked column: NULL (any type, REQ-2142) or a mask string."""
    batch = pa.RecordBatch.from_pydict(
        {
            "price": pa.array([None, None], pa.float64()),
            "name": ["***", "j***@x.com"],
            "tag": ["xxxx", None],
        }
    )
    compare(svc, "Orders", batch)


def test_a_column_with_no_field(svc):
    compare(svc, "Orders", one("nope", pa.array([None, None], pa.int64())))  # never set: no error
    compare(svc, "Orders", one("nope", pa.array([None, 1], pa.int64())))  # raises at its value
    compare(svc, "Orders", one("nope", pa.array([0], pa.date32())))
    compare(svc, "Orders", one("nope", pa.array([2932897], pa.date32())))  # to_pylist raises first


def test_columns_are_matched_to_fields_by_the_servers_key(svc):
    batch = pa.RecordBatch.from_pydict(
        {
            "CUSTOMER_ID": pa.array([5], pa.int64()),
            "Created_TZ": pa.array([0], pa.timestamp("us", "UTC")),
        }
    )
    compare(svc, "Orders", batch)


# ---- empty results and batching ---------------------------------------------------------------


def _mixed(n: int, seed: int) -> pa.RecordBatch:
    rng = np.random.default_rng(seed)

    def nulls(p: float) -> np.ndarray:
        return rng.random(n) < p

    return pa.RecordBatch.from_arrays(
        [
            pa.array(np.arange(n, dtype=np.int64)),
            pa.array(rng.integers(-(2**31), 2**31, n, dtype=np.int64), pa.int32(), mask=nulls(0.1)),
            pa.array(rng.normal(0, 1e6, n), mask=nulls(0.1)),
            pa.array(rng.random(n, dtype=np.float32), mask=nulls(0.1)),
            pa.array(
                [Decimal(int(v)).scaleb(-2) for v in rng.integers(-(10**15), 10**15, n)],
                pa.decimal128(18, 2),
            ),
            pa.array([f"name-{v}-é" for v in rng.integers(0, 10**6, n)], mask=nulls(0.3)),
            pa.array(
                rng.integers(946_684_800_000_000, 1_893_456_000_000_000, n),
                pa.timestamp("us"),
                mask=nulls(0.1),
            ),
            pa.array(
                rng.integers(946_684_800_000_000, 1_893_456_000_000_000, n),
                pa.timestamp("us", "UTC"),
            ),
            pa.array(rng.integers(0, 20_000, n, dtype=np.int32), pa.date32(), mask=nulls(0.1)),
            pa.array(rng.integers(0, 86_400_000_000, n), pa.time64("us")),
            pa.array(rng.random(n) < 0.5, mask=nulls(0.1)),
            pa.array(
                [bytes(rng.integers(0, 256, 8, dtype=np.uint8)) for _ in range(n)], mask=nulls(0.5)
            ),
        ],
        names=[
            "id",
            "qty",
            "price",
            "ratio",
            "amount",
            "name",
            "created",
            "created_tz",
            "day",
            "at",
            "active",
            "blob",
        ],
    )


def test_empty_results(svc):
    empty = _mixed(0, 1)
    assert compare(svc, "Orders", [empty]) == []
    assert compare(svc, "Orders", [empty], batch_rows=5000) == []
    assert compare(svc, "Orders", [empty, _mixed(3, 2), empty], batch_rows=2) != []


@pytest.mark.parametrize("batch_rows", [1, 7, 5000])
def test_batches_across_arrow_batches(svc, batch_rows):
    batches = [_mixed(n, seed) for seed, n in enumerate([1, 700, 533, 6000])]
    compare(svc, "Orders", batches, batch_rows=batch_rows)


def test_mixed_rows(svc):
    batches = [_mixed(n, 40 + seed) for seed, n in enumerate([10_000, 1, 4_999])]
    compare(svc, "Orders", batches)


# ---- nested and repeated messages (relationship-shaped rows) ----------------------------------

CUSTOMER = pa.struct(
    [("id", pa.int64()), ("name", pa.string()), ("joined", pa.timestamp("us", "UTC"))]
)
ORDER = pa.struct(
    [
        ("id", pa.int64()),
        ("price", pa.float64()),
        ("amount", pa.decimal128(18, 2)),
        ("created", pa.timestamp("us")),
        ("day", pa.date32()),
        ("name", pa.string()),
        ("not_a_field", pa.int64()),
    ]
)


def test_many_to_one(svc):
    customers = pa.array(
        [
            {"id": 1, "name": "ann", "joined": 0},
            None,
            {"id": None, "name": None, "joined": None},
            {"id": 2, "name": "bé", "joined": -1},
        ],
        CUSTOMER,
    )
    batch = pa.RecordBatch.from_arrays(
        [pa.array([10, 11, 12, 13], pa.int64()), customers, pa.array([1, None, 3, 4], pa.int64())],
        names=["id", "customers", "customer_id"],
    )
    msgs = compare(svc, "Orders", batch, mode="node")
    assert svc.wire.Orders.FromString(msgs[2]).HasField("customers"), (
        "an empty relation is still set"
    )
    compare(svc, "Orders", batch, mode="node", batch_rows=3)


def test_one_to_many(svc):
    order = {
        "id": 1,
        "price": 1.25,
        "amount": Decimal("3.50"),
        "created": 0,
        "day": 2**31 - 1,
        "name": "x",
        "not_a_field": 9,
    }
    lists = pa.array(
        [
            [order, None, {**order, "id": None, "price": None}],
            [],
            None,
            [None],
            [{k: None for k in order}],
        ],
        pa.list_(ORDER),
    )
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, 4, 5], pa.int64()), lists, pa.array(["a", None, "c", "d", "e"])],
        names=["id", "orders", "name"],
    )
    compare(svc, "Customers", batch, mode="node")
    compare(
        svc,
        "Customers",
        pa.RecordBatch.from_arrays([lists.cast(pa.large_list(ORDER))], names=["orders"]),
        mode="node",
    )


def test_two_levels_and_random_relations(svc):
    rng = np.random.default_rng(23)
    n = 3000
    customers = [
        None
        if rng.random() < 0.1
        else {
            "id": int(rng.integers(0, 10**9)),
            "name": f"c{k}",
            "joined": int(rng.integers(0, 10**15)),
        }
        for k in range(n)
    ]
    nested_order = pa.struct([("id", pa.int64()), ("customers", CUSTOMER)])
    orders = [
        None
        if rng.random() < 0.1
        else [
            None
            if rng.random() < 0.1
            else {"id": int(rng.integers(0, 10**9)), "customers": customers[(k + j) % n]}
            for j in range(int(rng.integers(0, 5)))
        ]
        for k in range(n)
    ]
    batch = pa.RecordBatch.from_arrays(
        [pa.array(np.arange(n), pa.int64()), pa.array(orders, pa.list_(nested_order))],
        names=["id", "orders"],
    )
    compare(svc, "Customers", batch, mode="node")
    compare(svc, "Customers", batch, mode="node", batch_rows=500)


def test_relation_shape_mismatches_are_dropped(svc):
    """_dict_row_to_message keeps a dict, or a list's dict items, only."""
    one_struct = pa.array(
        [
            {
                "id": 1,
                "price": 2.0,
                "amount": None,
                "created": None,
                "day": None,
                "name": None,
                "not_a_field": None,
            }
        ],
        ORDER,
    )
    compare(
        svc, "Customers", one("orders", one_struct), mode="node"
    )  # repeated from a dict: [dict]
    compare(
        svc,
        "Orders",
        one("customers", pa.array([[{"id": 1, "name": "a", "joined": 0}]], pa.list_(CUSTOMER))),
        mode="node",
    )
    compare(svc, "Orders", one("customers", pa.array([5], pa.int64())), mode="node")
    compare(svc, "Customers", one("orders", pa.array([[1, 2]], pa.list_(pa.int64()))), mode="node")
    compare(
        svc, "Orders", one("nope", pa.array([1], pa.int64())), mode="node"
    )  # unknown key: skipped


def test_nested_value_errors_raise(svc):
    bad = pa.array([{"id": 1, "name": "a", "joined": 2**63 - 2}], CUSTOMER)  # out of datetime range
    compare(svc, "Orders", one("customers", bad), mode="node")
    bad_unused = pa.array(
        [
            [
                {
                    "id": 1,
                    "price": None,
                    "amount": None,
                    "created": None,
                    "day": 2932897,
                    "name": None,
                    "not_a_field": None,
                }
            ]
        ],
        pa.list_(ORDER),
    )
    compare(svc, "Orders", one("customers", bad_unused), mode="node")


def test_relation_in_a_query_row_is_not_ported(svc):
    refused(
        svc, "Orders", one("customers", pa.array([{"id": 1, "name": "a", "joined": 0}], CUSTOMER))
    )


def test_framed_output_is_grpc_length_prefixed(svc):
    from python_ref import rust_encoder

    batch = _mixed(100, 77)
    enc = rust_encoder(svc, "Orders", batch.schema)
    framed = enc.encode_framed(batch) + enc.finish_framed()
    msgs = python_encode(svc.wire, "Orders", batch.schema, [batch])
    expected = b"".join(b"\x00" + struct.pack(">I", len(m)) + m for m in msgs)
    assert framed == expected


def test_zz_summary():
    print(f"\n{STATS}")
    assert STATS["messages"] > 0 and STATS["identical"] == STATS["messages"]
