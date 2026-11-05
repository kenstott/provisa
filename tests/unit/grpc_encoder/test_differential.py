# Copyright (c) 2026 Kenneth Stott
# Canary: 4f918971-c235-4443-a568-4b24fbffacb4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2194, REQ-1965: the gRPC answers the Rust encoder writes, against the frozen Python path
they replaced (python_ref.py, corrected where the encoder is meant to answer otherwise).

Both encode the same Arrow batches with the same compiled service. Each pair of messages is
decoded with one descriptor and compared field by field: float and double values within a
relative tolerance of 1e-14, everything else exactly (value and Python type), repeated fields item
by item, nested messages recursively, unknown fields equal. Where the reference raises, the encoder
raises too. The serialized bytes are compared as well.

Requires provisa._encoders (rust/build-local.sh)."""

from __future__ import annotations

import datetime as dt
import json
import math
from decimal import Decimal
from typing import Any

import numpy as np
import pyarrow as pa
import pytest
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.unknown_fields import UnknownFieldSet

from provisa.grpc.server import _nodes_field
from tests.unit.grpc_encoder.python_ref import (
    Service,
    node_reference,
    has_map,
    python_encode,
    rows_encode,
    rows_of,
    rust_encode,
)

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


def assert_same(cls: Any, expected: list[bytes], sent: list[bytes]) -> None:
    assert len(expected) == len(sent), f"{len(expected)} expected messages, {len(sent)} sent"
    for k, (e, s) in enumerate(zip(expected, sent, strict=True)):
        assert_same_message(cls.FromString(e), cls.FromString(s), f"{cls.__name__}[{k}]")
    assert expected == sent, "decoded equal, but the bytes differ"


def compare(
    svc: Service,
    type_name: str,
    batches: list[pa.RecordBatch] | pa.RecordBatch,
    *,
    batch_rows: int = 0,
    masks: list | None = None,
    decode_as: Any = None,
) -> list[bytes]:
    """Encodes through both paths and compares; returns the messages. ``decode_as`` is the
    service the messages are decoded with (the wire's by default)."""
    if isinstance(batches, pa.RecordBatch):
        batches = [batches]
    try:
        expected = python_encode(svc.wire, type_name, batches, batch_rows=batch_rows, masks=masks)
    except Exception as exc:  # noqa: BLE001  # allow-blind-except: whatever the reference raises, the encoder raises
        with pytest.raises(Exception) as sent_exc:  # noqa: B017, PT011
            rust_encode(svc.wire, type_name, batches, batch_rows=batch_rows, masks=masks)
        assert not isinstance(sent_exc.value, NotImplementedError), (
            f"the reference raised {exc!r}; the encoder refused instead: {sent_exc.value!r}"
        )
        if any(has_map(b) for b in batches):
            return []
        # REQ-2195: the rows form refuses it too (reading the cells, or writing them)
        with pytest.raises(Exception):  # noqa: B017, PT011
            rows_encode(
                svc.wire,
                type_name,
                [rows_of(b) for b in batches],
                list(batches[0].schema.names),
                batch_rows=batch_rows,
                masks=masks,
            )
        return []
    sent = rust_encode(svc.wire, type_name, batches, batch_rows=batch_rows, masks=masks)
    name = f"{type_name}Batch" if batch_rows else type_name
    assert_same(getattr(decode_as or svc.wire, name), expected, sent)
    if any(has_map(b) for b in batches):
        return sent
    # REQ-2195: the same result as a row result is encoded from its rows, to the same bytes
    from_rows = rows_encode(
        svc.wire,
        type_name,
        [rows_of(b) for b in batches],
        list(batches[0].schema.names),
        batch_rows=batch_rows,
        masks=masks,
    )
    assert from_rows == sent, "the rows form answers otherwise"
    return sent


def one(name: str, array: pa.Array) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays([array], names=[name])


def wire(svc: Service, column: str, array: pa.Array) -> Any:
    """The one message the encoder writes of a one-row column."""
    (m,) = rust_encode(svc.wire, "Orders", [one(column, array)])
    return svc.wire.Orders.FromString(m)


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
@pytest.mark.parametrize("column", ["id", "qty"])  # int64, int32
def test_ints(svc, column, arrow_type):
    info = np.iinfo(arrow_type.to_pandas_dtype())
    edges = [info.min, info.max, 0, 1, -1 if info.min < 0 else 2, None]
    rng = np.random.default_rng(1)
    rand = rng.integers(info.min, info.max, 1000, dtype=arrow_type.to_pandas_dtype(), endpoint=True)
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
        Decimal("9999999999.99"),
        None,
    ]
    compare(svc, "Orders", one("id", pa.array(decimals, pa.decimal128(18, 2))))
    compare(svc, "Orders", one("id", pa.array(decimals, pa.decimal256(40, 2))))
    compare(svc, "Orders", one("qty", pa.array([Decimal("3E+2")], pa.decimal128(5, -2))))
    compare(svc, "Orders", one("qty", pa.array([Decimal("3E+9")], pa.decimal128(5, -9))))
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


def test_int_from_text(svc):
    """int(text): what a numeric's text or a mask string gives an integer field."""
    good = [" 12 ", "-1_000", "+7", "0", "9223372036854775807", "-9223372036854775808", None]
    compare(svc, "Orders", one("id", pa.array(good)))
    compare(svc, "Orders", one("id", pa.array([b"42", b" -3"])))
    for bad in ("", "1_", "1.0", "abc", "9223372036854775808", "1" * 50, "***"):
        compare(svc, "Orders", one("id", pa.array([bad])))


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


def test_doubles(svc):
    rng = np.random.default_rng(7)
    compare(svc, "Orders", one("price", pa.array(SPECIAL)))
    compare(svc, "Orders", one("price", pa.array(_float_bits(rng, 50_000))))
    compare(svc, "Orders", one("price", pa.array(rng.random(10_000, dtype=np.float32))))
    compare(svc, "Orders", one("price", pa.array(np.array([1.5, -0.0, 65504.0], np.float16))))
    compare(svc, "Orders", one("price", pa.array([True, False, None])))
    compare(
        svc, "Orders", one("price", pa.array([0, -1, 2**53 + 1, -(2**63), 2**63 - 1], pa.int64()))
    )
    compare(svc, "Orders", one("price", pa.array([2**64 - 1, 2**53 + 1], pa.uint64())))


def test_floats(svc):
    """A float field: finite doubles past float32's range one at a time (each is refused by
    name, as PostgreSQL refuses a real out of range)."""
    rng = np.random.default_rng(8)
    finite = [v for v in SPECIAL if v is None or not abs(v) > 3.4028235677973366e38]
    compare(svc, "Orders", one("ratio", pa.array(finite)))
    for v in (3.4028235677973366e38 * 1.0000001, 1e39, -1e300, 1.7976931348623157e308):
        compare(svc, "Orders", one("ratio", pa.array([v])))
    in_range = rng.normal(0, 1e30, 20_000)
    compare(svc, "Orders", one("ratio", pa.array(in_range)))
    compare(svc, "Orders", one("ratio", pa.array(_float_bits(rng, 20_000).astype(np.float32))))


@pytest.mark.parametrize("column", ["price", "ratio", "amount"])
@pytest.mark.parametrize("precision,scale", [(18, 2), (38, 10), (38, 0), (38, 38), (5, -3)])
def test_floats_from_decimal(svc, column, precision, scale):
    rng = np.random.default_rng(precision * 100 + scale)
    digits = rng.integers(1, precision + 1, 10_000)
    values: list[Decimal | None] = []
    for d in digits:
        unscaled = int("".join(str(x) for x in rng.integers(0, 10, d)))
        sign = -1 if rng.random() < 0.5 else 1
        values.append(Decimal(sign * unscaled).scaleb(-scale))
    values += [Decimal(0).scaleb(-scale), None]
    if column == "ratio" and precision - scale > 38:  # past float32: one at a time
        values = [v for v in values if v is None or abs(v) < Decimal("3.4e38")]
    compare(svc, "Orders", one(column, pa.array(values, pa.decimal128(precision, scale))))


def test_floats_from_decimal256(svc):
    values = [Decimal("1.5"), Decimal("-123456789012345678901234567890123456789012.5"), None]
    compare(svc, "Orders", one("price", pa.array(values, pa.decimal256(60, 1))))


def test_float_from_text(svc):
    """float(text): a numeric of mixed scales reaches Arrow as its text (ir_arrow.row_batch)."""
    good = ["1.5", " -2 ", "1_0.2_5", "inf", "-Infinity", "nan", "1e-400", "1E+300", None]
    compare(svc, "Orders", one("price", pa.array(good)))
    compare(svc, "Orders", one("ratio", pa.array(["1.5", "-inf", "3.4e38"])))
    for bad in ("", "abc", "1_", "1e", "***"):
        compare(svc, "Orders", one("price", pa.array([bad])))


def test_a_numeric_past_a_doubles_range_is_refused_by_name(svc):
    """J502b: a numeric beyond the double range, in a double field, is refused naming the column,
    never sent as an infinity."""
    for text in ("1E+400", "-1e309", "123456789e400"):
        with pytest.raises(
            ValueError, match=r'column "price": value .* is out of range for type double precision'
        ):
            rust_encode(svc.wire, "Orders", [one("price", pa.array([text]))])
        with pytest.raises(ValueError, match=r'column "ratio".*out of range'):
            rust_encode(svc.wire, "Orders", [one("ratio", pa.array([text]))])
        compare(svc, "Orders", one("price", pa.array([text])))
    # the infinities PostgreSQL names are sent as the infinities they are
    up, down = [
        svc.wire.Orders.FromString(m)
        for m in rust_encode(svc.wire, "Orders", [one("price", pa.array(["Infinity", "-inf"]))])
    ]
    assert (up.price, down.price) == (math.inf, -math.inf)


def test_float_from_temporal_raises(svc):
    compare(svc, "Orders", one("price", pa.array([0], pa.date32())))
    compare(svc, "Orders", one("price", pa.array([0], pa.timestamp("us"))))
    compare(svc, "Orders", one("price", pa.array([dt.timedelta(1)], pa.duration("us"))))


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
    compare(
        svc,
        "Orders",
        one("active", pa.array([dt.timedelta(0), dt.timedelta(1)], pa.duration("us"))),
    )
    compare(svc, "Orders", one("active", pa.array([[], [1], None], pa.list_(pa.int64()))))


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
    compare(svc, "Orders", one("name", pa.array(_float_bits(rng, 50_000))))
    compare(svc, "Orders", one("name", pa.array(_float_bits(rng, 10_000).astype(np.float32))))
    compare(svc, "Orders", one("name", pa.array(10.0 ** np.arange(-30, 30))))


@pytest.mark.parametrize(
    "precision,scale", [(18, 2), (38, 10), (38, 0), (38, 38), (5, -3), (10, 9)]
)
def test_str_of_decimals(svc, precision, scale):
    rng = np.random.default_rng(precision + scale + 99)
    values: list[Decimal | None] = []
    for d in rng.integers(1, precision + 1, 5_000):
        unscaled = int("".join(str(x) for x in rng.integers(0, 10, d)))
        values.append(Decimal(-unscaled if rng.random() < 0.5 else unscaled).scaleb(-scale))
    values += [Decimal(0).scaleb(-scale), None]
    compare(svc, "Orders", one("name", pa.array(values, pa.decimal128(precision, scale))))
    compare(
        svc, "Orders", one("name", pa.array(values[:500], pa.decimal256(precision + 10, scale)))
    )


def test_str_of_dates(svc):
    """A date as its text; PostgreSQL's infinities say "infinity" / "-infinity" (REQ-1965)."""
    rng = np.random.default_rng(3)
    days = [0, -1, -719162, 2932896, 2**31 - 1, -(2**31 - 1), None] + rng.integers(
        -719162, 2932897, 10_000
    ).tolist()
    compare(svc, "Orders", one("day", pa.array(days, pa.date32())))
    compare(svc, "Orders", one("day", pa.array([0, 86_400_000 * 3 + 5, None], pa.date64())))
    for bad in (2932897, -719163, -(2**31)):
        compare(svc, "Orders", one("day", pa.array([bad], pa.date32())))
    up, down = [
        svc.wire.Orders.FromString(m)
        for m in rust_encode(
            svc.wire, "Orders", [one("day", pa.array([2**31 - 1, -(2**31 - 1)], pa.date32()))]
        )
    ]
    assert (up.day, down.day) == ("infinity", "-infinity")


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

ZONES = [
    None,
    "UTC",
    "+00:00",
    "+05:30",
    "-03:00",
    "+14:00",
    "-12:45",
    "America/New_York",
    "Europe/London",
    "Australia/Lord_Howe",
    "Asia/Kathmandu",
]


@pytest.mark.parametrize("tz", ZONES)
@pytest.mark.parametrize("unit", ["s", "ms", "us"])
def test_timestamps_as_text_and_as_timestamp(svc, unit, tz):
    """str(datetime) in a string field, an instant in a Timestamp field, the ordinal in an
    integer field; a named zone read as zoneinfo reads it (J498)."""
    rng = np.random.default_rng(5)
    per = {"s": 1_000_000, "ms": 1_000, "us": 1}[unit]
    lo, hi = (
        -62_135_596_800_000_000 // per + 86_400 * 1_000_000 // per,
        253_402_300_799_999_999 // per - 86_400 * 1_000_000 // per,
    )
    values = rng.integers(lo, hi, 4_000).tolist()
    edges = [v if v is None or unit == "us" else v // per for v in TS_EDGES_US]
    arrow_type = pa.timestamp(unit, tz)
    for column in ("name", "created", "created_tz", "id"):
        compare(svc, "Orders", one(column, pa.array(values, arrow_type)))
        for v in edges:  # one at a time: an out-of-range edge raises for its own batch only
            compare(svc, "Orders", one(column, pa.array([v], arrow_type)))


def test_named_zones_after_2037_keep_their_daylight_time(svc):
    """J498: zoneinfo applies a zone's POSIX rule past its last transition; pytz (pyarrow's
    to_pylist) had New York on standard time in July 2050."""
    july_2050 = 2_541_024_000_000_000  # 2050-07-10T00:00:00Z
    row = wire(svc, "name", pa.array([july_2050], pa.timestamp("us", "America/New_York")))
    assert row.name == "2050-07-09 20:00:00-04:00"


def test_nanosecond_timestamps(svc):
    """A nanosecond unit: its nanoseconds kept in a Timestamp field, and str() as pandas'
    Timestamp writes it (the fraction's nine digits when they are not all of microseconds)."""
    values = [0, 1, 1_700_000_000_123_456_789, 1_700_000_000_123_456_000, -1, None]
    for column in ("name", "created", "created_tz", "id"):
        for tz in (None, "UTC", "+05:30"):
            compare(svc, "Orders", one(column, pa.array(values, pa.timestamp("ns", tz))))
    row = wire(svc, "created", pa.array([1_700_000_000_123_456_789], pa.timestamp("ns")))
    assert (row.created.seconds, row.created.nanos) == (1_700_000_000, 123_456_789)


def test_timestamp_from_other_types(svc):
    compare(svc, "Orders", one("created", pa.array([0], pa.date32())))  # fromisoformat(date)
    compare(svc, "Orders", one("created", pa.array([1], pa.int64())))
    compare(svc, "Orders", one("created", pa.array([None], pa.date32())))


def test_timestamp_from_text(svc):
    """datetime.fromisoformat(text): naive as UTC, an offset moved to UTC."""
    good = [
        "2024-01-02T03:04:05",
        "2024-01-02 03:04:05.5",
        "2024-01-02",
        "20240102",
        "2024-01-02T03",
        "2024-01-02T0304",
        "2024-01-02T03:04:05,123",
        "2024-01-02T03:04:05.1234567",
        "2024-01-02T03:04:05Z",
        "2024-01-02T03:04:05+05:30",
        "2024-01-02T03:04:05-0800",
        "2024-01-02T03:04:05.25+05",
        "2024-W01-1",
        "2024W017",
        "0001-01-01T00:00:00",
        "9999-12-31T23:59:59.999999",
        None,
    ]
    compare(svc, "Orders", one("created", pa.array(good)))
    for bad in (
        "",
        "2024-13-01",
        "2023-02-29",
        "2024-01-01T24:00",
        "x",
        "0001-01-01T00:00:00+01:00",
    ):
        compare(svc, "Orders", one("created", pa.array([bad])))


def test_times(svc):
    rng = np.random.default_rng(9)
    us = [0, 1, 86_399_999_999, None] + rng.integers(0, 86_400_000_000, 5_000).tolist()
    compare(svc, "Orders", one("at", pa.array(us, pa.time64("us"))))
    compare(svc, "Orders", one("at", pa.array([0, 1, 86_399_999, None], pa.time32("ms"))))
    compare(svc, "Orders", one("at", pa.array([0, 1, 86_399, None], pa.time32("s"))))
    compare(svc, "Orders", one("at", pa.array([0, 1001, 86_399_999_999_999], pa.time64("ns"))))


def test_intervals(svc):
    rng = np.random.default_rng(13)
    n = 10_000
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
    for column in ("span", "blob"):  # ISO 8601 in a string field as in a bytes field (REQ-1965)
        compare(svc, "Orders", one(column, pa.array(random_part, pa.month_day_nano_interval())))
    for v in values[n:]:  # one at a time: a month-less one past timedelta's range raises
        compare(svc, "Orders", one("span", pa.array([v], pa.month_day_nano_interval())))
    for edge in ((0, 999_999_999, 86_400_000_000_000), (0, -999_999_999, -1000)):
        compare(svc, "Orders", one("span", pa.array([edge], pa.month_day_nano_interval())))


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_durations(svc, unit):
    rng = np.random.default_rng(17)
    per = {"s": 1_000_000, "ms": 1_000, "us": 1, "ns": 1}[unit]
    limit = 86_399_999_999_999_999_999 // per  # past timedelta.max for s and ms
    values = [0, -1, 1, -1001, 1001, None] + rng.integers(-(10**15), 10**15, 5_000).tolist()
    for column in ("span", "blob"):
        compare(svc, "Orders", one(column, pa.array(values, pa.duration(unit))))
    for edge in (
        999_999_999 * 86_400_000_000 // per,
        -999_999_999 * 86_400_000_000 // per,
        min(limit, 2**63 - 1),
        -(2**63),
    ):
        edge = max(min(edge, 2**63 - 1), -(2**63))  # an int64 holds no more
        if unit == "ns" and edge == -(2**63):
            # pandas reads this one as its NaT sentinel ("NaT"); it is a duration like any other
            span = wire(svc, "span", pa.array([edge], pa.duration(unit))).span
            assert span == "-P106751DT23H47M16.854775S"
            continue
        compare(svc, "Orders", one("span", pa.array([edge], pa.duration(unit))))


def test_bytes_in_a_string_field_are_postgres_hex(svc):
    """REQ-1965: bytes are never written as a Python b'...' literal."""
    raw = [b"", b"ab", b"\x00\xff\x80", bytes(range(256)), None]
    compare(svc, "Orders", one("name", pa.array(raw)))
    compare(svc, "Orders", one("name", pa.array(raw, pa.binary_view())))
    assert wire(svc, "name", pa.array([b"ab"])).name == "\\x6162"


# ---- arrays, structs and maps in a string field: JSON (REQ-1965) --------------------------------

STRUCT = pa.struct(
    [
        ("k", pa.float64()),
        ("on", pa.date32()),
        ("at", pa.timestamp("us", "UTC")),
        ("n", pa.int64()),
        ("d", pa.decimal128(10, 2)),
        ("b", pa.binary()),
        ("s", pa.string()),
        ("span", pa.duration("us")),
        ("t", pa.time64("us")),
        ("naive", pa.timestamp("us")),
        ("zoned", pa.timestamp("us", "America/New_York")),
    ]
)


def test_arrays_structs_and_maps_are_json(svc):
    lists = [["a", None, 'é"\\\n\x01'], [], None, ["x" * 3]]
    compare(svc, "Orders", one("payload", pa.array(lists, pa.list_(pa.string()))))
    compare(
        svc,
        "Orders",
        one("payload", pa.array([[[1, 2], []], [[None]]], pa.list_(pa.list_(pa.int64())))),
    )
    compare(
        svc,
        "Orders",
        one("payload", pa.array([[1.5, -0.0, 1e300], [5e-324]], pa.large_list(pa.float64()))),
    )
    compare(svc, "Orders", one("payload", pa.array([[1, 2, 3], None], pa.list_(pa.int64(), 3))))
    compare(svc, "Orders", one("payload", pa.array([[True, False]], pa.list_(pa.bool_()))))
    structs = [
        {
            "k": 1.5,
            "on": dt.date(2026, 1, 2),
            "at": dt.datetime(2026, 1, 2, 3, 4, 5, 600000, tzinfo=dt.timezone.utc),
            "n": None,
            "d": Decimal("1.10"),
            "b": b"\x00\xff",
            "s": "x",
            "span": dt.timedelta(days=1, seconds=5),
            "t": dt.time(1, 2, 3, 400),
            "naive": dt.datetime(2026, 1, 2, 3, 4, 5),
            "zoned": dt.datetime(2026, 7, 2, 3, 4, 5, 120000, tzinfo=dt.timezone.utc),
        },
        None,
        {k: None for k in STRUCT.names},
    ]
    compare(svc, "Orders", one("payload", pa.array(structs, STRUCT)))
    compare(svc, "Orders", one("payload", pa.array([structs[:1], [], None], pa.list_(STRUCT))))
    maps = [[("a", 1), ("b", None)], [], None, [("a", 1), ("a", 2)]]
    compare(svc, "Orders", one("payload", pa.array(maps, pa.map_(pa.string(), pa.int64()))))
    keyed = [[(1, "x"), (2, "y")]]
    compare(svc, "Orders", one("payload", pa.array(keyed, pa.map_(pa.int64(), pa.string()))))
    nested_map = [[("m", [("k", [1, 2])])]]
    compare(
        svc,
        "Orders",
        one(
            "payload",
            pa.array(nested_map, pa.map_(pa.string(), pa.map_(pa.string(), pa.list_(pa.int64())))),
        ),
    )
    # the same JSON in a bytes field
    compare(svc, "Orders", one("blob", pa.array(lists, pa.list_(pa.string()))))


def test_json_text_exactly(svc):
    row = wire(
        svc,
        "payload",
        pa.array(
            [[{"k": 1.5, "on": dt.date(2026, 1, 2), "n": None}]],
            pa.list_(pa.struct([("k", pa.float64()), ("on", pa.date32()), ("n", pa.int64())])),
        ),
    )
    assert row.payload == '[{"k":1.5,"on":"2026-01-02","n":null}]'
    row = wire(svc, "payload", pa.array([[("a", 1), ("b", 2)]], pa.map_(pa.string(), pa.int64())))
    assert row.payload == '{"a":1,"b":2}'


def test_nan_or_an_infinity_inside_json_is_refused_by_name(svc):
    for bad in (float("nan"), float("inf")):
        compare(svc, "Orders", one("payload", pa.array([[1.0, bad]])))
        with pytest.raises(ValueError, match='column "payload"'):
            rust_encode(svc.wire, "Orders", [one("payload", pa.array([[1.0, bad]]))])


def test_nested_values_out_of_range_raise(svc):
    compare(svc, "Orders", one("payload", pa.array([[2**31 - 1]], pa.list_(pa.date32()))))
    compare(
        svc,
        "Orders",
        one("payload", pa.array([{"t": 2**63 - 2}], pa.struct([("t", pa.timestamp("us"))]))),
    )


# ---- read masks: a JSON sub-path selection (REQ-803) -----------------------------------------


def test_read_mask_selects_inside_json_text(svc):
    docs = [
        '{"a": 1, "b": {"c": 2, "d": [1, 2]}, "e": "é"}',
        '[{"a": 1, "x": 2}, 3, {"b": {"c": null}}]',
        '{"a": 1, "a": 2}',
        '{"a": 1.0, "b": {"c": 1e400}}',
        '"text"',
        "5",
        None,
    ]
    mask = {"a": None, "b": {"c": None}}
    compare(svc, "Orders", one("payload", pa.array(docs)), masks=[mask])
    compare(svc, "Orders", one("payload", pa.array(["{not json"])), masks=[mask])


def test_read_mask_selects_inside_a_struct(svc):
    inner = pa.struct([("c", pa.int64()), ("d", pa.string())])
    struct = pa.struct([("a", pa.int64()), ("b", inner), ("e", pa.list_(inner))])
    values = [{"a": 1, "b": {"c": 2, "d": "x"}, "e": [{"c": 3, "d": "y"}]}, None]
    for mask in ({"a": None}, {"b": {"c": None}}, {"e": {"d": None}}, {"zz": None}):
        compare(svc, "Orders", one("payload", pa.array(values, struct)), masks=[mask])
    compare(
        svc,
        "Orders",
        one("payload", pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int64()))),
        masks=[{"b": None}],
    )


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
    compare(svc, "Orders", one("blob", pa.array([0, 2**31 - 1], pa.date32())))
    compare(
        svc, "Orders", one("blob", pa.array([1_700_000_000_000_000], pa.timestamp("us", "UTC")))
    )


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


def test_two_columns_of_one_field_write_the_last_value(svc):
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, None], pa.int64()), pa.array([None, 20, None], pa.int64())],
        names=["id", "ID"],
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
            pa.array([[int(v), None] for v in rng.integers(0, 9, n)], pa.list_(pa.int64())),
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
            "payload",
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


def test_a_sliced_batch(svc):
    batch = _mixed(1_000, 9).slice(137, 400)
    compare(svc, "Orders", batch)


# ---- a group's nodes (REQ-1405) -----------------------------------------------------------------


def _nodes(svc: Service, nodes: list[dict]) -> None:
    """encode_nodes against the frozen _dict_row_to_message, for nodes as JSON gives them."""
    nodes = json.loads(json.dumps(nodes))
    group = svc.wire.CustomersGroupByRow
    sent = _nodes_field(group, svc.wire.Customers, nodes)
    expected = node_reference(svc.wire, "Customers", nodes)
    decoded = group.FromString(sent)
    assert_same(svc.wire.Customers, expected, [n.SerializeToString() for n in decoded.nodes])
    reference = group(nodes=[svc.wire.Customers.FromString(e) for e in expected])
    assert sent == reference.SerializeToString()


def test_group_nodes(svc):
    _nodes(svc, [])
    _nodes(svc, [{"id": 1, "name": "ann", "joined": "2024-01-02T03:04:05.5+00:00"}, {}])
    _nodes(
        svc,
        [
            {
                "id": 1,
                "name": None,
                "not_a_field": 9,
                "orders": [
                    {
                        "id": 7,
                        "payload": {"k": [1, 2.5]},
                        "price": 1.25,
                        "created": "2024-01-02T03:04:05",
                    },
                    {"id": 8, "blob": "\\x01", "active": True, "span": "P1D"},
                    5,
                    None,
                ],
            },
            {"id": 2, "orders": {"id": 9}},
            {"id": 3, "orders": []},
        ],
    )
    _nodes(svc, [{"name": 10**30, "id": "12"}])


def test_group_node_relations(svc):
    nodes = [
        {"id": 1, "customers": {"id": 5, "name": "c"}},
        {"id": 2, "customers": {}},
        {"id": 3, "customers": [{"id": 1}]},
    ]
    nodes = json.loads(json.dumps(nodes))
    group = svc.wire.OrdersGroupByRow
    sent = group.FromString(_nodes_field(group, svc.wire.Orders, nodes))
    expected = node_reference(svc.wire, "Orders", nodes)
    assert_same(svc.wire.Orders, expected, [n.SerializeToString() for n in sent.nodes])
    assert sent.nodes[1].HasField("customers"), "an empty relation is still set"


def test_group_node_errors_raise(svc):
    for bad in ([{"id": "x"}], [{"price": float("nan")}], [{"id": [1]}], [{"id": 10**30}]):
        nodes = json.loads(json.dumps(bad))
        try:
            node_reference(svc.wire, "Orders", nodes)
        except Exception:  # noqa: BLE001  # allow-blind-except: whatever the reference raises
            with pytest.raises(Exception):  # noqa: B017, PT011
                _nodes_field(svc.wire.OrdersGroupByRow, svc.wire.Orders, nodes)
            continue
        group = svc.wire.OrdersGroupByRow
        sent = group.FromString(_nodes_field(group, svc.wire.Orders, nodes))
        assert_same(
            svc.wire.Orders,
            node_reference(svc.wire, "Orders", nodes),
            [n.SerializeToString() for n in sent.nodes],
        )
