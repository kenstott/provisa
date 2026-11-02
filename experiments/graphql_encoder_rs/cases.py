# Copyright (c) 2026 Kenneth Stott
# Canary: 2c8e5b1d-9f4a-4c6e-b2d7-5a1e3f9c8d40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The value corpus every surface's differential test runs: one Arrow array per type, at its
extremes, NULLs and random values, and the column modes a type is read under (REQ-1867: a json or
jsonb column's text is JSON, an integer-typed column is a number)."""

from __future__ import annotations

import random
import struct
import uuid
from datetime import date
from decimal import Decimal

import pyarrow as pa
from provisa.compiler.sql_types import ColumnRef

rng = random.Random(504)


def C(
    field,
    nested_in=None,
    cardinality=None,
    is_agg=False,
    column=None,
    *,
    json=False,
    json_text=(),
    integer=False,
):
    return ColumnRef(
        None,
        column or field,
        field,
        nested_in,
        cardinality,
        is_agg,
        json=json,
        json_text=frozenset(json_text),
        integer=integer,
    )


def rand_f64(n):
    return [struct.unpack("<d", rng.getrandbits(64).to_bytes(8, "little"))[0] for _ in range(n)]


def rand_f32(n):
    return [struct.unpack("<f", rng.getrandbits(32).to_bytes(4, "little"))[0] for _ in range(n)]


F64_SPECIAL = [
    0.0, -0.0, float("nan"), float("inf"), float("-inf"), 5e-324, 2.2250738585072014e-308,
    1.7976931348623157e308, 1e16, 1e15, 9999999999999998.0, 1.5e16, 1e-5, 1e-6, 1.2345e-5,
    9.99e-6, 1e-4, 0.1, 123.456, -1.5e-7, 1.0, -1.0, 2.0**53, 2.0**64, 1e22, 1e23, None,
]  # fmt: skip


def rand_dec(p, s, n):
    out = []
    for _ in range(n):
        digits = rng.randint(1, p)
        c = rng.randint(0, 10**digits - 1) * rng.choice((1, -1))
        out.append(Decimal(c).scaleb(-s))
    return out


TEXT = [
    "", "a", "é😀 ", "ctl\x00\x01\x1f\x7f\b\f\n\r\t", '"\\/', "{", "[", "{}", "[]", '{"a":1}',
    "[1,2]", '{"a": "[1]"}', '{"a":1} x', "[1,]", "[01]", "[NaN, Infinity, -Infinity]", "[1e400]",
    "[-0]", "[-0.0]", "[1E5, 1e-7, 2.50, 1.0]", '{"a":1,"a":2,"b":3}', '{"b":1,"a":2}',
    '["\\ud83d\\ude00"]', '["\\u00e9\\n\\/\\b"]', '[" \\t"]', '["\x01"]', '["\\x"]', " [1]",
    "[1] ", "[1]\n", "[true,false,null]", "[[[[]]]]", '{"k": "{\\"n\\": [\\"[1]\\"]}"}',
    "[12345678901234567890]", "[-9223372036854775808]", "[123456789012345678901234567890]",
    "x" * 5000, '{"a": {"b": [1, {"c": null}]}}', '["\\u0041\\u00DF"]', '{"":1}', "[1, 2",
    '{"a" 1}', "{1: 2}", "[1.5e+3, 1.5E-3]", "[.5]", "[1.]", "[+1]", "[-]", "[nul]", "[NaN1]",
    "[-NaN]", "5", "-0", "1.50", '"s"', "true", "null", " 7 ", "1e400", "[0.1, 0.10, 1e1, 10]",
    None,
]  # fmt: skip

#: JSON texts only: what a json/jsonb column holds (every one parses, or is refused by name)
JSON_TEXT = [t for t in TEXT if t is not None and not t.startswith("x")] + [None]


def _uuids(n):
    return [uuid.UUID(int=rng.getrandbits(128)).bytes for _ in range(n)]


def scalar_cases():
    """(name, array, modes): modes among "convert", "json", "integer"."""
    ts_vals = [
        0,
        1,
        -1,
        1_000_000,
        -62135596800_000_000,
        253402300799_999_999,
        2**63 - 1,
        -(2**63 - 1),
        None,
    ]
    ts_vals += [rng.randint(-62135596800_000_000, 253402300799_999_999) for _ in range(1500)]
    yield "bool", pa.array([True, False, None, True, False]), ("convert",)
    for t, lo, hi in [
        (pa.int8(), -128, 127),
        (pa.int16(), -(2**15), 2**15 - 1),
        (pa.int32(), -(2**31), 2**31 - 1),
        (pa.int64(), -(2**63), 2**63 - 1),
        (pa.uint8(), 0, 255),
        (pa.uint16(), 0, 2**16 - 1),
        (pa.uint32(), 0, 2**32 - 1),
        (pa.uint64(), 0, 2**64 - 1),
    ]:
        yield (
            str(t),
            pa.array([lo, hi, 0, None] + [rng.randint(lo, hi) for _ in range(300)], t),
            ("convert", "integer"),
        )
    yield "float64-special", pa.array(F64_SPECIAL, pa.float64()), ("convert",)
    yield "float64-random", pa.array(rand_f64(50_000), pa.float64()), ("convert",)
    yield (
        "float32-random",
        pa.array(rand_f32(50_000) + [None, 0.1, -0.0], pa.float32()),
        ("convert",),
    )
    yield "float16", pa.array([0.1, 1.5, -0.0, 65504.0, None], pa.float16()), ("convert",)
    for p, s in [
        (18, 2),
        (38, 10),
        (10, 8),
        (38, 7),
        (12, 0),
        (5, -2),
        (20, 6),
        (30, 15),
        (38, 20),
        (38, 0),
    ]:
        vals = rand_dec(p, s, 1500)
        yield (
            f"decimal({p},{s})",
            pa.array(vals + [Decimal(0).scaleb(-s), None], pa.decimal128(p, s)),
            ("convert",) if s else ("convert", "integer"),
        )
    yield (
        "decimal-tiny",
        pa.array(
            [Decimal(x) for x in ("1E-9", "1.2E-8", "0E-9", "1E-7", "0.000001", "-0.000000001")],
            pa.decimal128(10, 9),
        ),
        ("convert",),
    )
    yield (
        "decimal-big",
        pa.array(
            [Decimal(2**53 + 1), Decimal(2**64), Decimal(10**37), Decimal(-(10**37))],
            pa.decimal128(38, 0),
        ),
        ("convert", "integer"),
    )
    yield "decimal256", pa.array(rand_dec(70, 5, 300) + [None], pa.decimal256(76, 5)), ("convert",)
    yield (
        "decimal256-int",
        pa.array([Decimal(v) for v in (0, 1, -1, 2**200, -(10**75))], pa.decimal256(76, 0)),
        ("convert", "integer"),
    )
    yield (
        "decimal32",
        pa.array([Decimal("1.25"), Decimal("-99.99"), None], pa.decimal32(4, 2)),
        ("convert",),
    )
    yield "decimal64", pa.array([Decimal("12345678.901"), None], pa.decimal64(18, 3)), ("convert",)
    yield "utf8", pa.array(TEXT, pa.string()), ("convert",)
    yield "large_utf8", pa.array(TEXT, pa.large_string()), ("convert",)
    yield "string_view", pa.array(TEXT, pa.string_view()), ("convert",)
    yield "json-utf8", pa.array(JSON_TEXT, pa.string()), ("json",)
    yield "json-large", pa.array(JSON_TEXT, pa.large_string()), ("json",)
    yield "enum", pa.array(["red", "green", None, "red", "[1]"]).dictionary_encode(), ("convert",)
    yield "dict<int>", pa.array([5, 6, None, 5]).dictionary_encode(), ("convert", "integer")
    yield (
        "binary",
        pa.array(
            [b"", b"\x00\xff", None] + [rng.randbytes(rng.randint(0, 40)) for _ in range(100)]
        ),
        ("convert",),
    )
    yield "large_binary", pa.array([b"", b"\x01\x02", None], pa.large_binary()), ("convert",)
    yield "binary_view", pa.array([b"", b"\x01'\"", None], pa.binary_view()), ("convert",)
    yield (
        "fixed_binary",
        pa.array([b"\x00\x01\x02\x03", None, b"\xff\xff\xff\xff"], pa.binary(4)),
        ("convert",),
    )
    yield "uuid", pa.array(_uuids(30) + [None], pa.uuid()), ("convert",)
    yield (
        "date32",
        pa.array(
            [0, -1, 1, -719162, 2932896, 2**31 - 1, -(2**31 - 1), None]
            + [rng.randint(-719162, 2932896) for _ in range(1000)],
            pa.date32(),
        ),
        ("convert",),
    )
    yield (
        "date64",
        pa.array([0, -1, 86_400_000 * 3 + 5, -86_400_000 * 1000 - 1, None], pa.date64()),
        ("convert",),
    )
    for unit, div in [("s", 1_000_000), ("ms", 1_000), ("us", 1)]:
        vals = [v if v is None or abs(v) >= 2**63 - 1 else v // div for v in ts_vals]
        if div != 1:
            vals = [v for v in vals if v is None or abs(v) < 2**62]
        for tz in [
            None,
            "UTC",
            "+05:30",
            "-03:00",
            "America/New_York",
            "Asia/Kolkata",
            "Europe/London",
            "Australia/Lord_Howe",
        ]:
            shift = 15 * 3600 * 10**6
            v2 = [
                v
                for v in vals
                if v is None
                or abs(v) == 2**63 - 1
                or -62135596800_000_000 + shift < v * div < 253402300799_999_999 - shift
            ]
            yield f"timestamp[{unit},{tz}]", pa.array(v2, pa.timestamp(unit, tz=tz)), ("convert",)
    ns = [0, 1, -1, 123, 10**18, -(10**18), 2**63 - 1, -(2**63), -(2**63) + 1, None] + [
        rng.randint(-(2**63) + 1, 2**63 - 1) for _ in range(300)
    ]
    for tz in [None, "UTC", "America/New_York"]:
        yield f"timestamp[ns,{tz}]", pa.array(ns, pa.timestamp("ns", tz=tz)), ("convert",)
    day = 86_400
    yield (
        "time32[s]",
        pa.array([0, day - 1, None] + [rng.randrange(day) for _ in range(200)], pa.time32("s")),
        ("convert",),
    )
    yield (
        "time32[ms]",
        pa.array(
            [0, day * 1000 - 1, None] + [rng.randrange(day * 1000) for _ in range(200)],
            pa.time32("ms"),
        ),
        ("convert",),
    )
    yield (
        "time64[us]",
        pa.array(
            [0, 1, day * 10**6 - 1, None] + [rng.randrange(day * 10**6) for _ in range(200)],
            pa.time64("us"),
        ),
        ("convert",),
    )
    yield (
        "time64[ns]",
        pa.array(
            [0, 1, 999, 1000, 1001, day * 10**9 - 1, None]
            + [rng.randrange(day * 10**9) for _ in range(200)],
            pa.time64("ns"),
        ),
        ("convert",),
    )
    td_max = 999_999_999 * day * 10**6 + day * 10**6 - 1
    for unit, div in [("s", 10**6), ("ms", 10**3), ("us", 1)]:
        hi = min(td_max // div, 2**63 - 1)
        lo = max(-(999_999_999 * day * 10**6 // div), -(2**63 - 1))
        vals = (
            [0, 1, -1, hi, lo, None]
            + [rng.randint(lo, hi) for _ in range(300)]
            + [rng.randint(-(10**6), 10**6) for _ in range(100)]
        )
        yield f"duration[{unit}]", pa.array(vals, pa.duration(unit)), ("convert",)
    yield (
        "duration[ns]",
        pa.array(
            [0, 1, -1, 999, -1001, 2**63 - 1, -(2**63), None]
            + [rng.randint(-(2**63) + 1, 2**63 - 1) for _ in range(200)],
            pa.duration("ns"),
        ),
        ("convert",),
    )
    mdn = [
        pa.MonthDayNano([0, 0, 0]), pa.MonthDayNano([1, 2, 3_000]), pa.MonthDayNano([-1, -2, -3_000]),
        pa.MonthDayNano([14, 0, 0]), pa.MonthDayNano([-14, 3, -1]), pa.MonthDayNano([0, 0, -1]),
        pa.MonthDayNano([0, 0, 90_000 * 10**9]), pa.MonthDayNano([0, 999_999_999, 0]), None,
    ]  # fmt: skip
    for _ in range(1500):
        mdn.append(
            pa.MonthDayNano(
                [
                    rng.randint(-(2**31), 2**31 - 1),
                    rng.randint(-(2**31), 2**31 - 1),
                    rng.randint(-(2**63), 2**63 - 1),
                ]
            )
        )
    yield "interval", pa.array(mdn, pa.month_day_nano_interval()), ("convert",)
    yield "null", pa.array([None, None], pa.null()), ("convert", "json", "integer")
    yield (
        "list<int64>",
        pa.array([[1, 2], [], None, [None, 3], [1, 2]], pa.list_(pa.int64())),
        ("convert",),
    )
    yield (
        "list<float64>",
        pa.array([[0.1, float("nan")], [-0.0], [0.0], [float("inf")]], pa.list_(pa.float64())),
        ("convert",),
    )
    yield (
        "list<utf8>",
        pa.array([["a", "[1]", '{"x":"[2]"}'], ["{bad"], None], pa.list_(pa.string())),
        ("convert",),
    )
    yield (
        "list<decimal>",
        pa.array(
            [[Decimal("1.50"), Decimal("2.00")], [Decimal("0.00")]], pa.list_(pa.decimal128(5, 2))
        ),
        ("convert",),
    )
    yield "list<date32>", pa.array([[date(2020, 1, 2), None]], pa.list_(pa.date32())), ("convert",)
    yield (
        "list<ts>",
        pa.array([[0, 1_500_000]], pa.list_(pa.timestamp("us", tz="UTC"))),
        ("convert",),
    )
    yield (
        "list<ts ny>",
        pa.array([[0, 1_500_000]], pa.list_(pa.timestamp("ms", tz="America/New_York"))),
        ("convert",),
    )
    yield "list<ts ns>", pa.array([[0, 1_500]], pa.list_(pa.timestamp("ns"))), ("convert",)
    yield (
        "list<interval>",
        pa.array([[pa.MonthDayNano([1, 2, 3])], None], pa.list_(pa.month_day_nano_interval())),
        ("convert",),
    )
    yield "list<duration>", pa.array([[5, -5]], pa.list_(pa.duration("us"))), ("convert",)
    yield "list<binary>", pa.array([[b"\x00", None]], pa.list_(pa.binary())), ("convert",)
    yield (
        "list<dict>",
        pa.array([["a", "b", "a"]], pa.list_(pa.dictionary(pa.int32(), pa.string()))),
        ("convert",),
    )
    st = pa.struct(
        [
            ("a", pa.int64()),
            ("b", pa.string()),
            ("c", pa.decimal128(6, 3)),
            ("d", pa.list_(pa.int32())),
            ("t", pa.time64("us")),
            ("u", pa.duration("us")),
            ("e", pa.binary()),
        ]
    )
    yield (
        "struct",
        pa.array(
            [
                {
                    "a": 1,
                    "b": "[1]",
                    "c": Decimal("1.500"),
                    "d": [1],
                    "t": 3_600_000_001,
                    "u": -5,
                    "e": b"\x01",
                },
                None,
                {"a": None, "b": None, "c": None, "d": None, "t": None, "u": None, "e": None},
            ],
            st,
        ),
        ("convert", "json"),
    )
    yield (
        "list<struct>",
        pa.array(
            [[{"a": 1, "b": "x", "c": Decimal("0.001"), "d": [], "t": 0, "u": 0, "e": b""}], []],
            pa.list_(st),
        ),
        ("convert",),
    )
    yield "large_list", pa.array([[1], None], pa.large_list(pa.int8())), ("convert",)
    yield "fixed_size_list", pa.array([[1, 2], [3, None]], pa.list_(pa.int16(), 2)), ("convert",)
    yield "list_view", pa.array([[1, 2], None, []], pa.list_view(pa.int64())), ("convert",)
    yield "list<list>", pa.array([[[1], [], None]], pa.list_(pa.list_(pa.int64()))), ("convert",)
    ext = pa.ExtensionArray.from_storage(
        pa.uuid(), pa.array([uuid.UUID(int=5).bytes, None], pa.binary(16))
    )
    yield "list<uuid>", pa.ListArray.from_arrays(pa.array([0, 2], pa.int32()), ext), ("convert",)
    yield "list<null>", pa.array([[None]], pa.list_(pa.null())), ("convert",)
    yield (
        "map",
        pa.array([[("a", 1), ("b", None)], None, []], pa.map_(pa.string(), pa.int64())),
        ("convert",),
    )
    yield (
        "map<str,decimal/date>",
        pa.array(
            [[("a", {"d": Decimal("1.50"), "t": date(2020, 1, 1)})]],
            pa.map_(pa.string(), pa.struct([("d", pa.decimal128(5, 2)), ("t", pa.date32())])),
        ),
        ("convert",),
    )
    yield (
        "map<str,ts>",
        pa.array([[("a", 1_000_500)]], pa.map_(pa.string(), pa.timestamp("us", tz="UTC"))),
        ("convert",),
    )
    yield (
        "dense_union",
        pa.UnionArray.from_dense(
            pa.array([0, 1, 0, 1], pa.int8()),
            pa.array([0, 0, 1, 1], pa.int32()),
            [pa.array([1, None]), pa.array(["x", "y"])],
        ),
        ("convert",),
    )
    yield (
        "sparse_union",
        pa.UnionArray.from_sparse(
            pa.array([0, 1, 1], pa.int8()),
            [pa.array([1.0, 2.0, 3.0]), pa.array([True, False, None])],
        ),
        ("convert",),
    )
    yield (
        "run_end_encoded",
        pa.RunEndEncodedArray.from_arrays([2, 3, 5], [1, None, 2]),
        ("convert",),
    )


CASES = list(scalar_cases())


def orders():
    return pa.table(
        {
            "id": pa.array([1, 1, 1, 2, 2, 3, 4, 4], pa.int64()),
            "status": ["new", "new", "new", "paid", "paid", None, "new", "new"],
            "item_id": pa.array([10, 11, 10, 20, None, None, 40, 41], pa.int64()),
            "item_qty": pa.array([1, 2, 1, 3, None, None, 4, 4], pa.int32()),
            "part_id": pa.array([100, 101, 102, 200, None, None, None, 400], pa.int64()),
            "prod_name": ["p", "q", "p", None, None, None, "s", "s"],
            "cust_id": pa.array([7, 7, 7, 8, 8, None, 9, 9], pa.int64()),
            "cust_name": ["A", "A", "A", "B", "B", None, "C", "C"],
            "addr_city": ["X", "X", "X", None, None, None, "Z", "Z"],
            "amount": pa.array(
                [Decimal("1.50")] * 3 + [Decimal("2.25")] * 2 + [None] + [Decimal("9.00")] * 2,
                pa.decimal128(10, 2),
            ),
            "doc": ['{"k": 1.10}'] * 3 + ["[1, 2]"] * 2 + [None] + ['"t"'] * 2,
        }
    )


def project(table, plan):
    return pa.table({f"c{i}": table[c.column] for i, c in enumerate(plan)})


def rand_table(r: random.Random, n: int):
    def col(kind):
        def pick(pool):
            return [None if r.random() < 0.15 else r.choice(pool) for _ in range(n)]

        if kind == "int":
            return pa.array(pick([0, 1, 2, 3, 4]), pa.int64())
        if kind == "float":
            return pa.array(
                pick([0.0, -0.0, float("nan"), 1.5, 2.25, float("inf"), 1.0]), pa.float64()
            )
        if kind == "dec":
            return pa.array(
                pick([Decimal("0.00"), Decimal("1.50"), Decimal("1.00")]), pa.decimal128(5, 2)
            )
        if kind == "json":
            return pa.array(
                pick(
                    [
                        '{"a":1}',
                        '{"a":1.0}',
                        '{"a":true}',
                        "[-0.0]",
                        "[0.0]",
                        '"x"',
                        "[1]",
                        '{"b":2,"a":1}',
                        '{"a":1,"b":2}',
                        '"[1]"',
                        "1",
                        "1.0",
                        "true",
                    ]
                ),
                pa.string(),
            )
        if kind == "text":
            return pa.array(pick(["[1]", '{"a":1}', "x", ""]), pa.string())
        if kind == "struct":
            return pa.array(
                pick([{"a": 1, "c": "x"}, {"a": 2, "c": None}, {"a": None, "c": "[1]"}]),
                pa.struct([("a", pa.int64()), ("c", pa.string())]),
            )
        if kind == "list":
            return pa.array(pick([[1], [], [1, 2], [None]]), pa.list_(pa.int64()))
        if kind == "date":
            return pa.array(pick([0, 1, 2**31 - 1]), pa.date32())
        return pa.array(pick(list("abc")), pa.string())

    kinds = ["int", "float", "dec", "json", "text", "struct", "list", "date", "str"]
    return {f"{k}{i}": (k, col(k)) for i, k in enumerate(r.choices(kinds, k=8))}
