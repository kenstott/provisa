"""Differential test: the Rust encoder against the product's Python path (python_ref), on the same
Arrow batches and selection plan. Both outputs are parsed and compared structurally: key order,
value types and every value exactly, floats within a relative tolerance of 1e-14. A case where
both raise passes; one raising alone fails. Byte identity is tallied as well.

Not collected by CI (it lives under experiments/). Run it as RESULTS.md says.
"""

from __future__ import annotations

import json
import math
import random
import struct
import uuid
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pytest

import graphql_encoder_rs as rs
import python_ref as ref
from provisa.compiler.sql_types import ColumnRef

REL_TOL = 1e-14
TALLY = {"cases": 0, "byte_identical": 0, "both_raise": 0}


class Obj(list):
    """A parsed JSON object: its (key, value) pairs in order."""


def parse(b: bytes):
    return json.loads(b, object_pairs_hook=Obj)


def same(a, b, path="$"):
    if isinstance(a, float) or isinstance(b, float):
        assert type(a) is type(b), f"{path}: {a!r} vs {b!r}"
        assert math.isclose(a, b, rel_tol=REL_TOL, abs_tol=0.0), f"{path}: {a!r} vs {b!r}"
        return
    assert type(a) is type(b), f"{path}: {type(a).__name__} {a!r} vs {type(b).__name__} {b!r}"
    if isinstance(a, Obj):
        assert [k for k, _ in a] == [k for k, _ in b], f"{path}: keys {a!r} vs {b!r}"
        for (k, x), (_, y) in zip(a, b):
            same(x, y, f"{path}.{k}")
    elif isinstance(a, list):
        assert len(a) == len(b), f"{path}: length {len(a)} vs {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            same(x, y, f"{path}[{i}]")
    else:
        assert a == b, f"{path}: {a!r} vs {b!r}"


def C(field, nested_in=None, cardinality=None, is_agg=False, column=None):
    return ColumnRef(None, column or field, field, nested_in, cardinality, is_agg)


def compare(table: pa.Table, columns, root="r", limit=None, batches=None):
    """Run both encoders; assert they agree. Returns 'ok' or 'raise'."""
    batches = table.to_batches() if batches is None else batches
    p_err = r_err = None
    try:
        p = ref.encode(table.schema, batches, columns, root, limit)
    except Exception as e:  # noqa: BLE001 — any failure of the product's path
        p_err = e
    try:
        r = rs.encode_response(batches, ref.plan_tuples(columns), root, limit)
    except Exception as e:  # noqa: BLE001
        r_err = e
    TALLY["cases"] += 1
    if p_err is not None or r_err is not None:
        assert p_err is not None and r_err is not None, f"python: {p_err!r}; rust: {r_err!r}"
        assert not isinstance(r_err, NotImplementedError), f"not ported: {r_err}; python: {p_err!r}"
        TALLY["both_raise"] += 1
        return "raise"
    same(parse(p), parse(r))
    TALLY["byte_identical"] += p == r
    return "ok"


# ------------------------------------------------------------------ scalar cases

rng = random.Random(494)


def rand_f64(n):
    return [struct.unpack("<d", rng.getrandbits(64).to_bytes(8, "little"))[0] for _ in range(n)]


def rand_f32(n):
    return [struct.unpack("<f", rng.getrandbits(32).to_bytes(4, "little"))[0] for _ in range(n)]


F64_SPECIAL = [
    0.0,
    -0.0,
    float("nan"),
    float("inf"),
    float("-inf"),
    5e-324,
    2.2250738585072014e-308,
    1.7976931348623157e308,
    1e16,
    1e15,
    9999999999999998.0,
    1.5e16,
    1e-5,
    1e-6,
    1.2345e-5,
    9.99e-6,
    0.1,
    123.456,
    -1.5e-7,
    1.0,
    -1.0,
    2.0**53,
    2.0**64,
    None,
]


def rand_dec(p, s, n, nonzero=True):
    out = []
    for _ in range(n):
        digits = rng.randint(1, p)
        c = rng.randint(0, 10**digits - 1) * rng.choice((1, -1))
        out.append(Decimal(c).scaleb(-s))
    return out


def dec_case(p, s, values):
    return pa.array(values, pa.decimal128(p, s))


TEXT = [
    "",
    "a",
    "é😀 ",
    "ctl\x00\x01\x1f\x7f\b\f\n\r\t",
    '"\\/',
    "{",
    "[",
    "{}",
    "[]",
    '{"a":1}',
    "[1,2]",
    '{"a": "[1]"}',
    '{"a":1} x',
    "[1,]",
    "[01]",
    "[NaN, Infinity, -Infinity]",
    "[1e400]",
    "[-0]",
    "[-0.0]",
    "[1E5, 1e-7, 2.50, 1.0]",
    '{"a":1,"a":2,"b":3}',
    '{"b":1,"a":2}',
    '["\\ud83d\\ude00"]',
    '["\\u00e9\\n\\/\\b"]',
    '[" \\t"]',
    '["\x01"]',
    '["\\x"]',
    " [1]",
    "[1] ",
    "[1]\n",
    "[true,false,null]",
    "[[[[]]]]",
    '{"k": "{\\"n\\": [\\"[1]\\"]}"}',
    "[12345678901234567890]",
    "[-9223372036854775808]",
    "[1" + "0" * 4400 + "]",
    "x" * 5000,
    '{"a": {"b": [1, {"c": null}]}}',
    '["\\u0041\\u00DF"]',
    '{"":1}',
    "[1, 2",
    '{"a" 1}',
    "{1: 2}",
    "[1.5e+3, 1.5E-3]",
    "[.5]",
    "[1.]",
    "[+1]",
    "[-]",
    "[nul]",
    "[NaN1]",
    "[-NaN]",
    None,
]


def scalar_cases():
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
    ts_vals += [rng.randint(-62135596800_000_000, 253402300799_999_999) for _ in range(2000)]
    yield "bool", pa.array([True, False, None, True, False])
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
        yield str(t), pa.array([lo, hi, 0, None] + [rng.randint(lo, hi) for _ in range(500)], t)
    yield "float64-special", pa.array(F64_SPECIAL, pa.float64())
    yield "float64-random", pa.array(rand_f64(100_000), pa.float64())
    yield "float32-random", pa.array(rand_f32(100_000) + [None, 0.1, -0.0], pa.float32())
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
    ]:
        vals = rand_dec(p, s, 3000)
        if s <= 0:
            vals = [v for v in vals if abs(v) < 2**63]
        yield f"decimal({p},{s})", dec_case(p, s, vals + [Decimal(0).scaleb(-s), None])
    yield (
        "decimal-tiny",
        dec_case(
            10,
            9,
            [
                Decimal("1E-9"),
                Decimal("1.2E-8"),
                Decimal("0E-9"),
                Decimal("-0E-9"),
                Decimal("1E-7"),
                Decimal("0.000001"),
                Decimal("-0.000000001"),
            ],
        ),
    )
    yield "decimal-negzero", dec_case(5, 2, [Decimal("-0.00"), Decimal("0.00"), Decimal("1.00")])
    yield (
        "decimal-2^53",
        dec_case(
            38, 0, [Decimal(2**53 + 1), Decimal(2**63 - 1), Decimal(-(2**63)), Decimal(10**19)]
        ),
    )
    yield "decimal256", pa.array(rand_dec(50, 5, 500) + [None], pa.decimal256(50, 5))
    yield (
        "decimal256-int",
        pa.array([Decimal(v) for v in (0, 1, -1, 2**62, 10**18)], pa.decimal256(76, 0)),
    )
    yield "utf8", pa.array(TEXT, pa.string())
    yield "large_utf8", pa.array(TEXT, pa.large_string())
    yield "string_view", pa.array(TEXT, pa.string_view())
    yield "enum", pa.array(["red", "green", None, "red", "[1]"]).dictionary_encode()
    yield (
        "binary",
        pa.array(
            [b"", b"\x00\xff", None] + [rng.randbytes(rng.randint(0, 40)) for _ in range(200)]
        ),
    )
    yield "large_binary", pa.array([b"", b"\x01\x02", None], pa.large_binary())
    yield "fixed_binary", pa.array([b"\x00\x01\x02\x03", None, b"\xff\xff\xff\xff"], pa.binary(4))
    yield (
        "uuid",
        pa.array(
            [uuid.UUID(int=rng.getrandbits(128)).bytes for _ in range(50)] + [None], pa.uuid()
        ),
    )
    yield (
        "date32",
        pa.array(
            [0, -1, 1, -719162, 2932896, 2**31 - 1, -(2**31 - 1), None]
            + [rng.randint(-719162, 2932896) for _ in range(2000)],
            pa.date32(),
        ),
    )
    for unit, div in [("s", 1_000_000), ("ms", 1_000), ("us", 1)]:
        vals = [v if v is None or abs(v) >= 2**63 - 1 else v // div for v in ts_vals]
        if div != 1:
            vals = [v for v in vals if v is None or abs(v) < 2**62]
        for tz in [None, "UTC", "+05:30", "-03:00"]:
            v2 = vals
            if tz in ("+05:30",):  # shifted local time must stay below year 10000
                v2 = [
                    v
                    for v in vals
                    if v is None or v * div < 253402300799_999_999 - 6 * 3600 * 10**6
                ]
            if tz in ("-03:00",):
                v2 = [
                    v
                    for v in vals
                    if v is None
                    or v * div > -62135596800_000_000 + 4 * 3600 * 10**6
                    or abs(v) == 2**63 - 1
                ]
            yield f"timestamp[{unit},{tz}]", pa.array(v2, pa.timestamp(unit, tz=tz))
    day = 86_400
    yield (
        "time32[s]",
        pa.array([0, day - 1, None] + [rng.randrange(day) for _ in range(300)], pa.time32("s")),
    )
    yield (
        "time32[ms]",
        pa.array(
            [0, day * 1000 - 1, None] + [rng.randrange(day * 1000) for _ in range(300)],
            pa.time32("ms"),
        ),
    )
    yield (
        "time64[us]",
        pa.array(
            [0, 1, day * 10**6 - 1, None] + [rng.randrange(day * 10**6) for _ in range(300)],
            pa.time64("us"),
        ),
    )
    td_max = 999_999_999 * day * 10**6 + day * 10**6 - 1
    for unit, div in [("s", 10**6), ("ms", 10**3), ("us", 1)]:
        hi = min(td_max // div, 2**63 - 1)
        lo = max(-(999_999_999 * day * 10**6 // div), -(2**63 - 1))  # timedelta.min: floor days
        vals = [0, 1, -1, hi, lo, None] + [rng.randint(lo, hi) for _ in range(500)]
        vals += [rng.randint(-(10**6), 10**6) for _ in range(200)]
        yield f"duration[{unit}]", pa.array(vals, pa.duration(unit))
    mdn = [
        pa.MonthDayNano([0, 0, 0]),
        pa.MonthDayNano([1, 2, 3_000]),
        pa.MonthDayNano([-1, -2, -3_000]),
        pa.MonthDayNano([14, 0, 0]),
        pa.MonthDayNano([-14, 3, -1]),
        pa.MonthDayNano([0, 0, -1]),
        pa.MonthDayNano([0, 0, 90_000 * 10**9]),
        pa.MonthDayNano([0, 999_999_999, 0]),
        None,
    ]
    for _ in range(3000):
        mdn.append(
            pa.MonthDayNano(
                [
                    rng.randint(-(2**31), 2**31 - 1),
                    rng.randint(-(2**31), 2**31 - 1),
                    rng.randint(-(2**63), 2**63 - 1),
                ]
            )
        )
    yield "interval", pa.array(mdn, pa.month_day_nano_interval())
    yield "null", pa.array([None, None], pa.null())
    yield "list<int64>", pa.array([[1, 2], [], None, [None, 3], [1, 2]], pa.list_(pa.int64()))
    yield (
        "list<float64>",
        pa.array([[0.1, float("nan")], [-0.0], [0.0], [float("inf")]], pa.list_(pa.float64())),
    )
    yield (
        "list<utf8>",
        pa.array([["a", "[1]", '{"x":"[2]"}'], ["{bad"], None], pa.list_(pa.string())),
    )
    yield (
        "list<decimal>",
        pa.array(
            [[Decimal("1.50"), Decimal("2.00")], [Decimal("-0.00")]], pa.list_(pa.decimal128(5, 2))
        ),
    )
    yield (
        "list<decimal0>",
        pa.array([[Decimal("5"), Decimal(2**63 - 1)]], pa.list_(pa.decimal128(38, 0))),
    )
    yield "list<date32>", pa.array([[date(2020, 1, 2), None]], pa.list_(pa.date32()))
    yield "list<ts>", pa.array([[0, 1_500_000]], pa.list_(pa.timestamp("us", tz="UTC")))
    yield "list<ts+05:30>", pa.array([[0, 1_500_000]], pa.list_(pa.timestamp("ms", tz="+05:30")))
    yield (
        "list<time,duration,binary>",
        pa.array(
            [{"t": 3_600_000_001, "d": -5, "b": b"\x01"}],
            pa.struct([("t", pa.time64("us")), ("d", pa.duration("us")), ("b", pa.binary())]),
        ),
    )
    st = pa.struct(
        [
            ("a", pa.int64()),
            ("b", pa.string()),
            ("c", pa.decimal128(6, 3)),
            ("d", pa.list_(pa.int32())),
        ]
    )
    yield (
        "struct",
        pa.array(
            [
                {"a": 1, "b": "[1]", "c": Decimal("1.500"), "d": [1]},
                None,
                {"a": None, "b": None, "c": None, "d": None},
            ],
            st,
        ),
    )
    yield (
        "list<struct>",
        pa.array([[{"a": 1, "b": "x", "c": Decimal("0.001"), "d": []}], []], pa.list_(st)),
    )
    yield "large_list", pa.array([[1], None], pa.large_list(pa.int8()))
    yield "fixed_size_list", pa.array([[1, 2], [3, None]], pa.list_(pa.int16(), 2))
    yield "list<list>", pa.array([[[1], [], None]], pa.list_(pa.list_(pa.int64())))
    ext = pa.ExtensionArray.from_storage(
        pa.uuid(), pa.array([uuid.UUID(int=5).bytes, None], pa.binary(16))
    )
    yield "list<uuid>", pa.ListArray.from_arrays(pa.array([0, 2], pa.int32()), ext)
    yield "list<null>", pa.array([[None]], pa.list_(pa.null()))


CASES = list(scalar_cases())


def shapes(name: str, arr: pa.Array):
    """The value as a root field alone (rows deduplicated by it), beside a key, inside a
    one-to-many element, and as a many-to-one object."""
    n = len(arr)
    ids = pa.array([i % max(1, n // 3) for i in range(n)], pa.int64())
    t1 = pa.table({"v": arr})
    t2 = pa.table({"id": ids, "v": arr})
    t3 = pa.table({"id": ids, "x": pa.array([i % 7 for i in range(n)], pa.int64()), "v": arr})
    yield t1, [C("v")]
    yield t2, [C("id"), C("v")]
    yield t3, [C("id"), C("x", "items", "one-to-many"), C("v", "items", "one-to-many")]
    yield t2, [C("id"), C("v", "items", "one-to-many")]
    yield t3, [C("id"), C("x", "rel", "many-to-one"), C("v", "rel", "many-to-one")]


@pytest.mark.parametrize("name,arr", CASES, ids=[c[0] for c in CASES])
def test_scalar(name, arr):
    for table, cols in shapes(name, arr):
        assert compare(table, cols) == "ok"


# ------------------------------------------------------------------ nesting


def _orders():
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
        }
    )


NEST_PLANS = {
    "flat": [C("id"), C("status")],
    "many-to-one": [
        C("id"),
        C("status"),
        C("cust_id", "customer", "many-to-one"),
        C("cust_name", "customer", "many-to-one"),
    ],
    "many-to-one-2": [
        C("id"),
        C("cust_id", "customer", "many-to-one"),
        C("addr_city", "customer.address", "many-to-one"),
    ],
    "one-to-many": [
        C("id"),
        C("status"),
        C("item_id", "items", "one-to-many"),
        C("item_qty", "items", "one-to-many"),
    ],
    "one-to-many-2": [
        C("id"),
        C("item_id", "items", "one-to-many"),
        C("part_id", "items.parts", "one-to-many"),
    ],
    "o2m+m2o": [
        C("id"),
        C("item_id", "items", "one-to-many"),
        C("prod_name", "items.product", "many-to-one"),
        C("cust_name", "customer", "many-to-one"),
    ],
    "alias-dup": [
        C("id"),
        C("status", column="status"),
        C("id", column="cust_id"),
        C("item_id", "items", "one-to-many"),
        C("item_id", "items", "one-to-many", column="item_qty"),
    ],
    "field-vs-rel": [
        C("id"),
        C("customer", column="status"),
        C("cust_id", "customer", "many-to-one"),
    ],
    "field-vs-list": [C("id"), C("items", column="status"), C("item_id", "items", "one-to-many")],
    "agg": [C("id"), C("cust_name", is_agg=True), C("item_id", "items", "one-to-many")],
    "agg-flat": [C("id"), C("cust_name", is_agg=True)],
    "all-agg": [C("id", is_agg=True), C("status", is_agg=True)],
    "only-nested": [C("item_id", "items", "one-to-many"), C("cust_id", "customer", "many-to-one")],
    "deep-gap": [C("id"), C("addr_city", "customer.address", "many-to-one")],
    "child-first": [
        C("id"),
        C("addr_city", "customer.address", "many-to-one"),
        C("cust_id", "customer", "many-to-one"),
    ],
}


def _project(table, plan):
    return pa.table({f"c{i}": table[c.column] for i, c in enumerate(plan)})


@pytest.mark.parametrize("plan", list(NEST_PLANS), ids=list(NEST_PLANS))
@pytest.mark.parametrize("limit", [None, 0, 1, 2, 100])
def test_nesting(plan, limit):
    t = _project(_orders(), NEST_PLANS[plan])
    compare(t, NEST_PLANS[plan], root="orders", limit=limit)


@pytest.mark.parametrize("plan", list(NEST_PLANS), ids=list(NEST_PLANS))
def test_empty(plan):
    t = _project(_orders(), NEST_PLANS[plan]).slice(0, 0)
    assert compare(t, NEST_PLANS[plan]) == "ok"
    assert compare(t, NEST_PLANS[plan], batches=[]) == "ok"


def test_detect_truncated_crash():
    """Product defect: a repeated root row whose to-one parent was null, under which a nested to-one
    path sits, raises from _detect_truncated_paths (None.get). Both raise."""
    t = pa.table({"id": [1, 1], "a": pa.array([None, None], pa.int64()), "b": [5, 6]})
    plan = [C("id"), C("a", "rel", "many-to-one"), C("b", "rel.sub", "many-to-one")]
    assert compare(t, plan) == "raise"


def test_batches_split():
    t = _project(_orders(), NEST_PLANS["o2m+m2o"])
    t = pa.concat_tables([t] * 50)
    for sizes in ([1], [3], [7, 1, 13]):
        bs, at, i = [], 0, 0
        while at < t.num_rows:
            n = sizes[i % len(sizes)]
            bs.append(t.slice(at, n).to_batches()[0])
            at += n
            i += 1
        assert compare(t, NEST_PLANS["o2m+m2o"], batches=bs) == "ok"


def _rand_table(r: random.Random, n: int):
    def col(kind):
        if kind == "int":
            return pa.array(
                [None if r.random() < 0.15 else r.randint(0, 4) for _ in range(n)], pa.int64()
            )
        if kind == "float":
            pool = [0.0, -0.0, float("nan"), 1.5, 2.25, float("inf")]
            return pa.array(
                [None if r.random() < 0.15 else r.choice(pool) for _ in range(n)], pa.float64()
            )
        if kind == "dec":
            pool = [Decimal("0.00"), Decimal("-0.00"), Decimal("1.50"), Decimal("1.00")]
            return pa.array(
                [None if r.random() < 0.15 else r.choice(pool) for _ in range(n)],
                pa.decimal128(5, 2),
            )
        if kind == "json":
            pool = [
                '{"a":1}',
                '{"a":1.0}',
                '{"a":true}',
                "[NaN]",
                "[-0.0]",
                "[0.0]",
                "x",
                "[1]",
                '{"b":2,"a":1}',
                '{"a":1,"b":2}',
            ]
            return pa.array(
                [None if r.random() < 0.15 else r.choice(pool) for _ in range(n)], pa.string()
            )
        if kind == "struct":
            pool = [{"a": 1, "c": "x"}, {"a": 2, "c": None}, {"a": None, "c": "[1]"}]
            return pa.array(
                [None if r.random() < 0.15 else r.choice(pool) for _ in range(n)],
                pa.struct([("a", pa.int64()), ("c", pa.string())]),
            )
        if kind == "list":
            pool = [[1], [], [1, 2], [None]]
            return pa.array(
                [None if r.random() < 0.15 else r.choice(pool) for _ in range(n)],
                pa.list_(pa.int64()),
            )
        return pa.array(
            [None if r.random() < 0.15 else r.choice("abc") for _ in range(n)], pa.string()
        )

    kinds = ["int", "float", "dec", "json", "str", "struct", "list"]
    return {f"{k}{i}": col(k) for i, k in enumerate(r.choices(kinds, k=8))}


@pytest.mark.parametrize("seed", range(1500))
def test_random_nesting(seed):
    r = random.Random(seed)
    cols = _rand_table(r, r.randint(0, 60))
    names = list(cols)
    paths = ["a", "b", "a.c", "a.c.d", "b.e"]
    card = {p: r.choice(["one-to-many", "many-to-one"]) for p in paths}
    plan, data = [], {}
    for i in range(r.randint(1, 8)):
        src = r.choice(names)
        nested = None if r.random() < 0.4 else r.choice(paths)
        field = r.choice(["f", "g", "h", "a", "b", "c", "e", src])
        plan.append(
            C(field, nested, card[nested] if nested else None, is_agg=r.random() < 0.1, column=src)
        )
        data[f"c{i}"] = cols[src]
    t = pa.table(data)
    if _replaced_list(plan):
        pytest.skip(
            "a to-many list under a field its to-one parent re-sets (id reuse, see RESULTS.md)"
        )
    compare(t, plan, limit=r.choice([None, None, 3]))


def _replaced_list(plan) -> bool:
    """A to-many path whose list hangs under a name its to-one parent path re-sets each row: the
    parent's ``update`` drops the list, and ``_serialize_with_one_to_many`` keys elements by
    ``id(list)``, so whether a later row's element is found depends on CPython reusing the dropped
    list's address. That is not reproducible; such plans are left out (a product defect, reported)."""
    paths = {c.nested_in for c in plan if c.nested_in}
    for q in paths:
        if not any(c.cardinality == "one-to-many" for c in plan if c.nested_in == q):
            continue
        segs = q.split(".")
        for n in range(len(segs) - 1, 0, -1):
            parent = ".".join(segs[:n])
            if parent in paths:
                fields = {c.field_name for c in plan if c.nested_in == parent}
                if (
                    not any(c.cardinality == "one-to-many" for c in plan if c.nested_in == parent)
                    and segs[n] in fields
                ):
                    return True
                break
    return False


# ------------------------------------------------------------------ errors

ERRORS = {
    "decimal-over-u64": (pa.array([Decimal(2**64)], pa.decimal128(38, 0)), "raise"),
    "decimal-2^64-1": (pa.array([Decimal(2**64 - 1)], pa.decimal128(38, 0)), "raise"),
    "decimal-neg-scale-big": (pa.array([Decimal("1E+30")], pa.decimal128(5, -30)), "raise"),
    "json-big-int": (pa.array(["[123456789012345678901234]"]), "raise"),
    "json-lone-surrogate": (pa.array(['["\\ud800"]']), "raise"),
    "json-lone-surrogate-key": (pa.array(['{"\\udc00": 1}']), "raise"),
    "json-deep-300": (pa.array(["[" * 300 + "]" * 300]), "raise"),
    "json-deep-251-empty": (pa.array(["[" * 251 + "]" * 251]), "ok"),
    "json-deep-252-empty": (pa.array(["[" * 252 + "]" * 252]), "raise"),
    "json-deep-250": (pa.array(["[" * 250 + "1" + "]" * 250]), "ok"),
    "json-deep-251": (pa.array(["[" * 251 + "1" + "]" * 251]), "raise"),
    "date-out-of-range": (pa.array([2932897], pa.date32()), "raise"),
    "timestamp-out-of-range": (pa.array([253402300800_000_000], pa.timestamp("us")), "raise"),
    "timestamp-offset-past-9999": (
        pa.array([253402300799_000_000], pa.timestamp("us", tz="+05:30")),
        "raise",
    ),
    "duration-overflow": (pa.array([10**15], pa.duration("s")), "raise"),
    "duration-floor-days": (pa.array([-(999_999_999 * 86_400 + 1)], pa.duration("s")), "raise"),
    "struct-dupnames": (
        pa.StructArray.from_arrays([pa.array([1]), pa.array([2])], names=["a", "a"]),
        "raise",
    ),
    "interval-overflow": (
        pa.array([pa.MonthDayNano([0, 999_999_999, 86_400 * 10**9])], pa.month_day_nano_interval()),
        "raise",
    ),
}


@pytest.mark.parametrize("name", list(ERRORS))
def test_errors(name):
    arr, want = ERRORS[name]
    assert compare(pa.table({"v": arr}), [C("v")]) == want


def test_poison_in_repeated_row_is_not_written():
    """orjson fails only on what it writes: a too-big value in a repeated (dropped) row is fine on
    both paths, while a to_pylist failure in that row fails both."""
    t = pa.table({"id": [1, 1], "v": ["ok", "[123456789012345678901234]"]})
    assert compare(t, [C("id", is_agg=False), C("v", "rel", "many-to-one")]) == "ok"
    t = pa.table({"id": [1, 1], "d": pa.array([0, 2932897], pa.date32())})
    assert compare(t, [C("id"), C("d", "rel", "many-to-one")]) == "raise"


NOT_PORTED = {
    "named-zone": pa.array([0], pa.timestamp("us", tz="America/New_York")),
    "ns-timestamp": pa.array([0], pa.timestamp("ns")),
    "map": pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int64())),
    "list<interval>": pa.array(
        [[pa.MonthDayNano([1, 2, 3])]], pa.list_(pa.month_day_nano_interval())
    ),
}


@pytest.mark.parametrize("name", list(NOT_PORTED))
def test_not_ported(name):
    with pytest.raises(NotImplementedError):
        rs.encode_response(
            pa.table({"v": NOT_PORTED[name]}).to_batches(), [("v", None, None, False)], "r"
        )


def test_zz_tally():
    print(f"\nTALLY {TALLY}")
    assert TALLY["cases"] > 0
