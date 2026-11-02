# Copyright (c) 2026 Kenneth Stott
# Canary: c276c19f-5b29-4dfa-b8a8-3606c52a5aad
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Differential test, GraphQL: the Rust encoder against the product's Python path (python_ref, the
exact-values contract), on the same Arrow batches and selection plan (check.compare).

Not collected by CI (it lives under experiments/). Run it as RESULTS.md says.
"""

from __future__ import annotations

import random

import pyarrow as pa
import pytest
from decimal import Decimal

from tests.unit.json_encoder import encoders as rs
from tests.unit.json_encoder import python_ref as ref
from tests.unit.json_encoder.cases import CASES, C, orders, project, rand_table
from tests.unit.json_encoder.check import TALLY, compare
from provisa.executor.json_encoders import arrow_batches as rust_batches


def gql(table: pa.Table, columns, root="r", limit=None, batches=None):
    batches = table.to_batches() if batches is None else batches
    return compare(
        "graphql",
        lambda: ref.encode(table.schema, batches, columns, root, limit),
        lambda: rs.encode_response(rust_batches(batches), columns, root, limit),
    )


def mode_kw(mode):
    return {"json": mode == "json", "integer": mode == "integer"}


def shapes(arr: pa.Array, mode: str):
    """The value as a root field alone (rows deduplicated by it), beside a key, inside a
    one-to-many element, and as a many-to-one object."""
    n = len(arr)
    kw = mode_kw(mode)
    ids = pa.array([i % max(1, n // 3) for i in range(n)], pa.int64())
    t1 = pa.table({"v": arr})
    t2 = pa.table({"id": ids, "v": arr})
    t3 = pa.table({"id": ids, "x": pa.array([i % 7 for i in range(n)], pa.int64()), "v": arr})
    yield t1, [C("v", **kw)]
    yield t2, [C("id"), C("v", **kw)]
    yield t3, [C("id"), C("x", "items", "one-to-many"), C("v", "items", "one-to-many", **kw)]
    yield t2, [C("id"), C("v", "items", "one-to-many", **kw)]
    yield t3, [C("id"), C("x", "rel", "many-to-one"), C("v", "rel", "many-to-one", **kw)]


SCALAR = [(name, arr, mode) for name, arr, modes in CASES for mode in modes]


@pytest.mark.parametrize("name,arr,mode", SCALAR, ids=[f"{c[0]}-{c[2]}" for c in SCALAR])
def test_scalar(name, arr, mode):
    for table, cols in shapes(arr, mode):
        gql(table, cols)


# ------------------------------------------------------------------ nesting

NEST_PLANS = {
    "flat": [C("id"), C("status"), C("amount"), C("doc", json=True)],
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
    "json-vs-rel": [
        C("id"),
        C("customer", column="doc", json=True),
        C("cust_id", "customer", "many-to-one"),
    ],
    "json-vs-list": [
        C("id"),
        C("items", column="doc", json=True),
        C("item_id", "items", "one-to-many"),
    ],
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
    "null-parent-deep": [
        C("id"),
        C("addr_city", "customer", "many-to-one"),
        C("prod_name", "customer.sub", "many-to-one"),
    ],
}


@pytest.mark.parametrize("plan", list(NEST_PLANS), ids=list(NEST_PLANS))
@pytest.mark.parametrize("limit", [None, 0, 1, 2, 100])
def test_nesting(plan, limit):
    t = project(orders(), NEST_PLANS[plan])
    gql(t, NEST_PLANS[plan], root="orders", limit=limit)


@pytest.mark.parametrize("plan", list(NEST_PLANS), ids=list(NEST_PLANS))
def test_empty(plan):
    t = project(orders(), NEST_PLANS[plan]).slice(0, 0)
    assert gql(t, NEST_PLANS[plan]) == "ok"
    assert gql(t, NEST_PLANS[plan], batches=[]) == "ok"


def test_batches_split():
    t = project(orders(), NEST_PLANS["o2m+m2o"])
    t = pa.concat_tables([t] * 50)
    for sizes in ([1], [3], [7, 1, 13]):
        bs, at, i = [], 0, 0
        while at < t.num_rows:
            n = sizes[i % len(sizes)]
            bs.append(t.slice(at, n).to_batches()[0])
            at += n
            i += 1
        assert gql(t, NEST_PLANS["o2m+m2o"], batches=bs) == "ok"


def random_plan(seed: int):
    r = random.Random(seed)
    cols = rand_table(r, r.randint(0, 60))
    names = list(cols)
    paths = ["a", "b", "a.c", "a.c.d", "b.e"]
    card = {p: r.choice(["one-to-many", "many-to-one"]) for p in paths}
    plan, data = [], {}
    for i in range(r.randint(1, 8)):
        src = r.choice(names)
        kind, arr = cols[src]
        nested = None if r.random() < 0.4 else r.choice(paths)
        field = r.choice(["f", "g", "h", "a", "b", "c", "e", src])
        plan.append(
            C(
                field,
                nested,
                card[nested] if nested else None,
                is_agg=r.random() < 0.1,
                column=src,
                json=kind == "json",
                integer=kind == "dec" and r.random() < 0.3,
            )
        )
        data[f"c{i}"] = arr
    return r, pa.table(data), plan


@pytest.mark.parametrize("seed", range(1500))
def test_random_nesting(seed):
    r, t, plan = random_plan(seed)
    gql(t, plan, limit=r.choice([None, None, 3]))


# ------------------------------------------------------------------ errors and refusals

ERRORS = {
    # (array, column kwargs, outcome)
    "decimal-past-u64": (pa.array([Decimal(2**64)], pa.decimal128(38, 0)), {}, "ok"),
    "integer-decimal-past-u64": (
        pa.array([Decimal(2**100)], pa.decimal128(38, 0)),
        {"integer": True},
        "ok",
    ),
    "integer-decimal-fraction": (
        pa.array([Decimal("1.5")], pa.decimal128(5, 1)),
        {"integer": True},
        "raise",
    ),
    "json-big-int": (pa.array(["[123456789012345678901234]"]), {"json": True}, "ok"),
    "json-int-past-4300-digits": (pa.array(["[1" + "0" * 4400 + "]"]), {"json": True}, "raise"),
    "json-nan": (pa.array(["[NaN]"]), {"json": True}, "raise"),
    "json-infinity": (pa.array(['{"a": -Infinity}']), {"json": True}, "raise"),
    "json-not-json": (pa.array(["{bad"]), {"json": True}, "raise"),
    "json-scalar-text": (pa.array(["hello"]), {"json": True}, "raise"),
    "json-lone-surrogate": (pa.array(['["\\ud800"]']), {"json": True}, "raise"),
    "json-lone-surrogate-key": (pa.array(['{"\\udc00": 1}']), {"json": True}, "raise"),
    "json-deep-300": (pa.array(["[" * 300 + "]" * 300]), {"json": True}, "raise"),
    "json-deep-251-empty": (pa.array(["[" * 251 + "]" * 251]), {"json": True}, "ok"),
    "json-deep-252-empty": (pa.array(["[" * 252 + "]" * 252]), {"json": True}, "raise"),
    "json-deep-250": (pa.array(["[" * 250 + "1" + "]" * 250]), {"json": True}, "ok"),
    "json-deep-251": (pa.array(["[" * 251 + "1" + "]" * 251]), {"json": True}, "raise"),
    "json-deep-2000": (pa.array(["[" * 2000 + "]" * 2000]), {"json": True}, "raise"),
    "date-out-of-range": (pa.array([2932897], pa.date32()), {}, "raise"),
    "timestamp-out-of-range": (pa.array([253402300800_000_000], pa.timestamp("us")), {}, "raise"),
    "timestamp-offset-past-9999": (
        pa.array([253402300799_000_000], pa.timestamp("us", tz="+05:30")),
        {},
        "raise",
    ),
    "timestamp-zone-past-9999": (
        pa.array([253402300799_000_000], pa.timestamp("us", tz="Asia/Tokyo")),
        {},
        "raise",
    ),
    "duration-overflow": (pa.array([10**15], pa.duration("s")), {}, "raise"),
    "duration-floor-days": (pa.array([-(999_999_999 * 86_400 + 1)], pa.duration("s")), {}, "raise"),
    "struct-dupnames": (
        pa.StructArray.from_arrays([pa.array([1]), pa.array([2])], names=["a", "a"]),
        {},
        "raise",
    ),
    "interval-overflow": (
        pa.array([pa.MonthDayNano([0, 999_999_999, 86_400 * 10**9])], pa.month_day_nano_interval()),
        {},
        "raise",
    ),
    "date64-out-of-range": (pa.array([253402300800_000], pa.date64()), {}, "raise"),
}


@pytest.mark.parametrize("name", list(ERRORS))
def test_errors(name):
    arr, kw, want = ERRORS[name]
    assert gql(pa.table({"v": arr}), [C("v", **kw)]) == want


def test_refusal_in_repeated_row():
    """A JSON value is parsed for every row's key, a dropped one too: a refusal there refuses the
    answer; a value too deep to write in a dropped row is never written."""
    t = pa.table({"id": [1, 1], "v": ["[1]", "[NaN]"]})
    assert gql(t, [C("id"), C("v", "rel", "many-to-one", json=True)]) == "raise"
    t = pa.table({"id": [1, 1], "v": ["1", "[" * 300 + "]" * 300]})
    assert gql(t, [C("id"), C("v", "rel", "many-to-one", json=True)]) == "ok"
    t = pa.table({"id": [1, 1], "d": pa.array([0, 2932897], pa.date32())})
    assert gql(t, [C("id"), C("d", "rel", "many-to-one")]) == "raise"


def test_json_text_paths():
    """A string at a json_text key path is JSON the engine wrote (a nested relationship), parsed
    again; any other string inside the value stays a string."""
    t = pa.table(
        {
            "v": [
                '{"items": "[{\\"p\\": \\"{\\\\\\"q\\\\\\": 1}\\"}]", "note": "[1]"}',
                '{"items": ["[2]", "x"], "note": "{}"}',
                '{"items": "nope"}',
            ]
        }
    )
    for paths in [(), ("items",), ("items", "items.p"), ("note",)]:
        for i in range(3):
            gql(t.slice(i, 1), [C("v", json=True, json_text=paths)])


def test_zz_tally():
    print(f"\nTALLY {dict(TALLY)}")
    assert TALLY["graphql.cases"] > 0
