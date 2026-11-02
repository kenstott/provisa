# Copyright (c) 2026 Kenneth Stott
# Canary: 6e2b9c4f-1a7d-4d3e-8c5b-9f0a2e4d7c18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Differential test, REST, JSON:API and OData: each Rust encoder against the product's path for
that surface (python_ref), on the same Arrow batches and plan (check.compare).

Not collected by CI (it lives under experiments/). Run it as RESULTS.md says.
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pytest

from tests.unit.json_encoder import encoders as rs
from tests.unit.json_encoder import python_ref as ref
from tests.unit.json_encoder.cases import CASES, C, JSON_TEXT, orders, project, rand_table
from tests.unit.json_encoder.check import TALLY, compare
from provisa.api.odata.edm import edm_type
from provisa.api.odata.expand import Plan
from provisa.api.odata.model import EntitySet, Navigation, Property
from provisa.api.odata.query import QueryOptions
from tests.unit.json_encoder.test_compare import NEST_PLANS, random_plan, shapes
from provisa.executor.json_encoders import arrow_batches as rust_batches
from provisa.executor.json_encoders import odata_entity, warnings_json

SCALAR = [(name, arr, mode) for name, arr, modes in CASES for mode in modes]
IDS = [f"{c[0]}-{c[2]}" for c in SCALAR]

# ------------------------------------------------------------------ REST


def rest(table, columns, agg_alias=None, batches=None):
    batches = table.to_batches() if batches is None else batches
    return compare(
        "rest",
        lambda: ref.rest(table.schema, batches, columns, "t", agg_alias),
        lambda: rs.encode_rest(rust_batches(batches), columns, agg_alias),
    )


def rest_csv(table, columns):
    batches = table.to_batches()
    return compare(
        "rest_csv",
        lambda: ref.rest_csv(table.schema, batches, columns, "t"),
        lambda: rs.encode_rest_csv(rust_batches(batches), columns),
    )


@pytest.mark.parametrize("name,arr,mode", SCALAR, ids=IDS)
def test_rest_scalar(name, arr, mode):
    for table, cols in shapes(arr, mode):
        rest(table, cols)
        rest_csv(table, cols)


def test_rest_csv_quoting():
    t = pa.table({"v": ["", "a,b", 'q"q', "line\nbreak", "cr\rx", " lead", None, "é"]})
    for i in range(len(t)):
        rest_csv(t.slice(i, 1), [C("v")])
        rest_csv(pa.table({"id": [1], "v": t["v"].slice(i, 1)}), [C("id"), C("v")])


@pytest.mark.parametrize("plan", list(NEST_PLANS), ids=list(NEST_PLANS))
def test_rest_nesting(plan):
    t = project(orders(), NEST_PLANS[plan])
    rest(t, NEST_PLANS[plan])
    rest_csv(t, NEST_PLANS[plan])
    assert rest(t.slice(0, 0), NEST_PLANS[plan]) == "ok"
    assert rest(t, NEST_PLANS[plan], batches=[]) == "ok"


@pytest.mark.parametrize("seed", range(0, 1500, 3))
def test_rest_random(seed):
    _, t, plan = random_plan(seed)
    rest(t, plan)
    rest_csv(t, plan)


AGG_COLS = [
    C("count", "aggregate"),
    C("amount", "aggregate.sum"),
    C("amount", "aggregate.max", column="amount_max"),
    C("qty", "aggregate.avg", column="qty"),
    C("nodes", None, is_agg=True, json=True),
    C("hugeint", "aggregate.sum", column="huge", integer=True),
]


def agg_table(n=1):
    nodes = ['[{"id": 1, "amount": 1.50}, {"id": 2, "amount": null}]', "[]", None]
    return pa.table(
        {
            "count": pa.array([3 + i for i in range(n)], pa.int64()),
            "amount": pa.array([Decimal("12.50")] * n, pa.decimal128(38, 2)),
            "amount_max": pa.array([Decimal("9.00")] * n, pa.decimal128(38, 2)),
            "qty": pa.array([2.3333333333333335] * n, pa.float64()),
            "nodes": pa.array([nodes[i % 3] for i in range(n)]),
            "huge": pa.array([Decimal(2**70)] * n, pa.decimal128(38, 0)),
        }
    )


@pytest.mark.parametrize("n", [0, 1, 3])
def test_rest_aggregate(n):
    rest(agg_table(n), AGG_COLS, agg_alias="aggregate")


def group_table():
    return pa.table(
        {
            "status": ["new", "paid", None, "new"],
            "region": ["eu", None, None, "us"],
            "count": pa.array([2, 1, 4, 2], pa.int64()),
            "amount": pa.array(
                [Decimal("3.00"), None, Decimal("1.10"), Decimal("3.00")], pa.decimal128(10, 2)
            ),
            "nodes": [
                '[{"id": 1, "orderStatus": "new", "customer": {"custName": "A"}}]',
                "[]",
                '[{"id": 9}]',
                "[]",
            ],
        }
    )


GROUP_COLS = [
    C("orderStatus", "groupKey", "many-to-one", column="status"),
    C("region", "groupKey", "many-to-one"),
    C("count", "aggregate", "many-to-one"),
    C("amount", "aggregate.sum", "many-to-one"),
    C("nodes", None, is_agg=True, json=True),
]


def test_rest_group_by():
    t = group_table()
    rest(t, GROUP_COLS)
    rest(t, GROUP_COLS[:4])


# ------------------------------------------------------------------ JSON:API

GQL_TO_PHYS = {
    "orderStatus": "order_status",
    "customerId": "customer_id",
    "createdAt": "created_at",
}
REL_TO_PHYS = {"customer": "customer", "lineItems": "line_items"}
REL_SCALARS = {
    "customer": {"custName": "cust_name"},
    "lineItems": {"itemId": "item_id", "itemQty": "item_qty"},
}


def jsonapi(
    table,
    columns,
    *,
    includes=(),
    include_keys=None,
    base_keys=("id",),
    page_size=25,
    page_number=1,
    probe=1,
    total=None,
    extra=None,
    warnings=None,
    batches=None,
    table_name="orders",
):
    batches = table.to_batches() if batches is None else batches
    include_keys = (
        include_keys
        if include_keys is not None
        else {"customer": ["id"], "line_items": ["item_id"]}
    )
    extra = extra or {}
    return compare(
        "jsonapi",
        lambda: ref.jsonapi(
            table.schema,
            batches,
            columns,
            gql_table="orders",
            table_name=table_name,
            domain_id="sales",
            page_number=page_number,
            page_size=page_size,
            probe_rows=probe,
            total_count=total,
            base_keys=list(base_keys),
            gql_to_physical=GQL_TO_PHYS,
            rel_gql_to_physical=REL_TO_PHYS,
            rel_scalar_gql_to_physical=REL_SCALARS,
            physical_include_names=list(includes),
            include_keys=include_keys,
            extra=extra,
            warnings=warnings,
        ),
        lambda: rs.encode_jsonapi(
            rust_batches(batches),
            columns,
            resource_type=table_name,
            page_size=page_size,
            page_number=page_number,
            probe=bool(probe),
            total=total,
            id_columns=list(base_keys),
            scalar_names=GQL_TO_PHYS,
            rel_names=list(REL_TO_PHYS.items()),
            rel_scalar_names=REL_SCALARS,
            includes=list(includes),
            include_keys=include_keys,
            base_path=f"/data/jsonapi/sales/{table_name}",
            link_params=list(extra.items()),
            warnings=warnings_json(warnings),
        ),
    )


def jsonapi_orders():
    t = orders()
    return pa.table(
        {
            "id": t["id"],
            "status": t["status"],
            "cust_fk": t["cust_id"],
            "cust_id": t["cust_id"],
            "cust_name": t["cust_name"],
            "item_id": t["item_id"],
            "item_qty": t["item_qty"],
            "amount": t["amount"],
            "doc": t["doc"],
        }
    )


JA_COLS = [
    C("id"),
    C("orderStatus", column="status"),
    C("customerId", column="cust_fk"),
    C("amount"),
    C("doc", json=True),
]
JA_INCLUDE = [
    *JA_COLS,
    C("id", "customer", "many-to-one", column="cust_id"),
    C("custName", "customer", "many-to-one", column="cust_name"),
    C("itemId", "lineItems", "one-to-many", column="item_id"),
    C("itemQty", "lineItems", "one-to-many", column="item_qty"),
]

JA_VARIANTS = {
    "plain": {},
    "page-1": {"page_size": 1},
    "page-2-of-2": {"page_size": 2, "page_number": 2},
    "exact-page": {"page_size": 4, "probe": 0},
    "total": {"page_size": 2, "total": 7, "probe": 0},
    "total-0": {"total": 0, "probe": 0},
    "warnings": {"warnings": [{"code": "x.y", "params": {"n": 1}, "message": "cut at 5 rows — é"}]},
    "extra": {
        "extra": {
            "role": "analyst one",
            "sort": "-created_at,amount",
            "fields[orders]": "amount,id",
            "filter[status]": "new & paid=ü",
            "page[total]": "true",
        }
    },
    "composite-key": {"base_keys": ("id", "orderStatus")},
}


@pytest.mark.parametrize("variant", list(JA_VARIANTS), ids=list(JA_VARIANTS))
@pytest.mark.parametrize(
    "includes",
    [
        (),
        ("customer",),
        ("line_items",),
        ("customer", "line_items"),
        ("line_items", "customer", "line_items"),
    ],
    ids=["none", "to-one", "to-many", "both", "repeat"],
)
def test_jsonapi_read(variant, includes):
    kw = dict(JA_VARIANTS[variant])
    if "base_keys" in kw:
        kw["base_keys"] = ("id", "order_status")
    cols = JA_INCLUDE if includes else JA_COLS
    t = project(jsonapi_orders(), cols)
    jsonapi(t, cols, includes=includes, **kw)
    assert jsonapi(t.slice(0, 0), cols, includes=includes, **kw) == "ok"


@pytest.mark.parametrize("name,arr,mode", SCALAR, ids=IDS)
def test_jsonapi_scalar(name, arr, mode):
    """Each type as an attribute, as the resource's id (``str`` of the value), and as a foreign key
    (its relationship's id)."""
    n = len(arr)
    kw = {"json": mode == "json", "integer": mode == "integer"}
    ids = pa.array(range(n), pa.int64())
    t = pa.table({"id": ids, "v": arr})
    jsonapi(t, [C("id"), C("v", **kw)], page_size=1000)
    jsonapi(pa.table({"v": arr}), [C("id", column="v", **kw)], page_size=1000)
    jsonapi(t, [C("id"), C("customerId", column="v", **kw)], page_size=1000)
    t3 = pa.table({"id": ids, "v": arr, "w": arr})
    jsonapi(
        t3,
        [C("id"), C("v", **kw), C("id", "customer", "many-to-one", column="w", **kw)],
        includes=("customer",),
        page_size=1000,
    )


def test_jsonapi_composite_ids():
    t = pa.table({"a": ["x:y", "b\\c", None, "é"], "b": pa.array([1, 2, 3, 4], pa.int64())})
    jsonapi(t, [C("a"), C("b")], base_keys=("a", "b"))


def test_jsonapi_include_errors():
    """An included object missing its key, and a to-one that is a JSON scalar, fail both paths."""
    t = pa.table({"id": [1], "c": [5]})
    jsonapi(t, [C("id"), C("x", "customer", "many-to-one", column="c")], includes=("customer",))
    t = pa.table({"id": [1, 2], "customer": ["7", "{}"]})
    jsonapi(t, [C("id"), C("customer", json=True)], includes=("customer",))


@pytest.mark.parametrize("seed", range(1, 1500, 5))
def test_jsonapi_random(seed):
    r, t, plan = random_plan(seed)
    keys = [c.field_name for c in plan if c.nested_in is None]
    base = (r.choice(keys),) if keys else ("id",)
    jsonapi(
        t,
        plan,
        base_keys=base,
        includes=r.choice([(), ("a",), ("b",), ("a", "b")]),
        include_keys={"a": [r.choice(["f", "g", "h", "c"])], "b": ["e"]},
        page_size=r.choice([1, 3, 1000]),
    )


def test_jsonapi_group_by():
    for cols in (
        GROUP_COLS,
        GROUP_COLS[:4],
        [C("orderStatus", "groupKey", column="status"), C("count", "aggregate")],
    ):
        t = project(group_table(), cols)
        compare(
            "jsonapi",
            lambda: ref.jsonapi_group_by(
                t.schema,
                t.to_batches(),
                cols,
                root_field="orders_group_by",
                table_name="orders",
                gql_to_physical=GQL_TO_PHYS,
                rel_gql_to_physical=REL_TO_PHYS,
                rel_scalar_gql_to_physical=REL_SCALARS,
            ),
            lambda: rs.encode_jsonapi_group_by(
                t.to_batches(),
                cols,
                resource_type="orders",
                scalar_names=GQL_TO_PHYS,
                rel_names=list(REL_TO_PHYS.items()),
                rel_scalar_names=REL_SCALARS,
            ),
        )


def test_jsonapi_group_by_null_key():
    """Defect 4, fixed: a group whose every key is null is that key, each field null (the Python
    path called ``.items()`` on None, a 500 for any group-by over a nullable column holding
    NULL)."""
    cols = [
        C("region", "groupKey", "many-to-one"),
        C("tier", "groupKey", "many-to-one"),
        C("count", "aggregate", "many-to-one"),
    ]
    t = pa.table(
        {"region": [None, "eu"], "tier": [None, None], "count": pa.array([4, 1], pa.int64())}
    )
    outcome = compare(
        "jsonapi",
        lambda: ref.jsonapi_group_by(
            t.schema,
            t.to_batches(),
            cols,
            root_field="g",
            table_name="orders",
            gql_to_physical={},
            rel_gql_to_physical={},
            rel_scalar_gql_to_physical={},
        ),
        lambda: rs.encode_jsonapi_group_by(
            t.to_batches(),
            cols,
            resource_type="orders",
            scalar_names={},
            rel_names=[],
            rel_scalar_names={},
        ),
    )
    assert outcome == "ok"
    body = rs.encode_jsonapi_group_by(
        t.to_batches(),
        cols,
        resource_type="orders",
        scalar_names={"region": "region_name"},
        rel_names=[],
        rel_scalar_names={},
    )
    data = json.loads(body)["data"]
    assert data[0]["attributes"]["groupKey"] == {"region_name": None, "tier": None}
    assert data[1]["attributes"]["groupKey"] == {"region_name": "eu", "tier": None}


@pytest.mark.parametrize("n", [0, 1, 3])
def test_jsonapi_aggregate(n):
    t = agg_table(n)
    compare(
        "jsonapi",
        lambda: ref.jsonapi_aggregate(
            t.schema, t.to_batches(), AGG_COLS, root_field="orders_aggregate", agg_alias="aggregate"
        ),
        lambda: rs.encode_jsonapi_aggregate(t.to_batches(), AGG_COLS, "aggregate"),
    )


# ------------------------------------------------------------------ OData

ROOT = "https://h.example/odata/v4"


def entity(name, props, key=()):
    return EntitySet(
        name,
        1,
        name,
        tuple(key),
        tuple(Property(n, edm_type(t), n, t) for n, t in props),
        (),
        tuple(key),
    )


def nav(name, target, collection):
    return Navigation(name, target.name, target.type_name, collection, ())


def plan(
    navigation,
    target,
    parent_key,
    fields,
    *,
    page=2,
    probe=1,
    children=(),
    raw=(("$top", "2"),),
    options=None,
):
    return Plan(
        navigation,
        target,
        tuple(parent_key),
        options or QueryOptions(top=page),
        raw,
        tuple(fields),
        tuple(children),
        page,
        probe,
    )


def odata(
    table,
    ent,
    fields,
    plans=(),
    *,
    page=5000,
    count=None,
    warnings=None,
    items=(("$select", "id"),),
    options=None,
    batches=None,
    names=None,
):
    batches = table.to_batches() if batches is None else batches
    names = names or list(table.column_names)
    options = options or QueryOptions()
    from provisa.api.odata.expand import page_link

    context = f"{ROOT}/$metadata#{ent.name}"
    return compare(
        "odata",
        lambda: ref.odata(
            table.schema,
            batches,
            names,
            entity=ent,
            fields=tuple(fields),
            plans=tuple(plans),
            context=context,
            count=count,
            page=page,
            root=ROOT,
            items=items,
            options=options,
            warnings=warnings,
        ),
        lambda: rs.encode_odata(
            rust_batches(batches),
            names,
            odata_entity(ent, tuple(fields), tuple(plans)),
            context=context,
            count=count,
            page=page,
            next_link=page_link(f"{ROOT}/{ent.name}", items, options, page),
            root=ROOT,
            warnings=warnings_json(warnings),
        ),
    )


#: a registered type for each Edm type, and the Edm types every value is also written as
EDM_REGISTERED = {
    "Edm.Binary": "bytea",
    "Edm.Boolean": "boolean",
    "Edm.Date": "date",
    "Edm.DateTimeOffset": "timestamptz",
    "Edm.Decimal": "numeric(38,10)",
    "Edm.Double": "double",
    "Edm.Guid": "uuid",
    "Edm.Int16": "smallint",
    "Edm.Int32": "integer",
    "Edm.Int64": "bigint",
    "Edm.SByte": "tinyint",
    "Edm.Single": "float",
    "Edm.String": "varchar",
    "Edm.TimeOfDay": "time",
}


def natural_edm(t: pa.DataType) -> list[str]:
    if pa.types.is_boolean(t):
        return ["Edm.Boolean"]
    if pa.types.is_integer(t) or pa.types.is_dictionary(t) and pa.types.is_integer(t.value_type):
        return ["Edm.Int64", "Edm.Int32", "Edm.Int16", "Edm.SByte", "Edm.Decimal", "Edm.Double"]
    if pa.types.is_floating(t):
        return ["Edm.Double", "Edm.Single", "Edm.Decimal", "Edm.Int64"]
    if pa.types.is_decimal(t):
        return ["Edm.Decimal", "Edm.Double", "Edm.Int64"]
    if pa.types.is_date(t):
        return ["Edm.Date", "Edm.DateTimeOffset"]
    if pa.types.is_timestamp(t):
        return ["Edm.DateTimeOffset", "Edm.Date"]
    if pa.types.is_time(t):
        return ["Edm.TimeOfDay"]
    if (
        pa.types.is_binary(t)
        or pa.types.is_large_binary(t)
        or pa.types.is_fixed_size_binary(t)
        or t == pa.binary_view()
    ):
        return ["Edm.Binary"]
    if isinstance(t, pa.UuidType):
        return ["Edm.Guid", "Edm.Binary"]
    if (
        pa.types.is_string(t)
        or pa.types.is_large_string(t)
        or t == pa.string_view()
        or pa.types.is_dictionary(t)
    ):
        return [
            "Edm.Binary",
            "Edm.Decimal",
            "Edm.Double",
            "Edm.Int64",
            "Edm.DateTimeOffset",
            "Edm.Date",
            "Edm.TimeOfDay",
            "Edm.Guid",
        ]
    return []


ODATA_SCALAR = [(name, arr) for name, arr, modes in CASES if "convert" in modes]


@pytest.mark.parametrize("name,arr", ODATA_SCALAR, ids=[c[0] for c in ODATA_SCALAR])
def test_odata_scalar(name, arr):
    """Each value written as its natural Edm types, as a string and as a boolean."""
    for edm in [*natural_edm(arr.type), "Edm.String", "Edm.Boolean", "Edm.Guid"]:
        ent = entity("Things", [("v", EDM_REGISTERED[edm])])
        odata(pa.table({"v": arr}), ent, ["v"])


STRINGY = [
    "1.5", " 2_000 ", "1e400", "-0", "nan", "Infinity", "-inf", "0x10", "", "abc", "12", "+7", "1_0.5",
    "2020-01-02", "2020-01-02T03:04:05", "2020-01-02 03:04:05.123456789+05:30", "2020-W01-3",
    "20200102T030405Z", "2020-01-02T03:04:05-00:00", "2020-02-30", "10:11:12", "\\xdead", "\\xzz",
    "\\xde ad", "é", "550e8400-e29b-41d4-a716-446655440000", None,
]  # fmt: skip


def test_odata_text_as_every_edm():
    arr = pa.array(STRINGY, pa.string())
    for edm in EDM_REGISTERED:
        ent = entity("Things", [("v", EDM_REGISTERED[edm])])
        for i in range(len(STRINGY)):
            odata(pa.table({"v": arr.slice(i, 1)}), ent, ["v"])


def odata_people(n=6):
    items = [
        '[{"sku": "a", "price": 1.50, "qty": 2, "at": "2020-01-02 03:04:05", "tag": [1, 1.0]}, {"sku": "b", "price": 12345678901234567890.123, "qty": null, "at": "2020-01-02T03:04:05+05:30", "tag": {"x": 1e400}}, {"sku": "c", "price": NaN, "qty": 1, "at": null, "tag": null}]',
        "[]",
        '[{"sku": "z", "price": 0.1, "qty": 9, "at": "2021-06-01", "tag": "s"}]',
    ]
    owner = [
        '{"name": "O\'Brien", "since": "2019-12-31", "score": 3}',
        None,
        '{"name": "é/ü", "since": "2001-01-01", "score": 1.5}',
    ]
    return pa.table(
        {
            "id": pa.array(range(1, n + 1), pa.int64()),
            "name": [f"p'{i} é/?&" for i in range(n)],
            "amount": pa.array(
                [
                    Decimal("1.10"),
                    None,
                    Decimal("-0.00"),
                    Decimal("99999999999999999999.99"),
                    Decimal("0E-2"),
                    Decimal("1"),
                ],
                pa.decimal128(22, 2),
            ).slice(0, n),
            "born": pa.array(
                [
                    date(2000, 1, 1),
                    None,
                    date(1999, 12, 31),
                    date(1, 1, 1),
                    date(9999, 12, 31),
                    date(2020, 2, 29),
                ],
                pa.date32(),
            ).slice(0, n),
            "seen": pa.array(
                [0, None, 2**63 - 1, -(2**63 - 1), 1_000_000_123, 5], pa.timestamp("us", tz="UTC")
            ).slice(0, n),
            "items": [items[i % 3] for i in range(n)],
            "owner": [owner[i % 3] for i in range(n)],
            "guid": pa.array([uuid.UUID(int=i).bytes for i in range(n)], pa.uuid()),
        }
    )


PEOPLE = entity(
    "People",
    [
        ("id", "bigint"),
        ("name", "varchar"),
        ("amount", "numeric(22,2)"),
        ("born", "date"),
        ("seen", "timestamptz"),
        ("guid", "uuid"),
    ],
    key=("id",),
)
ITEMS = entity(
    "Items",
    [
        ("sku", "varchar"),
        ("price", "numeric(38,6)"),
        ("qty", "integer"),
        ("at", "timestamp"),
        ("tag", "json"),
    ],
    key=("sku",),
)
OWNERS = entity("Owners", [("name", "varchar"), ("since", "date"), ("score", "double")])


@pytest.mark.parametrize("page", [0, 1, 2, 5000])
@pytest.mark.parametrize("count", [None, 42])
@pytest.mark.parametrize("warned", [False, True])
def test_odata_read(page, count, warned):
    t = odata_people()
    warnings = [{"code": "rows.cut", "params": {}, "message": "cut"}] if warned else None
    odata(
        t,
        PEOPLE,
        ["id", "name", "amount", "born", "seen", "guid"],
        page=page,
        count=count,
        warnings=warnings,
    )
    odata(t.slice(0, 0), PEOPLE, ["id", "name"], page=page, count=count, warnings=warnings)


@pytest.mark.parametrize(
    "key", [("id",), ("name",), ("amount",), ("born",), ("seen",), ("guid",), ("id", "name"), ()]
)
@pytest.mark.parametrize("item_page", [1, 2, 3, 1000])
def test_odata_expand(key, item_page):
    t = odata_people()
    items_plan = plan(
        nav("items", ITEMS, True),
        ITEMS,
        key,
        ["sku", "price", "qty", "at", "tag"],
        page=item_page,
        raw=(("$top", str(item_page)), ("$select", "sku,price"), ("$orderby", "sku desc")),
    )
    owner_plan = plan(
        nav("owner", OWNERS, False),
        OWNERS,
        key,
        ["name", "since", "score"],
        page=1,
        probe=0,
        options=QueryOptions(),
    )
    odata(t, PEOPLE, ["id", "name", "amount"], [items_plan, owner_plan])
    odata(t, PEOPLE, ["name"], [owner_plan, items_plan], page=2)


def test_odata_expand_zero_page():
    t = odata_people()
    items_plan = plan(nav("items", ITEMS, True), ITEMS, ("id",), ["sku"], page=0, probe=0)
    odata(t.drop_columns(["items"]), PEOPLE, ["id"], [items_plan])


def test_odata_nested_expand():
    """An expansion inside an expansion: the inner list's nextLink addresses its parent item."""
    rows = [
        '[{"sku": "a", "parts": [{"pn": 1}, {"pn": 2}, {"pn": 3}]}, {"sku": "b", "parts": []}]',
        '[{"sku": "c\'d", "parts": [{"pn": 4}]}]',
    ]
    t = pa.table({"id": pa.array([1, 2], pa.int64()), "items": rows})
    parts = entity("Parts", [("pn", "integer")], key=("pn",))
    items = entity("Items", [("sku", "varchar")], key=("sku",))
    inner = plan(nav("parts", parts, True), parts, ("sku",), ["pn"], page=2)
    outer = plan(nav("items", items, True), items, ("id",), ["sku"], page=5, children=(inner,))
    odata(t, PEOPLE, ["id"], [outer])


def test_odata_errors():
    """A value whose Edm type cannot write it, a field the row lacks, malformed JSON in an
    expansion: both raise."""
    ent = entity("Things", [("v", "integer")])
    odata(pa.table({"v": ["x"]}), ent, ["v"])
    odata(pa.table({"v": [1]}), ent, ["v"], names=["w"])
    t = pa.table({"id": [1], "items": ["[{bad"]})
    odata(t, PEOPLE, ["id"], [plan(nav("items", ITEMS, True), ITEMS, ("id",), ["sku"], page=2)])
    t = pa.table({"id": [1], "items": [None]})
    odata(t, PEOPLE, ["id"], [plan(nav("items", ITEMS, True), ITEMS, ("id",), ["sku"], page=2)])


@pytest.mark.parametrize("seed", range(300))
def test_odata_random(seed):
    """Random tables written as random Edm types."""
    r = random.Random(seed)
    cols = rand_table(r, r.randint(0, 30))
    names = list(cols)
    props = [(n, r.choice(list(EDM_REGISTERED.values()))) for n in names]
    ent = entity("R", props)
    t = pa.table({n: cols[n][1] for n in names})
    odata(t, ent, r.sample(names, r.randint(1, len(names))), page=r.choice([1, 5, 5000]))


def test_json_columns_all_surfaces():
    arr = pa.array(JSON_TEXT, pa.string())
    for i in range(len(JSON_TEXT)):
        t = pa.table({"id": [1], "v": arr.slice(i, 1)})
        rest(t, [C("id"), C("v", json=True)])
        jsonapi(t, [C("id"), C("v", json=True)])


def test_zz_tally():
    print(f"\nTALLY {dict(TALLY)}")
    assert TALLY["odata.cases"] > 0


@pytest.mark.parametrize("depth", range(248, 256))
def test_depth_boundaries(depth):
    """orjson refuses nesting past 255 levels of the whole body: each envelope sits at its own
    depth, so the boundary moves with the surface."""
    for inner in ("", "1"):
        text = "[" * depth + inner + "]" * depth
        t = pa.table({"id": [1], "v": [text]})
        rest(t, [C("id"), C("v", json=True)])
        jsonapi(t, [C("id"), C("v", json=True)])
        a = pa.table({"v": [text]})
        agg = [C("v", "aggregate", json=True)]
        compare(
            "rest",
            lambda a=a: ref.rest(a.schema, a.to_batches(), agg, "t", "aggregate"),
            lambda a=a: rs.encode_rest(a.to_batches(), agg, "aggregate"),
        )
