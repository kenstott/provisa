# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The defects the Python JSON path had (rust/encoders/json/RESULTS.md), each fixed in the Rust
encoders or by their wiring, held to its correct answer through the product's encoder path
(provisa.executor.json_encoders)."""

# Requirements: REQ-2194, REQ-1867

from __future__ import annotations

import io
import json
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from provisa.executor import json_encoders as je
from provisa.executor.json_encoders import EncoderFailed
from provisa.executor.serialize import TO_ONE_TRUNCATED, to_one_truncated
from tests.unit.json_encoder.cases import C


def rows(batch: pa.RecordBatch, columns) -> list:
    return json.loads(je.graphql_field([batch], columns).body)


# ------------------------------------------------------------------ (1) dedup by typed value


def test_rows_differing_in_json_type_are_not_one():
    """A JSON string holding "[1]" is not the array [1], and 1, 1.0 and true are three values."""
    texts = ['"[1]"', "[1]", "1", "1.0", "true", "[1]", "1"]
    b = pa.record_batch({"v": pa.array(texts)})
    assert rows(b, [C("v", json=True)]) == [
        {"v": "[1]"},
        {"v": [1]},
        {"v": 1},
        {"v": 1.0},
        {"v": True},
    ]
    assert je.graphql_field([b], [C("v", json=True)]).body.decode() == (
        '[{"v":"[1]"},{"v":[1]},{"v":1},{"v":1.0},{"v":true}]'
    )


def test_list_elements_differing_in_type_are_not_one():
    """Inside a one-to-many list: the number 1.50 and the string "1.50" are two elements."""
    b = pa.record_batch(
        {"id": pa.array([1, 1, 1]), "x": pa.array(["[1.50]", '["1.50"]', "[1.50]"])}
    )
    cols = [C("id"), C("x", "items", "one-to-many", json=True)]
    assert json.loads(je.graphql_field([b], cols).body) == [
        {"id": 1, "items": [{"x": [1.5]}, {"x": ["1.50"]}]}
    ]
    assert b'"items":[{"x":[1.50]},{"x":["1.50"]}]' in je.graphql_field([b], cols).body


# ------------------------------------------------------------------ (2) zones by tzdata


def test_a_named_zone_after_2037_keeps_its_daylight_time():
    """2049-08-14 10:32:28Z in New York is EDT (-04:00), not pytz's -05:00 all year."""
    b = pa.record_batch(
        {"t": pa.array([2512549948 * 10**6], pa.timestamp("us", "America/New_York"))}
    )
    assert rows(b, [C("t")]) == [{"t": "2049-08-14T06:32:28-04:00"}]


def test_local_mean_time_keeps_its_seconds():
    b = pa.record_batch(
        {"t": pa.array([-2524521600 * 10**6], pa.timestamp("us", "Europe/Amsterdam"))}
    )
    assert rows(b, [C("t")]) == [{"t": "1890-01-01T00:17:30+00:17:30"}]


# ------------------------------------------------------------------ (3) nanoseconds


def test_the_int64_minimum_nanosecond_is_an_instant_not_nat():
    b = pa.record_batch({"t": pa.array([-(2**63)], pa.timestamp("ns"))})
    assert rows(b, [C("t")]) == [{"t": "1677-09-21T00:12:43.145224"}]


def test_a_zoned_nanosecond_past_the_int64_local_time_is_written():
    """pandas raised OutOfBoundsDatetime (a 500) where the local time left int64 nanoseconds."""
    b = pa.record_batch({"t": pa.array([2**63 - 1], pa.timestamp("ns", "Pacific/Kiritimati"))})
    assert rows(b, [C("t")]) == [{"t": "2262-04-12T13:47:16.854775+14:00"}]


def test_nanoseconds_are_floored_to_the_microsecond():
    b = pa.record_batch(
        {
            "t": pa.array([-1, 1999], pa.timestamp("ns")),
            "d": pa.array([-1, -(2**63)], pa.duration("ns")),
        }
    )
    assert rows(b, [C("t"), C("d")]) == [
        {"t": "1969-12-31T23:59:59.999999", "d": "-PT0.000001S"},
        {"t": "1970-01-01T00:00:00.000001", "d": "-P106751DT23H47M16.854776S"},
    ]


# ------------------------------------------------------------------ JSON:API (4, 5, 6, 7)

_ORDERS = pa.record_batch(
    {
        "id": pa.array([1, 1, 2]),
        "order_no": pa.array(["A-1", "A-1", "B-2"]),
        "flag": pa.array([True, True, False]),
        "item_id": pa.array([10, 11, 12]),
        "cust_id": pa.array([7, 8, 9]),
    }
)
_ORDER_COLS = [
    C("id"),
    C("order_no"),
    C("flag"),
    C("item_id", "items", "one-to-many", column="item_id"),
    C("cust_id", "customer", "many-to-one", column="cust_id"),
]


def jsonapi(batch, columns, **over):
    document = dict(
        resource_type="orders",
        page_size=10,
        page_number=1,
        probe=True,
        total=None,
        id_columns=["id"],
        scalar_names={},
        rel_names=[("items", "items"), ("customer", "customer")],
        rel_scalar_names={},
        includes=["items"],
        include_keys={"items": ["item_id"], "customer": ["cust_id"]},
        base_path="/data/jsonapi/sales/orders",
        link_params=[],
    )
    document.update(over)
    body, truncated = je.jsonapi([batch], columns, **document)
    return json.loads(body), truncated


def test_an_include_named_twice_is_the_one_include():
    once, _ = jsonapi(_ORDERS, _ORDER_COLS, includes=["items"])
    twice, _ = jsonapi(_ORDERS, _ORDER_COLS, includes=["items", "items"])
    assert twice == once
    assert once["data"][0]["relationships"]["items"] == {
        "data": [{"type": "items", "id": "10"}, {"type": "items", "id": "11"}]
    }


def test_ids_are_the_text_the_answer_writes():
    b = pa.record_batch(
        {
            "flag": pa.array([True]),
            "ratio": pa.array([1e16]),
            "doc": pa.array(['{"a": 1}']),
            "amount": pa.array([Decimal("1.50")], pa.decimal128(10, 2)),
        }
    )
    cols = [C("flag"), C("ratio"), C("doc", json=True), C("amount")]
    for key, text in (("flag", "true"), ("ratio", "1e+16"), ("doc", '{"a":1}'), ("amount", "1.50")):
        doc, _ = jsonapi(b, cols, id_columns=[key], rel_names=[], includes=[], include_keys={})
        assert doc["data"][0]["id"] == text, key


def test_an_id_column_is_an_attribute_when_the_id_is_other_keys():
    doc, _ = jsonapi(_ORDERS, _ORDER_COLS, id_columns=["order_no"], includes=[])
    first = doc["data"][0]
    assert first["id"] == "A-1"
    assert first["attributes"]["id"] == 1
    doc, _ = jsonapi(_ORDERS, _ORDER_COLS, id_columns=["id"], includes=[])
    assert "id" not in doc["data"][0]["attributes"]


# ------------------------------------------------------------------ (7) truncation warnings


def test_a_truncated_to_one_reaches_every_surface():
    """Order 1 matched two customers: only the first was answered, and each surface says so."""
    cols = [C("id"), C("cust_id", "customer", "many-to-one")]
    b = pa.record_batch({"id": pa.array([1, 1, 2]), "cust_id": pa.array([7, 8, 9])})
    assert je.graphql_field([b], cols).truncated == ["customer"]
    assert je.rest([b], cols)[1] == ["customer"]
    assert je.rest_csv([b], cols)[1] == ["customer"]
    doc, truncated = jsonapi(
        b, cols, rel_names=[("customer", "customer")], includes=[], include_keys={}
    )
    assert truncated == ["customer"]
    assert doc["meta"]["warnings"] == [to_one_truncated("customer").as_dict()]
    assert doc["meta"]["warnings"][0]["code"] == TO_ONE_TRUNCATED


def test_the_requests_warnings_come_first_in_meta():
    cols = [C("id"), C("cust_id", "customer", "many-to-one")]
    b = pa.record_batch({"id": pa.array([1, 1]), "cust_id": pa.array([7, 8])})
    earlier = {"code": "x.y", "params": {}, "message": "cut"}
    doc, _ = jsonapi(
        b,
        cols,
        rel_names=[("customer", "customer")],
        includes=[],
        include_keys={},
        warnings=je.warnings_json([earlier]),
    )
    assert doc["meta"]["warnings"] == [earlier, to_one_truncated("customer").as_dict()]


def test_an_unrepeated_to_one_says_nothing():
    cols = [C("id"), C("cust_id", "customer", "many-to-one")]
    b = pa.record_batch({"id": pa.array([1, 1, 2]), "cust_id": pa.array([7, 7, 9])})
    assert je.graphql_field([b], cols).truncated == []


# ------------------------------------------------------------------ (8) REST file formats


def test_rest_parquet_and_arrow_carry_the_engines_types():
    from provisa.api.rest.generator import _arrow_file

    b = pa.record_batch(
        {
            "a": pa.array([Decimal("1.50")], pa.decimal128(10, 2)),
            "b": pa.array([0], pa.timestamp("us", "UTC")),
            "c": pa.array(['{"k": 1}']),
        }
    )
    cols = [C("amount", column="a"), C("at", column="b"), C("doc", column="c", json=True)]
    parquet = _arrow_file([b], cols, "application/vnd.apache.parquet", "t", {})
    table = pq.read_table(io.BytesIO(parquet.body))
    assert table.schema.names == ["amount", "at", "doc"]
    assert table.schema.field("amount").type == pa.decimal128(10, 2)
    assert table.schema.field("at").type == pa.timestamp("us", "UTC")
    assert table.column("amount").to_pylist() == [Decimal("1.50")]
    stream = _arrow_file([b], cols, "application/vnd.apache.arrow.stream", "t", {})
    read = pa.ipc.open_stream(io.BytesIO(stream.body)).read_all()
    assert read.schema.field("amount").type == pa.decimal128(10, 2)
    assert read.column("doc").to_pylist() == ['{"k": 1}']


def test_rest_parquet_of_no_batch_is_the_fields():
    from provisa.api.rest.generator import _arrow_file

    out = _arrow_file([], [C("id")], "application/vnd.apache.parquet", "t", {})
    assert pq.read_table(io.BytesIO(out.body)).schema.names == ["id"]


# ------------------------------------------------------------------ (9) OData Edm types


def odata(values: pa.Array, edm: str) -> dict:
    b = pa.record_batch({"v": values})
    entity = ("T", {"v": edm}, ["v"], [])
    body = je.odata([b], ["v"], entity, context="c", count=None, page=10, next_link="", root="r")
    return json.loads(body)


def test_edm_boolean_reads_text_as_postgresql_does():
    assert [
        e["v"] for e in odata(pa.array(["false", " TRUE ", "of", "y", "0"]), "Edm.Boolean")["value"]
    ] == [
        False,
        True,
        False,
        True,
        False,
    ]
    with pytest.raises(EncoderFailed, match="is no Edm.Boolean"):
        odata(pa.array(["maybe"]), "Edm.Boolean")
    with pytest.raises(EncoderFailed, match="is no Edm.Boolean"):
        odata(pa.array([2]), "Edm.Boolean")


def test_edm_date_and_time_of_day_are_typed():
    assert odata(pa.array(["2020-01-02T03:04:05"]), "Edm.Date")["value"] == [{"v": "2020-01-02"}]
    assert odata(pa.array(["12:30:00"]), "Edm.TimeOfDay")["value"] == [{"v": "12:30:00"}]
    with pytest.raises(EncoderFailed, match="is no Edm.Date"):
        odata(pa.array([7]), "Edm.Date")
    with pytest.raises(EncoderFailed, match="is no Edm.TimeOfDay"):
        odata(pa.array([7]), "Edm.TimeOfDay")


def test_odata_infinite_dates_are_infinity():
    days = pa.array([2**31 - 1, -(2**31 - 1)], pa.date32())
    assert odata(days, "Edm.Date")["value"] == [{"v": "infinity"}, {"v": "-infinity"}]
    stamps = pa.array([2**63 - 1], pa.timestamp("us", "UTC"))
    assert odata(stamps, "Edm.DateTimeOffset")["value"] == [{"v": "infinity"}]
