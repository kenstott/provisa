# Copyright (c) 2026 Kenneth Stott
# Canary: 8d1e5a27-4c3b-49f0-a6e2-7b9c0d3f1e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1965 (amended 2026-11-11, TYPED ARRAYS ARE AN IR TYPE): an array of a primitive, at its
depth, is a type of the IR, listed natively per protocol and held natively or as JSON per store."""

# Requirements: REQ-1965

from __future__ import annotations

import json
import pathlib
import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pytest
import sqlalchemy as sa
from sqlalchemy.exc import StatementError

from provisa.compiler.type_map import nested_column_graphql_type, typed_array_graphql_type
from provisa.core.catalog_types import column_catalog_arrow_type
from provisa.core.ir_arrays import (
    ArrayType,
    ArrayValueRefused,
    TypedArray,
    array_of,
    column_ir_type,
)
from provisa.core.ir_types import column_typing, to_physical, to_sqlalchemy
from provisa.core.json_refusal import NonFiniteNumber
from provisa.grpc.proto_gen import typed_array_proto


# ---- the IR -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("native", "platform", "ir"),
    [
        ("array(integer)", "trino", "integer[]"),
        ("array(array(varchar))", "trino", "text[][]"),
        ("array(array(decimal(10,2)))", "trino", "numeric(10,2)[][]"),
        ("int4[]", "postgresql", "integer[]"),
        ("INTEGER[][]", "duckdb", "integer[][]"),
    ],
)
def test_an_array_of_a_primitive_round_trips_through_the_ir_at_its_depth(native, platform, ir):
    typing = column_typing(native, platform)
    name = column_ir_type(typing.data_type, typing.array_depth, typing.element_type)
    assert name == ir
    parsed = array_of(name)
    assert parsed is not None
    assert (parsed.element, parsed.depth) == (typing.element_type, typing.array_depth)
    assert parsed.name == name


@pytest.mark.parametrize("name", ["json[]", "vector[]", "text", "struct[]", "int[]", ""])
def test_only_an_array_of_a_canonical_primitive_is_a_typed_array(name):
    assert array_of(name) is None


def test_a_struct_or_a_map_stays_a_json_column():
    typing = column_typing("array(row(a integer))", "trino")
    assert column_ir_type(typing.data_type, typing.array_depth, typing.element_type) == "json"


def test_a_store_with_an_array_type_holds_it_natively_at_its_depth():
    assert to_physical("integer[]", "postgresql") == "INTEGER[]"
    assert to_physical("numeric(10,2)[][]", "postgresql") == "NUMERIC(10, 2)[][]"


def test_a_store_without_an_array_type_holds_it_as_json():
    assert to_physical("integer[][]", "sqlite") == "JSON"


# ---- an engine without native arrays: JSON in, the typed array out ------------------------------


def _round_trip(engine: sa.Engine, ir: str, value: list) -> tuple[object, list]:
    meta = sa.MetaData()
    table = sa.Table("t", meta, sa.Column("a", to_sqlalchemy(ir)))
    meta.create_all(engine)
    with engine.begin() as conn:
        conn.execute(table.insert().values(a=value))
        raw = conn.exec_driver_sql("SELECT a FROM t").scalar_one()
        read = conn.execute(sa.select(table.c.a)).scalar_one()
    return raw, read


@pytest.mark.parametrize(
    ("ir", "value"),
    [
        ("integer[]", [1, None, 3]),
        ("date[]", [date(2026, 11, 11), None]),
        ("numeric(10,2)[][]", [[Decimal("1.10"), None], [Decimal("12345678.99")]]),
        (
            "timestamptz[][]",
            [[datetime(2026, 1, 2, 3, 4, 5, 600000, tzinfo=timezone.utc)], []],
        ),
        ("bytea[]", [b"\x00\xff"]),
    ],
)
def test_an_engine_without_arrays_stores_json_and_reads_back_the_typed_array(ir, value):
    raw, read = _round_trip(sa.create_engine("sqlite://"), ir, value)
    # Held as JSON, a decimal as its digits so it is exact.
    assert isinstance(raw, str)
    json.loads(raw)
    if ir.startswith("numeric"):
        assert json.loads(raw)[0][0] == "1.10"
    assert read == value


def test_an_engine_with_arrays_holds_the_typed_array_natively():
    raw, read = _round_trip(sa.create_engine("duckdb:///:memory:"), "integer[][]", [[1, None], [3]])
    assert raw == [[1, None], [3]]
    assert read == [[1, None], [3]]


def test_a_value_deeper_than_the_array_is_refused_by_name():
    with pytest.raises(StatementError) as caught:
        _round_trip(sa.create_engine("sqlite://"), "integer[]", [[1]])
    assert isinstance(caught.value.orig, ArrayValueRefused)
    assert caught.value.orig.code == "array.value_refused"


def test_a_float_that_json_cannot_hold_is_refused_by_name():
    with pytest.raises(StatementError) as caught:
        _round_trip(sa.create_engine("sqlite://"), "double[]", [float("nan")])
    assert isinstance(caught.value.orig, NonFiniteNumber)


def test_the_store_type_is_the_typed_array():
    stored = to_sqlalchemy("text[][]")
    assert isinstance(stored, TypedArray)
    assert stored.array == ArrayType("text", 2)


# ---- each protocol lists it natively -------------------------------------------------------------


def test_graphql_declares_a_typed_array_as_a_list_nested_to_its_depth():
    assert str(typed_array_graphql_type(ArrayType("integer", 1))) == "[Int]"
    assert str(typed_array_graphql_type(ArrayType("text", 2))) == "[[String]]"
    # a decimal with a declared precision is served exact, as a string
    assert str(typed_array_graphql_type(ArrayType("numeric(10,2)", 1))) == "[String]"


def test_a_registered_array_column_is_a_graphql_list_from_its_attributes():
    typing = column_typing("array(array(bigint))", "trino")
    column = {
        "native_type": typing.native_type,
        "native_platform": typing.native_platform,
        "array_depth": typing.array_depth,
        "element_type": typing.element_type,
    }
    assert str(nested_column_graphql_type(column)) == "[[BigInt]]"


def test_grpc_carries_a_typed_array_as_a_repeated_field():
    assert typed_array_proto(ArrayType("integer", 1)) == ("repeated int32", [])
    assert typed_array_proto(ArrayType("timestamp", 1)) == (
        "repeated google.protobuf.Timestamp",
        [],
    )


def test_grpc_carries_a_deeper_array_as_repeated_messages_of_the_level_below():
    field, messages = typed_array_proto(ArrayType("text", 3))
    assert field == "repeated ProvisaStringArray2"
    assert messages == [
        "message ProvisaStringArray {\n  repeated string items = 1;\n}",
        "message ProvisaStringArray2 {\n  repeated ProvisaStringArray items = 1;\n}",
    ]


def test_grpc_typed_array_messages_compile():
    from google.protobuf import descriptor_pb2
    from grpc_tools import protoc

    field, messages = typed_array_proto(ArrayType("bigint", 2))
    source = 'syntax = "proto3";\npackage t;\n' + "\n".join(messages)
    source += f"\nmessage Row {{\n  {field} a = 1;\n}}\n"
    with tempfile.TemporaryDirectory() as tmp:
        pathlib.Path(tmp, "row.proto").write_text(source)
        out = pathlib.Path(tmp, "row.pb")
        assert protoc.main(["protoc", f"-I{tmp}", f"--descriptor_set_out={out}", "row.proto"]) == 0
        files = descriptor_pb2.FileDescriptorSet.FromString(out.read_bytes())
    repeated = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
    by_name = {m.name: m for m in files.file[0].message_type}
    (a,) = by_name["Row"].field
    assert (a.label, a.type_name) == (repeated, ".t.ProvisaInt64Array")
    (items,) = by_name["ProvisaInt64Array"].field
    assert (items.label, items.type) == (repeated, items.TYPE_INT64)


def test_the_catalog_lists_a_typed_array_as_an_arrow_list_at_its_depth():
    assert column_catalog_arrow_type("json", 1, "integer") == pa.list_(pa.int32())
    assert column_catalog_arrow_type("json", 2, "numeric(10,2)") == pa.list_(
        pa.list_(pa.decimal128(10, 2))
    )
    # a struct or a map stays the JSON column it is
    assert column_catalog_arrow_type("json", 0, None) == pa.utf8()
