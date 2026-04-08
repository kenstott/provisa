# Copyright (c) 2026 Kenneth Stott
# Canary: 28ffa9ec-6b6e-4da6-9c5b-0816ef0597e2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for graphql_remote mapper (REQ-308, REQ-312)."""
import pytest
from provisa.graphql_remote.mapper import map_schema, _unwrap_type, _gql_to_provisa_type


def _make_schema(query_fields=None, mutation_fields=None, extra_types=None):
    types = []
    if query_fields is not None:
        types.append({"kind": "OBJECT", "name": "Query", "fields": query_fields})
    if mutation_fields is not None:
        types.append({"kind": "OBJECT", "name": "Mutation", "fields": mutation_fields})
    if extra_types:
        types.extend(extra_types)
    return {
        "queryType": {"name": "Query"} if query_fields is not None else None,
        "mutationType": {"name": "Mutation"} if mutation_fields is not None else None,
        "types": types,
    }


def _scalar_type(name):
    return {"kind": "SCALAR", "name": name, "ofType": None}


def _object_type(name):
    return {"kind": "OBJECT", "name": name, "ofType": None}


def _list_of_object(name):
    return {"kind": "LIST", "name": None, "ofType": _object_type(name)}


def _non_null(inner):
    return {"kind": "NON_NULL", "name": None, "ofType": inner}


# --- _unwrap_type tests ---

def test_unwrap_scalar():
    t = _scalar_type("String")
    assert _unwrap_type(t) == ("SCALAR", "String")


def test_unwrap_non_null_scalar():
    t = _non_null(_scalar_type("Int"))
    assert _unwrap_type(t) == ("SCALAR", "Int")


def test_unwrap_list_of_object():
    t = _list_of_object("User")
    assert _unwrap_type(t) == ("OBJECT", "User")


def test_unwrap_non_null_list_of_object():
    t = _non_null(_list_of_object("User"))
    assert _unwrap_type(t) == ("OBJECT", "User")


# --- _gql_to_provisa_type tests ---

def test_scalar_string_maps_to_text():
    assert _gql_to_provisa_type(_scalar_type("String")) == "text"


def test_scalar_id_maps_to_text():
    assert _gql_to_provisa_type(_scalar_type("ID")) == "text"


def test_scalar_int_maps_to_integer():
    assert _gql_to_provisa_type(_scalar_type("Int")) == "integer"


def test_scalar_float_maps_to_numeric():
    assert _gql_to_provisa_type(_scalar_type("Float")) == "numeric"


def test_scalar_boolean_maps_to_boolean():
    assert _gql_to_provisa_type(_scalar_type("Boolean")) == "boolean"


def test_unknown_scalar_maps_to_text():
    assert _gql_to_provisa_type(_scalar_type("DateTime")) == "text"


def test_object_type_maps_to_jsonb():
    assert _gql_to_provisa_type(_object_type("User")) == "jsonb"


def test_non_null_object_maps_to_jsonb():
    assert _gql_to_provisa_type(_non_null(_object_type("User"))) == "jsonb"


# --- map_schema tests ---

def test_query_field_with_scalar_fields_produces_virtual_table():
    user_type = {
        "kind": "OBJECT",
        "name": "User",
        "fields": [
            {"name": "id", "type": _scalar_type("ID")},
            {"name": "name", "type": _scalar_type("String")},
            {"name": "age", "type": _scalar_type("Int")},
        ],
    }
    schema = _make_schema(
        query_fields=[{"name": "users", "type": _list_of_object("User"), "args": []}],
        extra_types=[user_type],
    )
    tables, functions = map_schema(schema, "myns", "src1")
    assert len(tables) == 1
    t = tables[0]
    assert t["name"] == "myns__users"
    assert t["field_name"] == "users"
    assert t["source_id"] == "src1"
    cols = {c["name"]: c["type"] for c in t["columns"]}
    assert cols["id"] == "text"
    assert cols["name"] == "text"
    assert cols["age"] == "integer"


def test_namespace_prefix_applied():
    user_type = {"kind": "OBJECT", "name": "Product", "fields": [{"name": "sku", "type": _scalar_type("String")}]}
    schema = _make_schema(
        query_fields=[{"name": "products", "type": _object_type("Product"), "args": []}],
        extra_types=[user_type],
    )
    tables, _ = map_schema(schema, "shop", "src2")
    assert tables[0]["name"] == "shop__products"


def test_non_scalar_nested_field_becomes_jsonb():
    address_type = {"kind": "OBJECT", "name": "Address", "fields": [{"name": "city", "type": _scalar_type("String")}]}
    user_type = {
        "kind": "OBJECT",
        "name": "User",
        "fields": [
            {"name": "id", "type": _scalar_type("ID")},
            {"name": "address", "type": _object_type("Address")},
        ],
    }
    schema = _make_schema(
        query_fields=[{"name": "users", "type": _object_type("User"), "args": []}],
        extra_types=[user_type, address_type],
    )
    tables, _ = map_schema(schema, "ns", "src3")
    cols = {c["name"]: c["type"] for c in tables[0]["columns"]}
    assert cols["address"] == "jsonb"
    assert cols["id"] == "text"


def test_empty_query_type_no_tables():
    schema = _make_schema(query_fields=[])
    tables, functions = map_schema(schema, "ns", "src")
    assert tables == []
    assert functions == []


def test_multiple_query_fields_produce_multiple_tables():
    type_a = {"kind": "OBJECT", "name": "TypeA", "fields": [{"name": "x", "type": _scalar_type("Int")}]}
    type_b = {"kind": "OBJECT", "name": "TypeB", "fields": [{"name": "y", "type": _scalar_type("String")}]}
    schema = _make_schema(
        query_fields=[
            {"name": "things_a", "type": _object_type("TypeA"), "args": []},
            {"name": "things_b", "type": _object_type("TypeB"), "args": []},
        ],
        extra_types=[type_a, type_b],
    )
    tables, _ = map_schema(schema, "ns", "src")
    assert len(tables) == 2
    names = {t["name"] for t in tables}
    assert "ns__things_a" in names
    assert "ns__things_b" in names


def test_scalar_query_fields_are_skipped():
    """Top-level query fields with scalar return type should not produce tables."""
    schema = _make_schema(
        query_fields=[{"name": "ping", "type": _scalar_type("String"), "args": []}],
    )
    tables, _ = map_schema(schema, "ns", "src")
    assert tables == []


def test_mutation_field_produces_tracked_function():
    result_type = {
        "kind": "OBJECT",
        "name": "CreateUserResult",
        "fields": [{"name": "id", "type": _scalar_type("ID")}, {"name": "ok", "type": _scalar_type("Boolean")}],
    }
    schema = _make_schema(
        query_fields=[],
        mutation_fields=[{
            "name": "createUser",
            "type": _object_type("CreateUserResult"),
            "args": [
                {"name": "name", "type": _scalar_type("String")},
                {"name": "age", "type": _scalar_type("Int")},
            ],
        }],
        extra_types=[result_type],
    )
    _, functions = map_schema(schema, "api", "src")
    assert len(functions) == 1
    fn = functions[0]
    assert fn["name"] == "api__createUser"
    assert fn["field_name"] == "createUser"
    args = {a["name"]: a["type"] for a in fn["arguments"]}
    assert args["name"] == "text"
    assert args["age"] == "integer"
    rs = {r["name"]: r["type"] for r in fn["return_schema"]}
    assert rs["id"] == "text"
    assert rs["ok"] == "boolean"


def test_domain_id_propagated():
    user_type = {"kind": "OBJECT", "name": "User", "fields": [{"name": "id", "type": _scalar_type("ID")}]}
    schema = _make_schema(
        query_fields=[{"name": "users", "type": _object_type("User"), "args": []}],
        extra_types=[user_type],
    )
    tables, _ = map_schema(schema, "ns", "src", domain_id="dom-1")
    assert tables[0]["domain_id"] == "dom-1"


def test_no_mutation_type_no_functions():
    user_type = {"kind": "OBJECT", "name": "User", "fields": [{"name": "id", "type": _scalar_type("ID")}]}
    schema = _make_schema(
        query_fields=[{"name": "users", "type": _object_type("User"), "args": []}],
        extra_types=[user_type],
    )
    # no mutation_fields → mutationType is None
    _, functions = map_schema(schema, "ns", "src")
    assert functions == []
