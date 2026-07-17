# Copyright (c) 2026 Kenneth Stott
# Canary: 80906bda-5f5e-425d-b363-edf86ffdbe2d
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for GraphQL remote field_overrides requirements: REQ-597"""

from __future__ import annotations

import inspect


# ---------------------------------------------------------------------------
# REQ-597: GraphQL remote source registration accepts a `field_overrides` map
# ({"fieldName": "query" | "mutation"}) applied after introspection, taking
# priority over structural classification. Only query-type fields can be
# reclassified as mutations; mutation-type fields have no override path.
# ---------------------------------------------------------------------------


def test_map_schema_accepts_field_overrides_parameter():
    # REQ-597: map_schema must accept a field_overrides dict parameter.
    from provisa.graphql_remote.mapper import map_schema

    sig = inspect.signature(map_schema)
    assert "field_overrides" in sig.parameters


def test_map_schema_field_overrides_defaults_to_none():
    # REQ-597: field_overrides must be optional (defaults to None).
    from provisa.graphql_remote.mapper import map_schema

    sig = inspect.signature(map_schema)
    param = sig.parameters["field_overrides"]
    assert param.default is None


def test_process_query_fields_accepts_field_overrides():
    # REQ-597: _process_query_fields receives field_overrides dict.
    from provisa.graphql_remote.mapper import _process_query_fields

    sig = inspect.signature(_process_query_fields)
    assert "field_overrides" in sig.parameters


def test_field_override_mutation_reclassifies_query_field_as_function():
    # REQ-597: A query-type field with override "mutation" is classified as a
    # tracked function (mutation), not a virtual table.
    from provisa.graphql_remote.mapper import _process_query_fields

    query_type = {
        "fields": [
            {
                "name": "createOrder",
                "type": {"kind": "OBJECT", "name": "Order", "ofType": None},
                "args": [],
                "description": None,
            }
        ]
    }
    tables, functions = _process_query_fields(
        query_type,
        namespace="test",
        source_id="src1",
        domain_id="",
        types=[
            {
                "name": "Order",
                "kind": "OBJECT",
                "fields": [
                    {
                        "name": "id",
                        "type": {"kind": "SCALAR", "name": "ID", "ofType": None},
                        "args": [],
                    }
                ],
                "interfaces": [],
                "enumValues": None,
                "inputFields": None,
            }
        ],
        field_overrides={"createOrder": "mutation"},
        max_object_depth=5,
        max_list_depth=2,
        max_list_items=100,
    )
    # Field overridden to mutation → should appear as a function, not a table
    function_names = [f["name"] for f in functions]
    table_names = [t["name"] for t in tables]
    assert "test__createOrder" in function_names
    assert "test__createOrder" not in table_names


def test_field_override_takes_priority_over_structural_classification():
    # REQ-597: override takes priority — a scalar-returning field normally classified
    # as a function stays as a function even if its name matches a table.
    # Conversely, an OBJECT field overridden to "mutation" becomes a function.
    from provisa.graphql_remote.mapper import _process_query_fields

    query_type = {
        "fields": [
            {
                "name": "orders",
                "type": {
                    "kind": "LIST",
                    "name": None,
                    "ofType": {"kind": "OBJECT", "name": "Order", "ofType": None},
                },
                "args": [],
                "description": None,
            }
        ]
    }
    # Without override, LIST of OBJECT → table. With "mutation" override → function.
    _, functions = _process_query_fields(
        query_type,
        namespace="test",
        source_id="src1",
        domain_id="",
        types=[
            {
                "name": "Order",
                "kind": "OBJECT",
                "fields": [
                    {
                        "name": "id",
                        "type": {"kind": "SCALAR", "name": "ID", "ofType": None},
                        "args": [],
                    }
                ],
                "interfaces": [],
                "enumValues": None,
                "inputFields": None,
            }
        ],
        field_overrides={"orders": "mutation"},
        max_object_depth=5,
        max_list_depth=2,
        max_list_items=100,
    )
    function_names = [f["name"] for f in functions]
    assert "test__orders" in function_names


def test_field_override_query_reclassifies_scalar_field_as_table():
    # REQ-597: override "query" for a query-type field keeps it as a queryable table.
    # Structural classification already puts OBJECT returns in tables;
    # explicit "query" override must not push it to functions.
    from provisa.graphql_remote.mapper import _process_query_fields

    query_type = {
        "fields": [
            {
                "name": "users",
                "type": {"kind": "OBJECT", "name": "User", "ofType": None},
                "args": [],
                "description": None,
            }
        ]
    }
    tables, functions = _process_query_fields(
        query_type,
        namespace="test",
        source_id="src1",
        domain_id="",
        types=[
            {
                "name": "User",
                "kind": "OBJECT",
                "fields": [
                    {
                        "name": "id",
                        "type": {"kind": "SCALAR", "name": "ID", "ofType": None},
                        "args": [],
                    }
                ],
                "interfaces": [],
                "enumValues": None,
                "inputFields": None,
            }
        ],
        field_overrides={"users": "query"},
        max_object_depth=5,
        max_list_depth=2,
        max_list_items=100,
    )
    table_names = [t["name"] for t in tables]
    function_names = [f["name"] for f in functions]
    assert "test__users" in table_names
    assert "test__users" not in function_names


def test_map_schema_passes_overrides_to_process_query_fields():
    # REQ-597: map_schema must forward field_overrides to internal processing.
    # A field overridden to mutation in map_schema must appear in functions output.
    from provisa.graphql_remote.mapper import map_schema

    schema = {
        "types": [
            {
                "name": "Query",
                "kind": "OBJECT",
                "fields": [
                    {
                        "name": "submitOrder",
                        "type": {"kind": "OBJECT", "name": "Order", "ofType": None},
                        "args": [],
                        "description": None,
                    }
                ],
                "interfaces": [],
                "enumValues": None,
                "inputFields": None,
            },
            {
                "name": "Order",
                "kind": "OBJECT",
                "fields": [
                    {
                        "name": "id",
                        "type": {"kind": "SCALAR", "name": "ID", "ofType": None},
                        "args": [],
                    }
                ],
                "interfaces": [],
                "enumValues": None,
                "inputFields": None,
            },
        ],
        "queryType": {"name": "Query"},
        "mutationType": None,
    }
    tables, functions, _ = map_schema(
        schema,
        namespace="test",
        source_id="src1",
        field_overrides={"submitOrder": "mutation"},
    )
    function_names = [f["name"] for f in functions]
    table_names = [t["name"] for t in tables]
    assert "test__submitOrder" in function_names
    assert "test__submitOrder" not in table_names
