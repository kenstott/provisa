# Copyright (c) 2026 Kenneth Stott
# Canary: 61eb9d39-096d-4c68-ba64-880b018b3e6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/grpc_remote/mapper.py — pure business logic, no I/O."""

from provisa.grpc_remote.mapper import (
    map_proto,
)

# Methods:
#   ListOrders   — server_streaming=True  → query (signal: streaming)
#   BrowseOrders — unary, returns OrderPage (has repeated Order items) → query (signal: repeated message field)
#   GetOrder     — unary, returns Order (no repeated message fields) → mutation
#   CreateOrder  — unary, returns Order → mutation
#   DeleteOrder  — unary, returns DeleteResponse → mutation
PROTO_DICT = {
    "package": "orders",
    "services": [
        {
            "name": "OrderService",
            "methods": [
                {
                    "name": "ListOrders",
                    "input_type": "ListRequest",
                    "output_type": "Order",
                    "server_streaming": True,
                    "client_streaming": False,
                },
                {
                    "name": "BrowseOrders",
                    "input_type": "BrowseRequest",
                    "output_type": "OrderPage",
                    "server_streaming": False,
                    "client_streaming": False,
                },
                {
                    "name": "GetOrder",
                    "input_type": "GetOrderRequest",
                    "output_type": "Order",
                    "server_streaming": False,
                    "client_streaming": False,
                },
                {
                    "name": "CreateOrder",
                    "input_type": "CreateOrderRequest",
                    "output_type": "Order",
                    "server_streaming": False,
                    "client_streaming": False,
                },
                {
                    "name": "DeleteOrder",
                    "input_type": "DeleteOrderRequest",
                    "output_type": "DeleteResponse",
                    "server_streaming": False,
                    "client_streaming": False,
                },
            ],
        }
    ],
    "messages": {
        "ListRequest": [{"name": "page", "type": "int32", "repeated": False}],
        "BrowseRequest": [{"name": "filter", "type": "string", "repeated": False}],
        "GetOrderRequest": [{"name": "order_id", "type": "string", "repeated": False}],
        "Order": [
            {"name": "id", "type": "string", "repeated": False},
            {"name": "amount", "type": "double", "repeated": False},
            {"name": "shipped", "type": "bool", "repeated": False},
            {
                "name": "tags",
                "type": "string",
                "repeated": True,
            },  # repeated scalar — does NOT trigger query
        ],
        "OrderPage": [
            {
                "name": "items",
                "type": "Order",
                "repeated": True,
            },  # repeated message — triggers query
            {"name": "total", "type": "int32", "repeated": False},
        ],
        "CreateOrderRequest": [
            {"name": "customer_id", "type": "string", "repeated": False},
            {"name": "amount", "type": "double", "repeated": False},
        ],
        "DeleteOrderRequest": [{"name": "order_id", "type": "string", "repeated": False}],
        "DeleteResponse": [{"name": "success", "type": "bool", "repeated": False}],
    },
    "enums": [],
}


def test_map_proto_query_count():
    queries, mutations = map_proto(PROTO_DICT, "", "src", "dom")
    assert len(queries) == 2  # ListOrders (streaming), BrowseOrders (repeated message field)


def test_map_proto_mutation_count():
    queries, mutations = map_proto(PROTO_DICT, "", "src", "dom")
    assert len(mutations) == 3  # GetOrder, CreateOrder, DeleteOrder


def test_server_streaming_is_query():
    queries, _ = map_proto(PROTO_DICT, "", "src", "dom")
    assert any(q.method == "ListOrders" for q in queries)


def test_repeated_message_field_is_query():
    queries, _ = map_proto(PROTO_DICT, "", "src", "dom")
    assert any(q.method == "BrowseOrders" for q in queries)


def test_repeated_scalar_field_does_not_trigger_query():
    # GetOrder returns Order which has repeated string tags — should be mutation
    _, mutations = map_proto(PROTO_DICT, "", "src", "dom")
    assert any(m.method == "GetOrder" for m in mutations)


def test_unary_single_entity_is_mutation():
    _, mutations = map_proto(PROTO_DICT, "", "src", "dom")
    assert any(m.method == "CreateOrder" for m in mutations)
    assert any(m.method == "DeleteOrder" for m in mutations)


def test_override_query_forces_query():
    queries, mutations = map_proto(
        PROTO_DICT,
        "",
        "src",
        "dom",
        method_overrides={"GetOrder": "query"},
    )
    assert any(q.method == "GetOrder" for q in queries)
    assert not any(m.method == "GetOrder" for m in mutations)


def test_override_mutation_forces_mutation():
    queries, mutations = map_proto(
        PROTO_DICT,
        "",
        "src",
        "dom",
        method_overrides={"ListOrders": "mutation"},
    )
    assert any(m.method == "ListOrders" for m in mutations)
    assert not any(q.method == "ListOrders" for q in queries)


def test_override_query_ignored_for_scalar_output():
    d = {
        "package": "",
        "services": [
            {
                "name": "Svc",
                "methods": [
                    {
                        "name": "GetVersion",
                        "input_type": "Empty",
                        "output_type": "string",
                        "server_streaming": False,
                        "client_streaming": False,
                    },
                ],
            }
        ],
        "messages": {"Empty": []},
        "enums": [],
    }
    queries, mutations = map_proto(d, "", "src", "dom", method_overrides={"GetVersion": "query"})
    assert len(queries) == 0
    assert len(mutations) == 1
    assert mutations[0].return_columns[0].name == "value"


def test_full_method_path_with_package():
    queries, _ = map_proto(PROTO_DICT, "", "src", "dom")
    list_orders = next(q for q in queries if q.method == "ListOrders")
    assert list_orders.full_method_path == "/orders.OrderService/ListOrders"


def test_full_method_path_no_package():
    d = {**PROTO_DICT, "package": ""}
    queries, _ = map_proto(d, "", "src", "dom")
    list_orders = next(q for q in queries if q.method == "ListOrders")
    assert list_orders.full_method_path == "/OrderService/ListOrders"


def test_server_streaming_flag_propagated():
    queries, _ = map_proto(PROTO_DICT, "", "src", "dom")
    list_orders = next(q for q in queries if q.method == "ListOrders")
    assert list_orders.server_streaming is True
    browse_orders = next(q for q in queries if q.method == "BrowseOrders")
    assert browse_orders.server_streaming is False


def test_repeated_scalar_maps_to_jsonb():
    _, mutations = map_proto(PROTO_DICT, "", "src", "dom")
    get_order = next(m for m in mutations if m.method == "GetOrder")
    col = next(c for c in get_order.return_columns if c.name == "tags")
    assert col.type == "jsonb"


def test_scalar_type_mapping():
    _, mutations = map_proto(PROTO_DICT, "", "src", "dom")
    get_order = next(m for m in mutations if m.method == "GetOrder")
    cols = {c.name: c.type for c in get_order.return_columns}
    assert cols["id"] == "text"
    assert cols["amount"] == "numeric"
    assert cols["shipped"] == "boolean"


def test_namespace_does_not_affect_service():
    queries, mutations = map_proto(PROTO_DICT, "myns", "src", "dom")
    for q in queries:
        assert q.service == "OrderService"


def test_enum_type_maps_to_text():
    d = {
        "package": "",
        "services": [
            {
                "name": "Svc",
                "methods": [
                    {
                        "name": "GetItem",
                        "input_type": "Req",
                        "output_type": "Res",
                        "server_streaming": False,
                        "client_streaming": False,
                    },
                ],
            }
        ],
        "messages": {
            "Req": [],
            "Res": [{"name": "status", "type": "Status", "repeated": False}],
        },
        "enums": ["Status"],
    }
    _, mutations = map_proto(d, "", "src", "dom")
    col = next(c for c in mutations[0].return_columns if c.name == "status")
    assert col.type == "text"


def test_unknown_message_type_maps_to_jsonb():
    d = {
        "package": "",
        "services": [
            {
                "name": "Svc",
                "methods": [
                    {
                        "name": "GetItem",
                        "input_type": "Req",
                        "output_type": "Res",
                        "server_streaming": False,
                        "client_streaming": False,
                    },
                ],
            }
        ],
        "messages": {
            "Req": [],
            "Res": [{"name": "meta", "type": "SomeNestedMsg", "repeated": False}],
        },
        "enums": [],
    }
    _, mutations = map_proto(d, "", "src", "dom")
    col = next(c for c in mutations[0].return_columns if c.name == "meta")
    assert col.type == "jsonb"


def test_enum_repeated_field_does_not_trigger_query():
    d = {
        "package": "",
        "services": [
            {
                "name": "Svc",
                "methods": [
                    {
                        "name": "GetItem",
                        "input_type": "Req",
                        "output_type": "Res",
                        "server_streaming": False,
                        "client_streaming": False,
                    },
                ],
            }
        ],
        "messages": {
            "Req": [],
            "Res": [{"name": "statuses", "type": "Status", "repeated": True}],
        },
        "enums": ["Status"],
    }
    queries, mutations = map_proto(d, "", "src", "dom")
    assert len(queries) == 0
    assert len(mutations) == 1
