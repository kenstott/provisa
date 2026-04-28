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
import pytest
from provisa.grpc_remote.mapper import (
    is_query_method,
    map_proto,
    QUERY_PREFIXES,
    GrpcQuery,
    GrpcMutation,
)

PROTO_DICT = {
    "package": "orders",
    "services": [
        {
            "name": "OrderService",
            "methods": [
                {"name": "GetOrder", "input_type": "GetOrderRequest", "output_type": "Order", "server_streaming": False, "client_streaming": False},
                {"name": "ListOrders", "input_type": "ListRequest", "output_type": "Order", "server_streaming": True, "client_streaming": False},
                {"name": "CreateOrder", "input_type": "CreateOrderRequest", "output_type": "Order", "server_streaming": False, "client_streaming": False},
                {"name": "DeleteOrder", "input_type": "DeleteOrderRequest", "output_type": "DeleteResponse", "server_streaming": False, "client_streaming": False},
            ],
        }
    ],
    "messages": {
        "GetOrderRequest": [{"name": "order_id", "type": "string", "repeated": False}],
        "ListRequest": [{"name": "page", "type": "int32", "repeated": False}],
        "Order": [
            {"name": "id", "type": "string", "repeated": False},
            {"name": "amount", "type": "double", "repeated": False},
            {"name": "shipped", "type": "bool", "repeated": False},
            {"name": "tags", "type": "string", "repeated": True},
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


def test_is_query_method_get():
    assert is_query_method("GetOrder") is True


def test_is_query_method_list():
    assert is_query_method("ListOrders") is True


def test_is_query_method_find():
    assert is_query_method("FindUser") is True


def test_is_query_method_fetch():
    assert is_query_method("FetchData") is True


def test_is_query_method_search():
    assert is_query_method("SearchProducts") is True


def test_is_query_method_stream():
    assert is_query_method("StreamEvents") is True


def test_is_mutation_method():
    assert is_query_method("CreateOrder") is False
    assert is_query_method("DeleteOrder") is False
    assert is_query_method("UpdateUser") is False


def test_map_proto_query_count():
    queries, mutations = map_proto(PROTO_DICT, "", "src", "dom")
    assert len(queries) == 2  # GetOrder, ListOrders


def test_map_proto_mutation_count():
    queries, mutations = map_proto(PROTO_DICT, "", "src", "dom")
    assert len(mutations) == 2  # CreateOrder, DeleteOrder


def test_full_method_path_with_package():
    queries, _ = map_proto(PROTO_DICT, "", "src", "dom")
    get_order = next(q for q in queries if q.method == "GetOrder")
    assert get_order.full_method_path == "/orders.OrderService/GetOrder"


def test_full_method_path_no_package():
    d = {**PROTO_DICT, "package": ""}
    queries, _ = map_proto(d, "", "src", "dom")
    get_order = next(q for q in queries if q.method == "GetOrder")
    assert get_order.full_method_path == "/OrderService/GetOrder"


def test_server_streaming_flag_propagated():
    queries, _ = map_proto(PROTO_DICT, "", "src", "dom")
    list_orders = next(q for q in queries if q.method == "ListOrders")
    assert list_orders.server_streaming is True
    get_order = next(q for q in queries if q.method == "GetOrder")
    assert get_order.server_streaming is False


def test_repeated_field_maps_to_jsonb():
    queries, _ = map_proto(PROTO_DICT, "", "src", "dom")
    get_order = next(q for q in queries if q.method == "GetOrder")
    col = next(c for c in get_order.columns if c.name == "tags")
    assert col.type == "jsonb"


def test_scalar_type_mapping():
    queries, _ = map_proto(PROTO_DICT, "", "src", "dom")
    get_order = next(q for q in queries if q.method == "GetOrder")
    cols = {c.name: c.type for c in get_order.columns}
    assert cols["id"] == "text"
    assert cols["amount"] == "numeric"
    assert cols["shipped"] == "boolean"


def test_namespace_prefix():
    queries, mutations = map_proto(PROTO_DICT, "myns", "src", "dom")
    for q in queries:
        assert q.service == "OrderService"  # service unchanged
    # full_method_path is unaffected by namespace (namespace is for table names, applied in router)


def test_enum_type_maps_to_text():
    d = {
        "package": "",
        "services": [{
            "name": "Svc",
            "methods": [{"name": "GetItem", "input_type": "Req", "output_type": "Res", "server_streaming": False, "client_streaming": False}],
        }],
        "messages": {
            "Req": [],
            "Res": [{"name": "status", "type": "Status", "repeated": False}],
        },
        "enums": ["Status"],
    }
    queries, _ = map_proto(d, "", "src", "dom")
    col = next(c for c in queries[0].columns if c.name == "status")
    assert col.type == "text"


def test_unknown_message_type_maps_to_jsonb():
    d = {
        "package": "",
        "services": [{
            "name": "Svc",
            "methods": [{"name": "GetItem", "input_type": "Req", "output_type": "Res", "server_streaming": False, "client_streaming": False}],
        }],
        "messages": {
            "Req": [],
            "Res": [{"name": "meta", "type": "SomeNestedMsg", "repeated": False}],
        },
        "enums": [],
    }
    queries, _ = map_proto(d, "", "src", "dom")
    col = next(c for c in queries[0].columns if c.name == "meta")
    assert col.type == "jsonb"
