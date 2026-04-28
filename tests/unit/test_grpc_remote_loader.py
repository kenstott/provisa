# Copyright (c) 2026 Kenneth Stott
# Canary: d5db22ac-bf18-4851-b046-5f0934ad10fd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/grpc_remote/loader.py — pure proto text parsing (no I/O)."""
import pytest
from provisa.grpc_remote.loader import parse_proto_text

SAMPLE_PROTO = """
syntax = "proto3";
package orders;

message GetOrderRequest {
  string order_id = 1;
  int32 customer_id = 2;
}

message Order {
  string id = 1;
  double amount = 2;
  bool shipped = 3;
  repeated string tags = 4;
}

message CreateOrderRequest {
  string customer_id = 1;
  double amount = 2;
}

service OrderService {
  rpc GetOrder (GetOrderRequest) returns (Order);
  rpc ListOrders (GetOrderRequest) returns (stream Order);
  rpc CreateOrder (CreateOrderRequest) returns (Order);
}
"""


def test_parse_package():
    result = parse_proto_text(SAMPLE_PROTO)
    assert result["package"] == "orders"


def test_parse_services():
    result = parse_proto_text(SAMPLE_PROTO)
    assert len(result["services"]) == 1
    assert result["services"][0]["name"] == "OrderService"


def test_parse_methods():
    methods = parse_proto_text(SAMPLE_PROTO)["services"][0]["methods"]
    names = {m["name"] for m in methods}
    assert names == {"GetOrder", "ListOrders", "CreateOrder"}


def test_server_streaming_flag():
    methods = parse_proto_text(SAMPLE_PROTO)["services"][0]["methods"]
    by_name = {m["name"]: m for m in methods}
    assert by_name["ListOrders"]["server_streaming"] is True
    assert by_name["GetOrder"]["server_streaming"] is False


def test_parse_messages_names():
    msgs = parse_proto_text(SAMPLE_PROTO)["messages"]
    assert set(msgs.keys()) >= {"GetOrderRequest", "Order", "CreateOrderRequest"}


def test_parse_message_fields():
    msgs = parse_proto_text(SAMPLE_PROTO)["messages"]
    order_fields = {f["name"]: f for f in msgs["Order"]}
    assert order_fields["id"]["type"] == "string"
    assert order_fields["amount"]["type"] == "double"
    assert order_fields["shipped"]["type"] == "bool"
    assert order_fields["tags"]["repeated"] is True


def test_parse_repeated_field():
    msgs = parse_proto_text(SAMPLE_PROTO)["messages"]
    tags = next(f for f in msgs["Order"] if f["name"] == "tags")
    assert tags["repeated"] is True


def test_no_package():
    text = """
    syntax = "proto3";
    service Svc { rpc Get (Req) returns (Res); }
    message Req {}
    message Res {}
    """
    result = parse_proto_text(text)
    assert result["package"] == ""


def test_strip_line_comments():
    text = """
    syntax = "proto3"; // this is a comment
    package test; // another
    message Msg { string name = 1; /* block */ }
    service Svc { rpc GetFoo (Msg) returns (Msg); }
    """
    result = parse_proto_text(text)
    assert result["package"] == "test"
    assert len(result["services"]) == 1


def test_parse_enum_names():
    text = """
    syntax = "proto3";
    enum Status { UNKNOWN = 0; ACTIVE = 1; }
    message Msg { Status status = 1; }
    service Svc { rpc GetMsg (Msg) returns (Msg); }
    """
    result = parse_proto_text(text)
    assert "Status" in result["enums"]


def test_input_output_types():
    methods = parse_proto_text(SAMPLE_PROTO)["services"][0]["methods"]
    by_name = {m["name"]: m for m in methods}
    assert by_name["GetOrder"]["input_type"] == "GetOrderRequest"
    assert by_name["GetOrder"]["output_type"] == "Order"


def test_empty_proto():
    result = parse_proto_text('syntax = "proto3";')
    assert result["package"] == ""
    assert result["services"] == []
    assert result["messages"] == {}
    assert result["enums"] == []


def test_multiple_services():
    text = """
    syntax = "proto3";
    message Req {}
    message Res {}
    service SvcA { rpc GetA (Req) returns (Res); }
    service SvcB { rpc GetB (Req) returns (Res); }
    """
    result = parse_proto_text(text)
    names = {s["name"] for s in result["services"]}
    assert names == {"SvcA", "SvcB"}
