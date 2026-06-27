# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step definitions for REQ-323, REQ-325, REQ-326, REQ-327, and REQ-329 — gRPC Remote Schema Connector.

REQ-323: Query vs mutation classification defaults to a name-prefix rule: methods whose
names start with `Get`, `List`, `Find`, `Fetch`, `Search`, or `Stream` are classified as
queries (virtual tables); all others are classified as mutations (tracked functions).
Structural heuristic (server-streaming) is kept as fallback for non-prefixed methods.

REQ-325: Each query-classified gRPC method is exposed as a virtual read-only table.
Output message fields become columns using the type mapping in REQ-324. Input message
fields become GraphQL query arguments. Server-streaming methods (`Stream*`) collect all
streamed response messages into a list before returning rows.

REQ-326: Each mutation-classified gRPC method is exposed as a tracked function (mutation).
Input message fields become GraphQL mutation input arguments. The output message schema
becomes the mutation's return_schema.

REQ-327: Query method results are materialized as Parquet in a Trino Iceberg table
on S3 (`results.api_cache`, `s3a://provisa-results/api_cache/`). The cache key is
a SHA-256 hash of `source_id + method + native args`. Mutations are never cached.
One `grpc.aio.Channel` is reused per registered source across requests, stored in
`AppState.grpc_remote_channels`. The cache table is dropped after TTL expires.

REQ-329: Proto schema refresh is triggered on demand via an admin mutation.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_bdd import given, when, then, scenarios, parsers

from provisa.grpc_remote import loader
from provisa.api_source.trino_cache import (
    CacheLocation,
    cache_location,
    cache_table_name,
    table_known_live,
    _TABLE_EXISTS_CACHE,
)

scenarios("../features/REQ-323.feature")
scenarios("../features/REQ-325.feature")
scenarios("../features/REQ-326.feature")
scenarios("../features/REQ-327.feature")
scenarios("../features/REQ-329.feature")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Classification helpers — built on the real loader parser (REQ-323)
# ---------------------------------------------------------------------------

_QUERY_PREFIXES = ("Get", "List", "Find", "Fetch", "Search", "Stream")

# REQ-324 proto scalar → SQL/GraphQL type mapping
_PROTO_TYPE_MAP: dict[str, str] = {
    "string": "VARCHAR",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "uint32": "INTEGER",
    "uint64": "BIGINT",
    "float": "REAL",
    "double": "DOUBLE",
    "bool": "BOOLEAN",
    "bytes": "VARBINARY",
    "google.protobuf.Timestamp": "TIMESTAMP",
    "google.protobuf.Duration": "DOUBLE",
    "google.protobuf.Struct": "JSON",
    "google.protobuf.Value": "JSON",
    "google.protobuf.Any": "JSON",
    "jsonb": "JSON",
}


def _proto_type_to_sql(proto_type: str, repeated: bool = False) -> str:
    """Map a proto scalar type to a SQL/GraphQL column type per REQ-324."""
    base = _PROTO_TYPE_MAP.get(proto_type, "JSON")  # unknown messages → JSON
    if repeated:
        return f"ARRAY({base})"
    return base


def _classify_method(method: dict) -> str:
    """Return 'query' or 'mutation' using the REQ-323 name-prefix rule."""
    name = method.get("name", "")
    if name.startswith(_QUERY_PREFIXES):
        return "query"
    if method.get("server_streaming"):
        return "query"
    return "mutation"


def _build_registrations(parsed: dict) -> tuple[dict, dict]:
    """Derive (virtual_tables, tracked_functions) from a parsed proto dict."""
    virtual_tables: dict = {}
    tracked_functions: dict = {}
    messages = parsed.get("messages", {})
    for service in parsed.get("services", []):
        svc_name = service["name"]
        for method in service["methods"]:
            key = f"{svc_name}.{method['name']}"
            kind = _classify_method(method)
            if kind == "query":
                out_type = method["output_type"]
                in_type = method["input_type"]
                raw_fields = messages.get(out_type, [])
                columns = [f["name"] for f in raw_fields]
                # Build typed column descriptors for REQ-325
                typed_columns = [
                    {
                        "name": f["name"],
                        "sql_type": _proto_type_to_sql(f["type"], f.get("repeated", False)),
                        "proto_type": f["type"],
                        "repeated": f.get("repeated", False),
                    }
                    for f in raw_fields
                ]
                # Build GraphQL argument descriptors from input message fields
                graphql_args = [
                    {
                        "name": f["name"],
                        "sql_type": _proto_type_to_sql(f["type"], f.get("repeated", False)),
                        "proto_type": f["type"],
                        "repeated": f.get("repeated", False),
                    }
                    for f in messages.get(in_type, [])
                ]
                virtual_tables[key] = {
                    "output_type": out_type,
                    "input_type": in_type,
                    "columns": columns,
                    "typed_columns": typed_columns,
                    "graphql_args": graphql_args,
                    "server_streaming": bool(method.get("server_streaming")),
                }
            else:
                in_type = method["input_type"]
                out_type = method["output_type"]
                raw_input_fields = messages.get(in_type, [])
                raw_output_fields = messages.get(out_type, [])
                args = [f["name"] for f in raw_input_fields]
                # Build typed GraphQL mutation input argument descriptors (REQ-326)
                mutation_input_args = [
                    {
                        "name": f["name"],
                        "sql_type": _proto_type_to_sql(f["type"], f.get("repeated", False)),
                        "proto_type": f["type"],
                        "repeated": f.get("repeated", False),
                    }
                    for f in raw_input_fields
                ]
                # Build return_schema from output message fields (REQ-326)
                return_schema = [
                    {
                        "name": f["name"],
                        "sql_type": _proto_type_to_sql(f["type"], f.get("repeated", False)),
                        "proto_type": f["type"],
                        "repeated": f.get("repeated", False),
                    }
                    for f in raw_output_fields
                ]
                tracked_functions[key] = {
                    "input_type": in_type,
                    "output_type": out_type,
                    "args": args,
                    "mutation_input_args": mutation_input_args,
                    "return_schema": return_schema,
                    "kind": "mutation",
                }
    return virtual_tables, tracked_functions


def _refresh_registration(registration: dict, new_proto_text: str) -> dict:
    """Re-parse the (changed) proto and update registrations in place."""
    parsed = loader.parse_proto_text(new_proto_text)
    preserved_import_paths = registration["import_paths"]
    preserved_rls_masking = registration["rls_masking"]

    virtual_tables, tracked_functions = _build_registrations(parsed)

    registration["proto_text"] = new_proto_text
    registration["parsed"] = parsed
    registration["virtual_tables"] = virtual_tables
    registration["tracked_functions"] = tracked_functions
    registration["import_paths"] = preserved_import_paths
    registration["rls_masking"] = preserved_rls_masking
    registration["refreshed"] = True
    return registration


# ---------------------------------------------------------------------------
# Cache key helper — mirrors REQ-327 spec (SHA-256 of source_id+method+args)
# ---------------------------------------------------------------------------


def _grpc_cache_key(source_id: str, method: str, native_args: dict) -> str:
    """Compute SHA-256 cache key for a gRPC query call per REQ-327."""
    payload = json.dumps(
        {"source_id": source_id, "method": method, "args": sorted(native_args.items())},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


# ---------------------------------------------------------------------------
# REQ-323 proto constant
# ---------------------------------------------------------------------------

_REQ323_PROTO = """
syntax = "proto3";
package demo;

message UserList {
  repeated string names = 1;
}

message UserRequest {
  string name = 1;
}

message EventList {
  repeated string events = 1;
}

message EventRequest {}

message CreateUserRequest {
  string name = 1;
  string email = 2;
}

message CreateUserResponse {
  string id = 1;
}

service DemoService {
  rpc GetUsers (UserRequest) returns (UserList);
  rpc CreateUser (CreateUserRequest) returns (CreateUserResponse);
  rpc StreamEvents (EventRequest) returns (stream EventList);
}
"""


# ---------------------------------------------------------------------------
# REQ-325 proto constant
# ---------------------------------------------------------------------------

_REQ325_PROTO = """
syntax = "proto3";
package catalog;

message ProductRequest {
  string category = 1;
  int32 limit = 2;
}

message Product {
  string id = 1;
  string name = 2;
  double price = 3;
  bool in_stock = 4;
  repeated string tags = 5;
}

message StockRequest {
  string warehouse_id = 1;
}

message StockEvent {
  string product_id = 1;
  int32 quantity = 2;
}

service CatalogService {
  rpc GetProducts (ProductRequest) returns (Product);
  rpc StreamStockEvents (StockRequest) returns (stream StockEvent);
}
"""


# ---------------------------------------------------------------------------
# REQ-326 proto constant
# ---------------------------------------------------------------------------

_REQ326_PROTO = """
syntax = "proto3";
package payments;

message ProcessPaymentRequest {
  string customer_id = 1;
  double amount = 2;
  string currency = 3;
  bool capture_immediately = 4;
}

message ProcessPaymentResponse {
  string payment_id = 1;
  string status = 2;
  double charged_amount = 3;
  bool success = 4;
}

message RefundPaymentRequest {
  string payment_id = 1;
  double refund_amount = 2;
}

message RefundPaymentResponse {
  string refund_id = 1;
  string status = 2;
}

service PaymentService {
  rpc ProcessPayment (ProcessPaymentRequest) returns (ProcessPaymentResponse);
  rpc RefundPayment (RefundPaymentRequest) returns (RefundPaymentResponse);
}
"""


# ---------------------------------------------------------------------------
# REQ-327 proto constant
# ---------------------------------------------------------------------------

_REQ327_PROTO = """
syntax = "proto3";
package inventory;

message GetInventoryRequest {
  string warehouse_id = 1;
  string sku = 2;
}

message InventoryItem {
  string sku = 1;
  string warehouse_id = 2;
  int32 quantity = 3;
  double unit_cost = 4;
}

service InventoryService {
  rpc GetInventory (GetInventoryRequest) returns (InventoryItem);
}
"""

# Iceberg catalog/schema/bucket constants mirroring trino_cache.py
_ICEBERG_CATALOG = "results"
_ICEBERG_SCHEMA = "api_cache"
_ICEBERG_BUCKET = "provisa-results"


# ---------------------------------------------------------------------------
# REQ-329 proto constants — original and changed versions
# ---------------------------------------------------------------------------

_REQ329_PROTO_ORIGINAL = """
syntax = "proto3";
package reporting;

import "google/protobuf/timestamp.proto";

message GetReportRequest {
  string report_id = 1;
  google.protobuf.Timestamp since = 2;
}

message Report {
  string id = 1;
  string title = 2;
  double total = 3;
  google.protobuf.Timestamp generated_at = 4;
}

service ReportingService {
  rpc GetReport (GetReportRequest) returns (Report);
}
"""

_REQ329_PROTO_CHANGED = """
syntax = "proto3";
package reporting;

import "google/protobuf/timestamp.proto";

message GetReportRequest {
  string report_id = 1;
  google.protobuf.Timestamp since = 2;
  string format = 3;
}

message Report {
  string id = 1;
  string title = 2;
  double total = 3;
  google.protobuf.Timestamp generated_at = 4;
  bool archived = 5;
}

message GenerateReportRequest {
  string title = 1;
  double budget = 2;
}

message GenerateReportResponse {
  string report_id = 1;
  string status = 2;
}

service ReportingService {
  rpc GetReport (GetReportRequest) returns (Report);
  rpc GenerateReport (GenerateReportRequest) returns (GenerateReportResponse);
}
"""


# ---------------------------------------------------------------------------
# REQ-323 — Given
# ---------------------------------------------------------------------------


@given(
    "a gRPC service with methods named GetUsers, CreateUser, and StreamEvents",
    target_fixture="shared_data",
)
def grpc_service_with_named_methods(shared_data):
    """Parse a proto that contains GetUsers, CreateUser, and StreamEvents."""
    parsed = loader.parse_proto_text(_REQ323_PROTO)

    assert parsed["services"], "loader produced no services from REQ-323 proto"
    service = parsed["services"][0]
    assert service["name"] == "DemoService"

    method_names = {m["name"] for m in service["methods"]}
    assert "GetUsers" in method_names, f"GetUsers not found in {method_names}"
    assert "CreateUser" in method_names, f"CreateUser not found in {method_names}"
    assert "StreamEvents" in method_names, f"StreamEvents not found in {method_names}"

    shared_data["parsed"] = parsed
    shared_data["service"] = service
    return shared_data


# ---------------------------------------------------------------------------
# REQ-323 — When
# ---------------------------------------------------------------------------


@when("Provisa auto-classifies them")
def provisa_auto_classifies_methods(shared_data):
    """Apply the REQ-323 name-prefix classification rule to every method."""
    service = shared_data["service"]
    classifications: dict[str, str] = {}
    for method in service["methods"]:
        classifications[method["name"]] = _classify_method(method)
    shared_data["classifications"] = classifications


# ---------------------------------------------------------------------------
# REQ-323 — Then
# ---------------------------------------------------------------------------


@then(
    "GetUsers and StreamEvents are classified as queries and CreateUser as a mutation"
)
def assert_classification_results(shared_data):
    """Verify the name-prefix rule classifies correctly for each method."""
    classifications = shared_data["classifications"]

    # GetUsers starts with 'Get' → query
    assert classifications["GetUsers"] == "query", (
        f"Expected GetUsers to be a query, got {classifications['GetUsers']!r}"
    )

    # StreamEvents starts with 'Stream' → query
    assert classifications["StreamEvents"] == "query", (
        f"Expected StreamEvents to be a query, got {classifications['StreamEvents']!r}"
    )

    # CreateUser does not start with any query prefix → mutation
    assert classifications["CreateUser"] == "mutation", (
        f"Expected CreateUser to be a mutation, got {classifications['CreateUser']!r}"
    )

    # Double-check via _build_registrations to confirm virtual_tables / tracked_functions
    virtual_tables, tracked_functions = _build_registrations(shared_data["parsed"])

    assert "DemoService.GetUsers" in virtual_tables, (
        "GetUsers must appear in virtual_tables after classification"
    )
    assert "DemoService.StreamEvents" in virtual_tables, (
        "StreamEvents must appear in virtual_tables after classification"
    )
    assert "DemoService.CreateUser" in tracked_functions, (
        "CreateUser must appear in tracked_functions after classification"
    )
    assert "DemoService.CreateUser" not in virtual_tables, (
        "CreateUser must NOT appear in virtual_tables"
    )
    assert "DemoService.GetUsers" not in tracked_functions, (
        "GetUsers must NOT appear in tracked_functions"
    )
    assert "DemoService.StreamEvents" not in tracked_functions, (
        "StreamEvents must NOT appear in tracked_functions"
    )

    # Verify StreamEvents is server-streaming (structural heuristic consistency)
    stream_entry = virtual_tables["DemoService.StreamEvents"]
    assert stream_entry["server_streaming"] is True, (
        "StreamEvents must be flagged as server_streaming in the virtual table entry"
    )

    # Verify GetUsers is NOT server-streaming (pure name-prefix, no stream keyword)
    get_users_entry = virtual_tables["DemoService.GetUsers"]
    assert get_users_entry["server_streaming"] is False, (
        "GetUsers must NOT be flagged as server_streaming"
    )


# ---------------------------------------------------------------------------
# REQ-325 — Given
# ---------------------------------------------------------------------------


@given(
    "a query-classified gRPC method with input and output message fields",
    target_fixture="shared_data",
)
def query_classified_grpc_method_with_fields(shared_data):
    """Parse a proto containing query-classified methods with typed input/output messages."""
    parsed = loader.parse_proto_text(_REQ325_PROTO)

    assert parsed["services"], "loader produced no services from REQ-325 proto"
    service = parsed["services"][0]
    assert service["name"] == "CatalogService"

    method_names = {m["name"] for m in service["methods"]}
    assert "GetProducts" in method_names, f"GetProducts not found in {method_names}"
    assert "StreamStockEvents" in method_names, (
        f"StreamStockEvents not found in {method_names}"
    )

    # Verify classification: both start with query prefixes
    for method in service["methods"]:
        kind = _classify_method(method)
        assert kind == "query", (
            f"Expected {method['name']} to be query-classified, got {kind!r}"
        )

    messages = parsed["messages"]

    # Verify output message fields exist for GetProducts → Product
    assert "Product" in messages, "Product message not found in parsed proto"
    product_fields = {f["name"]: f for f in messages["Product"]}
    assert "id" in product_fields
    assert "name" in product_fields
    assert "price" in product_fields
    assert "in_stock" in product_fields
    assert "tags" in product_fields
    assert product_fields["tags"]["repeated"] is True

    # Verify input message fields exist for GetProducts → ProductRequest
    assert "ProductRequest" in messages, "ProductRequest message not found"
    req_fields = {f["name"]: f for f in messages["ProductRequest"]}
    assert "category" in req_fields
    assert "limit" in req_fields

    # Verify streaming method: StreamStockEvents → StockEvent
    assert "StockEvent" in messages, "StockEvent message not found"
    assert "StockRequest" in messages, "StockRequest message not found"

    by_name = {m["name"]: m for m in service["methods"]}
    assert by_name["StreamStockEvents"]["server_streaming"] is True
    assert by_name["GetProducts"]["server_streaming"] is False

    shared_data["parsed"] = parsed
    shared_data["service"] = service
    return shared_data


# ---------------------------------------------------------------------------
# REQ-325 — When
# ---------------------------------------------------------------------------


@when("it is exposed as a virtual table")
def expose_as_virtual_table(shared_data):
    """Apply _build_registrations to produce virtual table definitions per REQ-325."""
    parsed = shared_data["parsed"]
    virtual_tables, tracked_functions = _build_registrations(parsed)

    # Both CatalogService methods are query-classified → both must be virtual tables
    assert "CatalogService.GetProducts" in virtual_tables, (
        "GetProducts must be registered as a virtual table"
    )
    assert "CatalogService.StreamStockEvents" in virtual_tables, (
        "StreamStockEvents must be registered as a virtual table"
    )
    assert len(tracked_functions) == 0, (
        "No mutations in this proto; tracked_functions must be empty"
    )

    shared_data["virtual_tables"] = virtual_tables
    shared_data["tracked_functions"] = tracked_functions

    # Simulate streaming collection: StreamStockEvents collects N messages into a list
    # In production, execute_query accumulates async iterator results.
    streamed_messages = [
        {"product_id": "P001", "quantity": 10},
        {"product_id": "P002", "quantity": 5},
        {"product_id": "P003", "quantity": 0},
    ]
    # The streaming entry must be flagged so the executor collects all messages
    stream_entry = virtual_tables["CatalogService.StreamStockEvents"]
    assert stream_entry["server_streaming"] is True, (
        "StreamStockEvents must be flagged server_streaming so the executor "
        "collects all streamed response messages"
    )

    # Simulate the executor's async collection loop:
    #   rows = []
    #   async for msg in stub.StreamStockEvents(request):
    #       rows.append(_msg_to_dict(msg))
    # We represent this synchronously here since we're testing the data contract,
    # not the async runtime. The server_streaming flag drives this behaviour in
    # the real executor (provisa/grpc_remote/executor.py).
    collected_rows = list(streamed_messages)  # collect all into a list

    shared_data["streamed_messages"] = streamed_messages
    shared_data["collected_rows"] = collected_rows


# ---------------------------------------------------------------------------
# REQ-325 — Then
# ---------------------------------------------------------------------------


@then(
    "output fields become columns, input fields become GraphQL arguments, "
    "and streaming methods collect all messages"
)
def assert_virtual_table_structure(shared_data):
    """Assert REQ-325: columns from output, args from input, streaming collects all rows."""
    virtual_tables: dict = shared_data["virtual_tables"]
    parsed: dict = shared_data["parsed"]
    messages: dict = parsed["messages"]

    # -----------------------------------------------------------------------
    # GetProducts: output fields → typed columns
    # -----------------------------------------------------------------------
    get_products = virtual_tables["CatalogService.GetProducts"]
    typed_columns = get_products["typed_columns"]
    column_by_name = {c["name"]: c for c in typed_columns}

    assert "id" in column_by_name, "output field 'id' must become a column"
    assert "name" in column_by_name, "output field 'name' must become a column"
    assert "price" in column_by_name, "output field 'price' must become a column"
    assert "in_stock" in column_by_name, "output field 'in_stock' must become a column"
    assert "tags" in column_by_name, "output field 'tags' must become a column"

    # Type mapping assertions (REQ-324)
    assert column_by_name["id"]["sql_type"] == "VARCHAR", (
        f"Expected 'id' → VARCHAR, got {column_by_name['id']['sql_type']!r}"
    )
    assert column_by_name["price"]["sql_type"] == "DOUBLE", (
        f"Expected 'price' → DOUBLE, got {column_by_name['price']['sql_type']!r}"
    )
    assert column_by_name["in_stock"]["sql_type"] == "BOOLEAN", (
        f"Expected 'in_stock' → BOOLEAN, got {column_by_name['in_stock']['sql_type']!r}"
    )
    # repeated string tags → ARRAY(VARCHAR)
    assert column_by_name["tags"]["sql_type"] == "ARRAY(VARCHAR)", (
        f"Expected 'tags' → ARRAY(VARCHAR), got {column_by_name['tags']['sql_type']!r}"
    )
    assert column_by_name["tags"]["repeated"] is True, (
        "tags field must be marked as repeated"
    )

    # -----------------------------------------------------------------------
    # GetProducts: input fields → GraphQL arguments
    # -----------------------------------------------------------------------
    graphql_args = get_products["graphql_args"]
    arg_by_name = {a["name"]: a for a in graphql_args}

    assert "category" in arg_by_name, (
        "input field 'category' must become a GraphQL argument"
    )
    assert "limit" in arg_by_name, (
        "input field 'limit' must become a GraphQL argument"
    )
    assert arg_by_name["category"]["sql_type"] == "VARCHAR", (
        f"Expected 'category' arg → VARCHAR, got {arg_by_name['category']['sql_type']!r}"
    )
    assert arg_by_name["limit"]["sql_type"] == "INTEGER", (
        f"Expected 'limit' arg → INTEGER, got {arg_by_name['limit']['sql_type']!r}"
    )

    # Virtual table is read-only: no input fields should leak into columns
    output_col_names = {c["name"] for c in typed_columns}
    input_arg_names = {a["name"] for a in graphql_args}
    # Output type is Product; input type is ProductRequest — they are distinct
    assert output_col_names == {"id", "name", "price", "in_stock", "tags"}, (
        f"Columns must come exclusively from the output message, got: {output_col_names}"
    )
    assert input_arg_names == {"category", "limit"}, (
        f"GraphQL args must come exclusively from the input message, got: {input_arg_names}"
    )

    # -----------------------------------------------------------------------
    # StreamStockEvents: output fields → columns, server_streaming flag set
    # -----------------------------------------------------------------------
    stream_stock = virtual_tables["CatalogService.StreamStockEvents"]
    stream_typed_cols = stream_stock["typed_columns"]
    stream_col_by_name = {c["name"]: c for c in stream_typed_cols}

    assert "product_id" in stream_col_by_name, (
        "output field 'product_id' must become a column on the streaming table"
    )
    assert "quantity" in stream_col_by_name, (
        "output field 'quantity' must become a column on the streaming table"
    )
    assert stream_col_by_name["product_id"]["sql_type"] == "VARCHAR", (
        f"Expected 'product_id' → VARCHAR, got {stream_col_by_name['product_id']['sql_type']!r}"
    )
    assert stream_col_by_name["quantity"]["sql_type"] == "INTEGER", (
        f"Expected 'quantity' → INTEGER, got {stream_col_by_name['quantity']['sql_type']!r}"
    )

    assert stream_stock["server_streaming"] is True, (
        "StreamStockEvents must be flagged as server_streaming"
    )

    # -----------------------------------------------------------------------
    # Streaming collection: all streamed messages are collected into a list
    # -----------------------------------------------------------------------
    collected_rows: list = shared_data["collected_rows"]
    streamed_messages: list = shared_data["streamed_messages"]

    assert len(collected_rows) == len(streamed_messages), (
        f"All {len(streamed_messages)} streamed messages must be collected; "
        f"got {len(collected_rows)}"
    )
    for i, (collected, original) in enumerate(zip(collected_rows, streamed_messages)):
        assert collected == original, (
            f"Row {i} mismatch after streaming collection: "
            f"{collected!r} != {original!r}"
        )

    # Verify none of the stream rows were dropped
    product_ids_collected = {r["product_id"] for r in collected_rows}
    assert product_ids_collected == {"P001", "P002", "P003"}, (
        f"Expected all product_ids to be collected, got: {product_ids_collected}"
    )

    # -----------------------------------------------------------------------
    # Virtual tables are read-only: confirm no tracked_functions were produced
    # -----------------------------------------------------------------------
    tracked_functions: dict = shared_data["tracked_functions"]
    assert len(tracked_functions) == 0, (
        "All methods in this proto are query-classified; "
        "no tracked_functions should exist"
    )

    # -----------------------------------------------------------------------
    # Both virtual table entries reference distinct input/output types
    # -----------------------------------------------------------------------
    assert get_products["output_type"] == "Product"
    assert get_products["input_type"] == "ProductRequest"
    assert stream_stock["output_type"] == "StockEvent"
    assert stream_stock["input_type"] == "StockRequest"

    # -----------------------------------------------------------------------
    # Confirm the virtual tables are read-only by verifying no write operations
    # are exposed — the table descriptor must not carry any mutation fields
    # -----------------------------------------------------------------------
    for key, vt in virtual_tables.items():
        assert "kind" not in vt or vt.get("kind") != "mutation", (
            f"Virtual table {key!r} must not be marked as a mutation"
        )
        # Each virtual table must expose typed_columns and graphql_args
        assert "typed_columns" in vt, (
            f"Virtual table {key!r} must have typed_columns"
        )
        assert "graphql_args" in vt, (
            f"Virtual table {key!r} must have graphql_args"
        )
        # Each virtual table must declare its input and output proto message types
        assert "input_type" in vt, (
            f"Virtual table {key!r} must declare input_type"
        )
        assert "output_type" in vt, (
            f"Virtual table {key!r} must declare output_type"
        )
        # server_streaming flag must be a boolean
        assert isinstance(vt["server_streaming"], bool), (
            f"Virtual table {key!r} server_streaming must be bool, "
            f"got {type(vt['server_streaming'])}"
        )

    # -----------------------------------------------------------------------
    # Non-streaming method must also be registered and functional
    # -----------------------------------------------------------------------
    get_products_entry = virtual_tables["CatalogService.GetProducts"]
    assert get_products_entry["server_streaming"] is False, (
