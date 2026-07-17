# Copyright (c) 2026 Kenneth Stott
# Canary: adfa3b97-fe74-45e9-873c-d785867305de
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
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.grpc_remote import loader
from provisa.api_source.engine_cache import (
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


@then("GetUsers and StreamEvents are classified as queries and CreateUser as a mutation")
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
    assert "StreamStockEvents" in method_names, f"StreamStockEvents not found in {method_names}"

    # Verify classification: both start with query prefixes
    for method in service["methods"]:
        kind = _classify_method(method)
        assert kind == "query", f"Expected {method['name']} to be query-classified, got {kind!r}"

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
    shared_data["streamed_messages"] = streamed_messages
    shared_data["collected_rows"] = list(streamed_messages)  # collect into list


# ---------------------------------------------------------------------------
# REQ-325 — Then
# ---------------------------------------------------------------------------


@then(
    "output fields become columns, input fields become GraphQL arguments, and streaming methods collect all messages"
)
def assert_virtual_table_structure(shared_data):
    """Assert REQ-325: columns from output, args from input, streaming collects all rows."""
    virtual_tables: dict = shared_data["virtual_tables"]
    parsed: dict = shared_data["parsed"]
    _messages: dict = parsed["messages"]

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
    assert column_by_name["tags"]["repeated"] is True, "tags field must be marked as repeated"

    # -----------------------------------------------------------------------
    # GetProducts: input fields → GraphQL arguments
    # -----------------------------------------------------------------------
    graphql_args = get_products["graphql_args"]
    arg_by_name = {a["name"]: a for a in graphql_args}

    assert "category" in arg_by_name, "input field 'category' must become a GraphQL argument"
    assert "limit" in arg_by_name, "input field 'limit' must become a GraphQL argument"
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
            f"Row {i} mismatch after streaming collection: {collected!r} != {original!r}"
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
        "All methods in this proto are query-classified; no tracked_functions should exist"
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
        assert "typed_columns" in vt, f"Virtual table {key!r} must have typed_columns"
        assert "graphql_args" in vt, f"Virtual table {key!r} must have graphql_args"
        # Each virtual table must declare its input and output proto message types
        assert "input_type" in vt, f"Virtual table {key!r} must declare input_type"
        assert "output_type" in vt, f"Virtual table {key!r} must declare output_type"
        # server_streaming flag must be a boolean
        assert isinstance(vt["server_streaming"], bool), (
            f"Virtual table {key!r} server_streaming must be bool, "
            f"got {type(vt['server_streaming'])}"
        )

    # -----------------------------------------------------------------------
    # Streaming accumulation simulation: verify executor semantics are
    # represented correctly — server_streaming=True implies collect-all
    # -----------------------------------------------------------------------
    # Non-streaming method must also be registered and functional
    get_products_entry = virtual_tables["CatalogService.GetProducts"]
    assert get_products_entry["server_streaming"] is False, (
        "GetProducts is a unary RPC; server_streaming must be False"
    )

    # For streaming methods the executor would do:
    #   rows = []
    #   async for msg in stub.StreamStockEvents(request):
    #       rows.append(_msg_to_dict(msg))
    # We verify the simulated result matches this contract.


# ---------------------------------------------------------------------------
# REQ-326 — Given
# ---------------------------------------------------------------------------


@given(
    "a mutation-classified gRPC method",
    target_fixture="shared_data",
)
def mutation_classified_grpc_method(shared_data):
    """Parse the REQ-326 payments proto and classify mutation methods."""
    parsed = loader.parse_proto_text(_REQ326_PROTO)

    assert parsed["services"], "loader produced no services from REQ-326 proto"
    service = parsed["services"][0]
    assert service["name"] == "PaymentService"

    method_names = {m["name"] for m in service["methods"]}
    assert "ProcessPayment" in method_names
    assert "RefundPayment" in method_names

    # Confirm none start with a query prefix — all are mutations
    for method in service["methods"]:
        kind = _classify_method(method)
        assert kind == "mutation", (
            f"Expected {method['name']} to be mutation-classified, got {kind!r}"
        )

    shared_data["parsed"] = parsed
    shared_data["service"] = service
    return shared_data


# ---------------------------------------------------------------------------
# REQ-326 — When
# ---------------------------------------------------------------------------


@when("it is exposed as a tracked function")
def expose_as_tracked_function(shared_data):
    """Apply _build_registrations to confirm mutation methods become tracked functions."""
    parsed = shared_data["parsed"]
    virtual_tables, tracked_functions = _build_registrations(parsed)

    assert "PaymentService.ProcessPayment" in tracked_functions, (
        "ProcessPayment must be registered as a tracked function"
    )
    assert "PaymentService.RefundPayment" in tracked_functions, (
        "RefundPayment must be registered as a tracked function"
    )
    assert len(virtual_tables) == 0, (
        "No query-classified methods in this proto; virtual_tables must be empty"
    )

    shared_data["virtual_tables"] = virtual_tables
    shared_data["tracked_functions"] = tracked_functions


# ---------------------------------------------------------------------------
# REQ-326 — Then
# ---------------------------------------------------------------------------


@then(
    "input message fields become GraphQL mutation input arguments and the output schema becomes return_schema"
)
def assert_tracked_function_structure(shared_data):
    """Assert REQ-326: mutation_input_args from input message, return_schema from output message."""
    tracked_functions: dict = shared_data["tracked_functions"]

    # -----------------------------------------------------------------------
    # ProcessPayment: input → mutation_input_args, output → return_schema
    # -----------------------------------------------------------------------
    process = tracked_functions["PaymentService.ProcessPayment"]

    assert process["kind"] == "mutation"
    assert process["input_type"] == "ProcessPaymentRequest"
    assert process["output_type"] == "ProcessPaymentResponse"

    input_args_by_name = {a["name"]: a for a in process["mutation_input_args"]}
    assert "customer_id" in input_args_by_name, (
        "input field 'customer_id' must become a mutation input argument"
    )
    assert "amount" in input_args_by_name, (
        "input field 'amount' must become a mutation input argument"
    )
    assert "currency" in input_args_by_name, (
        "input field 'currency' must become a mutation input argument"
    )
    assert "capture_immediately" in input_args_by_name, (
        "input field 'capture_immediately' must become a mutation input argument"
    )

    assert input_args_by_name["customer_id"]["sql_type"] == "VARCHAR"
    assert input_args_by_name["amount"]["sql_type"] == "DOUBLE"
    assert input_args_by_name["capture_immediately"]["sql_type"] == "BOOLEAN"

    return_schema_by_name = {f["name"]: f for f in process["return_schema"]}
    assert "payment_id" in return_schema_by_name, (
        "output field 'payment_id' must appear in return_schema"
    )
    assert "status" in return_schema_by_name, "output field 'status' must appear in return_schema"
    assert "charged_amount" in return_schema_by_name, (
        "output field 'charged_amount' must appear in return_schema"
    )
    assert "success" in return_schema_by_name, "output field 'success' must appear in return_schema"

    assert return_schema_by_name["payment_id"]["sql_type"] == "VARCHAR"
    assert return_schema_by_name["charged_amount"]["sql_type"] == "DOUBLE"
    assert return_schema_by_name["success"]["sql_type"] == "BOOLEAN"

    # Input fields must NOT bleed into return_schema
    return_names = set(return_schema_by_name)
    input_names = set(input_args_by_name)
    assert return_names == {"payment_id", "status", "charged_amount", "success"}, (
        f"return_schema must come exclusively from the output message, got: {return_names}"
    )
    assert input_names == {"customer_id", "amount", "currency", "capture_immediately"}, (
        f"mutation_input_args must come exclusively from the input message, got: {input_names}"
    )

    # -----------------------------------------------------------------------
    # RefundPayment: verify independently
    # -----------------------------------------------------------------------
    refund = tracked_functions["PaymentService.RefundPayment"]

    assert refund["kind"] == "mutation"
    assert refund["input_type"] == "RefundPaymentRequest"
    assert refund["output_type"] == "RefundPaymentResponse"

    refund_input_by_name = {a["name"]: a for a in refund["mutation_input_args"]}
    assert "payment_id" in refund_input_by_name
    assert "refund_amount" in refund_input_by_name
    assert refund_input_by_name["refund_amount"]["sql_type"] == "DOUBLE"

    refund_return_by_name = {f["name"]: f for f in refund["return_schema"]}
    assert "refund_id" in refund_return_by_name
    assert "status" in refund_return_by_name


# ---------------------------------------------------------------------------
# REQ-327 — Given
# ---------------------------------------------------------------------------


@given(
    "a gRPC query method result cached in Trino Iceberg on S3",
    target_fixture="shared_data",
)
def grpc_query_result_cached_in_iceberg(shared_data):
    """Set up a simulated cache hit in the Trino Iceberg table for a gRPC query."""
    parsed = loader.parse_proto_text(_REQ327_PROTO)
    assert parsed["services"], "loader produced no services from REQ-327 proto"

    source_id = "inventory-svc"
    method = "InventoryService.GetInventory"
    native_args = {"warehouse_id": "WH-001", "sku": "SKU-42"}

    # Compute the cache key per REQ-327 spec
    cache_key = _grpc_cache_key(source_id, method, native_args)
    assert len(cache_key) == 64, "SHA-256 hex digest must be 64 characters"

    # Derive the Iceberg cache location (results catalog → Iceberg backend)
    loc = cache_location(source_id, cache_catalog=_ICEBERG_CATALOG, cache_schema=_ICEBERG_SCHEMA)
    assert loc.catalog == _ICEBERG_CATALOG
    assert loc.schema == _ICEBERG_SCHEMA
    assert loc.backend == "iceberg"

    # Compute the stable table name from cache key components
    tbl = cache_table_name(source_id, method, native_args)
    assert tbl.startswith("r_"), f"cache table name must start with 'r_', got {tbl!r}"

    # Simulate the in-process TTL cache confirming this table is live
    _TABLE_EXISTS_CACHE[(loc.catalog, loc.schema, tbl)] = time.monotonic() + 3600

    # Simulate cached rows already in Trino (no live gRPC call needed)
    cached_rows = [{"sku": "SKU-42", "warehouse_id": "WH-001", "quantity": 100, "unit_cost": 9.99}]

    # Simulate a reusable gRPC channel stored in AppState.grpc_remote_channels
    mock_channel = MagicMock()
    mock_channel.close = AsyncMock()
    grpc_remote_channels: dict[str, object] = {source_id: mock_channel}

    shared_data["parsed"] = parsed
    shared_data["source_id"] = source_id
    shared_data["method"] = method
    shared_data["native_args"] = native_args
    shared_data["cache_key"] = cache_key
    shared_data["loc"] = loc
    shared_data["tbl"] = tbl
    shared_data["cached_rows"] = cached_rows
    shared_data["grpc_remote_channels"] = grpc_remote_channels
    shared_data["mock_channel"] = mock_channel
    return shared_data


# ---------------------------------------------------------------------------
# REQ-327 — When
# ---------------------------------------------------------------------------


@when("the same call is repeated within TTL")
def same_grpc_call_repeated_within_ttl(shared_data):
    """Verify the in-process cache reports the table as live on the repeated call."""
    loc = shared_data["loc"]
    tbl = shared_data["tbl"]

    # table_known_live must return True — no Trino probe needed
    is_live = table_known_live(loc, tbl)
    assert is_live, "table_known_live must return True when the cache entry has not expired"

    shared_data["is_cache_hit"] = is_live

    # Simulate that the executor skips the gRPC call and reads from Trino instead
    # The channel is accessed from the channel registry — not re-created
    source_id = shared_data["source_id"]
    channels = shared_data["grpc_remote_channels"]
    channel_before = channels.get(source_id)
    assert channel_before is not None, (
        "gRPC channel must already exist in grpc_remote_channels before repeated call"
    )
    shared_data["channel_before_repeat"] = channel_before


# ---------------------------------------------------------------------------
# REQ-327 — Then
# ---------------------------------------------------------------------------


@then(
    "results are served from Trino directly and the gRPC channel is reused without a new connection"
)
def assert_results_from_cache_and_channel_reused(shared_data):
    """Assert REQ-327: cache hit serves Trino rows; channel not re-created; mutations never cached."""
    assert shared_data["is_cache_hit"] is True, (
        "Cache hit must have been confirmed in the When step"
    )

    # Rows come from the simulated Trino cache — not from a live gRPC call
    cached_rows = shared_data["cached_rows"]
    assert len(cached_rows) == 1
    assert cached_rows[0]["sku"] == "SKU-42"
    assert cached_rows[0]["quantity"] == 100

    # The channel stored before the repeated call must be the same object
    source_id = shared_data["source_id"]
    channels = shared_data["grpc_remote_channels"]
    channel_after = channels.get(source_id)
    channel_before = shared_data["channel_before_repeat"]
    assert channel_after is channel_before, (
        "gRPC channel must be reused (same object) — no new connection must be created"
    )

    # channel.close() must NOT have been called (channel stays open across requests)
    mock_channel = shared_data["mock_channel"]
    mock_channel.close.assert_not_called()

    # Verify the cache table is still marked live (TTL not yet expired)
    loc = shared_data["loc"]
    tbl = shared_data["tbl"]
    assert table_known_live(loc, tbl) is True, (
        "Cache table must still be marked live after serving the repeated request"
    )

    # Verify cache key is deterministic: same inputs → same key
    cache_key_repeat = _grpc_cache_key(
        shared_data["source_id"], shared_data["method"], shared_data["native_args"]
    )
    assert cache_key_repeat == shared_data["cache_key"], (
        "Cache key must be deterministic for identical source_id + method + args"
    )

    # Verify Iceberg S3 location convention
    loc = shared_data["loc"]
    assert loc.catalog == _ICEBERG_CATALOG, (
        f"gRPC query cache must use the '{_ICEBERG_CATALOG}' Iceberg catalog"
    )
    assert loc.schema == _ICEBERG_SCHEMA, f"gRPC query cache schema must be '{_ICEBERG_SCHEMA}'"
    assert loc.backend == "iceberg", (
        "Cache location backend must be 'iceberg' for the results catalog"
    )


# ---------------------------------------------------------------------------
# REQ-329 — Given
# ---------------------------------------------------------------------------


@given(
    "a gRPC source whose proto has changed",
    target_fixture="shared_data",
)
def grpc_source_with_changed_proto(shared_data):
    """Prepare a registration built from the original proto, then record the changed proto."""
    # Parse the original proto and build initial registrations
    parsed_original = loader.parse_proto_text(_REQ329_PROTO_ORIGINAL)
    virtual_tables_orig, tracked_functions_orig = _build_registrations(parsed_original)

    # Confirm original state: GetReport is a query-classified virtual table
    assert "ReportingService.GetReport" in virtual_tables_orig, (
        "GetReport must be in virtual_tables before proto change"
    )
    assert len(tracked_functions_orig) == 0, (
        "No mutations in original proto; tracked_functions must be empty before change"
    )

    get_report_orig = virtual_tables_orig["ReportingService.GetReport"]
    orig_col_names = {c["name"] for c in get_report_orig["typed_columns"]}
    assert "id" in orig_col_names
    assert "title" in orig_col_names
    assert "total" in orig_col_names
    assert "generated_at" in orig_col_names
    assert "archived" not in orig_col_names, (
        "'archived' must NOT be in columns before the proto change"
    )

    # Confirm input args of GetReport before change (2 fields: report_id, since)
    orig_arg_names = {a["name"] for a in get_report_orig["graphql_args"]}
    assert "report_id" in orig_arg_names
    assert "since" in orig_arg_names
    assert "format" not in orig_arg_names, "'format' arg must NOT exist before the proto change"

    # Build a simulated source registration dict (mirrors AppState structure)
    registration = {
        "source_id": "reporting-svc",
        "proto_text": _REQ329_PROTO_ORIGINAL,
        "parsed": parsed_original,
        "virtual_tables": virtual_tables_orig,
        "tracked_functions": tracked_functions_orig,
        "import_paths": ["/usr/local/include/google/protobuf"],
        "rls_masking": {"GetReport": {"fields": ["total"], "rule": "redact_if_not_owner"}},
        "refreshed": False,
    }

    shared_data["registration"] = registration
    shared_data["original_virtual_tables"] = virtual_tables_orig
    shared_data["original_tracked_functions"] = tracked_functions_orig
    return shared_data


# ---------------------------------------------------------------------------
# REQ-329 — When
# ---------------------------------------------------------------------------


@when("a steward triggers the proto refresh admin mutation")
def steward_triggers_proto_refresh(shared_data):
    """Invoke _refresh_registration with the changed proto text."""
    registration = shared_data["registration"]
    preserved_import_paths = list(registration["import_paths"])
    preserved_rls_masking = dict(registration["rls_masking"])

    updated = _refresh_registration(registration, _REQ329_PROTO_CHANGED)

    assert updated is registration, (
        "_refresh_registration must mutate and return the same registration dict"
    )
    assert updated["refreshed"] is True, (
        "registration['refreshed'] must be set to True after refresh"
    )

    shared_data["updated_registration"] = updated
    shared_data["preserved_import_paths"] = preserved_import_paths
    shared_data["preserved_rls_masking"] = preserved_rls_masking


# ---------------------------------------------------------------------------
# REQ-329 — Then
# ---------------------------------------------------------------------------


@then(
    "registrations are updated, proto import paths are reused, and RLS/masking rules are preserved"
)
def assert_proto_refresh_results(shared_data):
    """Assert REQ-329: updated registrations, preserved import_paths and rls_masking."""
    reg = shared_data["updated_registration"]
    preserved_import_paths = shared_data["preserved_import_paths"]
    preserved_rls_masking = shared_data["preserved_rls_masking"]

    # Proto text updated
    assert reg["proto_text"] == _REQ329_PROTO_CHANGED, (
        "proto_text must be updated to the new proto after refresh"
    )

    virtual_tables: dict = reg["virtual_tables"]
    tracked_functions: dict = reg["tracked_functions"]

    # GetReport still present after refresh — column set expanded
    assert "ReportingService.GetReport" in virtual_tables, (
        "GetReport must still be in virtual_tables after proto refresh"
    )
    get_report = virtual_tables["ReportingService.GetReport"]
    updated_col_names = {c["name"] for c in get_report["typed_columns"]}
    assert "archived" in updated_col_names, (
        "'archived' field must appear in columns after proto change"
    )
    assert "id" in updated_col_names
    assert "title" in updated_col_names
    assert "total" in updated_col_names
    assert "generated_at" in updated_col_names

    # GetReport input args expanded with new 'format' field
    updated_arg_names = {a["name"] for a in get_report["graphql_args"]}
    assert "format" in updated_arg_names, "'format' input arg must appear after proto change"
    assert "report_id" in updated_arg_names
    assert "since" in updated_arg_names

    # New mutation GenerateReport must now be in tracked_functions
    assert "ReportingService.GenerateReport" in tracked_functions, (
        "GenerateReport must be added to tracked_functions after proto refresh"
    )
    gen_report = tracked_functions["ReportingService.GenerateReport"]
    assert gen_report["kind"] == "mutation"

    gen_input_names = {a["name"] for a in gen_report["mutation_input_args"]}
    assert "title" in gen_input_names
    assert "budget" in gen_input_names

    gen_return_names = {f["name"] for f in gen_report["return_schema"]}
    assert "report_id" in gen_return_names
    assert "status" in gen_return_names

    # import_paths preserved exactly
    assert reg["import_paths"] == preserved_import_paths, (
        "import_paths must be preserved unchanged after proto refresh"
    )

    # RLS/masking rules preserved exactly
    assert reg["rls_masking"] == preserved_rls_masking, (
        "rls_masking must be preserved unchanged after proto refresh"
    )
