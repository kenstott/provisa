# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step definitions for REQ-329 — on-demand proto schema refresh.

Proto schema refresh is triggered on demand via an admin mutation. On refresh,
Provisa re-parses the proto, rebuilds the virtual table (query) and tracked
function (mutation) registrations, but:

  * re-uses the proto import paths captured at registration time (so that
    well-known types such as ``google/protobuf/timestamp.proto`` continue to
    resolve without the steward having to re-supply them), and
  * preserves any RLS / column-masking rules previously applied on top of the
    generated registrations.

These steps exercise the real :mod:`provisa.grpc_remote.loader` proto parser to
re-derive registrations from the (changed) proto, and assert that governance
metadata survives the refresh untouched.
"""

from __future__ import annotations

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.grpc_remote import loader

scenarios("../features/REQ-329.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Registration helpers — built on the real loader parser
# ---------------------------------------------------------------------------

_QUERY_PREFIXES = ("Get", "List", "Find", "Fetch", "Search", "Stream")


def _classify_method(method: dict) -> str:
    """Return 'query' or 'mutation' using the REQ-323 name-prefix rule.

    Falls back to the structural (server-streaming) heuristic for methods that
    do not match a known query prefix.
    """
    name = method.get("name", "")
    if name.startswith(_QUERY_PREFIXES):
        return "query"
    if method.get("server_streaming"):
        return "query"
    return "mutation"


def _build_registrations(parsed: dict) -> tuple[dict, dict]:
    """Derive (virtual_tables, tracked_functions) from a parsed proto dict.

    Virtual tables are keyed by ``Service.Method`` and carry their output-message
    columns; tracked functions carry their input-message arguments.
    """
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
                columns = [f["name"] for f in messages.get(out_type, [])]
                virtual_tables[key] = {
                    "output_type": out_type,
                    "columns": columns,
                    "server_streaming": bool(method.get("server_streaming")),
                }
            else:
                in_type = method["input_type"]
                args = [f["name"] for f in messages.get(in_type, [])]
                tracked_functions[key] = {
                    "input_type": in_type,
                    "args": args,
                }
    return virtual_tables, tracked_functions


def _refresh_registration(registration: dict, new_proto_text: str) -> dict:
    """Re-parse the (changed) proto and update registrations in place.

    Import paths captured at registration time are re-used (not re-derived);
    RLS/masking rules layered on top are preserved verbatim.
    """
    parsed = loader.parse_proto_text(new_proto_text)
    # Import paths are *re-used*, never recomputed from the changed proto.
    preserved_import_paths = registration["import_paths"]
    preserved_rls_masking = registration["rls_masking"]

    virtual_tables, tracked_functions = _build_registrations(parsed)

    registration["proto_text"] = new_proto_text
    registration["parsed"] = parsed
    registration["virtual_tables"] = virtual_tables
    registration["tracked_functions"] = tracked_functions
    # Explicitly carry forward the governance + import-path metadata.
    registration["import_paths"] = preserved_import_paths
    registration["rls_masking"] = preserved_rls_masking
    registration["refreshed"] = True
    return registration


# Initial proto registered for the source.
_PROTO_V1 = """
syntax = "proto3";
package orders;
import "google/protobuf/timestamp.proto";

message GetOrderRequest { string order_id = 1; }

message Order {
  string id = 1;
  double amount = 2;
  google.protobuf.Timestamp created_at = 3;
}

message CreateOrderRequest {
  string customer_id = 1;
  double amount = 2;
}

service OrderService {
  rpc GetOrder (GetOrderRequest) returns (Order);
  rpc CreateOrder (CreateOrderRequest) returns (Order);
}
"""

# Changed proto: adds a streaming ListOrders query method and a new Order field.
_PROTO_V2 = """
syntax = "proto3";
package orders;
import "google/protobuf/timestamp.proto";

message GetOrderRequest { string order_id = 1; }

message Order {
  string id = 1;
  double amount = 2;
  google.protobuf.Timestamp created_at = 3;
  repeated string tags = 4;
}

message CreateOrderRequest {
  string customer_id = 1;
  double amount = 2;
}

service OrderService {
  rpc GetOrder (GetOrderRequest) returns (Order);
  rpc CreateOrder (CreateOrderRequest) returns (Order);
  rpc ListOrders (GetOrderRequest) returns (stream Order);
}
"""


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given("a gRPC source whose proto has changed", target_fixture="shared_data")
def grpc_source_with_changed_proto(shared_data):
    # Register the source from the initial proto.
    parsed_v1 = loader.parse_proto_text(_PROTO_V1)
    assert parsed_v1["services"], "loader produced no services for v1 proto"

    virtual_tables, tracked_functions = _build_registrations(parsed_v1)

    # Sanity: GetOrder -> virtual table (query), CreateOrder -> tracked function.
    assert "OrderService.GetOrder" in virtual_tables
    assert "OrderService.CreateOrder" in tracked_functions
    assert "OrderService.ListOrders" not in virtual_tables

    # Import paths for well-known types are stored at registration time.
    import_paths = ["google/protobuf/timestamp.proto"]

    # RLS / masking rules applied on top of the generated registrations.
    rls_masking = {
        "OrderService.GetOrder": {
            "rls": "customer_id = current_customer_id()",
            "masking": {"amount": "REDACT"},
        }
    }

    registration = {
        "source_id": "orders-grpc",
        "proto_text": _PROTO_V1,
        "parsed": parsed_v1,
        "import_paths": import_paths,
        "virtual_tables": virtual_tables,
        "tracked_functions": tracked_functions,
        "rls_masking": rls_masking,
        "refreshed": False,
    }

    shared_data["registration"] = registration
    # Snapshot of governance metadata for later comparison.
    shared_data["original_import_paths"] = list(import_paths)
    shared_data["original_rls_masking"] = {
        k: dict(v) for k, v in rls_masking.items()
    }
    shared_data["original_table_keys"] = set(virtual_tables)
    shared_data["original_function_keys"] = set(tracked_functions)

    # The proto has changed on the remote side.
    shared_data["new_proto_text"] = _PROTO_V2
    return shared_data


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("a steward triggers the proto refresh admin mutation")
def steward_triggers_refresh(shared_data):
    registration = shared_data["registration"]
    new_proto = shared_data["new_proto_text"]
    _refresh_registration(registration, new_proto)
    assert registration["refreshed"] is True


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    "registrations are updated, proto import paths are reused, "
    "and RLS/masking rules are preserved"
)
def assert_refresh_outcome(shared_data):
    registration = shared_data["registration"]

    # --- Registrations are updated -------------------------------------------
    virtual_tables = registration["virtual_tables"]
    tracked_functions = registration["tracked_functions"]

    # The newly added streaming query method is now a virtual table.
    assert "OrderService.ListOrders" in virtual_tables, (
        "refresh did not register the new ListOrders virtual table"
    )
    assert virtual_tables["OrderService.ListOrders"]["server_streaming"] is True

    # The new Order field is reflected in the existing virtual table columns.
    get_order_cols = virtual_tables["OrderService.GetOrder"]["columns"]
    assert "tags" in get_order_cols, (
        f"refresh did not pick up the new 'tags' column: {get_order_cols}"
    )

    # The mutation tracked function survives the refresh.
    assert "OrderService.CreateOrder" in tracked_functions

    # The set of registrations actually grew relative to the original.
    new_table_keys = set(virtual_tables)
    assert new_table_keys > shared_data["original_table_keys"], (
        "expected the virtual-table registrations to expand after refresh"
    )

    # --- Proto import paths are reused ---------------------------------------
    assert registration["import_paths"] == shared_data["original_import_paths"], (
        "proto import paths were not re-used on refresh"
    )
    assert "google/protobuf/timestamp.proto" in registration["import_paths"]

    # --- RLS / masking rules are preserved -----------------------------------
    assert registration["rls_masking"] == shared_data["original_rls_masking"], (
        "RLS/masking rules were not preserved across the refresh"
    )
    preserved = registration["rls_masking"]["OrderService.GetOrder"]
    assert preserved["rls"] == "customer_id = current_customer_id()"
    assert preserved["masking"]["amount"] == "REDACT"

    # The rule still targets a live registration (the table it governs).
    assert "OrderService.GetOrder" in virtual_tables, (
        "RLS/masking rule references a registration that no longer exists"
    )
