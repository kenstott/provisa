# Copyright (c) 2026 Kenneth Stott
# Canary: 35e6a967-597d-4950-a970-625c6c674b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Map parsed proto schema to GrpcQuery / GrpcMutation descriptors (Phase AR).

Pure logic — no I/O, no grpcio dependency.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Query-method name prefixes (REQ-323)
# ---------------------------------------------------------------------------

QUERY_PREFIXES: tuple[str, ...] = (
    "Get", "List", "Find", "Fetch", "Search", "Stream",
)

# ---------------------------------------------------------------------------
# Proto → SQL type map (REQ-324)
# ---------------------------------------------------------------------------

_PROTO_TO_SQL: dict[str, str] = {
    "string": "text",
    "bytes": "text",
    "int32": "integer",
    "uint32": "integer",
    "sint32": "integer",
    "fixed32": "integer",
    "sfixed32": "integer",
    "int64": "bigint",
    "uint64": "bigint",
    "sint64": "bigint",
    "fixed64": "bigint",
    "sfixed64": "bigint",
    "float": "real",
    "double": "numeric",
    "bool": "boolean",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ColumnDef:
    name: str
    type: str  # SQL type


@dataclass
class GrpcQuery:
    service: str
    method: str
    full_method_path: str  # e.g. "/orders.OrderService/GetOrder"
    input_message: str
    output_message: str
    columns: list[ColumnDef] = field(default_factory=list)
    input_fields: list[ColumnDef] = field(default_factory=list)
    server_streaming: bool = False


@dataclass
class GrpcMutation:
    service: str
    method: str
    full_method_path: str
    input_message: str
    output_message: str
    input_fields: list[ColumnDef] = field(default_factory=list)
    return_columns: list[ColumnDef] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_query_method(method_name: str) -> bool:
    """Return True when the method name starts with a query-indicating prefix."""
    return any(method_name.startswith(p) for p in QUERY_PREFIXES)


def _proto_field_to_sql(field_type: str, repeated: bool, enum_names: set[str]) -> str:
    if repeated:
        return "jsonb"
    if field_type in enum_names:
        return "text"
    sql = _PROTO_TO_SQL.get(field_type)
    if sql:
        return sql
    # Nested message or unknown type → jsonb
    return "jsonb"


def _message_columns(
    msg_name: str,
    messages: dict[str, list[dict]],
    enum_names: set[str],
) -> list[ColumnDef]:
    return [
        ColumnDef(
            name=f["name"],
            type=_proto_field_to_sql(f["type"], f["repeated"], enum_names),
        )
        for f in messages.get(msg_name, [])
    ]


def _full_method_path(package: str, service_name: str, method_name: str) -> str:
    if package:
        return f"/{package}.{service_name}/{method_name}"
    return f"/{service_name}/{method_name}"


# ---------------------------------------------------------------------------
# Main mapper
# ---------------------------------------------------------------------------

def map_proto(
    proto_dict: dict,
    namespace: str,
    source_id: str,
    domain_id: str,
) -> tuple[list[GrpcQuery], list[GrpcMutation]]:
    """Map an intermediate proto dict to (queries, mutations).

    All names are prefixed with ``namespace__`` when namespace is non-empty.
    """
    queries: list[GrpcQuery] = []
    mutations: list[GrpcMutation] = []

    package = proto_dict.get("package", "")
    messages: dict[str, list[dict]] = proto_dict.get("messages", {})
    enum_names: set[str] = set(proto_dict.get("enums", []))

    for service in proto_dict.get("services", []):
        svc_name: str = service["name"]
        for method in service.get("methods", []):
            method_name: str = method["name"]
            input_type: str = method["input_type"]
            output_type: str = method["output_type"]
            server_streaming: bool = method.get("server_streaming", False)

            path = _full_method_path(package, svc_name, method_name)
            input_cols = _message_columns(input_type, messages, enum_names)
            output_cols = _message_columns(output_type, messages, enum_names)

            if is_query_method(method_name):
                queries.append(
                    GrpcQuery(
                        service=svc_name,
                        method=method_name,
                        full_method_path=path,
                        input_message=input_type,
                        output_message=output_type,
                        columns=output_cols,
                        input_fields=input_cols,
                        server_streaming=server_streaming,
                    )
                )
            else:
                mutations.append(
                    GrpcMutation(
                        service=svc_name,
                        method=method_name,
                        full_method_path=path,
                        input_message=input_type,
                        output_message=output_type,
                        input_fields=input_cols,
                        return_columns=output_cols,
                    )
                )

    return queries, mutations
