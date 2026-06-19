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

# REQ-323: a method whose name begins with one of these read verbs is a query by default.
# Per-method overrides take precedence; the structural heuristics (server-streaming /
# repeated-message output) act as a fallback for methods that don't follow this convention.
_QUERY_NAME_PREFIXES = ("get", "list", "find", "fetch", "search", "stream")


def _has_query_name_prefix(method_name: str) -> bool:
    name = method_name.lower()
    return any(name.startswith(p) for p in _QUERY_NAME_PREFIXES)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ColumnDef:
    name: str
    type: str  # SQL type
    object_fields: list["ColumnDef"] = field(default_factory=list)


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


def _output_has_repeated_message_field(
    output_type: str, messages: dict[str, list[dict]], enum_names: set[str]
) -> bool:
    """Return True if the output message has at least one repeated non-scalar, non-enum field.

    Repeated scalar fields (e.g. repeated string tags) do NOT qualify — they are array
    properties of a single entity. Repeated message fields (e.g. repeated Order orders)
    indicate a list-wrapper response pattern.
    """
    for f in messages.get(output_type, []):
        if f.get("repeated") and f["type"] not in _PROTO_TO_SQL and f["type"] not in enum_names:
            return True
    return False


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
    depth: int = 0,
) -> list[ColumnDef]:
    cols = []
    for f in messages.get(msg_name, []):
        sql_type = _proto_field_to_sql(f["type"], f["repeated"], enum_names)
        sub_fields: list[ColumnDef] = []
        if (
            not f["repeated"]
            and f["type"] not in _PROTO_TO_SQL
            and f["type"] not in enum_names
            and depth == 0
        ):
            sub_fields = _message_columns(f["type"], messages, enum_names, depth=1)
        cols.append(ColumnDef(name=f["name"], type=sql_type, object_fields=sub_fields))
    return cols


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
    method_overrides: dict[str, str] | None = None,
) -> tuple[list[GrpcQuery], list[GrpcMutation]]:
    """Map an intermediate proto dict to (queries, mutations).

    Classification priority:
      1. method_overrides[method_name] == "query" / "mutation" — explicit override
      2. server_streaming=True — streaming → query
      3. output message has a repeated message-type field (list-wrapper pattern) → query
      4. everything else (including scalar output) → mutation

    All names are prefixed with ``namespace__`` when namespace is non-empty.
    """
    queries: list[GrpcQuery] = []
    mutations: list[GrpcMutation] = []
    overrides = method_overrides or {}

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

            scalar_sql = _PROTO_TO_SQL.get(output_type)
            is_scalar_output = scalar_sql is not None or output_type not in messages
            if is_scalar_output:
                output_cols = [ColumnDef(name="value", type=scalar_sql or "text")]
            else:
                output_cols = _message_columns(output_type, messages, enum_names)

            override = overrides.get(method_name, "").lower()
            if override == "mutation":
                is_query = False
            elif override == "query":
                is_query = True
            elif _has_query_name_prefix(method_name):
                # REQ-323: Get/List/Find/Fetch/Search/Stream name prefix → query (read).
                is_query = True
            else:
                # Structural fallback for methods that don't follow the naming convention.
                is_query = not is_scalar_output and (
                    server_streaming
                    or _output_has_repeated_message_field(output_type, messages, enum_names)
                )

            if is_query:
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
