# Copyright (c) 2026 Kenneth Stott
# Canary: 7d539aaa-e909-42e0-a7fe-e45be5c0a5ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generate .proto file content from SchemaInput (per role).

Mirrors schema_gen visibility logic: only visible tables/columns per role.
"""

# Requirements: REQ-039, REQ-045, REQ-051
from __future__ import annotations

from provisa.compiler.schema_gen import (
    SchemaInput,
    _IMPLICIT_TRAVERSAL_DOMAINS,
    _assign_names,
    _build_domain_alias_map,
    _build_visible_tables,
    _can_see_relationship,
)

# the engine type → proto type
_PROTO_TYPE_MAP: dict[str, str] = {
    "tinyint": "int32",
    "smallint": "int32",
    "integer": "int32",
    "int": "int32",
    "bigint": "int64",
    "varchar": "string",
    "char": "string",
    "varbinary": "bytes",
    "bytea": "bytes",
    "blob": "bytes",
    "uuid": "string",
    "boolean": "bool",
    "real": "double",
    "double": "double",
    "decimal": "double",
    "numeric": "double",
    "timestamp": "google.protobuf.Timestamp",
    "timestamp with time zone": "google.protobuf.Timestamp",
    "datetime": "google.protobuf.Timestamp",  # SQLite / MySQL timestamp type name
    "date": "string",
    "time": "string",
    "time with time zone": "string",
    "json": "string",
    "jsonb": "string",
    "text": "string",
    "float4": "float",
    "float8": "double",
}


def _physical_to_proto(column_type: str) -> str:
    normalized = column_type.lower().strip()
    if normalized in _PROTO_TYPE_MAP:
        return _PROTO_TYPE_MAP[normalized]
    base = normalized.split("(")[0].strip()
    if base in _PROTO_TYPE_MAP:
        return _PROTO_TYPE_MAP[base]
    if normalized.startswith("array(") and normalized.endswith(")"):
        inner = normalized[6:-1]
        return _physical_to_proto(inner)
    raise ValueError(f"Unmapped column type for proto: {column_type!r}")


def _needs_timestamp_import(columns: list[tuple[str, str]]) -> bool:
    return any(_physical_to_proto(dtype) == "google.protobuf.Timestamp" for _, dtype in columns)


def _is_array_type(column_type: str) -> bool:
    return column_type.lower().strip().startswith("array(")


def _to_proto_type_name(gql_name: str) -> str:
    """Convert GQL type name to proto3 PascalCase. PS__Pets → PsPets."""
    if "__" in gql_name:
        prefix, rest = gql_name.split("__", 1)
        return prefix.capitalize() + rest
    return gql_name


def _to_proto_field_name(gql_name: str) -> str:
    """Convert GQL field name to proto3 snake_case. ps__pets → ps_pets."""
    return gql_name.replace("__", "_")


def generate_proto(si: SchemaInput) -> str:  # REQ-039, REQ-045, REQ-051
    """Generate a .proto file content string for a role's visible schema."""
    tables = _build_visible_tables(si)
    if not tables:
        raise ValueError(f"No tables visible to role {si.role['id']!r}. Cannot generate proto.")

    domain_alias_map = _build_domain_alias_map(si.domains)
    _assign_names(
        tables,
        si.naming_rules,
        domain_prefix=si.domain_prefix,
        domain_alias_map=domain_alias_map,
    )
    # Convert GQL-style names (PS__Pets / ps__pets) to proto3 conventions (PsPets / ps_pets)
    for t in tables:
        t.type_name = _to_proto_type_name(t.type_name)
        t.field_name = _to_proto_field_name(t.field_name)

    table_lookup = {t.table_id: t for t in tables}
    visible_rels = [r for r in si.relationships if _can_see_relationship(r, table_lookup)]

    all_columns: list[tuple[str, str]] = []
    for t in tables:
        for col in t.visible_columns:
            meta = t.column_metadata.get(col["column_name"])
            if meta:
                all_columns.append((col["column_name"], meta.data_type))

    lines: list[str] = []
    lines.append('syntax = "proto3";')
    lines.append("")
    lines.append("package provisa.v1;")
    lines.append("")

    if _needs_timestamp_import(all_columns):
        lines.append('import "google/protobuf/timestamp.proto";')
    lines.append('import "google/protobuf/field_mask.proto";')
    lines.append("")

    # --- Query message (mirrors GraphQL type Query) ---
    _root_ids = si.root_table_ids
    _accessible = set(si.role.get("domain_access") or [])
    _all_access = not _accessible or "*" in _accessible
    root_tables = [
        t
        for t in sorted(tables, key=lambda t: t.type_name)
        if (_root_ids is None or t.table_id in _root_ids)
        and (_all_access or t.domain_id not in _IMPLICIT_TRAVERSAL_DOMAINS)
    ]
    lines.append("message Query {")
    for i, t in enumerate(root_tables, start=1):
        lines.append(f"  repeated {t.type_name} {t.field_name} = {i};")
    lines.append("}")
    lines.append("")

    # --- Data + Filter + Request messages ---
    nosql_types = {"mongodb", "cassandra"}
    for t in sorted(tables, key=lambda t: t.type_name):
        sorted_cols = sorted(t.visible_columns, key=lambda c: c["column_name"])
        field_num = 1

        lines.append(f"message {t.type_name} {{")
        used_fields: set[str] = set()
        for col in sorted_cols:
            meta = t.column_metadata.get(col["column_name"])
            if meta is None:
                continue
            proto_type = _physical_to_proto(meta.data_type)
            repeated = "repeated " if _is_array_type(meta.data_type) else ""
            lines.append(f"  {repeated}{proto_type} {col['column_name']} = {field_num};")
            used_fields.add(col["column_name"])
            field_num += 1

        for rel in visible_rels:
            if rel["source_table_id"] == t.table_id:
                target = table_lookup.get(rel["target_table_id"])
                if target is None or target.field_name in used_fields:
                    continue
                used_fields.add(target.field_name)
                if rel["cardinality"] == "many-to-one":
                    lines.append(f"  {target.type_name} {target.field_name} = {field_num};")
                elif rel["cardinality"] == "one-to-many":
                    lines.append(
                        f"  repeated {target.type_name} {target.field_name} = {field_num};"
                    )
                else:
                    continue
                field_num += 1

        lines.append("}")
        lines.append("")

        lines.append(f"message {t.type_name}Filter {{")
        filter_num = 1
        for col in sorted_cols:
            meta = t.column_metadata.get(col["column_name"])
            if meta is None:
                continue
            proto_type = _physical_to_proto(meta.data_type)
            filter_proto = "string" if proto_type == "google.protobuf.Timestamp" else proto_type
            lines.append(f"  {filter_proto} {col['column_name']} = {filter_num};")
            filter_num += 1
        lines.append("}")
        lines.append("")

        lines.append(f"message {t.type_name}Request {{")
        lines.append(f"  {t.type_name}Filter filter = 1;")
        lines.append("  int32 limit = 2;")
        lines.append("  int32 offset = 3;")
        lines.append("  google.protobuf.FieldMask read_mask = 4;")
        lines.append("}")
        lines.append("")

    # --- Mutation input messages ---
    for t in sorted(tables, key=lambda t: t.type_name):
        if si.source_types and si.source_types.get(t.source_id, "") in nosql_types:
            continue
        sorted_cols = sorted(t.visible_columns, key=lambda c: c["column_name"])
        lines.append(f"message {t.type_name}Input {{")
        input_num = 1
        for col in sorted_cols:
            meta = t.column_metadata.get(col["column_name"])
            if meta is None:
                continue
            proto_type = _physical_to_proto(meta.data_type)
            lines.append(f"  {proto_type} {col['column_name']} = {input_num};")
            input_num += 1
        lines.append("}")
        lines.append("")

    # --- Mutation response ---
    lines.append("message MutationResponse {")
    lines.append("  int32 affected_rows = 1;")
    lines.append("}")
    lines.append("")

    # --- Service ---
    lines.append("service ProvisaService {")
    for t in sorted(tables, key=lambda t: t.type_name):
        lines.append(
            f"  rpc Query{t.type_name}({t.type_name}Request) returns (stream {t.type_name});"
        )
    for t in sorted(tables, key=lambda t: t.type_name):
        if si.source_types and si.source_types.get(t.source_id, "") in nosql_types:
            continue
        lines.append(f"  rpc Insert{t.type_name}({t.type_name}Input) returns (MutationResponse);")
    lines.append("}")
    lines.append("")

    return "\n".join(lines)
