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

from __future__ import annotations

from provisa.compiler.schema_gen import (
    SchemaInput,
    _build_visible_tables,
    _assign_names,
    _can_see_relationship,
)

# Trino type → proto type
_PROTO_TYPE_MAP: dict[str, str] = {
    "tinyint": "int32",
    "smallint": "int32",
    "integer": "int32",
    "int": "int32",
    "bigint": "int64",
    "varchar": "string",
    "char": "string",
    "varbinary": "bytes",
    "uuid": "string",
    "boolean": "bool",
    "real": "double",
    "double": "double",
    "decimal": "double",
    "numeric": "double",
    "timestamp": "google.protobuf.Timestamp",
    "timestamp with time zone": "google.protobuf.Timestamp",
    "date": "string",
    "time": "string",
    "time with time zone": "string",
    "json": "string",
    "jsonb": "string",
}


def _trino_to_proto(trino_type: str) -> str:
    """Map a Trino data type to a proto3 type."""
    normalized = trino_type.lower().strip()
    if normalized in _PROTO_TYPE_MAP:
        return _PROTO_TYPE_MAP[normalized]
    base = normalized.split("(")[0].strip()
    if base in _PROTO_TYPE_MAP:
        return _PROTO_TYPE_MAP[base]
    if normalized.startswith("array(") and normalized.endswith(")"):
        inner = normalized[6:-1]
        return _trino_to_proto(inner)
    raise ValueError(f"Unmapped Trino type for proto: {trino_type!r}")


def _needs_timestamp_import(columns: list[tuple[str, str]]) -> bool:
    """Check if any column maps to google.protobuf.Timestamp."""
    return any(
        _trino_to_proto(dtype) == "google.protobuf.Timestamp"
        for _, dtype in columns
    )


def _is_array_type(trino_type: str) -> bool:
    return trino_type.lower().strip().startswith("array(")


def generate_proto(si: SchemaInput) -> str:
    """Generate a .proto file content string for a role's visible schema."""
    tables = _build_visible_tables(si)
    if not tables:
        raise ValueError(
            f"No tables visible to role {si.role['id']!r}. "
            f"Cannot generate proto."
        )

    _assign_names(tables, si.naming_rules)
    table_lookup = {t.table_id: t for t in tables}

    # Collect visible relationships
    visible_rels = [
        r for r in si.relationships
        if _can_see_relationship(r, table_lookup)
    ]

    # Check if timestamp import needed
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
        lines.append("")

    # Generate message types per table
    for t in sorted(tables, key=lambda t: t.type_name):
        # Collect columns sorted for deterministic field numbers
        sorted_cols = sorted(t.visible_columns, key=lambda c: c["column_name"])
        field_num = 1

        # --- Data message ---
        lines.append(f"message {t.type_name} {{")
        col_field_nums: dict[str, int] = {}
        for col in sorted_cols:
            meta = t.column_metadata.get(col["column_name"])
            if meta is None:
                raise ValueError(
                    f"Column {col['column_name']!r} on {t.table_name!r} "
                    f"missing Trino metadata."
                )
            proto_type = _trino_to_proto(meta.data_type)
            repeated = "repeated " if _is_array_type(meta.data_type) else ""
            lines.append(f"  {repeated}{proto_type} {col['column_name']} = {field_num};")
            col_field_nums[col["column_name"]] = field_num
            field_num += 1

        # Add relationship fields
        for rel in visible_rels:
            if rel["source_table_id"] == t.table_id:
                target = table_lookup[rel["target_table_id"]]
                if rel["cardinality"] == "many-to-one":
                    lines.append(f"  {target.type_name} {target.field_name} = {field_num};")
                elif rel["cardinality"] == "one-to-many":
                    lines.append(
                        f"  repeated {target.type_name} {target.field_name} = {field_num};"
                    )
                field_num += 1

        lines.append("}")
        lines.append("")

        # --- Filter input message ---
        lines.append(f"message {t.type_name}Filter {{")
        filter_num = 1
        for col in sorted_cols:
            meta = t.column_metadata.get(col["column_name"])
            if meta is None:
                continue
            proto_type = _trino_to_proto(meta.data_type)
            # Filters use the base scalar type (not Timestamp)
            if proto_type == "google.protobuf.Timestamp":
                filter_proto = "string"
            else:
                filter_proto = proto_type
            lines.append(f"  {filter_proto} {col['column_name']} = {filter_num};")
            filter_num += 1
        lines.append("}")
        lines.append("")

        # --- Request message ---
        lines.append(f"message {t.type_name}Request {{")
        lines.append(f"  {t.type_name}Filter filter = 1;")
        lines.append("  int32 limit = 2;")
        lines.append("  int32 offset = 3;")
        lines.append("}")
        lines.append("")

    # --- Mutation input messages ---
    nosql_types = {"mongodb", "cassandra"}
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
            proto_type = _trino_to_proto(meta.data_type)
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
            f"  rpc Query{t.type_name}({t.type_name}Request) "
            f"returns (stream {t.type_name});"
        )
    for t in sorted(tables, key=lambda t: t.type_name):
        if si.source_types and si.source_types.get(t.source_id, "") in nosql_types:
            continue
        lines.append(
            f"  rpc Insert{t.type_name}({t.type_name}Input) "
            f"returns (MutationResponse);"
        )
    lines.append("}")
    lines.append("")

    return "\n".join(lines)
