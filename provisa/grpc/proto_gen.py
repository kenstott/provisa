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

from typing import TYPE_CHECKING, Any

from provisa.compiler.aggregate_gen import _classify_columns
from provisa.compiler.schema_gen import (
    SchemaInput,
    _IMPLICIT_TRAVERSAL_DOMAINS,
    _assign_names,
    _build_domain_alias_map,
    _build_visible_tables,
    _can_see_relationship,
)

if TYPE_CHECKING:
    from provisa.grpc.field_numbering import FieldNumberAllocator

# the engine type → proto type
_PROTO_TYPE_MAP: dict[str, str] = {
    "tinyint": "int32",
    "smallint": "int32",
    "integer": "int32",
    "int": "int32",
    "bigint": "int64",
    "int64": "int64",  # BigQuery's own INFORMATION_SCHEMA.COLUMNS data_type for integers
    "long": "int64",  # Databricks SQL's own name for BIGINT (DESCRIBE TABLE / INFORMATION_SCHEMA)
    "varchar": "string",
    "char": "string",
    "varbinary": "bytes",
    "bytea": "bytes",
    "blob": "bytes",
    "bytes": "bytes",  # BigQuery's own binary column type name
    "uuid": "string",
    "boolean": "bool",
    "bool": "bool",  # BigQuery's own boolean type name
    "real": "double",
    "double": "double",
    "float64": "double",  # BigQuery's own floating-point type name
    "decimal": "double",
    "numeric": "double",
    "bignumeric": "double",  # BigQuery's extended-precision numeric type
    "number": "double",  # OpenAPI JSON-Schema "number" (provisa.openapi.register._OPENAPI_TYPE_MAP)
    "timestamp": "google.protobuf.Timestamp",
    "timestamp with time zone": "google.protobuf.Timestamp",
    "timestamptz": "google.protobuf.Timestamp",  # postgres timestamp with time zone alias
    "datetime": "google.protobuf.Timestamp",  # SQLite / MySQL timestamp type name
    "date": "string",
    "time": "string",
    "time with time zone": "string",
    "timetz": "string",  # postgres time with time zone alias
    "interval": "string",  # ISO 8601 duration text (ir_types.iso8601_duration)
    "json": "string",
    "jsonb": "string",
    "text": "string",
    "string": "string",  # OpenAPI JSON-Schema "string" (provisa.openapi.register._OPENAPI_TYPE_MAP)
    "float4": "float",
    "float": "float",  # the canonical IR name (provisa.core.ir_types) for 4-byte floats
    "float8": "double",
    "double precision": "double",  # PostgreSQL's information_schema spelling (sla_breach_log)
}


def _physical_to_proto(column_type: str) -> str:
    normalized = column_type.lower().strip()
    if normalized in _PROTO_TYPE_MAP:
        return _PROTO_TYPE_MAP[normalized]
    base = normalized.split("(")[0].strip()
    if base in _PROTO_TYPE_MAP:
        return _PROTO_TYPE_MAP[base]
    # REQ-1965: a natively array, struct or map column is a JSON column: a string field carrying
    # the JSON the source or engine produced, at every depth. Never ``repeated``: a repeated
    # field could only be filled by decoding that JSON here, value by value.
    from provisa.core.native_types import parse_native_type

    if parse_native_type(normalized).nested:
        return "string"
    raise ValueError(f"Unmapped column type for proto: {column_type!r}")


def _needs_timestamp_import(columns: list[tuple[str, str]]) -> bool:
    return any(_physical_to_proto(dtype) == "google.protobuf.Timestamp" for _, dtype in columns)


def _is_array_type(column_type: str) -> bool:
    """Whether a parameter is an array (REQ-1986: a query vector). A parameter is an argument,
    not a column: it stays a repeated field of its element type, not a JSON string."""
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


# GraphQL scalar name (command argument type) → proto3 type. Mirrors the REST surface's
# _arg_type_to_openapi mapping so a command's arg shape is identical across surfaces (REQ-1156).
_ARG_PROTO_TYPE: dict[str, str] = {"int": "int64", "float": "double", "boolean": "bool"}


def _arg_proto_type(arg_type: str) -> str:
    return _ARG_PROTO_TYPE.get((arg_type or "").lower(), "string")


#: REQ-1365: the RPC that dereferences a semantic address -- the GraphQL ``_resolve`` root field,
#: which every role's schema carries, so every role's service carries it too (REQ-1929). A proto
#: identifier cannot start with an underscore, so the RPC is ``Resolve``.
RESOLVE_RPC = "Resolve"


def command_rpc_name(fn_name: str) -> str:
    """The per-command RPC / request-message base name: ``active_users`` → ``ActiveUsers`` (REQ-1156).

    The single naming authority both proto_gen and the server use, so ``Call{name}`` RPCs round-trip
    back to the registered command name deterministically."""
    return "".join(part.capitalize() for part in fn_name.replace("__", "_").split("_") if part)


def _numbers_for(
    field_numbers: "FieldNumberAllocator | None",
    table_id,
    namespace: str,
    names: list[str],
    authoritative: bool,
) -> dict[str, int]:
    """Field numbers for ``names`` in this (table_id, namespace) — stable across regenerations when
    ``field_numbers`` is given (REQ-1903), else the old fresh-enumerate behavior. ``field_numbers``
    is only None for callers outside the served wire/per-role schema build (e.g. endpoint_dev.py's
    design-time proto preview), which is never decoded by a real cached client stub."""
    if field_numbers is None:
        return {name: i for i, name in enumerate(names, start=1)}
    numbers = field_numbers.numbers_for(table_id, namespace, names)
    if authoritative:
        field_numbers.reconcile_removed(table_id, namespace, set(names))
    return numbers


def parameter_name(column_name: str) -> str:  # REQ-1986
    """A parameter column's name as its ``{Type}Params`` field: the column without ``_nf_``."""
    return column_name[4:] if column_name.startswith("_nf_") else column_name


def _parameter_proto_types(t) -> dict[str, str]:  # REQ-1986
    """``{Type}Params`` field name → its proto field's label and type (``optional double``,
    ``repeated double`` for an array parameter such as a query vector), sorted."""
    types: dict[str, str] = {}
    for nfc in sorted(t.native_filter_columns, key=lambda c: c["column_name"]):
        meta = t.column_metadata.get(nfc["column_name"].lower())
        # Typed as schema_gen types the parameter's GraphQL argument: text when unrecorded. An
        # array parameter is repeated of its element type (a column of that type is JSON).
        array = False
        if meta is None:
            proto_type = "string"
        else:
            array = _is_array_type(meta.data_type)
            typed = meta.data_type.strip()[len("array(") : -1] if array else meta.data_type
            proto_type = _physical_to_proto(typed)
        filter_proto = "string" if proto_type == "google.protobuf.Timestamp" else proto_type
        label = "repeated" if array else "optional"
        types[parameter_name(nfc["column_name"])] = f"{label} {filter_proto}"
    return types


def _emit_aggregate_messages(
    lines: list[str],
    t,
    field_numbers: "FieldNumberAllocator | None" = None,
    authoritative: bool = False,
) -> None:
    """Emit ``{Type}AggregateResult`` (+ its per-function sub-messages) for a table with
    ``enable_aggregates`` and/or ``enable_group_by`` set (REQ-1359).

    Mirrors the nested shape GraphQL's ``build_agg_fields_type`` builds (count/sum/avg/stddev/
    variance/min/max), reusing the same numeric/comparable column classification rather than
    reimplementing REQ-196's rules."""
    numeric_cols, comparable_cols = _classify_columns(t.visible_columns, t.column_metadata)

    if numeric_cols:
        numeric_names = [col_name for col_name, _col_type in numeric_cols]
        numbers = _numbers_for(
            field_numbers, t.table_id, "agg_numeric", numeric_names, authoritative
        )
        for suffix in ("SumFields", "AvgFields", "StddevFields", "VarianceFields"):
            lines.append(f"message {t.type_name}{suffix} {{")
            # `optional`: an aggregate over no rows is NULL, the field left unset -- never 0.
            for col_name in numeric_names:
                lines.append(f"  optional double {col_name} = {numbers[col_name]};")
            lines.append("}")
            lines.append("")

    if comparable_cols:
        comparable_names = [col_name for col_name, _col_type in comparable_cols]
        numbers = _numbers_for(
            field_numbers, t.table_id, "agg_comparable", comparable_names, authoritative
        )
        comparable_type = dict(comparable_cols)
        for suffix in ("MinFields", "MaxFields"):
            lines.append(f"message {t.type_name}{suffix} {{")
            for col_name in comparable_names:
                proto_type = _physical_to_proto(comparable_type[col_name])
                # As the row's columns: a scalar is `optional`, a Timestamp has presence already.
                presence = "" if proto_type == "google.protobuf.Timestamp" else "optional "
                lines.append(f"  {presence}{proto_type} {col_name} = {numbers[col_name]};")
            lines.append("}")
            lines.append("")

    lines.append(f"message {t.type_name}AggregateRequest {{")
    lines.append("  repeated string funcs = 1;")
    # REQ-1882: mirrors GroupByRequest.columns — see that field's comment.
    lines.append("  repeated string columns = 2;")
    lines.append("}")
    lines.append("")

    lines.append(f"message {t.type_name}AggregateResult {{")
    agg_num = 1
    lines.append(f"  int32 count = {agg_num};")
    agg_num += 1
    if numeric_cols:
        for suffix, field_name in (
            ("SumFields", "sum"),
            ("AvgFields", "avg"),
            ("StddevFields", "stddev"),
            ("VarianceFields", "variance"),
        ):
            lines.append(f"  {t.type_name}{suffix} {field_name} = {agg_num};")
            agg_num += 1
    if comparable_cols:
        for suffix, field_name in (("MinFields", "min"), ("MaxFields", "max")):
            lines.append(f"  {t.type_name}{suffix} {field_name} = {agg_num};")
            agg_num += 1
    lines.append("}")
    lines.append("")

    if t.enable_group_by:
        lines.append(f"message {t.type_name}GroupByRequest {{")
        lines.append("  repeated string by = 1;")
        lines.append(f"  {t.type_name}Filter filter = 2;")
        lines.append("  int32 limit = 3;")
        lines.append("  int32 offset = 4;")
        lines.append("  bool include_nodes = 5;")
        lines.append("  repeated string include = 6;")
        lines.append("  repeated string funcs = 7;")
        # REQ-1882: restricts sum/avg/stddev/variance/min/max to a caller-chosen subset of
        # columns, mirroring `funcs`'s function-level restriction but at the column level — an
        # unset `funcs` already meant "every function"; before this field existed, an unset (or
        # even a set) `funcs` still meant "every eligible column of that type" with no way to
        # narrow further. Empty/unset means every eligible column, same convention as `funcs`.
        lines.append("  repeated string columns = 8;")
        # REQ-2007: the group-by's order, as GraphQL's {Type}GroupByOrderBy list.
        lines.append(f"  repeated {t.type_name}GroupByOrder order_by = 9;")
        lines.append("}")
        lines.append("")

        # REQ-2007: one entry of a group-by's order. ``func`` is count, sum, avg, stddev,
        # variance, min, max, or group_key (order by a key the request groups by); ``column`` is
        # the column (none for count); ``direction`` is asc, desc, or one with nulls_first/last.
        lines.append(f"message {t.type_name}GroupByOrder {{")
        lines.append("  string func = 1;")
        lines.append("  string column = 2;")
        lines.append("  string direction = 3;")
        lines.append("}")
        lines.append("")

        lines.append(f"message {t.type_name}GroupByRow {{")
        lines.append("  string group_key = 1;")
        lines.append(f"  {t.type_name}AggregateResult aggregate = 2;")
        lines.append(f"  repeated {t.type_name} nodes = 3;")
        lines.append("}")
        lines.append("")


def _visible_commands(si: SchemaInput) -> list[dict]:
    """Commands (tracked functions) visible to this role, deduped by name (REQ-1156).

    The commands the role may call (``mutation_authz.command_reachable``): assigned to it
    (``visible_to`` names it) and in a domain it reaches, as on every other surface."""
    from provisa.security.mutation_authz import command_reachable

    out: list[dict] = []
    seen: set[str] = set()
    # Functions AND webhooks are governed commands (REQ-872): both get a Call{Cmd} RPC.
    for fn in [*(si.functions or []), *(si.webhooks or [])]:
        name = fn.get("name")
        if not name or name in seen:
            continue
        if not command_reachable(fn, si.role):
            continue
        seen.add(name)
        out.append(fn)
    return sorted(out, key=lambda f: f["name"])


def generate_proto(
    si: SchemaInput,
    field_numbers: "FieldNumberAllocator | None" = None,
    authoritative: bool = False,
) -> str:  # REQ-039, REQ-045, REQ-051, REQ-1903
    """Generate a .proto file content string for a role's visible schema.

    ``field_numbers`` (REQ-1903): when given, every per-column field number is allocated through it
    instead of a fresh ``enumerate(sorted_cols)`` — stable across regenerations, so an older-
    generation client stub never decodes a later generation's bytes into the wrong field. Pass the
    SAME allocator instance across every ``generate_proto`` call in one schema build (every role
    plus the union/wire schema): the server's wire bytes are decoded against a role's own
    downloaded ``.proto``, so they must agree on numbers for the same column.

    ``authoritative``: set only for the union/wire schema call, whose column set for a table is the
    FULL set (every column, ``visible_to=[]``) — the one point in a build that can tell a genuinely
    dropped column from a role simply not seeing it, so only that call may retire a field number.
    """
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
    from provisa.security.rights import reaches_all_domains

    # Same root rule as the GraphQL Query type (schema_gen): an implicit-traversal domain is a
    # root only for a role that reaches it. An empty domain_access reaches no domain.
    _accessible = set(si.role["domain_access"])
    _all_access = reaches_all_domains(si.role["domain_access"])
    root_tables = [
        t
        for t in sorted(tables, key=lambda t: t.type_name)
        if (_root_ids is None or t.table_id in _root_ids)
        and (
            _all_access
            or t.domain_id not in _IMPLICIT_TRAVERSAL_DOMAINS
            or t.domain_id in _accessible
        )
    ]
    lines.append("message Query {")
    for i, t in enumerate(root_tables, start=1):
        lines.append(f"  repeated {t.type_name} {t.field_name} = {i};")
    lines.append("}")
    lines.append("")

    # --- Data + Filter + Request messages ---
    for t in sorted(tables, key=lambda t: t.type_name):
        sorted_cols = sorted(t.visible_columns, key=lambda c: c["column_name"])
        col_names = [
            c["column_name"] for c in sorted_cols if t.column_metadata.get(c["column_name"])
        ]

        used_fields: set[str] = set(col_names)
        rel_fields: list[tuple[dict, Any]] = []
        for rel in visible_rels:
            if rel["source_table_id"] == t.table_id:
                target = table_lookup.get(rel["target_table_id"])
                if target is None or target.field_name in used_fields:
                    continue
                used_fields.add(target.field_name)
                rel_fields.append((rel, target))
        rel_names = [target.field_name for _rel, target in rel_fields]

        # REQ-1903: columns + relation fields share ONE number space per table (they're all fields
        # of the same {Type} message), numbered stably across regenerations.
        row_numbers = _numbers_for(
            field_numbers, t.table_id, "row", col_names + rel_names, authoritative
        )

        lines.append(f"message {t.type_name} {{")
        for col in sorted_cols:
            meta = t.column_metadata.get(col["column_name"])
            if meta is None:
                continue
            proto_type = _physical_to_proto(meta.data_type)
            field_num = row_numbers[col["column_name"]]
            # `optional`: a null is the field left unset, never its type's zero (a masked amount
            # read as 0.0). Every scalar column, the NOT NULL ones too: a regex mask fails closed
            # to NULL on any column that is not text (REQ-2142). A Timestamp is a message, which
            # has presence already. The same for every role and the wire, so they read one byte
            # stream alike.
            presence = "" if proto_type == "google.protobuf.Timestamp" else "optional "
            lines.append(f"  {presence}{proto_type} {col['column_name']} = {field_num};")

        for rel, target in rel_fields:
            field_num = row_numbers[target.field_name]
            if rel["cardinality"] in ("many-to-one", "one-to-one"):
                lines.append(f"  {target.type_name} {target.field_name} = {field_num};")
            elif rel["cardinality"] == "one-to-many":
                lines.append(f"  repeated {target.type_name} {target.field_name} = {field_num};")
            else:
                raise ValueError(
                    f"unhandled relationship cardinality {rel['cardinality']!r} for {rel['id']!r}"
                )

        lines.append("}")
        lines.append("")

        # REQ-1899: a batched-rows counterpart to the per-row Query{Type} RPC below — one
        # {Type}Batch message carries up to _GRPC_BATCH_ROWS rows instead of one message per row,
        # so a large scan pays gRPC/HTTP2 per-message framing/serialization overhead far less
        # often (live-measured: 2,000,000 individual QueryOrderItems messages took ~190s vs.
        # Flight SQL's ~20s for the same data — the per-row streaming contract, not fetch
        # strategy, was the bottleneck; see REQ-1898's amendment). Additive: Query{Type} and
        # {Type} are UNCHANGED, so every existing gRPC client keeps working exactly as before.
        lines.append(f"message {t.type_name}Batch {{")
        lines.append(f"  repeated {t.type_name} rows = 1;")
        lines.append("}")
        lines.append("")

        # REQ-1903: {Type}Filter is its own message, so it gets its own independent number space
        # (a filter-only field doesn't have to start where the row message's numbering left off).
        filter_numbers = _numbers_for(field_numbers, t.table_id, "filter", col_names, authoritative)
        lines.append(f"message {t.type_name}Filter {{")
        for col in sorted_cols:
            meta = t.column_metadata.get(col["column_name"])
            if meta is None:
                continue
            proto_type = _physical_to_proto(meta.data_type)
            filter_proto = "string" if proto_type == "google.protobuf.Timestamp" else proto_type
            # `optional` gives HasField() real presence detection on these scalar fields, so
            # query_ir can distinguish "filter col = 0/false/\"\"" from "col not filtered" (REQ-1860).
            filter_num = filter_numbers[col["column_name"]]
            lines.append(f"  optional {filter_proto} {col['column_name']} = {filter_num};")
        lines.append("}")
        lines.append("")

        # REQ-1986, REQ-1991: the table's parameters, a message of their own beside the filter: a
        # filter field is an equality on a data column, a parameter is a value the source takes.
        param_types = _parameter_proto_types(t)
        if param_types:
            param_numbers = _numbers_for(
                field_numbers, t.table_id, "params", list(param_types), authoritative
            )
            lines.append(f"message {t.type_name}Params {{")
            for name, proto_type in param_types.items():
                lines.append(f"  {proto_type} {name} = {param_numbers[name]};")
            lines.append("}")
            lines.append("")

        lines.append(f"message {t.type_name}Request {{")
        lines.append(f"  {t.type_name}Filter filter = 1;")
        lines.append("  int32 limit = 2;")
        lines.append("  int32 offset = 3;")
        lines.append("  google.protobuf.FieldMask read_mask = 4;")
        # REQ-1899: client-chosen row count per {Type}Batch message for the Query{Type}Batch RPC
        # only — ignored by the plain per-row Query{Type} RPC. 0/unset falls back to the server's
        # own conservative default (_GRPC_BATCH_ROWS, provisa/grpc/server.py). The server is the
        # one place that knows every column's proto type, so it can't safely pick a big default
        # that's still safe for every table's width — the CLIENT knows which table it's asking
        # about and how wide its rows are, so it opts into a larger batch only when it knows
        # that's safe (e.g. a narrow table it already knows the row size for).
        lines.append("  int32 batch_rows = 5;")
        if param_types:
            lines.append(f"  {t.type_name}Params params = 6;")
        lines.append("}")
        lines.append("")

        # REQ-1359: aggregate/group-by protocol parity with GraphQL/JSON:API/REST.
        if t.enable_aggregates or t.enable_group_by:
            _emit_aggregate_messages(lines, t, field_numbers, authoritative)

    # --- Mutation input messages ---
    for t in sorted(tables, key=lambda t: t.type_name):
        # executor/write_capability.py; REQ-2074: and the role's insert grant, whose columns the
        # input carries.
        if "insert" not in t.write_ops or "insert" not in t.write_grants:
            continue
        granted = set(t.write_grants["insert"].get("columns") or [])
        sorted_cols = sorted(
            (c for c in t.visible_columns if c["column_name"] in granted),
            key=lambda c: c["column_name"],
        )
        input_col_names = [
            c["column_name"] for c in sorted_cols if t.column_metadata.get(c["column_name"])
        ]
        # REQ-1903: {Type}Input is its own message — independent number space, same as Filter.
        input_numbers = _numbers_for(
            field_numbers, t.table_id, "input", input_col_names, authoritative
        )
        lines.append(f"message {t.type_name}Input {{")
        for col in sorted_cols:
            meta = t.column_metadata.get(col["column_name"])
            if meta is None:
                continue
            proto_type = _physical_to_proto(meta.data_type)
            lines.append(
                f"  {proto_type} {col['column_name']} = {input_numbers[col['column_name']]};"
            )
        lines.append("}")
        lines.append("")

    # --- Mutation response ---
    lines.append("message MutationResponse {")
    lines.append("  int32 affected_rows = 1;")
    lines.append("}")
    lines.append("")

    # --- Command invocation (REQ-1156) ---
    # A single generic RPC exposes every registered command (tracked function/webhook) over gRPC
    # without a per-command proto: the request carries the command name + JSON-encoded args and the
    # response carries the JSON-encoded governed rows. Invocation routes through the one shared
    # invoke_tracked_function executor, so the command admission and governance apply identically to every
    # other surface.
    lines.append("message CommandRequest {")
    lines.append("  string name = 1;")
    lines.append("  string args_json = 2;")
    lines.append("}")
    lines.append("")
    lines.append("message CommandResponse {")
    lines.append("  string rows_json = 1;")
    lines.append("}")
    lines.append("")

    # --- resolve(uri, as_of, as_recorded) (REQ-1365, REQ-1163): the GraphQL field's arguments; the answer, whose shape
    # is the addressed row's, as JSON (as a command's rows are).
    lines.append(f"message {RESOLVE_RPC}AddressRequest {{")
    lines.append("  string uri = 1;")
    lines.append("  string as_of = 2;")
    lines.append("  string as_recorded = 3;")
    lines.append("}")
    lines.append("")
    lines.append(f"message {RESOLVE_RPC}AddressResponse {{")
    lines.append("  string result_json = 1;")
    lines.append("}")
    lines.append("")

    # --- Per-command request messages (REQ-1156) ---
    # Beyond the generic CallCommand, every command visible to the role gets a typed request message
    # + its own RPC, so a gRPC client discovers commands by name with typed arguments via reflection
    # (not one opaque CallCommand). Query-kind -> unary CommandResponse (set returns carried as JSON
    # rows, since a command's return shape is dynamic); mutation-kind -> unary MutationResponse. All
    # route through the single invoke_tracked_function executor server-side.
    commands = _visible_commands(si)
    for fn in commands:
        cmd = command_rpc_name(fn["name"])
        lines.append(f"message {cmd}Request {{")
        for i, arg in enumerate([a for a in (fn.get("arguments") or []) if a.get("name")], start=1):
            lines.append(f"  {_arg_proto_type(arg.get('type', 'String'))} {arg['name']} = {i};")
        lines.append("}")
        lines.append("")

    # --- Service ---
    lines.append("service ProvisaService {")
    lines.append("  rpc CallCommand(CommandRequest) returns (CommandResponse);")
    lines.append(
        f"  rpc {RESOLVE_RPC}({RESOLVE_RPC}AddressRequest) returns ({RESOLVE_RPC}AddressResponse);"
    )
    for fn in commands:
        cmd = command_rpc_name(fn["name"])
        resp = "MutationResponse" if fn.get("kind") == "mutation" else "CommandResponse"
        lines.append(f"  rpc Call{cmd}({cmd}Request) returns ({resp});")
    # The tables a role may CALL are the roots of its Query message above — the root rule of the
    # GraphQL Query type. A table that is in the role's surface only to be traversed to (the
    # meta domain, for a role that does not reach it: read through each type's ``_meta``, never
    # from the root) has its message and no RPC, as it has no root field.
    for t in root_tables:
        lines.append(
            f"  rpc Query{t.type_name}({t.type_name}Request) returns (stream {t.type_name});"
        )
        # REQ-1899: additive batched-rows RPC, same request shape, {Type}Batch response stream.
        lines.append(
            f"  rpc Query{t.type_name}Batch({t.type_name}Request) "
            f"returns (stream {t.type_name}Batch);"
        )
        # REQ-1359: aggregate/group-by RPCs, gated the same way as GraphQL's
        # enable_aggregates/enable_group_by root fields.
        if t.enable_aggregates:
            lines.append(
                f"  rpc Query{t.type_name}Aggregate({t.type_name}AggregateRequest) "
                f"returns ({t.type_name}AggregateResult);"
            )
        if t.enable_group_by:
            lines.append(
                f"  rpc Query{t.type_name}GroupBy({t.type_name}GroupByRequest) "
                f"returns (stream {t.type_name}GroupByRow);"
            )
    for t in root_tables:
        # executor/write_capability.py; REQ-2074: and the role's insert grant.
        if "insert" not in t.write_ops or "insert" not in t.write_grants:
            continue
        lines.append(f"  rpc Insert{t.type_name}({t.type_name}Input) returns (MutationResponse);")
    lines.append("}")
    lines.append("")

    return "\n".join(lines)
