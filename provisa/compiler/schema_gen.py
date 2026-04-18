# Copyright (c) 2026 Kenneth Stott
# Canary: fe7dee37-1a51-4599-a719-d5e9249736c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build graphql-core schema from registration model + Trino metadata per role.

No third-party GraphQL framework (REQ-007). Uses graphql-core directly.
Domain-scoped, per-role column filtering (REQ-008, REQ-021).
"""

import re
from dataclasses import dataclass, field

from graphql import (
    GraphQLArgument,
    GraphQLBoolean,
    GraphQLDirective,
    GraphQLEnumType,
    GraphQLEnumValue,
    GraphQLField,
    GraphQLFloat,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLSchema,
    GraphQLString,
    specified_directives,
)
from graphql.language import DirectiveLocation

from provisa.compiler.aggregate_gen import build_aggregate_types
from provisa.compiler.enum_detect import build_enum_filter_types, resolve_column_type
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.naming import apply_convention, domain_gql_alias, domain_to_sql_name, generate_name, to_type_name
from provisa.compiler.type_map import FILTER_TYPE_MAP, JSONScalar, trino_to_graphql


@dataclass
class SchemaInput:
    """All data needed to generate a GraphQL schema for one role."""

    tables: list[dict]  # from table_repo.list_all() — includes "columns" sub-list
    relationships: list[dict]  # from rel_repo.list_all()
    column_types: dict[int, list[ColumnMetadata]]  # table_id → Trino column metadata
    naming_rules: list[dict]  # [{pattern, replacement}]
    role: dict  # from role_repo.get()
    domains: list[dict]  # from domain_repo.list_all()
    source_types: dict[str, str] | None = None  # source_id → type (for mutation eligibility)
    domain_prefix: bool = False  # prepend domain_id__ to all names
    physical_table_map: dict[str, str] | None = None  # virtual → physical table name
    naming_convention: str = "snake_case"  # none, snake_case, camelCase, PascalCase
    relay_pagination: bool = False  # global opt-in for _connection fields
    functions: list[dict] = field(default_factory=list)  # tracked DB functions
    webhooks: list[dict] = field(default_factory=list)  # tracked webhooks
    enum_types: dict = field(default_factory=dict)  # pg_name → GraphQLEnumType (REQ-221)
    approved_queries: list[dict] = field(default_factory=list)  # approved persisted queries for subscription SDL


@dataclass
class _TableInfo:
    """Internal: resolved table info for schema generation."""

    table_id: int
    field_name: str  # snake_case GraphQL field name
    type_name: str  # PascalCase GraphQL type name
    domain_id: str
    source_id: str
    schema_name: str
    table_name: str  # original DB table name
    visible_columns: list[dict]  # [{column_name, visible_to, alias?, description?}]
    column_metadata: dict[str, ColumnMetadata]  # column_name → metadata
    native_filter_columns: list[dict] = field(default_factory=list)  # [{column_name, native_filter_type}]
    alias: str | None = None  # explicit GraphQL name override
    description: str | None = None  # GraphQL type/field description
    naming_convention: str = "snake_case"  # resolved convention for this table
    relay_pagination: bool = False  # resolved relay flag for this table
    gql_fields: dict[str, GraphQLField] = field(default_factory=dict)


# --- GraphQL enum for ORDER BY direction ---

OrderDirection = GraphQLEnumType(
    "order_by",
    {
        "asc": GraphQLEnumValue("asc"),
        "desc": GraphQLEnumValue("desc"),
        "asc_nulls_first": GraphQLEnumValue("asc_nulls_first"),
        "asc_nulls_last": GraphQLEnumValue("asc_nulls_last"),
        "desc_nulls_first": GraphQLEnumValue("desc_nulls_first"),
        "desc_nulls_last": GraphQLEnumValue("desc_nulls_last"),
    },
)

# --- Provisa directive enums ---

RouteEngineEnum = GraphQLEnumType(
    "RouteEngine",
    {
        "FEDERATED": GraphQLEnumValue("FEDERATED", description="Route via Trino federation"),
        "DIRECT": GraphQLEnumValue("DIRECT", description="Route directly to source"),
    },
    description="Execution engine routing hint for @route.",
)

JoinStrategyEnum = GraphQLEnumType(
    "JoinStrategy",
    {
        "BROADCAST": GraphQLEnumValue("BROADCAST", description="Broadcast join distribution"),
        "PARTITIONED": GraphQLEnumValue("PARTITIONED", description="Partitioned (hash) join distribution"),
    },
    description="Join distribution strategy hint for @join.",
)

# Directive locations
_QS = [DirectiveLocation.QUERY, DirectiveLocation.SUBSCRIPTION]
_QMS = [DirectiveLocation.QUERY, DirectiveLocation.MUTATION, DirectiveLocation.SUBSCRIPTION]

PROVISA_DIRECTIVES = [
    GraphQLDirective(
        name="route",
        locations=_QMS,
        args={"engine": GraphQLArgument(GraphQLNonNull(RouteEngineEnum), description="FEDERATED or DIRECT")},
        description="Override execution engine routing.",
    ),
    GraphQLDirective(
        name="join",
        locations=_QS,
        args={"strategy": GraphQLArgument(GraphQLNonNull(JoinStrategyEnum), description="BROADCAST or PARTITIONED")},
        description="Set Trino join distribution strategy.",
    ),
    GraphQLDirective(
        name="reorder",
        locations=_QS,
        args={"enabled": GraphQLArgument(GraphQLNonNull(GraphQLBoolean), description="Set false to disable join reordering")},
        description="Control Trino join reordering.",
    ),
    GraphQLDirective(
        name="broadcastSize",
        locations=_QS,
        args={"size": GraphQLArgument(GraphQLNonNull(GraphQLString), description="Max broadcast table size, e.g. '100MB'")},
        description="Override max broadcast table size for Trino.",
    ),
    GraphQLDirective(
        name="watermark",
        locations=[DirectiveLocation.FIELD],
        args={},
        description="Mark field as the watermark column for subscription polling.",
    ),
    GraphQLDirective(
        name="sink",
        locations=[DirectiveLocation.SUBSCRIPTION],
        args={
            "topic": GraphQLArgument(GraphQLNonNull(GraphQLString), description="Kafka topic name"),
            "broker": GraphQLArgument(GraphQLString, description="Kafka bootstrap server (host:port)"),
        },
        description="Redirect subscription output to a Kafka topic.",
    ),
    GraphQLDirective(
        name="redirect",
        locations=_QS,
        args={
            "format": GraphQLArgument(GraphQLString, description="Output format: parquet, csv, arrow"),
            "threshold": GraphQLArgument(GraphQLInt, description="Row count threshold to trigger redirect"),
        },
        description="Redirect large results to object store.",
    ),
]

# --- Relay-style connection types for cursor pagination (REQ-218) ---

PageInfoType = GraphQLObjectType(
    "PageInfo",
    lambda: {
        "hasNextPage": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
        "hasPreviousPage": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
        "startCursor": GraphQLField(GraphQLString),
        "endCursor": GraphQLField(GraphQLString),
    },
)


def _build_connection_types(
    type_name: str, node_type: GraphQLObjectType,
) -> tuple[GraphQLObjectType, GraphQLObjectType]:
    """Build Edge and Connection types for cursor pagination."""
    edge_type = GraphQLObjectType(
        f"{type_name}Edge",
        lambda node_type=node_type: {
            "cursor": GraphQLField(GraphQLNonNull(GraphQLString)),
            "node": GraphQLField(GraphQLNonNull(node_type)),
        },
    )
    connection_type = GraphQLObjectType(
        f"{type_name}Connection",
        lambda edge_type=edge_type: {
            "edges": GraphQLField(
                GraphQLNonNull(GraphQLList(GraphQLNonNull(edge_type)))
            ),
            "pageInfo": GraphQLField(GraphQLNonNull(PageInfoType)),
        },
    )
    return edge_type, connection_type


def _build_visible_tables(si: SchemaInput) -> list[_TableInfo]:
    """Filter tables by role's domain access. Build per-table metadata."""
    role = si.role
    accessible = set(role["domain_access"])
    all_access = "*" in accessible

    result: list[_TableInfo] = []
    for table in si.tables:
        if not all_access and table["domain_id"] not in accessible:
            continue

        table_id = table["id"]
        if table_id not in si.column_types or not si.column_types[table_id]:
            import logging as _log
            _log.getLogger(__name__).warning(
                "No column metadata for table %r (id=%s) — skipping.", table["table_name"], table_id
            )
            continue
        col_meta = {m.column_name.lower(): m for m in si.column_types[table_id]}

        # Filter columns by role visibility; split native filter cols from regular cols
        visible_cols = [
            c for c in table["columns"]
            if role["id"] in c["visible_to"] and not c.get("native_filter_type")
        ]
        # Native filter cols are API parameters (path/query params), not data columns.
        # They are always exposed as query args regardless of visible_to — the role
        # only needs access to the table itself.
        native_filter_cols = [
            c for c in table["columns"]
            if c.get("native_filter_type")
        ]

        if not visible_cols and not native_filter_cols:
            continue

        # Resolve naming convention: table → source → global
        table_conv = table.get("naming_convention")
        source_conv = table.get("source_naming_convention")
        resolved_conv = table_conv or source_conv or si.naming_convention

        # Resolve relay_pagination: table → source → global (None = inherit)
        table_relay = table.get("relay_pagination")
        source_relay = table.get("source_relay_pagination")
        if table_relay is not None:
            resolved_relay = bool(table_relay)
        elif source_relay is not None:
            resolved_relay = bool(source_relay)
        else:
            resolved_relay = si.relay_pagination

        result.append(_TableInfo(
            table_id=table_id,
            field_name="",  # set after naming
            type_name="",
            domain_id=table["domain_id"],
            source_id=table["source_id"],
            schema_name=table["schema_name"],
            table_name=table["table_name"],
            visible_columns=visible_cols,
            native_filter_columns=native_filter_cols,
            column_metadata=col_meta,
            alias=table.get("alias"),
            description=table.get("description"),
            naming_convention=resolved_conv,
            relay_pagination=resolved_relay,
        ))

    return result


def _assign_names(
    tables: list[_TableInfo],
    naming_rules: list[dict],
    domain_prefix: bool = False,
    domain_alias_map: dict[str, str] | None = None,
) -> None:
    """Assign unique GraphQL names to each table."""
    # Group by domain for uniqueness scoping
    domain_groups: dict[str, list[_TableInfo]] = {}
    for t in tables:
        domain_groups.setdefault(t.domain_id, []).append(t)

    for domain_id, group in domain_groups.items():
        domain_table_names = [t.table_name for t in group]
        for t in group:
            t.field_name = generate_name(
                t.table_name, t.schema_name, t.source_id,
                domain_table_names, naming_rules,
                alias=t.alias,
            )
            if domain_prefix:
                alias = (domain_alias_map or {}).get(domain_id)
                if alias:
                    t.field_name = f"{alias}__{t.field_name}"
                    t.type_name = f"{alias.upper()}_{to_type_name(t.field_name.split('__', 1)[1])}"
                else:
                    domain_snake = domain_to_sql_name(domain_id)
                    t.field_name = f"{domain_snake}__{t.field_name}"
                    t.type_name = to_type_name(t.field_name)
            else:
                t.type_name = to_type_name(t.field_name)


def _build_column_fields(
    table: _TableInfo,
    convention: str = "snake_case",
    enum_types: dict | None = None,
) -> dict[str, GraphQLField]:
    """Build GraphQL fields for visible columns.

    Naming priority: explicit alias > convention-based alias > raw column name.
    Enum columns are mapped to GraphQLEnumType when enum_types is provided (REQ-221).
    """
    fields: dict[str, GraphQLField] = {}
    _enums = enum_types or {}
    for col in table.visible_columns:
        col_name = col["column_name"]
        meta = table.column_metadata.get(col_name.lower())
        if meta is None:
            raise ValueError(
                f"Registered column {col_name!r} on table {table.table_name!r} "
                f"not found in Trino metadata."
            )
        enum_gql = resolve_column_type(meta.data_type, _enums)
        if enum_gql is not None:
            gql_type = enum_gql if meta.is_nullable else GraphQLNonNull(enum_gql)
        else:
            gql_type = trino_to_graphql(meta.data_type)
            if not meta.is_nullable and not isinstance(gql_type, GraphQLList):
                gql_type = GraphQLNonNull(gql_type)
        # Naming priority: explicit alias > convention > raw name
        explicit_alias = col.get("alias")
        if explicit_alias:
            field_name = explicit_alias
        else:
            conv_alias = apply_convention(col_name, convention)
            field_name = conv_alias if conv_alias else col_name
        description = col.get("description")
        fields[field_name] = GraphQLField(gql_type, description=description)
    return fields


def _build_where_input(
    table: _TableInfo, type_name: str, enum_types: dict | None = None,
) -> GraphQLInputObjectType | None:
    """Build a typed WHERE input for a table's visible columns."""
    _enums = enum_types or {}
    _enum_filters = build_enum_filter_types(_enums)
    input_fields: dict[str, GraphQLInputField] = {}
    for col in table.visible_columns:
        col_name = col["column_name"]
        meta = table.column_metadata.get(col_name.lower())
        if meta is None:
            continue  # already validated in _build_column_fields
        # Normalize to unqualified pg_name for filter lookup (REQ-221)
        pg_name = meta.data_type.lower().strip()
        if "." in pg_name:
            pg_name = pg_name.rsplit(".", 1)[1]
        enum_filter = _enum_filters.get(pg_name)
        if enum_filter:
            input_fields[col_name] = GraphQLInputField(enum_filter)
        else:
            gql_type = trino_to_graphql(meta.data_type)
            scalar = gql_type.of_type if isinstance(gql_type, GraphQLList) else gql_type
            filter_type = FILTER_TYPE_MAP.get(scalar)
            if filter_type:
                input_fields[col_name] = GraphQLInputField(filter_type)

    if not input_fields:
        return None

    name = f"{type_name}Where"
    where_input: GraphQLInputObjectType | None = None

    def thunk():
        fields = dict(input_fields)
        fields["_and"] = GraphQLInputField(GraphQLList(GraphQLNonNull(where_input)))
        fields["_or"] = GraphQLInputField(GraphQLList(GraphQLNonNull(where_input)))
        return fields

    where_input = GraphQLInputObjectType(name, thunk)
    return where_input


def _build_order_by_inputs(
    tables: list[_TableInfo],
    visible_rels: list[dict],
    table_lookup: dict[int, _TableInfo],
) -> dict[int, GraphQLInputObjectType]:
    """Build ORDER BY input types using Hasura v2 pattern: {column: direction}.

    Each visible column becomes a field with OrderDirection type.
    Relationship fields become nested order_by input references via thunks.
    Returns table_id → OrderBy input type mapping.
    """
    order_by_types: dict[int, GraphQLInputObjectType] = {}

    for t in tables:
        visible_col_names = [
            c["column_name"] for c in t.visible_columns
            if c["column_name"].lower() in t.column_metadata
        ]
        if not visible_col_names:
            continue

        # Capture t in closure for the thunk
        def make_fields(table=t, cols=visible_col_names):
            fields: dict[str, GraphQLInputField] = {
                name: GraphQLInputField(OrderDirection) for name in cols
            }
            # Add nested relationship ordering
            for rel in visible_rels:
                if rel["source_table_id"] == table.table_id:
                    tgt_id = rel["target_table_id"]
                    if tgt_id in order_by_types:
                        target = table_lookup[tgt_id]
                        fields[target.field_name] = GraphQLInputField(
                            order_by_types[tgt_id],
                        )
            return fields

        ob_type = GraphQLInputObjectType(f"{t.type_name}OrderBy", make_fields)
        order_by_types[t.table_id] = ob_type

    return order_by_types


def _can_see_relationship(
    rel: dict, table_lookup: dict[int, _TableInfo]
) -> bool:
    """Check if both sides of a relationship are visible to the role,
    including the join columns themselves."""
    src_id = rel["source_table_id"]
    if src_id not in table_lookup:
        return False
    src_visible = {c["column_name"] for c in table_lookup[src_id].visible_columns}
    if not rel.get("source_column") or rel["source_column"] not in src_visible:
        return False
    if rel.get("target_function_name"):
        # Computed relationship — target is a DB function, no table check needed
        return True
    tgt_id = rel.get("target_table_id")
    if tgt_id not in table_lookup:
        return False
    tgt_visible = {c["column_name"] for c in table_lookup[tgt_id].visible_columns}
    return rel.get("target_column") in tgt_visible


_ACTION_SCALAR_MAP: dict[str, object] = {
    "String": GraphQLString,
    "Int": GraphQLInt,
    "Float": GraphQLFloat,
    "Boolean": GraphQLBoolean,
    "DateTime": GraphQLString,
    "Date": GraphQLString,
    "BigInt": GraphQLString,
    "JSON": GraphQLString,
}


def _mutation_name(op: str, field_name: str) -> str:
    """Place operation prefix after domain when domain_prefix is used.

    'insert' + 'customer_insights__customer_segments'
        → 'customer_insights__insert_customer_segments'
    'insert' + 'customer_segments'
        → 'insert_customer_segments'
    """
    if "__" in field_name:
        domain, table = field_name.split("__", 1)
        return f"{domain}__{op}_{table}"
    return f"{op}_{field_name}"


def _json_schema_to_gql_type(schema: dict, type_name: str):
    """Convert a JSON Schema object/array definition to a GraphQL return type."""
    if not schema:
        return None
    top = schema.get("type", "object")
    if top == "array":
        props = (schema.get("items") or {}).get("properties") or {}
    else:
        props = schema.get("properties") or {}
    if not props:
        return None
    _JS_MAP = {"string": GraphQLString, "integer": GraphQLInt, "number": GraphQLFloat, "boolean": GraphQLBoolean}
    gql_fields = {
        k: GraphQLField(_JS_MAP.get((v.get("type") if isinstance(v, dict) else "string") or "string", GraphQLString))
        for k, v in props.items()
    }
    obj = GraphQLObjectType(type_name, lambda f=gql_fields: f)
    if top == "array":
        return GraphQLList(GraphQLNonNull(obj))
    return obj


def _build_action_fields(
    si: SchemaInput,
    obj_types: dict[int, GraphQLObjectType],
    tables: list["_TableInfo"],
    domain_alias_map: dict[str, str] | None = None,
) -> tuple[dict[str, GraphQLField], dict[str, GraphQLField]]:
    """Build query/mutation fields for tracked functions and webhooks.

    Returns (extra_query_fields, extra_mutation_fields).
    """
    # Build a lookup: (schema_name, table_name) → GraphQL object type
    table_type_lookup: dict[tuple[str, str], GraphQLObjectType] = {
        (t.schema_name, t.table_name): obj_types[t.table_id]
        for t in tables
        if t.table_id in obj_types
    }

    extra_query: dict[str, GraphQLField] = {}
    extra_mutation: dict[str, GraphQLField] = {}

    role_id = si.role["id"]
    accessible = set(si.role["domain_access"])
    all_access = "*" in accessible

    def _gql_scalar(type_str: str):
        return _ACTION_SCALAR_MAP.get(type_str, GraphQLString)

    def _build_args(arguments: list[dict], response_fields: set[str] | None = None) -> dict[str, GraphQLArgument]:
        result = {}
        for a in arguments:
            if not a.get("name"):
                continue
            name = a["name"]
            if response_fields and name in response_fields:
                name = f"_{name}"
            result[name] = GraphQLArgument(_gql_scalar(a.get("type", "String")))
        return result

    def _build_callable_fields(
        items: list[dict],
        extra_query: dict[str, GraphQLField],
        extra_mutation: dict[str, GraphQLField],
        resolve_return_and_args,
    ) -> None:
        """Emit query/mutation fields for a list of callable items (functions or webhooks).

        Args:
            items: List of function or webhook dicts.
            extra_query: Accumulator for query fields (mutated in place).
            extra_mutation: Accumulator for mutation fields (mutated in place).
            resolve_return_and_args: Callable(item) → (gql_return, args) that computes
                the GraphQL return type and argument dict for one item.
        """
        for item in items:
            visible_to = item.get("visible_to") or []
            if visible_to and role_id not in visible_to:
                continue
            domain_id = item.get("domain_id", "")
            if not all_access and domain_id and domain_id not in accessible:
                continue

            gql_return, args = resolve_return_and_args(item)

            if item.get("kind", "mutation") == "query":
                args["limit"] = GraphQLArgument(GraphQLInt)
                args["offset"] = GraphQLArgument(GraphQLInt)
                args["where"] = GraphQLArgument(JSONScalar)
                args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(GraphQLString)))

            gql_field = GraphQLField(gql_return, args=args, description=item.get("description"))
            field_key = item["name"]
            if si.domain_prefix and domain_id:
                alias = (domain_alias_map or {}).get(domain_id) or domain_to_sql_name(domain_id)
                field_key = f"{alias}__{field_key}"

            if item.get("kind", "mutation") == "query":
                extra_query[field_key] = gql_field
            else:
                extra_mutation[field_key] = gql_field

    def _resolve_function(func: dict):
        returns_str = func.get("returns", "")
        # returns_str is "schema.table" — look up the GraphQL object type
        parts = returns_str.split(".", 1) if returns_str else []
        if len(parts) == 2:
            ret_type = table_type_lookup.get((parts[0], parts[1]))
            gql_return = GraphQLList(GraphQLNonNull(ret_type)) if ret_type else GraphQLString
        else:
            return_schema = func.get("return_schema")
            if return_schema:
                type_name = "".join(p.capitalize() for p in func["name"].split("_")) + "ReturnType"
                gql_return = _json_schema_to_gql_type(return_schema, type_name) or GraphQLString
            else:
                gql_return = GraphQLString
            ret_type = None
        # Detect collision between function arg names and return type field names
        ret_fields: set[str] = set(ret_type.fields.keys()) if ret_type and hasattr(ret_type, "fields") else set()
        args = _build_args(
            func["arguments"] if isinstance(func["arguments"], list) else [],
            response_fields=ret_fields,
        )
        return gql_return, args

    def _resolve_webhook(wh: dict):
        returns_str = wh.get("returns") or ""
        inline = wh.get("inline_return_type") or []
        if returns_str:
            # normalize "source.schema.table" → look up by (schema, table)
            rparts = returns_str.split(".")
            if len(rparts) >= 2:
                schema_part, table_part = rparts[-2], rparts[-1]
                ret_type = table_type_lookup.get((schema_part, table_part))
            else:
                ret_type = None
            gql_return = GraphQLList(GraphQLNonNull(ret_type)) if ret_type else GraphQLString
        elif inline:
            wh_type_name = "".join(p.capitalize() for p in wh["name"].split("_")) + "ReturnType"
            inline_fields = {
                f["name"]: GraphQLField(_gql_scalar(f.get("type", "String")))
                for f in inline
                if f.get("name")
            }
            wh_obj = GraphQLObjectType(wh_type_name, lambda f=inline_fields: f)
            gql_return = GraphQLList(GraphQLNonNull(wh_obj))
            ret_type = None
        else:
            ret_type = None
            gql_return = GraphQLString
        wh_ret_fields: set[str] = set(ret_type.fields.keys()) if ret_type and hasattr(ret_type, "fields") else set()
        args = _build_args(
            wh["arguments"] if isinstance(wh["arguments"], list) else [],
            response_fields=wh_ret_fields,
        )
        return gql_return, args

    _build_callable_fields(si.functions, extra_query, extra_mutation, _resolve_function)
    _build_callable_fields(si.webhooks, extra_query, extra_mutation, _resolve_webhook)

    return extra_query, extra_mutation


_CDC_SOURCES: frozenset[str] = frozenset({"postgresql", "mongodb", "kafka", "debezium"})


def _build_subscription_fields(
    si: SchemaInput,
    tables: list[_TableInfo],
    gql_types: dict[int, GraphQLObjectType],
) -> dict[str, GraphQLField]:
    """Build Subscription root fields.

    Includes pre-approved tables that have a watermark column set (polling-based)
    or whose source supports native CDC (postgresql, mongodb, kafka, debezium).
    Tables in `tables` are already filtered by role visibility.

    Also includes approved persisted queries the role has been granted access to
    via the visible_to field (REQ-022–REQ-026).
    """
    raw_by_id = {t["id"]: t for t in si.tables}
    fields: dict[str, GraphQLField] = {}
    role_id = si.role["id"]

    for t in tables:
        raw = raw_by_id.get(t.table_id, {})
        if raw.get("governance") != "pre-approved":
            continue
        source_type = (si.source_types or {}).get(t.source_id, "")
        if source_type not in _CDC_SOURCES and not raw.get("watermark_column"):
            continue
        fields[t.field_name] = GraphQLField(
            GraphQLList(GraphQLNonNull(gql_types[t.table_id])),
            description=t.description,
        )

    # Approved persisted queries the role can subscribe to
    for q in si.approved_queries:
        visible_to = q.get("visible_to") or []
        if visible_to and "*" not in visible_to and role_id not in visible_to:
            continue
        stable_id = q.get("stable_id") or ""
        if not stable_id:
            continue
        # stable_id is a UUID — sanitize to a valid GraphQL field name
        field_name = "q_" + stable_id.replace("-", "_")
        desc = q.get("business_purpose") or q.get("query_text", "")[:80]
        fields[field_name] = GraphQLField(
            GraphQLList(GraphQLNonNull(JSONScalar)),
            description=desc,
        )

    return fields


def generate_schema(si: SchemaInput) -> GraphQLSchema:
    """Generate a graphql-core schema for a specific role.

    The schema includes:
    - Object types per registered table (filtered by domain access + column visibility)
    - Relationship fields (many-to-one → object, one-to-many → list)
    - Root query fields with where, order_by, limit, offset args
    """
    tables = _build_visible_tables(si)
    if not tables:
        raise ValueError(
            f"No tables visible to role {si.role['id']!r}. "
            f"Check domain_access and column visibility."
        )

    domain_alias_map = {
        d["id"]: domain_gql_alias(d["id"], d.get("graphql_alias"))
        for d in si.domains
        if domain_gql_alias(d["id"], d.get("graphql_alias"))
    }
    _assign_names(tables, si.naming_rules, domain_prefix=si.domain_prefix, domain_alias_map=domain_alias_map)

    # Build base column fields (enum_types wires PG enum → GraphQLEnumType, REQ-221)
    for t in tables:
        t.gql_fields = _build_column_fields(t, convention=t.naming_convention, enum_types=si.enum_types)

    table_lookup: dict[int, _TableInfo] = {t.table_id: t for t in tables}

    # Filter relationships to those where both sides are visible
    visible_rels = [
        r for r in si.relationships
        if _can_see_relationship(r, table_lookup)
    ]

    # Create GraphQL object types with thunks (handles circular relationships)
    gql_types: dict[int, GraphQLObjectType] = {}

    for t in tables:
        tid = t.table_id

        def make_fields(tid=tid):
            info = table_lookup[tid]
            fields = dict(info.gql_fields)

            # Add relationship fields
            for rel in visible_rels:
                if rel["source_table_id"] != tid:
                    continue
                fn_name = rel.get("target_function_name")
                if fn_name:
                    # Computed relationship: target is a DB function
                    func = next((f for f in si.functions if f["name"] == fn_name), None)
                    if func:
                        returns_str = func.get("returns", "")
                        parts = returns_str.split(".", 1) if returns_str else []
                        ret_type = None
                        if len(parts) == 2:
                            ret_type = next(
                                (gql_types[t.table_id] for t in tables
                                 if t.schema_name == parts[0] and t.table_name == parts[1]),
                                None,
                            )
                        if ret_type:
                            field_key = re.sub(r"[^a-zA-Z0-9_]", "_", rel["id"])
                            fields[field_key] = GraphQLField(
                                GraphQLList(GraphQLNonNull(ret_type))
                            )
                else:
                    target = table_lookup.get(rel["target_table_id"])
                    if target:
                        target_type = gql_types[target.table_id]
                        field_name = rel.get("graphql_alias") or target.field_name
                        if rel["cardinality"] == "many-to-one":
                            fields[field_name] = GraphQLField(target_type)
                        elif rel["cardinality"] == "one-to-many":
                            fields[field_name] = GraphQLField(
                                GraphQLList(GraphQLNonNull(target_type))
                            )

            return fields

        gql_types[tid] = GraphQLObjectType(
            t.type_name, make_fields, description=t.description,
        )

    # Pre-build all ORDER BY input types (shared across root fields)
    order_by_types = _build_order_by_inputs(tables, visible_rels, table_lookup)

    # Build root query fields
    query_fields: dict[str, GraphQLField] = {}

    for t in tables:
        gql_type = gql_types[t.table_id]
        args: dict[str, GraphQLArgument] = {
            "limit": GraphQLArgument(GraphQLInt),
            "offset": GraphQLArgument(GraphQLInt),
        }

        where_input = _build_where_input(t, t.type_name, enum_types=si.enum_types)
        if where_input:
            args["where"] = GraphQLArgument(where_input)

        order_by_input = order_by_types.get(t.table_id)
        if order_by_input:
            args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(order_by_input)))

        # distinct_on: deduplicate by specified columns
        visible_col_names = [
            c["column_name"] for c in t.visible_columns
            if c["column_name"].lower() in t.column_metadata
        ]
        if visible_col_names:
            distinct_enum = GraphQLEnumType(
                f"{t.type_name}DistinctOnColumn",
                {name: GraphQLEnumValue(name) for name in visible_col_names},
            )
            args["distinct_on"] = GraphQLArgument(
                GraphQLList(GraphQLNonNull(distinct_enum))
            )

        # Native filter args: path params (required) and query params (optional).
        # Follow Hasura DDN convention: direct top-level args, no prefix unless there is
        # a name collision with a response body field, in which case prefix with "_".
        _response_field_names = set(visible_col_names) | {"where", "order_by", "limit", "offset", "distinct_on"}
        for nfc in t.native_filter_columns:
            col_name = nfc["column_name"]
            bare_name = col_name[4:] if col_name.startswith("_nf_") else col_name
            arg_name = f"_{bare_name}" if bare_name in _response_field_names else bare_name
            meta = t.column_metadata.get(col_name.lower())
            nfc_gql_type = trino_to_graphql(meta.data_type) if meta else GraphQLString
            scalar = nfc_gql_type.of_type if isinstance(nfc_gql_type, GraphQLList) else nfc_gql_type
            required = nfc.get("native_filter_type") == "path_param"
            args[arg_name] = GraphQLArgument(
                GraphQLNonNull(scalar) if required else scalar,
                description=f"Native API filter ({nfc.get('native_filter_type', 'query_param')})",
            )

        query_fields[t.field_name] = GraphQLField(
            GraphQLList(GraphQLNonNull(gql_type)),
            args=args,
        )

        # Aggregate field: <table>_aggregate
        agg_type = build_aggregate_types(
            t.type_name, t.visible_columns, t.column_metadata, gql_type,
        )
        if agg_type:
            agg_args: dict[str, GraphQLArgument] = {}
            agg_where = _build_where_input(t, f"{t.type_name}Agg", enum_types=si.enum_types)
            if agg_where:
                agg_args["where"] = GraphQLArgument(agg_where)
            query_fields[f"{t.field_name}_aggregate"] = GraphQLField(
                agg_type,
                args=agg_args,
            )

        # Connection field: <table>_connection (cursor pagination, REQ-218) — opt-in only
        if t.relay_pagination:
            _edge_type, conn_type = _build_connection_types(t.type_name, gql_type)
            conn_args: dict[str, GraphQLArgument] = {
                "first": GraphQLArgument(GraphQLInt),
                "after": GraphQLArgument(GraphQLString),
                "last": GraphQLArgument(GraphQLInt),
                "before": GraphQLArgument(GraphQLString),
            }
            conn_where = _build_where_input(t, f"{t.type_name}Conn", enum_types=si.enum_types)
            if conn_where:
                conn_args["where"] = GraphQLArgument(conn_where)
            conn_ob = order_by_types.get(t.table_id)
            if conn_ob:
                conn_args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(conn_ob)))
            query_fields[f"{t.field_name}_connection"] = GraphQLField(
                conn_type, args=conn_args,
            )

    query_type = GraphQLObjectType("Query", lambda: query_fields)

    # Build mutation types for RDBMS tables (REQ-031–REQ-037)
    nosql_types = {"mongodb", "cassandra"}
    mutation_fields: dict[str, GraphQLField] = {}

    for t in tables:
        # Skip NoSQL sources — no mutations
        if si.source_types and si.source_types.get(t.source_id, "") in nosql_types:
            continue

        # Build input type for insert (all visible columns)
        insert_fields: dict[str, GraphQLInputField] = {}
        for col in t.visible_columns:
            col_name = col["column_name"]
            meta = t.column_metadata.get(col_name.lower())
            if meta is None:
                continue
            gql_type = trino_to_graphql(meta.data_type)
            if isinstance(gql_type, GraphQLList):
                gql_type = GraphQLString  # fallback for arrays in input
            insert_fields[col_name] = GraphQLInputField(gql_type)

        if not insert_fields:
            continue

        insert_input = GraphQLInputObjectType(
            f"{t.type_name}InsertInput", lambda fields=insert_fields: fields,
        )

        # Build set input type for update (same columns)
        set_input = GraphQLInputObjectType(
            f"{t.type_name}SetInput", lambda fields=insert_fields: fields,
        )

        # Where input for update/delete (use mutation-specific name to avoid conflict)
        where_input = _build_where_input(t, f"{t.type_name}Mutation", enum_types=si.enum_types)

        # Mutation response type
        response_type = GraphQLObjectType(
            f"{t.type_name}MutationResponse",
            lambda t=t: {
                "affected_rows": GraphQLField(GraphQLNonNull(GraphQLInt)),
            },
        )

        # On-conflict column list for upsert
        conflict_col_enum = GraphQLEnumType(
            f"{t.type_name}ConflictColumn",
            {name: GraphQLEnumValue(name) for name in insert_fields},
        )

        # insert_<table>(input: InsertInput!): MutationResponse!
        mutation_fields[_mutation_name("insert", t.field_name)] = GraphQLField(
            GraphQLNonNull(response_type),
            args={"input": GraphQLArgument(GraphQLNonNull(insert_input))},
        )

        # upsert_<table>(input: InsertInput!, on_conflict: [ConflictColumn!]!): MutationResponse!
        mutation_fields[_mutation_name("upsert", t.field_name)] = GraphQLField(
            GraphQLNonNull(response_type),
            args={
                "input": GraphQLArgument(GraphQLNonNull(insert_input)),
                "on_conflict": GraphQLArgument(
                    GraphQLNonNull(GraphQLList(GraphQLNonNull(conflict_col_enum)))
                ),
            },
        )

        # update_<table>(set: SetInput!, where: WhereInput!): MutationResponse!
        if where_input:
            mutation_fields[_mutation_name("update", t.field_name)] = GraphQLField(
                GraphQLNonNull(response_type),
                args={
                    "set": GraphQLArgument(GraphQLNonNull(set_input)),
                    "where": GraphQLArgument(GraphQLNonNull(where_input)),
                },
            )

            # delete_<table>(where: WhereInput!): MutationResponse!
            mutation_fields[_mutation_name("delete", t.field_name)] = GraphQLField(
                GraphQLNonNull(response_type),
                args={"where": GraphQLArgument(GraphQLNonNull(where_input))},
            )

    extra_query, extra_mutation = _build_action_fields(si, gql_types, tables, domain_alias_map)
    query_fields.update(extra_query)
    mutation_fields.update(extra_mutation)

    mutation_type = None
    if mutation_fields:
        mutation_type = GraphQLObjectType("Mutation", lambda: mutation_fields)

    subscription_fields = _build_subscription_fields(si, tables, gql_types)
    subscription_type = GraphQLObjectType("Subscription", lambda: subscription_fields) if subscription_fields else None

    return GraphQLSchema(
        query=query_type,
        mutation=mutation_type,
        subscription=subscription_type,
        directives=[*specified_directives, *PROVISA_DIRECTIVES],
        types=[RouteEngineEnum, JoinStrategyEnum],
    )
