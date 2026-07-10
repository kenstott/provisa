# Copyright (c) 2026 Kenneth Stott
# Canary: fe7dee37-1a51-4599-a719-d5e9249736c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build graphql-core schema from registration model + the engine metadata per role.

No third-party GraphQL framework (REQ-007). Uses graphql-core directly.
Domain-scoped, per-role column filtering (REQ-008, REQ-021).
"""

# Requirements: REQ-007, REQ-008, REQ-010, REQ-021, REQ-032, REQ-033, REQ-034, REQ-036, REQ-037, REQ-039, REQ-133, REQ-134, REQ-154, REQ-155, REQ-156, REQ-157, REQ-194, REQ-196, REQ-197, REQ-200, REQ-201, REQ-202, REQ-205, REQ-206, REQ-207, REQ-209, REQ-210, REQ-212, REQ-213, REQ-218, REQ-219, REQ-221, REQ-253, REQ-259, REQ-260, REQ-363

import re
from typing import cast

from graphql import (
    GraphQLArgument,
    GraphQLBoolean as _GraphQLBoolean,
    GraphQLDirective,
    GraphQLEnumType,
    GraphQLEnumValue,
    GraphQLField,
    GraphQLFloat as _GraphQLFloat,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLInt as _GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLOutputType,
    GraphQLScalarType,
    GraphQLSchema,
    GraphQLString as _GraphQLString,
    specified_directives,
)
from graphql.language import DirectiveLocation

from provisa.compiler.aggregate_gen import (
    build_agg_fields_type,
    build_aggregate_types,
    build_having_exp_type,
)
from provisa.compiler.enum_detect import build_enum_filter_types, resolve_column_type
from provisa.compiler.naming import (
    active_gql_convention,
    apply_gql_name,
    domain_gql_alias,
    domain_to_sql_name,
    generate_name,
    rel_field_name,
    to_type_name,
)
from provisa.compiler.type_map import FILTER_TYPE_MAP, JSONScalar, column_type_to_graphql
from provisa.compiler.schema_types import SchemaInput, _TableInfo
from provisa.compiler.actions_schema import _build_action_fields, _mutation_name

# graphql-core 3.2.x: __new__ returns GraphQLNamedType instead of Self;
# re-bind scalars with explicit GraphQLScalarType annotation so Pyright narrows correctly.
GraphQLString: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLString)
GraphQLInt: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLInt)
GraphQLBoolean: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLBoolean)
GraphQLFloat: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLFloat)


# --- GraphQL enum for ORDER BY direction ---

OrderDirection = cast(
    GraphQLEnumType,
    GraphQLEnumType(
        "order_by",
        {
            "asc": GraphQLEnumValue("asc"),
            "desc": GraphQLEnumValue("desc"),
            "asc_nulls_first": GraphQLEnumValue("asc_nulls_first"),
            "asc_nulls_last": GraphQLEnumValue("asc_nulls_last"),
            "desc_nulls_first": GraphQLEnumValue("desc_nulls_first"),
            "desc_nulls_last": GraphQLEnumValue("desc_nulls_last"),
        },
    ),
)

# --- Provisa directive enums ---

RouteEngineEnum = cast(
    GraphQLEnumType,
    GraphQLEnumType(
        "RouteEngine",
        {
            "FEDERATED": GraphQLEnumValue("FEDERATED", description="Route vithe engine federation"),
            "DIRECT": GraphQLEnumValue("DIRECT", description="Route directly to source"),
        },
        description="Execution engine routing hint for @route.",
    ),
)

JoinStrategyEnum = cast(
    GraphQLEnumType,
    GraphQLEnumType(
        "JoinStrategy",
        {
            "BROADCAST": GraphQLEnumValue("BROADCAST", description="Broadcast join distribution"),
            "PARTITIONED": GraphQLEnumValue(
                "PARTITIONED", description="Partitioned (hash) join distribution"
            ),
        },
        description="Join distribution strategy hint for @join.",
    ),
)

# Directive locations
_QS = [DirectiveLocation.QUERY, DirectiveLocation.SUBSCRIPTION]
_QMS = [DirectiveLocation.QUERY, DirectiveLocation.MUTATION, DirectiveLocation.SUBSCRIPTION]

PROVISA_DIRECTIVES = [
    GraphQLDirective(
        name="route",
        locations=_QMS,
        args={
            "engine": GraphQLArgument(
                GraphQLNonNull(RouteEngineEnum), description="FEDERATED or DIRECT"
            )
        },
        description="Override execution engine routing.",
    ),
    GraphQLDirective(
        name="join",
        locations=_QS,
        args={
            "strategy": GraphQLArgument(
                GraphQLNonNull(JoinStrategyEnum), description="BROADCAST or PARTITIONED"
            )
        },
        description="Set the engine join distribution strategy.",
    ),
    GraphQLDirective(
        name="reorder",
        locations=_QS,
        args={
            "enabled": GraphQLArgument(
                GraphQLNonNull(GraphQLBoolean), description="Set false to disable join reordering"
            )
        },
        description="Control the engine join reordering.",
    ),
    GraphQLDirective(
        name="broadcastSize",
        locations=_QS,
        args={
            "size": GraphQLArgument(
                GraphQLNonNull(GraphQLString), description="Max broadcast table size, e.g. '100MB'"
            )
        },
        description="Override max broadcast table size for the engine.",
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
            "broker": GraphQLArgument(
                GraphQLString, description="Kafka bootstrap server (host:port)"
            ),
        },
        description="Redirect subscription output to a Kafka topic.",
    ),
    GraphQLDirective(
        name="redirect",
        locations=_QS,
        args={
            "format": GraphQLArgument(
                GraphQLString, description="Output format: parquet, csv, arrow"
            ),
            "threshold": GraphQLArgument(
                GraphQLInt, description="Row count threshold to trigger redirect"
            ),
        },
        description="Redirect large results to object store.",
    ),
    GraphQLDirective(
        name="cached",
        locations=_QMS,
        args={
            "ttl": GraphQLArgument(
                GraphQLInt, description="Cache TTL in seconds; 0 = disable caching"
            )
        },
        description="Override response cache TTL for this query.",
    ),
    GraphQLDirective(
        name="noCache",
        locations=_QMS,
        args={},
        description="Bypass the response cache entirely — skip both read and write.",
    ),
]

# --- Relay-style connection types for cursor pagination (REQ-218) ---

PageInfoType = cast(
    GraphQLObjectType,
    GraphQLObjectType(
        "PageInfo",
        lambda: {
            "hasNextPage": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
            "hasPreviousPage": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
            "startCursor": GraphQLField(GraphQLString),
            "endCursor": GraphQLField(GraphQLString),
        },
    ),
)


def _build_connection_types(
    type_name: str,
    node_type: GraphQLObjectType,
) -> tuple[GraphQLObjectType, GraphQLObjectType]:
    """Build Edge and Connection types for cursor pagination."""
    edge_type = cast(
        GraphQLObjectType,
        GraphQLObjectType(
            f"{type_name}Edge",
            lambda node_type=node_type: {
                "cursor": GraphQLField(GraphQLNonNull(GraphQLString)),
                "node": GraphQLField(GraphQLNonNull(node_type)),
            },
        ),
    )
    connection_type = cast(
        GraphQLObjectType,
        GraphQLObjectType(
            f"{type_name}Connection",
            lambda edge_type=edge_type: {
                "edges": GraphQLField(GraphQLNonNull(GraphQLList(GraphQLNonNull(edge_type)))),
                "pageInfo": GraphQLField(GraphQLNonNull(PageInfoType)),
            },
        ),
    )
    return edge_type, connection_type


# Domains implicitly reachable via JOIN from any data domain (traversal only).
# Tables in these domains are included in all role contexts so SQL JOINs
# can reference them, but V001 still blocks direct FROM-clause access.
_IMPLICIT_TRAVERSAL_DOMAINS: frozenset[str] = frozenset({"meta", "ops"})


def _build_visible_tables(si: SchemaInput) -> list[_TableInfo]:  # REQ-008, REQ-039, REQ-363
    """Filter tables by role's domain access. Build per-table metadata."""
    role = si.role
    accessible = set(role["domain_access"])
    # Consistent with visible_to=[]: empty list means no restriction (all domains accessible).
    all_access = not accessible or "*" in accessible

    result: list[_TableInfo] = []
    for table in si.tables:
        if (
            not all_access
            and table["domain_id"] not in accessible
            and table["domain_id"] not in _IMPLICIT_TRAVERSAL_DOMAINS
        ):
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
        # visible_to=[] means unrestricted (visible to all roles)
        visible_cols = [
            c
            for c in table["columns"]
            if (not c["visible_to"] or role["id"] in c["visible_to"])
            and not c.get("native_filter_type")
        ]
        # Native filter cols are API parameters (path/query params), not data columns.
        # They are always exposed as query args regardless of visible_to — the role
        # only needs access to the table itself.
        _raw_nfc = [c for c in table["columns"] if c.get("native_filter_type")]
        _base_names = {
            c["column_name"] for c in _raw_nfc if not c["column_name"].startswith("_nf_")
        }
        native_filter_cols = [
            c
            for c in _raw_nfc
            if not c["column_name"].startswith("_nf_") or c["column_name"][4:] not in _base_names
        ]

        if not visible_cols and not native_filter_cols:
            if table.get("columns") or not col_meta:
                # Columns were defined but none visible to this role, or no metadata available
                continue
            # No registered columns but synthesized metadata exists (e.g., govdata JAR YAML)
            visible_cols = [
                {"column_name": name, "visible_to": [], "native_filter_type": None}
                for name in col_meta
            ]

        table_conv = table.get("gql_naming_convention")
        source_conv = table.get("source_gql_naming_convention")
        resolved_conv = table_conv or source_conv or None

        # Resolve relay_pagination: table → source → global (None = inherit)
        table_relay = table.get("relay_pagination")
        source_relay = table.get("source_relay_pagination")
        if table_relay is not None:
            resolved_relay = bool(table_relay)
        elif source_relay is not None:
            resolved_relay = bool(source_relay)
        else:
            resolved_relay = si.relay_pagination

        result.append(
            _TableInfo(
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
                gql_convention_override=resolved_conv,
                relay_pagination=resolved_relay,
                enable_aggregates=bool(table.get("enable_aggregates", False)),
                enable_group_by=bool(table.get("enable_group_by", False)),
            )
        )

    return result


def _assign_names(  # REQ-154, REQ-155, REQ-194, REQ-195, REQ-411, REQ-412, REQ-416
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
        domain_snake = domain_to_sql_name(domain_id)
        dp = f"{domain_snake}__"

        def _strip(name: str) -> str:
            return name[len(dp) :] if (domain_prefix and name.lower().startswith(dp)) else name

        domain_table_names = [_strip(t.table_name) if not t.alias else t.table_name for t in group]
        for t in group:
            base_table_name = _strip(t.table_name) if not t.alias else t.table_name
            t.field_name = generate_name(
                base_table_name,
                t.schema_name,
                t.source_id,
                domain_table_names,
                naming_rules,
                alias=t.alias,
                convention=t.gql_convention_override or active_gql_convention(),
            )
            if domain_prefix:
                alias = (domain_alias_map or {}).get(domain_id)
                if alias:
                    t.field_name = f"{alias}__{t.field_name}"
                    t.type_name = f"{alias.upper()}__{to_type_name(t.field_name.split('__', 1)[1])}"
                elif domain_snake:
                    t.field_name = f"{domain_snake}__{t.field_name}"
                    t.type_name = to_type_name(t.field_name)
                else:
                    t.type_name = to_type_name(t.field_name)
            else:
                t.type_name = to_type_name(t.field_name)


_OBJECT_FIELD_TYPE_MAP: dict[str, GraphQLScalarType] = {
    "string": GraphQLString,
    "integer": GraphQLInt,
    "number": GraphQLFloat,
    "boolean": GraphQLBoolean,
}


def _build_object_type(
    col_name: str,
    object_fields: list[dict],
    override: str | None,
    registry: dict[str, GraphQLObjectType],
    type_prefix: str = "",
) -> GraphQLObjectType:
    """Build a GraphQLObjectType for an object column, reusing from registry by name."""
    type_name = type_prefix + to_type_name(col_name) + "Object"
    if type_name in registry:
        return registry[type_name]
    sub_fields: dict[str, GraphQLField] = {}
    for sf in object_fields:
        sf_name = sf["name"]
        sf_alias = sf.get("alias") or apply_gql_name(sf_name, override)
        nested = sf.get("fields") or []
        if nested and sf.get("type") == "object":
            sf_gql: GraphQLOutputType = _build_object_type(sf_name, nested, override, registry)
        else:
            sf_gql = _OBJECT_FIELD_TYPE_MAP.get(sf.get("type", "string"), GraphQLString)
        sub_fields[sf_alias] = GraphQLField(sf_gql, description=sf.get("description"))
    obj_type = cast(GraphQLObjectType, GraphQLObjectType(type_name, lambda: sub_fields))
    registry[type_name] = obj_type
    return obj_type


def _build_column_fields(  # REQ-008, REQ-010, REQ-039, REQ-155, REQ-156, REQ-221
    table: _TableInfo,
    override: str | None = None,
    enum_types: dict | None = None,
    object_type_registry: dict[str, GraphQLObjectType] | None = None,
    governed_gql_types: set[str] | None = None,
) -> dict[str, GraphQLField]:
    """Build GraphQL fields for visible columns.

    Naming priority: explicit alias > convention-based alias > raw column name.
    Enum columns are mapped to GraphQLEnumType when enum_types is provided (REQ-221).
    """
    fields: dict[str, GraphQLField] = {}
    _enums = enum_types or {}
    _obj_registry: dict[str, GraphQLObjectType] = (
        object_type_registry if object_type_registry is not None else {}
    )
    _governed = governed_gql_types or set()
    for col in table.visible_columns:
        col_name = col["column_name"]
        if _governed and col.get("gql_object_type") in _governed:
            continue
        meta = table.column_metadata.get(col_name.lower())
        if meta is None:
            import logging as _clog

            _clog.getLogger(__name__).warning(
                "Registered column %r on table %r not found in the engine metadata — skipping.",
                col_name,
                table.table_name,
            )
            continue
        object_fields = col.get("object_fields")
        if isinstance(object_fields, str):
            import json as _json

            object_fields = _json.loads(object_fields)
        if object_fields and meta.data_type in ("json", "jsonb"):
            _type_prefix = ""
            if "__" in table.type_name:
                _type_prefix = table.type_name.split("__", 1)[0] + "__"
            gql_type: GraphQLOutputType = _build_object_type(
                col_name, object_fields, override, _obj_registry, _type_prefix
            )
        else:
            enum_gql = resolve_column_type(meta.data_type, _enums)
            if enum_gql is not None:
                gql_type = enum_gql if meta.is_nullable else GraphQLNonNull(enum_gql)
            else:
                scalar_type = column_type_to_graphql(meta.data_type)
                if not meta.is_nullable and not isinstance(scalar_type, GraphQLList):
                    gql_type = GraphQLNonNull(scalar_type)
                else:
                    gql_type = scalar_type
        # Naming priority: explicit alias > convention > raw name
        explicit_alias = col.get("alias")
        if explicit_alias:
            field_name = explicit_alias
        else:
            conv_alias = apply_gql_name(col_name, override)
            field_name = conv_alias if conv_alias else col_name
        description = col.get("description")
        fields[field_name] = GraphQLField(gql_type, description=description)
    return fields


def _gql_col_name(col: dict, table: _TableInfo) -> str:
    """Return the GQL-exposed name for a column (alias > convention > raw physical)."""
    if alias := col.get("alias"):
        return alias
    raw = col["column_name"]
    conv = apply_gql_name(raw, table.gql_convention_override)
    return conv if conv else raw


def _build_where_input(  # REQ-008, REQ-221
    table: _TableInfo,
    type_name: str,
    enum_types: dict | None = None,
) -> GraphQLInputObjectType | None:
    """Build a typed WHERE input for a table's visible columns."""
    _enums = enum_types or {}
    _enum_filters = build_enum_filter_types(_enums)
    input_fields: dict[str, GraphQLInputField] = {}
    for col in table.visible_columns:
        col_name = col["column_name"]
        gql_name = _gql_col_name(col, table)
        meta = table.column_metadata.get(col_name.lower())
        if meta is None:
            continue  # already validated in _build_column_fields
        # Normalize to unqualified pg_name for filter lookup (REQ-221)
        pg_name = meta.data_type.lower().strip()
        if "." in pg_name:
            pg_name = pg_name.rsplit(".", 1)[1]
        enum_filter = _enum_filters.get(pg_name)
        if enum_filter:
            input_fields[gql_name] = GraphQLInputField(enum_filter)
        else:
            gql_type = column_type_to_graphql(meta.data_type)
            scalar = gql_type.of_type if isinstance(gql_type, GraphQLList) else gql_type
            filter_type = FILTER_TYPE_MAP.get(scalar)
            if filter_type:
                input_fields[gql_name] = GraphQLInputField(filter_type)

    # Virtual columns support eq/neq filtering
    _str_filter = FILTER_TYPE_MAP.get(GraphQLString)
    if _str_filter:
        input_fields["_name_"] = GraphQLInputField(_str_filter)
        input_fields["_domain_"] = GraphQLInputField(_str_filter)

    if not input_fields:
        return None

    name = f"{type_name}Where"
    where_input_holder: list[GraphQLInputObjectType] = []

    def thunk():
        wi = where_input_holder[0]
        fields = dict(input_fields)
        fields["_and"] = GraphQLInputField(GraphQLList(GraphQLNonNull(wi)))
        fields["_or"] = GraphQLInputField(GraphQLList(GraphQLNonNull(wi)))
        return fields

    where_input = cast(GraphQLInputObjectType, GraphQLInputObjectType(name, thunk))
    where_input_holder.append(where_input)
    return where_input


def _build_order_by_inputs(  # REQ-200, REQ-201, REQ-202
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
            _gql_col_name(c, t)
            for c in t.visible_columns
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

        ob_type = cast(
            GraphQLInputObjectType, GraphQLInputObjectType(f"{t.type_name}OrderBy", make_fields)
        )
        order_by_types[t.table_id] = ob_type

    return order_by_types


def _build_distinct_on_enum(
    table: _TableInfo,
) -> GraphQLEnumType | None:
    visible_col_names = [
        _gql_col_name(c, table)
        for c in table.visible_columns
        if c["column_name"].lower() in table.column_metadata
    ]
    if not visible_col_names:
        return None
    return cast(
        GraphQLEnumType,
        GraphQLEnumType(
            f"{table.type_name}DistinctOnColumn",
            {name: GraphQLEnumValue(name) for name in visible_col_names},
        ),
    )


def _build_db_field_args(
    where_input: GraphQLInputObjectType | None,
    order_by_input: GraphQLInputObjectType | None,
    distinct_enum: GraphQLEnumType | None,
) -> dict[str, GraphQLArgument]:
    args: dict[str, GraphQLArgument] = {
        "limit": GraphQLArgument(GraphQLInt),
        "offset": GraphQLArgument(GraphQLInt),
        # REQ-263a: statistical row sampling — TABLESAMPLE BERNOULLI(<pct>), 0-100.
        "sample": GraphQLArgument(GraphQLFloat),
    }
    if where_input:
        args["where"] = GraphQLArgument(where_input)
    if order_by_input:
        args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(order_by_input)))
    if distinct_enum:
        args["distinct_on"] = GraphQLArgument(GraphQLList(GraphQLNonNull(distinct_enum)))
    return args


def _can_see_relationship(rel: dict, table_lookup: dict[int, _TableInfo]) -> bool:
    """Check if both sides of a relationship are visible to the role,
    including the join columns themselves."""
    src_id = rel["source_table_id"]
    if src_id not in table_lookup:
        return False
    src_visible = {c["column_name"] for c in table_lookup[src_id].visible_columns}
    if rel.get("target_function_name"):
        # Computed relationship — target is a DB function, no table check needed
        if not rel.get("source_column") or rel["source_column"] not in src_visible:
            return False
        return True
    tgt_id = rel.get("target_table_id")
    if tgt_id not in table_lookup:
        return False
    # Remote-managed relationships (e.g. GraphQL remote) have no FK columns — allow when
    # both tables are visible.
    if not rel.get("source_column"):
        return True
    if rel["source_column"] not in src_visible:
        return False
    tgt_visible = {c["column_name"] for c in table_lookup[tgt_id].visible_columns}
    return rel.get("target_column") in tgt_visible


_CDC_SOURCES: frozenset[str] = frozenset({"postgresql", "mongodb", "kafka", "debezium"})


def _build_subscription_fields(  # REQ-219, REQ-258, REQ-260
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

    for t in tables:
        raw = raw_by_id.get(t.table_id, {})
        source_type = (si.source_types or {}).get(t.source_id, "")
        if source_type not in _CDC_SOURCES and not raw.get("watermark_column"):
            continue
        fields[t.field_name] = GraphQLField(
            GraphQLList(GraphQLNonNull(gql_types[t.table_id])),
            description=t.description,
        )

    return fields


def _dedup_tables(tables: list[_TableInfo]) -> list[_TableInfo]:
    """Remove tables sharing a type_name, keeping the first occurrence (lowest id)."""
    seen: set[str] = set()
    result: list[_TableInfo] = []
    for t in tables:
        if t.type_name not in seen:
            seen.add(t.type_name)
            result.append(t)
    return result


def _build_domain_alias_map(domains: list[dict]) -> dict[str, str]:
    return {
        d["id"]: domain_gql_alias(d["id"], d.get("graphql_alias"))
        for d in domains
        if domain_gql_alias(d["id"], d.get("graphql_alias"))
    }


def _add_virtual_system_fields(fields: dict[str, GraphQLField]) -> None:
    fields["_name_"] = GraphQLField(GraphQLNonNull(GraphQLString), description="Logical table name")
    fields["_domain_"] = GraphQLField(
        GraphQLNonNull(GraphQLString), description="Domain identifier"
    )


def _add_meta_field(
    fields: dict[str, GraphQLField],
    info: _TableInfo,
    meta_rt: _TableInfo,
    gql_types: dict[int, GraphQLObjectType],
) -> None:
    if info.domain_id != "meta":
        fields["_meta"] = GraphQLField(
            gql_types[meta_rt.table_id],
            description="Registration metadata for this table",
        )


def _add_ops_traversal_fields(
    fields: dict[str, GraphQLField],
    ops_targets: list[_TableInfo],
    gql_types: dict[int, GraphQLObjectType],
    where_types: dict[int, GraphQLInputObjectType | None],
    order_by_types: dict[int, GraphQLInputObjectType],
    distinct_enums: dict[int, GraphQLEnumType | None],
) -> None:
    for ops_t in ops_targets:
        if ops_t.table_id not in gql_types:
            continue
        ops_base = (
            ops_t.field_name.split("__", 1)[1] if "__" in ops_t.field_name else ops_t.field_name
        )
        fields[f"_{ops_base}"] = GraphQLField(
            GraphQLList(GraphQLNonNull(gql_types[ops_t.table_id])),
            args=_build_db_field_args(
                where_types.get(ops_t.table_id),
                order_by_types.get(ops_t.table_id),
                distinct_enums.get(ops_t.table_id),
            ),
            description=f"Operational {ops_t.table_name} records for this table",
        )


def _add_computed_relationship_field(
    fields: dict[str, GraphQLField],
    rel: dict,
    fn_name: str,
    functions: list[dict],
    tables: list[_TableInfo],
    gql_types: dict[int, GraphQLObjectType],
) -> None:
    func = next((f for f in functions if f["name"] == fn_name), None)
    if not func:
        return
    returns_str = func.get("returns", "")
    parts = returns_str.split(".", 1) if returns_str else []
    if len(parts) != 2:
        return
    ret_type = next(
        (
            gql_types[t.table_id]
            for t in tables
            if t.schema_name == parts[0] and t.table_name == parts[1]
        ),
        None,
    )
    if ret_type:
        field_key = re.sub(r"[^a-zA-Z0-9_]", "_", rel["id"])
        fields[field_key] = GraphQLField(GraphQLList(GraphQLNonNull(ret_type)))


def _add_standard_relationship_field(
    fields: dict[str, GraphQLField],
    rel: dict,
    table_lookup: dict[int, _TableInfo],
    gql_types: dict[int, GraphQLObjectType],
    where_types: dict[int, GraphQLInputObjectType | None],
    order_by_types: dict[int, GraphQLInputObjectType],
    distinct_enums: dict[int, GraphQLEnumType | None],
) -> None:
    target = table_lookup.get(rel["target_table_id"])
    if not target:
        return
    target_type = gql_types[target.table_id]
    cardinality = rel["cardinality"]
    field_name = rel.get("graphql_alias") or rel_field_name(target.field_name, cardinality)
    if cardinality == "many-to-one":
        fields[field_name] = GraphQLField(target_type)
    elif cardinality == "one-to-many":
        fields[field_name] = GraphQLField(
            GraphQLList(GraphQLNonNull(target_type)),
            args=_build_db_field_args(
                where_types.get(target.table_id),
                order_by_types.get(target.table_id),
                distinct_enums.get(target.table_id),
            ),
        )


def _make_object_type_fields(
    tid: int,
    table_lookup: dict[int, _TableInfo],
    gql_types: dict[int, GraphQLObjectType],
    meta_rt: _TableInfo | None,
    ops_traversal_targets: list[_TableInfo],
    visible_rels: list[dict],
    functions: list[dict],
    tables: list[_TableInfo],
    where_types: dict[int, GraphQLInputObjectType | None],
    order_by_types: dict[int, GraphQLInputObjectType],
    distinct_enums: dict[int, GraphQLEnumType | None],
) -> dict[str, GraphQLField]:
    info = table_lookup[tid]
    fields: dict[str, GraphQLField] = dict(info.gql_fields)

    _add_virtual_system_fields(fields)

    if meta_rt is not None:
        _add_meta_field(fields, info, meta_rt, gql_types)

    if meta_rt is not None and tid == meta_rt.table_id:
        _add_ops_traversal_fields(
            fields, ops_traversal_targets, gql_types, where_types, order_by_types, distinct_enums
        )

    for rel in visible_rels:
        if rel["source_table_id"] != tid:
            continue
        fn_name = rel.get("target_function_name")
        if fn_name:
            _add_computed_relationship_field(fields, rel, fn_name, functions, tables, gql_types)
        else:
            _add_standard_relationship_field(
                fields, rel, table_lookup, gql_types, where_types, order_by_types, distinct_enums
            )

    return fields


def _build_native_filter_args(
    t: _TableInfo,
    args: dict[str, GraphQLArgument],
) -> None:
    """Append native filter args to args in-place."""
    visible_col_names = [
        c["column_name"] for c in t.visible_columns if c["column_name"].lower() in t.column_metadata
    ]
    response_field_names = set(visible_col_names) | {
        "where",
        "order_by",
        "limit",
        "offset",
        "distinct_on",
    }
    for nfc in t.native_filter_columns:
        col_name = nfc["column_name"]
        bare_name = col_name[4:] if col_name.startswith("_nf_") else col_name
        arg_name = f"_{bare_name}" if bare_name in response_field_names else bare_name
        meta = t.column_metadata.get(col_name.lower())
        nfc_gql_type = column_type_to_graphql(meta.data_type) if meta else GraphQLString
        scalar = nfc_gql_type.of_type if isinstance(nfc_gql_type, GraphQLList) else nfc_gql_type
        required = nfc.get("native_filter_type") == "path_param"
        args[arg_name] = GraphQLArgument(
            GraphQLNonNull(scalar) if required else scalar,
            description=f"Native API filter ({nfc.get('native_filter_type', 'query_param')})",
        )


def _build_aggregate_query_field(
    t: _TableInfo,
    gql_type: GraphQLObjectType,
    enum_types: dict,
    agg_fields_type: GraphQLObjectType | None = None,
) -> tuple[str, GraphQLField] | None:
    agg_type = build_aggregate_types(
        t.type_name, t.visible_columns, t.column_metadata, gql_type, agg_fields_type
    )
    if not agg_type:
        return None
    agg_args: dict[str, GraphQLArgument] = {}
    agg_where = _build_where_input(t, f"{t.type_name}Agg", enum_types=enum_types)
    if agg_where:
        agg_args["where"] = GraphQLArgument(agg_where)
    if (t.gql_convention_override or active_gql_convention()) == "apollo_graphql":
        agg_field_name = f"{t.field_name}Aggregate"
    else:
        agg_field_name = f"{t.field_name}_aggregate"
    return agg_field_name, GraphQLField(agg_type, args=agg_args)


def _build_group_by_query_field(
    t: _TableInfo,
    gql_type: GraphQLObjectType,
    where_type: GraphQLInputObjectType | None,
    order_by_type: GraphQLInputObjectType | None,
    distinct_enum: GraphQLEnumType | None,
    enum_types: dict,
    agg_fields_type: GraphQLObjectType | None = None,
) -> tuple[str, GraphQLField] | None:
    """Build {field_name}_group_by root query field (REQ-654, REQ-655)."""
    by_enum = distinct_enum or _build_distinct_on_enum(t)
    if not by_enum:
        return None

    if agg_fields_type is None:
        agg_fields_type = build_agg_fields_type(t.type_name, t.visible_columns, t.column_metadata)

    # REQ-655: aggregates field accepts where: arg for FILTER (WHERE ...) per function
    agg_where = _build_where_input(t, f"{t.type_name}GroupByAgg", enum_types=enum_types)
    agg_field_args: dict[str, GraphQLArgument] = {}
    if agg_where:
        agg_field_args["where"] = GraphQLArgument(agg_where)

    aggregates_field = GraphQLField(
        GraphQLNonNull(agg_fields_type),
        args=agg_field_args,
    )

    group_by_row_type = cast(
        GraphQLObjectType,
        GraphQLObjectType(
            f"{t.type_name}GroupByRow",
            lambda agg=aggregates_field, gt=gql_type: {
                "groupKey": GraphQLField(GraphQLNonNull(cast(GraphQLScalarType, JSONScalar))),
                "aggregate": agg,
                "nodes": GraphQLField(GraphQLList(GraphQLNonNull(gt))),
            },
        ),
    )

    # REQ-655: having: arg on root field for SQL HAVING
    having_exp = build_having_exp_type(t.type_name, t.visible_columns, t.column_metadata)

    gb_args: dict[str, GraphQLArgument] = {
        "by": GraphQLArgument(GraphQLNonNull(GraphQLList(GraphQLNonNull(by_enum)))),
        "limit": GraphQLArgument(GraphQLInt),
        "offset": GraphQLArgument(GraphQLInt),
    }
    if where_type:
        gb_args["where"] = GraphQLArgument(where_type)
    if order_by_type:
        gb_args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(order_by_type)))
    if distinct_enum:
        gb_args["distinct_on"] = GraphQLArgument(GraphQLList(GraphQLNonNull(distinct_enum)))
    if having_exp:
        gb_args["having"] = GraphQLArgument(having_exp)

    conv = t.gql_convention_override or active_gql_convention()
    if conv == "apollo_graphql":
        gb_field_name = f"{t.field_name}GroupBy"
    else:
        gb_field_name = f"{t.field_name}_group_by"

    return gb_field_name, GraphQLField(
        GraphQLNonNull(GraphQLList(GraphQLNonNull(group_by_row_type))),
        args=gb_args,
    )


def _build_connection_query_field(
    t: _TableInfo,
    gql_type: GraphQLObjectType,
    order_by_types: dict[int, GraphQLInputObjectType],
    enum_types: dict,
) -> tuple[str, GraphQLField]:
    _edge_type, conn_type = _build_connection_types(t.type_name, gql_type)
    conn_args: dict[str, GraphQLArgument] = {
        "first": GraphQLArgument(GraphQLInt),
        "after": GraphQLArgument(GraphQLString),
        "last": GraphQLArgument(GraphQLInt),
        "before": GraphQLArgument(GraphQLString),
    }
    conn_where = _build_where_input(t, f"{t.type_name}Conn", enum_types=enum_types)
    if conn_where:
        conn_args["where"] = GraphQLArgument(conn_where)
    conn_ob = order_by_types.get(t.table_id)
    if conn_ob:
        conn_args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(conn_ob)))
    return f"{t.field_name}_connection", GraphQLField(conn_type, args=conn_args)


def _build_mutation_fields_for_table(  # REQ-032, REQ-033, REQ-034, REQ-036, REQ-037, REQ-212
    t: _TableInfo,
    enum_types: dict,
) -> dict[str, GraphQLField]:
    """Build insert/upsert/update/delete mutation fields for one table. Returns {} if skipped."""
    insert_fields: dict[str, GraphQLInputField] = {}
    for col in t.visible_columns:
        col_name = col["column_name"]
        meta = t.column_metadata.get(col_name.lower())
        if meta is None:
            continue
        col_gql_type = column_type_to_graphql(meta.data_type)
        if isinstance(col_gql_type, GraphQLList):
            col_gql_type = GraphQLString  # fallback for arrays in input
        insert_fields[col_name] = GraphQLInputField(col_gql_type)

    if not insert_fields:
        return {}

    insert_input = cast(
        GraphQLInputObjectType,
        GraphQLInputObjectType(f"{t.type_name}InsertInput", lambda fields=insert_fields: fields),
    )
    set_input = cast(
        GraphQLInputObjectType,
        GraphQLInputObjectType(f"{t.type_name}SetInput", lambda fields=insert_fields: fields),
    )
    where_input = _build_where_input(t, f"{t.type_name}Mutation", enum_types=enum_types)
    response_type = cast(
        GraphQLObjectType,
        GraphQLObjectType(
            f"{t.type_name}MutationResponse",
            lambda t=t: {"affected_rows": GraphQLField(GraphQLNonNull(GraphQLInt))},
        ),
    )
    conflict_col_enum = cast(
        GraphQLEnumType,
        GraphQLEnumType(
            f"{t.type_name}ConflictColumn",
            {name: GraphQLEnumValue(name) for name in insert_fields},
        ),
    )

    conv = t.gql_convention_override or active_gql_convention()
    result: dict[str, GraphQLField] = {}
    result[_mutation_name("insert", t.field_name, conv)] = GraphQLField(
        GraphQLNonNull(response_type),
        args={"input": GraphQLArgument(GraphQLNonNull(insert_input))},
    )
    result[_mutation_name("upsert", t.field_name, conv)] = GraphQLField(
        GraphQLNonNull(response_type),
        args={
            "input": GraphQLArgument(GraphQLNonNull(insert_input)),
            "on_conflict": GraphQLArgument(
                GraphQLNonNull(GraphQLList(GraphQLNonNull(conflict_col_enum)))
            ),
        },
    )
    if where_input:
        result[_mutation_name("update", t.field_name, conv)] = GraphQLField(
            GraphQLNonNull(response_type),
            args={
                "set": GraphQLArgument(GraphQLNonNull(set_input)),
                "where": GraphQLArgument(GraphQLNonNull(where_input)),
            },
        )
        result[_mutation_name("delete", t.field_name, conv)] = GraphQLField(
            GraphQLNonNull(response_type),
            args={"where": GraphQLArgument(GraphQLNonNull(where_input))},
        )
    return result


def build_table_path_map(si: SchemaInput) -> dict[str, dict]:
    """Return {gql_field_name: {schema_name, table_name, domain_id, table_description, domain_description}} for REST path routing."""
    tables = _build_visible_tables(si)
    if not tables:
        return {}
    domain_alias_map = _build_domain_alias_map(si.domains)
    _assign_names(
        tables, si.naming_rules, domain_prefix=si.domain_prefix, domain_alias_map=domain_alias_map
    )
    domain_descs = {d["id"]: d.get("description") for d in si.domains}
    return {
        t.field_name: {
            "schema_name": t.schema_name,
            "table_name": t.table_name,
            "domain_id": t.domain_id,
            "table_description": t.description,
            "domain_description": domain_descs.get(t.domain_id),
        }
        for t in tables
    }


def generate_schema(
    si: SchemaInput,
) -> (
    GraphQLSchema
):  # REQ-007, REQ-008, REQ-021, REQ-133, REQ-134, REQ-196, REQ-197, REQ-213, REQ-218, REQ-253
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

    domain_alias_map = _build_domain_alias_map(si.domains)
    _assign_names(
        tables, si.naming_rules, domain_prefix=si.domain_prefix, domain_alias_map=domain_alias_map
    )
    tables = _dedup_tables(tables)

    # Build base column fields — share object_type_registry so same-named object types are reused
    _object_type_registry: dict[str, GraphQLObjectType] = {}
    for t in tables:
        t.gql_fields = _build_column_fields(
            t,
            override=t.gql_convention_override,
            enum_types=si.enum_types,
            object_type_registry=_object_type_registry,
            governed_gql_types=si.governed_gql_types or None,
        )

    table_lookup: dict[int, _TableInfo] = {t.table_id: t for t in tables}
    visible_rels = [r for r in si.relationships if _can_see_relationship(r, table_lookup)]
    gql_types: dict[int, GraphQLObjectType] = {}

    _meta_rt = next(
        (t for t in tables if t.domain_id == "meta" and t.table_name == "registered_tables"),
        None,
    )
    _ops_traversal_targets: list[_TableInfo] = [
        t
        for t in tables
        if t.domain_id == "ops" and any(c["column_name"] == "table_name" for c in t.visible_columns)
    ]

    order_by_types = _build_order_by_inputs(tables, visible_rels, table_lookup)
    where_types = {
        t.table_id: _build_where_input(t, t.type_name, enum_types=si.enum_types) for t in tables
    }
    distinct_enums = {t.table_id: _build_distinct_on_enum(t) for t in tables}

    for t in tables:
        tid = t.table_id

        def make_fields(tid=tid):
            return _make_object_type_fields(
                tid,
                table_lookup,
                gql_types,
                _meta_rt,
                _ops_traversal_targets,
                visible_rels,
                si.functions,
                tables,
                where_types,
                order_by_types,
                distinct_enums,
            )

        gql_types[tid] = cast(
            GraphQLObjectType,
            GraphQLObjectType(t.type_name, make_fields, description=t.description),
        )

    # Build root query fields
    query_fields: dict[str, GraphQLField] = {}
    _root_ids = si.root_table_ids
    _accessible_domains = set(si.role.get("domain_access") or [])
    _all_access = "*" in _accessible_domains

    for t in tables:
        if _root_ids is not None and t.table_id not in _root_ids:
            continue
        if (
            not _all_access
            and t.domain_id in _IMPLICIT_TRAVERSAL_DOMAINS
            and t.domain_id not in _accessible_domains
        ):
            continue
        gql_type = gql_types[t.table_id]
        args = _build_db_field_args(
            where_types.get(t.table_id),
            order_by_types.get(t.table_id),
            distinct_enums.get(t.table_id),
        )
        _build_native_filter_args(t, args)
        query_fields[t.field_name] = GraphQLField(GraphQLList(GraphQLNonNull(gql_type)), args=args)

        # Build agg_fields_type once to avoid duplicate GraphQL type names when both flags on.
        shared_agg_fields_type = None
        if t.enable_aggregates or t.enable_group_by:
            shared_agg_fields_type = build_agg_fields_type(
                t.type_name, t.visible_columns, t.column_metadata
            )

        # REQ-653: table-level enable_aggregates gates the _aggregate root field.
        # REQ-197: role-level "no_aggregations" capability (or top-level key) overrides.
        # Also blocked when allow_aggregations is explicitly set to False.
        _role_blocks_agg = (
            si.role.get("no_aggregations")
            or "no_aggregations" in (si.role.get("capabilities") or [])
            or si.role.get("allow_aggregations") is False
        )
        if t.enable_aggregates and not _role_blocks_agg:
            agg_result = _build_aggregate_query_field(
                t, gql_type, si.enum_types, shared_agg_fields_type
            )
            if agg_result:
                query_fields[agg_result[0]] = agg_result[1]

        # REQ-653/654: table-level enable_group_by gates the _group_by root field.
        if t.enable_group_by:
            gb_result = _build_group_by_query_field(
                t,
                gql_type,
                where_types.get(t.table_id),
                order_by_types.get(t.table_id),
                distinct_enums.get(t.table_id),
                si.enum_types,
                shared_agg_fields_type,
            )
            if gb_result:
                query_fields[gb_result[0]] = gb_result[1]

        if t.relay_pagination:
            conn_name, conn_field = _build_connection_query_field(
                t, gql_type, order_by_types, si.enum_types
            )
            query_fields[conn_name] = conn_field

    query_type = cast(GraphQLObjectType, GraphQLObjectType("Query", lambda: query_fields))

    # Build mutation types for RDBMS tables (REQ-031–REQ-037)
    nosql_types = {"mongodb", "cassandra"}
    mutation_fields: dict[str, GraphQLField] = {}

    for t in tables:
        if si.source_types and si.source_types.get(t.source_id, "") in nosql_types:
            continue
        mutation_fields.update(_build_mutation_fields_for_table(t, si.enum_types))

    extra_query, extra_mutation = _build_action_fields(si, gql_types, tables, domain_alias_map)
    query_fields.update(extra_query)
    mutation_fields.update(extra_mutation)

    mutation_type: GraphQLObjectType | None = None
    if mutation_fields:
        mutation_type = cast(
            GraphQLObjectType, GraphQLObjectType("Mutation", lambda: mutation_fields)
        )

    subscription_fields = _build_subscription_fields(si, tables, gql_types)
    subscription_type: GraphQLObjectType | None = (
        cast(GraphQLObjectType, GraphQLObjectType("Subscription", lambda: subscription_fields))
        if subscription_fields
        else None
    )

    return GraphQLSchema(
        query=query_type,
        mutation=mutation_type,
        subscription=subscription_type,
        directives=[*specified_directives, *PROVISA_DIRECTIVES],
        types=[RouteEngineEnum, JoinStrategyEnum],
    )
