# Copyright (c) 2026 Kenneth Stott
# Canary: 59c1a956-decc-48aa-be44-4fbdb480d46a
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

# Requirements: REQ-007, REQ-008, REQ-010, REQ-021, REQ-032, REQ-033, REQ-034, REQ-036, REQ-037, REQ-039, REQ-133, REQ-134, REQ-154, REQ-155, REQ-156, REQ-157, REQ-194, REQ-196, REQ-197, REQ-200, REQ-201, REQ-202, REQ-205, REQ-206, REQ-207, REQ-209, REQ-210, REQ-212, REQ-213, REQ-218, REQ-219, REQ-221, REQ-253, REQ-258, REQ-259, REQ-260, REQ-363, REQ-1614

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, cast

from graphql import (
    GraphQLArgument,
    GraphQLEnumType,
    GraphQLEnumValue,
    GraphQLFloat,
    GraphQLField,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLSchema,
    specified_directives,
)

from provisa.compiler.aggregate_gen import (
    build_agg_fields_type,
    build_aggregate_types,
    build_group_by_order_by_type,
    build_having_exp_type,
)
from provisa.compiler import hasura_schema
from provisa.compiler.naming import (
    active_gql_convention,
    hasura_argument_name,
    hasura_convention_of,
    hasura_pk_constraint,
    hasura_served_names,
    root_field_variant,
    apply_gql_name,
    bare_field_name,
    domain_to_sql_name,
    generate_name,
    refuse_taken_sql_addresses,
    relationship_field_name,
    SYSTEM_SCHEMA_GQL_PREFIX,
    system_schema_names,
    to_type_name,
    unprefixed_field_name,
)
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.metric_expand import metric_reference_tables
from provisa.compiler.sql_types import extracts_paths
from provisa.compiler.ontology_classes import served_class_columns
from provisa.core.modeling_tags import append_modeling_tag
from provisa.compiler.type_map import JSONScalar, registered_graphql_type, array_graphql_type
from provisa.compiler.schema_types import SchemaInput, _TableInfo, offers_domain
from provisa.security.rights import (
    GOVERNANCE_META_COLUMNS,
    META_DOMAIN_ID,
    SQL_ONLY_TABLES,
    Capability,
    column_served,
    has_capability,
    SYSTEM_DOMAINS,
    acts_in_domain,
    reaches_domain,
    served_across_domains,
)
from provisa.compiler.actions_schema import (
    CommandFieldTaken,
    _build_action_fields,
    _mutation_name,
)

from provisa.compiler.schema_directives import (
    GraphQLInt,
    GraphQLString,
    JoinStrategyEnum,
    PROVISA_DIRECTIVES,
    RouteEngineEnum,
    _build_connection_types,
)
from provisa.compiler.schema_inputs import (
    _build_column_fields,
    _build_db_field_args,
    _build_distinct_on_enum,
    _build_order_by_inputs,
    _build_where_input,
    _gql_col_name,
)


def _build_visible_tables(  # REQ-008, REQ-039, REQ-363
    si: SchemaInput, *, for_sql: bool = False
) -> list[_TableInfo]:
    """Filter tables by role's domain access. Build per-table metadata. ``for_sql`` keeps meta's
    default tier, which a role with no meta grant reads on SQL alone (REQ-1132): the other surfaces
    describe what it reads of its data, and to it meta is no domain."""
    tables, sql_only = _visible_tables(si)
    return tables if for_sql else [t for t in tables if t.table_id not in sql_only]


@dataclass(frozen=True)
class RoleTables:  # REQ-1882
    """One role's tables, found and named once for every part of its surface -- its schema,
    context, path map and proto -- which each found and named them again from the same input. Built
    by :func:`role_tables` from one role's schema input and read only by that input's parts. The
    context and path map read the tables as found; the schema gives its tables fields and proto
    renames its own, so each of those takes copies (:meth:`served_copies`)."""

    named: tuple[_TableInfo, ...]
    #: The ids of the tables the role reads on SQL alone (_visible_tables).
    sql_only: frozenset[int]

    def for_sql(self) -> list[_TableInfo]:
        """The tables as ``_build_visible_tables(si, for_sql=True)`` finds them, named; read, never
        changed."""
        return list(self.named)

    def served(self) -> list[_TableInfo]:
        """The tables as ``_build_visible_tables(si)`` finds them, named; read, never changed."""
        return [t for t in self.named if t.table_id not in self.sql_only]

    def served_copies(self) -> list[_TableInfo]:
        """:meth:`served`, each table a copy of its own to change: its fields and names."""
        return [_copied(t) for t in self.served()]


def _copied(table: _TableInfo) -> _TableInfo:
    """A shallow copy of ``table``: its own attributes, its column lists shared (none is changed
    in place). ``copy.copy`` rebuilds it through the pickle protocol, several times slower."""
    out = object.__new__(_TableInfo)
    out.__dict__.update(table.__dict__)
    return out


def role_tables(si: SchemaInput) -> RoleTables:
    """``si``'s role's tables, found and named once (:class:`RoleTables`). A table's names do not
    depend on which other tables are named with it, so naming the SQL tables names the served ones
    too; a domain holding two tables of one SQL address raises here, as building the role's context
    does."""
    tables, sql_only = _visible_tables(si)
    _assign_names(
        tables,
        si.naming_rules,
        domain_prefix=si.domain_prefix,
        domain_alias_map=_build_domain_alias_map(si.domains),
    )
    return RoleTables(named=tuple(tables), sql_only=sql_only)


def _visible_tables(si: SchemaInput) -> tuple[list[_TableInfo], frozenset[int]]:
    """The role's tables as SQL reads them (``_build_visible_tables(si, for_sql=True)``), and the
    ids of those it reads on SQL alone: meta's default tier, for a role with no meta grant."""
    role = si.role
    # A ROLE's domain_access: it reaches the domains it lists, "*" is the only way to say all, an
    # empty list is no domains, and a system domain is reached by its capability
    # (rights.reaches_domain).
    # A COLUMN's visible_to below reads the same way: an empty list is no role (REQ-2154).

    # REQ-1319: role-visible metrics keyed by the single semantic table their expression
    # references. The aggregate compiler selects from exactly one table (no join builder),
    # so only single-table metrics project into a metrics block; a multi-table metric stays
    # reachable through the raw-SQL `metrics.<name>` expansion (REQ-1317).
    metrics_by_table: dict[str, list[dict]] = {}
    for m in si.metrics:
        # REQ-1317: a metric is read by the roles it names; none named is no role.
        if role["id"] not in (m.get("visible_to") or ()):
            continue
        refs = metric_reference_tables(m["name"], m["expression"])
        if len(refs) == 1:
            metrics_by_table.setdefault(refs[0], []).append(m)

    def _reaches(table: dict) -> bool:
        # The role reaches the table's domain: one it lists, "*", or a system domain it holds.
        return reaches_domain(role["domain_access"], table["domain_id"])

    tables_by_id = {t["id"]: t for t in si.tables}

    def _served(table_id: int) -> frozenset[str]:
        bound = tables_by_id[table_id]
        reached = _reaches(bound)
        return frozenset(
            c["column_name"]
            for c in bound["columns"]
            if column_served(role, bound["domain_id"], c, reaches=reached)
            and not c.get("native_filter_type")
        )

    result: list[_TableInfo] = []
    sql_only: set[int] = set()
    for table in si.tables:
        # REQ-2163: a Class table's column is served where one of the columns it is bound to is,
        # and the Class is reached by a reader served any of them.
        class_served = served_class_columns(table, _served) if "class_bindings" in table else None
        # A table outside reach is still served when it publishes a column to the role
        # (REQ-1959) — with its PUBLIC columns only, which column_served decides below.
        reaches = _reaches(table) if class_served is None else bool(class_served)
        if table["domain_id"] == META_DOMAIN_ID and not reaches:
            sql_only.add(table["id"])
        if not reaches and not served_across_domains(role, table):
            continue

        table_id = table["id"]
        if table_id not in si.column_types or not si.column_types[table_id]:
            import logging as _log

            _log.getLogger(__name__).warning(
                "No column metadata for table %r (id=%s) — skipping.", table["table_name"], table_id
            )
            continue
        col_meta = {m.column_name.lower(): m for m in si.column_types[table_id]}
        _served_as_text(table, col_meta, si)

        # Filter columns by role visibility; split native filter cols from regular cols.
        # REQ-1132/REQ-1134: in the meta
        # (catalog) domain, GOVERNANCE columns (visible_to, masks, view_sql, …) are hidden unless the
        # role holds view_governance — enforced HERE so every query surface (GraphQL, SQL,
        # cypher) that derives from this per-role schema sees the same governed catalog.
        _hide_meta_gov = table["domain_id"] == META_DOMAIN_ID and not has_capability(
            role, Capability.VIEW_GOVERNANCE
        )
        # Which columns the role is served is ONE rule (security.rights.column_served): the
        # column's grant names the role (empty is no role, REQ-2154), and outside the role's
        # reach only a public column (REQ-1959). No capability stands in for a grant (REQ-1327).
        visible_cols = [
            c
            for c in table["columns"]
            if (
                column_served(role, table["domain_id"], c, reaches=reaches)
                if class_served is None
                else c["column_name"] in class_served
            )
            and not c.get("native_filter_type")
            and not (_hide_meta_gov and c["column_name"] in GOVERNANCE_META_COLUMNS)
        ]
        # Native filter cols are API parameters (path/query params), not data columns, so they
        # carry no visible_to gate of their own: a role that can read the endpoint's rows may
        # always pass its path/query arguments.
        _raw_nfc = [c for c in table["columns"] if c.get("native_filter_type")]
        _base_names = {
            c["column_name"] for c in _raw_nfc if not c["column_name"].startswith("_nf_")
        }
        native_filter_cols = [
            c
            for c in _raw_nfc
            if not c["column_name"].startswith("_nf_") or c["column_name"][4:] not in _base_names
        ]
        # REQ-1614: but the exemption is not itself an entitlement. There is no table-level grant
        # in this model — a role's reach over a table IS its column grants — so the reach the
        # parameters ride on is read off those: at least one data column visible here. Without
        # this, a role no column was granted to still saw every API-backed table, because its
        # parameters alone satisfied the survival test below and kept it in the schema. That is
        # what a sandbox visitor met: the whole model reduced to the five REST endpoints, offered
        # as fields whose only arguments were the ones the gate never covered.
        if not visible_cols:
            native_filter_cols = []

        if not visible_cols and not native_filter_cols:
            if table.get("columns") or not col_meta or not reaches:
                # Columns were defined but none visible to this role, or no metadata available
                continue
            # No registered columns but synthesized metadata exists (e.g. a remote schema's types)
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
                # REQ-1320: aggregates default ON for fact tables — modeling_role == "fact"
                # enables the _aggregate root field without an explicit enable_aggregates.
                enable_aggregates=(
                    bool(table.get("enable_aggregates", False))
                    or table.get("modeling_role") == "fact"
                ),
                enable_group_by=bool(table.get("enable_group_by", False)),
                read_only=bool(table.get("view_sql")),  # REQ-1157: MV/view → query-only
                time_travel=si.time_travel.get(table_id),  # REQ-1163
                write_ops=frozenset(table["write_ops"]),
                write_returns_rows=table["write_returns_rows"],
                write_refused_forms=frozenset(table["write_refused_forms"]),
                # REQ-2074: the role's write grants on the table, by operation.
                # A capability opening a system domain brings no write (rights.acts_in_domain).
                write_grants={
                    g["operation"]: g
                    for g in table.get("write_grants") or []
                    if g["role"] == role["id"]
                    and (
                        table["domain_id"] not in SYSTEM_DOMAINS
                        or acts_in_domain(role["domain_access"], table["domain_id"])
                    )
                },
                modeling_role=table.get("modeling_role"),  # REQ-1320
                modeling_history=table.get("modeling_history"),  # REQ-1320
                metrics=metrics_by_table.get(table["table_name"], []),  # REQ-1319
                deprecation_reason=table.get("deprecation_reason"),  # REQ-1375
                tags=list(table.get("tags") or ()),  # REQ-1415
            )
        )

    return result, frozenset(sql_only)


def _served_as_text(
    table: dict, col_meta: dict[str, ColumnMetadata], si: SchemaInput
) -> None:  # REQ-151, REQ-152
    """Type each path column of ``table`` on a SQL database as text in ``col_meta``: the governed
    relation serves it as the text at its path (stage2._read_stored, ``->>``), whatever the type of
    the JSON column it is read from, so its field is a String and a filter on it compares text. A
    path column a connector resolves keeps its registered type."""
    paths = [c["column_name"].lower() for c in table["columns"] if c.get("path")]
    if not paths or not extracts_paths(
        (si.source_types or {}).get(table["source_id"], ""), table["table_name"]
    ):
        return
    for name in paths:
        if name in col_meta:
            col_meta[name] = replace(
                col_meta[name], data_type="varchar", subtype=None, native_type=None
            )


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

        # REQ-1933: one SQL address per table in a domain. Registration refuses a taken one; a
        # model that holds two anyway is an error, never resolved by the order tables are listed.
        refuse_taken_sql_addresses(
            [
                {
                    "domain_id": domain_id,
                    "source_id": t.source_id,
                    "schema_name": t.schema_name,
                    "table_name": _strip(t.table_name) if not t.alias else t.table_name,
                    "alias": t.alias,
                    "gql_naming_convention": t.gql_convention_override,
                }
                for t in group
            ],
            naming_rules,
        )
        for t in group:
            base_table_name = _strip(t.table_name) if not t.alias else t.table_name
            convention = t.gql_convention_override or active_gql_convention()
            t.field_name = generate_name(
                base_table_name, naming_rules, alias=t.alias, convention=convention
            )
            hasura_type = None
            if domain_id not in SYSTEM_SCHEMA_GQL_PREFIX:
                t.hasura = hasura_convention_of(convention)
            if t.hasura is not None:
                # REQ-194: a Hasura-preset table is named as Hasura names it.
                served = hasura_served_names(convention, base_table_name, naming_rules, t.alias)
                t.field_name, hasura_type = served.select, served.type
                t.hasura_constraint = served.constraint
            if domain_id in SYSTEM_SCHEMA_GQL_PREFIX:
                # REQ-2163: a system schema's names carry its own prefix, domains prefixed or not.
                t.field_name, t.type_name = system_schema_names(domain_id, t.field_name)
            elif domain_prefix:
                alias = (domain_alias_map or {}).get(domain_id)
                if alias:
                    t.field_name = f"{alias}__{t.field_name}"
                    t.type_name = f"{alias.upper()}__{to_type_name(t.field_name.split('__', 1)[1])}"
                elif domain_snake:
                    t.field_name = f"{domain_snake}__{t.field_name}"
                    t.type_name = to_type_name(t.field_name)
                else:
                    t.type_name = to_type_name(t.field_name)
                if hasura_type is not None and "__" in t.type_name:
                    t.type_name = f"{t.type_name.split('__', 1)[0]}__{hasura_type}"
                    t.hasura_constraint = f"{t.field_name.split('__', 1)[0]}__{t.hasura_constraint}"
                elif hasura_type is not None:
                    t.type_name = hasura_type
            else:
                t.type_name = hasura_type or to_type_name(t.field_name)
    _refuse_system_schema_names(tables)


def _refuse_system_schema_names(tables: list[_TableInfo]) -> None:
    """REQ-2163: no model table takes a name a system schema's table has (``on_terms``, its type
    ``ON_Terms``); one that would is refused by name rather than shadowing it."""
    system = {
        name: t
        for t in tables
        if t.domain_id in SYSTEM_SCHEMA_GQL_PREFIX
        for name in (t.field_name, t.type_name)
    }
    for t in tables:
        if t.domain_id in SYSTEM_SCHEMA_GQL_PREFIX:
            continue
        for name in (t.field_name, t.type_name):
            if name in system:
                raise ValueError(
                    f"the table {t.domain_id}.{t.table_name} is exposed in GraphQL as {name!r}, "
                    f"which is the {system[name].domain_id} schema's table "
                    f"{system[name].table_name!r}; give it an alias"
                )


#: REQ-1365 (amended 2026-10-27, "THE RESOLVER IS META'S"): the extension marking the Query
#: field that dereferences a semantic address -- the meta domain's ``resolve``, beside the catalog
#: every role reaches. Every surface finds it by this mark (:func:`resolve_field`), its name being
#: meta's: ``<meta alias>__resolve`` where domains prefix their fields.
RESOLVE_EXTENSION = "provisa_resolve"


def resolve_field_name(domain_prefix: bool, domain_alias_map: dict[str, str]) -> str:
    """The resolver's Query field name: ``resolve`` named as a field of the meta domain is."""
    name = apply_gql_name("resolve")
    if not domain_prefix:
        return name
    alias = domain_alias_map.get(META_DOMAIN_ID) or domain_to_sql_name(META_DOMAIN_ID)
    return f"{alias}__{name}"


def resolve_field(schema: Any) -> str:
    """The name of ``schema``'s resolver field (:data:`RESOLVE_EXTENSION`)."""
    for name, field in schema.query_type.fields.items():
        if (field.extensions or {}).get(RESOLVE_EXTENSION):
            return name
    raise LookupError("the schema has no semantic-address resolver field")


def _add_resolve_field(query_fields: dict, name: str) -> None:
    """Add meta's ``resolve(uri, as_of, as_recorded)`` to a role's Query type (REQ-1365), named
    ``name`` (:func:`resolve_field_name`). It is in every role's schema, as the catalog every role
    reaches is, so REST and MCP offer it under meta's name by the REQ-1929 rule. A table or command
    already named for it is refused by name rather than shadowed."""
    if name in query_fields:
        raise ValueError(
            f"a table or command is exposed as the GraphQL field {name!r}, which is the meta "
            "domain's semantic-address resolver; give it an alias"
        )
    query_fields[name] = GraphQLField(
        cast(GraphQLScalarType, JSONScalar),
        args={
            "uri": GraphQLArgument(GraphQLNonNull(GraphQLString)),
            "as_of": GraphQLArgument(GraphQLString),
            "as_recorded": GraphQLArgument(GraphQLString),
        },
        description=(
            "The governed value a semantic address names: "
            "provisa://<org>/<domain>/tables/<table>/pk=<value>[/<column>], optionally as of "
            "a point in time, and as recorded at one."
        ),
        extensions={RESOLVE_EXTENSION: True},
    )


def _type_description(t: _TableInfo) -> str | None:
    """REQ-1320: surface modeling role/SCD in introspection docs.

    Appends a " [fact]" / " [dimension, scd2]"-style suffix to the table description
    when a modeling role is set, so GraphQL introspection carries the star-schema shape.
    REQ-1415: the table's tag ids follow the role in the same suffix.
    """
    # REQ-1986: a table that requires parameters says so, and what it therefore cannot give.
    from provisa.core.parameters import describe_limits

    description = describe_limits(t.description, t.native_filter_columns)
    if not t.modeling_role and not t.tags:
        return description
    return append_modeling_tag(description, t.modeling_role, t.modeling_history, t.tags)


def _can_see_relationship(rel: dict, table_lookup: dict[int, _TableInfo]) -> bool:
    """Whether both sides of a relationship are visible to the role. A computed relationship's
    source column is the function's argument and must be visible; a table relationship's join
    columns need not be."""
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
    # REQ-1586: a junction-backed edge reaches its rows through its junction, so the role must
    # see the junction too; the join emitter reads it as the edge's second hop.
    if rel.get("via_table_id") and rel["via_table_id"] not in table_lookup:
        return False
    # Both tables are visible: the relationship is offered. Its join columns are read by the
    # engine to join, never returned, so the role need not be able to select them (governance
    # keeps an approved join's key: stage2._approved_join_key).
    return True


_CDC_SOURCES: frozenset[str] = frozenset({"postgresql", "mongodb", "kafka", "debezium"})


def _build_subscription_fields(  # REQ-219, REQ-258, REQ-260
    si: SchemaInput,
    tables: list[_TableInfo],
    gql_types: dict[int, GraphQLObjectType],
    args_of: Callable[[_TableInfo], dict[str, GraphQLArgument]],
    where_types: dict[int, GraphQLInputObjectType | None],
) -> dict[str, GraphQLField]:
    """Build Subscription root fields.

    Includes registered tables that have a watermark column set (polling-based)
    or whose source supports native CDC (postgresql, mongodb, kafka, debezium).
    Tables in `tables` are already filtered by role visibility.

    A field takes its table's query arguments (``args_of``): each frame is the equivalent query's
    answer, so ``where``, ``order_by`` and ``limit`` shape every frame alike.

    Also includes approved persisted queries the role has been granted access to
    via the visible_to field (REQ-022–REQ-026).
    """
    raw_by_id = {t["id"]: t for t in si.tables}
    fields: dict[str, GraphQLField] = {}

    for t in tables:
        # Only a root table subscribes, as only a root table is queried (root_table_ids).
        if si.root_table_ids is not None and t.table_id not in si.root_table_ids:
            continue
        # REQ-2163: a table SQL alone serves has no GraphQL field (security.rights).
        if (t.domain_id, t.table_name) in SQL_ONLY_TABLES:
            continue
        where_type = where_types.get(t.table_id)
        if t.hasura and where_type is not None:
            # REQ-194: a Hasura-preset table streams its rows past a cursor, as Hasura serves
            # every table: each batch is a read of the table, so it needs no change feed. A
            # table with no comparable column has no where type and nothing a cursor can pass.
            fields[hasura_schema.root_field(t, suffix="stream")] = hasura_schema.stream_field(
                t, gql_types[t.table_id], where_type, _column_input_fields(t, pk_only=False)
            )
        raw = raw_by_id.get(t.table_id, {})
        source_type = (si.source_types or {}).get(t.source_id, "")
        if source_type not in _CDC_SOURCES and not raw.get("watermark_column"):
            continue
        fields[t.field_name] = GraphQLField(
            GraphQLList(GraphQLNonNull(gql_types[t.table_id])),
            args=args_of(t),
            description=t.description,
        )

    return fields


def _table_field_args(
    t: _TableInfo,
    where_types: dict[int, GraphQLInputObjectType | None],
    order_by_types: dict[int, GraphQLInputObjectType],
    distinct_enums: dict[int, GraphQLEnumType | None],
) -> dict[str, GraphQLArgument]:
    """The arguments of ``t``'s root field: its filter, order, paging and distinct, its native
    filters and its similarity search. A subscription field takes the same (REQ-258)."""
    args = _build_db_field_args(
        where_types.get(t.table_id),
        order_by_types.get(t.table_id),
        distinct_enums.get(t.table_id),
        t.hasura,
    )
    _build_native_filter_args(t, args)
    _build_similar_arg(t, args)
    _add_time_travel_args(t, args)
    return args


def _add_time_travel_args(t: _TableInfo, args: dict[str, GraphQLArgument]) -> None:
    """REQ-1163: time travel is an argument of the relation that has versions -- a bitemporal
    table or view takes ``as_of`` (valid time) and ``as_recorded`` (recorded time); an Iceberg or
    Delta table travels by its snapshots, ``as_recorded`` alone (REQ-372). Each is one ISO 8601
    time, validated as the SQL arguments are (provisa.mv.time_travel)."""
    if t.time_travel == "bitemporal":
        args["as_of"] = GraphQLArgument(
            GraphQLString, description="Read the versions in force at this valid time"
        )
    if t.time_travel in ("bitemporal", "snapshot"):
        args["as_recorded"] = GraphQLArgument(
            GraphQLString, description="Read what had been recorded by this time"
        )


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
    from provisa.compiler.naming import domain_alias_map

    return domain_alias_map(domains)


def _embedding_columns(t: _TableInfo) -> dict[str, str]:  # REQ-421, REQ-430
    """The table's embedding columns the role is served: GraphQL name -> physical name."""
    return {_gql_col_name(c, t): c["column_name"] for c in t.visible_columns if c.get("embedding")}


def _build_similar_arg(t: _TableInfo, args: dict[str, GraphQLArgument]) -> None:  # REQ-430
    """``similar: {column, text | vector}`` on a table with an embedding column: its rows ranked
    by cosine similarity to the text (embedded by the column's model) or the vector."""
    columns = _embedding_columns(t)
    if not columns:
        return
    column_enum = cast(
        GraphQLEnumType,
        GraphQLEnumType(
            f"{t.type_name}EmbeddingColumn",
            {name: GraphQLEnumValue(name) for name in columns},
        ),
    )
    float_type = cast(GraphQLScalarType, GraphQLFloat)
    similar_input = cast(
        GraphQLInputObjectType,
        GraphQLInputObjectType(
            f"{t.type_name}Similar",
            {
                "column": GraphQLInputField(GraphQLNonNull(column_enum)),
                "text": GraphQLInputField(cast(GraphQLScalarType, GraphQLString)),
                "vector": GraphQLInputField(GraphQLList(GraphQLNonNull(float_type))),
            },
            description="Rank rows by cosine similarity: a text or a vector, exactly one",
        ),
    )
    args["similar"] = GraphQLArgument(
        similar_input, description="Rank the rows by cosine similarity to a text or a vector"
    )


def _add_virtual_system_fields(fields: dict[str, GraphQLField]) -> None:
    fields["_name_"] = GraphQLField(
        GraphQLNonNull(GraphQLString),
        description="The table's name as a query names it, without its domain",
    )
    fields["_domain_"] = GraphQLField(
        GraphQLNonNull(GraphQLString), description="The table's domain"
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
        ops_base = bare_field_name(ops_t.field_name, ops_t.domain_id)
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
) -> str | None:
    func = next((f for f in functions if f["name"] == fn_name), None)
    if not func:
        return None
    returns_str = func.get("returns", "")
    parts = returns_str.split(".", 1) if returns_str else []
    if len(parts) != 2:
        return None
    ret_table = next(
        (t for t in tables if t.schema_name == parts[0] and t.table_name == parts[1]),
        None,
    )
    if ret_table is None:
        return None
    # REQ-194: named as a table relationship is — its GraphQL alias, else for the table it
    # reaches (here the one the function returns) and its cardinality.
    field_key = relationship_field_name(
        rel["cardinality"],
        target_field_name=unprefixed_field_name(ret_table.field_name, ret_table.domain_id),
        graphql_alias=rel.get("graphql_alias"),
    )
    fields[field_key] = GraphQLField(GraphQLList(GraphQLNonNull(gql_types[ret_table.table_id])))
    return field_key


def _where_relationships(  # REQ-2136
    table: _TableInfo,
    visible_rels: list[dict],
    table_lookup: dict[int, _TableInfo],
    where_types: dict[int, GraphQLInputObjectType | None],
) -> Callable[[], dict[str, GraphQLInputField]]:
    """``table``'s relationships as where conditions, each under its field's name and taking its
    target's where: a row with a related row matching it (sql_where compiles it to EXISTS). Read
    when the schema is built, once every table's where exists. A junction-backed edge's related
    rows are those its junction names (REQ-1586), under the field name it is served as."""

    def fields() -> dict[str, GraphQLInputField]:
        # The name the join is registered under, so the condition and the field are one name.
        from provisa.compiler.context import _join_field_name

        out: dict[str, GraphQLInputField] = {}
        for rel in visible_rels:
            if rel["source_table_id"] != table.table_id:
                continue
            target = table_lookup.get(rel["target_table_id"])
            target_where = where_types.get(target.table_id) if target else None
            if target is None or target_where is None:
                continue
            out[_join_field_name(rel, target, table_lookup)] = GraphQLInputField(target_where)
        return out

    return fields


def _add_standard_relationship_field(
    fields: dict[str, GraphQLField],
    rel: dict,
    table_lookup: dict[int, _TableInfo],
    gql_types: dict[int, GraphQLObjectType],
    where_types: dict[int, GraphQLInputObjectType | None],
    order_by_types: dict[int, GraphQLInputObjectType],
    distinct_enums: dict[int, GraphQLEnumType | None],
) -> str | None:
    target = table_lookup.get(rel["target_table_id"])
    if not target:
        return None
    target_type = gql_types[target.table_id]
    cardinality = rel["cardinality"]
    # REQ-1586: a junction-backed edge is named for its nomination, not for the table it reaches —
    # several such edges can share one target and would otherwise collapse onto one field.
    via_table_name = None
    if rel.get("via_table_id"):
        via = table_lookup.get(rel["via_table_id"])
        if not via:
            return None
        via_table_name = via.table_name
    field_name = relationship_field_name(
        cardinality,
        target_field_name=unprefixed_field_name(target.field_name, target.domain_id),
        graphql_alias=rel.get("graphql_alias"),
        via_label_source=rel.get("via_label_source"),
        via_type_value=rel.get("via_type_value"),
        via_table_name=via_table_name,
        cypher_alias=rel.get("alias"),
    )
    if cardinality in ("many-to-one", "one-to-one"):
        # REQ-1163: a related relation that has versions takes its time-travel arguments here too.
        travel: dict[str, GraphQLArgument] = {}
        _add_time_travel_args(target, travel)
        fields[field_name] = GraphQLField(
            target_type,
            args=travel or None,
            deprecation_reason=rel.get("deprecation_reason"),  # REQ-1375
        )
    elif cardinality == "one-to-many":
        many_args = _build_db_field_args(
            where_types.get(target.table_id),
            order_by_types.get(target.table_id),
            distinct_enums.get(target.table_id),
            target.hasura,
        )
        _add_time_travel_args(target, many_args)
        fields[field_name] = GraphQLField(
            GraphQLList(GraphQLNonNull(target_type)),
            args=many_args,
            deprecation_reason=rel.get("deprecation_reason"),  # REQ-1375
        )
    else:
        raise ValueError(f"unhandled relationship cardinality {cardinality!r} for {rel['id']!r}")
    return field_name


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
    if _embedding_columns(info):
        # REQ-430: the score of a ``similar`` search.
        fields["_similarity"] = GraphQLField(
            cast(GraphQLScalarType, GraphQLFloat),
            description="Cosine similarity to the field's similar argument",
        )

    if meta_rt is not None:
        _add_meta_field(fields, info, meta_rt, gql_types)

    if meta_rt is not None and tid == meta_rt.table_id:
        _add_ops_traversal_fields(
            fields, ops_traversal_targets, gql_types, where_types, order_by_types, distinct_enums
        )

    # REQ-1929: a relationship never hides a column -- one named for a column's field is refused.
    column_fields = set(info.gql_fields)
    # field -> (relationship id, computed?) for the relationships of this type.
    held: dict[str, tuple[str, bool]] = {}
    for rel in visible_rels:
        if rel["source_table_id"] != tid:
            continue
        fn_name = rel.get("target_function_name")
        if fn_name:
            name = _add_computed_relationship_field(
                fields, rel, fn_name, functions, tables, gql_types
            )
        else:
            name = _add_standard_relationship_field(
                fields, rel, table_lookup, gql_types, where_types, order_by_types, distinct_enums
            )
        if name is None:
            continue
        if name in column_fields:
            from provisa.compiler.context import RelationshipHidesColumn

            raise RelationshipHidesColumn(info.type_name, name, rel["id"])
        computed = bool(fn_name)
        holder = held.get(name)
        if holder is not None and (computed or holder[1]):
            # REQ-194: a computed relationship and another named for one field would leave one
            # replacing the other. Refused, naming both; a GraphQL alias on either resolves it.
            # (Two table relationships are refused where their joins register, context.py.)
            from provisa.compiler.context import RelationshipFieldTaken

            raise RelationshipFieldTaken(info.type_name, name, holder[0], rel["id"])
        held[name] = (rel["id"], computed)

    return fields


def _build_native_filter_args(
    t: _TableInfo,
    args: dict[str, GraphQLArgument],
) -> None:
    """Append native filter args to args in-place."""
    from provisa.core.parameters import PARAMETER_EXTENSION, is_required

    visible_col_names = [
        c["column_name"] for c in t.visible_columns if c["column_name"].lower() in t.column_metadata
    ]
    # A parameter takes the same argument name on the base, _aggregate and _group_by fields, so it
    # steps aside from every argument any of them has of its own.
    response_field_names = set(visible_col_names) | {
        "where",
        "order_by",
        "limit",
        "offset",
        "distinct_on",
        "by",
        "having",
    }
    for nfc in t.native_filter_columns:
        col_name = nfc["column_name"]
        bare_name = col_name[4:] if col_name.startswith("_nf_") else col_name
        # REQ-1986: the argument is the parameter's alias where it has one, as a column's field is
        # its alias; either is a name the parameter is bound under (core.parameters.bound_names).
        spelled = nfc.get("alias") or col_name
        given = spelled[4:] if spelled.startswith("_nf_") else spelled
        arg_name = f"_{given}" if given in response_field_names else given
        meta = t.column_metadata.get(col_name.lower())
        # An array parameter (a query vector) takes a list, as its column holds one.
        nfc_gql_type = array_graphql_type(meta.data_type) if meta else GraphQLString
        # REQ-1986: non-null where the source or a steward requires the parameter.
        args[arg_name] = GraphQLArgument(
            GraphQLNonNull(nfc_gql_type) if is_required(nfc) else nfc_gql_type,
            description=f"Native API filter ({nfc['native_filter_type']})",
            extensions={PARAMETER_EXTENSION: bare_name},
        )


def _agg_field_names(t: _TableInfo) -> dict[str, str]:
    """Each visible column's GraphQL field name on the row type, keyed by physical name: the
    names its aggregate, group-by and having fields carry too (REQ-196, REQ-197)."""
    return {c["column_name"]: _gql_col_name(c, t) for c in t.visible_columns}


def _build_aggregate_query_field(
    t: _TableInfo,
    gql_type: GraphQLObjectType,
    enum_types: dict,
    agg_fields_type: GraphQLObjectType | None = None,
    where_type: GraphQLInputObjectType | None = None,
) -> tuple[str, GraphQLField] | None:
    agg_type = build_aggregate_types(
        t.type_name,
        t.visible_columns,
        t.column_metadata,
        gql_type,
        _agg_field_names(t),
        agg_fields_type,
        hasura=t.hasura,
    )
    if not agg_type:
        return None
    agg_args: dict[str, GraphQLArgument] = {}
    # REQ-194: a Hasura-preset table's aggregate takes its one bool_exp.
    agg_where = (
        where_type
        if t.hasura
        else _build_where_input(t, f"{t.type_name}Agg", enum_types=enum_types)
    )
    if agg_where:
        agg_args["where"] = GraphQLArgument(agg_where)
    # REQ-1986, REQ-1991: an aggregate over a parameterized table takes its parameters.
    _build_native_filter_args(t, agg_args)
    # REQ-194, REQ-467: the variant's name is the authority's, as the names table holds it.
    agg_field_name = root_field_variant(
        t.field_name, "aggregate", t.gql_convention_override or active_gql_convention()
    )
    return agg_field_name, GraphQLField(agg_type, args=agg_args)


def _build_group_by_query_field(
    t: _TableInfo,
    gql_type: GraphQLObjectType,
    where_type: GraphQLInputObjectType | None,
    distinct_enum: GraphQLEnumType | None,
    enum_types: dict,
    agg_fields_type: GraphQLObjectType | None = None,
) -> tuple[str, GraphQLField] | None:
    """Build {field_name}_group_by root query field (REQ-654, REQ-655)."""
    by_enum = distinct_enum or _build_distinct_on_enum(t)
    if not by_enum:
        return None

    if agg_fields_type is None:
        agg_fields_type = build_agg_fields_type(
            t.type_name, t.visible_columns, t.column_metadata, _agg_field_names(t)
        )

    # REQ-655: aggregates field accepts where: arg for FILTER (WHERE ...) per function. REQ-194: a
    # Hasura-preset table's takes its one bool_exp.
    agg_where = (
        where_type
        if t.hasura
        else _build_where_input(t, f"{t.type_name}GroupByAgg", enum_types=enum_types)
    )
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
    having_exp = build_having_exp_type(
        t.type_name, t.visible_columns, t.column_metadata, _agg_field_names(t)
    )

    gb_args: dict[str, GraphQLArgument] = {
        "by": GraphQLArgument(GraphQLNonNull(GraphQLList(GraphQLNonNull(by_enum)))),
        "limit": GraphQLArgument(GraphQLInt),
        "offset": GraphQLArgument(GraphQLInt),
    }
    if where_type:
        gb_args["where"] = GraphQLArgument(where_type)
    # REQ-2007: a group-by is ordered by its keys and its aggregates, not by the table's columns.
    gb_order_by = build_group_by_order_by_type(
        t.type_name,
        t.visible_columns,
        t.column_metadata,
        _agg_field_names(t),
        list(by_enum.values),
    )
    gb_args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(gb_order_by)))
    if distinct_enum:
        gb_args["distinct_on"] = GraphQLArgument(GraphQLList(GraphQLNonNull(distinct_enum)))
    if having_exp:
        gb_args["having"] = GraphQLArgument(having_exp)
    # REQ-1986, REQ-1991: a group-by over a parameterized table takes its parameters.
    _build_native_filter_args(t, gb_args)

    gb_field_name = root_field_variant(
        t.field_name, "group_by", t.gql_convention_override or active_gql_convention()
    )

    return gb_field_name, GraphQLField(
        GraphQLNonNull(GraphQLList(GraphQLNonNull(group_by_row_type))),
        args=gb_args,
    )


def _build_connection_query_field(
    t: _TableInfo,
    gql_type: GraphQLObjectType,
    order_by_types: dict[int, GraphQLInputObjectType],
    enum_types: dict,
    where_type: GraphQLInputObjectType | None,
) -> tuple[str, GraphQLField]:
    _edge_type, conn_type = _build_connection_types(t.type_name, gql_type)
    conn_args: dict[str, GraphQLArgument] = {
        "first": GraphQLArgument(GraphQLInt),
        "after": GraphQLArgument(GraphQLString),
        "last": GraphQLArgument(GraphQLInt),
        "before": GraphQLArgument(GraphQLString),
    }
    # REQ-194: a Hasura-preset table's connection takes its one bool_exp.
    conn_where = (
        where_type
        if t.hasura
        else _build_where_input(t, f"{t.type_name}Conn", enum_types=enum_types)
    )
    if conn_where:
        conn_args["where"] = GraphQLArgument(conn_where)
    conn_ob = order_by_types.get(t.table_id)
    if conn_ob:
        conn_args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(conn_ob)))
    return root_field_variant(
        t.field_name, "connection", t.gql_convention_override or active_gql_convention()
    ), GraphQLField(conn_type, args=conn_args)


def _pk_input_fields(t: _TableInfo) -> dict[str, GraphQLScalarType | GraphQLEnumType]:
    """Table ``t``'s visible primary-key columns, by field name, with their input types."""
    return _column_input_fields(t, pk_only=True)


def _column_input_fields(
    t: _TableInfo, *, pk_only: bool
) -> dict[str, GraphQLScalarType | GraphQLEnumType]:
    """Table ``t``'s visible columns (only its primary key's when ``pk_only``) holding a scalar or
    an enum, by field name, with their input types."""
    out: dict[str, GraphQLScalarType | GraphQLEnumType] = {}
    for col in t.visible_columns:
        if pk_only and not col.get("is_primary_key"):
            continue
        meta = t.column_metadata.get(col["column_name"].lower())
        if meta is None:
            continue
        gql = registered_graphql_type(meta.data_type, meta.subtype)
        if isinstance(gql, (GraphQLScalarType, GraphQLEnumType)):
            out[_gql_col_name(col, t)] = gql
    return out


def _build_mutation_fields_for_table(  # REQ-032, REQ-033, REQ-034, REQ-036, REQ-037, REQ-212
    t: _TableInfo,
    enum_types: dict,
    row_type: GraphQLObjectType,
    where_type: GraphQLInputObjectType | None,
    unique_constraints: list[dict],
) -> dict[str, GraphQLField]:
    """Build insert/upsert/update/delete mutation fields for one table, for exactly the
    operations the table's source takes and the role holds a write grant for (REQ-2074), each
    input holding its grant's columns. Returns {} if none. A Hasura-preset table's are Hasura's
    (REQ-194)."""
    granted = t.write_ops & t.write_grants.keys()
    if not granted:
        return {}
    insert_fields = _write_inputs(t, "insert")
    update_fields = _write_inputs(t, "update")
    if t.hasura:
        return _build_hasura_mutation_fields(
            t, granted, insert_fields, update_fields, row_type, where_type, unique_constraints
        )
    return _build_provisa_mutation_fields(t, granted, insert_fields, update_fields, enum_types)


def _write_inputs(t: _TableInfo, operation: str) -> dict[str, GraphQLInputField]:
    """The input fields of ``operation``'s grant: each granted column under its published name."""
    allowed = set((t.write_grants.get(operation) or {}).get("columns") or [])
    fields: dict[str, GraphQLInputField] = {}
    for col in t.visible_columns:
        col_name = col["column_name"]
        meta = t.column_metadata.get(col_name.lower())
        if meta is None or col_name not in allowed:
            continue
        col_gql_type = registered_graphql_type(meta.data_type, meta.subtype)
        if isinstance(col_gql_type, GraphQLList):
            col_gql_type = GraphQLString  # fallback for arrays in input
        # Named as reads name it (its published name); mutation_gen maps it back.
        fields[_gql_col_name(col, t)] = GraphQLInputField(col_gql_type)
    return fields


def _build_hasura_mutation_fields(  # REQ-194
    t: _TableInfo,
    granted: set[str] | frozenset[str],
    insert_fields: dict[str, GraphQLInputField],
    update_fields: dict[str, GraphQLInputField],
    row_type: GraphQLObjectType,
    where_type: GraphQLInputObjectType | None,
    unique_constraints: list[dict],
) -> dict[str, GraphQLField]:
    """Hasura's mutations of a Hasura-preset table: ``insert_<t>`` (``objects``) and
    ``insert_<t>_one`` (``object``), each with ``on_conflict``; ``update_<t>`` (``_set``,
    ``where``) and ``update_<t>_by_pk`` (``_set``, ``pk_columns``); ``delete_<t>`` (``where``)
    and ``delete_<t>_by_pk`` (the key). Each for exactly the writes the role holds a grant for."""
    assert t.hasura is not None
    conv = t.hasura

    def arg(name: str) -> str:
        return hasura_argument_name(conv, name)

    response = hasura_schema.mutation_response(t, row_type)
    pk = _pk_input_fields(t)
    result: dict[str, GraphQLField] = {}
    if "insert" in granted and insert_fields:
        insert_input = cast(
            GraphQLInputObjectType,
            GraphQLInputObjectType(
                hasura_schema.type_name(t, "insert_input"), lambda f=insert_fields: f
            ),
        )
        insert_args = {arg("objects"): GraphQLArgument(_non_null_list(insert_input))}
        one_args = {arg("object"): GraphQLArgument(GraphQLNonNull(insert_input))}
        constraints = _hasura_constraints(t, pk, unique_constraints)
        if (
            "update" in granted
            and constraints
            and update_fields
            and "on_conflict" not in t.write_refused_forms
        ):
            on_conflict = _hasura_on_conflict(t, constraints, list(update_fields))
            insert_args[arg("on_conflict")] = GraphQLArgument(on_conflict)
            one_args[arg("on_conflict")] = GraphQLArgument(on_conflict)
        result[hasura_schema.root_field(t, prefix="insert")] = GraphQLField(
            response, args=insert_args
        )
        result[hasura_schema.root_field(t, prefix="insert", suffix="one")] = GraphQLField(
            row_type, args=one_args
        )
    if "update" in granted and update_fields:
        set_input = cast(
            GraphQLInputObjectType,
            GraphQLInputObjectType(
                hasura_schema.type_name(t, "set_input"), lambda f=update_fields: f
            ),
        )
        if where_type is not None:
            result[hasura_schema.root_field(t, prefix="update")] = GraphQLField(
                response,
                args={
                    arg("_set"): GraphQLArgument(set_input),
                    arg("where"): GraphQLArgument(GraphQLNonNull(where_type)),
                },
            )
        if pk:
            pk_input = cast(
                GraphQLInputObjectType,
                GraphQLInputObjectType(
                    hasura_schema.type_name(t, "pk_columns_input"),
                    lambda: {n: GraphQLInputField(GraphQLNonNull(g)) for n, g in pk.items()},
                ),
            )
            result[hasura_schema.root_field(t, prefix="update", suffix="by_pk")] = GraphQLField(
                row_type,
                args={
                    arg("_set"): GraphQLArgument(set_input),
                    arg("pk_columns"): GraphQLArgument(GraphQLNonNull(pk_input)),
                },
            )
    if "delete" in granted:
        if where_type is not None:
            result[hasura_schema.root_field(t, prefix="delete")] = GraphQLField(
                response, args={arg("where"): GraphQLArgument(GraphQLNonNull(where_type))}
            )
        if pk:
            result[hasura_schema.root_field(t, prefix="delete", suffix="by_pk")] = GraphQLField(
                row_type, args=hasura_schema.pk_arguments(pk)
            )
    return result


def _non_null_list(of: GraphQLInputObjectType) -> GraphQLNonNull:
    return GraphQLNonNull(GraphQLList(GraphQLNonNull(of)))


def _hasura_constraints(
    t: _TableInfo, pk: dict[str, GraphQLScalarType | GraphQLEnumType], unique: list[dict]
) -> list[str]:
    """The constraints an on_conflict may name: the primary key's, then each declared UNIQUE
    constraint whose columns the role sees."""
    visible = {c["column_name"] for c in t.visible_columns}
    names = [hasura_pk_constraint(t.table_name)] if pk else []
    names += [uc["name"] for uc in unique if all(c in visible for c in uc["columns"])]
    return names


def _hasura_on_conflict(
    t: _TableInfo, constraints: list[str], update_columns: list[str]
) -> GraphQLInputObjectType:
    """``<t>_on_conflict``: the constraint whose conflict it handles and the columns it updates
    then (none: the row is left as it is)."""
    conv = t.hasura
    assert conv is not None
    constraint = cast(
        GraphQLEnumType,
        GraphQLEnumType(t.hasura_constraint, {c: GraphQLEnumValue(c) for c in constraints}),
    )
    update_column = hasura_schema.column_enum(t, "update_column", update_columns)
    return cast(
        GraphQLInputObjectType,
        GraphQLInputObjectType(
            hasura_schema.type_name(t, "on_conflict"),
            lambda: {
                "constraint": GraphQLInputField(GraphQLNonNull(constraint)),
                hasura_argument_name(conv, "update_columns"): GraphQLInputField(
                    GraphQLNonNull(GraphQLList(GraphQLNonNull(update_column))),
                    default_value=[],
                ),
            },
        ),
    )


def _build_provisa_mutation_fields(
    t: _TableInfo,
    granted: set[str] | frozenset[str],
    insert_fields: dict[str, GraphQLInputField],
    update_fields: dict[str, GraphQLInputField],
    enum_types: dict,
) -> dict[str, GraphQLField]:
    """Provisa's mutations of a table: insert, upsert, update and delete, each by ``input``,
    ``set`` and ``where``."""
    insert_input = cast(
        GraphQLInputObjectType,
        GraphQLInputObjectType(f"{t.type_name}InsertInput", lambda fields=insert_fields: fields),
    )
    set_input = cast(
        GraphQLInputObjectType,
        GraphQLInputObjectType(f"{t.type_name}SetInput", lambda fields=update_fields: fields),
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
    # Exactly the writes the table's source can take (executor/write_capability.py); an upsert
    # is an insert that may update, so it needs both.
    if "insert" in granted and insert_fields:
        result[_mutation_name("insert", t.field_name, conv)] = GraphQLField(
            GraphQLNonNull(response_type),
            args={"input": GraphQLArgument(GraphQLNonNull(insert_input))},
        )
    if (
        {"insert", "update"} <= granted
        and insert_fields
        and "on_conflict" not in t.write_refused_forms
    ):
        result[_mutation_name("upsert", t.field_name, conv)] = GraphQLField(
            GraphQLNonNull(response_type),
            args={
                "input": GraphQLArgument(GraphQLNonNull(insert_input)),
                "on_conflict": GraphQLArgument(
                    GraphQLNonNull(GraphQLList(GraphQLNonNull(conflict_col_enum)))
                ),
            },
        )
    if where_input and "update" in granted and update_fields:
        result[_mutation_name("update", t.field_name, conv)] = GraphQLField(
            GraphQLNonNull(response_type),
            args={
                "set": GraphQLArgument(GraphQLNonNull(set_input)),
                "where": GraphQLArgument(GraphQLNonNull(where_input)),
            },
        )
    if where_input and "delete" in granted:
        result[_mutation_name("delete", t.field_name, conv)] = GraphQLField(
            GraphQLNonNull(response_type),
            args={"where": GraphQLArgument(GraphQLNonNull(where_input))},
        )
    return result


def build_table_path_map(si: SchemaInput, *, found: RoleTables | None = None) -> dict[str, dict]:
    """Return {gql_field_name: {table_id, schema_name, table_name, domain_id, table_description,
    domain_description}} for REST path routing and the root-field lookup (REQ-2128). ``found``:
    ``si``'s tables already found and named for the role's other parts (:func:`role_tables`)."""
    if found is not None:
        tables = found.served()
    else:
        tables = _build_visible_tables(si)
        _assign_names(
            tables,
            si.naming_rules,
            domain_prefix=si.domain_prefix,
            domain_alias_map=_build_domain_alias_map(si.domains),
        )
    if not tables:
        return {}
    domain_descs = {d["id"]: d.get("description") for d in si.domains}
    return {
        t.field_name: {
            "table_id": t.table_id,
            "schema_name": t.schema_name,
            "table_name": t.table_name,
            "domain_id": t.domain_id,
            "table_description": t.description,
            "domain_description": domain_descs.get(t.domain_id),
        }
        for t in tables
        # REQ-2163: a table SQL alone serves has no root field and no REST path.
        if (t.domain_id, t.table_name) not in SQL_ONLY_TABLES
    }


def generate_schema(
    si: SchemaInput,
    *,
    found: RoleTables | None = None,
) -> (
    GraphQLSchema
):  # REQ-007, REQ-008, REQ-021, REQ-133, REQ-134, REQ-196, REQ-197, REQ-213, REQ-218, REQ-253
    """Generate a graphql-core schema for a specific role.

    The schema includes:
    - Object types per registered table (filtered by domain access + column visibility)
    - Relationship fields (many-to-one → object, one-to-many → list)
    - Root query fields with where, order_by, limit, offset args

    ``found``: ``si``'s tables already found and named for the role's other parts
    (:func:`role_tables`).
    """
    # REQ-2154: a role served no table is not refused as such: its schema is empty but for the
    # catalog's resolve field every role's Query has (_add_resolve_field), so it is still valid.
    domain_alias_map = _build_domain_alias_map(si.domains)
    if found is not None:
        tables = found.served_copies()
    else:
        tables = _build_visible_tables(si)
        _assign_names(
            tables,
            si.naming_rules,
            domain_prefix=si.domain_prefix,
            domain_alias_map=domain_alias_map,
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
            masked=si.masked_columns,
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
    where_types: dict[int, GraphQLInputObjectType | None] = {}
    for t in tables:
        where_types[t.table_id] = _build_where_input(
            t,
            t.type_name,
            enum_types=si.enum_types,
            relationships=_where_relationships(t, visible_rels, table_lookup, where_types),
        )
    distinct_enums = {t.table_id: _build_distinct_on_enum(t) for t in tables}

    # A table's root-field arguments, built once: its query and its subscription field take the same
    # ones, and an input type among them (similar's) may be named only once in a schema (REQ-430).
    field_args: dict[int, dict[str, GraphQLArgument]] = {}

    def args_of(t: _TableInfo) -> dict[str, GraphQLArgument]:
        if t.table_id not in field_args:
            field_args[t.table_id] = _table_field_args(
                t, where_types, order_by_types, distinct_enums
            )
        return field_args[t.table_id]

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
            # REQ-1320: type description carries the modeling role/SCD suffix.
            GraphQLObjectType(t.type_name, make_fields, description=_type_description(t)),
        )

    # Build root query fields
    query_fields: dict[str, GraphQLField] = {}
    _root_ids = si.root_table_ids
    for t in tables:
        if _root_ids is not None and t.table_id not in _root_ids:
            continue
        # REQ-2163: a table SQL alone serves has no GraphQL field (security.rights).
        if (t.domain_id, t.table_name) in SQL_ONLY_TABLES:
            continue
        gql_type = gql_types[t.table_id]
        query_fields[t.field_name] = GraphQLField(
            GraphQLList(GraphQLNonNull(gql_type)),
            args=args_of(t),
            # REQ-1375: table-level deprecation surfaces on the root query field — the
            # GraphQL construct @deprecated is valid on (objects themselves cannot carry it).
            deprecation_reason=t.deprecation_reason,
        )
        if t.hasura:
            # REQ-194: Hasura's read of one row by its primary key.
            pk = _pk_input_fields(t)
            if pk:
                query_fields[hasura_schema.root_field(t, suffix="by_pk")] = (
                    hasura_schema.by_pk_field(gql_type, pk)
                )

        # Build agg_fields_type once to avoid duplicate GraphQL type names when both flags on.
        shared_agg_fields_type = None
        if t.enable_aggregates or t.enable_group_by:
            shared_agg_fields_type = build_agg_fields_type(
                t.type_name,
                t.visible_columns,
                t.column_metadata,
                _agg_field_names(t),
                metrics=t.metrics,  # REQ-1319: metrics block inside the aggregate result
                hasura=t.hasura,
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
                t, gql_type, si.enum_types, shared_agg_fields_type, where_types.get(t.table_id)
            )
            if agg_result:
                query_fields[agg_result[0]] = agg_result[1]

        # REQ-653/654: table-level enable_group_by gates the _group_by root field.
        if t.enable_group_by:
            gb_result = _build_group_by_query_field(
                t,
                gql_type,
                where_types.get(t.table_id),
                distinct_enums.get(t.table_id),
                si.enum_types,
                shared_agg_fields_type,
            )
            if gb_result:
                query_fields[gb_result[0]] = gb_result[1]

        if t.relay_pagination:
            conn_name, conn_field = _build_connection_query_field(
                t, gql_type, order_by_types, si.enum_types, where_types.get(t.table_id)
            )
            query_fields[conn_name] = conn_field

    query_type = cast(GraphQLObjectType, GraphQLObjectType("Query", lambda: query_fields))

    # Build mutation types for the tables whose sources take writes (REQ-031–REQ-037)
    mutation_fields: dict[str, GraphQLField] = {}
    # REQ-194: each table's declared UNIQUE constraints, which a Hasura on_conflict may name.
    declared_unique = {raw["id"]: raw.get("unique_constraints") or [] for raw in si.tables}

    for t in tables:
        # Mirror the query-root visibility gate: only seed/root tables get mutation fields.
        if _root_ids is not None and t.table_id not in _root_ids:
            continue
        # REQ-2163: a table SQL alone serves has no GraphQL field (security.rights).
        if (t.domain_id, t.table_name) in SQL_ONLY_TABLES:
            continue
        # REQ-1157: a view_sql/MV-backed relation is derived, not a base table. Writes either fail
        # at the source (non-updatable view) or land in the mv_cache snapshot the next refresh
        # overwrites — silent data loss. Expose it query-only; never generate insert/update/delete.
        if t.read_only or not t.write_ops:
            continue
        # A mutation field returns the rows it wrote; a table whose write route cannot return
        # them (executor/write_capability.py) is written through SQL and COPY, and offers none.
        if not t.write_returns_rows:
            continue
        mutation_fields.update(
            _build_mutation_fields_for_table(
                t,
                si.enum_types,
                gql_types[t.table_id],
                where_types.get(t.table_id),
                declared_unique.get(t.table_id, []),
            )
        )

    extra_query, extra_mutation = _build_action_fields(si, gql_types, tables, domain_alias_map)
    for root, fields, extra in (
        ("Query", query_fields, extra_query),
        ("Mutation", mutation_fields, extra_mutation),
    ):
        for field in extra.keys() & fields.keys():
            # A command named for a table's own field would replace it in the schema.
            command = extra[field].extensions["provisa_command"]
            raise CommandFieldTaken(root, field, command, "a table's field")
        fields.update(extra)
    # REQ-1365: the resolver is meta's field, so a per-domain schema has it only when meta is asked
    # for; meta filters by the Domains menu like any domain.
    if offers_domain(si, META_DOMAIN_ID):
        _add_resolve_field(query_fields, resolve_field_name(si.domain_prefix, domain_alias_map))

    mutation_type: GraphQLObjectType | None = None
    if mutation_fields:
        mutation_type = cast(
            GraphQLObjectType, GraphQLObjectType("Mutation", lambda: mutation_fields)
        )

    subscription_fields = _build_subscription_fields(
        si,
        tables,
        gql_types,
        args_of,
        where_types,
    )
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
