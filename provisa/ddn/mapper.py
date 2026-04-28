# Copyright (c) 2026 Kenneth Stott
# Canary: 58032d33-1cc1-4fb5-be01-83df6c011466
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Map DDN intermediate models to Provisa config objects."""

from __future__ import annotations

from typing import Any

from provisa.core.models import (
    Column,
    Domain,
    Function,
    FunctionArgument,
    GovernanceLevel,
    NamingConfig,
    ProvisaConfig,
    Relationship,
    RLSRule,
    Role,
    Source,
    SourceType,
    Table,
)
from provisa.ddn.models import (
    DDNAggregateExpression,
    DDNCommand,
    DDNConnector,
    DDNMetadata,
    DDNModel,
    DDNModelPermission,
    DDNObjectType,
    DDNRelationship,
    DDNTypePermission,
)
from provisa.import_shared.warnings import WarningCollector


def _source_type_from_url(url: str) -> SourceType:
    """Infer source type from connector URL."""
    lower = url.lower()
    if "postgres" in lower or "pg" in lower:
        return SourceType.postgresql
    if "mysql" in lower:
        return SourceType.mysql
    if "mssql" in lower or "sqlserver" in lower:
        return SourceType.sqlserver
    if "mongo" in lower:
        return SourceType.mongodb
    if "clickhouse" in lower:
        return SourceType.clickhouse
    if "snowflake" in lower:
        return SourceType.snowflake
    if "bigquery" in lower:
        return SourceType.bigquery
    return SourceType.postgresql


def _safe_id(name: str) -> str:
    """Sanitize a name into a valid Provisa ID."""
    return name.replace(" ", "_").replace(".", "_").replace("/", "_")


def _map_connectors(
    connectors: list[DDNConnector],
    source_overrides: dict[str, Any] | None,
) -> list[Source]:
    """Map DDN connectors to Provisa sources."""
    sources: list[Source] = []
    overrides = source_overrides or {}
    for conn in connectors:
        sid = _safe_id(conn.name)
        stype = _source_type_from_url(conn.url)

        defaults: dict[str, Any] = {
            "id": sid,
            "type": stype,
            "host": "localhost",
            "port": 5432,
            "database": "default",
            "username": "postgres",
            "password": "${env:DB_PASSWORD}",
        }
        if sid in overrides:
            defaults.update(overrides[sid])

        sources.append(Source(**defaults))
    return sources


def _build_field_to_column_map(
    object_types: list[DDNObjectType],
) -> dict[str, dict[str, str]]:
    """Build mapping: object_type_name -> {graphql_field -> physical_column}.

    Uses dataConnectorTypeMapping[].fieldMapping from ObjectType.
    """
    result: dict[str, dict[str, str]] = {}
    for ot in object_types:
        field_map: dict[str, str] = {}
        for tm in ot.type_mappings:
            for fm in tm.field_mappings:
                field_map[fm.graphql_field] = fm.column
        # For fields not in mapping, field name = column name
        for fname in ot.fields:
            if fname not in field_map:
                field_map[fname] = fname
        result[ot.name] = field_map
    return result


def _resolve_column(
    graphql_field: str,
    field_col_map: dict[str, str],
) -> str:
    """Resolve a GraphQL field name to its physical column name."""
    return field_col_map.get(graphql_field, graphql_field)


def _build_type_perms_index(
    type_perms: list[DDNTypePermission],
) -> dict[str, list[DDNTypePermission]]:
    """Index type permissions by type name."""
    idx: dict[str, list[DDNTypePermission]] = {}
    for tp in type_perms:
        idx.setdefault(tp.type_name, []).append(tp)
    return idx


def _build_model_perms_index(
    model_perms: list[DDNModelPermission],
) -> dict[str, list[DDNModelPermission]]:
    """Index model permissions by model name."""
    idx: dict[str, list[DDNModelPermission]] = {}
    for mp in model_perms:
        idx.setdefault(mp.model_name, []).append(mp)
    return idx


def _build_object_type_index(
    object_types: list[DDNObjectType],
) -> dict[str, DDNObjectType]:
    return {ot.name: ot for ot in object_types}


def _build_model_index(models: list[DDNModel]) -> dict[str, DDNModel]:
    return {m.name: m for m in models}


def _model_table_id(model: DDNModel, connector_name: str) -> str:
    """Build a table_id for a model."""
    source_id = _safe_id(connector_name or model.connector_name)
    collection = model.collection or model.name
    return f"{source_id}.public.{collection}"


def _map_model_to_table(
    model: DDNModel,
    ot: DDNObjectType,
    field_col_maps: dict[str, dict[str, str]],
    type_perms_idx: dict[str, list[DDNTypePermission]],
    governance: GovernanceLevel,
    domain_map: dict[str, str],
) -> Table:
    """Map a DDN Model + ObjectType to a Provisa Table."""
    field_col_map = field_col_maps.get(ot.name, {})
    source_id = _safe_id(model.connector_name)
    collection = model.collection or model.name

    # Determine domain from subgraph
    domain_id = domain_map.get(model.subgraph, model.subgraph or "default")

    # Build columns from ObjectType fields
    type_perms = type_perms_idx.get(ot.name, [])
    role_fields: dict[str, set[str]] = {}
    for tp in type_perms:
        role_fields.setdefault(tp.role, set()).update(tp.allowed_fields)

    columns: list[Column] = []
    for gql_field, gql_type in ot.fields.items():
        physical_col = _resolve_column(gql_field, field_col_map)

        # Determine which roles can see this column
        visible_to: list[str] = []
        for role, allowed in role_fields.items():
            if gql_field in allowed:
                visible_to.append(role)

        col = Column(
            name=physical_col,
            visible_to=sorted(visible_to),
        )
        if gql_field != physical_col:
            col.alias = gql_field
        columns.append(col)

    alias = model.graphql_type_name or None

    return Table(
        source_id=source_id,
        domain_id=domain_id,
        schema_name="public",
        table_name=collection,
        governance=governance,
        columns=columns,
        alias=alias,
    )


def _map_rls_rules(
    model_perms_idx: dict[str, list[DDNModelPermission]],
    model_index: dict[str, DDNModel],
    field_col_maps: dict[str, dict[str, str]],
    ot_index: dict[str, DDNObjectType],
) -> list[RLSRule]:
    """Map DDN ModelPermissions to Provisa RLS rules."""
    rules: list[RLSRule] = []
    for model_name, perms in model_perms_idx.items():
        model = model_index.get(model_name)
        if not model:
            continue
        table_id = _model_table_id(model, model.connector_name)
        field_col_map = field_col_maps.get(model.object_type, {})

        for mp in perms:
            if not mp.filter:
                continue
            sql_filter = _ddn_filter_to_sql(mp.filter, field_col_map)
            if sql_filter and sql_filter != "TRUE":
                rules.append(RLSRule(
                    table_id=table_id,
                    role_id=mp.role,
                    filter=sql_filter,
                ))
    return rules


def _ddn_filter_to_sql(
    flt: dict[str, Any], field_col_map: dict[str, str],
) -> str:
    """Convert a DDN permission filter to a SQL WHERE clause.

    DDN filters use field-level predicates like:
    {"fieldName": {"_eq": "value"}} or with session vars.
    """
    if not flt:
        return "TRUE"
    parts: list[str] = []
    for key, value in flt.items():
        if key == "_and" and isinstance(value, list):
            sub = [_ddn_filter_to_sql(v, field_col_map) for v in value]
            parts.append("(" + " AND ".join(sub) + ")")
        elif key == "_or" and isinstance(value, list):
            sub = [_ddn_filter_to_sql(v, field_col_map) for v in value]
            parts.append("(" + " OR ".join(sub) + ")")
        elif key == "_not" and isinstance(value, dict):
            inner = _ddn_filter_to_sql(value, field_col_map)
            parts.append(f"NOT ({inner})")
        elif isinstance(value, dict):
            col = _resolve_column(key, field_col_map)
            for op, operand in value.items():
                sql_part = _ddn_op_to_sql(col, op, operand)
                if sql_part:
                    parts.append(sql_part)
    if not parts:
        return "TRUE"
    return " AND ".join(parts) if len(parts) > 1 else parts[0]


_DDN_OPS: dict[str, str] = {
    "_eq": "=",
    "_neq": "!=",
    "_gt": ">",
    "_lt": "<",
    "_gte": ">=",
    "_lte": "<=",
    "_in": "IN",
    "_nin": "NOT IN",
    "_like": "LIKE",
    "_is_null": "IS NULL",
}


def _ddn_op_to_sql(col: str, op: str, operand: Any) -> str:
    """Convert a single DDN filter operation to SQL."""
    if op == "_is_null":
        return f"{col} IS NULL" if operand else f"{col} IS NOT NULL"
    sql_op = _DDN_OPS.get(op)
    if not sql_op:
        return f"{col} /* unsupported op: {op} */"
    # Session variable reference
    if isinstance(operand, dict) and len(operand) == 1:
        key = next(iter(operand))
        if key.startswith("x-hasura-") or key.startswith("X-Hasura-"):
            return f"{col} {sql_op} ${{{key}}}"
    if isinstance(operand, str):
        return f"{col} {sql_op} '{operand}'"
    if isinstance(operand, list):
        items = ", ".join(
            f"'{v}'" if isinstance(v, str) else str(v) for v in operand
        )
        return f"{col} {sql_op} ({items})"
    if operand is None:
        return f"{col} {sql_op} NULL"
    return f"{col} {sql_op} {operand}"


def _map_relationships(
    ddn_rels: list[DDNRelationship],
    model_index: dict[str, DDNModel],
    ot_index: dict[str, DDNObjectType],
    field_col_maps: dict[str, dict[str, str]],
) -> list[Relationship]:
    """Map DDN relationships to Provisa relationships."""
    result: list[Relationship] = []
    for rel in ddn_rels:
        # Find source model from source_type (ObjectType name)
        source_model = _find_model_for_type(rel.source_type, model_index)
        target_model = model_index.get(rel.target_model)
        if not source_model or not target_model:
            continue

        source_tid = _model_table_id(source_model, source_model.connector_name)
        target_tid = _model_table_id(target_model, target_model.connector_name)

        # Resolve field names to physical columns
        src_field_map = field_col_maps.get(rel.source_type, {})
        tgt_ot = ot_index.get(target_model.object_type, None)
        tgt_field_map = field_col_maps.get(target_model.object_type, {}) if tgt_ot else {}

        if not rel.field_mapping:
            continue

        src_gql = next(iter(rel.field_mapping.keys()))
        tgt_gql = next(iter(rel.field_mapping.values()))
        src_col = _resolve_column(src_gql, src_field_map)
        tgt_col = _resolve_column(tgt_gql, tgt_field_map)

        cardinality = "many-to-one" if rel.rel_type == "Object" else "one-to-many"
        rel_id = f"{source_tid}.{rel.name}"

        result.append(Relationship(
            id=rel_id,
            source_table_id=source_tid,
            target_table_id=target_tid,
            source_column=src_col,
            target_column=tgt_col,
            cardinality=cardinality,
        ))
    return result


def _find_model_for_type(
    type_name: str, model_index: dict[str, DDNModel],
) -> DDNModel | None:
    """Find the model that uses the given ObjectType."""
    for m in model_index.values():
        if m.object_type == type_name:
            return m
    return None


def _collect_roles(
    type_perms: list[DDNTypePermission],
    model_perms: list[DDNModelPermission],
) -> list[Role]:
    """Collect all unique roles from permissions."""
    role_ids: set[str] = set()
    for tp in type_perms:
        if tp.role:
            role_ids.add(tp.role)
    for mp in model_perms:
        if mp.role:
            role_ids.add(mp.role)
    return [
        Role(id=rid, capabilities=["read"], domain_access=["*"])
        for rid in sorted(role_ids)
    ]


def _map_commands(
    commands: list[DDNCommand],
    model_index: dict[str, DDNModel],
) -> list[Function]:
    """Map DDN Commands to Provisa Functions."""
    functions: list[Function] = []
    for cmd in commands:
        source_id = _safe_id(cmd.connector_name)
        args = [
            FunctionArgument(name=aname, type=atype)
            for aname, atype in cmd.arguments.items()
        ]
        fn_name = cmd.graphql_root_field or cmd.name
        desc = f"DDN {cmd.command_type}" if cmd.command_type else None
        fn_kind = "query" if cmd.command_type == "query" else "mutation"

        functions.append(Function(
            name=fn_name,
            source_id=source_id,
            schema_name="public",
            function_name=cmd.source_name or cmd.name,
            returns=cmd.return_type or "void",
            arguments=args,
            domain_id=cmd.subgraph or "default",
            description=desc,
            kind=fn_kind,
        ))
    return functions


def _map_aggregate_expressions(
    agg_exprs: list[DDNAggregateExpression],
) -> dict[str, dict[str, Any]]:
    """Map DDN AggregateExpressions to aggregate config dicts.

    Returns: {object_type_name: {field: [functions], count: bool, ...}}
    """
    result: dict[str, dict[str, Any]] = {}
    for agg in agg_exprs:
        entry: dict[str, Any] = {
            "count": agg.count_enabled,
            "count_distinct": agg.count_distinct,
            "fields": dict(agg.aggregatable_fields),
        }
        result[agg.operand_type or agg.name] = entry
    return result


def convert_hml(
    metadata: DDNMetadata,
    collector: WarningCollector | None = None,
    governance_default: GovernanceLevel = GovernanceLevel.pre_approved,
    domain_map: dict[str, str] | None = None,
    source_overrides: dict[str, Any] | None = None,
) -> ProvisaConfig:
    """Convert DDN HML metadata to a ProvisaConfig.

    Args:
        metadata: Parsed DDN metadata.
        collector: Warning collector for unsupported features.
        governance_default: Default governance level for tables.
        domain_map: Optional subgraph->domain mapping.
        source_overrides: Optional per-source connection overrides.
    """
    if collector is None:
        collector = WarningCollector()
    domain_map = domain_map or {}
    source_overrides = source_overrides or {}

    # Build indices
    field_col_maps = _build_field_to_column_map(metadata.object_types)
    type_perms_idx = _build_type_perms_index(metadata.type_permissions)
    model_perms_idx = _build_model_perms_index(metadata.model_permissions)
    ot_index = _build_object_type_index(metadata.object_types)
    model_index = _build_model_index(metadata.models)

    # Sources from connectors
    sources = _map_connectors(metadata.connectors, source_overrides)

    # Domains from subgraphs (non-globals)
    domain_ids = set()
    for sg in metadata.subgraphs:
        did = domain_map.get(sg, sg)
        domain_ids.add(did)
    domain_ids.add("default")
    domains = [Domain(id=did) for did in sorted(domain_ids)]

    # Tables from Models + ObjectTypes
    tables: list[Table] = []
    for model in metadata.models:
        ot = ot_index.get(model.object_type)
        if not ot:
            collector.warn(
                "missing_type",
                f"Model '{model.name}' references unknown ObjectType "
                f"'{model.object_type}'",
            )
            continue
        table = _map_model_to_table(
            model, ot, field_col_maps, type_perms_idx,
            governance_default, domain_map,
        )
        tables.append(table)

    # RLS rules
    rls_rules = _map_rls_rules(
        model_perms_idx, model_index, field_col_maps, ot_index,
    )

    # Relationships
    relationships = _map_relationships(
        metadata.relationships, model_index, ot_index, field_col_maps,
    )

    # Roles
    roles = _collect_roles(metadata.type_permissions, metadata.model_permissions)

    # Functions from commands
    functions = _map_commands(metadata.commands, model_index)

    # Aggregate expressions (stored as metadata on tables, not a first-class
    # Provisa config field — attach as description annotations)
    agg_config = _map_aggregate_expressions(metadata.aggregate_expressions)
    for table in tables:
        model = _find_model_by_collection(table.table_name, metadata.models)
        if model and model.aggregate_expression:
            agg = agg_config.get(
                model.object_type, agg_config.get(model.aggregate_expression),
            )
            if agg:
                agg_desc = _format_agg_description(agg)
                if table.description:
                    table.description += f"\n{agg_desc}"
                else:
                    table.description = agg_desc

    # Emit warnings for skipped kinds
    for kind, count in metadata.skipped_kinds.items():
        if kind not in {"BooleanExpressionType", "AuthConfig"}:
            collector.warn(
                "skipped_kind",
                f"Skipped {count} {kind} document(s)",
            )

    return ProvisaConfig(
        sources=sources,
        domains=domains,
        naming=NamingConfig(),
        tables=tables,
        relationships=relationships,
        roles=roles,
        rls_rules=rls_rules,
        functions=functions,
    )


def _find_model_by_collection(
    collection: str, models: list[DDNModel],
) -> DDNModel | None:
    for m in models:
        if m.collection == collection or m.name == collection:
            return m
    return None


def _format_agg_description(agg: dict[str, Any]) -> str:
    """Format aggregate config as a description annotation."""
    parts = []
    if agg.get("count"):
        parts.append("count")
    if agg.get("count_distinct"):
        parts.append("count_distinct")
    fields = agg.get("fields", {})
    for fname, fns in fields.items():
        if fns:
            parts.append(f"{fname}({','.join(fns)})")
    return f"[aggregates: {', '.join(parts)}]" if parts else ""
