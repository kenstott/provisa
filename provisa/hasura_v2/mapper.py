# Copyright (c) 2026 Kenneth Stott
# Canary: 356f1b55-f2d0-472d-a664-0a9062edff7d
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Map Hasura v2 intermediate models to Provisa config objects."""

from __future__ import annotations

from typing import Any

from provisa.core.models import (
    AuthConfig,
    Column,
    EventTrigger,
    Function,
    FunctionArgument,
    GovernanceLevel,
    NamingConfig,
    ProvisaConfig,
    Relationship,
    RLSRule,
    Role,
    ScheduledTrigger,
    Source,
    SourceType,
    Table,
    Webhook,
)
from provisa.hasura_v2.models import (
    HasuraAction,
    HasuraMetadata,
    HasuraSource,
    HasuraTable,
)
from provisa.import_shared.filters import bool_expr_to_sql
from provisa.import_shared.warnings import WarningCollector


_HASURA_KIND_TO_SOURCE_TYPE: dict[str, str] = {
    "postgres": "postgresql",
    "pg": "postgresql",
    "mysql": "mysql",
    "mssql": "sqlserver",
    "bigquery": "bigquery",
    "citus": "postgresql",
}


def _source_type_from_kind(kind: str) -> SourceType:
    mapped = _HASURA_KIND_TO_SOURCE_TYPE.get(kind.lower(), "postgresql")
    return SourceType(mapped)


def _extract_connection_info(
    conn: dict[str, Any],
) -> dict[str, Any]:
    """Extract host/port/database from Hasura connection_info."""
    db_url = conn.get("database_url", "")
    if isinstance(db_url, dict):
        db_url = db_url.get("from_env", "")

    result: dict[str, Any] = {
        "host": "localhost",
        "port": 5432,
        "database": "default",
        "username": "postgres",
        "password": "${env:DB_PASSWORD}",
    }

    if isinstance(db_url, str) and "://" in db_url:
        # Parse postgres://user:pass@host:port/dbname
        try:
            rest = db_url.split("://", 1)[1]
            if "@" in rest:
                creds, hostpart = rest.rsplit("@", 1)
                if ":" in creds:
                    result["username"] = creds.split(":", 1)[0]
                    result["password"] = creds.split(":", 1)[1]
                else:
                    result["username"] = creds
            else:
                hostpart = rest
            if "/" in hostpart:
                hp, result["database"] = hostpart.split("/", 1)
            else:
                hp = hostpart
            if ":" in hp:
                result["host"], port_str = hp.split(":", 1)
                result["port"] = int(port_str)
            else:
                result["host"] = hp
        except (ValueError, IndexError):
            pass

    pool = conn.get("pool_settings", {})
    result["pool_min"] = pool.get("min_connections", 1)
    result["pool_max"] = pool.get("max_connections", 5)
    return result


def _map_source(hs: HasuraSource) -> Source:
    conn = _extract_connection_info(hs.connection_info)
    return Source(
        id=hs.name,
        type=_source_type_from_kind(hs.kind),
        host=conn["host"],
        port=conn["port"],
        database=conn["database"],
        username=conn["username"],
        password=conn["password"],
        pool_min=conn.get("pool_min", 1),
        pool_max=conn.get("pool_max", 5),
    )


def _collect_roles(metadata: HasuraMetadata) -> dict[str, Role]:
    """Collect all roles mentioned across permissions."""
    roles: dict[str, Role] = {}

    for source in metadata.sources:
        for table in source.tables:
            for perms in (
                table.select_permissions,
                table.insert_permissions,
                table.update_permissions,
                table.delete_permissions,
            ):
                for p in perms:
                    if p.role not in roles:
                        roles[p.role] = Role(
                            id=p.role,
                            capabilities=["read"],
                            domain_access=["*"],
                        )

    for action in metadata.actions:
        for perm in action.permissions:
            role_id = perm.get("role", "")
            if role_id and role_id not in roles:
                roles[role_id] = Role(
                    id=role_id,
                    capabilities=["read"],
                    domain_access=["*"],
                )

    # Inherited roles
    for ir in metadata.inherited_roles:
        if ir.role_name not in roles:
            roles[ir.role_name] = Role(
                id=ir.role_name,
                capabilities=["read"],
                domain_access=["*"],
            )
        # Set parent to first role in set (simplified mapping)
        if ir.role_set:
            roles[ir.role_name].parent_role_id = ir.role_set[0]
            # Ensure child roles exist
            for child_role in ir.role_set:
                if child_role not in roles:
                    roles[child_role] = Role(
                        id=child_role,
                        capabilities=["read"],
                        domain_access=["*"],
                    )

    # Upgrade capabilities based on permission types
    for source in metadata.sources:
        for table in source.tables:
            for p in table.insert_permissions:
                if p.role in roles and "write" not in roles[p.role].capabilities:
                    roles[p.role].capabilities.append("write")
            for p in table.update_permissions:
                if p.role in roles and "write" not in roles[p.role].capabilities:
                    roles[p.role].capabilities.append("write")
            for p in table.delete_permissions:
                if p.role in roles and "write" not in roles[p.role].capabilities:
                    roles[p.role].capabilities.append("write")

    return roles


def _table_id(source_name: str, schema: str, table_name: str) -> str:
    return f"{source_name}.{schema}.{table_name}"


def _map_table(
    ht: HasuraTable,
    source_name: str,
    governance: GovernanceLevel,
    collector: WarningCollector,
) -> tuple[Table, list[RLSRule], list[Relationship], list[Function]]:
    """Map a Hasura table to Provisa Table + side-effects."""
    tid = _table_id(source_name, ht.schema_name, ht.name)

    # Build columns from select permissions
    all_columns: dict[str, Column] = {}
    for perm in ht.select_permissions:
        cols = perm.columns
        if cols == "*" or (isinstance(cols, list) and "*" in cols):
            # Wildcard — we'll use a placeholder
            if "*" not in all_columns:
                all_columns["*"] = Column(
                    name="*", visible_to=[], writable_by=[],
                )
            all_columns["*"].visible_to.append(perm.role)
        elif isinstance(cols, list):
            for col_name in cols:
                if col_name not in all_columns:
                    all_columns[col_name] = Column(
                        name=col_name, visible_to=[], writable_by=[],
                    )
                all_columns[col_name].visible_to.append(perm.role)

    # Writable columns from insert/update permissions
    for perm in ht.insert_permissions:
        cols = perm.columns
        if isinstance(cols, list):
            for col_name in cols:
                if col_name not in all_columns:
                    all_columns[col_name] = Column(
                        name=col_name, visible_to=[], writable_by=[],
                    )
                all_columns[col_name].writable_by.append(perm.role)
    for perm in ht.update_permissions:
        cols = perm.columns
        if isinstance(cols, list):
            for col_name in cols:
                if col_name not in all_columns:
                    all_columns[col_name] = Column(
                        name=col_name, visible_to=[], writable_by=[],
                    )
                all_columns[col_name].writable_by.append(perm.role)

    # Apply custom column names as aliases
    for orig_name, alias in ht.custom_column_names.items():
        if orig_name in all_columns:
            all_columns[orig_name].alias = alias

    columns = list(all_columns.values())

    # Table alias from custom_root_fields
    table_alias = (
        ht.custom_root_fields.get("select")
        or ht.custom_root_fields.get("select_by_pk")
        or ht.custom_name
    )

    table = Table(
        source_id=source_name,
        domain_id="default",
        schema_name=ht.schema_name,
        table_name=ht.name,
        governance=governance,
        columns=columns,
        alias=table_alias,
    )

    # RLS rules from select permission filters
    rls_rules: list[RLSRule] = []
    for perm in ht.select_permissions:
        if perm.filter:
            sql_filter = bool_expr_to_sql(perm.filter)
            if sql_filter != "TRUE":
                rls_rules.append(RLSRule(
                    table_id=tid,
                    role_id=perm.role,
                    filter=sql_filter,
                ))

    # Relationships
    relationships: list[Relationship] = []
    for rel in ht.object_relationships:
        if not rel.column_mapping:
            continue
        src_col = next(iter(rel.column_mapping.keys()))
        tgt_col = next(iter(rel.column_mapping.values()))
        target_tid = _table_id(source_name, rel.remote_schema, rel.remote_table)
        relationships.append(Relationship(
            id=f"{tid}.{rel.name}",
            source_table_id=tid,
            target_table_id=target_tid,
            source_column=src_col,
            target_column=tgt_col,
            cardinality="many-to-one",
        ))
    for rel in ht.array_relationships:
        if not rel.column_mapping:
            continue
        src_col = next(iter(rel.column_mapping.keys()))
        tgt_col = next(iter(rel.column_mapping.values()))
        target_tid = _table_id(source_name, rel.remote_schema, rel.remote_table)
        relationships.append(Relationship(
            id=f"{tid}.{rel.name}",
            source_table_id=tid,
            target_table_id=target_tid,
            source_column=src_col,
            target_column=tgt_col,
            cardinality="one-to-many",
        ))

    # Computed fields -> functions
    functions: list[Function] = []
    for cf in ht.computed_fields:
        functions.append(Function(
            name=cf.name,
            source_id=source_name,
            schema_name=cf.function_schema,
            function_name=cf.function_name,
            returns=tid,
            exposed_as="query",
            domain_id="default",
        ))

    # Event triggers -> warnings
    for et in ht.event_triggers:
        collector.warn(
            "event_triggers",
            f"Event trigger '{et.name}' on {et.table_schema}.{et.table_name} "
            f"converted with limited fidelity (webhook: {et.webhook})",
        )

    return table, rls_rules, relationships, functions


def _map_action(
    action: HasuraAction, collector: WarningCollector
) -> Function | Webhook | None:
    """Map a Hasura action to either a Function or Webhook."""
    handler = action.definition.handler
    visible_to = [p.get("role", "") for p in action.permissions if p.get("role")]

    if handler.startswith("http://") or handler.startswith("https://"):
        # Webhook-backed action
        args = []
        for arg in action.definition.arguments:
            args.append(FunctionArgument(
                name=arg.get("name", ""),
                type=arg.get("type", "String"),
            ))
        return Webhook(
            name=action.name,
            url=handler,
            method="POST",
            timeout_ms=5000,
            arguments=args,
            visible_to=visible_to,
            domain_id="default",
        )

    # DB-backed action — treat as function
    collector.warn(
        "actions",
        f"Action '{action.name}' with non-HTTP handler '{handler}' "
        "mapped as function placeholder",
    )
    return Function(
        name=action.name,
        source_id="default",
        schema_name="public",
        function_name=action.name,
        returns="void",
        visible_to=visible_to,
        domain_id="default",
    )


def convert_metadata(
    metadata: HasuraMetadata,
    collector: WarningCollector | None = None,
    governance_default: GovernanceLevel = GovernanceLevel.pre_approved,
    domain_map: dict[str, str] | None = None,
    auth_env: dict[str, str] | None = None,
    source_overrides: dict[str, Any] | None = None,
) -> ProvisaConfig:
    """Convert Hasura v2 metadata to a ProvisaConfig.

    Args:
        metadata: Parsed Hasura v2 metadata.
        collector: Warning collector for unsupported features.
        governance_default: Default governance level for tables.
        domain_map: Optional schema->domain mapping.
        auth_env: Optional auth environment variables.
        source_overrides: Optional per-source connection overrides.
    """
    if collector is None:
        collector = WarningCollector()

    domain_map = domain_map or {}
    source_overrides = source_overrides or {}

    # Sources
    sources: list[Source] = []
    for hs in metadata.sources:
        src = _map_source(hs)
        # Apply overrides
        if hs.name in source_overrides:
            overrides = source_overrides[hs.name]
            for k, v in overrides.items():
                if hasattr(src, k):
                    object.__setattr__(src, k, v)
        sources.append(src)

    # Roles
    roles_dict = _collect_roles(metadata)
    roles = sorted(roles_dict.values(), key=lambda r: r.id)

    # Tables, RLS, Relationships, Functions
    tables: list[Table] = []
    all_rls: list[RLSRule] = []
    all_rels: list[Relationship] = []
    all_functions: list[Function] = []
    all_event_triggers: list[EventTrigger] = []

    for hs in metadata.sources:
        for ht in hs.tables:
            table, rls_rules, rels, fns = _map_table(
                ht, hs.name, governance_default, collector,
            )
            # Apply domain mapping
            dm_key = f"{ht.schema_name}"
            if dm_key in domain_map:
                table.domain_id = domain_map[dm_key]
            tables.append(table)
            all_rls.extend(rls_rules)
            all_rels.extend(rels)
            all_functions.extend(fns)

            # Event triggers
            for et in ht.event_triggers:
                all_event_triggers.append(EventTrigger(
                    table_id=_table_id(hs.name, ht.schema_name, ht.name),
                    operations=et.operations,
                    webhook_url=et.webhook,
                    retry_max=et.retry_conf.get("num_retries", 3),
                    retry_delay=et.retry_conf.get("interval_sec", 1.0),
                ))

        # Tracked functions
        for hf in hs.functions:
            all_functions.append(Function(
                name=hf.name,
                source_id=hs.name,
                schema_name=hf.schema_name,
                function_name=hf.name,
                returns="void",
                domain_id="default",
            ))

    # Actions -> Functions or Webhooks
    webhooks: list[Webhook] = []
    for action in metadata.actions:
        result = _map_action(action, collector)
        if isinstance(result, Webhook):
            webhooks.append(result)
        elif isinstance(result, Function):
            all_functions.append(result)

    # Cron triggers -> Scheduled triggers
    scheduled: list[ScheduledTrigger] = []
    for ct in metadata.cron_triggers:
        scheduled.append(ScheduledTrigger(
            id=ct.name,
            cron=ct.schedule,
            url=ct.webhook,
            enabled=ct.enabled,
        ))

    # Remote schemas -> warnings (already emitted in parser)

    # Auth config from env
    auth = AuthConfig()
    if auth_env:
        provider = auth_env.get("AUTH_PROVIDER", "none")
        auth.provider = provider
        if provider == "firebase":
            auth.firebase = {
                "project_id": auth_env.get("FIREBASE_PROJECT_ID", ""),
            }
        elif provider == "keycloak":
            auth.keycloak = {
                "url": auth_env.get("KEYCLOAK_URL", ""),
                "realm": auth_env.get("KEYCLOAK_REALM", ""),
            }

    # Naming config — read enable_relay from graphql_engine config
    enable_relay = bool(metadata.graphql_engine.get("enable_relay", False))
    naming = NamingConfig(relay_pagination=enable_relay)

    # Domains — collect unique domain_ids
    domain_ids = {t.domain_id for t in tables} | {"default"}
    from provisa.core.models import Domain
    domains = [Domain(id=did) for did in sorted(domain_ids)]

    return ProvisaConfig(
        sources=sources,
        domains=domains,
        naming=naming,
        tables=tables,
        relationships=all_rels,
        roles=roles,
        rls_rules=all_rls,
        event_triggers=all_event_triggers,
        scheduled_triggers=scheduled,
        functions=all_functions,
        webhooks=webhooks,
        auth=auth,
    )
