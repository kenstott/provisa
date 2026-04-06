# Copyright (c) 2026 Kenneth Stott
# Canary: 170a46bc-f0d1-4978-bf08-ad3de09f1eea
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Parse Hasura v2 metadata directory into intermediate models."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from provisa.hasura_v2.models import (
    HasuraAction,
    HasuraActionDefinition,
    HasuraComputedField,
    HasuraCronTrigger,
    HasuraEventTrigger,
    HasuraFunction,
    HasuraInheritedRole,
    HasuraMetadata,
    HasuraPermission,
    HasuraRelationship,
    HasuraRemoteSchema,
    HasuraSource,
    HasuraTable,
)
from provisa.import_shared.warnings import WarningCollector


def _load_yaml(path: Path) -> Any:
    """Load a YAML file, returning None if it doesn't exist."""
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_table(raw: dict[str, Any]) -> HasuraTable:
    """Parse a single table entry from tables.yaml."""
    tbl_ref = raw.get("table", {})
    if isinstance(tbl_ref, str):
        name = tbl_ref
        schema = "public"
    else:
        name = tbl_ref.get("name", "")
        schema = tbl_ref.get("schema", "public")

    config = raw.get("configuration", {})
    custom_name = config.get("custom_name") or raw.get("custom_name")
    custom_cols = config.get("custom_column_names", {})
    custom_root = config.get("custom_root_fields", raw.get("custom_root_fields", {}))

    table = HasuraTable(
        name=name,
        schema_name=schema,
        custom_name=custom_name,
        custom_column_names=custom_cols,
        custom_root_fields=custom_root or {},
        is_enum=raw.get("is_enum", False),
        configuration=config,
    )

    # Permissions
    for perm_raw in raw.get("select_permissions", []):
        table.select_permissions.append(_parse_permission(perm_raw))
    for perm_raw in raw.get("insert_permissions", []):
        table.insert_permissions.append(_parse_permission(perm_raw))
    for perm_raw in raw.get("update_permissions", []):
        table.update_permissions.append(_parse_permission(perm_raw))
    for perm_raw in raw.get("delete_permissions", []):
        table.delete_permissions.append(_parse_permission(perm_raw))

    # Relationships
    for rel_raw in raw.get("object_relationships", []):
        table.object_relationships.append(_parse_relationship(rel_raw, "object"))
    for rel_raw in raw.get("array_relationships", []):
        table.array_relationships.append(_parse_relationship(rel_raw, "array"))

    # Computed fields
    for cf_raw in raw.get("computed_fields", []):
        table.computed_fields.append(_parse_computed_field(cf_raw))

    # Event triggers
    for et_raw in raw.get("event_triggers", []):
        table.event_triggers.append(_parse_event_trigger(et_raw, name, schema))

    return table


def _parse_permission(raw: dict[str, Any]) -> HasuraPermission:
    role = raw.get("role", "")
    perm = raw.get("permission", {})
    return HasuraPermission(
        role=role,
        columns=perm.get("columns", []),
        filter=perm.get("filter", {}),
        allow_aggregations=perm.get("allow_aggregations", False),
        check=perm.get("check", {}),
    )


def _parse_relationship(raw: dict[str, Any], rel_type: str) -> HasuraRelationship:
    name = raw.get("name", "")
    using = raw.get("using", {})

    # foreign_key_constraint_on or manual_configuration
    fk = using.get("foreign_key_constraint_on", {})
    manual = using.get("manual_configuration", {})

    if rel_type == "object" and isinstance(fk, str):
        # object rel via FK column on this table
        return HasuraRelationship(
            name=name, rel_type=rel_type,
            remote_table="", remote_schema="public",
            column_mapping={fk: "id"},
        )

    if rel_type == "object" and fk and isinstance(fk, dict):
        remote_tbl = fk.get("table", {})
        if isinstance(remote_tbl, str):
            r_name, r_schema = remote_tbl, "public"
        else:
            r_name = remote_tbl.get("name", "")
            r_schema = remote_tbl.get("schema", "public")
        return HasuraRelationship(
            name=name, rel_type=rel_type,
            remote_table=r_name, remote_schema=r_schema,
            column_mapping={fk.get("column", ""): "id"},
        )

    if rel_type == "array" and fk and isinstance(fk, dict):
        remote_tbl = fk.get("table", {})
        if isinstance(remote_tbl, str):
            r_name, r_schema = remote_tbl, "public"
        else:
            r_name = remote_tbl.get("name", "")
            r_schema = remote_tbl.get("schema", "public")
        col = fk.get("column", "")
        return HasuraRelationship(
            name=name, rel_type=rel_type,
            remote_table=r_name, remote_schema=r_schema,
            column_mapping={"id": col},
        )

    if manual:
        remote_tbl = manual.get("remote_table", {})
        if isinstance(remote_tbl, str):
            r_name, r_schema = remote_tbl, "public"
        else:
            r_name = remote_tbl.get("name", "")
            r_schema = remote_tbl.get("schema", "public")
        col_map = manual.get("column_mapping", {})
        return HasuraRelationship(
            name=name, rel_type=rel_type,
            remote_table=r_name, remote_schema=r_schema,
            column_mapping=col_map,
        )

    return HasuraRelationship(name=name, rel_type=rel_type,
                              remote_table="", remote_schema="public")


def _parse_computed_field(raw: dict[str, Any]) -> HasuraComputedField:
    name = raw.get("name", "")
    defn = raw.get("definition", {})
    fn = defn.get("function", {})
    if isinstance(fn, str):
        fn_name, fn_schema = fn, "public"
    else:
        fn_name = fn.get("name", "")
        fn_schema = fn.get("schema", "public")
    return HasuraComputedField(
        name=name, function_name=fn_name, function_schema=fn_schema,
        table_argument=defn.get("table_argument"),
    )


def _parse_event_trigger(
    raw: dict[str, Any], table_name: str, table_schema: str
) -> HasuraEventTrigger:
    ops: list[str] = []
    defn = raw.get("definition", {})
    if defn.get("enable_manual", False):
        ops.append("manual")
    for op in ("insert", "update", "delete"):
        if defn.get(op):
            ops.append(op)
    return HasuraEventTrigger(
        name=raw.get("name", ""),
        table_name=table_name,
        table_schema=table_schema,
        webhook=raw.get("webhook") or raw.get("webhook_from_env", ""),
        operations=ops,
        retry_conf=raw.get("retry_conf", {}),
    )


def _parse_function(raw: dict[str, Any]) -> HasuraFunction:
    fn_ref = raw.get("function", {})
    if isinstance(fn_ref, str):
        name, schema = fn_ref, "public"
    else:
        name = fn_ref.get("name", "")
        schema = fn_ref.get("schema", "public")
    config = raw.get("configuration", {})
    exposed = config.get("exposed_as", "mutation")
    return HasuraFunction(name=name, schema_name=schema, exposed_as=exposed)


def _parse_action(raw: dict[str, Any]) -> HasuraAction:
    name = raw.get("name", "")
    defn_raw = raw.get("definition", {})
    defn = HasuraActionDefinition(
        kind=defn_raw.get("kind", "synchronous"),
        handler=defn_raw.get("handler", ""),
        action_type=defn_raw.get("type", "mutation"),
        arguments=defn_raw.get("arguments", []),
        output_type=defn_raw.get("output_type", ""),
    )
    perms = raw.get("permissions", [])
    return HasuraAction(name=name, definition=defn, permissions=perms)


def _parse_cron_trigger(raw: dict[str, Any]) -> HasuraCronTrigger:
    return HasuraCronTrigger(
        name=raw.get("name", ""),
        webhook=raw.get("webhook", ""),
        schedule=raw.get("schedule", ""),
        include_in_metadata=raw.get("include_in_metadata", True),
        enabled=raw.get("comment", "") != "disabled",
    )


def _parse_inherited_role(raw: dict[str, Any]) -> HasuraInheritedRole:
    return HasuraInheritedRole(
        role_name=raw.get("role_name", ""),
        role_set=raw.get("role_set", []),
    )


def parse_metadata_dir(
    metadata_dir: Path, collector: WarningCollector | None = None
) -> HasuraMetadata:
    """Parse a Hasura v2 metadata directory into HasuraMetadata.

    Supports both flat layout (tables.yaml, actions.yaml at root)
    and databases/ layout (databases/default/tables/...).
    """
    if collector is None:
        collector = WarningCollector()

    metadata = HasuraMetadata()
    md = metadata_dir

    # Try v3-style databases/ layout first
    databases_dir = md / "databases"
    if databases_dir.exists():
        for db_dir in sorted(databases_dir.iterdir()):
            if not db_dir.is_dir():
                continue
            source = _parse_database_dir(db_dir, collector)
            metadata.sources.append(source)
    else:
        # Flat layout: single default source
        source = HasuraSource(name="default", kind="postgres")
        tables_data = _load_yaml(md / "tables.yaml")
        if isinstance(tables_data, list):
            for raw_tbl in tables_data:
                source.tables.append(_parse_table(raw_tbl))
        functions_data = _load_yaml(md / "functions.yaml")
        if isinstance(functions_data, list):
            for raw_fn in functions_data:
                source.functions.append(_parse_function(raw_fn))
        metadata.sources.append(source)

    # Actions
    actions_data = _load_yaml(md / "actions.yaml")
    if isinstance(actions_data, dict):
        for raw_action in actions_data.get("actions", []):
            metadata.actions.append(_parse_action(raw_action))
    elif isinstance(actions_data, list):
        for raw_action in actions_data:
            metadata.actions.append(_parse_action(raw_action))

    # Cron triggers
    cron_data = _load_yaml(md / "cron_triggers.yaml")
    if isinstance(cron_data, list):
        for raw_cron in cron_data:
            metadata.cron_triggers.append(_parse_cron_trigger(raw_cron))

    # Inherited roles
    ir_data = _load_yaml(md / "inherited_roles.yaml")
    if isinstance(ir_data, list):
        for raw_ir in ir_data:
            metadata.inherited_roles.append(_parse_inherited_role(raw_ir))

    # Remote schemas
    rs_data = _load_yaml(md / "remote_schemas.yaml")
    if isinstance(rs_data, list):
        for raw_rs in rs_data:
            name = raw_rs.get("name", "unknown")
            metadata.remote_schemas.append(HasuraRemoteSchema(
                name=name, definition=raw_rs.get("definition", {}),
            ))
            collector.warn(
                "remote_schemas",
                f"Remote schema '{name}' skipped (not supported in Provisa)",
                source_path="remote_schemas.yaml",
            )

    return metadata


def _parse_database_dir(
    db_dir: Path, collector: WarningCollector
) -> HasuraSource:
    """Parse a databases/<name>/ directory."""
    source = HasuraSource(name=db_dir.name, kind="postgres")

    # tables/ directory with individual YAML files
    tables_dir = db_dir / "tables"
    if tables_dir.exists():
        # Check for tables.yaml index
        tables_index = tables_dir / "tables.yaml"
        if tables_index.exists():
            data = _load_yaml(tables_index)
            if isinstance(data, list):
                for raw_tbl in data:
                    source.tables.append(_parse_table(raw_tbl))
        else:
            # Individual table files
            for tbl_file in sorted(tables_dir.glob("*.yaml")):
                data = _load_yaml(tbl_file)
                if isinstance(data, dict):
                    source.tables.append(_parse_table(data))
                elif isinstance(data, list):
                    for raw_tbl in data:
                        source.tables.append(_parse_table(raw_tbl))

    # Single tables.yaml at db level
    tables_yaml = db_dir / "tables.yaml"
    if tables_yaml.exists() and not tables_dir.exists():
        data = _load_yaml(tables_yaml)
        if isinstance(data, list):
            for raw_tbl in data:
                source.tables.append(_parse_table(raw_tbl))

    # Functions
    functions_dir = db_dir / "functions"
    if functions_dir.exists():
        for fn_file in sorted(functions_dir.glob("*.yaml")):
            data = _load_yaml(fn_file)
            if isinstance(data, dict):
                source.functions.append(_parse_function(data))
            elif isinstance(data, list):
                for raw_fn in data:
                    source.functions.append(_parse_function(raw_fn))

    functions_yaml = db_dir / "functions.yaml"
    if functions_yaml.exists() and not functions_dir.exists():
        data = _load_yaml(functions_yaml)
        if isinstance(data, list):
            for raw_fn in data:
                source.functions.append(_parse_function(raw_fn))

    return source
