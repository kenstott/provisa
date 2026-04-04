# Copyright (c) 2025 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Parse DDN HML files (YAML with ``kind`` field) into intermediate models."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from provisa.ddn.models import (
    DDNAggregateExpression,
    DDNCommand,
    DDNConnector,
    DDNFieldMapping,
    DDNMetadata,
    DDNModel,
    DDNModelPermission,
    DDNObjectType,
    DDNRelationship,
    DDNTypeMapping,
    DDNTypePermission,
)
from provisa.import_shared.warnings import WarningCollector

_SKIP_KINDS = {"OrderByExpression"}
_WARN_KINDS = {"BooleanExpressionType", "AuthConfig"}


def _load_yaml_docs(path: Path) -> list[dict[str, Any]]:
    """Load all YAML documents from a file."""
    with open(path, encoding="utf-8") as f:
        docs = list(yaml.safe_load_all(f))
    return [d for d in docs if isinstance(d, dict)]


def _subgraph_from_path(file_path: Path, root: Path) -> str:
    """Derive subgraph name from directory structure.

    DDN projects typically have: <root>/<subgraph>/metadata/*.hml
    """
    try:
        rel = file_path.relative_to(root)
    except ValueError:
        return "default"
    parts = rel.parts
    if len(parts) >= 2:
        # First directory under root is the subgraph
        sg = parts[0]
        if sg.lower() == "globals":
            return "globals"
        return sg
    return "default"


def _parse_connector(doc: dict[str, Any], subgraph: str) -> DDNConnector:
    defn = doc.get("definition", {})
    name = defn.get("name", "")
    url_raw = defn.get("url", {})
    if isinstance(url_raw, dict):
        url = url_raw.get("singleUrl", {}).get("value", "")
    elif isinstance(url_raw, str):
        url = url_raw
    else:
        url = ""

    scalar_map: dict[str, str] = {}
    for st in defn.get("schema", {}).get("scalar_types", {}).items() if isinstance(
        defn.get("schema", {}).get("scalar_types"), dict
    ) else []:
        scalar_map[st[0]] = st[0]

    return DDNConnector(
        name=name,
        subgraph=subgraph,
        url=url,
        scalar_type_map=scalar_map,
        schema_info=defn.get("schema", {}),
    )


def _parse_object_type(doc: dict[str, Any], subgraph: str) -> DDNObjectType:
    defn = doc.get("definition", {})
    name = defn.get("name", "")

    fields: dict[str, str] = {}
    for f in defn.get("fields", []):
        fname = f.get("name", "")
        ftype = f.get("type", "")
        if isinstance(ftype, dict):
            ftype = ftype.get("type", str(ftype))
        fields[fname] = str(ftype)

    type_mappings: list[DDNTypeMapping] = []
    for tm in defn.get("dataConnectorTypeMapping", []):
        connector = tm.get("dataConnectorName", "")
        source_type = tm.get("dataConnectorObjectType", "")
        fm_list: list[DDNFieldMapping] = []
        fm_raw = tm.get("fieldMapping", {})
        for gql_field, mapping in fm_raw.items():
            col_info = mapping.get("column", {})
            if isinstance(col_info, dict):
                col_name = col_info.get("name", gql_field)
            elif isinstance(col_info, str):
                col_name = col_info
            else:
                col_name = gql_field
            fm_list.append(DDNFieldMapping(
                graphql_field=gql_field, column=col_name,
            ))
        type_mappings.append(DDNTypeMapping(
            connector_name=connector,
            source_type=source_type,
            field_mappings=fm_list,
        ))

    return DDNObjectType(
        name=name,
        subgraph=subgraph,
        fields=fields,
        type_mappings=type_mappings,
    )


def _parse_model(doc: dict[str, Any], subgraph: str) -> DDNModel:
    defn = doc.get("definition", {})
    name = defn.get("name", "")
    object_type = defn.get("objectType", "")

    source = defn.get("source", {})
    connector_name = source.get("dataConnectorName", "")
    collection = source.get("collection", "")

    filter_expr = defn.get("filterExpressionType", None)
    order_by = defn.get("orderByExpression", None)
    aggregate_expr = defn.get("aggregateExpression", None)

    gql = defn.get("graphql", {})
    gql_type_name = None
    gql_select_many = None
    gql_select_unique = None

    select_many = gql.get("selectMany", {})
    if isinstance(select_many, dict):
        gql_select_many = select_many.get("queryRootField", None)

    select_unique = gql.get("selectUniques", [])
    if isinstance(select_unique, list) and select_unique:
        gql_select_unique = select_unique[0].get("queryRootField", None)

    type_name_raw = gql.get("typeName", None)
    if type_name_raw:
        gql_type_name = type_name_raw

    return DDNModel(
        name=name,
        subgraph=subgraph,
        object_type=object_type,
        connector_name=connector_name,
        collection=collection,
        filter_expression_type=filter_expr,
        order_by_expression=order_by,
        aggregate_expression=aggregate_expr,
        graphql_type_name=gql_type_name,
        graphql_select_many=gql_select_many,
        graphql_select_unique=gql_select_unique,
    )


def _parse_relationship(doc: dict[str, Any], subgraph: str) -> DDNRelationship:
    defn = doc.get("definition", {})
    name = defn.get("name", "")
    source_type = defn.get("sourceType", defn.get("source", ""))
    target_raw = defn.get("target", {})

    target_model = ""
    if isinstance(target_raw, dict):
        model_info = target_raw.get("model", {})
        if isinstance(model_info, dict):
            target_model = model_info.get("name", "")
        elif isinstance(model_info, str):
            target_model = model_info

    rel_type = target_raw.get("model", {}).get("relationshipType", "") if isinstance(
        target_raw.get("model"), dict
    ) else ""
    if not rel_type:
        rel_type = defn.get("relationshipType", "")

    mapping_raw = defn.get("mapping", [])
    field_mapping: dict[str, str] = {}
    if isinstance(mapping_raw, list):
        for m in mapping_raw:
            if isinstance(m, dict):
                src = m.get("source", {})
                tgt = m.get("target", {})
                src_field = src.get("fieldPath", [None])[0] if isinstance(src, dict) else None
                tgt_field = tgt.get("fieldPath", [None])[0] if isinstance(tgt, dict) else None
                if src_field and tgt_field:
                    field_mapping[src_field] = tgt_field

    # Also check graphql v1-style mapping
    if not field_mapping:
        gql_mapping = defn.get("mapping", {})
        if isinstance(gql_mapping, dict) and not isinstance(gql_mapping, list):
            field_mapping = dict(gql_mapping)

    return DDNRelationship(
        name=name,
        subgraph=subgraph,
        source_type=source_type,
        target_model=target_model,
        rel_type=rel_type,
        field_mapping=field_mapping,
    )


def _parse_type_permission(doc: dict[str, Any], subgraph: str) -> DDNTypePermission:
    defn = doc.get("definition", {})
    type_name = defn.get("typeName", "")
    perms = defn.get("permissions", [])
    if not perms:
        return DDNTypePermission(type_name=type_name, subgraph=subgraph)
    # DDN TypePermissions has a list of role permissions
    result_perms: list[DDNTypePermission] = []
    for p in perms:
        role = p.get("role", "")
        output = p.get("output", {})
        allowed = output.get("allowedFields", [])
        result_perms.append(DDNTypePermission(
            type_name=type_name, subgraph=subgraph,
            role=role, allowed_fields=allowed,
        ))
    # Return first; caller collects all from _parse_type_permissions_multi
    return result_perms[0] if result_perms else DDNTypePermission(
        type_name=type_name, subgraph=subgraph,
    )


def _parse_type_permissions_multi(
    doc: dict[str, Any], subgraph: str,
) -> list[DDNTypePermission]:
    """Parse TypePermissions doc into multiple per-role entries."""
    defn = doc.get("definition", {})
    type_name = defn.get("typeName", "")
    perms = defn.get("permissions", [])
    result: list[DDNTypePermission] = []
    for p in perms:
        role = p.get("role", "")
        output = p.get("output", {})
        allowed = output.get("allowedFields", [])
        result.append(DDNTypePermission(
            type_name=type_name, subgraph=subgraph,
            role=role, allowed_fields=allowed,
        ))
    return result


def _parse_model_permission(
    doc: dict[str, Any], subgraph: str,
) -> list[DDNModelPermission]:
    """Parse ModelPermissions doc into multiple per-role entries."""
    defn = doc.get("definition", {})
    model_name = defn.get("modelName", "")
    perms = defn.get("permissions", [])
    result: list[DDNModelPermission] = []
    for p in perms:
        role = p.get("role", "")
        flt = p.get("filter", {})
        if flt is None:
            flt = {}
        result.append(DDNModelPermission(
            model_name=model_name, subgraph=subgraph,
            role=role, filter=flt,
        ))
    return result


def _parse_aggregate_expression(
    doc: dict[str, Any], subgraph: str,
) -> DDNAggregateExpression:
    defn = doc.get("definition", {})
    name = defn.get("name", "")
    operand = defn.get("operand", {})
    operand_type = ""
    agg_fields: dict[str, list[str]] = {}
    count_enabled = False
    count_distinct = False

    if isinstance(operand, dict):
        obj_operand = operand.get("object", {})
        if isinstance(obj_operand, dict):
            operand_type = obj_operand.get("aggregatedType", "")
            for af in obj_operand.get("aggregatableFields", []):
                fname = af.get("fieldName", "")
                fns = [e.get("name", "") for e in af.get("aggregateExpression", {}).get(
                    "enabledAggregationFunctions", af.get("enableAggregationFunctions", []),
                )] if isinstance(af.get("aggregateExpression"), dict) else []
                if not fns:
                    fns = af.get("enableAggregationFunctions", [])
                    if isinstance(fns, list) and fns and isinstance(fns[0], dict):
                        fns = [fn.get("name", "") for fn in fns]
                agg_fields[fname] = fns

    count_raw = defn.get("count", {})
    if isinstance(count_raw, dict):
        count_enabled = count_raw.get("enable", False)
        count_distinct = count_raw.get("enableDistinct", count_raw.get("distinct", False))

    return DDNAggregateExpression(
        name=name,
        subgraph=subgraph,
        operand_type=operand_type,
        count_enabled=count_enabled,
        count_distinct=count_distinct,
        aggregatable_fields=agg_fields,
    )


def _parse_command(doc: dict[str, Any], subgraph: str) -> DDNCommand:
    defn = doc.get("definition", {})
    name = defn.get("name", "")

    source = defn.get("source", {})
    connector_name = source.get("dataConnectorName", "")

    # Function or procedure in source
    fn_ref = source.get("function", [])
    proc_ref = source.get("procedure", [])
    if fn_ref:
        command_type = "function"
        source_name = fn_ref[0] if isinstance(fn_ref, list) else fn_ref
    elif proc_ref:
        command_type = "procedure"
        source_name = proc_ref[0] if isinstance(proc_ref, list) else proc_ref
    else:
        command_type = "function"
        source_name = ""

    # Also handle string-form source names
    if isinstance(source_name, list):
        source_name = source_name[0] if source_name else ""

    return_type = defn.get("outputType", "")
    if isinstance(return_type, dict):
        return_type = return_type.get("type", str(return_type))

    arguments: dict[str, str] = {}
    for arg in defn.get("arguments", []):
        arg_name = arg.get("name", "")
        arg_type = arg.get("type", "")
        if isinstance(arg_type, dict):
            arg_type = arg_type.get("type", str(arg_type))
        arguments[arg_name] = str(arg_type)

    gql = defn.get("graphql", {})
    root_field = gql.get("rootFieldName", None)

    return DDNCommand(
        name=name,
        subgraph=subgraph,
        connector_name=connector_name,
        command_type=command_type,
        source_name=str(source_name),
        return_type=str(return_type),
        arguments=arguments,
        graphql_root_field=root_field,
    )


def parse_hml_dir(
    hml_dir: Path, collector: WarningCollector | None = None,
) -> DDNMetadata:
    """Parse a DDN HML project directory into DDNMetadata.

    Recursively finds all .hml files (YAML with ``kind`` field) and
    routes each document to the appropriate parser.
    """
    if collector is None:
        collector = WarningCollector()

    metadata = DDNMetadata()
    root = hml_dir

    hml_files = sorted(root.rglob("*.hml"))
    if not hml_files:
        # Also try .yaml files that contain kind fields
        hml_files = sorted(root.rglob("*.yaml"))

    for fpath in hml_files:
        subgraph = _subgraph_from_path(fpath, root)
        if subgraph != "globals":
            metadata.subgraphs.add(subgraph)

        docs = _load_yaml_docs(fpath)
        for doc in docs:
            kind = doc.get("kind", "")
            _dispatch_doc(doc, kind, subgraph, fpath, metadata, collector)

    return metadata


def _dispatch_doc(
    doc: dict[str, Any],
    kind: str,
    subgraph: str,
    fpath: Path,
    metadata: DDNMetadata,
    collector: WarningCollector,
) -> None:
    """Route a single HML document by kind."""
    if kind == "DataConnectorLink":
        metadata.connectors.append(_parse_connector(doc, subgraph))
    elif kind == "ObjectType":
        metadata.object_types.append(_parse_object_type(doc, subgraph))
    elif kind == "Model":
        metadata.models.append(_parse_model(doc, subgraph))
    elif kind == "Relationship":
        metadata.relationships.append(_parse_relationship(doc, subgraph))
    elif kind == "TypePermissions":
        perms = _parse_type_permissions_multi(doc, subgraph)
        metadata.type_permissions.extend(perms)
    elif kind == "ModelPermissions":
        perms = _parse_model_permission(doc, subgraph)
        metadata.model_permissions.extend(perms)
    elif kind == "AggregateExpression":
        metadata.aggregate_expressions.append(
            _parse_aggregate_expression(doc, subgraph),
        )
    elif kind == "Command":
        metadata.commands.append(_parse_command(doc, subgraph))
    elif kind in _SKIP_KINDS:
        metadata.skipped_kinds[kind] = metadata.skipped_kinds.get(kind, 0) + 1
    elif kind in _WARN_KINDS:
        metadata.skipped_kinds[kind] = metadata.skipped_kinds.get(kind, 0) + 1
        collector.warn(
            kind,
            f"{kind} skipped (not supported in Provisa)",
            source_path=str(fpath),
        )
    elif kind:
        metadata.skipped_kinds[kind] = metadata.skipped_kinds.get(kind, 0) + 1
