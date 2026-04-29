# Copyright (c) 2026 Kenneth Stott
# Canary: 8120ef97-e240-4db8-8f69-20a2ddbb4ac6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Map GraphQL introspection to Provisa tables + functions (REQ-308, REQ-312)."""
from __future__ import annotations

_SCALAR_TO_PROVISA = {
    "String": "text",
    "ID": "text",
    "Int": "integer",
    "Float": "numeric",
    "Boolean": "boolean",
}

def _unwrap_type(type_ref: dict) -> tuple[str, str]:
    """Unwrap NON_NULL/LIST wrappers; return (kind, name) of the leaf type."""
    while type_ref.get("kind") in ("NON_NULL", "LIST"):
        next_ref = type_ref.get("ofType")
        if not next_ref:
            break
        type_ref = next_ref
    return type_ref.get("kind", "SCALAR"), type_ref.get("name", "")

def _gql_to_provisa_type(type_ref: dict) -> str:
    kind, name = _unwrap_type(type_ref)
    if kind == "SCALAR":
        return _SCALAR_TO_PROVISA.get(name, "text")
    # Non-scalar (OBJECT, ENUM, etc.) → JSON string in V1
    return "jsonb"

def _build_gql_field_selection(fields: list[dict], types: list[dict], depth: int = 0) -> str:
    """Recursively build a GQL selection string for object type fields."""
    if depth > 3:
        return "__typename"
    parts = []
    for f in (fields or []):
        kind, name = _unwrap_type(f["type"])
        if kind in ("SCALAR", "ENUM"):
            parts.append(f["name"])
        elif kind == "OBJECT" and name:
            sub_type = _find_type(types, name)
            if sub_type and sub_type.get("fields"):
                sub_sel = _build_gql_field_selection(sub_type["fields"], types, depth + 1)
                parts.append(f"{f['name']} {{ {sub_sel} }}")
    return " ".join(parts) if parts else "__typename"


def _is_list_type(type_ref: dict) -> bool:
    """Return True if the outermost non-NON_NULL wrapper is LIST."""
    while type_ref.get("kind") == "NON_NULL":
        type_ref = type_ref.get("ofType", {})
    return type_ref.get("kind") == "LIST"


def _build_object_fields_recursive(fields: list[dict], types: list[dict], depth: int = 0) -> list[dict]:
    """Build structured object field dicts (with nested 'fields') from GQL type fields."""
    if depth > 3:
        return []
    result = []
    for f in (fields or []):
        kind, name = _unwrap_type(f["type"])
        if kind in ("SCALAR", "ENUM"):
            result.append({"name": f["name"], "type": _gql_to_provisa_type(f["type"])})
        elif kind == "OBJECT" and name:
            obj_type = _find_type(types, name)
            if obj_type and obj_type.get("fields"):
                sub_fields = _build_object_fields_recursive(obj_type["fields"], types, depth + 1)
                if sub_fields:
                    result.append({"name": f["name"], "type": "object", "fields": sub_fields})
    return result


def _build_columns(fields: list[dict], types: list[dict] | None = None) -> list[dict]:
    result = []
    for f in (fields or []):
        kind, name = _unwrap_type(f["type"])
        col: dict = {
            "name": f["name"],
            "type": _gql_to_provisa_type(f["type"]),
            "description": f.get("description") or None,
        }
        if kind == "OBJECT" and types is not None and name:
            obj_type = _find_type(types, name)
            if obj_type and obj_type.get("fields"):
                sub_sel = _build_gql_field_selection(obj_type["fields"], types, 0)
                col["gql_selection"] = f"{f['name']} {{ {sub_sel} }}"
                col["gql_object_fields"] = _build_object_fields_recursive(obj_type["fields"], types, 0)
                col["gql_object_type"] = name
                col["gql_is_list"] = _is_list_type(f["type"])
        result.append(col)
    return result

def _gql_type_string(type_ref: dict) -> str:
    """Reconstruct GQL type string for variable declarations (e.g. 'Int!', '[String!]!')."""
    kind = type_ref.get("kind")
    if kind == "NON_NULL":
        return _gql_type_string(type_ref.get("ofType", {})) + "!"
    if kind == "LIST":
        return f"[{_gql_type_string(type_ref.get('ofType', {}))}]"
    return type_ref.get("name", "String")


def _build_required_args(field: dict) -> list[dict]:
    """Return required arg metadata for non-null args with no default value."""
    result = []
    for arg in (field.get("args") or []):
        if arg.get("type", {}).get("kind") == "NON_NULL" and arg.get("defaultValue") is None:
            result.append({
                "name": arg["name"],
                "gql_type": _gql_type_string(arg["type"]),
                "provisa_type": _gql_to_provisa_type(arg["type"]),
            })
    return result

def _build_return_schema(fields: list[dict]) -> list[dict]:
    """Build inline_return_type compatible schema from object fields."""
    return [
        {"name": f["name"], "type": _gql_to_provisa_type(f["type"])}
        for f in (fields or [])
    ]

def _find_type(types: list[dict], name: str) -> dict | None:
    for t in types:
        if t.get("name") == name:
            return t
    return None

def _infer_fk_columns(
    field_name: str,
    gql_type_name: str,
    source_scalar_names: set[str],
    types: list[dict],
) -> tuple[str, str]:
    """Infer (source_column, target_column) from naming conventions.

    For a many-to-one object field named F of type T:
    - Check source scalars for {F}{TargetField.capitalize()} pattern for each
      scalar field on T. The first match wins.
    - Returns ("", "") when no match is found.
    """
    target_type = _find_type(types, gql_type_name)
    if not target_type:
        return "", ""
    scalar_fields = [
        f["name"] for f in (target_type.get("fields") or [])
        if _unwrap_type(f["type"])[0] in ("SCALAR", "ENUM")
    ]
    for sf in scalar_fields:
        candidate = field_name + sf[0].upper() + sf[1:]
        if candidate in source_scalar_names:
            return candidate, sf
    return "", ""


def _detect_relationships(
    tables: list[dict],
    types: list[dict],
    queryable_type_names: set[str],
    type_to_table: dict[str, str],
) -> list[dict]:
    """Emit relationships for intra-source OBJECT column references.

    For many-to-one fields, infers source_column/target_column from naming
    conventions (e.g. breedName → breed.name). Falls back to empty columns
    for list (one-to-many) object fields where the FK lives on the target side.
    """
    relationships = []
    for table in tables:
        src_scalars = {
            c["name"] for c in (table.get("columns") or [])
            if c.get("type") != "jsonb"
        }
        for col in table.get("columns") or []:
            gql_type = col.get("gql_object_type")
            if not gql_type or gql_type not in queryable_type_names:
                continue
            target_table = type_to_table.get(gql_type)
            if not target_table or target_table == table["name"]:
                continue
            cardinality = "one-to-many" if col.get("gql_is_list") else "many-to-one"
            rel_id = f"gql_remote__{table['source_id']}__{table['name']}__{col['name']}"
            if cardinality == "many-to-one":
                src_col, tgt_col = _infer_fk_columns(col["name"], gql_type, src_scalars, types)
            else:
                src_col, tgt_col = "", ""
            relationships.append({
                "id": rel_id,
                "source_table_id": table["name"],
                "target_table_id": target_table,
                "source_column": src_col,
                "target_column": tgt_col,
                "cardinality": cardinality,
                "remote_managed": True,
            })
    return relationships


def map_schema(
    schema: dict,
    namespace: str,
    source_id: str,
    domain_id: str = "",
) -> tuple[list[dict], list[dict], list[dict]]:
    """Map __schema to (virtual_tables, tracked_functions, relationships).

    Names use the GQL field name directly; source_id provides disambiguation.
    Query fields with OBJECT return type → virtual tables.
    Mutation fields → tracked functions with return_schema.
    Non-scalar leaf types → jsonb column (REQ-306 scope).
    Intra-source relationships detected via object references and scalar FK naming.
    """
    types: list[dict] = schema.get("types", [])
    query_type_name = (schema.get("queryType") or {}).get("name", "Query")
    mutation_type_name = (schema.get("mutationType") or {}).get("name")

    query_type = _find_type(types, query_type_name)
    mutation_type = _find_type(types, mutation_type_name) if mutation_type_name else None

    # Collect which GQL type names are directly queryable (root query return types)
    # and build a mapping from GQL type name → registered table name.
    # Prefer no-required-arg fields so join targets can be bulk-fetched.
    queryable_type_names: set[str] = set()
    type_to_table: dict[str, str] = {}
    for field in (query_type or {}).get("fields") or []:
        _, ret_name = _unwrap_type(field["type"])
        if ret_name:
            queryable_type_names.add(ret_name)
            tname = f"{namespace}__{field['name']}" if namespace else field['name']
            has_required = bool(_build_required_args(field))
            if ret_name not in type_to_table or has_required is False:
                type_to_table[ret_name] = tname

    tables: list[dict] = []
    functions: list[dict] = []

    for field in (query_type or {}).get("fields") or []:
        ret_kind, ret_name = _unwrap_type(field["type"])
        if ret_kind != "OBJECT":
            continue  # skip scalars at the top level
        required_args = _build_required_args(field)
        return_type = _find_type(types, ret_name)
        columns = _build_columns((return_type or {}).get("fields") or [], types)
        table_name = f"{namespace}__{field['name']}" if namespace else field['name']
        tables.append({
            "name": table_name,
            "field_name": field["name"],
            "source_id": source_id,
            "columns": columns,
            "domain_id": domain_id,
            "description": field.get("description") or (return_type or {}).get("description") or None,
            "required_args": required_args,
        })

    for field in (mutation_type or {}).get("fields") or []:
        ret_kind, ret_name = _unwrap_type(field["type"])
        return_type = _find_type(types, ret_name) if ret_kind == "OBJECT" else None
        return_schema = _build_return_schema((return_type or {}).get("fields") or []) if return_type else []
        args = [
            {"name": a["name"], "type": _gql_to_provisa_type(a["type"]), "description": a.get("description") or None}
            for a in (field.get("args") or [])
        ]
        fn_name = f"{namespace}__{field['name']}" if namespace else field['name']
        functions.append({
            "name": fn_name,
            "field_name": field["name"],
            "source_id": source_id,
            "arguments": args,
            "return_schema": return_schema,
            "domain_id": domain_id,
            "description": field.get("description") or None,
        })

    relationships = _detect_relationships(tables, types, queryable_type_names, type_to_table)
    return tables, functions, relationships
