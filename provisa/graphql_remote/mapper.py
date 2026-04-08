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
        type_ref = type_ref["ofType"]
    return type_ref.get("kind", "SCALAR"), type_ref.get("name", "")

def _gql_to_provisa_type(type_ref: dict) -> str:
    kind, name = _unwrap_type(type_ref)
    if kind == "SCALAR":
        return _SCALAR_TO_PROVISA.get(name, "text")
    # Non-scalar (OBJECT, ENUM, etc.) → JSON string in V1
    return "jsonb"

def _build_columns(fields: list[dict]) -> list[dict]:
    return [
        {
            "name": f["name"],
            "type": _gql_to_provisa_type(f["type"]),
            "description": None,
        }
        for f in (fields or [])
    ]

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

def map_schema(
    schema: dict,
    namespace: str,
    source_id: str,
    domain_id: str = "",
) -> tuple[list[dict], list[dict]]:
    """Map __schema to (virtual_tables, tracked_functions).

    All names are prefixed with namespace__ (REQ-312).
    Query fields with OBJECT return type → virtual tables.
    Mutation fields → tracked functions with return_schema.
    Non-scalar leaf types → jsonb column (REQ-306 scope).
    """
    types: list[dict] = schema.get("types", [])
    query_type_name = (schema.get("queryType") or {}).get("name", "Query")
    mutation_type_name = (schema.get("mutationType") or {}).get("name")

    query_type = _find_type(types, query_type_name)
    mutation_type = _find_type(types, mutation_type_name) if mutation_type_name else None

    tables: list[dict] = []
    functions: list[dict] = []

    for field in (query_type or {}).get("fields") or []:
        ret_kind, ret_name = _unwrap_type(field["type"])
        if ret_kind != "OBJECT":
            continue  # skip scalars at the top level
        return_type = _find_type(types, ret_name)
        columns = _build_columns((return_type or {}).get("fields") or [])
        table_name = f"{namespace}__{field['name']}"
        tables.append({
            "name": table_name,
            "field_name": field["name"],
            "source_id": source_id,
            "columns": columns,
            "domain_id": domain_id,
        })

    for field in (mutation_type or {}).get("fields") or []:
        ret_kind, ret_name = _unwrap_type(field["type"])
        return_type = _find_type(types, ret_name) if ret_kind == "OBJECT" else None
        return_schema = _build_return_schema((return_type or {}).get("fields") or []) if return_type else []
        args = [
            {"name": a["name"], "type": _gql_to_provisa_type(a["type"])}
            for a in (field.get("args") or [])
        ]
        fn_name = f"{namespace}__{field['name']}"
        functions.append({
            "name": fn_name,
            "field_name": field["name"],
            "source_id": source_id,
            "arguments": args,
            "return_schema": return_schema,
            "domain_id": domain_id,
        })

    return tables, functions
