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

def _build_columns(fields: list[dict]) -> list[dict]:
    return [
        {
            "name": f["name"],
            "type": _gql_to_provisa_type(f["type"]),
            "description": f.get("description") or None,
        }
        for f in (fields or [])
    ]

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

def _detect_relationships(tables: list[dict], types: list[dict], queryable_type_names: set[str]) -> list[dict]:
    """Detect intra-source relationships from explicit GQL object references.

    For each OBJECT-typed field that references a queryable type, emits both directions:
    - many-to-one: source.field → target.pk
    - one-to-many (reverse): target.pk → source.field

    Also detects scalar FK fields matching the pattern <field>_id where a table named
    <field>s exists, emitting both directions using the scalar column.
    """
    gql_type_to_table: dict[str, dict] = {}
    for t in tables:
        for typ in types:
            if typ.get("name", "").lower() == t["field_name"].rstrip("s").lower():
                gql_type_to_table.setdefault(typ["name"], t)
                break

    tables_by_name: dict[str, dict] = {t["name"]: t for t in tables}
    types_by_name: dict[str, dict] = {t.get("name", ""): t for t in types}

    relationships: list[dict] = []
    seen: set[str] = set()

    def _add_rel(src_table: str, src_col: str, tgt_table: str, tgt_col: str, cardinality: str) -> None:
        key = f"{src_table}.{src_col}->{tgt_table}.{tgt_col}"
        if key in seen:
            return
        seen.add(key)
        relationships.append({
            "id": f"gql_auto__{src_table}__{src_col}-to-{tgt_table}__{tgt_col}",
            "source_table_id": src_table,
            "target_table_id": tgt_table,
            "source_column": src_col,
            "target_column": tgt_col,
            "cardinality": cardinality,
        })

    for t in tables:
        raw_type = types_by_name.get(t["field_name"].rstrip("s").title()) or types_by_name.get(
            t["field_name"].rstrip("s").capitalize()
        )
        if not raw_type:
            continue
        for rf in raw_type.get("fields") or []:
            rf_kind, rf_type_name = _unwrap_type(rf["type"])
            if rf_kind == "OBJECT" and rf_type_name in queryable_type_names:
                target = gql_type_to_table.get(rf_type_name)
                if target and target["name"] != t["name"]:
                    tgt_cols = {c["name"] for c in target.get("columns", [])}
                    pk = next((c for c in ("id", "name") if c in tgt_cols), None)
                    if pk:
                        # Check if a scalar FK column exists (e.g. employee_id for employee field)
                        src_cols = {c["name"] for c in t.get("columns", [])}
                        scalar_fk = rf["name"] + "_id" if rf["name"] + "_id" in src_cols else None
                        src_col = scalar_fk if scalar_fk else rf["name"]
                        _add_rel(t["name"], src_col, target["name"], pk, "many-to-one")
                        _add_rel(target["name"], pk, t["name"], src_col, "one-to-many")
            elif rf_kind == "SCALAR" and rf["name"].endswith("_name"):
                # Detect scalar _name FKs: breed_name → animalBreeds.name
                prefix = rf["name"][:-5]  # strip "_name"
                for candidate_name, candidate in tables_by_name.items():
                    if candidate_name != t["name"] and candidate_name.lower().startswith(prefix.lower()):
                        cand_cols = {c["name"] for c in candidate.get("columns", [])}
                        if "name" in cand_cols:
                            _add_rel(t["name"], rf["name"], candidate_name, "name", "many-to-one")
                            _add_rel(candidate_name, "name", t["name"], rf["name"], "one-to-many")
                            break

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
    queryable_type_names: set[str] = set()
    for field in (query_type or {}).get("fields") or []:
        _, ret_name = _unwrap_type(field["type"])
        if ret_name:
            queryable_type_names.add(ret_name)

    tables: list[dict] = []
    functions: list[dict] = []

    for field in (query_type or {}).get("fields") or []:
        ret_kind, ret_name = _unwrap_type(field["type"])
        if ret_kind != "OBJECT":
            continue  # skip scalars at the top level
        required_args = _build_required_args(field)
        return_type = _find_type(types, ret_name)
        columns = _build_columns((return_type or {}).get("fields") or [])
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

    relationships = _detect_relationships(tables, types, queryable_type_names)
    return tables, functions, relationships
