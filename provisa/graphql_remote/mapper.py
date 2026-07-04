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

# Requirements: REQ-307, REQ-308, REQ-309, REQ-310, REQ-311, REQ-312, REQ-313
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


def _build_gql_field_selection(
    fields: list[dict],
    types: list[dict],
    depth: int = 0,
    max_depth: int = 5,
    max_list_depth: int = 2,
    max_list_items: int = 100,
) -> str:
    """Recursively build a GQL selection string for object type fields."""
    if depth > max_depth:
        return "__typename"
    parts = []
    for f in fields or []:
        is_list = _is_list_type(f["type"])
        if is_list and depth >= max_list_depth:
            continue
        kind, name = _unwrap_type(f["type"])
        if kind in ("SCALAR", "ENUM"):
            parts.append(f["name"])
        elif kind == "OBJECT" and name:
            sub_type = _find_type(types, name)
            if sub_type and sub_type.get("fields"):
                sub_sel = _build_gql_field_selection(
                    sub_type["fields"], types, depth + 1, max_depth, max_list_depth, max_list_items
                )
                field_ref = f"{f['name']}(first: {max_list_items})" if is_list else f["name"]
                parts.append(f"{field_ref} {{ {sub_sel} }}")
    return " ".join(parts) if parts else "__typename"


def _is_list_type(type_ref: dict) -> bool:
    """Return True if the outermost non-NON_NULL wrapper is LIST."""
    while type_ref.get("kind") == "NON_NULL":
        type_ref = type_ref.get("ofType", {})
    return type_ref.get("kind") == "LIST"


def _build_object_fields_recursive(
    fields: list[dict],
    types: list[dict],
    depth: int = 0,
    max_depth: int = 5,
    max_list_depth: int = 2,
    max_list_items: int = 100,
) -> list[dict]:
    """Build structured object field dicts (with nested 'fields') from GQL type fields."""
    if depth > max_depth:
        return []
    result = []
    for f in fields or []:
        if _is_list_type(f["type"]) and depth >= max_list_depth:
            continue
        kind, name = _unwrap_type(f["type"])
        if kind in ("SCALAR", "ENUM"):
            result.append({"name": f["name"], "type": _gql_to_provisa_type(f["type"])})
        elif kind == "OBJECT" and name:
            obj_type = _find_type(types, name)
            if obj_type and obj_type.get("fields"):
                sub_fields = _build_object_fields_recursive(
                    obj_type["fields"], types, depth + 1, max_depth, max_list_depth, max_list_items
                )
                if sub_fields:
                    result.append(
                        {
                            "name": f["name"],
                            "type": "jsonb" if _is_list_type(f["type"]) else "object",
                            "fields": sub_fields,
                        }
                    )
    return result


def _build_columns(
    fields: list[dict],
    types: list[dict] | None = None,
    max_object_depth: int = 5,
    max_list_depth: int = 2,
    max_list_items: int = 100,
) -> list[dict]:
    result = []
    for f in fields or []:
        kind, name = _unwrap_type(f["type"])
        col: dict = {
            "name": f["name"],
            "type": _gql_to_provisa_type(f["type"]),
            "description": f.get("description") or None,
        }
        if kind == "OBJECT" and types is not None and name:
            obj_type = _find_type(types, name)
            if obj_type and obj_type.get("fields"):
                sub_sel = _build_gql_field_selection(
                    obj_type["fields"], types, 0, max_object_depth, max_list_depth, max_list_items
                )
                col["gql_selection"] = f"{f['name']} {{ {sub_sel} }}"
                col["gql_object_fields"] = _build_object_fields_recursive(
                    obj_type["fields"], types, 0, max_object_depth, max_list_depth, max_list_items
                )
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


_LIMIT_ARGS = {"first", "limit"}
_OFFSET_ARGS = {"offset"}
_CURSOR_ARGS = {"after"}


def _detect_pagination_args(args: list[dict]) -> dict:
    """Return pagination arg names by sniffing field args for common server patterns.

    Priority for limit: "first" (PostGraphile/Relay/pg_graphql) > "limit" (Hasura).
    Returns {"limit_arg": str|None, "offset_arg": str|None, "cursor_arg": str|None}.
    """
    arg_names = {a["name"] for a in (args or [])}
    if "first" in arg_names:
        limit_arg = "first"
    elif "limit" in arg_names:
        limit_arg = "limit"
    else:
        limit_arg = None
    offset_arg = "offset" if "offset" in arg_names else None
    cursor_arg = "after" if "after" in arg_names else None
    return {"limit_arg": limit_arg, "offset_arg": offset_arg, "cursor_arg": cursor_arg}


def _build_required_args(field: dict) -> list[dict]:
    """Return required arg metadata for non-null args with no default value."""
    result = []
    for arg in field.get("args") or []:
        if arg.get("type", {}).get("kind") == "NON_NULL" and arg.get("defaultValue") is None:
            result.append(
                {
                    "name": arg["name"],
                    "gql_type": _gql_type_string(arg["type"]),
                    "provisa_type": _gql_to_provisa_type(arg["type"]),
                }
            )
    return result


def _build_return_schema(fields: list[dict]) -> list[dict]:
    """Build inline_return_type compatible schema from object fields."""
    return [{"name": f["name"], "type": _gql_to_provisa_type(f["type"])} for f in (fields or [])]


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
        f["name"]
        for f in (target_type.get("fields") or [])
        if _unwrap_type(f["type"])[0] in ("SCALAR", "ENUM")
    ]
    for sf in scalar_fields:
        candidate = field_name + sf[0].upper() + sf[1:]
        if candidate in source_scalar_names:
            return candidate, sf
    return "", ""


def _detect_relationships(  # REQ-313
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
        src_scalars = {c["name"] for c in (table.get("columns") or []) if c.get("type") != "jsonb"}
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
            relationships.append(
                {
                    "id": rel_id,
                    "source_table_id": table["name"],
                    "target_table_id": target_table,
                    "source_column": src_col,
                    "target_column": tgt_col,
                    "cardinality": cardinality,
                    "remote_managed": True,
                }
            )
    return relationships


def _qualify_name(namespace: str, field_name: str) -> str:  # REQ-312
    """Return namespace__field_name or field_name when namespace is empty."""
    return f"{namespace}__{field_name}" if namespace else field_name


def _build_args_list(field: dict) -> list[dict]:
    """Build the arguments list for a function entry from a GQL field."""
    return [
        {
            "name": a["name"],
            "type": _gql_to_provisa_type(a["type"]),
            "description": a.get("description") or None,
        }
        for a in (field.get("args") or [])
    ]


def _build_query_function_return_schema(
    field: dict,
    ret_kind: str,
    ret_name: str,
    types: list[dict],
) -> list[dict]:
    """Return the return_schema list for a query field treated as a function."""
    if ret_kind == "OBJECT" and ret_name:
        obj_type = _find_type(types, ret_name)
        return _build_return_schema((obj_type or {}).get("fields") or [])
    return [{"name": "value", "type": _gql_to_provisa_type(field["type"])}]


def _make_function_entry(
    field: dict,
    namespace: str,
    source_id: str,
    domain_id: str,
    return_schema: list[dict],
) -> dict:
    """Assemble a tracked-function dict from a GQL field."""
    return {
        "name": _qualify_name(namespace, field["name"]),
        "field_name": field["name"],
        "source_id": source_id,
        "arguments": _build_args_list(field),
        "return_schema": return_schema,
        "domain_id": domain_id,
        "description": field.get("description") or None,
    }


def _is_query_field_function(ret_kind: str, override: str) -> bool:
    """Return True when a query field should be mapped as a function, not a table."""
    return override == "mutation" or (ret_kind != "OBJECT" and override != "query")


def _map_query_field_as_function(
    field: dict,
    namespace: str,
    source_id: str,
    domain_id: str,
    types: list[dict],
) -> dict:
    """Map a single query field to a tracked-function entry."""
    ret_kind, ret_name = _unwrap_type(field["type"])
    return_schema = _build_query_function_return_schema(field, ret_kind, ret_name, types)
    return _make_function_entry(field, namespace, source_id, domain_id, return_schema)


def _map_query_field_as_table(  # REQ-308
    field: dict,
    namespace: str,
    source_id: str,
    domain_id: str,
    types: list[dict],
    max_object_depth: int,
    max_list_depth: int,
    max_list_items: int,
) -> dict:
    """Map a single query field to a virtual-table entry."""
    _, ret_name = _unwrap_type(field["type"])
    return_type = _find_type(types, ret_name)
    columns = _build_columns(
        (return_type or {}).get("fields") or [],
        types,
        max_object_depth,
        max_list_depth,
        max_list_items,
    )
    from provisa.compiler.naming import apply_sql_name as _apply_sql_name

    table_name = _qualify_name(namespace, field["name"])
    return {
        "name": table_name,
        "sql_name": _apply_sql_name(table_name),
        "field_name": field["name"],
        "gql_type_name": ret_name,
        "source_id": source_id,
        "columns": columns,
        "domain_id": domain_id,
        "description": field.get("description") or (return_type or {}).get("description") or None,
        "required_args": _build_required_args(field),
        "pagination": _detect_pagination_args(field.get("args") or []),
    }


def _type_has_list(type_ref: dict) -> bool:
    """Whether a type reference is LIST-wrapped (through any NON_NULL wrappers)."""
    while type_ref.get("kind") in ("NON_NULL", "LIST"):
        if type_ref.get("kind") == "LIST":
            return True
        type_ref = type_ref.get("ofType") or {}
    return False


def _mutation_return_leaves(field: dict, types: list[dict]) -> tuple[list[str], set[str]]:
    """Object leaf types a mutation's return type exposes, and which are list-valued (REQ-871).

    Walks a payload type's fields (unwrapping NON_NULL/LIST) collecting OBJECT leaves and
    marking LIST-valued ones as changed-records collections; scalar/stats fields are ignored.
    When the return type is itself an OBJECT (no payload wrapper), that type is the leaf.
    """
    ret_kind, ret_name = _unwrap_type(field["type"])
    leaves: list[str] = []
    list_valued: set[str] = set()
    # The return type may BE the table object directly (e.g. createUser: User) ...
    if ret_kind == "OBJECT" and ret_name:
        leaves.append(ret_name)
    # ... or be a payload wrapper whose object-typed fields point at the table(s).
    return_type = _find_type(types, ret_name) if ret_name else None
    for f in (return_type or {}).get("fields") or []:
        k, n = _unwrap_type(f["type"])
        if k == "OBJECT" and n:
            leaves.append(n)
            if _type_has_list(f["type"]):
                list_valued.add(n)
    return leaves, list_valued


def _mutation_input_stem(field: dict) -> str:
    """Type name of the mutation's first non-scalar (input-object) argument, for stem matching."""
    for arg in field.get("args") or []:
        kind, name = _unwrap_type(arg.get("type") or {})
        if kind in ("INPUT_OBJECT", "OBJECT") and name:
            return name
    return ""


def _map_mutation_field(  # REQ-308, REQ-871
    field: dict,
    namespace: str,
    source_id: str,
    domain_id: str,
    types: list[dict],
    type_to_table: dict[str, str] | None = None,
    table_names: list[str] | None = None,
) -> dict:
    """Map a single mutation field to a tracked-function entry.

    Attaches non-binding ``suggested_associations`` (REQ-871): ranked (table, score, reason)
    hints for which table this mutation likely writes. Hints only — nothing is bound here;
    an admin confirms, and a confirmed association registers with empty writable_by.
    """
    ret_kind, ret_name = _unwrap_type(field["type"])
    return_type = _find_type(types, ret_name) if ret_kind == "OBJECT" else None
    return_schema = (
        _build_return_schema((return_type or {}).get("fields") or []) if return_type else []
    )
    entry = _make_function_entry(field, namespace, source_id, domain_id, return_schema)

    from provisa.security.association_suggester import suggest_graphql

    leaves, list_valued = _mutation_return_leaves(field, types)
    candidates = suggest_graphql(
        return_leaf_types=leaves,
        list_valued_types=list_valued,
        type_to_table=type_to_table or {},
        op_name=field["name"],
        input_type_stem=_mutation_input_stem(field),
        table_names=table_names or [],
    )
    entry["suggested_associations"] = [
        {"table": c.table, "score": c.score, "reason": c.reason} for c in candidates
    ]
    return entry


def _collect_queryable_types(
    query_type: dict | None,
    namespace: str,
) -> tuple[set[str], dict[str, str]]:
    """Collect queryable GQL type names and their preferred table name mapping.

    Prefers no-required-arg fields so join targets can be bulk-fetched.
    """
    queryable_type_names: set[str] = set()
    type_to_table: dict[str, str] = {}
    for field in (query_type or {}).get("fields") or []:
        _, ret_name = _unwrap_type(field["type"])
        if ret_name:
            queryable_type_names.add(ret_name)
            tname = _qualify_name(namespace, field["name"])
            has_required = bool(_build_required_args(field))
            if ret_name not in type_to_table or has_required is False:
                type_to_table[ret_name] = tname
    return queryable_type_names, type_to_table


def _process_query_fields(
    query_type: dict | None,
    namespace: str,
    source_id: str,
    domain_id: str,
    types: list[dict],
    field_overrides: dict[str, str],
    max_object_depth: int,
    max_list_depth: int,
    max_list_items: int,
) -> tuple[list[dict], list[dict]]:
    """Partition query fields into tables and functions."""
    tables: list[dict] = []
    functions: list[dict] = []
    for field in (query_type or {}).get("fields") or []:
        override = field_overrides.get(field["name"], "").lower()
        ret_kind, _ = _unwrap_type(field["type"])
        if _is_query_field_function(ret_kind, override):
            functions.append(
                _map_query_field_as_function(field, namespace, source_id, domain_id, types)
            )
        else:
            tables.append(
                _map_query_field_as_table(
                    field,
                    namespace,
                    source_id,
                    domain_id,
                    types,
                    max_object_depth,
                    max_list_depth,
                    max_list_items,
                )
            )
    return tables, functions


def _process_mutation_fields(
    mutation_type: dict | None,
    namespace: str,
    source_id: str,
    domain_id: str,
    types: list[dict],
    type_to_table: dict[str, str] | None = None,
    table_names: list[str] | None = None,
) -> list[dict]:
    """Map all mutation fields to tracked-function entries."""
    return [
        _map_mutation_field(
            field, namespace, source_id, domain_id, types, type_to_table, table_names
        )
        for field in (mutation_type or {}).get("fields") or []
    ]


def map_schema(  # REQ-308, REQ-312, REQ-313
    schema: dict,
    namespace: str,
    source_id: str,
    domain_id: str = "",
    max_object_depth: int = 5,
    max_list_depth: int = 2,
    max_list_items: int = 100,
    field_overrides: dict[str, str] | None = None,
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

    queryable_type_names, type_to_table = _collect_queryable_types(query_type, namespace)

    _overrides = field_overrides or {}
    tables, functions = _process_query_fields(
        query_type,
        namespace,
        source_id,
        domain_id,
        types,
        _overrides,
        max_object_depth,
        max_list_depth,
        max_list_items,
    )
    functions.extend(
        _process_mutation_fields(
            mutation_type,
            namespace,
            source_id,
            domain_id,
            types,
            type_to_table,
            [t["name"] for t in tables],
        )
    )

    relationships = _detect_relationships(tables, types, queryable_type_names, type_to_table)
    return tables, functions, relationships
