# Copyright (c) 2026 Kenneth Stott
# Canary: e6b1702e-7879-4727-ac41-ec12b02a5845
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Auto-register OpenAPI operations as Provisa tables and tracked functions."""
from __future__ import annotations
import json
import logging
from provisa.compiler.naming import to_snake_case
from provisa.openapi.mapper import OpenAPIQuery, OpenAPIMutation, parse_spec

log = logging.getLogger(__name__)

_VERB_PREFIXES = (
    "get", "list", "fetch", "search", "find", "query",
    "create", "post", "add", "insert",
    "update", "put", "patch", "edit",
    "delete", "remove", "destroy",
)


def _singularize(word: str) -> str:
    """Best-effort English singularization for the noun segment of an alias."""
    if word.endswith("ies") and len(word) > 3:
        return word[:-3] + "y"
    if word.endswith("ses") or word.endswith("xes") or word.endswith("zes"):
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss") and len(word) > 2:
        return word[:-1]
    return word


def _operation_id_to_alias(op_id: str) -> str:
    """Convert camelCase/PascalCase/snake_case operationId to a snake_case alias.

    Format: {noun_singular}_{modifiers} (e.g. findPetsByStatus → pet_by_status).
    """
    # camelCase / PascalCase → snake_case
    s = to_snake_case(op_id)
    # strip leading verb segment
    for verb in _VERB_PREFIXES:
        if s.startswith(verb + "_"):
            s = s[len(verb) + 1:]
            break
        if s == verb:
            return s
    # singularize the first (noun) segment
    parts = s.split("_", 1)
    parts[0] = _singularize(parts[0])
    return "_".join(parts) or op_id.lower()


_OPENAPI_TYPE_MAP = {
    "string": "varchar",
    "integer": "integer",
    "number": "numeric",
    "boolean": "boolean",
    "array": "jsonb",
    "object": "jsonb",
}


def _openapi_to_provisa_type(t: str | None) -> str:
    return _OPENAPI_TYPE_MAP.get(t or "string", "text")


def _schema_to_columns(schema: dict | None) -> list[dict]:
    """Extract column list from a JSON Schema object or array-of-objects schema."""
    if not schema:
        return []
    # Unwrap array wrapper
    if schema.get("type") == "array" and "items" in schema:
        schema = schema["items"]
    props = schema.get("properties", {})
    return [
        {"name": name, "type": _openapi_to_provisa_type(prop.get("type"))}
        for name, prop in props.items()
    ]


async def upsert_table(
    source_id: str,
    query: OpenAPIQuery,
    conn,
    domain_id: str = "",
    base_url: str = "",
    auth_config: dict | None = None,
    cache_ttl: int = 300,
) -> None:
    """Register an OpenAPI GET operation as a virtual table and api_endpoint."""
    import json
    from provisa.core.models import Column, GovernanceLevel, Table
    from provisa.core.repositories import table as table_repo

    columns = _schema_to_columns(query.response_schema)
    # Add path/query params as native-filter columns with _nf_ prefix
    existing_names = {c["name"] for c in columns}
    for p in query.path_params:
        if p["name"] not in existing_names:
            columns.append({"name": f"_nf_{p['name']}", "type": _openapi_to_provisa_type(p.get("type")), "native_filter_type": "path_param"})
    for p in query.query_params:
        if p["name"] not in existing_names:
            columns.append({"name": f"_nf_{p['name']}", "type": _openapi_to_provisa_type(p.get("type")), "native_filter_type": "query_param"})

    table_name = query.operation_id
    alias = _operation_id_to_alias(table_name)

    tbl = Table(
        source_id=source_id,
        domain_id=domain_id or "",
        schema_name="openapi",
        table_name=table_name,
        alias=alias if alias != table_name else None,
        governance=GovernanceLevel.pre_approved,
        columns=[
            Column(
                name=c["name"],
                visible_to=[],
                native_filter_type=c.get("native_filter_type"),
            )
            for c in columns
        ],
        description=query.summary,
    )
    await table_repo.upsert(conn, tbl)
    log.debug("Upserted table %s for operation %s", table_name, query.operation_id)

    # Upsert api_sources so api_endpoints FK is satisfied
    await conn.execute(
        """
        INSERT INTO api_sources (id, type, base_url, auth)
        VALUES ($1, 'openapi', $2, $3)
        ON CONFLICT (id) DO UPDATE SET
            base_url = EXCLUDED.base_url,
            auth     = EXCLUDED.auth
        """,
        source_id,
        base_url,
        json.dumps(auth_config) if auth_config else None,
    )

    # Build ApiColumn JSON for api_endpoints
    response_col_names = {c["name"] for c in _schema_to_columns(query.response_schema)}
    api_columns = []
    for c in _schema_to_columns(query.response_schema):
        api_columns.append({"name": c["name"], "type": c["type"], "filterable": True})
    for p in query.path_params:
        api_columns.append({
            "name": p["name"],
            "type": _openapi_to_provisa_type(p.get("type")),
            "filterable": False,
            "param_type": "path",
            "param_name": p["name"],
        })
    for p in query.query_params:
        if p["name"] not in response_col_names:
            api_columns.append({
                "name": p["name"],
                "type": _openapi_to_provisa_type(p.get("type")),
                "filterable": False,
                "param_type": "query",
                "param_name": p["name"],
            })

    await conn.execute(
        """
        INSERT INTO api_endpoints (source_id, path, method, table_name, columns, ttl)
        VALUES ($1, $2, 'GET', $3, $4::jsonb, $5)
        ON CONFLICT (table_name) DO UPDATE SET
            source_id = EXCLUDED.source_id,
            path      = EXCLUDED.path,
            columns   = EXCLUDED.columns,
            ttl       = EXCLUDED.ttl
        """,
        source_id,
        query.path,
        table_name,
        json.dumps(api_columns),
        cache_ttl,
    )


async def upsert_tracked_function(
    source_id: str,
    mutation: OpenAPIMutation,
    conn,
    domain_id: str = "",
) -> None:
    """Register an OpenAPI non-GET operation as a tracked function."""
    from provisa.core.models import Function, FunctionArgument
    from provisa.core.repositories import function as function_repo

    input_cols = _schema_to_columns(mutation.input_schema)
    return_cols = _schema_to_columns(mutation.response_schema)

    fn_name = mutation.operation_id
    return_schema = json.dumps(return_cols) if return_cols else None

    func = Function(
        name=fn_name,
        source_id=source_id,
        schema_name="openapi",
        function_name=mutation.operation_id,
        returns="",
        arguments=[FunctionArgument(name=c["name"], type=c["type"]) for c in input_cols],
        visible_to=[],
        writable_by=[],
        domain_id=domain_id or "",
        description=mutation.summary,
        kind="mutation",
    )
    await function_repo.upsert_function(conn, func, return_schema=return_schema)
    log.debug("Upserted tracked function %s for operation %s", fn_name, mutation.operation_id)


async def auto_register_openapi_source(
    source_id: str,
    spec: dict,
    conn,
    domain_id: str = "",
    base_url: str = "",
    auth_config: dict | None = None,
    cache_ttl: int = 300,
) -> tuple[int, int]:
    """Parse spec and upsert virtual tables + tracked functions. Returns (n_tables, n_mutations)."""
    queries, mutations = parse_spec(spec)
    for q in queries:
        await upsert_table(source_id, q, conn, domain_id, base_url, auth_config, cache_ttl)
    for m in mutations:
        await upsert_tracked_function(source_id, m, conn, domain_id)
    return len(queries), len(mutations)
