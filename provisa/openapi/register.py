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
from provisa.openapi.mapper import OpenAPIQuery, OpenAPIMutation, parse_spec

log = logging.getLogger(__name__)

_OPENAPI_TYPE_MAP = {
    "string": "text",
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
    namespace: str = "",
    domain_id: str = "",
) -> None:
    """Register an OpenAPI GET operation as a virtual table."""
    from provisa.core.models import Column, GovernanceLevel, Table
    from provisa.core.repositories import table as table_repo

    columns = _schema_to_columns(query.response_schema)
    # Add path params and query params as filterable columns if not already present
    existing_names = {c["name"] for c in columns}
    for p in query.path_params + query.query_params:
        if p["name"] not in existing_names:
            columns.append({"name": p["name"], "type": _openapi_to_provisa_type(p.get("type"))})

    table_name = f"{namespace}__{query.operation_id}" if namespace else query.operation_id

    tbl = Table(
        source_id=source_id,
        domain_id=domain_id or "",
        schema_name="openapi",
        table_name=table_name,
        governance=GovernanceLevel.pre_approved,
        columns=[
            Column(
                name=c["name"],
                visible_to=[],
            )
            for c in columns
        ],
        description=query.summary,
    )
    await table_repo.upsert(conn, tbl)
    log.debug("Upserted table %s for operation %s", table_name, query.operation_id)


async def upsert_tracked_function(
    source_id: str,
    mutation: OpenAPIMutation,
    conn,
    namespace: str = "",
    domain_id: str = "",
) -> None:
    """Register an OpenAPI non-GET operation as a tracked function."""
    input_cols = _schema_to_columns(mutation.input_schema)
    return_cols = _schema_to_columns(mutation.response_schema)

    fn_name = f"{namespace}__{mutation.operation_id}" if namespace else mutation.operation_id
    arguments = json.dumps([{"name": c["name"], "type": c["type"]} for c in input_cols])
    return_schema = json.dumps(return_cols) if return_cols else None

    await conn.execute(
        """
        INSERT INTO tracked_functions
            (name, source_id, schema_name, function_name, returns,
             arguments, visible_to, writable_by, domain_id, description, kind, return_schema)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (name) DO UPDATE SET
            source_id     = EXCLUDED.source_id,
            schema_name   = EXCLUDED.schema_name,
            function_name = EXCLUDED.function_name,
            returns       = EXCLUDED.returns,
            arguments     = EXCLUDED.arguments,
            domain_id     = EXCLUDED.domain_id,
            description   = EXCLUDED.description,
            kind          = EXCLUDED.kind,
            return_schema = EXCLUDED.return_schema,
            updated_at    = NOW()
        """,
        fn_name, source_id, "openapi", mutation.operation_id, "",
        arguments, [], [], domain_id or "", mutation.summary,
        "mutation", return_schema,
    )
    log.debug("Upserted tracked function %s for operation %s", fn_name, mutation.operation_id)


async def auto_register_openapi_source(
    source_id: str,
    spec: dict,
    conn,
    namespace: str = "",
    domain_id: str = "",
) -> tuple[int, int]:
    """Parse spec and upsert virtual tables + tracked functions. Returns (n_tables, n_mutations)."""
    queries, mutations = parse_spec(spec)
    for q in queries:
        await upsert_table(source_id, q, conn, namespace, domain_id)
    for m in mutations:
        await upsert_tracked_function(source_id, m, conn, namespace, domain_id)
    return len(queries), len(mutations)
