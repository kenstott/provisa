# Copyright (c) 2026 Kenneth Stott
# Canary: 9f1a2b3c-4d5e-6f7a-8b9c-0d1e2f3a4b5c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Native source introspection helpers for available_schemas / available_tables.

Returns None when no native path exists — caller falls back to Trino.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.api.admin.types import AvailableTableType
    from provisa.executor.pool import SourcePool

_MYSQL_SYSTEM_DBS = {"information_schema", "mysql", "performance_schema", "sys"}
_SQLSERVER_SYSTEM_SCHEMAS = {
    "sys", "INFORMATION_SCHEMA", "guest", "db_owner", "db_accessadmin",
    "db_securityadmin", "db_ddladmin", "db_backupoperator", "db_datareader",
    "db_datawriter", "db_denydatareader", "db_denydatawriter",
}
_PG_SYSTEM_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast", "mv_cache"}


async def native_schemas(
    source_id: str,
    source_type: str,
    pool: "SourcePool",
    config_conn,
) -> list[str] | None:
    """Return schema list via native introspection or None to fall back to Trino."""
    t = source_type.lower()

    if t in ("graphql", "graphql_remote"):
        return ["default"]

    if t in ("grpc", "grpc_remote"):
        return ["default"]

    if t == "kafka":
        return ["default"]

    if t in ("neo4j", "sparql"):
        return []

    if t == "openapi":
        return ["openapi"]

    # RDBMS — requires a live driver in source_pools
    if not pool.has(source_id):
        return None

    try:
        if t == "postgresql":
            result = await pool.execute(
                source_id,
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT IN ('information_schema','pg_catalog','pg_toast','mv_cache') "
                "ORDER BY schema_name",
            )
            return [row[0] for row in result.rows]

        if t in ("mysql", "mariadb"):
            result = await pool.execute(source_id, "SHOW DATABASES")
            return [row[0] for row in result.rows if row[0] not in _MYSQL_SYSTEM_DBS]

        if t == "sqlserver":
            result = await pool.execute(
                source_id,
                "SELECT name FROM sys.schemas WHERE name NOT IN ("
                "'sys','INFORMATION_SCHEMA','guest','db_owner','db_accessadmin',"
                "'db_securityadmin','db_ddladmin','db_backupoperator','db_datareader',"
                "'db_datawriter','db_denydatareader','db_denydatawriter') ORDER BY name",
            )
            return [row[0] for row in result.rows]

        if t == "duckdb":
            result = await pool.execute(
                source_id,
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
            )
            return [row[0] for row in result.rows]

    except Exception:
        return None

    return None


def _openapi_is_table(query) -> bool:
    """Return True if this OpenAPI GET operation can return multiple rows.

    Accepts:
    - response_schema type == "array"
    - response_schema is an object with exactly one property of type "array"
      (pagination wrapper pattern)
    """
    schema = query.response_schema
    if schema is None:
        return False
    # Direct array
    if schema.get("type") == "array":
        return True
    # Pagination wrapper: object with exactly one array-typed property
    if schema.get("type") == "object":
        props = schema.get("properties") or {}
        array_props = [v for v in props.values() if isinstance(v, dict) and v.get("type") == "array"]
        if len(array_props) == 1:
            return True
    return False


def _unwrap_gql_type(type_node: dict) -> dict:
    """Unwrap NON_NULL wrappers to get the inner type node."""
    while type_node and type_node.get("kind") == "NON_NULL":
        type_node = type_node.get("ofType") or {}
    return type_node


def _gql_field_returns_list(field: dict) -> bool:
    """Return True if a GraphQL query field's return type is LIST (after unwrapping NON_NULL)."""
    type_node = _unwrap_gql_type(field.get("type") or {})
    return type_node.get("kind") == "LIST"


async def native_tables(
    source_id: str,
    source_type: str,
    schema_name: str,
    pool: "SourcePool",
    config_conn,
    state,
) -> "list[AvailableTableType] | None":
    """Return table list via native introspection or None to fall back to Trino."""
    from provisa.api.admin.types import AvailableTableType

    t = source_type.lower()

    # ── OpenAPI ──────────────────────────────────────────────────────────────
    if t == "openapi":
        if schema_name != "openapi":
            return []
        spec_info = getattr(state, "openapi_specs", {}).get(source_id)
        if spec_info is None:
            return None
        from provisa.openapi.mapper import parse_spec
        queries, _ = parse_spec(spec_info["spec"])
        return [
            AvailableTableType(name=q.operation_id, comment=q.summary)
            for q in queries
            if _openapi_is_table(q)
        ]

    # ── GraphQL / GraphQL Remote ─────────────────────────────────────────────
    if t in ("graphql", "graphql_remote"):
        if schema_name != "default":
            return []
        gql_sources = getattr(state, "graphql_remote_sources", {})
        reg = gql_sources.get(source_id)
        if reg is None:
            return None
        url = reg.get("url") or reg.get("endpoint") or ""
        auth = reg.get("auth") or reg.get("auth_config")
        if not url:
            return None
        try:
            from provisa.graphql_remote.introspect import introspect_schema
            schema = await introspect_schema(url, auth)
        except Exception:
            return None
        # Find Query type
        query_type_name = (schema.get("queryType") or {}).get("name") or "Query"
        types_by_name = {tp["name"]: tp for tp in (schema.get("types") or [])}
        query_type = types_by_name.get(query_type_name)
        if query_type is None:
            return []
        fields = query_type.get("fields") or []
        return [
            AvailableTableType(name=f["name"], comment=f.get("description"))
            for f in fields
            if _gql_field_returns_list(f)
        ]

    # ── gRPC / gRPC Remote ───────────────────────────────────────────────────
    if t in ("grpc", "grpc_remote"):
        if schema_name != "default":
            return []
        grpc_sources = getattr(state, "grpc_remote_sources", {})
        reg = grpc_sources.get(source_id)
        if reg is None:
            return None
        proto_text = reg.get("proto_text") or ""
        if not proto_text:
            return None
        try:
            from provisa.grpc_remote.loader import parse_proto_text
            proto_dict = parse_proto_text(proto_text)
        except Exception:
            return None
        messages = proto_dict.get("messages") or {}
        results: list[AvailableTableType] = []
        for service in (proto_dict.get("services") or []):
            for method in (service.get("methods") or []):
                is_streaming = method.get("server_streaming", False)
                if is_streaming:
                    results.append(AvailableTableType(name=method["name"], comment=None))
                    continue
                # Check if response message has any repeated field
                output_type = method.get("output_type", "")
                response_fields = messages.get(output_type) or []
                if any(f.get("repeated") for f in response_fields):
                    results.append(AvailableTableType(name=method["name"], comment=None))
        return results

    # ── Kafka ─────────────────────────────────────────────────────────────────
    if t == "kafka":
        if schema_name != "default":
            return []
        try:
            rows = await config_conn.fetch(
                "SELECT topic FROM kafka_topics WHERE source_id = $1", source_id
            )
            return [AvailableTableType(name=row["topic"], comment=None) for row in rows]
        except Exception:
            return None

    # ── Neo4j / SPARQL ────────────────────────────────────────────────────────
    if t in ("neo4j", "sparql"):
        return []

    # ── RDBMS ─────────────────────────────────────────────────────────────────
    if not pool.has(source_id):
        return None

    try:
        if t == "postgresql":
            result = await pool.execute(
                source_id,
                "SELECT table_name, obj_description("
                "(quote_ident(table_schema)||'.'||quote_ident(table_name))::regclass, 'pg_class') "
                "FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name",
                [schema_name],
            )
            return [AvailableTableType(name=row[0], comment=row[1]) for row in result.rows]

        if t in ("mysql", "mariadb"):
            result = await pool.execute(
                source_id,
                "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
                [schema_name],
            )
            return [AvailableTableType(name=row[0], comment=row[1] or None) for row in result.rows]

        if t == "sqlserver":
            result = await pool.execute(
                source_id,
                "SELECT TABLE_NAME, NULL FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
                [schema_name],
            )
            return [AvailableTableType(name=row[0], comment=None) for row in result.rows]

        if t == "duckdb":
            result = await pool.execute(
                source_id,
                "SELECT table_name, NULL FROM information_schema.tables "
                "WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
                [schema_name],
            )
            return [AvailableTableType(name=row[0], comment=None) for row in result.rows]

    except Exception:
        return None

    return None
