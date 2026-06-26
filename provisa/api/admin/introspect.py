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

# Requirements: REQ-012, REQ-250, REQ-252, REQ-295, REQ-307, REQ-314, REQ-322, REQ-147

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.api.admin.types import AvailableTableType
    from provisa.executor.pool import SourcePool

_MYSQL_SYSTEM_DBS = {"information_schema", "mysql", "performance_schema", "sys"}
_SQLSERVER_SYSTEM_SCHEMAS = {
    "sys",
    "INFORMATION_SCHEMA",
    "guest",
    "db_owner",
    "db_accessadmin",
    "db_securityadmin",
    "db_ddladmin",
    "db_backupoperator",
    "db_datareader",
    "db_datawriter",
    "db_denydatareader",
    "db_denydatawriter",
}
_PG_SYSTEM_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast", "public"}

PROVISA_INTERNAL_SCHEMAS: frozenset[str] = frozenset(
    {
        "platform",
        "audit",
    }
)


def is_provisa_internal(schema: str) -> bool:
    """True for any provisa-managed schema that should be hidden from users."""
    return schema in PROVISA_INTERNAL_SCHEMAS or schema.startswith("org_")


PROVISA_INTERNAL_TABLES: frozenset[str] = frozenset(
    {
        "sources",
        "domains",
        "naming_rules",
        "registered_tables",
        "table_columns",
        "relationships",
        "roles",
        "rls_rules",
        "materialized_views",
        "mv_refresh_log",
        "relationship_candidates",
        "kafka_sources",
        "kafka_topics",
        "kafka_sinks",
        "api_sources",
        "api_endpoints",
        "api_endpoint_candidates",
        "live_query_state",
        "tracked_functions",
        "tracked_webhooks",
        "table_meta_links",
        "file_source_mtimes",
        "orgs",
        "user_profiles",
        "user_org_memberships",
        "local_users",
        "user_role_assignments",
        "org_invites",
        "query_audit_log",
        "tenants",
        "tenant_config",
        "source_catalog_cache",
        "iceberg_tables",
        "iceberg_namespace_properties",
    }
)


async def native_schemas(  # REQ-012, REQ-250, REQ-252
    source_id: str,
    source_type: str,
    pool: "SourcePool",
    config_conn,
) -> list[str] | None:
    """Return schema list via native introspection or None to fall back to Trino."""
    t = source_type.lower()

    if t in ("graphql", "graphql_remote"):
        return ["graphql"]

    if t in ("grpc", "grpc_remote"):
        return ["grpc"]

    if t == "kafka":
        return ["kafka"]

    if t == "neo4j":
        return ["neo4j"]

    if t == "sparql":
        return ["sparql"]

    if t == "openapi":
        return ["openapi"]

    if t == "sqlite":
        return ["main"]

    if t == "govdata":
        row = await config_conn.fetchrow("SELECT database FROM sources WHERE id = $1", source_id)
        if row and row["database"]:
            return [s.strip().lower() for s in row["database"].split(",") if s.strip()]
        return []

    # RDBMS — requires a live driver in source_pools
    if not pool.has(source_id):
        return None

    try:
        if t == "postgresql":
            pg_exclude = "','".join(sorted(_PG_SYSTEM_SCHEMAS))
            result = await pool.execute(
                source_id,
                f"SELECT schema_name FROM information_schema.schemata "
                f"WHERE schema_name NOT IN ('{pg_exclude}') "
                f"ORDER BY schema_name",
            )
            return [row[0] for row in result.rows]

        if t in ("mysql", "mariadb"):
            result = await pool.execute(source_id, "SHOW DATABASES")
            return [row[0] for row in result.rows if row[0] not in _MYSQL_SYSTEM_DBS]

        if t == "sqlserver":
            ss_exclude = "','".join(sorted(_SQLSERVER_SYSTEM_SCHEMAS))
            result = await pool.execute(
                source_id,
                f"SELECT name FROM sys.schemas WHERE name NOT IN ('{ss_exclude}') ORDER BY name",
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
    """Return True if this OpenAPI GET operation is a table candidate.

    All GET operations with non-null responses qualify: the execution layer
    auto-wraps single-object responses as [item] so every GET can be queried
    as a table.
    """
    return query.response_schema is not None


def _unwrap_gql_type(type_node: dict) -> dict:
    """Unwrap NON_NULL wrappers to get the inner type node."""
    while type_node and type_node.get("kind") == "NON_NULL":
        type_node = type_node.get("ofType") or {}
    return type_node


def _gql_field_returns_list(field: dict) -> bool:
    """Return True if a GraphQL query field's return type is LIST (after unwrapping NON_NULL)."""
    type_node = _unwrap_gql_type(field.get("type") or {})
    return type_node.get("kind") == "LIST"


async def _native_tables_openapi(  # REQ-314, REQ-316
    source_id: str,
    schema_name: str,
    state,
) -> "list[AvailableTableType] | None":
    from provisa.api.admin.types import AvailableTableType

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


async def _native_tables_graphql(  # REQ-307, REQ-308
    source_id: str,
    schema_name: str,
    config_conn,
    state,
) -> "list[AvailableTableType] | None":
    from provisa.api.admin.types import AvailableTableType

    if schema_name != "graphql":
        return []
    gql_sources = getattr(state, "graphql_remote_sources", {})
    reg = gql_sources.get(source_id)
    if reg is not None:
        url = reg.get("url") or reg.get("endpoint") or ""
        auth = reg.get("auth") or reg.get("auth_config")
    else:
        # Source not yet in state (no registered tables) — query the physical endpoint directly.
        row = await config_conn.fetchrow("SELECT path FROM sources WHERE id = $1", source_id)
        url = (row["path"] or "") if row else ""
        auth = None
    if not url:
        return []
    try:
        from provisa.graphql_remote.introspect import introspect_schema

        schema = await introspect_schema(url, auth)
    except Exception:
        return []
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


async def _native_tables_grpc(  # REQ-322, REQ-323, REQ-325
    source_id: str,
    schema_name: str,
    state,
) -> "list[AvailableTableType] | None":
    from provisa.api.admin.types import AvailableTableType

    if schema_name != "grpc":
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
    for service in proto_dict.get("services") or []:
        for method in service.get("methods") or []:
            is_streaming = method.get("server_streaming", False)
            if is_streaming:
                results.append(AvailableTableType(name=method["name"], comment=None))
                continue
            output_type = method.get("output_type", "")
            response_fields = messages.get(output_type) or []
            if any(f.get("repeated") for f in response_fields):
                results.append(AvailableTableType(name=method["name"], comment=None))
    return results


async def _native_tables_kafka(  # REQ-147
    source_id: str,
    schema_name: str,
    config_conn,
) -> "list[AvailableTableType] | None":
    from provisa.api.admin.types import AvailableTableType

    if schema_name != "kafka":
        return []
    try:
        rows = await config_conn.fetch(
            "SELECT topic FROM kafka_topics WHERE source_id = $1", source_id
        )
        return [AvailableTableType(name=row["topic"], comment=None) for row in rows]
    except Exception:
        return None


async def _native_tables_sqlite(
    source_id: str,
    schema_name: str,
    config_conn,
) -> "list[AvailableTableType] | None":
    import sqlite3 as _sqlite3

    from provisa.api.admin.types import AvailableTableType

    if schema_name != "main":
        return []
    row = await config_conn.fetchrow("SELECT path FROM sources WHERE id = $1", source_id)
    if not row or not row["path"]:
        return None
    sq = _sqlite3.connect(row["path"])
    try:
        names = [
            r[0]
            for r in sq.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        ]
    finally:
        sq.close()
    return [AvailableTableType(name=n, comment=None) for n in names]


async def _native_tables_govdata(
    source_id: str,
    schema_name: str,
    config_conn,
) -> "list[AvailableTableType] | None":
    import asyncio as _asyncio
    import logging as _logging

    from provisa.api.admin.types import AvailableTableType
    from provisa.core.models import GovDataSource, GovDataSubject
    from provisa.core.secrets import resolve_secrets as _resolve_secrets
    from provisa.govdata.source import fetch_tables as _fetch_tables

    schema_lower = schema_name.lower()

    cred_row = await config_conn.fetchrow("SELECT username FROM sources WHERE id = $1", source_id)
    api_key = _resolve_secrets((cred_row["username"] or "") if cred_row else "")

    gds = GovDataSource(
        id=source_id,
        subject=GovDataSubject.all,
        govdata_schemas=[schema_lower],
        domain_id="default",
        api_key=api_key,
    )

    try:
        loop = _asyncio.get_running_loop()
        names = await loop.run_in_executor(None, _fetch_tables, gds, schema_lower)
        return [AvailableTableType(name=n, comment=None) for n in names]
    except Exception as _e:
        _logging.getLogger(__name__).warning("govdata native_tables FAILED: %s", _e, exc_info=True)
        return None


async def _native_tables_rdbms(  # REQ-012, REQ-252
    source_id: str,
    source_type: str,
    schema_name: str,
    pool: "SourcePool",
) -> "list[AvailableTableType] | None":
    from provisa.api.admin.types import AvailableTableType

    if not pool.has(source_id):
        return None

    t = source_type.lower()
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


async def native_tables(  # REQ-012, REQ-250, REQ-252, REQ-295, REQ-307, REQ-314, REQ-322, REQ-147
    source_id: str,
    source_type: str,
    schema_name: str,
    pool: "SourcePool",
    config_conn,
    state,
) -> "list[AvailableTableType] | None":
    """Return table list via native introspection or None to fall back to Trino."""
    t = source_type.lower()

    if t == "openapi":
        return await _native_tables_openapi(source_id, schema_name, state)

    if t in ("graphql", "graphql_remote"):
        return await _native_tables_graphql(source_id, schema_name, config_conn, state)

    if t in ("grpc", "grpc_remote"):
        return await _native_tables_grpc(source_id, schema_name, state)

    if t == "kafka":
        return await _native_tables_kafka(source_id, schema_name, config_conn)

    if t in ("neo4j", "sparql"):
        return []

    if t == "sqlite":
        return await _native_tables_sqlite(source_id, schema_name, config_conn)

    if t == "govdata":
        return await _native_tables_govdata(source_id, schema_name, config_conn)

    return await _native_tables_rdbms(source_id, source_type, schema_name, pool)
