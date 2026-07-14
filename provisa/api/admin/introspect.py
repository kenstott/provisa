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

Returns None when no native path exists — caller falls back to the engine.
"""

# Requirements: REQ-012, REQ-250, REQ-252, REQ-295, REQ-307, REQ-314, REQ-322, REQ-147, REQ-887
# complexity-gate: allow-ble=5 reason="best-effort source-schema introspection: GraphQL SDL parse, protobuf descriptor parse, govdata native-table probe, and pg_proc routine-catalog read each return empty/None on any failure over an external/pluggable source, so a source that cannot be introspected yields no discovered schema rather than aborting source registration"

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.core.schema_org import kafka_topics, sources

if TYPE_CHECKING:
    from provisa.api.admin.types import AvailableTableType
    from provisa.core.database import Connection
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
    config_conn: "Connection",
) -> list[str] | None:
    """Return schema list via native introspection or None to fall back to the engine."""
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
        result = await config_conn.execute_core(
            select(sources.c.database).where(sources.c.id == source_id)
        )
        row = result.fetchone()
        if row and row[0]:
            return [s.strip().lower() for s in row[0].split(",") if s.strip()]
        return []

    # RDBMS — requires a live driver in source_pools
    if not pool.has(source_id):
        return None

    # Let introspection errors propagate — swallowing them here masks a real
    # source failure as an empty schema list.
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
    config_conn: "Connection",
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
        result = await config_conn.execute_core(
            select(sources.c.path).where(sources.c.id == source_id)
        )
        row = result.fetchone()
        url = (row[0] or "") if row else ""
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
    config_conn: "Connection",
) -> "list[AvailableTableType] | None":
    from provisa.api.admin.types import AvailableTableType

    if schema_name != "kafka":
        return []
    try:
        result = await config_conn.execute_core(
            select(kafka_topics.c.topic).where(kafka_topics.c.source_id == source_id)
        )
        return [AvailableTableType(name=row[0], comment=None) for row in result.fetchall()]
    except Exception:
        return None


async def _native_tables_sqlite(
    source_id: str,
    schema_name: str,
    config_conn: "Connection",
) -> "list[AvailableTableType] | None":
    import sqlite3 as _sqlite3

    from provisa.api.admin.types import AvailableTableType

    if schema_name != "main":
        return []
    result = await config_conn.execute_core(select(sources.c.path).where(sources.c.id == source_id))
    row = result.fetchone()
    if not row or not row[0]:
        return None
    sq = _sqlite3.connect(row[0])
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
    config_conn: "Connection",
) -> "list[AvailableTableType] | None":
    import asyncio as _asyncio
    import logging as _logging

    from provisa.api.admin.types import AvailableTableType
    from provisa.core.models import GovDataSource, GovDataSubject
    from provisa.core.secrets import resolve_secrets as _resolve_secrets
    from provisa.govdata.source import fetch_tables as _fetch_tables

    schema_lower = schema_name.lower()

    result = await config_conn.execute_core(
        select(sources.c.username).where(sources.c.id == source_id)
    )
    cred_row = result.fetchone()
    api_key = _resolve_secrets((cred_row[0] or "") if cred_row else "")

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
    config_conn: "Connection",
    state,
) -> "list[AvailableTableType] | None":
    """Return table list via native introspection or None to fall back to the engine."""
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


# ── Stored-procedure / routine auto-discovery (REQ-887) ──────────────────────
#
# Extends database-source introspection to discover source-resident routines
# (stored procedures + functions) from the vendor catalog and classify each as
# read-returning ("query") or write/side-effecting ("mutation"). Discovered
# routines auto-register through the existing tracked-function representation
# (REQ-205–208); see register.register_discovered_routines. Mirrors the OpenAPI
# discovery pattern (REQ-316/317) — introspection + auto-registration, no
# parallel registry.

# GraphQL scalar names — reuse the tracked-function argument type vocabulary.
_PG_TYPE_TO_GQL: dict[str, str] = {
    "smallint": "Int",
    "integer": "Int",
    "bigint": "Int",
    "int2": "Int",
    "int4": "Int",
    "int8": "Int",
    "real": "Float",
    "double precision": "Float",
    "numeric": "Float",
    "decimal": "Float",
    "float4": "Float",
    "float8": "Float",
    "boolean": "Boolean",
    "bool": "Boolean",
}


def _pg_type_to_gql(pg_type: str) -> str:
    """Map a Postgres argument type name to a GraphQL scalar type name."""
    base = pg_type.strip().lower().split("(")[0].strip()
    if base in _PG_TYPE_TO_GQL:
        return _PG_TYPE_TO_GQL[base]
    if base.startswith("timestamp") or base in ("date", "time", "timestamptz"):
        return "DateTime"
    return "String"


@dataclass
class RoutineArg:
    """A single input argument of a discovered routine."""

    name: str
    type: str  # GraphQL scalar type name


@dataclass
class DiscoveredRoutine:
    """A stored procedure / function discovered from a source's routine catalog."""

    schema_name: str
    routine_name: str
    kind: str  # "query" (read-returning) | "mutation" (write/side-effecting)
    returns_setof: bool
    arguments: list[RoutineArg] = field(default_factory=list)
    description: str | None = None


def classify_routine(prokind: str, provolatile: str) -> str:
    """Classify a Postgres routine as read-returning ("query") or side-effecting ("mutation").

    prokind: 'p' = procedure, 'f' = function, 'a' = aggregate, 'w' = window.
    provolatile: 'i' = immutable, 's' = stable, 'v' = volatile.

    Procedures always mutate (called via CALL, may run COMMIT). Functions are
    read-returning only when the planner-declared volatility is immutable/stable;
    a volatile function may side-effect, so it is treated as a mutation. This
    mirrors the REQ-887 scenario (prokind + provolatile drive the split).
    """
    if prokind == "p":
        return "mutation"
    if provolatile in ("i", "s"):
        return "query"
    return "mutation"


# proargtypes covers only IN arguments; proargnames aligns positionally with the
# leading IN args (OUT/INOUT names, if any, trail). obj_description carries the
# COMMENT ON FUNCTION text. Restricted to prokind IN ('f','p') — aggregates and
# window functions are not callable as tracked functions.
_PG_ROUTINE_SQL = (
    "SELECT n.nspname, p.proname, p.prokind::text, p.provolatile::text, p.proretset, "
    "COALESCE(p.proargnames, ARRAY[]::text[]) AS arg_names, "
    "ARRAY(SELECT format_type(t, NULL) FROM unnest(p.proargtypes) AS t) AS arg_types, "
    "obj_description(p.oid, 'pg_proc') AS description "
    "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
    "WHERE n.nspname = $1 AND p.prokind IN ('f', 'p') "
    "ORDER BY p.proname"
)


def _row_to_routine(row) -> DiscoveredRoutine:
    """Build a DiscoveredRoutine from a Postgres pg_proc catalog row."""
    schema, name, prokind, provolatile, proretset, arg_names, arg_types, description = row
    args: list[RoutineArg] = []
    names = list(arg_names or [])
    types = list(arg_types or [])
    for i, pg_type in enumerate(types):
        arg_name = names[i] if i < len(names) and names[i] else f"arg{i + 1}"
        args.append(RoutineArg(name=arg_name, type=_pg_type_to_gql(pg_type)))
    return DiscoveredRoutine(
        schema_name=schema,
        routine_name=name,
        kind=classify_routine(prokind, provolatile),
        returns_setof=bool(proretset),
        arguments=args,
        description=description,
    )


async def native_routines(  # REQ-887
    source_id: str,
    source_type: str,
    schema_name: str,
    pool: "SourcePool",
) -> "list[DiscoveredRoutine] | None":
    """Discover + classify stored procedures/functions in a schema.

    Returns None when no native routine-catalog path exists for the source type
    (caller skips routine registration — there is no engine fallback for routines).
    Introspection errors propagate: a failing catalog query is a real source
    fault, not an empty routine set.
    """
    if not pool.has(source_id):
        return None

    t = source_type.lower()
    if t == "postgresql":
        result = await pool.execute(source_id, _PG_ROUTINE_SQL, [schema_name])
        return [_row_to_routine(row) for row in result.rows]

    # Other vendors (mysql, sqlserver, oracle) not yet wired — no fallback.
    return None


async def register_discovered_routines(  # REQ-887
    conn: "Connection",
    source_id: str,
    routines: "list[DiscoveredRoutine]",
    domain_id: str = "",
) -> tuple[int, int]:
    """Auto-register discovered routines as tracked functions. Returns (registered, skipped).

    Conflict rule — explicit hand-registration wins, discovery never clobbers it:
      * If no tracked function owns the exposed name → register it.
      * If a tracked function with the same name already points at this exact
        routine (same source_id + schema + function_name) → upsert (idempotent
        re-introspection; REQ-870 preserves existing writable_by grants).
      * If a tracked function with the same name points at a *different* routine
        (a hand-registered function, or a different proc) → skip; a discovered
        routine must not overwrite an explicit registration.
    """
    from provisa.core.models import Function, FunctionArgument
    from provisa.core.repositories import function as function_repo

    registered = 0
    skipped = 0
    for r in routines:
        existing = await function_repo.get_function(conn, r.routine_name)
        if existing is not None and not (
            existing.get("source_id") == source_id
            and existing.get("schema_name") == r.schema_name
            and existing.get("function_name") == r.routine_name
        ):
            skipped += 1
            continue
        func = Function(
            name=r.routine_name,
            source_id=source_id,
            schema_name=r.schema_name,
            function_name=r.routine_name,
            returns="",
            arguments=[FunctionArgument(name=a.name, type=a.type) for a in r.arguments],
            visible_to=[],
            writable_by=[],
            domain_id=domain_id or "",
            description=r.description,
            kind=r.kind,
        )
        await function_repo.upsert_function(conn, func)
        registered += 1
    return registered, skipped
