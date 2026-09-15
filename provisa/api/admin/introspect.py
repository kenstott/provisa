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

import json

from sqlalchemy import select

from provisa.core.schema_org import kafka_topics, sources

if TYPE_CHECKING:
    from provisa.cassandra.fetch import CassandraConnection
    from provisa.elasticsearch.fetch import ESConnection
    from provisa.prometheus.fetch import PrometheusConnection
    from provisa.redis.fetch import RedisConnection
    from provisa.api.admin.types import AvailableTableType
    from provisa.core.database import Connection
    from provisa.executor.pool import SourcePool


async def _source_database(source_id: str, config_conn: "Connection") -> str:
    """The source's stored ``database`` field — Databricks (Unity Catalog name) and BigQuery
    (project) need it to qualify ``information_schema``; unlike postgresql/trino, their connection
    has no single ambient default catalog the way a plain ``information_schema.schemata`` query
    could rely on."""
    result = await config_conn.execute_core(
        select(sources.c.database).where(sources.c.id == source_id)
    )
    row = result.fetchone()
    return row[0] if row and row[0] else ""


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
_TRINO_SYSTEM_SCHEMAS = {"information_schema"}
# REQ-1753: HANA's own catalog schemas (SYS.SCHEMAS), never a user's data. "SYSTEM" is
# deliberately NOT here — it is the SYSTEM user's own default schema, exactly where an operator's
# tables typically live (verified live this session), not a HANA-internal one.
_SAPHANA_SYSTEM_SCHEMAS = {
    "SYS",
    "PUBLIC",
    "_SYS_BI",
    "_SYS_BIC",
    "_SYS_REPO",
    "_SYS_STATISTICS",
    "_SYS_TASK",
    "_SYS_XS",
    "_SYS_AFL",
    "_SYS_EPM",
    "HANA_XS_BASE",
    "SAP_REST_API",
}

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

    if t == "elasticsearch":
        return ["default"]  # REQ-1672: one schema; the index is the table

    if t == "redis":
        return ["default"]  # REQ-1675: one schema; a key prefix is the table

    if t == "cassandra":
        return await _native_schemas_cassandra(source_id, config_conn)  # REQ-1676: keyspaces

    if t == "prometheus":
        return ["default"]  # REQ-1689: the connector's fixed schema; a metric is the table

    if t == "openapi":
        return ["openapi"]

    if t == "sqlite":
        return ["main"]

    # REQ-1732: csv/parquet (the standalone SourceType, distinct from the `files` directory
    # crawler above) map to exactly ONE DuckDB view per source (DuckDBCsvConnector/
    # DuckDBParquetConnector's "view_ddl", never an "attach" — duckdb_runtime.py's
    # _attached_alias deliberately returns None for it, since there's no nested database to list
    # schemas/tables from). "main" is a fixed placeholder schema, matching sqlite's single-
    # namespace convention, so the picker has something to select rather than showing empty.
    # REQ-1743: delta_lake/iceberg (DuckDBDeltaConnector/DuckDBIcebergConnector) are SCAN-mechanism
    # connectors exactly like csv/parquet above — one view_ddl per source, no ATTACH. The REQ-1673
    # seam (duckdb_runtime._attached_alias) deliberately returns None for a view_ddl source since
    # there is no attached database to list schemas/tables from, so introspect_schemas/
    # introspect_tables come back `[]` (not None) and available_schemas/available_tables would
    # short-circuit on that empty list before ever reaching the engine-catalog fallback — the
    # schema/table pickers would stay permanently empty and Register Table could never complete
    # for these two types. Same fix as REQ-1732: a fixed "main" placeholder schema.
    if t in ("csv", "parquet", "delta_lake", "iceberg"):
        return ["main"]

    # REQ-1742: google_sheets (DuckDBGsheetsConnector, connector_duckdb.py) is a SCAN-mechanism
    # source exactly like csv/parquet above — one DuckDB view per source
    # (``CREATE VIEW "<id>" AS SELECT * FROM read_gsheet(...)``), never an "attach", so
    # duckdb_runtime.py's ``_attached_alias`` returns None for it and the REQ-1673 introspection
    # seam has no database to list schemas/tables from. Before this branch existed, google_sheets
    # fell all the way through to the RDBMS dispatch below, which requires a `pool.has(source_id)`
    # driver pool entry google_sheets never has, returning None — the Register Table form's schema
    # picker was empty for every google_sheets source, with no way to ever register a table on one
    # through the UI. "main" matches csv/parquet's fixed placeholder-schema convention.
    if t == "google_sheets":
        return ["main"]

    # REQ-1745: rss/websocket/ingest (REQ-1739's UI-newly-reachable streaming types) are
    # MATERIALIZE_ONLY with no live-scannable relation — like elasticsearch/redis/prometheus above,
    # a fixed "default" schema gives the Register Table picker something to select. Without this
    # branch, available_schemas fell through to the introspect_schemas seam, which returns [] for a
    # source with no ATTACH/view_ddl connector detail — the picker never populated and no table
    # could ever be registered against these types at all.
    if t in ("rss", "websocket", "ingest"):
        return ["default"]

    if t == "files":
        # Schema is always the sql-normalised source-id (matches pgwire_replica.schema_name())
        return [source_id.replace("-", "_")]

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

    if t in ("mysql", "mariadb", "tidb"):
        # tidb speaks the identical MySQL wire protocol (REQ-950) — was missing from this tuple,
        # so a tidb source fell through to `return None` below, then available_schemas' engine-
        # catalog fallback silently returned [] (the catalog does not exist pre-registration,
        # discovery_fallback swallows the failure) — the schema picker was permanently empty.
        result = await pool.execute(source_id, "SHOW DATABASES")
        return [row[0] for row in result.rows if row[0] not in _MYSQL_SYSTEM_DBS]

    if t == "sqlserver":
        ss_exclude = "','".join(sorted(_SQLSERVER_SYSTEM_SCHEMAS))
        result = await pool.execute(
            source_id,
            f"SELECT name FROM sys.schemas WHERE name NOT IN ('{ss_exclude}') ORDER BY name",
        )
        return [row[0] for row in result.rows]

    if t == "saphana":
        # REQ-1753: saphana's direct driver is the generic SQLAlchemyDriver (registry.py's
        # _SQLALCHEMY_FALLBACK), same as trino's — $1, not ? or %s. SYS.SCHEMAS is HANA's own
        # catalog view (there is no information_schema); this branch was the missing piece that
        # made a saphana source connectable at all (REQ-1753's registry.py fix) but its schema
        # picker still empty without it.
        hana_exclude = "','".join(sorted(_SAPHANA_SYSTEM_SCHEMAS))
        result = await pool.execute(
            source_id,
            f"SELECT SCHEMA_NAME FROM SYS.SCHEMAS WHERE SCHEMA_NAME NOT IN ('{hana_exclude}') "
            "ORDER BY SCHEMA_NAME",
        )
        return [row[0] for row in result.rows]

    if t == "duckdb":
        # REQ-1746: DuckDBDriver.connect() (source_pools) opens the attached .duckdb file
        # DIRECTLY, so this connection always carries "system"/"temp" alongside the file's own
        # catalog (named after the file, e.g. "widgets") — every one of them has its own "main"
        # schema. An unfiltered information_schema.schemata scan returns "main" once per catalog
        # (verified live: 3x for a fresh file), which the Register Table schema picker then
        # renders as duplicate <option value="main"> entries (React key collision). Scope to the
        # file's own catalog — current_database() is the connection's default/current one, i.e.
        # the attached file itself, matching what native_tables' "duckdb" branch below then reads.
        result = await pool.execute(
            source_id,
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE catalog_name = current_database() ORDER BY schema_name",
        )
        return [row[0] for row in result.rows]

    # REQ-1732: trino-as-a-SOURCE (a remote coordinator Provisa reads directly and lands, distinct
    # from trino-as-ENGINE) has a real DIRECT driver (executor/drivers/registry.py's `_make_trino`,
    # SQLAlchemyDriver) and so a live SourcePool entry, but was missing from this dispatch — every
    # branch above it falls through to `return None`, and the generic engine-catalog fallback in
    # available_schemas queries the ACTIVE ENGINE's catalog for this source, which does not exist
    # until a table on it is registered (REQ-1673's "seam" only covers ATTACH-mechanism sources on
    # a native engine) — so a trino source's schema picker was empty before any table could ever be
    # registered on it. Trino's information_schema is ANSI-standard, same shape as postgresql's.
    if t == "trino":
        result = await pool.execute(
            source_id,
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
        )
        return [row[0] for row in result.rows if row[0] not in _TRINO_SYSTEM_SCHEMAS]

    # Same "no catalog exists pre-registration" gap as trino above, for the warehouse DIRECT
    # drivers (executor/drivers/snowflake.py, databricks.py, bigquery.py, mssql_warehouse.py —
    # REQ-987/988): on an engine that has no live ATTACH connector for these (e.g. DuckDB, which
    # only ever lands them via WarehouseNativeConnector), the REQ-1673 seam's _attached_alias has
    # no "attach" details to key off and always returns [] — so these 4 source types' Register
    # Table schema picker never showed anything until a table was registered by some other means.
    if t == "snowflake":
        result = await pool.execute(
            source_id,
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
        )
        return [row[0] for row in result.rows if row[0] != "INFORMATION_SCHEMA"]

    if t == "databricks":
        # Unity Catalog information_schema is catalog-qualified; the connection carries no default
        # catalog (databricks.py's DatabricksDriver.connect ignores `database`), so the source's
        # stored catalog name has to be read back to qualify the query.
        catalog = await _source_database(source_id, config_conn)
        if not catalog:
            return None
        result = await pool.execute(
            source_id,
            f"SELECT schema_name FROM `{catalog}`.information_schema.schemata ORDER BY schema_name",
        )
        return [row[0] for row in result.rows if row[0] != "information_schema"]

    if t == "bigquery":
        # BigQuery's project-level INFORMATION_SCHEMA.SCHEMATA lists every dataset in the project
        # (bigquery.py's BigQueryDriver.connect scopes the client to the source's project).
        project = await _source_database(source_id, config_conn)
        if not project:
            return None
        result = await pool.execute(
            source_id, f"SELECT schema_name FROM `{project}`.INFORMATION_SCHEMA.SCHEMATA"
        )
        return sorted(row[0] for row in result.rows)

    if t == "hiveserver2":
        # REQ-1731 gap: hiveserver2 (executor/drivers/hive.py's HiveDriver) is a real DIRECT
        # driver, so it lives in source_pools like postgresql/mysql above, but had no dispatch
        # branch here — every RDBMS branch above it is an explicit `if t == ...`, so it fell to
        # `return None` and the generic engine-catalog fallback in available_schemas, which has no
        # ATTACH for hiveserver2 on a plain DuckDB core-lane engine. The Register Table schema
        # picker stayed permanently empty pre-registration — same class of gap already fixed above
        # for trino/snowflake/databricks/bigquery/saphana. Verified live against a stock HS2
        # (apache/hive:4.0.0): `SHOW SCHEMAS` returns one `database_name` column, same rows as
        # `SHOW DATABASES` (HS2 treats them as synonyms), no system schemas to exclude.
        result = await pool.execute(source_id, "SHOW SCHEMAS")
        return [row[0] for row in result.rows]

    if t == "exasol":
        # REQ-1731 gap: same missing-dispatch-branch symptom as hiveserver2 above — ExasolDriver
        # (executor/drivers/exasol.py, via pyexasol) is a real DIRECT driver with a source_pools
        # entry, but no branch here at all. EXA_SCHEMAS is Exasol's own system-catalog view for
        # schemas (the same one pyexasol's own ext.py list_schemas() helper reads); EXASOL_SYSTEM
        # schemas (SYS/EXA_STATISTICS) are excluded the same way the postgresql/sqlserver branches
        # above exclude their own catalog schemas.
        result = await pool.execute(
            source_id,
            "SELECT schema_name FROM EXA_SCHEMAS "
            "WHERE schema_name NOT IN ('SYS', 'EXA_STATISTICS') ORDER BY schema_name",
        )
        return [row[0] for row in result.rows]

    if t in ("fabric", "synapse"):
        # T-SQL, same shape as the sqlserver branch above — the connection is already scoped to
        # the source's one warehouse database (mssql_warehouse.py's MssqlWarehouseDriver.connect).
        fab_exclude = "','".join(sorted(_SQLSERVER_SYSTEM_SCHEMAS))
        result = await pool.execute(
            source_id,
            f"SELECT name FROM sys.schemas WHERE name NOT IN ('{fab_exclude}') ORDER BY name",
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


async def _native_tables_files(
    source_id: str,
    schema_name: str,
    config_conn: "Connection",
) -> "list[AvailableTableType] | None":
    from pathlib import Path as _Path

    from provisa.api.admin.types import AvailableTableType

    result = await config_conn.execute_core(select(sources.c.path).where(sources.c.id == source_id))
    row = result.fetchone()
    if not row or not row[0]:
        return None

    glob_path: str = row[0]
    # Derive the directory by taking the non-glob prefix of the path.
    parts = _Path(glob_path).parts
    dir_parts: list[str] = []
    for p in parts:
        if any(c in p for c in ("*", "?", "[")):
            break
        dir_parts.append(p)
    if not dir_parts:
        return None
    directory = _Path(*dir_parts) if len(dir_parts) > 1 else _Path(dir_parts[0])
    if not directory.is_dir():
        return None

    stems = sorted(p.stem for p in directory.rglob("*.csv") if p.is_file())
    return [AvailableTableType(name=stem, comment=None) for stem in stems]


async def _native_tables_sqlite(
    source_id: str,
    schema_name: str,
    config_conn: "Connection",
) -> "list[AvailableTableType] | None":
    from provisa.api.admin.types import AvailableTableType
    from provisa.federation import connector_sqlite

    if schema_name != "main":
        return []
    result = await config_conn.execute_core(select(sources.c.path).where(sources.c.id == source_id))
    row = result.fetchone()
    if not row or not row[0]:
        return None
    names = connector_sqlite.table_names(row[0])
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


def _es_connection_for(row: dict, state) -> "ESConnection":  # REQ-1672
    """The HTTP connection for an elasticsearch source row. The password is a secret reference on
    the config Source (the sources table carries none), resolved when the config declares it."""
    from provisa.core.secrets import resolve_secrets
    from provisa.elasticsearch.fetch import ESConnection

    mapping = row.get("mapping") or {}
    if isinstance(mapping, str):
        mapping = json.loads(mapping)
    cfg_src = next(
        (
            s
            for s in getattr(getattr(state, "config", None), "sources", []) or []
            if s.id == row["id"]
        ),
        None,
    )
    password = resolve_secrets(getattr(cfg_src, "password", "") or "") if cfg_src else ""
    return ESConnection.build(
        resolve_secrets(row.get("host") or "localhost"),
        int(row.get("port") or 9200),
        tls=bool(mapping.get("tls", False)),
        username=row.get("username") or None,
        password=password or None,
    )


async def _es_source_row(source_id: str, config_conn: "Connection") -> dict | None:
    result = await config_conn.execute_core(
        select(
            sources.c.id, sources.c.host, sources.c.port, sources.c.username, sources.c.mapping
        ).where(sources.c.id == source_id)
    )
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def _native_tables_elasticsearch(  # REQ-1672
    source_id: str, schema_name: str, config_conn: "Connection", state
) -> "list[AvailableTableType] | None":
    """The mapping DSL's tables when the source declares any, else the live indices."""
    import asyncio as _asyncio

    from provisa.api.admin.types import AvailableTableType
    from provisa.elasticsearch.fetch import list_indices

    if schema_name != "default":
        return []
    row = await _es_source_row(source_id, config_conn)
    if row is None:
        return None
    mapping = row.get("mapping") or {}
    if isinstance(mapping, str):
        mapping = json.loads(mapping)
    declared = [t["name"] for t in mapping.get("tables", []) if t.get("name")]
    if declared:
        return [AvailableTableType(name=n, comment=None) for n in declared]
    conn = _es_connection_for(row, state)
    names = await _asyncio.to_thread(list_indices, conn)
    return [AvailableTableType(name=n, comment=None) for n in names]


def _redis_connection_for(row: dict, state) -> "RedisConnection":  # REQ-1675
    """The redis-py connection for a redis source row; the password is the config Source's secret
    reference (the sources table carries none)."""
    from provisa.core.secrets import resolve_secrets
    from provisa.redis.fetch import RedisConnection

    cfg_src = next(
        (
            s
            for s in getattr(getattr(state, "config", None), "sources", []) or []
            if s.id == row["id"]
        ),
        None,
    )
    password = resolve_secrets(getattr(cfg_src, "password", "") or "") if cfg_src else ""
    return RedisConnection(
        host=resolve_secrets(row.get("host") or "localhost"),
        port=int(row.get("port") or 6379),
        password=password or None,
    )


async def _native_tables_redis(  # REQ-1675
    source_id: str, schema_name: str, config_conn: "Connection", state
) -> "list[AvailableTableType] | None":
    """The mapping DSL's tables when the source declares any, else the key prefixes present."""
    import asyncio as _asyncio

    from provisa.api.admin.types import AvailableTableType
    from provisa.redis.fetch import list_prefixes

    if schema_name != "default":
        return []
    row = await _es_source_row(source_id, config_conn)  # the same columns a redis row needs
    if row is None:
        return None
    mapping = row.get("mapping") or {}
    if isinstance(mapping, str):
        mapping = json.loads(mapping)
    declared = [t["name"] for t in mapping.get("tables", []) if t.get("name")]
    if declared:
        return [AvailableTableType(name=n, comment=None) for n in declared]
    names = await _asyncio.to_thread(list_prefixes, _redis_connection_for(row, state))
    return [AvailableTableType(name=n, comment=None) for n in names]


def _cassandra_connection_for(row: dict, state) -> "CassandraConnection":  # REQ-1676
    """The CQL connection for a cassandra source row; the password is the config Source's secret
    reference (the sources table carries none)."""
    from provisa.cassandra.fetch import CassandraConnection
    from provisa.core.secrets import resolve_secrets

    cfg_src = next(
        (
            s
            for s in getattr(getattr(state, "config", None), "sources", []) or []
            if s.id == row["id"]
        ),
        None,
    )
    password = resolve_secrets(getattr(cfg_src, "password", "") or "") if cfg_src else ""
    return CassandraConnection.build(
        resolve_secrets(row.get("host") or "localhost"),
        int(row.get("port") or 9042),
        username=row.get("username") or None,
        password=password or None,
    )


async def _native_schemas_cassandra(source_id: str, config_conn: "Connection") -> list[str] | None:
    import asyncio as _asyncio

    from provisa.api.app import state
    from provisa.cassandra.fetch import list_keyspaces

    row = await _es_source_row(source_id, config_conn)
    if row is None:
        return None
    return await _asyncio.to_thread(list_keyspaces, _cassandra_connection_for(row, state))


async def _native_tables_cassandra(  # REQ-1676
    source_id: str, schema_name: str, config_conn: "Connection", state
) -> "list[AvailableTableType] | None":
    import asyncio as _asyncio

    from provisa.api.admin.types import AvailableTableType
    from provisa.cassandra.fetch import list_tables

    row = await _es_source_row(source_id, config_conn)
    if row is None:
        return None
    names = await _asyncio.to_thread(
        list_tables, _cassandra_connection_for(row, state), schema_name
    )
    return [AvailableTableType(name=n, comment=None) for n in names]


def _prometheus_connection_for(row: dict, state) -> "PrometheusConnection":  # REQ-1689
    """The HTTP connection for a prometheus source row; a bearer token is the config Source's
    password secret reference (the sources table carries none)."""
    from provisa.core.secrets import resolve_secrets
    from provisa.prometheus.fetch import PrometheusConnection
    from provisa.prometheus.source import endpoint_url

    mapping = row.get("mapping") or {}
    if isinstance(mapping, str):
        mapping = json.loads(mapping)
    cfg_src = next(
        (
            s
            for s in getattr(getattr(state, "config", None), "sources", []) or []
            if s.id == row["id"]
        ),
        None,
    )
    token = resolve_secrets(getattr(cfg_src, "password", "") or "") if cfg_src else ""
    return PrometheusConnection.build(
        resolve_secrets(endpoint_url(row.get("host"), row.get("port"), mapping)),
        token=token or None,
    )


async def _native_tables_prometheus(  # REQ-1689
    source_id: str, schema_name: str, config_conn: "Connection", state
) -> "list[AvailableTableType] | None":
    """The mapping DSL's tables when the source declares any, else the metric names."""
    import asyncio as _asyncio

    from provisa.api.admin.types import AvailableTableType
    from provisa.prometheus.fetch import list_metrics

    if schema_name != "default":
        return []
    row = await _es_source_row(source_id, config_conn)
    if row is None:
        return None
    mapping = row.get("mapping") or {}
    if isinstance(mapping, str):
        mapping = json.loads(mapping)
    declared = [t["name"] for t in mapping.get("tables", []) if t.get("name")]
    if declared:
        return [AvailableTableType(name=n, comment=None) for n in declared]
    names = await _asyncio.to_thread(list_metrics, _prometheus_connection_for(row, state))
    return [AvailableTableType(name=n, comment=None) for n in names]


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

        if t in ("mysql", "mariadb", "tidb"):
            # REQ-1732: `%s`, not `?` — aiomysql's paramstyle (MySQLDriver.execute only rewrites
            # `$N`, never touches a literal `?`). Verified live: passing `?` here raises
            # "not all arguments converted during string formatting" inside pymysql's own escaping
            # — silently swallowed by this function's outer `except Exception: return None`, so the
            # picker just came back empty rather than erroring. This never surfaced before because
            # mysql/mariadb are normally registered under Trino, where a real ATTACH connector
            # answers available_tables through the engine-catalog fallback instead, this broken
            # pool query never actually running. tidb (identical MySQL wire protocol) was missing
            # from this tuple too — same silent-empty-picker symptom.
            result = await pool.execute(
                source_id,
                "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
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

        if t == "saphana":
            # SYS.TABLES is HANA's own catalog view; TABLE_TYPE excludes views (COLUMN/ROW covers
            # both HANA storage engines — a view's TABLE_TYPE is neither).
            result = await pool.execute(
                source_id,
                "SELECT TABLE_NAME, COMMENTS FROM SYS.TABLES WHERE SCHEMA_NAME = $1 "
                "AND TABLE_TYPE IN ('COLUMN', 'ROW') ORDER BY TABLE_NAME",
                [schema_name],
            )
            return [AvailableTableType(name=row[0], comment=row[1] or None) for row in result.rows]

        if t == "duckdb":
            # REQ-1746: scoped to the attached file's own catalog for the same reason
            # native_schemas' "duckdb" branch is above — a DIRECT connection to the file also
            # carries "system"/"temp" catalogs alongside it.
            result = await pool.execute(
                source_id,
                "SELECT table_name, NULL FROM information_schema.tables "
                "WHERE table_catalog = current_database() AND table_schema = ? "
                "AND table_type = 'BASE TABLE' ORDER BY table_name",
                [schema_name],
            )
            return [AvailableTableType(name=row[0], comment=None) for row in result.rows]

        # REQ-1732: see native_schemas's trino branch — same "no catalog exists pre-registration"
        # gap, for tables. $1 (not `?`) because trino's direct driver is the generic
        # SQLAlchemyDriver, whose _to_named_params expects PG-style positional placeholders.
        if t == "trino":
            result = await pool.execute(
                source_id,
                "SELECT table_name, NULL FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name",
                [schema_name],
            )
            return [AvailableTableType(name=row[0], comment=None) for row in result.rows]

        # REQ-1731 gap: see native_schemas's hiveserver2 branch — same missing-dispatch-branch
        # symptom, for tables. Verified live against a stock HS2: Hive has no information_schema
        # (`SELECT ... FROM INFORMATION_SCHEMA.TABLES` raises "Table not found 'TABLES'"), so this
        # uses `SHOW TABLES IN <schema>` (impyla's HiveDriver.execute has no identifier-binding
        # paramstyle, same constraint as the snowflake/databricks/bigquery f-string branches in
        # native_tables/native_columns below — schema_name here is always one native_schemas itself
        # already returned from `SHOW SCHEMAS`, never raw user input).
        if t == "hiveserver2":
            result = await pool.execute(source_id, f"SHOW TABLES IN {schema_name}")
            return [AvailableTableType(name=row[0], comment=None) for row in result.rows]

        # REQ-1731 gap: see native_schemas's exasol branch — same missing-dispatch-branch symptom,
        # for tables. EXA_ALL_TABLES is Exasol's own system-catalog view (pyexasol's own ext.py
        # list_tables() helper reads it the same way); f-string, not a bound param, because
        # ExasolDriver.execute (executor/drivers/exasol.py) ignores `params` entirely — pyexasol
        # has no bind-parameter execute path this driver wires up, same constraint as the
        # snowflake/databricks/bigquery f-string branches elsewhere in this file. schema_name here
        # is always a value native_schemas itself already returned from EXA_SCHEMAS.
        if t == "exasol":
            result = await pool.execute(
                source_id,
                f"SELECT table_name FROM EXA_ALL_TABLES WHERE table_schema = '{schema_name}' "
                "ORDER BY table_name",
            )
            return [AvailableTableType(name=row[0], comment=None) for row in result.rows]

    except Exception:
        return None

    return None


async def native_columns(  # REQ-1732
    source_id: str,
    source_type: str,
    schema_name: str,
    table_name: str,
    pool: "SourcePool",
    config_conn: "Connection | None" = None,
) -> "list[tuple[str, str]] | None":
    """``[(column_name, data_type)]`` via native introspection, or None to fall back to the engine.

    trino/mysql/mariadb/tidb (REQ-1732/1749): postgresql/sqlserver never needed this because they
    are ATTACH-mechanism on whatever engine they are normally registered under (their own native
    engine, or Trino's own JDBC connector), so resolve_available_columns_metadata's generic
    engine-catalog fallback already sees a real catalog by the time this is called. trino-as-a-
    SOURCE has no engine that attaches it live except another Trino, and mysql/mariadb/tidb have no
    DuckDB ATTACH connector at all (verified: connector_duckdb.py has none) — this is the only path
    when the active engine doesn't natively attach the type (e.g. mysql/trino under DuckDB).
    duckdb (REQ-1749) DOES have a DuckDB ATTACH connector (DuckDBDuckdbConnector), but the ATTACH
    only happens once a table on it is registered (REQ-1673) — pre-registration, this is the only
    path there too.

    snowflake/databricks/bigquery/fabric/synapse: same gap as native_schemas — these DIRECT
    warehouse drivers have no ATTACH-mechanism seam under an engine (e.g. DuckDB) that only lands
    them, so this is the only path there too. ``config_conn`` is required for databricks/bigquery,
    which need the source's stored catalog/project to qualify information_schema."""
    t = source_type.lower()
    if not pool.has(source_id):
        return None
    if t == "trino":
        result = await pool.execute(
            source_id,
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
            [schema_name, table_name],
        )
        return [(row[0], row[1]) for row in result.rows]
    if t == "saphana":
        # REQ-1753: same "no ATTACH-mechanism seam" gap as trino above — saphana has no DuckDB
        # ATTACH connector, so this is the only path. SYS.TABLE_COLUMNS is HANA's own catalog view.
        result = await pool.execute(
            source_id,
            "SELECT COLUMN_NAME, DATA_TYPE_NAME FROM SYS.TABLE_COLUMNS "
            "WHERE SCHEMA_NAME = $1 AND TABLE_NAME = $2 ORDER BY POSITION",
            [schema_name, table_name],
        )
        return [(row[0], row[1]) for row in result.rows]
    if t == "duckdb":
        # REQ-1749: contrary to this docstring's original claim, a duckdb-type source registered
        # under the DuckDB ENGINE itself has the same pre-registration gap as trino above — the
        # engine's own ATTACH (DuckDBDuckdbConnector, alias `_src_<source_id>`) happens only once a
        # table on it is registered (REQ-1673), so resolve_available_columns_metadata's engine-
        # catalog fallback saw nothing yet and the column checkboxes never appeared. native_schemas
        # and _native_tables_rdbms's own "duckdb" branches already sidestep this via the DIRECT
        # pool connection (REQ-1746) — this mirrors that, scoped the same way to current_database().
        result = await pool.execute(
            source_id,
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog = current_database() AND table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [schema_name, table_name],
        )
        return [(row[0], row[1]) for row in result.rows]
    if t in ("mysql", "mariadb", "tidb"):
        # %s, not ?  — see _native_tables_rdbms's mysql/mariadb branch for why.
        result = await pool.execute(
            source_id,
            "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            [schema_name, table_name],
        )
        return [(row[0], row[1]) for row in result.rows]
    if t == "snowflake":
        # SnowflakeDriver.execute forwards `params` straight to snowflake-connector's cursor,
        # whose paramstyle only binds scalars into a literal SQL string (no identifier binding),
        # and pool.execute's own signature is list-params-only — schema/table names are inlined
        # instead, same as the databricks/bigquery/fabric branches below.
        result = await pool.execute(
            source_id,
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY ordinal_position",
        )
        return [(row[0], row[1]) for row in result.rows]
    if t == "databricks":
        if config_conn is None:
            return None
        catalog = await _source_database(source_id, config_conn)
        if not catalog:
            return None
        result = await pool.execute(
            source_id,
            f"SELECT column_name, data_type FROM `{catalog}`.information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY ordinal_position",
        )
        return [(row[0], row[1]) for row in result.rows]
    if t == "bigquery":
        if config_conn is None:
            return None
        project = await _source_database(source_id, config_conn)
        if not project:
            return None
        result = await pool.execute(
            source_id,
            f"SELECT column_name, data_type FROM `{project}`.`{schema_name}`.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table_name}' ORDER BY ordinal_position",
        )
        return [(row[0], row[1]) for row in result.rows]
    if t == "hiveserver2":
        # REQ-1731 gap: see _native_tables_rdbms's hiveserver2 branch — same missing-dispatch
        # symptom, for columns. `DESCRIBE <schema>.<table>` is Hive's own catalog command (no
        # information_schema); returns (col_name, data_type, comment) rows. schema_name/table_name
        # here are always values native_schemas/this same dispatch already returned from HS2 itself
        # (SHOW SCHEMAS / SHOW TABLES IN), never raw user input — same constraint noted above.
        result = await pool.execute(source_id, f"DESCRIBE {schema_name}.{table_name}")
        return [(row[0], row[1]) for row in result.rows]
    if t == "exasol":
        # REQ-1731 gap: see _native_tables_rdbms's exasol branch — same missing-dispatch symptom,
        # for columns. EXA_ALL_COLUMNS is Exasol's own system-catalog view (column_type is the
        # full rendered type, e.g. "VARCHAR(64) UTF8" — same shape pyexasol's own ext.py
        # list_columns() helper reads). f-string for the same reason noted above (ExasolDriver.
        # execute ignores `params`); schema_name/table_name are always values this same dispatch
        # already returned from EXA_SCHEMAS/EXA_ALL_TABLES.
        result = await pool.execute(
            source_id,
            "SELECT column_name, column_type FROM EXA_ALL_COLUMNS "
            f"WHERE column_schema = '{schema_name}' AND column_table = '{table_name}' "
            "ORDER BY column_ordinal_position",
        )
        return [(row[0], row[1]) for row in result.rows]
    if t in ("fabric", "synapse"):
        result = await pool.execute(
            source_id,
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            [schema_name, table_name],
        )
        return [(row[0], row[1]) for row in result.rows]
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

    if t == "elasticsearch":
        return await _native_tables_elasticsearch(source_id, schema_name, config_conn, state)

    if t == "redis":
        return await _native_tables_redis(source_id, schema_name, config_conn, state)

    if t == "cassandra":
        return await _native_tables_cassandra(source_id, schema_name, config_conn, state)

    if t == "prometheus":
        return await _native_tables_prometheus(source_id, schema_name, config_conn, state)

    if t == "sqlite":
        return await _native_tables_sqlite(source_id, schema_name, config_conn)

    # REQ-1732/REQ-1743: see native_schemas's csv/parquet/delta_lake/iceberg branch — the source IS
    # the one table, named after the source id itself (each connector's view_ddl).
    if t in ("csv", "parquet", "delta_lake", "iceberg"):
        from provisa.api.admin.types import AvailableTableType

        if schema_name != "main":
            return []
        return [AvailableTableType(name=source_id, comment=None)]

    # REQ-1742: see native_schemas's google_sheets branch — the source IS the one table, named
    # after the source id itself (DuckDBGsheetsConnector's view_ddl), same shape as csv/parquet.
    if t == "google_sheets":
        from provisa.api.admin.types import AvailableTableType

        if schema_name != "main":
            return []
        return [AvailableTableType(name=source_id, comment=None)]

    # REQ-1745: rss/websocket — one feed/socket per source — and ingest, as a matching one-table-
    # per-source placeholder (real ingest usage allows several independently-named backing tables
    # per source, per state.ingest_tables' source_id -> {table_name -> columns} shape; a genuine
    # "type a new table name" input, mirroring the neo4j/sparql custom-projection mode, is the real
    # fix for that and is left as a follow-up — this unblocks the ONE-table case, which is exactly
    # what was completely unreachable before). Named after the source id, as csv/parquet above.
    if t in ("rss", "websocket", "ingest"):
        from provisa.api.admin.types import AvailableTableType

        if schema_name != "default":
            return []
        return [AvailableTableType(name=source_id, comment=None)]

    if t == "files":
        return await _native_tables_files(source_id, schema_name, config_conn)

    if t == "govdata":
        return await _native_tables_govdata(source_id, schema_name, config_conn)

    # See native_schemas's snowflake/databricks/bigquery/fabric/synapse branches for why these
    # need their own path rather than falling through to _native_tables_rdbms's engine-catalog
    # assumption (that function has no config_conn to look up a source's catalog/project with).
    if t == "snowflake":
        from provisa.api.admin.types import AvailableTableType

        result = await pool.execute(
            source_id,
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema_name}' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name",
        )
        return [AvailableTableType(name=row[0], comment=None) for row in result.rows]

    if t == "databricks":
        from provisa.api.admin.types import AvailableTableType

        catalog = await _source_database(source_id, config_conn)
        if not catalog:
            return None
        result = await pool.execute(
            source_id,
            f"SELECT table_name FROM `{catalog}`.information_schema.tables "
            f"WHERE table_schema = '{schema_name}' ORDER BY table_name",
        )
        return [AvailableTableType(name=row[0], comment=None) for row in result.rows]

    if t == "bigquery":
        from provisa.api.admin.types import AvailableTableType

        project = await _source_database(source_id, config_conn)
        if not project:
            return None
        result = await pool.execute(
            source_id,
            f"SELECT table_name FROM `{project}`.`{schema_name}`.INFORMATION_SCHEMA.TABLES "
            "ORDER BY table_name",
        )
        return [AvailableTableType(name=row[0], comment=None) for row in result.rows]

    if t in ("fabric", "synapse"):
        from provisa.api.admin.types import AvailableTableType

        result = await pool.execute(
            source_id,
            "SELECT TABLE_NAME, NULL FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
            [schema_name],
        )
        return [AvailableTableType(name=row[0], comment=None) for row in result.rows]

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
