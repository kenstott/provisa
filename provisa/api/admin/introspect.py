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
# complexity-gate: allow-ble=5 reason="best-effort source-schema introspection: GraphQL SDL parse, protobuf descriptor parse, and pg_proc routine-catalog read each return empty/None on any failure over an external/pluggable source, so a source that cannot be introspected yields no discovered schema rather than aborting source registration"

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
_PG_SYSTEM_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}
# The source types that speak PostgreSQL's wire protocol and expose its information_schema shape.
_POSTGRES_WIRE = ("postgresql", "cockroachdb", "yugabytedb", "greenplum", "redshift")
# information_schema.columns.data_type values that are markers, not type names.
_PG_COLLAPSED_TYPES = frozenset({"ARRAY", "USER-DEFINED"})
# Redshift is a Postgres 8.0 fork — same wire protocol and catalog shape as _PG_SYSTEM_SCHEMAS,
# plus its own pg_internal schema (holds temp tables), which upstream Postgres doesn't have.
_REDSHIFT_SYSTEM_SCHEMAS = _PG_SYSTEM_SCHEMAS | {"pg_internal"}
# REQ-1768: "public" was excluded here (2026-06-07) to hide Provisa's own control-plane schema when
# its backing Postgres was registered as a "postgresql"-type data source. That rationale is now
# stale: the 2026-06-26 multi-tenancy migration moved Provisa's own tables into "platform"/"audit"
# and per-org "org_<id>" schemas, and is_provisa_internal() (below) already excludes those from
# every native_schemas caller (schema_query.py). Excluding "public" unconditionally instead hid the
# default schema of every genuine external PostgreSQL source — the schema real Postgres data lives
# in for the overwhelming majority of installations — leaving the schema picker permanently empty
# for any newly-registered postgresql source. Never caught before REQ-1768 added the first e2e test
# that actually registers a plain postgresql source through the UI.
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

# Oracle's built-in accounts (a schema IS a user in Oracle). SYSTEM is deliberately NOT in this
# set even though it's one of Oracle's own accounts: demo/sources/oracle/prime.py creates
# widgets(id, name) directly under SYSTEM (matching source-to-query-generic-rdbms.spec.ts's own
# REQ-1744 precedent, schema "SYSTEM"/table "WIDGETS") rather than a dedicated application user —
# excluding it here would make the demo fixture's own table permanently unreachable through the
# picker. A real deployment should still use its own dedicated user, but the picker has no way to
# tell "SYSTEM used deliberately" from "SYSTEM used by convention" apart from this fixture's own
# choice, so it stays visible. Non-exhaustive by design (an on-prem Oracle install accumulates
# more of these via optional components) but covers every OTHER account a stock
# gvenzl/oracle-free image ships.
_ORACLE_SYSTEM_SCHEMAS = {
    "SYS",
    "OUTLN",
    "XDB",
    "ANONYMOUS",
    "APPQOSSYS",
    "AUDSYS",
    "CTXSYS",
    "DBSNMP",
    "DBSFWUSER",
    "DIP",
    "DVSYS",
    "DVF",
    "GGSYS",
    "GSMADMIN_INTERNAL",
    "GSMCATUSER",
    "GSMUSER",
    "LBACSYS",
    "MDDATA",
    "MDSYS",
    "OJVMSYS",
    "OLAPSYS",
    "ORACLE_OCM",
    "ORDDATA",
    "ORDPLUGINS",
    "ORDSYS",
    "REMOTE_SCHEDULER_AGENT",
    "SI_INFORMTN_SCHEMA",
    "SYSBACKUP",
    "SYSDG",
    "SYSKM",
    "SYSRAC",
    "WMSYS",
    "XS$NULL",
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
        "mv_build_state",
        "mv_refresh_log",
        "relationship_candidates",
        "kafka_sources",
        "kafka_topics",
        "kafka_sinks",
        "api_sources",
        "api_endpoints",
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
        # The schema a gRPC source's tables are registered under, so a table picked here is
        # registered where its readers look for it.
        return ["grpc_remote"]

    if t == "kafka":
        # REQ-147's kafka_topics/kafka_sources catalog is populated ONLY by _process_kafka_sources
        # (app_loaders.py) at boot from static config — never by the dynamic Sources-form/
        # Register-Table-form UI flow REQ-1739/1745 added for rss/websocket/ingest. A kafka source
        # created through that UI flow has zero kafka_topics rows: this is a DATA state, not a
        # different engine or a different source type — a source is engine-agnostic regardless of
        # how it was provisioned, so the dispatch here reads that state instead of assuming every
        # kafka source is config-declared. Pre-declared topics keep the "kafka" schema (unchanged,
        # REQ-147); a source with none falls through to the same "default" placeholder pattern
        # rss/websocket/ingest use (REQ-1766) — the picker isn't permanently empty either way.
        has_topics = await config_conn.execute_core(
            select(kafka_topics.c.id).where(kafka_topics.c.source_id == source_id).limit(1)
        )
        return ["kafka"] if has_topics.fetchone() else ["default"]

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

    if t == "pinot":
        return ["default"]  # REQ-1730: Pinot tables live in Trino's implicit "default" schema too

    if t == "druid":
        return ["druid"]  # REQ-1730: Druid's own fixed INFORMATION_SCHEMA.TABLES schema name

    if t == "hive_s3":
        row = await _es_source_row(source_id, config_conn)
        if row is None:
            return None
        import asyncio as _asyncio

        from provisa.hive.fetch import HiveS3Connection, list_schemas

        conn = HiveS3Connection.build(row.get("database"), row.get("mapping") or {})
        return await _asyncio.to_thread(list_schemas, conn)

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
    # kafka deliberately excluded: it already has its own dispatch above this function's `t ==
    # "kafka"` branch (REQ-147, backed by the kafka_topics/kafka_sources catalog tables) — a
    # SEPARATE, Trino-connector-backed mechanism that always intercepts kafka regardless of engine.
    if t in ("rss", "websocket", "ingest"):
        return ["default"]

    # REQ-1923: a mailbox has no schema of its own; its canonical tables are listed under one.
    if t == "google_workspace":
        from provisa.google_workspace.loader import SCHEMA

        return [SCHEMA]
    if t == "microsoft_365":
        from provisa.microsoft365.loader import SCHEMA

        return [SCHEMA]

    if t == "files":
        # Schema is always the sql-normalised source-id (matches pgwire_replica.schema_name())
        return [source_id.replace("-", "_")]

    if t == "govdata":
        # AskAmerica: the schemas the source lists (``federation.askamerica.schemas``), each a
        # schema of its adapter's one database.
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
    if t in ("postgresql", "cockroachdb", "yugabytedb", "greenplum"):
        # cockroachdb/yugabytedb/greenplum speak the identical Postgres wire protocol (same
        # grouping trino_connectors.py's _TRINO_JDBC_TYPES already uses for their Trino catalog) —
        # were missing from this tuple, so each fell through to `return None`, then
        # available_schemas' engine-catalog fallback silently returned [] (the catalog does not
        # exist pre-registration, discovery_fallback swallows the failure) — the schema picker was
        # permanently empty and Register Table could never complete for any of the three.
        pg_exclude = "','".join(sorted(_PG_SYSTEM_SCHEMAS))
        result = await pool.execute(
            source_id,
            f"SELECT schema_name FROM information_schema.schemata "
            f"WHERE schema_name NOT IN ('{pg_exclude}') "
            f"ORDER BY schema_name",
        )
        return [row[0] for row in result.rows]

    if t == "redshift":
        # Same fell-through-to-None gap as saphana/trino above — redshift's DIRECT driver is the
        # generic SQLAlchemyDriver (registry.py's _SQLALCHEMY_FALLBACK), so it never had a
        # dispatch branch here at all and the Register Table schema picker was permanently empty
        # under any engine other than Trino (verified live). Separate branch from the postgres-
        # wire group above because Redshift's own pg_internal schema (temp tables) isn't a
        # upstream-Postgres concept, so it needs its own exclude set.
        rs_exclude = "','".join(sorted(_REDSHIFT_SYSTEM_SCHEMAS))
        result = await pool.execute(
            source_id,
            f"SELECT schema_name FROM information_schema.schemata "
            f"WHERE schema_name NOT IN ('{rs_exclude}') "
            f"ORDER BY schema_name",
        )
        return [row[0] for row in result.rows]

    if t == "oracle":
        # Oracle has no information_schema; ALL_USERS is the schema-equivalent catalog (a schema
        # IS a user in Oracle). Same fell-through-to-None gap as the postgres-wire group above —
        # missing here left the Register Table schema picker permanently empty for oracle.
        oracle_exclude = "','".join(sorted(_ORACLE_SYSTEM_SCHEMAS))
        result = await pool.execute(
            source_id,
            f"SELECT username FROM all_users WHERE username NOT IN ('{oracle_exclude}') "
            f"ORDER BY username",
        )
        return [row[0] for row in result.rows]

    if t == "clickhouse":
        # ClickHouse has no DuckDB ATTACH connector (connector_duckdb.py has none) and no
        # information_schema-based path — system.databases is its own catalog (a "database" is a
        # schema). Same fell-through-to-None gap as the postgres-wire/oracle groups above.
        # ClickHouseDriver.execute always ignores params ("SQL arrives fully formed") — inline,
        # same constraint as the snowflake/databricks/bigquery branches below.
        result = await pool.execute(
            source_id,
            "SELECT name FROM system.databases "
            "WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') "
            "ORDER BY name",
        )
        return [row[0] for row in result.rows]

    if t in ("mysql", "mariadb", "tidb", "singlestore"):
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
    from provisa.api.admin._table_paging import paging_type
    from provisa.core.paging import ENDPOINT, paging_row

    return [
        AvailableTableType(
            name=q.operation_id,
            comment=q.summary,
            paging_kind=ENDPOINT,  # REQ-318: with the paging the operation suggests
            pagination=paging_type(paging_row(q.pagination)),
        )
        for q in queries
        if _openapi_is_table(q)
    ]


async def _native_tables_graphql(  # REQ-307, REQ-308, REQ-1923
    source_id: str,
    schema_name: str,
    config_conn: "Connection",
    state,
) -> "list[AvailableTableType] | None":
    """Every table a remote GraphQL source offers, registered or not: adding the source
    registers none, and this list is what the steward registers from."""
    from provisa.api.admin._graphql_table_registration import offered_tables, source_offer
    from provisa.api.admin.types import AvailableTableType

    if schema_name != "graphql":
        return []
    offered = await source_offer(state, source_id)
    if offered is None:
        return []
    return [
        AvailableTableType(
            name=t["name"],
            comment=t["description"],
            # REQ-318: a connection table may set its own row bound when it is registered.
            paging_kind="connection" if t["connection"] else None,
            paging_ceiling_rows=state.config.graphql_remote.max_rows if t["connection"] else None,
        )
        for t in offered_tables(*offered)
    ]


async def _native_tables_grpc(  # REQ-322, REQ-323, REQ-325
    source_id: str,
    schema_name: str,
    state,
) -> "list[AvailableTableType] | None":
    """Every table a gRPC source offers -- one per query method of its proto -- under the name
    it registers with. Adding the source registers none of them (REQ-322)."""
    from provisa.grpc_remote.mapper import query_table_name
    from provisa.api.admin.types import AvailableTableType

    if schema_name != "grpc_remote":
        return []
    reg = getattr(state, "grpc_remote_sources", {}).get(source_id)
    if reg is None:
        return None
    return [
        AvailableTableType(name=query_table_name(reg.get("namespace", ""), q), comment=None)
        for q in reg.get("queries") or []
    ]


async def specification_columns(  # REQ-464
    source_id: str, source_type: str, schema_name: str, state
) -> "dict[str, list[str]] | None":
    """``{table: [field, ...]}`` for a source registered from a specification — an OpenAPI
    document's operations, a gRPC proto's query methods, a GraphQL schema's tables — read from
    the specification the source was registered with, each table under the name
    ``native_tables`` lists it by. The source itself is never called: None when this process
    does not hold the specification (a plain GraphQL source whose schema has not been read
    yet is one), and for any other kind.

    The fields are the ones a registration of the table would offer: its response columns and
    its native-filter columns (``_nf_<parameter>``)."""
    t = source_type.lower()
    if t == "openapi":
        spec_info = getattr(state, "openapi_specs", {}).get(source_id)
        if spec_info is None or schema_name != "openapi":
            return None
        from provisa.openapi.mapper import parse_spec
        from provisa.openapi.register import _schema_to_columns

        queries, _ = parse_spec(spec_info["spec"])
        return {
            q.operation_id: [
                *(c["name"] for c in _schema_to_columns(q.response_schema)),
                *(f"_nf_{p['name']}" for p in (*q.path_params, *q.query_params)),
            ]
            for q in queries
            if _openapi_is_table(q)
        }
    if t in ("grpc", "grpc_remote"):
        reg = getattr(state, "grpc_remote_sources", {}).get(source_id)
        if reg is None or schema_name != "grpc_remote":
            return None
        from provisa.grpc_remote.mapper import query_table_name

        return {
            query_table_name(reg.get("namespace", ""), q): [
                *(c.name for c in q.columns),
                *(f"_nf_{c.name}" for c in q.input_fields),
            ]
            for q in reg.get("queries") or []
        }
    if t in ("graphql", "graphql_remote"):
        reg = getattr(state, "graphql_remote_sources", {}).get(source_id)
        if reg is None or schema_name != "graphql":
            return None
        if not reg.get("brand") and reg.get("schema") is None:
            return None  # its schema has not been read from its endpoint yet: no call here
        from provisa.api.admin._graphql_table_registration import (
            offered_columns,
            offered_tables,
            source_offer,
        )

        offered = await source_offer(state, source_id)
        if offered is None:
            return None
        return {
            table["name"]: [
                name for name, _type, _comment in offered_columns(*offered, table["name"])
            ]
            for table in offered_tables(*offered)
        }
    return None


async def _native_tables_kafka(  # REQ-147
    source_id: str,
    schema_name: str,
    config_conn: "Connection",
) -> "list[AvailableTableType] | None":
    from provisa.api.admin.types import AvailableTableType

    # REQ-1766: native_schemas only ever returns "default" for THIS source when it has no
    # kafka_topics rows (see that function's own comment) — mirror rss/websocket/ingest's
    # one-table-per-source placeholder rather than an empty list for that data state.
    if schema_name == "default":
        return [AvailableTableType(name=source_id, comment=None)]
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
            sources.c.id,
            sources.c.host,
            sources.c.port,
            sources.c.username,
            sources.c.database,
            sources.c.mapping,
            sources.c.federation_hints,
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


async def _native_tables_pinot(  # REQ-1730
    source_id: str, schema_name: str, config_conn: "Connection"
) -> "list[AvailableTableType] | None":
    """Every table the controller knows — Pinot has one flat table namespace (no keyspace/index
    grouping), so `schema_name` must be the fixed "default" `native_schemas` above returns."""
    import asyncio as _asyncio

    from provisa.api.admin.types import AvailableTableType
    from provisa.pinot.fetch import PinotConnection, list_tables

    if schema_name != "default":
        return []
    row = await _es_source_row(source_id, config_conn)
    if row is None:
        return None
    conn = PinotConnection.build(
        row.get("host"),
        row.get("port"),
        (row.get("federation_hints") or {}).get("pinot_broker_url"),
    )
    names = await _asyncio.to_thread(list_tables, conn)
    return [AvailableTableType(name=n, comment=None) for n in names]


async def _native_tables_druid(  # REQ-1730
    source_id: str, schema_name: str, config_conn: "Connection"
) -> "list[AvailableTableType] | None":
    """Every datasource in Druid's fixed "druid" schema."""
    import asyncio as _asyncio

    from provisa.api.admin.types import AvailableTableType
    from provisa.druid.fetch import DruidConnection, list_tables

    if schema_name != "druid":
        return []
    row = await _es_source_row(source_id, config_conn)
    if row is None:
        return None
    conn = DruidConnection.build(row.get("host"), row.get("port"))
    names = await _asyncio.to_thread(list_tables, conn)
    return [AvailableTableType(name=n, comment=None) for n in names]


async def _native_tables_hive_s3(  # REQ-1730
    source_id: str, schema_name: str, config_conn: "Connection"
) -> "list[AvailableTableType] | None":
    """Every "<table>" directory under the schema's warehouse prefix."""
    import asyncio as _asyncio

    from provisa.api.admin.types import AvailableTableType
    from provisa.hive.fetch import HiveS3Connection, list_tables

    row = await _es_source_row(source_id, config_conn)
    if row is None:
        return None
    conn = HiveS3Connection.build(row.get("database"), row.get("mapping") or {})
    names = await _asyncio.to_thread(list_tables, conn, schema_name)
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
    if t in ("postgresql", "cockroachdb", "yugabytedb", "greenplum", "redshift"):
        # cockroachdb/yugabytedb/greenplum/redshift: same wire-compatible grouping as
        # native_schemas's postgres-wire branch — were missing from this tuple, so each fell
        # through to the generic engine-catalog fallback (empty pre-registration), leaving the
        # Register Table TABLE picker permanently empty even once the schema picker itself
        # worked (verified live: cockroachdb's schema list resolves fine, but "widgets" never
        # appeared in the table select without this; redshift's regclass/pg_class support is
        # identical to upstream Postgres's for this already-schema-scoped query).
        result = await pool.execute(
            source_id,
            "SELECT table_name, obj_description("
            "(quote_ident(table_schema)||'.'||quote_ident(table_name))::regclass, 'pg_class') "
            "FROM information_schema.tables "
            "WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name",
            [schema_name],
        )
        return [AvailableTableType(name=row[0], comment=row[1]) for row in result.rows]

    if t == "oracle":
        # Oracle has no per-table comment catalog as cheap as postgres's obj_description;
        # ALL_TAB_COMMENTS carries it. Same fell-through-to-None gap as the postgres-wire
        # group above.
        result = await pool.execute(
            source_id,
            "SELECT t.table_name, c.comments FROM all_tables t "
            "LEFT JOIN all_tab_comments c ON c.owner = t.owner AND c.table_name = t.table_name "
            "WHERE t.owner = $1 ORDER BY t.table_name",
            [schema_name],
        )
        return [AvailableTableType(name=row[0], comment=row[1]) for row in result.rows]

    if t == "clickhouse":
        # Same fell-through-to-None gap as native_schemas's clickhouse branch — system.tables
        # is ClickHouse's own catalog. ClickHouseDriver.execute always ignores params (SQL
        # arrives fully formed), so schema_name is inlined — always a value native_schemas's
        # own clickhouse branch already returned from system.databases, never raw user input,
        # same constraint noted for the snowflake/databricks/bigquery branches elsewhere here.
        result = await pool.execute(
            source_id,
            f"SELECT name, comment FROM system.tables "
            f"WHERE database = '{schema_name}' AND engine NOT LIKE '%View%' ORDER BY name",
        )
        return [AvailableTableType(name=row[0], comment=row[1] or None) for row in result.rows]

    if t in ("mysql", "mariadb", "tidb", "singlestore"):
        # REQ-1732: `$1` — MySQLDriver.execute binds `$N` as PyMySQL's `%s` and escapes every
        # literal `%` first, so a `%s` written here is a literal and the binding fails. tidb
        # speaks the same wire protocol.
        result = await pool.execute(
            source_id,
            "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = $1 AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
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

    return None


#: The org-vault binding a read of a source's adapter runs inside (the source's key is a
#: reference into the org's vault).
from provisa.core.secrets_store import bound_to_request_org as _adapter_bound  # noqa: E402


async def _adapter_source(source_id: str):
    """The registered Source whose adapter is read, or a named refusal."""
    from provisa.api.admin.schema_query import _source_for_introspection

    source = await _source_for_introspection(source_id)
    if source is None:
        raise LookupError(f"source {source_id!r} is not registered")
    return source


async def _native_columns_govdata(
    source_id: str, schema_name: str, table_name: str
) -> list[tuple[str, str]]:
    """One table's columns from the adapter's information_schema (see native_tables), for a
    schema the source was given; a schema outside its list is refused by name."""
    from provisa.federation.askamerica import require_schema_served
    from provisa.federation.pgwire_replica import adapter_columns

    source = await _adapter_source(source_id)
    require_schema_served(source, schema_name)
    schema = schema_name.strip().lower()
    async with _adapter_bound():
        columns = await adapter_columns(source, schema, table_name)
    return columns.get(table_name, [])


async def native_columns(  # REQ-1732
    source_id: str,
    source_type: str,
    schema_name: str,
    table_name: str,
    pool: "SourcePool",
    config_conn: "Connection | None" = None,
) -> "list[tuple[str, str]] | None":
    """``[(column_name, data_type)]`` via native introspection, or None to fall back to the engine.

    postgresql/sqlserver: an engine that attaches them live has a catalog to list, but a source
    the operator floors has NO live attach on any engine (REQ-1912) — on Trino no catalog is
    registered for it — so its columns are listed here, through the source's own driver.
    trino/mysql/mariadb/tidb (REQ-1732/1749): trino-as-a-
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
    if t == "govdata":
        return await _native_columns_govdata(source_id, schema_name, table_name)
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
    if t == "clickhouse":
        # Same gap as native_schemas/_native_tables_rdbms's clickhouse branches — system.columns
        # is ClickHouse's own catalog. ClickHouseDriver.execute always ignores params; schema_name/
        # table_name are always values this same dispatch already returned from system.databases/
        # system.tables, never raw user input, same constraint as the branches above.
        result = await pool.execute(
            source_id,
            f"SELECT name, type FROM system.columns "
            f"WHERE database = '{schema_name}' AND table = '{table_name}' "
            f"ORDER BY position",
        )
        return [(row[0], row[1]) for row in result.rows]
    if t in _POSTGRES_WIRE:
        # cockroachdb/yugabytedb/greenplum/redshift: connector_duckdb.py has no ATTACH connector
        # for any of them, so this direct-pool path is the only one. postgresql: the path for a
        # source the bound engine holds no live attach of (REQ-1912 — a floored source has no
        # engine catalog to list). All speak the identical wire protocol/information_schema shape.
        # information_schema reports ANSI spellings ("character varying", "double precision",
        # "timestamp without time zone") that no downstream type map keys on; the IR table
        # (provisa.core.ir_types, the one native→canonical authority) resolves them — an
        # unknown type raises there rather than registering an unmappable column.
        from provisa.core.ir_types import to_ir

        result = await pool.execute(
            source_id,
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
            [schema_name, table_name],
        )
        # ``ARRAY`` and ``USER-DEFINED`` (an enum, a domain, a composite) are not type names but
        # information_schema's markers for a composite/array type, which the IR collapses to text.
        return [
            (row[0], "text" if row[1] in _PG_COLLAPSED_TYPES else to_ir(row[1]))
            for row in result.rows
        ]
    if t == "sqlserver":
        # The path for a SQL Server source the bound engine holds no live attach of (REQ-1912).
        # T-SQL's own spellings (nvarchar, datetime2, bit, uniqueidentifier, money) resolve
        # through the IR table's sqlserver overlay.
        from provisa.core.ir_types import to_ir

        result = await pool.execute(
            source_id,
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            [schema_name, table_name],
        )
        return [(row[0], to_ir(row[1], "sqlserver")) for row in result.rows]
    if t == "oracle":
        # Same gap, Oracle's own catalog view (no information_schema) — ALL_TAB_COLUMNS mirrors
        # native_tables_rdbms's ALL_TABLES/ALL_TAB_COMMENTS pairing above.
        result = await pool.execute(
            source_id,
            "SELECT column_name, data_type FROM all_tab_columns "
            "WHERE owner = $1 AND table_name = $2 ORDER BY column_id",
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
    if t in ("mysql", "mariadb", "tidb", "singlestore"):
        # `$N` — see _native_tables_rdbms's mysql/mariadb branch for why.
        result = await pool.execute(
            source_id,
            "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = $1 AND TABLE_NAME = $2 ORDER BY ORDINAL_POSITION",
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


# -- every column name of a schema, in one statement (REQ-464) -----------------------------------
#
# What the table search loads lazily: the column names of every table of a schema, fetched in
# ONE statement through the source's own driver, where ``native_columns`` costs a statement a
# table. One entry per source kind: its catalog's statement, and how the schema is given
# (bound, or written in for a driver that binds none — the names come from this same dispatch's
# schema list, as in ``native_columns``). Each returns rows of (table, column) in column order.

_PARAM_DOLLAR = (
    "SELECT table_name, column_name FROM information_schema.columns "
    "WHERE table_schema = $1 ORDER BY table_name, ordinal_position"
)
_PARAM_QMARK_UPPER = (
    "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
    "WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION"
)
#: kind -> (statement, whether the schema is bound as its one parameter).
_SCHEMA_COLUMNS_SQL: dict[str, tuple[str, bool]] = {
    "trino": (_PARAM_DOLLAR, True),
    **{kind: (_PARAM_DOLLAR, True) for kind in _POSTGRES_WIRE},
    "sqlserver": (_PARAM_QMARK_UPPER, True),
    "fabric": (_PARAM_QMARK_UPPER, True),
    "synapse": (_PARAM_QMARK_UPPER, True),
    "oracle": (
        "SELECT table_name, column_name FROM all_tab_columns "
        "WHERE owner = $1 ORDER BY table_name, column_id",
        True,
    ),
    "saphana": (
        "SELECT TABLE_NAME, COLUMN_NAME FROM SYS.TABLE_COLUMNS "
        "WHERE SCHEMA_NAME = $1 ORDER BY TABLE_NAME, POSITION",
        True,
    ),
    "duckdb": (
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_catalog = current_database() AND table_schema = ? "
        "ORDER BY table_name, ordinal_position",
        True,
    ),
    **{
        kind: (
            "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = $1 ORDER BY TABLE_NAME, ORDINAL_POSITION",
            True,
        )
        for kind in ("mysql", "mariadb", "tidb", "singlestore")
    },
    "clickhouse": (
        "SELECT table, name FROM system.columns WHERE database = '{schema}' "
        "ORDER BY table, position",
        False,
    ),
    "snowflake": (
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = '{schema}' ORDER BY table_name, ordinal_position",
        False,
    ),
    "exasol": (
        "SELECT column_table, column_name FROM EXA_ALL_COLUMNS "
        "WHERE column_schema = '{schema}' ORDER BY column_table, column_ordinal_position",
        False,
    ),
}
#: Kinds whose statement is qualified by the source's stored catalog or project.
_SCHEMA_COLUMNS_QUALIFIED_SQL: dict[str, str] = {
    "databricks": (
        "SELECT table_name, column_name FROM `{database}`.information_schema.columns "
        "WHERE table_schema = '{schema}' ORDER BY table_name, ordinal_position"
    ),
    "bigquery": (
        "SELECT table_name, column_name FROM `{database}`.`{schema}`.INFORMATION_SCHEMA.COLUMNS "
        "ORDER BY table_name, ordinal_position"
    ),
}


def _grouped(rows) -> dict[str, list[str]]:
    columns: dict[str, list[str]] = {}
    for row in rows:
        columns.setdefault(row[0], []).append(row[1])
    return columns


async def native_schema_columns(  # REQ-464
    source_id: str,
    source_type: str,
    schema_name: str,
    pool: "SourcePool",
    config_conn: "Connection | None" = None,
) -> "dict[str, list[str]] | None":
    """``{table: [column, ...]}`` for every table of ``schema_name``, in ONE statement through
    the source's own driver; None when this kind has no such statement here (no driver pool
    for the source, a kind listed some other way, or a catalog with no schema-wide column view —
    hiveserver2 describes one table at a time)."""
    t = source_type.lower()
    if not pool.has(source_id):
        return None
    if t in _SCHEMA_COLUMNS_SQL:
        statement, bound = _SCHEMA_COLUMNS_SQL[t]
        if bound:
            result = await pool.execute(source_id, statement, [schema_name])
        else:
            result = await pool.execute(source_id, statement.format(schema=schema_name))
        return _grouped(result.rows)
    if t in _SCHEMA_COLUMNS_QUALIFIED_SQL:
        if config_conn is None:
            return None
        database = await _source_database(source_id, config_conn)
        if not database:
            return None
        result = await pool.execute(
            source_id,
            _SCHEMA_COLUMNS_QUALIFIED_SQL[t].format(database=database, schema=schema_name),
        )
        return _grouped(result.rows)
    return None


_PK_SQL = (
    "SELECT kcu.column_name FROM information_schema.table_constraints tc "
    "JOIN information_schema.key_column_usage kcu "
    "ON tc.constraint_name = kcu.constraint_name "
    "AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name "
    "WHERE tc.table_schema = {schema} AND tc.table_name = {table} "
    "AND tc.constraint_type = 'PRIMARY KEY' ORDER BY kcu.ordinal_position"
)


async def native_primary_keys(  # REQ-1912
    source_id: str,
    source_type: str,
    schema_name: str,
    table_name: str,
    pool: "SourcePool",
) -> "list[str] | None":
    """The table's primary-key columns, in key order, through the source's own driver; None for a
    type with no driver listing of keys (the caller then asks the engine's catalog, when the
    engine holds a live attach of the source). The sibling of :func:`native_columns` for the
    families whose columns it lists from ``information_schema``."""
    t = source_type.lower()
    if not pool.has(source_id):
        return None
    if t in _POSTGRES_WIRE:
        result = await pool.execute(
            source_id, _PK_SQL.format(schema="$1", table="$2"), [schema_name, table_name]
        )
        return [row[0] for row in result.rows]
    if t == "sqlserver":
        result = await pool.execute(
            source_id, _PK_SQL.format(schema="?", table="?"), [schema_name, table_name]
        )
        return [row[0] for row in result.rows]
    return None


class SourceNotListable(RuntimeError):
    """A source's catalog was asked for, and there is neither a listing through the source's own
    driver nor a live attach of it on the bound engine to list it through (REQ-1912)."""

    def __init__(self, source_id: str, source_type: str, engine_name: str, what: str) -> None:
        self.source_id = source_id
        self.source_type = source_type
        self.engine_name = engine_name
        super().__init__(
            f"cannot list the {what} of source {source_id!r}: its type {source_type!r} has no "
            f"listing through the source's own driver, and engine {engine_name!r} holds no live "
            "attach of it (its reads are served from replicas), so there is no engine catalog "
            "of it to list either"
        )


async def unattached_source(state, source_id: str):
    """The registered source ``source_id`` when the bound engine holds NO live attach of it
    (``replica_routing.has_live_attach``: the operator floors it, or the engine reaches it only
    by replicating it), else None — also None for an id that is not a registered source (a
    built-in catalog, the derived-view source), whose engine catalog is Provisa's own."""
    from provisa.federation.registry_view import registered_sources
    from provisa.federation.replica_routing import has_live_attach

    for source in await registered_sources(state):
        if source.id == source_id:
            if has_live_attach(source, state.federation_engine.engine):
                return None
            return source
    return None


async def require_live_attach(state, source_id: str, what: str) -> None:
    """Refuse an engine-catalog listing of a source the bound engine holds no live attach of
    (REQ-1912): a floored source has no engine catalog — on Trino none is registered — so the
    query is not attempted. Called after the source's own driver had no listing; the error names
    the source and the reason."""
    source = await unattached_source(state, source_id)
    if source is not None:
        raise SourceNotListable(
            source_id, source.type.value, state.federation_engine.engine.name, what
        )


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

    if t == "govdata":
        # The adapter serves every schema it has; the source offers the ones it was given
        # (``federation.askamerica``). A schema outside that list has no tables here; one
        # inside it is listed by the engine (None).
        from provisa.federation.askamerica import serves_schema

        row = (
            await config_conn.execute_core(
                select(sources.c.id, sources.c.database).where(sources.c.id == source_id)
            )
        ).fetchone()
        if row is None or not serves_schema(row, schema_name):
            return []
        # Listed from the adapter's information_schema, which it answers as soon as it
        # listens — not through the engine's attach, which waits on the adapter's pg_catalog.
        from provisa.api.admin.types import AvailableTableType
        from provisa.federation.pgwire_replica import adapter_tables

        source = await _adapter_source(source_id)
        async with _adapter_bound():
            names = await adapter_tables(source, schema_name.strip().lower())
        return [AvailableTableType(name=name, comment=None) for name in names]

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

    if t == "pinot":
        return await _native_tables_pinot(source_id, schema_name, config_conn)

    if t == "druid":
        return await _native_tables_druid(source_id, schema_name, config_conn)

    if t == "hive_s3":
        return await _native_tables_hive_s3(source_id, schema_name, config_conn)

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

    # REQ-1923: the canonical mail tables, the same for every mailbox.
    if t == "google_workspace":
        from provisa.api.admin.types import AvailableTableType
        from provisa.core.canonical_mail import TABLES as CANONICAL
        from provisa.google_workspace.loader import SCHEMA, TABLES

        if schema_name != SCHEMA:
            return []
        return [AvailableTableType(name=name, comment=CANONICAL[name].note) for name in TABLES]
    if t == "microsoft_365":
        from provisa.api.admin.types import AvailableTableType
        from provisa.core.canonical_mail import TABLES as CANONICAL
        from provisa.microsoft365.loader import SCHEMA, TABLES

        if schema_name != SCHEMA:
            return []
        return [AvailableTableType(name=name, comment=CANONICAL[name].note) for name in TABLES]

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


# ── Stored-procedure / routine discovery (REQ-887) ──────────────────────────
#
# Extends database-source introspection to discover source-resident routines
# (stored procedures + functions) from the vendor catalog and classify each as
# read-returning ("query") or write/side-effecting ("mutation"). Discovered routines are
# OFFERED, never registered: a steward registers the ones the catalog should carry as
# commands, one at a time (the admin ``availableFunctions`` picker, then the command form) —
# registration is the curation step, for commands as for tables.

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
