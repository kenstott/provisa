# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c60-7b19-4d54-9e02-1c7a0d6f8b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB-family connectors (REQ-893, REQ-899, REQ-901, REQ-907, REQ-908).

Native DuckDB attaches/scanners, postgres_fdw/file_fdw/sqlite_fdw links, and
pg_duckdb scan connectors. Extracted from connector.py; leaf module.
"""

# complexity-gate: allow-ble=1 reason="connector probe (REQ-904) reports any extension load failure as unavailable, surfacing the error type in the ProbeResult; relocated verbatim from connector.py"

from __future__ import annotations


from typing import TYPE_CHECKING

from provisa.federation.connector_base import Capability, Connector, Mechanism, ProbeResult

if TYPE_CHECKING:
    from provisa.core.models import Source


class DuckDBPostgresConnector(Connector):
    engine = "duckdb"
    source_type = "postgresql"
    materialized_store = True  # REQ-846: PG is the one proven materialized store today
    mechanism = Mechanism.ATTACH_RW

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, write=True)

    def details(self, source: Source) -> dict:
        pw = f" password={source.password}" if source.password else ""
        dsn = (
            f"host={source.host} port={source.port} dbname={source.database} "
            f"user={source.username}{pw}"
        )
        # Attach the raw remote under a private ``_src_`` alias, distinct from the physical catalog
        # (``_to_catalog_name(id)``) the runtime creates for the compiler-visible views — else a
        # hyphen-free id collides (id "ordr" → catalog "ordr" == attach alias "ordr"). Quote it:
        # source ids carry hyphens (pet-store-pg) that DuckDB's ATTACH grammar rejects unquoted. An
        # attached Postgres exposes its own schemas verbatim, so the remote schema IS the registered
        # schema — the runtime defaults there (no remote_schema override).
        alias = f"_src_{source.id}"
        return {"attach": f"ATTACH '{dsn}' AS \"{alias}\" (TYPE postgres)", "raw_alias": alias}


class DuckDBCsvConnector(Connector):
    engine = "duckdb"
    source_type = "csv"
    mechanism = Mechanism.ATTACH_RW  # a scanner view references the file in place

    def capability(self) -> Capability:
        return Capability()

    def details(self, source: Source) -> dict:
        return {
            "view_ddl": f"CREATE VIEW {source.id} AS SELECT * FROM read_csv_auto('{source.path}')"
        }


class DuckDBParquetConnector(Connector):
    engine = "duckdb"
    source_type = "parquet"
    mechanism = Mechanism.ATTACH_RW  # a scanner view references the file in place

    def capability(self) -> Capability:
        return Capability(
            predicate_pushdown=True
        )  # parquet supports predicate + projection pushdown

    def details(self, source: Source) -> dict:
        return {
            "view_ddl": f"CREATE VIEW {source.id} AS SELECT * FROM read_parquet('{source.path}')"
        }


class DuckDBSqliteConnector(Connector):
    engine = "duckdb"
    source_type = "sqlite"
    mechanism = Mechanism.ATTACH_RW  # the sqlite extension attaches the file in place

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, write=True)

    def details(self, source: Source) -> dict:
        # ``_src_`` alias: distinct from the physical catalog the runtime creates (see the postgres
        # connector). DuckDB's sqlite scanner exposes every table under the catalog's ``main`` schema
        # regardless of the registered schema name — the runtime references the remote there.
        alias = f"_src_{source.id}"
        return {
            "attach": f"ATTACH '{source.path}' AS \"{alias}\" (TYPE sqlite)",
            "raw_alias": alias,
            "remote_schema": "main",
        }


class DuckDBDuckdbConnector(Connector):
    engine = "duckdb"
    source_type = "duckdb"
    mechanism = Mechanism.ATTACH_RW  # DuckDB attaches another DuckDB database file in place (core)

    def capability(self) -> Capability:
        return Capability(
            predicate_pushdown=True, join_pushdown=True, aggregate_pushdown=True, write=True
        )

    def details(self, source: Source) -> dict:
        # A DuckDB→DuckDB ATTACH exposes the remote database's own schemas; the runtime references the
        # remote under its default ``main`` schema, aliased away from the physical catalog.
        alias = f"_src_{source.id}"
        return {
            "attach": f"ATTACH '{source.path}' AS \"{alias}\" (TYPE duckdb)",
            "raw_alias": alias,
            "remote_schema": "main",
        }


# --- DuckDB extensions: external DB / warehouse / lake / SaaS reach in place (REQ-899) ---
#
# Each connector declares the extension that gives DuckDB the source (Connector.extension), staged into
# DuckDB's extension directory at startup — from the community registry (install_from_community=True) or
# the core registry (False). The probe is LOAD-ONLY (REQ-904): it installs + loads the extension and
# asserts the scanner/attach symbol is registered — it NEVER opens a live connection. details() emits the
# in-place DDL a live EngineRuntime issues; credentials arrive via a DuckDB SECRET / federation_hints at
# attach time, out of scope for the probe.


class _DuckDBExtensionConnector(Connector):  # REQ-899
    engine = "duckdb"
    mechanism = (
        Mechanism.ATTACH_RW
    )  # referenced in place (ATTACH catalog or scanner view), never landed
    install_from_community = True

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def _install_sql(self) -> str:
        return (
            f"INSTALL {self.extension} FROM community"
            if self.install_from_community
            else f"INSTALL {self.extension}"
        )

    async def probe(self, fetch) -> ProbeResult:  # REQ-904 / REQ-899 — load-only, no live source
        try:
            await fetch(self._install_sql())
            await fetch(f"LOAD {self.extension}")
        except Exception as e:
            return ProbeResult(
                False,
                f"{self.extension} did not load: {type(e).__name__}",
                f"stage it: {self._install_sql()}",
            )
        rows = await fetch(
            "SELECT count(*) AS n FROM duckdb_functions() "
            f"WHERE function_name = '{self.probe_symbol}'"
        )
        if rows and rows[0]["n"]:
            return ProbeResult(True, f"{self.extension} loaded; {self.probe_symbol} registered")
        return ProbeResult(
            False,
            f"{self.extension} loaded but {self.probe_symbol} is not registered",
            f"verify this {self.extension} build exposes {self.probe_symbol}",
        )


class DuckDBMssqlConnector(_DuckDBExtensionConnector):  # REQ-899
    """Microsoft SQL Server, attached in place via the mssql extension (native TDS)."""

    source_type = "sqlserver"
    key = "duckdb_mssql"
    extension = "mssql"
    probe_symbol = "mssql_scan"

    def details(self, source: Source) -> dict:
        dsn = (
            f"Server={source.host},{source.port};Database={source.database};"
            f"User Id={source.username};Password={source.password}"
        )
        return {"attach": f"ATTACH '{dsn}' AS \"{source.id}\" (TYPE mssql)"}


class DuckDBMongoConnector(_DuckDBExtensionConnector):  # REQ-899
    """MongoDB, attached in place via the mongo extension; collections read as tables."""

    source_type = "mongodb"
    key = "duckdb_mongo"
    extension = "mongo"
    probe_symbol = "mongo_scan"

    def details(self, source: Source) -> dict:
        dsn = f"host={source.host} port={source.port}"
        return {"attach": f"ATTACH '{dsn}' AS \"{source.id}\" (TYPE mongo)"}


class DuckDBSnowflakeConnector(_DuckDBExtensionConnector):  # REQ-899
    """Snowflake, attached read-only via the snowflake extension. Credentials arrive as a DuckDB
    SECRET (TYPE snowflake) named by convention; secret creation is a provisioning step, not the attach."""

    source_type = "snowflake"
    key = "duckdb_snowflake"
    extension = "snowflake"
    probe_symbol = "snowflake_query"

    def details(self, source: Source) -> dict:
        return {
            "secret": f"sf_{source.id}",
            "attach": f"ATTACH '' AS \"{source.id}\" (TYPE snowflake, SECRET sf_{source.id}, READ_ONLY)",
        }


class DuckDBBigQueryConnector(_DuckDBExtensionConnector):  # REQ-899
    """BigQuery, attached read-only via the bigquery extension. The GCP project id comes from the
    source's federation_hints; auth is ADC / GOOGLE_APPLICATION_CREDENTIALS in the engine environment."""

    source_type = "bigquery"
    key = "duckdb_bigquery"
    extension = "bigquery"
    probe_symbol = "bigquery_scan"

    def details(self, source: Source) -> dict:
        project = source.federation_hints["project"]
        return {
            "attach": f"ATTACH 'project={project}' AS \"{source.id}\" (TYPE bigquery, READ_ONLY)"
        }


class DuckDBFirebirdConnector(_DuckDBExtensionConnector):  # REQ-899
    """Firebird (3/4/5), attached in place via the firebird extension (projection + filter pushdown)."""

    source_type = "firebird"
    key = "duckdb_firebird"
    extension = "firebird"
    probe_symbol = "firebird_scan"

    def details(self, source: Source) -> dict:
        dsn = (
            f"firebird://{source.username}:{source.password}"
            f"@{source.host}:{source.port}/{source.path}"
        )
        return {"attach": f"ATTACH '{dsn}' AS \"{source.id}\" (TYPE firebird)"}


class DuckDBGsheetsConnector(_DuckDBExtensionConnector):  # REQ-899
    """Google Sheets, referenced in place via a read_gsheet scanner view (gsheets extension). The
    spreadsheet id comes from federation_hints; auth is a DuckDB SECRET (TYPE gsheet)."""

    source_type = "google_sheets"
    key = "duckdb_gsheets"
    extension = "gsheets"
    probe_symbol = "read_gsheet"

    def capability(self) -> Capability:
        return Capability()  # a Sheets scan has no predicate pushdown

    def details(self, source: Source) -> dict:
        sheet = source.federation_hints["spreadsheet_id"]
        return {"view_ddl": f"CREATE VIEW {source.id} AS SELECT * FROM read_gsheet('{sheet}')"}


class DuckDBAirportConnector(_DuckDBExtensionConnector):  # REQ-899
    """Arrow Flight server, attached in place via the airport extension. The Flight location is the
    source's base_url; auth is a DuckDB SECRET (TYPE airport)."""

    source_type = "airport"
    key = "duckdb_airport"
    extension = "airport"
    probe_symbol = "airport_take_flight"

    def details(self, source: Source) -> dict:
        return {"attach": f"ATTACH '{source.base_url}' AS \"{source.id}\" (TYPE AIRPORT)"}


class DuckDBIcebergConnector(_DuckDBExtensionConnector):  # REQ-899
    """Apache Iceberg table, referenced in place via an iceberg_scan scanner view. Unlike the other
    six, iceberg is a CORE extension (install_from_community=False). The table location is the source's
    path (a warehouse/object-store URI); object-store access needs httpfs + a DuckDB SECRET, provisioned
    at attach time and out of scope for the load-only probe."""

    source_type = "iceberg"
    key = "duckdb_iceberg"
    extension = "iceberg"
    install_from_community = False  # core registry — INSTALL iceberg (no FROM community)
    probe_symbol = "iceberg_scan"

    def details(self, source: Source) -> dict:
        return {
            "view_ddl": f"CREATE VIEW {source.id} AS SELECT * FROM iceberg_scan('{source.path}')"
        }


# --- Postgres: a single-node federator that ATTACHes remote sources via postgres_fdw (SQL/MED) ---


async def _probe_pg_extension(fetch, ext: str, *, auto_create: bool) -> ProbeResult:  # REQ-904
    """Probe a Postgres extension/FDW: created -> available; installable -> available iff the engine
    auto-creates it on attach; absent -> unavailable with an install remediation."""
    if await fetch(f"SELECT 1 FROM pg_extension WHERE extname = '{ext}'"):
        return ProbeResult(True, f"{ext} installed")
    if await fetch(f"SELECT 1 FROM pg_available_extensions WHERE name = '{ext}'"):
        return ProbeResult(
            auto_create,
            f"{ext} available" + ("" if auto_create else " but not created"),
            None if auto_create else f"CREATE EXTENSION {ext}",
        )
    return ProbeResult(False, f"{ext} not installed in this Postgres", f"install {ext} extension")


class PostgresFdwConnector(Connector):  # REQ-893
    """Attach a remote PostgreSQL source into a Postgres engine via postgres_fdw (SQL/MED).

    A remote source is referenced in place through a foreign server + imported foreign schema — the
    SQL-standard analog of a Trino catalog / DuckDB ATTACH. ``details`` carries the ordered DDL the
    engine issues once to attach the source; per-query the engine just reads the foreign tables.
    """

    engine = "postgres"
    source_type = "postgresql"
    materialized_store = True  # REQ-846: PG is the one proven materialized store today
    mechanism = Mechanism.ATTACH_RW
    key = "postgres_fdw"

    async def probe(
        self, fetch
    ) -> ProbeResult:  # REQ-904 — engine creates it on attach, so installable is enough
        return await _probe_pg_extension(fetch, "postgres_fdw", auto_create=True)

    def capability(self) -> Capability:
        # postgres_fdw pushes down predicates, joins between same-server foreign tables, and (PG14+)
        # aggregates; a cross-SERVER join still materializes locally (single-node — REQ-894). It is
        # writable (INSERT/UPDATE/DELETE on foreign tables since PG9.3).
        return Capability(
            predicate_pushdown=True, join_pushdown=True, aggregate_pushdown=True, write=True
        )

    def details(self, source: Source) -> dict:
        server = f"fdw_{source.id}"
        local_schema = f"fdw_{source.id}"
        # Remote schema override rides on federation_hints (Source has no `schema` field — and
        # ``source.schema`` would resolve to pydantic's BaseModel.schema method, never the default).
        remote_schema = source.federation_hints.get("schema") or "public"
        return {
            "attach_ddl": [
                "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
                f"CREATE SERVER IF NOT EXISTS {server} FOREIGN DATA WRAPPER postgres_fdw "
                f"OPTIONS (host '{source.host}', port '{source.port}', dbname '{source.database}')",
                f"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER {server} "
                f"OPTIONS (user '{source.username}', password '{source.password}')",
                f"CREATE SCHEMA IF NOT EXISTS {local_schema}",
                f"IMPORT FOREIGN SCHEMA {remote_schema} FROM SERVER {server} INTO {local_schema}",
            ],
            "local_schema": local_schema,
        }


class FileFdwConnector(Connector):  # REQ-893
    """Attach a CSV file into a Postgres engine via file_fdw (a stock/core PG contrib FDW).

    file_fdw needs an explicit column list, so the per-table CREATE FOREIGN TABLE is completed by the
    engine runtime from the registry's column metadata; ``details`` carries the column-independent
    server setup plus the file OPTIONS the foreign table binds.
    """

    engine = "postgres"
    source_type = "csv"
    mechanism = Mechanism.ATTACH_RW
    key = "file_fdw"

    async def probe(self, fetch) -> ProbeResult:  # REQ-904
        return await _probe_pg_extension(fetch, "file_fdw", auto_create=True)

    def capability(self) -> Capability:
        return Capability()  # file_fdw is a plain sequential scan — no pushdown

    def details(self, source: Source) -> dict:
        return {
            "server_ddl": [
                "CREATE EXTENSION IF NOT EXISTS file_fdw",
                "CREATE SERVER IF NOT EXISTS fdw_file_srv FOREIGN DATA WRAPPER file_fdw",
            ],
            "server": "fdw_file_srv",
            "table_options": f"OPTIONS (filename '{source.path}', format 'csv', header 'true')",
        }


class SqliteFdwConnector(Connector):  # REQ-907
    """Attach a SQLite file into a Postgres engine via sqlite_fdw (external contrib; links system libsqlite3).

    A SQLite database file is referenced in place through a foreign server (OPTIONS database '<path>')
    plus an imported foreign schema. Same ATTACH shape as postgres_fdw; per-query the engine reads the
    foreign tables. sqlite_fdw's only runtime dependency is the OS libsqlite3.
    """

    engine = "postgres"
    source_type = "sqlite"
    mechanism = Mechanism.ATTACH_RW
    key = "sqlite_fdw"
    runtime_deps = ("libsqlite3 (system — OS-provided on macOS/Linux)",)

    async def probe(self, fetch) -> ProbeResult:  # REQ-904
        return await _probe_pg_extension(fetch, "sqlite_fdw", auto_create=True)

    def capability(self) -> Capability:
        # sqlite_fdw pushes some predicates to SQLite and supports write on foreign tables.
        return Capability(predicate_pushdown=True, write=True)

    def details(self, source: Source) -> dict:
        server = f"fdw_{source.id}"
        local_schema = f"fdw_{source.id}"
        return {
            "attach_ddl": [
                "CREATE EXTENSION IF NOT EXISTS sqlite_fdw",
                f"CREATE SERVER IF NOT EXISTS {server} FOREIGN DATA WRAPPER sqlite_fdw "
                f"OPTIONS (database '{source.path}')",
                f"CREATE SCHEMA IF NOT EXISTS {local_schema}",
                f"IMPORT FOREIGN SCHEMA public FROM SERVER {server} INTO {local_schema}",
            ],
            "local_schema": local_schema,
        }


class MysqlFdwConnector(Connector):  # REQ-907
    """Attach a remote MySQL/MariaDB source into a Postgres engine via mysql_fdw (external contrib).

    Same ATTACH shape as postgres_fdw. mysql_fdw links a MySQL client library (libmysqlclient /
    mariadb-connector-c), which the distribution must bundle — so this connector is probe-gated and
    only becomes active where the extension is actually installed.
    """

    engine = "postgres"
    source_type = "mysql"
    mechanism = Mechanism.ATTACH_RW
    key = "mysql_fdw"
    runtime_deps = ("libmysqlclient / mariadb-connector-c (bundled — must ship + relocate)",)

    async def probe(self, fetch) -> ProbeResult:  # REQ-904
        return await _probe_pg_extension(fetch, "mysql_fdw", auto_create=True)

    def capability(self) -> Capability:
        # mysql_fdw pushes predicates and joins, and supports write on foreign tables.
        return Capability(predicate_pushdown=True, join_pushdown=True, write=True)

    def details(self, source: Source) -> dict:
        server = f"fdw_{source.id}"
        local_schema = f"fdw_{source.id}"
        # Remote schema override rides on federation_hints (Source has no `schema` field — and
        # ``source.schema`` would resolve to pydantic's BaseModel.schema method, never the default).
        remote_schema = source.federation_hints.get("schema") or source.database
        return {
            "attach_ddl": [
                "CREATE EXTENSION IF NOT EXISTS mysql_fdw",
                f"CREATE SERVER IF NOT EXISTS {server} FOREIGN DATA WRAPPER mysql_fdw "
                f"OPTIONS (host '{source.host}', port '{source.port}')",
                f"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER {server} "
                f"OPTIONS (username '{source.username}', password '{source.password}')",
                f"CREATE SCHEMA IF NOT EXISTS {local_schema}",
                f"IMPORT FOREIGN SCHEMA {remote_schema} FROM SERVER {server} INTO {local_schema}",
            ],
            "local_schema": local_schema,
        }


# --- Postgres + pg_duckdb: attach files IN PLACE via an embedded DuckDB (no landing) ---


class _PgDuckdbScanConnector(Connector):  # REQ-901
    """Attach a file source into a Postgres engine via pg_duckdb's DuckDB reader (read_csv/read_parquet).

    pg_duckdb embeds DuckDB inside Postgres, so a file is referenced in place through a DuckDB table
    function wrapped in a named-column view — the same ATTACH shape as DuckDB's scanner views, but the
    query runs entirely inside the embedded PG. pg_duckdb must be in shared_preload_libraries (declared
    here); the per-table column list is completed by the engine runtime from registry metadata, since
    the DuckDB functions expose columns via the ``r['name']`` element syntax.
    """

    engine = "postgres"
    mechanism = Mechanism.ATTACH_RW
    _reader = ""  # read_csv | read_parquet | read_json | iceberg_scan
    _scan_args = ""  # extra reader args appended after the path, e.g. ", allow_moved_paths := true"
    runtime_deps = (
        "libduckdb (bundled — the embedded DuckDB engine)",
        "libssl/libcrypto via httpfs (bundled — relocated to @loader_path)",
    )  # also requires pg_duckdb in shared_preload_libraries (see probe)

    async def probe(self, fetch) -> ProbeResult:  # REQ-904
        # The lesson case: presence is not enough — pg_duckdb only works if it is preloaded.
        pre = await fetch("SELECT current_setting('shared_preload_libraries') AS v")
        if not (pre and "pg_duckdb" in (pre[0]["v"] or "")):
            return ProbeResult(
                False,
                "pg_duckdb not in shared_preload_libraries",
                "add pg_duckdb to shared_preload_libraries and restart Postgres",
            )
        if await fetch("SELECT 1 FROM pg_extension WHERE extname = 'pg_duckdb'"):
            return ProbeResult(True, "pg_duckdb preloaded and installed")
        if await fetch("SELECT 1 FROM pg_available_extensions WHERE name = 'pg_duckdb'"):
            return ProbeResult(True, "pg_duckdb preloaded", "CREATE EXTENSION pg_duckdb")
        return ProbeResult(False, "pg_duckdb not installed", "install pg_duckdb")

    def capability(self) -> Capability:
        # DuckDB pushes down predicates and projection into the file scan (parquet also skips row groups).
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        return {
            "requires_preload": "pg_duckdb",
            "reader": self._reader,
            "scan": f"{self._reader}('{source.path}'{self._scan_args})",
        }


class PgDuckdbCsvConnector(_PgDuckdbScanConnector):  # REQ-901
    source_type = "csv"
    _reader = "read_csv"
    key = "pg_duckdb_csv"


class PgDuckdbParquetConnector(_PgDuckdbScanConnector):  # REQ-901
    source_type = "parquet"
    _reader = "read_parquet"
    key = "pg_duckdb_parquet"


class PgDuckdbJsonConnector(_PgDuckdbScanConnector):  # REQ-901
    source_type = "json"
    _reader = "read_json"
    key = "pg_duckdb_json"


class PgDuckdbIcebergConnector(_PgDuckdbScanConnector):  # REQ-908
    """Attach an Apache Iceberg table IN PLACE via pg_duckdb's iceberg_scan (DuckDB iceberg extension).

    Reads the table's current snapshot from its metadata; ``allow_moved_paths`` lets DuckDB resolve
    manifest/data paths relative to the table location. The iceberg extension links aws-sdk-cpp / avro-c
    / roaring, all STATIC-linked into libduckdb by the vcpkg build (no extra runtime dylib). Requires a
    pg_duckdb built WITH the iceberg extension — the probe verifies iceberg_scan is registered, not just
    that pg_duckdb is loaded (a pg_duckdb without iceberg passes the base probe but lacks the function).
    """

    source_type = "iceberg"
    _reader = "iceberg_scan"
    _scan_args = ", allow_moved_paths := true"
    key = "pg_duckdb_iceberg"
    runtime_deps = (
        "libduckdb (bundled — the embedded DuckDB engine)",
        "aws-sdk-cpp / avro-c / roaring (bundled — static-linked into libduckdb via vcpkg)",
    )

    async def probe(self, fetch) -> ProbeResult:  # REQ-904/908
        base = await super().probe(fetch)
        if not base.available:
            return base
        if await fetch("SELECT 1 FROM pg_proc WHERE proname = 'iceberg_scan'"):
            return ProbeResult(True, "pg_duckdb with iceberg extension")
        return ProbeResult(
            False,
            "pg_duckdb is loaded but was built without the iceberg extension",
            "rebuild pg_duckdb with the iceberg DuckDB extension (vcpkg)",
        )


# --- ClickHouse: an OLAP federator that ATTACHes external sources via integration engines ---
#
# ClickHouse reaches external data through native integration engines, not an FDW. Relational
# sources (PostgreSQL/MySQL) mount as a DATABASE engine that auto-exposes every remote table — the
# CREATE DATABASE analog of postgres_fdw's IMPORT FOREIGN SCHEMA (details carry ``attach_ddl`` +
# ``local_schema``). File sources (csv/parquet) and MongoDB mount as a per-table TABLE engine
# (S3/URL/File by path scheme, or MongoDB); details carry the ``engine_clause`` the runtime binds
# into a ``CREATE TABLE`` — columns inferred where the engine supports it (S3/URL/File), supplied
# from registry metadata where it does not (MongoDB). mechanism is ATTACH throughout: the data is
# referenced in place, never landed.
