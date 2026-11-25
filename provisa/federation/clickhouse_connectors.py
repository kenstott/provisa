# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c60-7b19-4d54-9e02-1c7a0d6f8b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holders.

"""ClickHouse connector definitions (REQ-909).

ClickHouse attaches external sources via native integration engines:
- Relational sources (PG/MySQL) mount as a DATABASE engine (auto-exposes every remote table).
- File sources (csv/parquet) and MongoDB mount as a per-table TABLE engine.
- Lakehouse sources (Iceberg/DeltaLake) mount via native lakehouse table engines (zero-copy).

All ATTACH_RW — referenced in place, nothing lands. ClickHouse is its own native store.

A dev-mode code source's pgwire sidecar mounts as a PostgreSQL DATABASE engine too (REQ-2267).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.models import Source

from provisa.compiler.naming import attach_catalog, engine_attach_name
from provisa.federation.clickhouse_quoting import ch_ident, ch_lit
from provisa.federation.codehost_connectors import DATABASE, USER, CodeHost, is_dev
from provisa.federation.connector_base import Capability, Connector, Mechanism, ProbeResult

#: The schema a sidecar serves its functions' tables in to a connection that names none
#: (provisa/codehost/pgfront.py, ``SCHEMA``).
SIDECAR_SCHEMA = "public"


class ClickHousePostgresConnector(Connector):
    """Mount a remote PostgreSQL source into ClickHouse via the PostgreSQL database engine.

    ``CREATE DATABASE ... ENGINE = PostgreSQL(...)`` exposes every remote table under a local
    database — the CREATE DATABASE analog of postgres_fdw's IMPORT FOREIGN SCHEMA. ClickHouse pushes
    WHERE predicates to PostgreSQL and can INSERT back through the engine.
    """

    engine = "clickhouse"
    source_type = "postgresql"
    materialized_store = True  # REQ-846: PG is the one proven materialized store today
    mechanism = Mechanism.ATTACH_RW
    key = "clickhouse_postgres"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, write=True)

    def details(self, source: Source) -> dict:
        local_schema = engine_attach_name("ch", attach_catalog(source))
        # Remote schema override rides on federation_hints (Source has no `schema` field — and
        # ``source.schema`` would resolve to pydantic's BaseModel.schema method, never the default).
        remote_schema = source.federation_hints.get("schema") or "public"
        return {
            "attach_ddl": [
                f"CREATE DATABASE IF NOT EXISTS {ch_ident(local_schema)} ENGINE = PostgreSQL("
                f"{ch_lit(f'{source.host}:{source.port}')}, {ch_lit(source.database)}, "
                f"{ch_lit(source.username)}, {ch_lit(source.password)}, {ch_lit(remote_schema)})"
            ],
            "local_schema": local_schema,
        }


class _ClickHousePgwireConnector(Connector):  # REQ-1730
    """Mount a connector-pgwire-replica source (files/sharepoint/splunk, ``strategy.py``'s
    ``_CONNECTOR_PGWIRE_REPLICA``) into ClickHouse LIVE via the built-in PostgreSQL database engine,
    pointed at the source's own bundled Calcite pgwire server instead of a real remote Postgres.

    Same trick as ``PgFilesConnector``/``_DuckDBPgwireConnector`` (REQ-1690/1730): the pgwire bridge
    (``pgwire_replica.py``) is nothing but a real Postgres wire endpoint, and ClickHouse's PostgreSQL
    engine needs no extension/probe to reach one — it is built in. Once registered,
    ``FederationEngine.complete_reach()`` skips its own land-reach fallback for this type, so the
    source is live-attached instead of falling to ``pgwire_replica.ConnectorReplica``'s landed-copy
    path. Read-only: Calcite serves no DML, so ``write=True`` (the plain postgresql connector's own
    default) is deliberately NOT claimed here."""

    served_by_bundle = True

    engine = "clickhouse"
    mechanism = Mechanism.ATTACH_R

    async def probe(self, fetch) -> ProbeResult:  # REQ-904
        from provisa.runtime_deps import BundleResolver, bundle_spec_for
        from provisa.runtime_deps.pgwire_bundles import BundleUnavailable

        del fetch  # ClickHouse's PostgreSQL engine is built in; only the bundle needs checking
        try:
            spec = bundle_spec_for(self.source_type)
            asset = spec.asset_filename  # resolves this host's variant; unbuilt platforms raise
        except BundleUnavailable as e:
            return ProbeResult(
                False,
                str(e),
                "run Provisa on macOS arm64, Linux x86_64 or Windows x86_64, or publish a bundle "
                "for this platform in the kenstott/calcite release",
            )
        if BundleResolver().is_cached(spec):
            return ProbeResult(True, f"{spec.artifact_name} bundle cached ({spec.version})")
        return ProbeResult(True, f"{asset} is fetched on first use from {spec.download_url}")

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        from provisa.federation.pgwire_replica import (
            ensure_endpoint,
            remote_schema,
            serves_table_schemas,
        )
        from provisa.federation.pgwire_endpoint import sign_in_password

        ports = ensure_endpoint(source)  # starts (once) the source's bundled Calcite pgwire server
        local_schema = engine_attach_name("ch_pgwire", attach_catalog(source))
        schema = remote_schema(source, getattr(source, "schema_name", None))
        if serves_table_schemas(source):
            # A server of several schemas (AskAmerica): one ClickHouse database per server
            # schema, since the PostgreSQL database engine mounts exactly one.
            local_schema = f"{local_schema}_{schema}"
        return {
            "attach_ddl": [
                f"CREATE DATABASE IF NOT EXISTS {ch_ident(local_schema)} ENGINE = PostgreSQL("
                f"{ch_lit(f'{ports.pgwire_host}:{ports.pgwire_port}')}, 'provisa', 'provisa', "
                f"{ch_lit(sign_in_password(source))}, {ch_lit(schema)})"
            ],
            "local_schema": local_schema,
        }


class ClickHouseFilesConnector(_ClickHousePgwireConnector):  # REQ-1730
    source_type = "files"


class ClickHouseSharepointConnector(_ClickHousePgwireConnector):  # REQ-1730
    source_type = "sharepoint"


class ClickHouseCloudopsConnector(_ClickHousePgwireConnector):  # REQ-1947
    source_type = "cloudops"


class ClickHouseSalesforceConnector(_ClickHousePgwireConnector):  # REQ-1946
    source_type = "salesforce"


class ClickHouseServicenowConnector(_ClickHousePgwireConnector):  # REQ-1954
    source_type = "servicenow"


class ClickHouseJiraConnector(_ClickHousePgwireConnector):  # REQ-2069
    source_type = "jira"


class ClickHouseGovdataConnector(_ClickHousePgwireConnector):  # REQ-540
    source_type = "govdata"


class ClickHouseSplunkConnector(_ClickHousePgwireConnector):  # REQ-1730
    source_type = "splunk"


class ClickHouseCodeHostConnector(CodeHost, Connector):  # REQ-2267
    """A dev-mode code source's pgwire sidecar, mounted into ClickHouse LIVE through the built-in
    PostgreSQL database engine (codehost_connectors).

    The engine sends no ``options`` at its connection's start, so it cannot name the schemas the
    source's tables are registered under (``provisa.schemas``): it mounts ``public``, where a
    sidecar serves every function of its one source when a connection names none
    (pgfront.schemas_of). One database per source, whatever schemas its tables are registered
    under: the live view of each table selects from it by the table's name, its function's.

    A built source's host accepts only the organisation's TLS client identity, which this engine
    cannot present (``attaches_built``): a built source keeps its type's reach and lands. Its
    database is dropped when the sidecar stops for good (codesource.attach._drop_on_engine,
    ClickHouseFederationRuntime.drop_source), so the next attach dials the sidecar as it is then.
    """

    engine = "clickhouse"
    mechanism = Mechanism.ATTACH_R
    materialized_store = False
    files_for = "process"
    attaches_built = False

    def __init__(self, source_type: str) -> None:
        self.source_type = source_type

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    async def probe(self, fetch) -> ProbeResult:
        del fetch  # ClickHouse's PostgreSQL database engine is built in
        return ProbeResult(True, "ClickHouse's PostgreSQL database engine, built in")

    def details(self, source: Source) -> dict:
        if not is_dev(source):
            raise ValueError(
                f"code source {source.id!r} is built: ClickHouse cannot present the TLS identity "
                "its host accepts, so it is landed, not attached"
            )
        at = self.target(source)
        local_schema = engine_attach_name("ch", attach_catalog(source))
        # The sidecar checks no credentials (it is reached only where the deployment's engines
        # are): the engine's password argument is required, and empty.
        # REQ-2267: ``is_code_source`` tells attach_source to create the DATABASE at the
        # live-view-schema path directly, skipping the intermediate VIEW. ClickHouse does not
        # push WHERE conditions through a VIEW to its underlying PostgreSQL DATABASE engine in
        # a JOIN; without the VIEW the engine queries the sidecar directly and pushes the
        # _nf_ parameter conditions, so the function is called with the right inputs.
        return {
            "attach_ddl": [
                f"CREATE DATABASE IF NOT EXISTS {ch_ident(local_schema)} ENGINE = PostgreSQL("
                f"{ch_lit(f'{at.host}:{at.port}')}, {ch_lit(DATABASE)}, {ch_lit(USER)}, '', "
                f"{ch_lit(SIDECAR_SCHEMA)})"
            ],
            "local_schema": local_schema,
            "is_code_source": True,
        }


class ClickHouseMysqlConnector(Connector):
    """Mount a remote MySQL/MariaDB source into ClickHouse via the MySQL database engine.

    Same CREATE DATABASE shape as the PostgreSQL engine — every remote table is exposed under a
    local database. ClickHouse pushes predicates to MySQL and can INSERT back through the engine.
    """

    engine = "clickhouse"
    source_type = "mysql"
    mechanism = Mechanism.ATTACH_RW
    key = "clickhouse_mysql"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, write=True)

    def details(self, source: Source) -> dict:
        local_schema = engine_attach_name("ch", attach_catalog(source))
        return {
            "attach_ddl": [
                f"CREATE DATABASE IF NOT EXISTS {ch_ident(local_schema)} ENGINE = MySQL("
                f"{ch_lit(f'{source.host}:{source.port}')}, {ch_lit(source.database)}, "
                f"{ch_lit(source.username)}, {ch_lit(source.password)})"
            ],
            "local_schema": local_schema,
        }


class ClickHouseSqliteConnector(Connector):  # REQ-1178
    """Mount a SQLite database file into ClickHouse via the SQLite database engine.

    ``CREATE DATABASE ... ENGINE = SQLite('<path>')`` exposes every table of a local SQLite file under
    a local database — the CREATE DATABASE auto-expose shape, like PostgreSQL/MySQL but over a file, so
    no server. ClickHouse parity with the SQLite reach the pg (sqlite_fdw) and duckdb (sqlite) engines
    already offer."""

    engine = "clickhouse"
    source_type = "sqlite"
    mechanism = Mechanism.ATTACH_RW
    key = "clickhouse_sqlite"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        if source.path is None:
            raise ValueError(f"sqlite source {source.id!r} has no path")
        local_schema = engine_attach_name("ch", attach_catalog(source))
        return {
            "attach_ddl": [
                f"CREATE DATABASE IF NOT EXISTS {ch_ident(local_schema)} "
                f"ENGINE = SQLite({ch_lit(source.path)})"
            ],
            "local_schema": local_schema,
        }


class ClickHouseMongoConnector(Connector):
    """Mount a MongoDB collection into ClickHouse via the MongoDB table engine.

    MongoDB is a per-table engine (one collection per table) and cannot infer its schema, so the
    per-table ``CREATE TABLE`` column list is completed by the runtime from registry metadata; the
    ``engine_clause`` carries a ``{table}`` placeholder the runtime binds to the collection name.
    ClickHouse pushes simple predicates down to MongoDB.
    """

    engine = "clickhouse"
    source_type = "mongodb"
    mechanism = Mechanism.ATTACH_RW
    key = "clickhouse_mongo"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets
        from provisa.mongodb.fetch import is_connection_string

        if is_connection_string(resolve_secrets(source.host or "")):
            # REQ-1985: ClickHouse's MongoDB table engine takes one host and port.
            raise ValueError(
                f"source {source.id!r}: the ClickHouse engine reaches MongoDB by one host and "
                "port, and this source is given by a connection string"
            )
        return {
            "engine_clause": (
                f"MongoDB({ch_lit(f'{source.host}:{source.port}')}, {ch_lit(source.database)}, "
                f"'{{table}}', {ch_lit(source.username)}, {ch_lit(source.password)})"
            ),
            "requires_columns": True,
        }


def _clickhouse_s3_creds(source: Source) -> tuple[str, str] | None:
    """S3/S3-compatible (R2) access key + secret from federation_hints — accepts the ClickHouse
    ``aws_key``/``aws_secret`` or the generic ``access_key_id``/``secret_access_key`` spelling."""
    h = source.federation_hints
    key = h.get("aws_key") or h.get("access_key_id")
    secret = h.get("aws_secret") or h.get("secret_access_key")
    if not key:
        return None  # a public bucket: no credentials
    if not secret:
        raise ValueError(
            f"source {source.id!r} names an access key with no secret: give aws_secret "
            "(or secret_access_key) beside it"
        )
    return key, secret


def _clickhouse_file_engine(source: Source, fmt: str) -> str:
    """The ClickHouse table-engine clause for a file source, chosen by the path scheme: ``s3://`` (or
    an https S3-compatible endpoint carrying credentials — e.g. Cloudflare R2) → S3, a plain
    ``http(s)://`` → URL, otherwise a local File. Credentials, when the bucket is private, ride on
    federation_hints; absent means a public bucket."""
    path = source.path
    if path is None:
        raise ValueError(f"file source {source.id!r} has no path")
    s3_creds = _clickhouse_s3_creds(source)
    if path.startswith("s3://") or (path.startswith("https://") and s3_creds):
        creds = "".join(f", {ch_lit(part)}" for part in s3_creds or ())
        return f"S3({ch_lit(path)}{creds}, {ch_lit(fmt)})"
    if path.startswith(("http://", "https://")):
        return f"URL({ch_lit(path)}, {ch_lit(fmt)})"
    # The File table engine is format-FIRST (unlike S3/URL, which are url-first).
    return f"File({ch_lit(fmt)}, {ch_lit(path)})"


def _clickhouse_lake_engine(source: Source, engine: str) -> str:
    """A ClickHouse lakehouse table-engine clause (``IcebergS3`` / ``DeltaLake``) over an object-store
    URL + optional S3-compatible credentials. Reads the table's metadata + data in place — zero copy."""
    path = source.path
    if path is None:
        raise ValueError(f"lake source {source.id!r} has no path (object-store URL)")
    creds = "".join(f", {ch_lit(part)}" for part in _clickhouse_s3_creds(source) or ())
    return f"{engine}({ch_lit(path)}{creds})"


class _ClickHouseFileConnector(Connector):
    """Mount a file source into ClickHouse via an S3/URL/File table engine (chosen by path scheme).

    ClickHouse infers the column schema for these engines, so the runtime issues a bare
    ``CREATE TABLE ... ENGINE = <clause>`` with no column list. The data is read in place. ``validate``
    tells the runtime to probe the attached table (read one row) so bad credentials / an unreachable
    object fail loud at attach time, not at first query."""

    engine = "clickhouse"
    mechanism = (
        Mechanism.SCAN
    )  # S3/URL/File table engine reads the file in place — no copy (REQ-951)
    _format = ""  # ClickHouse input-format name

    def details(self, source: Source) -> dict:
        return {
            "engine_clause": _clickhouse_file_engine(source, self._format),
            "infer": True,
            "validate": True,
        }


class ClickHouseCsvConnector(_ClickHouseFileConnector):
    source_type = "csv"
    key = "clickhouse_csv"
    _format = "CSVWithNames"

    def capability(self) -> Capability:
        return Capability()  # CSV scan: no predicate pushdown


class ClickHouseParquetConnector(_ClickHouseFileConnector):
    source_type = "parquet"
    key = "clickhouse_parquet"
    _format = "Parquet"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)  # column + row-group pruning


class _ClickHouseLakeConnector(Connector):
    """Mount a lakehouse table (Iceberg / Delta Lake) on object storage into ClickHouse via its native
    lakehouse table engine (``IcebergS3`` / ``DeltaLake``). Read in place (zero copy); ``validate``
    probes the attach so a bad credential / unreachable table fails loud at attach time."""

    engine = "clickhouse"
    mechanism = (
        Mechanism.SCAN
    )  # IcebergS3/DeltaLake engine reads the table in place — no copy (REQ-951)
    _engine_name = ""  # ClickHouse lakehouse table engine

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)  # lakehouse manifest/stat pruning

    def details(self, source: Source) -> dict:
        return {
            "engine_clause": _clickhouse_lake_engine(source, self._engine_name),
            "infer": True,
            "validate": True,
        }


class ClickHouseIcebergConnector(_ClickHouseLakeConnector):
    source_type = "iceberg"
    key = "clickhouse_iceberg"
    _engine_name = "IcebergS3"


class ClickHouseDeltaLakeConnector(_ClickHouseLakeConnector):
    source_type = "delta_lake"
    key = "clickhouse_delta"
    _engine_name = "DeltaLake"
