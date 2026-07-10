# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c60-7b19-4d54-9e02-1c7a0d6f8b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Connector abstraction for the federation engine (REQ-842).

A Connector is indexed by ``(federation_engine, source_type)`` and encapsulates the
engine-specific catalog operations for that source type: it projects a source (asset)
into a persisted engine ``CatalogEntry`` and declares its capability and mechanism.

A connector declares HOW the engine/Provisa obtains a source's rows — the ``Mechanism`` (REQ-841/
947/951): ATTACH_RW / ATTACH_R (engine reads the live source in place, read-write / read-only),
DIRECT (Provisa's native driver reads it single-source, bypassing the engine), FETCH (Provisa's
adapter reads an API/push source). Materialization (landing into a store) is an ORTHOGONAL strategy,
not a mechanism — any readable source is landable (``materializable``); a FETCH/DIRECT source, which
the engine cannot read live, is ALWAYS materialized so the engine can see it via the replica.

``CatalogEntry`` is derived, rebuildable engine state (REQ-843) — never a migrated table.
"""

# complexity-gate: allow-ble=1 reason="connector probe (REQ-904) reports any extension load failure as unavailable, surfacing the error type in the ProbeResult"

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.models import Source

from provisa.federation.connector_base import Capability, Connector, Mechanism

# --- Trino: a broad federator (many source types, all ATTACH via catalogs) ---


# The Trino connector is the SOURCE OF TRUTH for a source type's catalog (REQ-842): it declares the
# Trino ``connector.name`` (``trino_connector`` — the ``USING`` clause) and produces the catalog
# ``.properties`` (``details``). A source type with no Trino connector class is simply NOT reachable
# by Trino — no catalog is created for it (catalog.py consults this registry, never a parallel map).


class _TrinoConnector(Connector):
    """Base for a Trino catalog connector: ATTACH mechanism, a declared ``trino_connector`` name, and
    a ``details`` that returns the connector ``.properties`` (minus ``connector.name``, which the
    ``CREATE CATALOG ... USING <trino_connector>`` clause supplies)."""

    engine = "trino"
    mechanism = Mechanism.ATTACH_RW  # primary: federated live in place
    trino_connector: str = ""  # the Trino connector.name for the USING clause

    def capability(self) -> Capability:
        return Capability(
            predicate_pushdown=True, join_pushdown=True, aggregate_pushdown=True, write=True
        )


class _TrinoJdbcConnector(_TrinoConnector):
    """Any JDBC-reachable relational/warehouse/lake source: a connection-url + credentials, built from
    the source's ``jdbc_url``. One class parametrized by ``source_type`` + ``trino_connector``."""

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets

        host = resolve_secrets(source.host or "")
        jdbc_url = source.jdbc_url(host=host, port=source.port)
        if not jdbc_url:
            return {}
        return {
            "connection-url": jdbc_url,
            "connection-user": resolve_secrets(source.username or ""),
            "connection-password": resolve_secrets(source.password or ""),
            "statistics.enabled": "false",
        }


class TrinoPostgresConnector(_TrinoJdbcConnector):
    source_type = "postgresql"
    trino_connector = "postgresql"
    materialized_store = True  # REQ-846: PG is the one proven materialized store today


class TrinoMysqlConnector(_TrinoJdbcConnector):
    source_type = "mysql"
    trino_connector = "mysql"


class TrinoSqlServerConnector(_TrinoJdbcConnector):
    source_type = "sqlserver"
    trino_connector = "sqlserver"


def _jdbc_trino(source_type: str, trino_connector: str) -> type[_TrinoJdbcConnector]:
    """A JDBC Trino connector class for ``source_type`` published under ``trino_connector``."""
    return type(
        f"Trino_{source_type}_Connector",
        (_TrinoJdbcConnector,),
        {"source_type": source_type, "trino_connector": trino_connector},
    )


# The remaining JDBC-family source types (relational + warehouse + lake), each its own catalog under
# the named Trino connector (REQ-229). connection details come from the source's jdbc_url uniformly.
_TRINO_JDBC_TYPES: dict[str, str] = {
    "mariadb": "mariadb",
    "singlestore": "singlestore",
    "oracle": "oracle",
    # Wire-compatible RDBs read via the base wire's Trino connector (REQ-950)
    "cockroachdb": "postgresql",
    "yugabytedb": "postgresql",
    "greenplum": "postgresql",
    "tidb": "mysql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "redshift": "redshift",
    "databricks": "delta_lake",
    "hive": "hive",
    "hive_s3": "hive",
    "delta_lake": "delta_lake",
    "iceberg": "iceberg",
    "druid": "druid",
    "exasol": "exasol",
}


class TrinoPgBackedConnector(_TrinoConnector):
    """sqlite/openapi: their data is LANDED into the local Postgres (sqlite migrated at registration,
    openapi responses cached) and Trino reads that PG replica. FETCH — Provisa materializes them
    first, then Trino reads the replica — not an in-place ATTACH of the live source."""

    trino_connector = "postgresql"
    mechanism = Mechanism.FETCH  # Provisa lands sqlite/openapi into PG; Trino reads the replica

    def details(self, source: Source) -> dict:
        import os

        pg_host = os.environ.get("POSTGRES_HOST") or os.environ["PG_HOST"]
        jdbc = (
            f"jdbc:postgresql://{pg_host}:{os.environ.get('PG_PORT', '5432')}/"
            f"{os.environ['PG_DATABASE']}?autosave=conservative"
        )
        return {
            "connection-url": jdbc,
            "connection-user": os.environ["PG_USER"],
            "connection-password": os.environ["PG_PASSWORD"],
            "statistics.enabled": "false",
        }


class TrinoSqliteConnector(TrinoPgBackedConnector):
    source_type = "sqlite"


class TrinoOpenapiConnector(TrinoPgBackedConnector):
    source_type = "openapi"


class TrinoMongoConnector(_TrinoConnector):
    source_type = "mongodb"
    trino_connector = "mongodb"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets

        host = resolve_secrets(source.host or "")
        user = resolve_secrets(source.username or "")
        pw = resolve_secrets(source.password or "")
        url = (
            f"mongodb://{user}:{pw}@{host}:{source.port}/"
            if user
            else f"mongodb://{host}:{source.port}/"
        )
        return {"mongodb.connection-url": url, "mongodb.schema-collection": "_schema"}


class TrinoCassandraConnector(_TrinoConnector):
    source_type = "cassandra"
    trino_connector = "cassandra"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets

        return {
            "cassandra.contact-points": resolve_secrets(source.host or ""),
            "cassandra.native-protocol-port": str(source.port),
            "cassandra.load-policy.dc-aware.local-dc": "datacenter1",
            "cassandra.consistency-level": "ONE",
        }


class TrinoFilesConnector(_TrinoConnector):
    source_type = "files"
    trino_connector = "file"

    def capability(self) -> Capability:
        return Capability()

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets

        if source.path is None:
            raise ValueError(
                f"Source {source.id!r}: 'path' (glob pattern) is required for files connector"
            )
        return {
            "glob": resolve_secrets(source.path),
            "recursive": "true",
            "schema-name": source.id.replace("-", "_"),
            "execution-engine": "LINQ4J",
            "case-insensitive-name-matching": "true",
        }


class TrinoSharepointConnector(_TrinoConnector):
    source_type = "sharepoint"
    trino_connector = "sharepoint"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets

        mapping = {
            k: resolve_secrets(v) if isinstance(v, str) else v for k, v in source.mapping.items()
        }
        props: dict = {
            "site-url": resolve_secrets(source.base_url or source.host or ""),
            "auth-type": mapping.get("auth_type", "CLIENT_CREDENTIALS"),
        }
        if source.username:
            props["client-id"] = resolve_secrets(source.username)
        pw = resolve_secrets(source.password or "")
        if pw:
            props["client-secret"] = pw
        if source.database:
            props["tenant-id"] = resolve_secrets(source.database)
        if mapping.get("certificate_path"):
            props["certificate-path"] = mapping["certificate_path"]
        if mapping.get("certificate_password"):
            props["certificate-password"] = mapping["certificate_password"]
        props["case-insensitive-name-matching"] = "true"
        return props


class TrinoSplunkConnector(_TrinoConnector):
    source_type = "splunk"
    trino_connector = "splunk"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets

        mapping = {
            k: resolve_secrets(v) if isinstance(v, str) else v for k, v in source.mapping.items()
        }
        host = resolve_secrets(source.host or "")
        port = source.port or 8089
        pw = resolve_secrets(source.password or "")
        props: dict = {"url": resolve_secrets(source.base_url or f"https://{host}:{port}")}
        if mapping.get("use_token", True) and pw:
            props["token"] = pw
        else:
            if source.username:
                props["user"] = resolve_secrets(source.username)
            if pw:
                props["password"] = pw
        if source.database:
            props["app"] = source.database
        if mapping.get("datamodel_filter"):
            props["datamodel-filter"] = mapping["datamodel_filter"]
        if mapping.get("disable_ssl_validation"):
            props["disable-ssl-validation"] = "true"
        props["case-insensitive-name-matching"] = "true"
        return props


class _TrinoMappingDslConnector(_TrinoConnector):
    """redis/elasticsearch/prometheus: catalog properties come from the type's mapping-DSL generator
    (the source module is the source of truth for the DSL); this connector routes to it."""

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets
        from provisa.core.trino_catalog_files import catalog_properties_for

        props = catalog_properties_for(source, resolve_secrets(source.password or ""))
        return props or {}


class TrinoRedisConnector(_TrinoMappingDslConnector):
    source_type = "redis"
    trino_connector = "redis"


class TrinoElasticsearchConnector(_TrinoMappingDslConnector):
    source_type = "elasticsearch"
    trino_connector = "elasticsearch"


class TrinoPrometheusConnector(_TrinoMappingDslConnector):
    source_type = "prometheus"
    trino_connector = "prometheus"


class TrinoKafkaConnector(_TrinoConnector):
    source_type = "kafka"
    trino_connector = "kafka"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        # Kafka catalog props are built per ``kafka_sources[]`` entry, not from a Source row, by
        # trino_catalog_files.kafka_catalog_props — Kafka registers via its own create_kafka_catalog
        # path. This connector exists so Kafka is REACHABLE (in engine.connectors) for federate().
        del source
        return {}


def build_trino_connectors() -> list[_TrinoConnector]:
    """Every Trino catalog connector (REQ-842) — the complete, authoritative reach of the Trino
    engine, one per ``SOURCE_TO_CONNECTOR`` type. This is THE source of truth: a type absent here is
    not Trino-reachable and gets no catalog."""
    connectors: list[_TrinoConnector] = [
        TrinoPostgresConnector(),
        TrinoMysqlConnector(),
        TrinoSqlServerConnector(),
        TrinoSqliteConnector(),
        TrinoOpenapiConnector(),
        TrinoMongoConnector(),
        TrinoCassandraConnector(),
        TrinoFilesConnector(),
        TrinoSharepointConnector(),
        TrinoSplunkConnector(),
        TrinoRedisConnector(),
        TrinoElasticsearchConnector(),
        TrinoPrometheusConnector(),
        TrinoKafkaConnector(),
    ]
    connectors.extend(
        cls() for cls in (_jdbc_trino(st, tc) for st, tc in _TRINO_JDBC_TYPES.items())
    )
    return connectors


# source_type -> Trino connector, the registry catalog.py consults ("no connector ⇒ no catalog").
TRINO_CONNECTORS: dict[str, _TrinoConnector] = {c.source_type: c for c in build_trino_connectors()}


# --- DuckDB: a partial federator (postgres via ATTACH; files via scanner views) ---


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
        local_schema = f"ch_{source.id}"
        # Remote schema override rides on federation_hints (Source has no `schema` field — and
        # ``source.schema`` would resolve to pydantic's BaseModel.schema method, never the default).
        remote_schema = source.federation_hints.get("schema") or "public"
        return {
            "attach_ddl": [
                f'CREATE DATABASE IF NOT EXISTS "{local_schema}" ENGINE = PostgreSQL('
                f"'{source.host}:{source.port}', '{source.database}', "
                f"'{source.username}', '{source.password}', '{remote_schema}')"
            ],
            "local_schema": local_schema,
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
        local_schema = f"ch_{source.id}"
        return {
            "attach_ddl": [
                f'CREATE DATABASE IF NOT EXISTS "{local_schema}" ENGINE = MySQL('
                f"'{source.host}:{source.port}', '{source.database}', "
                f"'{source.username}', '{source.password}')"
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
        return {
            "engine_clause": (
                f"MongoDB('{source.host}:{source.port}', '{source.database}', "
                f"'{{table}}', '{source.username}', '{source.password}')"
            ),
            "requires_columns": True,
        }


def _clickhouse_file_engine(source: Source, fmt: str) -> str:
    """The ClickHouse table-engine clause for a file source, chosen by the path scheme:
    ``s3://`` → S3, ``http(s)://`` → URL, otherwise a local File. S3 credentials, when the bucket
    is private, ride on federation_hints (aws_key/aws_secret); absent means a public bucket."""
    path = source.path
    if path is None:
        raise ValueError(f"file source {source.id!r} has no path")
    if path.startswith("s3://"):
        key = source.federation_hints.get("aws_key")
        secret = source.federation_hints.get("aws_secret")
        creds = f", '{key}', '{secret}'" if key else ""
        return f"S3('{path}'{creds}, '{fmt}')"
    if path.startswith(("http://", "https://")):
        return f"URL('{path}', '{fmt}')"
    # The File table engine is format-FIRST (unlike S3/URL, which are url-first).
    return f"File('{fmt}', '{path}')"


class _ClickHouseFileConnector(Connector):
    """Mount a file source into ClickHouse via an S3/URL/File table engine (chosen by path scheme).

    ClickHouse infers the column schema for these engines, so the runtime issues a bare
    ``CREATE TABLE ... ENGINE = <clause>`` with no column list. The data is read in place.
    """

    engine = "clickhouse"
    mechanism = Mechanism.ATTACH_RW
    _format = ""  # ClickHouse input-format name

    def details(self, source: Source) -> dict:
        return {"engine_clause": _clickhouse_file_engine(source, self._format), "infer": True}


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


# --- Warehouse-native (Snowflake): self-only, land-into-self is a no-op ---


class WarehouseNativeConnector(Connector):
    """A self-only engine (e.g. Snowflake) cannot attach an external source live; Provisa reads
    it and lands a replica into the engine's store, which the engine reads — MATERIALIZED, not a
    live attach (the engine never lands; Provisa does — REQ-848/951)."""

    mechanism = Mechanism.DIRECT

    def __init__(self, engine: str, source_type: str) -> None:
        self.engine = engine
        self.source_type = source_type

    def capability(self) -> Capability:
        # Its own native store — full pushdown and writable.
        return Capability(
            predicate_pushdown=True, join_pushdown=True, aggregate_pushdown=True, write=True
        )

    def details(self, source: Source) -> dict:
        return {}  # land-into-self: nothing to attach; the table is already native
