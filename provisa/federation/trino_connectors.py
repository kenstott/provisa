# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c60-7b19-4d54-9e02-1c7a0d6f8b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino connector registry (REQ-842).

All Trino catalog connectors — one per reachable source type — extracted from connector.py.
``TRINO_CONNECTORS`` is the single source of truth for a type's Trino ``connector.name``
(the ``USING`` clause in ``CREATE CATALOG``).  A source type absent here gets no Trino catalog.

Trino connector classes follow the ``_TrinoConnector`` base: declare ``trino_connector``
(the ``connector.name``), and ``details()`` returns the catalog ``.properties`` body.
"""

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


# Lake/object source types Trino reads IN PLACE via a lakehouse catalog (iceberg/hive/delta) — a SCAN
# (no copy, freshness follows the files), not a live-DB VIRTUAL attach (REQ-951).
_TRINO_SCAN_TYPES = frozenset({"hive", "hive_s3", "delta_lake", "iceberg"})


def _jdbc_trino(source_type: str, trino_connector: str) -> type[_TrinoJdbcConnector]:
    """A JDBC Trino connector class for ``source_type`` published under ``trino_connector``. Lake types
    (``_TRINO_SCAN_TYPES``) declare SCAN; relational/warehouse types keep the base ATTACH_RW."""
    attrs: dict = {"source_type": source_type, "trino_connector": trino_connector}
    if source_type in _TRINO_SCAN_TYPES:
        attrs["mechanism"] = Mechanism.SCAN
    return type(f"Trino_{source_type}_Connector", (_TrinoJdbcConnector,), attrs)


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


class TrinoPinotConnector(_TrinoConnector):
    source_type = "pinot"
    trino_connector = "pinot"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, aggregate_pushdown=True)

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets

        # Trino's pinot connector reaches the cluster through the Pinot CONTROLLER's REST endpoint
        # (it discovers brokers/servers from there). pinot.controller-urls is host:port of the
        # controller — 9000 is the controller's default REST port.
        host = resolve_secrets(source.host or "")
        return {"pinot.controller-urls": f"{host}:{source.port or 9000}"}


# NOTE: no TrinoKuduConnector — Trino REMOVED the kudu (and phoenix) connector during its Java 24
# migration (documented through Trino 472, absent from trinodb/trino:481; the runtime factory list
# and /usr/lib/trino/plugin confirm it). A registry entry here asserts "Trino-reachable"; kudu is
# not, on this Trino build, so it stays out (REQ-1097). Re-add if a kudu-capable Trino is adopted.


class TrinoFilesConnector(_TrinoConnector):
    source_type = "files"
    trino_connector = "file"
    mechanism = Mechanism.SCAN  # the file catalog reads the glob in place — no copy (REQ-951)

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
    engine. This is THE source of truth for a type's Trino ``connector.name`` (``trino_connector``):
    a type absent here is not Trino-reachable and gets no catalog (REQ-947)."""
    connectors: list[_TrinoConnector] = [
        TrinoPostgresConnector(),
        TrinoMysqlConnector(),
        TrinoSqlServerConnector(),
        TrinoSqliteConnector(),
        TrinoOpenapiConnector(),
        TrinoMongoConnector(),
        TrinoCassandraConnector(),
        TrinoPinotConnector(),
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


def trino_connector_name(source_type: str) -> str | None:
    """The Trino catalog ``connector.name`` (the ``CREATE CATALOG … USING <name>`` label) for a source
    type, or None if Trino has no connector for it. The single source of truth is the Trino connector
    objects themselves — this retires the parallel ``SOURCE_TO_CONNECTOR`` name map (REQ-947)."""
    connector = TRINO_CONNECTORS.get(source_type)
    return connector.trino_connector if connector is not None else None
