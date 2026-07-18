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
_TRINO_SCAN_TYPES = frozenset({"hive", "delta_lake", "iceberg"})


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
    "delta_lake": "delta_lake",
    "iceberg": "iceberg",
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


class TrinoDruidConnector(_TrinoConnector):
    """Apache Druid, read via its broker's Avatica endpoint. NOT a plain JDBC connector: Trino's druid
    connector has no table-statistics support and rejects the base ``statistics.enabled`` property
    ("Configuration property 'statistics.enabled' was not used"), so it cannot reuse
    _TrinoJdbcConnector. connection-url comes from Source.jdbc_url() (the Avatica URL); user/password
    are omitted when empty (Druid quickstart has no auth)."""

    source_type = "druid"
    trino_connector = "druid"

    def capability(self) -> Capability:
        # Druid is read-only through Trino; it does push down predicates and aggregates.
        return Capability(predicate_pushdown=True, aggregate_pushdown=True)

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets

        host = resolve_secrets(source.host or "")
        jdbc_url = source.jdbc_url(host=host, port=source.port)
        if not jdbc_url:
            return {}
        props: dict = {"connection-url": jdbc_url}
        user = resolve_secrets(source.username or "")
        pw = resolve_secrets(source.password or "")
        if user:
            props["connection-user"] = user
        if pw:
            props["connection-password"] = pw
        return props


def _hive_metastore_props(source: Source) -> dict:
    """Common Hive catalog props: the Thrift metastore endpoint (Source.host:port, default 9083).
    Empty when host is unset — create_catalog then no-ops (a hive source with no metastore host is
    not reachable, matching the JDBC connectors that return {} for an empty connection-url)."""
    from provisa.core.secrets import resolve_secrets

    host = resolve_secrets(source.host or "")
    if not host:
        return {}
    port = source.port or 9083  # Hive metastore Thrift default port
    return {
        "hive.metastore": "thrift",
        "hive.metastore.uri": f"thrift://{host}:{port}",
        # Hive 3/4 metastores translate non-ACID "managed" tables to EXTERNAL; Trino refuses writes to
        # external tables unless this is enabled. A federated Hive lake's tables are external by nature
        # (data owned by the warehouse/object store), so governed writes must target them (REQ-1097).
        "hive.non-managed-table-writes-enabled": "true",
    }


class TrinoHiveConnector(_TrinoConnector):
    """Apache Hive tables read IN PLACE via a Thrift metastore (SCAN, a lakehouse read — REQ-951).
    Trino's hive connector is NOT a JDBC connector: it needs hive.metastore.uri
    (thrift://<metastore>:9083), not connection-url/statistics.enabled, so it cannot reuse
    _TrinoJdbcConnector (whose jdbc_url() is empty for hive anyway — that empty url is exactly why the
    generic JDBC path silently no-op'd the hive catalog before REQ-1097). The metastore endpoint comes
    from Source.host/Source.port; storage is the Hadoop-native filesystem (fs.hadoop.enabled) covering
    the local/HDFS warehouse paths the metastore records."""

    source_type = "hive"
    trino_connector = "hive"
    mechanism = Mechanism.SCAN  # lakehouse read: files/objects the metastore points at, no copy

    def details(self, source: Source) -> dict:
        props = _hive_metastore_props(source)
        if not props:
            return {}
        # One `hive` source type; the object store its tables live on is a config choice — the storage
        # backend (hadoop/local, S3, or ADLS) is carried in source.mapping["storage"] rather than in a
        # separate source type. Each backend wires Trino's hive connector with its native filesystem.
        storage = (source.mapping.get("storage") or "hadoop").lower()
        if storage in ("hadoop", "hdfs", "local", ""):
            # Table data on the Hadoop-native filesystem (local file:/ or HDFS paths the metastore
            # recorded). Trino's hadoop filesystem handles both; no object-store props are emitted.
            props["fs.hadoop.enabled"] = "true"
            return props
        if storage == "s3":
            return _hive_s3_props(source, props)
        if storage in ("azure", "adls"):
            return _hive_adls_props(source, props)
        raise ValueError(f"Source {source.id!r}: unknown hive storage backend {storage!r}")


class TrinoHiveS3Connector(_TrinoConnector):
    """REQ-229: ``hive_s3`` is a distinct Trino-only source type — a Hive lake whose table data lives on
    S3-compatible object storage. It reuses the Hive Thrift metastore (connector.name is still ``hive``)
    but, unlike the generic ``hive`` type where the object store is a ``mapping['storage']`` choice,
    selecting ``hive_s3`` DECLARES S3 storage: it always wires Trino's native S3 filesystem from the
    source mapping, and a missing s3 mapping is a misconfiguration that fails loud (no fallback)."""

    source_type = "hive_s3"
    trino_connector = "hive"
    mechanism = Mechanism.SCAN  # lakehouse read: S3 objects the metastore points at, no copy

    def details(self, source: Source) -> dict:
        props = _hive_metastore_props(source)
        if not props:
            return {}
        return _hive_s3_props(source, props, err_ctx="hive_s3")


def _hive_s3_props(source: Source, props: dict, err_ctx: str = "hive S3 storage") -> dict:
    """Wire Trino's hive connector with the NATIVE S3 filesystem (fs.native-s3.enabled + s3.*). The S3
    settings come from source.mapping (endpoint/access_key_id/secret_access_key/region) — an S3-backed
    hive source with no S3 mapping is a misconfiguration, so this fails loud (no fallback). ``err_ctx``
    names the requirement in the error (``hive_s3`` for the dedicated type, ``hive S3 storage`` for the
    storage-mapping path on the generic ``hive`` type)."""
    from provisa.core.secrets import resolve_secrets

    m = {k: resolve_secrets(v) if isinstance(v, str) else v for k, v in source.mapping.items()}
    endpoint = m.get("s3_endpoint") or m.get("endpoint")
    access = m.get("access_key_id") or m.get("aws_access_key_id")
    secret = m.get("secret_access_key") or m.get("aws_secret_access_key")
    region = m.get("region") or m.get("s3_region")
    if not (endpoint and access and secret and region):
        raise ValueError(
            f"Source {source.id!r}: {err_ctx} requires s3 endpoint, access_key_id, "
            "secret_access_key and region in mapping"
        )
    props["fs.native-s3.enabled"] = "true"
    props["s3.endpoint"] = endpoint
    props["s3.aws-access-key"] = access
    props["s3.aws-secret-key"] = secret
    props["s3.region"] = region
    # path-style addressing is required by MinIO and any non-AWS S3-compatible endpoint; default on
    # unless the mapping explicitly declares virtual-hosted addressing (documented default, REQ-1097 —
    # not a silent fallback for a missing required value).
    props["s3.path-style-access"] = "false" if m.get("path_style") is False else "true"
    return props


def _hive_adls_props(source: Source, props: dict) -> dict:
    """Wire Trino's hive connector with the NATIVE Azure filesystem (fs.native-azure.enabled +
    azure.*) for a Hive lake whose tables live on ADLS Gen2. The storage account + credential (shared
    access key or SAS token) come from source.mapping — a missing account/credential is a
    misconfiguration, so this fails loud (no fallback)."""
    from provisa.core.secrets import resolve_secrets

    m = {k: resolve_secrets(v) if isinstance(v, str) else v for k, v in source.mapping.items()}
    account = m.get("storage_account")
    access_key = m.get("access_key")
    sas_token = m.get("sas_token")
    if not account or not (access_key or sas_token):
        raise ValueError(
            f"Source {source.id!r}: hive ADLS storage requires storage_account and an access_key "
            "or sas_token in mapping"
        )
    props["fs.native-azure.enabled"] = "true"
    props["azure.auth-type"] = "ACCESS_KEY" if access_key else "SAS"
    if access_key:
        props["azure.access-key"] = access_key
    else:
        props["azure.sas-token"] = sas_token
    return props


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
        TrinoDruidConnector(),
        TrinoHiveConnector(),
        TrinoHiveS3Connector(),
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
