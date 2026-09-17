# Copyright (c) 2026 Kenneth Stott
# Canary: 0f86dfed-dfca-45a0-beba-39bb56617b90
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

    def post_create(self, conn, source: Source, catalog_name: str) -> None:
        """Optional follow-up DDL after ``CREATE CATALOG`` succeeds. Default no-op — most
        connectors expose their schemas/tables the moment the catalog exists. Lakehouse
        connectors backed by a metastore that starts out empty (delta_lake/iceberg, REQ-1743)
        override this to register the source's one table into the metastore."""
        return


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

    def details(self, source: Source) -> dict:
        import os

        from provisa.core.secrets import resolve_secrets

        host = resolve_secrets(source.host or "")
        port = source.port
        # REQ-ITEST: Trino runs inside Docker; source.host may be 'localhost' (host-visible
        # address) which is unreachable from inside the coordinator/worker containers.
        # PROVISA_ENGINE_CONTROL_PLANE_HOST/PORT redirect catalog JDBC URLs to the
        # Docker-internal address (e.g. 'postgres:5432') — identical to what
        # trino_system_catalogs.engine_visible_address() does for system catalogs.
        # Not set in production → no behaviour change.
        host = os.environ.get("PROVISA_ENGINE_CONTROL_PLANE_HOST", host)
        port = int(os.environ.get("PROVISA_ENGINE_CONTROL_PLANE_PORT", port or 5432))
        jdbc_url = source.jdbc_url(host=host, port=port)
        if not jdbc_url:
            return {}
        return {
            "connection-url": jdbc_url,
            "connection-user": resolve_secrets(source.username or ""),
            "connection-password": resolve_secrets(source.password or ""),
            "statistics.enabled": "false",
        }


class TrinoMysqlConnector(_TrinoJdbcConnector):
    source_type = "mysql"
    trino_connector = "mysql"


class TrinoSqlServerConnector(_TrinoJdbcConnector):
    source_type = "sqlserver"
    trino_connector = "sqlserver"


class TrinoSnowflakeConnector(_TrinoConnector):
    """Snowflake, read live via Trino's own dedicated snowflake connector (REQ-1730). Not a plain
    JDBC connector: besides connection-url/user/password it needs snowflake.account/database/
    warehouse/role as SEPARATE catalog properties — the generic _TrinoJdbcConnector (connection-url
    only) produced no working catalog here, the same gap delta_lake/iceberg had (REQ-842).
    Source.host is the full "<account>.snowflakecomputing.com" the Sources form's Account URL field
    collects (SourceFormFields.tsx) — SnowflakeDriver.connect (the DIRECT driver) already passes
    this same value straight to snowflake-connector-python's own `account` kwarg, so it's a proven
    account identifier; warehouse/role come from federation_hints exactly like that driver reads
    them (SourcesPage.tsx's federationHints only ever sets warehouse/schema/role for this type)."""

    source_type = "snowflake"
    trino_connector = "snowflake"

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets

        host = resolve_secrets(source.host or "")
        if not host:
            return {}
        suffix = ".snowflakecomputing.com"
        account = host[: -len(suffix)] if host.endswith(suffix) else host
        host_with_domain = host if host.endswith(suffix) else f"{host}{suffix}"
        props = {
            "connection-url": f"jdbc:snowflake://{host_with_domain}",
            "connection-user": resolve_secrets(source.username or ""),
            "connection-password": resolve_secrets(source.password or ""),
            "snowflake.account": account,
            "snowflake.database": resolve_secrets(source.database or ""),
        }
        warehouse = source.federation_hints.get("warehouse")
        if warehouse:
            props["snowflake.warehouse"] = warehouse
        role = source.federation_hints.get("role")
        if role:
            props["snowflake.role"] = role
        return props


class TrinoBigQueryConnector(_TrinoConnector):
    """BigQuery, read live via Trino's own dedicated bigquery connector (REQ-1730). Not a JDBC
    connector at all (BigQuery has no JDBC wire) — needs bigquery.project-id plus a credentials
    property, so the generic _TrinoJdbcConnector (empty jdbc_url() for this type, same gap
    delta_lake/iceberg/snowflake/csv/parquet had) produced no working catalog (REQ-842).
    Source.database is the GCP project id (SourceFormFields.tsx's "Project ID" field). Credentials:
    service_account auth stores the key file path in federation_hints["credentials_path"]
    (SourcesPage.tsx) — read and base64-encode it into bigquery.credentials-key, the same bytes
    Trino's connector would read from a credentials-file path, just inlined so no new mount into
    Trino's container is needed. application_default auth (no credentials_path) falls back to
    GOOGLE_APPLICATION_CREDENTIALS on this (the app) process — the same key BigQueryDriver's own
    Application Default Credentials resolution already uses."""

    source_type = "bigquery"
    trino_connector = "bigquery"

    def details(self, source: Source) -> dict:
        import base64
        import os

        from provisa.core.secrets import resolve_secrets

        project = resolve_secrets(source.database or "")
        if not project:
            return {}
        props = {"bigquery.project-id": project}
        creds_path = source.federation_hints.get("credentials_path") or os.environ.get(
            "GOOGLE_APPLICATION_CREDENTIALS"
        )
        if creds_path and os.path.exists(creds_path):
            with open(creds_path, "rb") as f:
                props["bigquery.credentials-key"] = base64.b64encode(f.read()).decode("ascii")
        return props


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
    "clickhouse": "clickhouse",
    "redshift": "redshift",
    "databricks": "delta_lake",
    "exasol": "exasol",
}


class TrinoPgBackedConnector(_TrinoConnector):
    """sqlite/openapi/data-quality checkers: their data is LANDED into the local Postgres by the
    event loop and the query path's residency prep (a sqlite file read by its connector, openapi
    responses fetched, checker scans run) and Trino reads that PG replica. FETCH — Provisa
    materializes them first, then Trino reads the replica — not an in-place ATTACH of the live
    source."""

    trino_connector = "postgresql"
    mechanism = Mechanism.FETCH  # Provisa lands the rows into PG; Trino reads the replica

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


# REQ-1602-followup: graphql_remote is landed the same way openapi is (materialize_exec.py) --
# Trino reads the PG replica, not the live GraphQL endpoint. Without a connector here
# create_catalog skips every graphql_remote source and its catalog name resolves to nothing
# (CATALOG_NOT_FOUND), exactly the failure TrinoGreatExpectationsConnector's comment describes.
class TrinoGraphqlRemoteConnector(TrinoPgBackedConnector):
    source_type = "graphql_remote"


# REQ-1443: a checker has no remote table to federate against — its rows ARE one scan's results,
# produced by running the contract and landed like any other produced dataset. Trino reads that
# landed replica out of the Postgres store, so the checker's catalog is PG-backed exactly as
# sqlite's and openapi's are. Without a connector here `create_catalog` skips the source entirely
# and the compiler's emitted catalog name resolves to nothing (CATALOG_NOT_FOUND).
class TrinoGreatExpectationsConnector(TrinoPgBackedConnector):
    source_type = "great_expectations"


class TrinoSodaConnector(TrinoPgBackedConnector):
    source_type = "soda"


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
    """The ``file`` catalog's own config docstring (extracted from the bundled
    calcite-trino-file plugin, trino/plugins/trino-file/) names the trade-off directly:
    "execution engine: DUCKDB (default), PARQUET, LINQ4J or ARROW. DUCKDB reads CSV/Parquet
    natively without Hadoop; PARQUET converts via Hadoop, which is incompatible with the JDK 25
    that Trino requires." LINQ4J (the value this connector used before) turned out to have the
    same problem transitively for any glob containing a .parquet file — its own Parquet
    statistics extractor calls Hadoop's UserGroupInformation.getCurrentUser(), which throws
    UnsupportedOperationException("getSubject is not supported") on JDK 25 (Security Manager
    fully removed, JEP 486) and silently EXCLUDES the whole table rather than failing loud
    (verified live 2026-09-17 via the container's own docker logs). DUCKDB has no such
    dependency and was verified live against both a single file and this exact multi-file
    recursive glob (REQ-842/REQ-1730)."""

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
            "execution-engine": "DUCKDB",
            "case-insensitive-name-matching": "true",
        }


class _TrinoSingleFileConnector(_TrinoConnector):
    """csv/parquet: a single-file instance of the same ``file`` catalog TrinoFilesConnector uses
    for a glob of many (REQ-842/REQ-1730) — csv/parquet had no Trino connector class at all before
    this (same empty-catalog-properties gap delta_lake/iceberg and snowflake had), so single-file
    sources were never Trino-reachable. The ``file`` connector exposes one table per glob-matched
    file, named after the file's own basename (verified live via a northwind glob, whose
    customers.csv/orders.csv/products.csv become tables "customers"/"orders"/"products") — the
    table name isn't otherwise configurable.

    DuckDBCsvConnector/DuckDBParquetConnector (connector_duckdb.py) instead register ONE view
    named after the SOURCE ID under a fixed "main" schema (REQ-1732) — the same schema/table pair
    ``registered_tables`` records regardless of engine, so a source registered under DuckDB and
    replayed under Trino (REQ-1730) must resolve to that exact "main".<source.id> pair here too.
    schema-name is therefore fixed at "main" (isolated per-source already, since each source gets
    its own Trino catalog); callers must point ``path`` at a file whose OWN basename already IS
    the source id (a per-run copy — this class does not rename anything itself).

    ``execution-engine: DUCKDB`` (see TrinoFilesConnector's own docstring for why) is what makes
    parquet work here at all — the LINQ4J default this class started with silently excluded any
    .parquet file (verified live 2026-09-17)."""

    trino_connector = "file"
    mechanism = Mechanism.SCAN

    def capability(self) -> Capability:
        return Capability()

    def details(self, source: Source) -> dict:
        import os

        from provisa.core.secrets import resolve_secrets

        if source.path is None:
            raise ValueError(f"Source {source.id!r}: 'path' is required for {self.source_type}")
        path = resolve_secrets(source.path)
        # The literal single-file path is not itself a valid glob for this connector — verified
        # live 2026-09-17: pointing `glob` straight at one file produced a catalog with zero
        # tables. A wildcard matching within the file's own directory works (that directory holds
        # exactly this one file, so the wildcard still resolves to exactly one table).
        glob = f"{os.path.dirname(path)}/*"
        return {
            "glob": glob,
            "recursive": "false",
            "schema-name": "main",
            "execution-engine": "DUCKDB",
            "case-insensitive-name-matching": "true",
        }


class TrinoCsvConnector(_TrinoSingleFileConnector):
    source_type = "csv"


class TrinoParquetConnector(_TrinoSingleFileConnector):
    source_type = "parquet"


def _lake_metastore_uri() -> str:
    """The shared Hive Thrift metastore delta_lake/iceberg catalogs register their one table into
    (REQ-1730). Unlike the ``hive``/``hive_s3`` types, a delta_lake/iceberg Source carries no
    host/port of its own (REQ-899: it is a bare warehouse ``path``, matching DuckDB's own
    catalog-less delta_scan/iceberg_scan reads) — the metastore is shared infrastructure, not a
    per-source config choice, so its address is an environment override
    (PROVISA_ENGINE_LAKEHOUSE_METASTORE_HOST/PORT), the same override-else-absent shape as
    PROVISA_ENGINE_CONTROL_PLANE_HOST (trino_connectors.py's TrinoPostgresConnector). Unset in
    production today (no metastore is deployed) — a delta_lake/iceberg catalog then gets no
    properties and no catalog, same graceful no-op as every other empty-props connector here."""
    import os

    host = os.environ.get("PROVISA_ENGINE_LAKEHOUSE_METASTORE_HOST", "")
    if not host:
        return ""
    port = os.environ.get("PROVISA_ENGINE_LAKEHOUSE_METASTORE_PORT", "9083")
    return f"thrift://{host}:{port}"


def _lake_post_create(conn, source: Source, catalog_name: str, *, is_iceberg: bool) -> None:
    """Register the source's one table into the shared metastore (REQ-1730).

    A fresh hive_metastore-backed delta_lake/iceberg catalog starts out with no databases/tables
    at all — unlike ``hive``/``hive_s3``, whose metastore is populated ahead of time by an
    external process, delta_lake/iceberg's fixture tables are written directly to disk (pyiceberg/
    deltalake, REQ-1743) with no metastore entry. Trino's own ``register_table`` procedure is the
    documented way to attach an existing table's files to a catalog without rewriting them;
    schema "main" and table = source.id mirror exactly what DuckDBIcebergConnector/
    DuckDBDeltaConnector name the view under DuckDB (connector_duckdb.py), so the SAME
    schema.table the app recorded in ``registered_tables`` under DuckDB resolves under Trino too.
    """
    import re

    from provisa.core.catalog import _validate_identifier
    from provisa.core.secrets import resolve_secrets
    from provisa.federation.trino_types import TrinoQueryError

    path = resolve_secrets(source.path or "")
    if not path:
        return
    table = _validate_identifier(source.id.replace("-", "_"))
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.main")
    cur.fetchall()
    location = path if "://" in path else f"file://{path}"
    register_sql = (
        f"CALL {catalog_name}.system.register_table("
        f"schema_name => 'main', table_name => '{table}', table_location => '{location}'"
    )
    try:
        cur.execute(register_sql + ")")
        cur.fetchall()
    except TrinoQueryError as exc:
        # A table whose metadata dir carries more than one same-generation metadata file (here:
        # make-file-lake-fixtures.py's own v1.metadata.json, copied alongside the real
        # 00001-<uuid>.metadata.json pyiceberg wrote, for a DuckDB iceberg_scan compatibility some
        # older DuckDB builds needed) makes Trino's own latest-file auto-discovery ambiguous
        # (ICEBERG_INVALID_METADATA: "More than one latest metadata file found ... are [file://.../
        # 00001-<uuid>.metadata.json, file://.../v1.metadata.json]"). register_table's optional
        # metadata_file_name disambiguates (trino#16363 documents the same case for Spark-written
        # tables) — parsed straight out of Trino's own error rather than listed off the local
        # filesystem, since this process may run somewhere that can't see the path it's given at
        # all (e.g. a remote engine host); Trino's own error message names the real file either way.
        if not is_iceberg or "More than one latest metadata file found" not in str(exc):
            raise
        candidates = re.findall(r"([^/\s,]+\.metadata\.json)", str(exc))
        versioned = [f for f in candidates if not re.match(r"^v\d+\.metadata\.json$", f)]
        if not versioned:
            raise
        cur.execute(register_sql + f", metadata_file_name => '{versioned[0]}')")
        cur.fetchall()


class TrinoIcebergConnector(_TrinoConnector):
    """Apache Iceberg table read IN PLACE via a Hive Thrift metastore (SCAN, REQ-951/REQ-1730).
    Trino's iceberg connector is not a JDBC connector — it needs iceberg.catalog.type +
    hive.metastore.uri, not connection-url, so (like TrinoHiveConnector) it cannot reuse
    _TrinoJdbcConnector. post_create registers the source's one table via CALL
    iceberg.system.register_table since the shared metastore starts out empty."""

    source_type = "iceberg"
    trino_connector = "iceberg"
    mechanism = Mechanism.SCAN

    def details(self, source: Source) -> dict:
        uri = _lake_metastore_uri()
        if not uri:
            return {}
        return {
            "iceberg.catalog.type": "hive_metastore",
            "hive.metastore.uri": uri,
            "fs.hadoop.enabled": "true",
            "iceberg.register-table-procedure.enabled": "true",
        }

    def post_create(self, conn, source: Source, catalog_name: str) -> None:
        _lake_post_create(conn, source, catalog_name, is_iceberg=True)


class TrinoDeltaLakeConnector(_TrinoConnector):
    """Delta Lake table read IN PLACE via a Hive Thrift metastore (SCAN, REQ-951/REQ-1730). Like
    TrinoIcebergConnector, Trino's delta_lake connector needs hive.metastore.uri, not a JDBC
    connection-url. post_create registers the source's one table via CALL
    delta.system.register_table."""

    source_type = "delta_lake"
    trino_connector = "delta_lake"
    mechanism = Mechanism.SCAN

    def details(self, source: Source) -> dict:
        uri = _lake_metastore_uri()
        if not uri:
            return {}
        return {
            "hive.metastore.uri": uri,
            "fs.hadoop.enabled": "true",
            "delta.register-table-procedure.enabled": "true",
        }

    def post_create(self, conn, source: Source, catalog_name: str) -> None:
        _lake_post_create(conn, source, catalog_name, is_iceberg=False)


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
    """REQ-1730: the underlying trino-splunk plugin (a standalone wrapper over Calcite's splunk
    adapter, distinct from the bundled Calcite pgwire bridge DuckDB attaches through) used to
    register its schema under the fixed literal "splunk" regardless of configuration — a table
    registered under DuckDB (whose pgwire bridge exposes schema=<sql-normalized source id>,
    pgwire_replica.schema_name()) physically addressed a schema Trino's connector never had,
    verified live via SCHEMA_NOT_FOUND. Fixed upstream (calcite/splunk's SplunkDriver now honors a
    'schema' connection property, kenstott/calcite@28db96ca1) — passing the SAME schema name here
    keeps DuckDB and Trino addressing the identical physical schema for a table registered once
    under either engine."""

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
        props: dict = {
            "url": resolve_secrets(source.base_url or f"https://{host}:{port}"),
            # Same sql-normalization pgwire_replica.schema_name() applies — inlined rather than
            # imported to avoid pulling this module (and its own strategy/engine/executor import
            # chain) into trino_connectors.py's dependency graph (lint-imports' compiler/cypher
            # must-not-import-executor contract; core.models -> trino_connectors is already on
            # that path). Keep this literally in sync with pgwire_replica.schema_name() if it ever
            # changes.
            "schema": source.id.replace("-", "_"),
        }
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


class TrinoGsheetsConnector(_TrinoConnector):
    """Google Sheets read live in place through Trino's ``gsheets`` connector. Trino addresses sheets
    through a METADATA sheet — a spreadsheet whose rows map a table name to the sheet range that backs
    it — so the catalog needs that sheet's id plus a service-account key, and every table in it becomes
    a table in this catalog. That is a different addressing model from the DuckDB engine's
    ``read_gsheet`` scanner view, which names one spreadsheet per source.

    Read-only: the connector has no write path upstream."""

    source_type = "google_sheets"
    trino_connector = "gsheets"
    mechanism = (
        Mechanism.ATTACH_R
    )  # live in place, read-only — Sheets is not writable through Trino

    def capability(self) -> Capability:
        return (
            Capability()
        )  # the Sheets connector pushes nothing down; Trino filters after the fetch

    def details(self, source: Source) -> dict:
        from provisa.core.secrets import resolve_secrets

        # Both are required by Trino's connector — a catalog missing either fails to load, so an
        # absent value is a misconfigured source and is raised here rather than papered over.
        if not source.database:
            raise ValueError(
                f"Source {source.id!r}: 'database' (Google metadata sheet id) is required for the "
                "gsheets connector"
            )
        credentials = source.mapping.get("credentials_json")
        if not credentials:
            raise ValueError(
                f"Source {source.id!r}: mapping 'credentials_json' (service-account key path) is "
                "required for the gsheets connector"
            )
        return {
            "gsheets.metadata-sheet-id": resolve_secrets(source.database),
            "gsheets.credentials-path": resolve_secrets(credentials),
            "case-insensitive-name-matching": "true",
        }


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
        TrinoSnowflakeConnector(),
        TrinoBigQueryConnector(),
        TrinoSqliteConnector(),
        TrinoOpenapiConnector(),
        TrinoGraphqlRemoteConnector(),
        TrinoGreatExpectationsConnector(),
        TrinoSodaConnector(),
        TrinoMongoConnector(),
        TrinoCassandraConnector(),
        TrinoPinotConnector(),
        TrinoDruidConnector(),
        TrinoHiveConnector(),
        TrinoHiveS3Connector(),
        TrinoIcebergConnector(),
        TrinoDeltaLakeConnector(),
        TrinoFilesConnector(),
        TrinoCsvConnector(),
        TrinoParquetConnector(),
        TrinoSharepointConnector(),
        TrinoSplunkConnector(),
        TrinoRedisConnector(),
        TrinoElasticsearchConnector(),
        TrinoPrometheusConnector(),
        TrinoGsheetsConnector(),
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
