# Copyright (c) 2026 Kenneth Stott
# Canary: a1f2052b-6d0c-4f11-ac0a-a427d9968419
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino dynamic catalog management via SQL CREATE/DROP CATALOG."""

# Requirements: REQ-012, REQ-017, REQ-250, REQ-251

import logging
import os
import re
import time

import trino

from provisa.core.models import Source

log = logging.getLogger(__name__)

# A coordinator that (re)started reports SERVER_STARTING_UP until it finishes
# initializing. Catalog registration runs on every app boot and must work
# "regardless of Trino start order" (see create_kafka_catalog), so it waits for
# the coordinator to become query-ready rather than failing boot.
_STARTING_UP = "SERVER_STARTING_UP"
_READY_TIMEOUT_SECS = float(os.environ.get("PROVISA_TRINO_READY_TIMEOUT", "120"))


def wait_until_ready(conn: trino.dbapi.Connection, timeout: float = _READY_TIMEOUT_SECS) -> None:
    """Block until the coordinator answers a trivial query (past SERVER_STARTING_UP).

    Raises the last error if the coordinator is still initializing at the deadline —
    a genuinely down engine must surface, not be swallowed.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            return
        except trino.exceptions.TrinoQueryError as e:
            if e.error_name != _STARTING_UP or time.monotonic() >= deadline:
                raise
            time.sleep(2)


_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str) -> str:
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid Trino identifier: {name!r}")
    return name


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for Trino SQL string literals."""
    return value.replace("'", "''")


def _to_catalog_name(source_id: str) -> str:
    return _validate_identifier(source_id.replace("-", "_"))


def _build_catalog_properties(
    source: Source, resolved_password: str
) -> dict[str, str]:  # REQ-250, REQ-251
    """Build Trino connector properties from a source definition."""
    from provisa.core.secrets import resolve_secrets

    stype = source.type.value
    host = resolve_secrets(source.host or "")
    port = source.port
    username = resolve_secrets(source.username or "")

    # REQ-251: NoSQL/non-relational connectors (redis/elasticsearch/prometheus)
    # build their catalog properties from the type-specific mapping DSL.
    from provisa.core.trino_catalog_files import catalog_properties_for

    _mapping_props = catalog_properties_for(source, resolved_password)
    if _mapping_props is not None:
        return _mapping_props

    # SQLite and OpenAPI sources — data lives in the local PG instance
    # (SQLite tables are migrated to PG at registration; OpenAPI responses cached there)
    if stype in ("sqlite", "openapi"):
        pg_host = os.environ.get("POSTGRES_HOST", os.environ.get("PG_HOST", "postgres"))
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_database = os.environ.get("PG_DATABASE", "provisa")
        pg_user = os.environ.get("PG_USER", "provisa")
        pg_pw = os.environ.get("PG_PASSWORD", "provisa")
        jdbc = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}?autosave=conservative"
        return {
            "connection-url": jdbc,
            "connection-user": pg_user,
            "connection-password": pg_pw,
            "statistics.enabled": "false",
        }

    # MongoDB connector
    if stype == "mongodb":
        url = f"mongodb://{host}:{port}/"
        if username:
            url = f"mongodb://{username}:{resolved_password}@{host}:{port}/"
        return {
            "mongodb.connection-url": url,
            "mongodb.schema-collection": "_schema",
        }

    # SharePoint connector (Apache Calcite, kenstott/calcite)
    if stype == "sharepoint":
        mapping = {
            k: resolve_secrets(v) if isinstance(v, str) else v for k, v in source.mapping.items()
        }
        site_url = resolve_secrets(source.base_url or source.host or "")
        auth_type = mapping.get("auth_type", "CLIENT_CREDENTIALS")
        props: dict[str, str] = {
            "site-url": site_url,
            "auth-type": auth_type,
        }
        if username:
            props["client-id"] = username
        if resolved_password:
            props["client-secret"] = resolved_password
        if source.database:
            props["tenant-id"] = resolve_secrets(source.database)
        if mapping.get("certificate_path"):
            props["certificate-path"] = mapping["certificate_path"]
        if mapping.get("certificate_password"):
            props["certificate-password"] = mapping["certificate_password"]
        props["case-insensitive-name-matching"] = "true"
        return props

    # Splunk connector (Apache Calcite, kenstott/calcite)
    if stype == "splunk":
        mapping = {
            k: resolve_secrets(v) if isinstance(v, str) else v for k, v in source.mapping.items()
        }
        splunk_port = port or 8089
        splunk_url = resolve_secrets(source.base_url or f"https://{host}:{splunk_port}")
        props = {"url": splunk_url}
        use_token = mapping.get("use_token", True)
        if use_token and resolved_password:
            props["token"] = resolved_password
        else:
            if username:
                props["user"] = username
            if resolved_password:
                props["password"] = resolved_password
        if source.database:
            props["app"] = source.database
        if mapping.get("datamodel_filter"):
            props["datamodel-filter"] = mapping["datamodel_filter"]
        if mapping.get("disable_ssl_validation"):
            props["disable-ssl-validation"] = "true"
        props["case-insensitive-name-matching"] = "true"
        return props

    # File connector (Apache Calcite, kenstott/calcite).
    # LINQ4J workaround: DuckDB engine resolves CSV as .parquet regardless of format
    # (kenstott/calcite#229). LINQ4J reads CSV/XLSX/JSON/etc. directly via Calcite.
    if stype == "files":
        if source.path is None:
            raise ValueError(
                f"Source {source.id!r}: 'path' (glob pattern) is required for files connector"
            )
        glob = resolve_secrets(source.path)
        return {
            "glob": glob,
            "recursive": "true",
            "schema-name": source.id.replace("-", "_"),
            "execution-engine": "LINQ4J",
            "case-insensitive-name-matching": "true",
        }

    # Cassandra connector
    if stype == "cassandra":
        return {
            "cassandra.contact-points": host,
            "cassandra.native-protocol-port": str(port),
            "cassandra.load-policy.dc-aware.local-dc": "datacenter1",
            "cassandra.consistency-level": "ONE",
        }

    # JDBC-based connectors (PG, MySQL, SQL Server, Oracle, etc.)
    props: dict[str, str] = {}
    jdbc_url = source.jdbc_url(host=host, port=port)
    if jdbc_url:
        props["connection-url"] = jdbc_url
        props["connection-user"] = username
        props["connection-password"] = resolved_password
        props["statistics.enabled"] = "false"
    return props


def create_catalog(
    conn: trino.dbapi.Connection, source: Source, resolved_password: str
) -> None:  # REQ-012, REQ-250, REQ-251
    """Create a Trino dynamic catalog for a registered source.

    Skips creation if the catalog already exists (e.g., from static catalog properties).
    """
    catalog_name = _to_catalog_name(source.id)

    # Skip if catalog already exists
    if catalog_exists(conn, catalog_name):
        return

    stype = source.type.value
    if stype in ("sqlite", "openapi"):
        connector = "postgresql"
    else:
        try:
            connector = _validate_identifier(source.connector)
        except KeyError:
            log.warning("No Trino connector for source type %r — skipping catalog creation", stype)
            return
    props = _build_catalog_properties(source, resolved_password)

    if not props:
        # Some source types (e.g., DuckDB) don't have Trino connectors
        return

    # REQ-250/251: write table-description files the connector reads before the
    # catalog is created (redis/elasticsearch/prometheus).
    from provisa.core.trino_catalog_files import is_mapping_dsl_source, write_table_definitions

    if is_mapping_dsl_source(source):
        write_table_definitions(source, resolved_password)

    props_sql = ", ".join(f"\"{k}\" = '{_escape_sql_string(v)}'" for k, v in props.items())
    sql = f"CREATE CATALOG IF NOT EXISTS {catalog_name} USING {connector} WITH ({props_sql})"

    try:
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchall()
    except Exception as e:
        log.warning(
            "Catalog creation failed for %s (connector=%s): %s. "
            "Source may need static catalog config or manual setup.",
            catalog_name,
            connector,
            e,
        )


def create_kafka_catalog(conn: trino.dbapi.Connection, kafka_source: dict) -> None:  # REQ-147
    """Register a ``kafka_sources[]`` entry as a Trino dynamic catalog.

    Kafka is the one source type whose connector props are not built by
    ``_build_catalog_properties`` (no host/JDBC url), so it never went through the
    dynamic ``CREATE CATALOG`` path — it only wrote a static ``.properties`` file,
    which a ``catalog.management=dynamic`` Trino that started before the app never
    loads. Register it dynamically here (idempotent) so kafka sources work
    regardless of Trino start order, like every other source.
    """
    from provisa.core.trino_catalog_files import kafka_catalog_props

    catalog_name = _to_catalog_name(kafka_source["id"])
    wait_until_ready(conn)  # a coordinator that just restarted races app boot
    if catalog_exists(conn, catalog_name):
        return
    props = kafka_catalog_props(kafka_source)
    if not props.get("kafka.nodes"):
        return
    props_sql = ", ".join(f"\"{k}\" = '{_escape_sql_string(v)}'" for k, v in props.items())
    sql = f"CREATE CATALOG IF NOT EXISTS {catalog_name} USING kafka WITH ({props_sql})"
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchall()
    except trino.exceptions.Error as e:
        log.warning("Kafka catalog creation failed for %s: %s", catalog_name, e)


def analyze_source_tables(  # REQ-636
    conn: trino.dbapi.Connection,
    source: "Source",
    tables: list,
) -> None:
    """Run ANALYZE on each registered table for a source.

    Errors are logged and swallowed — connector may not support ANALYZE,
    and registration must not fail because of it.
    """
    catalog_name = _to_catalog_name(source.id)
    cur = conn.cursor()
    for tbl in tables:
        if tbl.source_id != source.id:
            continue
        schema = _validate_identifier(tbl.schema_name)
        table = _validate_identifier(tbl.table_name)
        sql = f"ANALYZE {catalog_name}.{schema}.{table}"
        try:
            cur.execute(sql)
            cur.fetchall()
            log.info("ANALYZE %s.%s.%s ok", catalog_name, schema, table)
        except Exception as e:
            log.debug("ANALYZE %s.%s.%s skipped: %s", catalog_name, schema, table, e)


def drop_catalog(conn: trino.dbapi.Connection, source_id: str) -> None:  # REQ-012
    """Drop a Trino dynamic catalog."""
    catalog_name = _to_catalog_name(source_id)
    sql = f"DROP CATALOG IF EXISTS {catalog_name}"
    cur = conn.cursor()
    cur.execute(sql)
    cur.fetchall()


def catalog_exists(conn: trino.dbapi.Connection, source_id: str) -> bool:  # REQ-636
    """Check if a Trino catalog exists."""
    catalog_name = _to_catalog_name(source_id)
    cur = conn.cursor()
    cur.execute("SHOW CATALOGS")
    catalogs = [row[0] for row in cur.fetchall()]
    return catalog_name in catalogs
