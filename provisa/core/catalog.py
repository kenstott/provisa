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

import logging
import re
import signal

import trino

log = logging.getLogger(__name__)

from provisa.core.models import Source

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


def _build_catalog_properties(source: Source, resolved_password: str) -> dict[str, str]:
    """Build Trino connector properties from a source definition."""
    from provisa.core.secrets import resolve_secrets

    stype = source.type.value
    host = resolve_secrets(source.host or "")
    port = source.port
    username = resolve_secrets(source.username or "")

    # MongoDB connector
    if stype == "mongodb":
        url = f"mongodb://{host}:{port}/"
        if username:
            url = f"mongodb://{username}:{resolved_password}@{host}:{port}/"
        return {
            "mongodb.connection-url": url,
            "mongodb.schema-collection": "_schema",
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
    return props


def create_catalog(conn: trino.dbapi.Connection, source: Source, resolved_password: str) -> None:
    """Create a Trino dynamic catalog for a registered source.

    Skips creation if the catalog already exists (e.g., from static catalog properties).
    """
    catalog_name = _to_catalog_name(source.id)

    # Skip if catalog already exists
    if catalog_exists(conn, catalog_name):
        return

    connector = _validate_identifier(source.connector)
    props = _build_catalog_properties(source, resolved_password)

    if not props:
        # Some source types (e.g., DuckDB) don't have Trino connectors
        return

    props_sql = ", ".join(
        f'"{k}" = \'{_escape_sql_string(v)}\'' for k, v in props.items()
    )
    sql = f"CREATE CATALOG IF NOT EXISTS {catalog_name} USING {connector} WITH ({props_sql})"

    try:
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchall()
    except Exception as e:
        log.warning(
            "Catalog creation failed for %s (connector=%s): %s. "
            "Source may need static catalog config or manual setup.",
            catalog_name, connector, e,
        )


def analyze_source_tables(
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


def drop_catalog(conn: trino.dbapi.Connection, source_id: str) -> None:
    """Drop a Trino dynamic catalog."""
    catalog_name = _to_catalog_name(source_id)
    sql = f"DROP CATALOG IF EXISTS {catalog_name}"
    cur = conn.cursor()
    cur.execute(sql)
    cur.fetchall()


def catalog_exists(conn: trino.dbapi.Connection, source_id: str) -> bool:
    """Check if a Trino catalog exists."""
    catalog_name = _to_catalog_name(source_id)
    cur = conn.cursor()
    cur.execute("SHOW CATALOGS")
    catalogs = [row[0] for row in cur.fetchall()]
    return catalog_name in catalogs
