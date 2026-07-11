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
        except trino.exceptions.TrinoConnectionError:
            # A coordinator restarting/under load drops the socket ("connection
            # reset/refused") — the same "not ready yet" signal as SERVER_STARTING_UP,
            # not a permanent failure. Keep waiting until the deadline; a genuinely
            # unreachable engine still surfaces when the deadline passes.
            if time.monotonic() >= deadline:
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


def _build_catalog_properties(source: Source, resolved_password: str) -> dict[str, str]:
    # REQ-250, REQ-251, REQ-842. Trino connector catalog properties for a source,
    # derived from the source type's Trino Connector class (single source of truth).
    # A type with no Trino connector returns {} (not reachable by Trino). Exercised
    # by the splunk/sharepoint/file-lake connector unit tests to assert the props a
    # source produces; ``resolved_password`` is unused (the connector resolves
    # secrets itself) but kept for call-site compatibility.
    del resolved_password
    from provisa.federation.connector import TRINO_CONNECTORS

    connector = TRINO_CONNECTORS.get(source.type.value)
    return connector.details(source) if connector is not None else {}


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

    # REQ-842: the Trino Connector class is the source of truth for reach + catalog. A type with no
    # Trino connector is not reachable by Trino — no catalog (never a parallel type→name map).
    from provisa.federation.connector import TRINO_CONNECTORS

    stype = source.type.value
    trino_connector = TRINO_CONNECTORS.get(stype)
    if trino_connector is None:
        log.warning("No Trino connector for source type %r — skipping catalog creation", stype)
        return
    connector = _validate_identifier(trino_connector.trino_connector)
    props = _build_catalog_properties(source, resolved_password)

    if not props:
        # A reachable type with no Source-row props (e.g. kafka, registered via create_kafka_catalog).
        return

    # REQ-250/251: write table-description files the connector reads before the
    # catalog is created (redis/elasticsearch/prometheus).
    from provisa.core.trino_catalog_files import is_mapping_dsl_source, write_table_definitions

    if is_mapping_dsl_source(source):
        write_table_definitions(source, resolved_password)

    props_sql = ", ".join(f"\"{k}\" = '{_escape_sql_string(v)}'" for k, v in props.items())
    sql = f"CREATE CATALOG IF NOT EXISTS {catalog_name} USING {connector} WITH ({props_sql})"

    # Failed source registration must not continue silently — propagate.
    cur = conn.cursor()
    cur.execute(sql)
    cur.fetchall()


def create_kafka_catalog(conn: trino.dbapi.Connection, kafka_source: dict) -> None:  # REQ-147
    """Register a ``kafka_sources[]`` entry as a Trino dynamic catalog.

    Kafka is the one source type whose connector props are not built from a
    Source row (no host/JDBC url), so it never went through ``create_catalog``'s
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
        except trino.exceptions.Error as e:
            # Best-effort: the connector may not support ANALYZE, or the table may
            # be transiently unavailable. Any Trino-side error is non-fatal here.
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
