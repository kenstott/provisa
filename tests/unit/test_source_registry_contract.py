# Copyright (c) 2026 Kenneth Stott
# Canary: f6ae55d0-e895-4703-a4dd-d82bfd22401f
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source registry contract (REQ-229, REQ-842, REQ-950).

The source half of the combinatorial matrix: ~40 source types route through a few
registries — the Trino connector table (catalog.py's source of truth), the
per-type JDBC-url builder, and the SQLGlot dialect map. A type added to the enum
but wired into none of these is a connector that silently produces no catalog / a
broken direct route. Per-file line coverage cannot see that; this contract does,
by asserting the registries stay internally valid and consistent across EVERY
type — the drift that bites when the next source is added.

Deliberately NOT asserted: connection-url generation for warehouse/lake JDBC
types (snowflake/bigquery/clickhouse/redshift/databricks/hive/druid/exasol/
iceberg/delta_lake). Source.jdbc_url intentionally returns "" for them (they carry
no native async driver in V1 and build catalog properties by another path), so
requiring a url there would be a false failure, not a finding.
"""

from __future__ import annotations

import pytest
import sqlglot

from provisa.core.models import Source, SourceType
from provisa.core.source_registry import (
    SOURCE_TO_DIALECT,
    _MYSQL_WIRE_TYPES,
    _PG_WIRE_TYPES,
)
from provisa.federation.trino_connectors import TRINO_CONNECTORS, trino_connector_name

# Relational types whose rows a native driver reads directly (bypassing Trino):
# Source.jdbc_url is the authoritative url builder for exactly these (the pg/mysql
# wire families plus the three that carry their own jdbc_url prefix). singlestore
# et al. are Trino-connector-only and outside jdbc_url's domain.
_NATIVE_JDBC_TYPES = sorted(_PG_WIRE_TYPES | _MYSQL_WIRE_TYPES | {"sqlserver", "oracle", "mariadb"})


# --------------------------------------------------------------------------- #
# 1. Every Trino connector declares a valid USING name.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("source_type", sorted(TRINO_CONNECTORS), ids=lambda s: s)
def test_every_trino_connector_declares_using_name(source_type):
    """`CREATE CATALOG ... USING <trino_connector>` needs a non-empty connector
    name and the registry must be keyed by the connector's own source_type — a
    blank or mis-keyed entry produces no usable catalog for that source."""
    connector = TRINO_CONNECTORS[source_type]
    assert connector.source_type == source_type, "connector mis-keyed in the registry"
    assert connector.trino_connector, f"{source_type}: empty Trino USING name"


# --------------------------------------------------------------------------- #
# 2. Every native-relational type builds a well-formed JDBC url.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("source_type", _NATIVE_JDBC_TYPES, ids=lambda s: s)
def test_native_relational_type_builds_jdbc_url(source_type):
    """The per-type Source.jdbc_url template must yield a real url carrying host,
    port and database — a broken template silently disables that source's direct
    route."""
    src = Source(
        id=f"contract-{source_type}",
        type=SourceType(source_type),
        host="db.internal",
        port=1234,
        database="warehouse",
        username="reader",
    )
    url = src.jdbc_url()
    assert url.startswith("jdbc:"), f"{source_type}: not a jdbc url: {url!r}"
    assert "db.internal" in url and "1234" in url and "warehouse" in url, (
        f"{source_type}: url missing host/port/database: {url!r}"
    )


# --------------------------------------------------------------------------- #
# 3. Every declared SQL dialect is one SQLGlot actually knows.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("dialect", sorted(set(SOURCE_TO_DIALECT.values())), ids=lambda d: d)
def test_every_dialect_resolves_in_sqlglot(dialect):
    """Direct-route transpilation passes this dialect string to SQLGlot; a typo or
    a dialect SQLGlot dropped breaks every single-source query for that type."""
    sqlglot.Dialect.get_or_raise(dialect)


# --------------------------------------------------------------------------- #
# 4. The Trino connector.name has ONE source of truth (REQ-947).
# --------------------------------------------------------------------------- #
def test_connector_name_is_derived_from_the_trino_registry():
    """The parallel ``SOURCE_TO_CONNECTOR`` name map is retired (REQ-947): the Trino
    ``connector.name`` comes solely from the Trino connector objects, read via
    ``trino_connector_name``. Every Trino-reachable type resolves to its object's
    ``trino_connector``; a type with no Trino connector resolves to None — so drift
    between two maps is structurally impossible, not merely asserted-absent."""
    for st, connector in TRINO_CONNECTORS.items():
        assert trino_connector_name(st) == connector.trino_connector
    assert trino_connector_name("duckdb") is None  # no Trino connector → no USING name


# --------------------------------------------------------------------------- #
# 5. Every dialect type is also catalog-reachable (dialect implies a connector).
# --------------------------------------------------------------------------- #
def test_every_sql_dialect_type_is_catalog_reachable():
    """A type with a SQLGlot dialect is a SQL source Provisa can push down to; it
    must also have a Trino connector (be catalog-reachable) OR a Provisa-native
    driver (DIRECT-routable). A dialect with neither is an orphan."""
    from provisa.executor.drivers.registry import has_native_driver

    # A type reached by a NATIVE Provisa driver rather than a Trino catalog: sqlite/duckdb, and the
    # warehouse direct-source drivers (bigquery/fabric/synapse/…). Derived from the driver registry so
    # the invariant tracks it, never a hand-maintained list.
    orphans = [
        st for st in SOURCE_TO_DIALECT if st not in TRINO_CONNECTORS and not has_native_driver(st)
    ]
    assert not orphans, f"SQL types with a dialect but no connector/driver: {orphans}"
