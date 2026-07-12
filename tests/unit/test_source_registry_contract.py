# Copyright (c) 2026 Kenneth Stott
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
    SOURCE_TO_CONNECTOR,
    SOURCE_TO_DIALECT,
    _MYSQL_WIRE_TYPES,
    _PG_WIRE_TYPES,
)
from provisa.federation.connector import TRINO_CONNECTORS

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
# 4. The two connector registries never disagree.
# --------------------------------------------------------------------------- #
def test_connector_registries_do_not_conflict():
    """SOURCE_TO_CONNECTOR (source_registry) and TRINO_CONNECTORS (federation, the
    catalog source of truth) legitimately cover different sets, but where BOTH
    name a type they must name the SAME Trino connector — a conflict means one
    path catalogs the source differently than the other."""
    conflicts = {
        st: (SOURCE_TO_CONNECTOR[st], TRINO_CONNECTORS[st].trino_connector)
        for st in set(SOURCE_TO_CONNECTOR) & set(TRINO_CONNECTORS)
        if SOURCE_TO_CONNECTOR[st] != TRINO_CONNECTORS[st].trino_connector
    }
    assert not conflicts, f"connector-name drift between the two registries: {conflicts}"


# --------------------------------------------------------------------------- #
# 5. Every dialect type is also catalog-reachable (dialect implies a connector).
# --------------------------------------------------------------------------- #
def test_every_sql_dialect_type_is_catalog_reachable():
    """A type with a SQLGlot dialect is a SQL source Provisa can push down to; it
    must also have a Trino connector (be catalog-reachable) OR be a known
    native-driver-only type. A dialect with neither is an orphan."""
    # Types served by a NATIVE engine, not a Trino catalog: sqlite via the
    # SQLAlchemy fallback, duckdb via the DuckDB engine backend.
    driver_only = {"sqlite", "duckdb"}
    orphans = [
        st for st in SOURCE_TO_DIALECT if st not in TRINO_CONNECTORS and st not in driver_only
    ]
    assert not orphans, f"SQL types with a dialect but no connector/driver: {orphans}"
