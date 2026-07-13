# Copyright (c) 2026 Kenneth Stott
# Canary: bd0b8d35-bfcc-4465-bb89-285979f05154
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source-type registry: maps a source type to its SQLGlot dialect and wire-protocol family.
Keyed by the string source-type value so it has no dependency on the SourceType enum.

The Trino catalog ``connector.name`` is NOT held here (REQ-947): it lives on the Trino connector
objects (``TRINO_CONNECTORS[st].trino_connector`` in ``provisa.federation.connector``), read via
``trino_connector_name(source_type)``. The former parallel ``SOURCE_TO_CONNECTOR`` map is retired —
it was a hand-maintained duplicate of those objects' names. REACH, likewise, is the federation
engine's OWN connector registry (``reachable_source_types``), never a parallel map here."""

# Requirements: REQ-229, REQ-250, REQ-251, REQ-372, REQ-950, REQ-947

# Wire-protocol families (REQ-950): a wire-compatible RDB reuses its base wire's JDBC driver,
# native async driver, and SQLGlot dialect — it only needs registry entries, no new code.
_PG_WIRE_TYPES: frozenset[str] = frozenset({"postgresql", "cockroachdb", "yugabytedb", "greenplum"})
_MYSQL_WIRE_TYPES: frozenset[str] = frozenset({"mysql", "tidb"})


# Map source types to SQLGlot dialect names (enables direct-route single-source queries)
SOURCE_TO_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "mysql": "mysql",
    "mariadb": "mysql",
    "singlestore": "singlestore",
    # Wire-compatible RDBs — same SQLGlot dialect as their base wire (REQ-950)
    "cockroachdb": "postgres",
    "yugabytedb": "postgres",
    "greenplum": "postgres",
    "tidb": "mysql",
    "sqlite": "sqlite",  # served by the SQLAlchemy fallback driver (no native async driver)
    "sqlserver": "tsql",
    "oracle": "oracle",
    "duckdb": "duckdb",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "redshift": "redshift",
    "databricks": "databricks",
    "fabric": "tsql",  # Microsoft Fabric Warehouse — T-SQL over TDS (REQ-986)
    "synapse": "tsql",  # Azure Synapse — T-SQL over TDS
    "hive": "hive",
    "druid": "druid",
    "exasol": "exasol",
}

# Source types that are CONNECTOR_ONLY — no direct driver, no SQLGlot dialect (REQ-229)
LAKE_ONLY_SOURCES: set[str] = {"iceberg", "hive_s3", "delta_lake"}

# Source types that support time-travel vithe engine FOR TIMESTAMP/VERSION AS OF (REQ-372)
TIME_TRAVEL_SOURCES: set[str] = {"iceberg", "delta_lake"}
