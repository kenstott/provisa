# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3edad7-ae23-4bc9-a95a-9c0fea6dd528
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
    "athena": "athena",  # REQ-1981
    # REQ-2000: PostgreSQL text, read by the Qdrant driver and lowered to Qdrant's API
    # (provisa/qdrant/dialect.py registers the name with SQLGlot).
    "qdrant": "qdrant",
    "databricks": "databricks",
    "fabric": "tsql",  # Microsoft Fabric Warehouse — T-SQL over TDS (REQ-986)
    "synapse": "tsql",  # Azure Synapse — T-SQL over TDS
    "hive": "hive",
    # REQ-1731 gap: hiveserver2 (executor/drivers/hive.py's HiveDriver, direct over impyla) was
    # never added here even though "hive" (the Trino-scanned lake-STORAGE type, HS2 never
    # involved — see hive.py's own module doc) already was. Missing from this map, model.dialect
    # (core/models.py) returned None for a hiveserver2 source, and schema_mutation.py's
    # `model.dialect or ""` (REQ-1757) then stored dialect="", so decide_route's own
    # `decision.dialect or "postgres"` fallback (pgwire/_pipeline.py) silently compiled ANSI
    # double-quoted SQL (``"wh"."widgets"``) for HS2, which only accepts backtick-quoted
    # identifiers — verified live: HS2 raised "ParseException ... cannot recognize input near
    # '\"wh\"'" on exactly that SQL. Hive's own sqlglot dialect (shared with plain "hive" above)
    # emits backticks and is what HiveDriver's SQL actually needs to speak.
    "hiveserver2": "hive",
    "druid": "druid",
    # REQ-1946: written through their own pgwire server, which takes PostgreSQL. Neither has a
    # direct driver registered for its type, so no read is routed to the server by this entry.
    "sharepoint": "postgres",
    "salesforce": "postgres",
    # REQ-1994: a mail source's calendar and task writes are made through its API by a driver
    # that reads the statement as PostgreSQL. No direct driver is registered for either type,
    # so no read is routed to that driver by this entry.
    "google_workspace": "postgres",
    "microsoft_365": "postgres",
    "exasol": "exasol",
}

# Source types that are CONNECTOR_ONLY — no direct driver, no SQLGlot dialect (REQ-229)
LAKE_ONLY_SOURCES: set[str] = {"iceberg", "delta_lake", "hive_s3"}

# Source types that support time-travel vithe engine FOR TIMESTAMP/VERSION AS OF (REQ-372)
TIME_TRAVEL_SOURCES: set[str] = {"iceberg", "delta_lake"}

# REQ-2000: source types that are live-only -- a property of the type, never a per-source option.
# The maintainer: "its reserved for scenarios where the engine cannot replicate the live db query
# feature." A type has it only when its defining query capability cannot be reproduced by the
# engine over a replica (Qdrant: its own vector index and query API). A live-only source's tables
# have no replica and no keyed fetch: a statement on the source runs on it, and its part of an
# engine-answered statement is fetched live for that statement.
LIVE_ONLY_TYPES: frozenset[str] = frozenset({"qdrant"})


def is_live_only(source_type: str | None) -> bool:
    """Whether ``source_type`` is live-only (``LIVE_ONLY_TYPES``)."""
    return source_type in LIVE_ONLY_TYPES


# REQ-2000: the ``qdrant`` dialect named above is registered with SQLGlot wherever this mapping is
# read, so a Qdrant source's statements transpile and its writes find their dialect.
import provisa.qdrant.dialect  # noqa: E402, F401
