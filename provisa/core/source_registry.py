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
    # REQ-2100: a code table's rows are read in DuckDB when its host answers the engine.
    "node": "duckdb",
    "python": "duckdb",
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


# REQ-1865: row fetch by keys -- each source type's declaration of how the full rows of a list of
# primary keys are read from it, which row_materialize needs (a row is fetched by its key when it is
# first read and when it goes stale). Each entry points at the type's fetch, ``module:attribute``; a
# type with no entry does not support it, and row_materialize on its tables is refused by name.
# ``tests/unit/test_row_fetch_by_keys.py`` holds this to the fetches ``load_keys`` is wired with.
_KEYED_LOADERS = "provisa.events.source_loader"
# A SQL source read through its own driver: ``SELECT ... WHERE (pk) IN (...)`` in its dialect.
_DIRECT_DRIVER_KEYED: frozenset[str] = frozenset(
    {
        "athena",
        "bigquery",
        "cockroachdb",
        "databricks",
        "duckdb",
        "exasol",
        "fabric",
        "greenplum",
        "hiveserver2",
        "mariadb",
        "mysql",
        "oracle",
        "postgresql",
        "redshift",
        "saphana",
        "singlestore",
        "snowflake",
        "sqlserver",
        "synapse",
        "tidb",
        "trino",
        "yugabytedb",
    }
)
# A type reached through its connector's Calcite pgwire server, read by key over that server.
_PGWIRE_BRIDGE_KEYED: frozenset[str] = frozenset(
    {"cloudops", "files", "govdata", "jira", "salesforce", "servicenow", "sharepoint", "splunk"}
)
# A lake the engine scans in place: the key read is the engine's own ``WHERE (pk) IN (...)``.
_ENGINE_TERMINAL_KEYED: frozenset[str] = frozenset({"delta_lake", "hive", "iceberg"})

ROW_FETCH_BY_KEYS: dict[str, str] = {
    "cassandra": f"{_KEYED_LOADERS}:make_cassandra_keyed_loader",
    "clickhouse": f"{_KEYED_LOADERS}:make_clickhouse_keyed_loader",
    "cosmosdb": "provisa.cosmosdb.loader:make_cosmosdb_keyed_loader",
    "druid": f"{_KEYED_LOADERS}:make_druid_keyed_loader",
    "elasticsearch": f"{_KEYED_LOADERS}:make_elasticsearch_keyed_loader",
    "hive_s3": f"{_KEYED_LOADERS}:make_hive_s3_keyed_loader",
    "kafka": f"{_KEYED_LOADERS}:make_kafka_keyed_loader",
    "mongodb": f"{_KEYED_LOADERS}:make_mongodb_keyed_loader",
    # Through its registered query_template, filtered on a property it projects.
    "neo4j": f"{_KEYED_LOADERS}:make_neo4j_keyed_loader",
    "pinot": f"{_KEYED_LOADERS}:make_pinot_keyed_loader",
    "redis": f"{_KEYED_LOADERS}:make_redis_keyed_loader",
    **{t: f"{_KEYED_LOADERS}:make_direct_keyed_loader" for t in _DIRECT_DRIVER_KEYED},
    **{
        t: "provisa.federation.pgwire_replica:make_pgwire_keyed_loader"
        for t in _PGWIRE_BRIDGE_KEYED
    },
    **{t: f"{_KEYED_LOADERS}:SourceRowLoader.load_keys" for t in _ENGINE_TERMINAL_KEYED},
    # Only for a table whose remote schema has a field returning its rows for a list of its key
    # (ROW_FETCH_BY_KEYS_PER_TABLE).
    "graphql_remote": f"{_KEYED_LOADERS}:make_graphql_remote_keyed_loader",
}

# Types whose fetch by keys is each table's own: the type declares one, and a table has it only
# where its source offers it for that table -- a remote GraphQL schema with a field that returns
# the table's rows for a list of its key (graphql_remote.by_keys).
ROW_FETCH_BY_KEYS_PER_TABLE: frozenset[str] = frozenset({"graphql_remote"})


def supports_row_fetch_by_keys(source_type: str | None) -> bool:
    """Whether ``source_type`` declares a row fetch by primary keys (``ROW_FETCH_BY_KEYS``); for
    a type in ``ROW_FETCH_BY_KEYS_PER_TABLE``, whether some of its tables may have one."""
    return source_type in ROW_FETCH_BY_KEYS


# Types refused row_materialize for a reason of their own, said before the missing fetch is.
_ROW_MATERIALIZE_REFUSED: dict[str, str] = {
    **{
        t: "an API source is filtered by its own parameters, not replicated row by row"
        for t in ("openapi", "grpc_remote")
    },
    "govdata": "a govdata source is read live on every request and is never replicated",
}


def no_key_fetch_refusal(table_name: str) -> str:
    """The refusal of row_materialize for a table whose own fetch by keys its remote lacks."""
    return (
        f"{table_name}: row_materialize needs row fetch by primary keys, and its remote "
        "schema has no field that returns its rows for a list of its primary key"
    )


def row_materialize_refusal(
    table_name: str, source_type: str | None, *, table_fetches_by_keys: bool | None = None
) -> str | None:
    """Why ``table_name``, of a ``source_type`` source, may not set row_materialize (REQ-1865), or
    None when it may: a type refused for a reason of its own says it; any other type without a
    row fetch by keys is refused for that. For a type in ``ROW_FETCH_BY_KEYS_PER_TABLE`` the
    caller says whether this table has one (``table_fetches_by_keys``)."""
    if source_type in ROW_FETCH_BY_KEYS_PER_TABLE:
        if table_fetches_by_keys is None:
            raise TypeError(
                f"a {source_type} table's fetch by keys is its own: say whether it has one"
            )
        return None if table_fetches_by_keys else no_key_fetch_refusal(table_name)
    reason = _ROW_MATERIALIZE_REFUSED.get(source_type or "")
    if reason is not None:
        return (
            f"table {table_name!r}: row_materialize=True is refused for source type "
            f"{source_type!r}: {reason}"
        )
    if not supports_row_fetch_by_keys(source_type):
        return (
            f"{table_name}: row_materialize needs row fetch by primary keys, which "
            f"{source_type} sources don't support"
        )
    return None


# REQ-2000: the ``qdrant`` dialect named above is registered with SQLGlot wherever this mapping is
# read, so a Qdrant source's statements transpile and its writes find their dialect.
import provisa.qdrant.dialect  # noqa: E402, F401
