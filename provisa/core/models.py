# Copyright (c) 2026 Kenneth Stott
# Canary: bd0b8d35-bfcc-4465-bb89-285979f05154
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pydantic models for Provisa configuration."""

# Requirements: REQ-003, REQ-005, REQ-019, REQ-020, REQ-041, REQ-042, REQ-052, REQ-053, REQ-119,
# REQ-120, REQ-124, REQ-125, REQ-176, REQ-194, REQ-204, REQ-229, REQ-238, REQ-240, REQ-247,
# REQ-250, REQ-251, REQ-281, REQ-369, REQ-370, REQ-393, REQ-399, REQ-400, REQ-415, REQ-419, REQ-421

import re
from enum import Enum

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from provisa.core.source_registry import (
    _MYSQL_WIRE_TYPES,
    _PG_WIRE_TYPES,
    SOURCE_TO_DIALECT,
)


class SourceType(str, Enum):
    # RDBMS
    postgresql = "postgresql"
    mysql = "mysql"
    singlestore = "singlestore"
    mariadb = "mariadb"
    sqlserver = "sqlserver"
    oracle = "oracle"
    firebird = "firebird"  # Firebird 3/4/5 — DuckDB firebird community extension (REQ-899)
    duckdb = "duckdb"
    # Postgres-wire-compatible RDBs — reuse the postgres driver/dialect/Trino connector (REQ-950)
    cockroachdb = "cockroachdb"
    yugabytedb = "yugabytedb"
    greenplum = "greenplum"
    # MySQL-wire-compatible RDBs — reuse the mysql driver/dialect/Trino connector (REQ-950)
    tidb = "tidb"
    # Cloud DW
    snowflake = "snowflake"
    bigquery = "bigquery"
    databricks = "databricks"
    fabric = "fabric"  # Microsoft Fabric Warehouse (T-SQL over TDS, Azure AD auth)
    synapse = "synapse"  # Azure Synapse SQL (T-SQL over TDS, Azure AD auth)
    redshift = "redshift"
    # Analytics / OLAP
    clickhouse = "clickhouse"
    elasticsearch = "elasticsearch"
    pinot = "pinot"
    druid = "druid"
    exasol = "exasol"
    # Data Lake
    delta_lake = "delta_lake"
    iceberg = "iceberg"
    hive = "hive"
    hive_s3 = "hive_s3"  # S3-backed Hive lake table (connector-only; no direct driver)
    # NoSQL
    mongodb = "mongodb"
    cassandra = "cassandra"
    redis = "redis"
    kudu = "kudu"
    accumulo = "accumulo"
    # Streaming
    kafka = "kafka"
    websocket = "websocket"  # External WebSocket feed — connect, subscribe, receive events
    rss = "rss"  # RSS 2.0 / Atom feed — poll, watermark by pubDate/updated
    # Graph / Semantic
    neo4j = "neo4j"
    sparql = "sparql"
    # File-based
    sqlite = "sqlite"
    csv = "csv"
    parquet = "parquet"
    # Other
    google_sheets = "google_sheets"
    prometheus = "prometheus"
    graphql_remote = "graphql_remote"
    openapi = "openapi"
    grpc_remote = "grpc_remote"
    airport = "airport"  # Arrow Flight server — DuckDB airport community extension (REQ-899)
    # Push receiver — external services POST JSON events (Phase AS)
    ingest = "ingest"
    # U.S. government open data via Apache Calcite / GovData JDBC adapter
    govdata = "govdata"
    # Enterprise SaaS via Apache Calcite connectors
    sharepoint = "sharepoint"
    splunk = "splunk"
    # File crawler — directory of CSV/Parquet/XLSX/JSON surfaced as tables
    files = "files"


class Cardinality(str, Enum):
    many_to_one = "many-to-one"
    one_to_many = "one-to-many"


_SAFE_ID_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")


class SourceCdcConfig(BaseModel):  # REQ-824
    """Source-level CDC transport config (Debezium/Kafka), entered once per source.

    Holds the delta-transport connection that is common to every table captured
    from this source, so per-table live config never repeats it. Per-table poll
    settings (watermark_column, poll_interval) stay on LiveDeliveryConfig.
    """

    bootstrap_servers: str  # Kafka bootstrap servers for the Debezium/Kafka delta stream
    topic_prefix: str  # Debezium connector topic prefix; topics derived {prefix}.{schema}.{table}
    schema_registry_url: str | None = None  # Confluent Schema Registry URL (Avro); None = JSON
    # REQ-931: consumer group is a RECEIVER-side setting (Provisa's consumer identity), not sender-
    # dictated like the transport fields above. None = inherit the Provisa-level default
    # (ProvisaConfig.cdc_consumer_group_id); set only for deliberate per-source offset isolation.
    consumer_group_id: str | None = None


class Source(BaseModel):  # REQ-012, REQ-052, REQ-053, REQ-204, REQ-229, REQ-250, REQ-251, REQ-281
    model_config = ConfigDict(populate_by_name=True)

    id: str

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not _SAFE_ID_PATTERN.match(v):
            raise ValueError(f"Source id must be alphanumeric with hyphens/underscores, got: {v!r}")
        return v

    type: SourceType
    host: str = ""
    port: int = 0
    database: str = ""
    username: str = ""
    password: str = ""  # Secret reference e.g. ${env:PG_PASSWORD}
    path: str | None = None  # File path or URL for file-based sources (csv, parquet, sqlite)
    base_url: str | None = None  # Base URL for OpenAPI sources (e.g. https://api.example.com/v1)
    pool_min: int = Field(default=1, alias="pool_min")
    pool_max: int = Field(default=5, alias="pool_max")
    # REQ-053: PgBouncer is opt-in per PostgreSQL source. Default is direct asyncpg pooling
    # (a warm per-source pool, REQ-052); set use_pgbouncer=true to route through PgBouncer on
    # pgbouncer_port (statement_cache_size is then forced to 0). Not defaulted on because it
    # requires a running PgBouncer for every PG source.
    use_pgbouncer: bool = Field(default=False, alias="use_pgbouncer")
    pgbouncer_port: int = Field(default=6432, alias="pgbouncer_port")
    cache_enabled: bool = True
    cache_ttl: int | None = None  # overrides global default; None = inherit
    # Force MATERIALIZED federation for this source's tables even when it could be reached live —
    # the manual counterpart to cost-based promotion, for when the connector is a poor fit (REQ-826).
    prefer_materialized: bool = False
    # REQ-929: source-level default change signal — how Provisa learns rows changed. Orthogonal to a
    # table's watermark_column (which gates the poll subscription path + append landing, REQ-926/927).
    # Pull: ttl | probe | ttl_probe. Push: native | debezium | kafka. Tables inherit unless overriding.
    change_signal: str = "ttl"
    # REQ-860: opt-in source-level freshness gate. When true, a query reading a table from this
    # source is gated by a freshness decision (built from change_signal + cache_ttl via the
    # freshness module, REQ-856/858) before execution; a stale/failed verdict triggers the caller's
    # refresh/produce path. Default off — gating requires a declared change_signal + cache_ttl.
    freshness_gate: bool = False
    # REQ-861: optional producer command (argv list) for a FILE-based source (csv/parquet/sqlite/
    # files). When the source's freshness gate reports stale, this command runs (subprocess, no
    # shell) BEFORE the file is read — refreshing the file IN PLACE without changing ``path`` or
    # defining an MV. None = no producer; the file is read as-is. Non-zero exit fails loud.
    producer_command: list[str] | None = None
    cache_catalog: str | None = (
        None  # the engine catalog for API cache; None = source's own catalog
    )
    cache_schema: str = "api_cache"  # schema within that catalog
    gql_naming_convention: str | None = None  # overrides global; None = inherit
    # REQ-281: Provisa-branded federation hints (join/reorder/broadcast_size, the @provisa
    # vocabulary), translated to the engine session props at query time. Raw the engine keys still pass
    # through for backward compatibility but are deprecated.
    federation_hints: dict[str, str] = Field(default_factory=dict)
    # REQ-251: type-specific mapping DSL for NoSQL/non-relational sources
    # (redis/elasticsearch/prometheus). Holds {"tables": [...]} plus connector options.
    mapping: dict = Field(default_factory=dict)
    approval_hook: bool = False  # REQ-204/247: scope the ABAC approval hook to this source
    allowed_domains: list[str] = Field(
        default_factory=list
    )  # restrict this source to specific domains; empty = unrestricted
    description: str = ""
    # REQ-824: source-level CDC transport (Debezium/Kafka), entered once per source.
    # Only meaningful for CDC-capable RDBMS sources; None for everything else.
    cdc: SourceCdcConfig | None = None

    @property
    def connector(self) -> str | None:
        """The Trino catalog ``connector.name`` (the ``USING`` label) for this source type, or None if
        Trino has no connector for it. Derived from the Trino connector registry — no parallel map
        (REQ-947). Deferred import: ``core`` reaches ``federation`` lazily, as ``core.catalog`` does."""
        from provisa.federation.trino_connectors import trino_connector_name

        return trino_connector_name(self.type.value)

    @property
    def dialect(self) -> str | None:
        """SQLGlot dialect name, or None for NoSQL sources."""
        return SOURCE_TO_DIALECT.get(self.type.value)

    @property
    def catalog_name(self) -> str:
        """the engine catalog name — sanitized source id."""
        return self.id.replace("-", "_")

    def jdbc_url(self, host: str | None = None, port: int | None = None) -> str:
        st = self.type.value
        h = host or self.host
        po = port or self.port
        # Postgres-wire RDBs (postgres + cockroach/yugabyte/greenplum) all use the pgjdbc driver;
        # autosave=conservative prevents pgjdbc crashing on the pg_type_typname_nsp_index
        # duplicate-key race when two concurrent queries register the same anonymous composite type.
        if st in _PG_WIRE_TYPES:
            return f"jdbc:postgresql://{h}:{po}/{self.database}?autosave=conservative"
        # MySQL-wire RDBs (mysql + tidb) use the MySQL JDBC driver; mariadb keeps its own.
        if st in _MYSQL_WIRE_TYPES:
            return f"jdbc:mysql://{h}:{po}/{self.database}"
        # Druid is queried through its Avatica endpoint on the broker (Trino's druid connector wraps
        # the Avatica JDBC driver) — not a standard jdbc://host:port/db shape and no database segment.
        if st == "druid":
            return f"jdbc:avatica:remote:url=http://{h}:{po}/druid/v2/sql/avatica/"
        prefix = {
            "sqlserver": "jdbc:sqlserver",
            "oracle": "jdbc:oracle:thin",
            "mariadb": "jdbc:mariadb",
            "redshift": "jdbc:redshift",
            "exasol": "jdbc:exa",
        }
        p = prefix.get(st)
        if p is None:
            return ""
        if self.type == SourceType.sqlserver:
            return f"{p}://{h}:{po};databaseName={self.database}"
        if self.type == SourceType.oracle:
            return f"{p}:@{h}:{po}/{self.database}"
        if self.type == SourceType.exasol:
            # Exasol JDBC is colon-delimited with no //-authority and no db in the URL
            # (schema is selected per-query): jdbc:exa:<host>:<port>.
            return f"{p}:{h}:{po}"
        return f"{p}://{h}:{po}/{self.database}"


class Domain(BaseModel):  # REQ-471
    id: str
    description: str = ""
    graphql_alias: str | None = None
    ddl_catalog: str | None = None  # the engine catalog for DDL; defaults to system Iceberg catalog
    ddl_schema: str | None = None  # schema within ddl_catalog; defaults to domain id


class NamingRule(BaseModel):
    pattern: str
    replace: str


class NamingConfig(BaseModel):  # REQ-154, REQ-155, REQ-194, REQ-195, REQ-415, REQ-416
    convention: str = "apollo_graphql"
    sql_convention: str = "snake"
    rules: list[NamingRule] = Field(default_factory=list)
    relay_pagination: bool = False  # opt-in: generate _connection fields + Edge/PageInfo types
    # REQ-415: FK-derived relationship names follow Hasura V2 conventions —
    # singular object (many-to-one), plural array (one-to-many) via inflection.
    hasura_v2_relationship_style: bool = False
    domain_prefix: bool = False  # prepend domain initials to GraphQL names (namespaced mode)
    # Tri-state domain feature. None = inert (default); False = single stored default_domain,
    # domain hidden from names/UI/access; True = namespaced, domain_id required.
    use_domains: bool | None = None
    default_domain: str = (
        "default"  # stored domain_id when use_domains is False (must be non-empty)
    )

    @field_validator("convention", "sql_convention")
    @classmethod
    def _validate_convention(cls, v: str) -> str:
        from provisa.compiler.naming import VALID_CONVENTIONS

        if v not in VALID_CONVENTIONS:
            raise ValueError(
                f"Invalid naming convention {v!r}. Valid options: {sorted(VALID_CONVENTIONS)}"
            )
        return v

    @model_validator(mode="after")
    def _validate_default_domain(self) -> "NamingConfig":
        # Non-empty/identifier rule only applies when single-domain mode is engaged.
        if self.use_domains is False:
            if not self.default_domain:
                raise ValueError("naming.default_domain must be non-empty when use_domains=false")
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_-]*$", self.default_domain):
                raise ValueError(
                    f"naming.default_domain {self.default_domain!r} is not a valid identifier"
                )
        return self


class ObjectField(BaseModel):
    name: str
    type: str = "string"  # provisa scalar type: string, integer, number, boolean, object
    alias: str | None = None
    description: str | None = None
    visible_to: list[str] = []  # empty = inherit from parent column
    fields: list["ObjectField"] = []  # nested sub-fields when type == "object"


class Column(
    BaseModel
):  # REQ-038, REQ-039, REQ-119, REQ-151, REQ-155, REQ-156, REQ-393, REQ-399, REQ-421
    name: str
    data_type: str | None = (
        None  # source column type (e.g. "varchar", "integer"); lets startup skip the engine introspection
    )
    visible_to: list[str]
    writable_by: list[str] = []  # roles allowed to mutate this column
    unmasked_to: list[str] = []  # roles that see unmasked data
    mask_type: str | None = None  # regex, constant, truncate
    mask_pattern: str | None = None  # regex pattern
    mask_replace: str | None = None  # regex replacement
    mask_value: str | None = None  # constant value
    mask_precision: str | None = None  # truncate precision (year, month, day, etc.)
    alias: str | None = None  # GraphQL field name override
    description: str | None = None  # GraphQL field description
    path: str | None = None  # JSON extraction path (e.g. "payload.order_id")
    native_filter_type: str | None = None  # "path_param" | "query_param" for OpenAPI sources
    is_primary_key: bool = False  # user-designated PK (informational, not enforced)
    is_foreign_key: bool = False  # derived from relationships (source_column side)
    is_alternate_key: bool = (
        False  # derived from relationships (target_column when PK already exists)
    )
    object_fields: list[ObjectField] = []  # sub-fields for object/jsonb columns
    scope: str = "domain"  # "domain" | "public" | "restricted"
    # REQ-421: declare a column as an embedding vector. embedding_model references a
    # registered vector model (REQ-419); embedding_source_column is the text column it
    # is generated from (for the generation tier). dimensions come from the model.
    embedding: bool = False
    embedding_model: str | None = None
    embedding_source_column: str | None = None
    # REQ-689/REQ-691/REQ-692: column holds a client-decryptable envelope blob. The backend
    # passes the ciphertext through undecrypted; clients configured with a KMS key decrypt it.
    # Surfaced to clients as the GraphQL @encrypted directive and the SQL/Arrow encrypted-column
    # metadata flag.
    encrypted: bool = False


class ColumnPreset(BaseModel):
    """Auto-set a column value on insert/update from session or built-in source."""

    column: str
    source: str  # "header", "now", "literal"
    name: str | None = None  # header name (for source=header)
    value: str | None = None  # literal value (for source=literal)
    data_type: str | None = None  # the engine data type of the column (for coercion)


class UniqueConstraint(BaseModel):  # REQ-1093
    """A source-declared UNIQUE constraint: an ordered column set (single or composite).

    Recorded only from a declared source constraint, never inferred from data. `columns`
    preserves ordinal_position — order is significant for index-prefix usability and DDL.
    """

    name: str
    columns: list[str]


class KafkaSinkAttachment(BaseModel):  # REQ-565
    """Per-table opt-in Kafka sink config (REQ-176–180)."""

    topic: str
    key_column: str | None = None
    triggers: list[str] = Field(default_factory=lambda: ["change_event"])


class LiveOutputConfig(BaseModel):  # REQ-565
    """Single output destination for a live query (SSE fanout or Kafka sink)."""

    type: str  # "sse" | "kafka"
    topic: str | None = None  # Kafka topic (required when type="kafka")
    key_column: str | None = None  # Kafka message key column
    bootstrap_servers: str | None = None  # Kafka bootstrap (required when type="kafka")


class LiveKafkaParams(BaseModel):  # REQ-813
    """Per-table params for strategy=kafka (arbitrary Kafka delta topic).

    Transport (bootstrap_servers) is inherited from the source's cdc block
    (REQ-824), never repeated here — only the topic shape is per-table.
    """

    topic: str  # Kafka topic carrying this table's deltas
    format: str = "json"  # "json" | "avro"
    key_column: str | None = None  # column used as the Kafka message key
    field_mapping: dict[str, str] = Field(default_factory=dict)  # kafka field → table column


class LiveDeliveryConfig(BaseModel):  # REQ-565, REQ-813
    """Unified live change-feed config attached to a table.

    ``strategy`` selects the delta-capture mechanism (REQ-813/814); it replaces
    the old binary ``delivery: poll|cdc``:
      * poll     — watermark polling routed through the engine (watermark_column/poll_interval)
      * native   — source-native push (PostgreSQL LISTEN/NOTIFY, MongoDB change streams)
      * debezium — Debezium/Kafka CDC; transport inherited from Source.cdc (REQ-824)
      * kafka    — arbitrary Kafka delta topic (see ``kafka`` params); transport from Source.cdc

    ``query_id`` and ``outputs`` are optional so this covers both raw table
    change-feeds and live persisted-query output fan-out.
    """

    strategy: str = "poll"  # poll | native | debezium | kafka
    watermark_column: str | None = None  # strategy=poll: column whose max value is the watermark
    poll_interval: int = 10  # strategy=poll: seconds between polls
    kafka: LiveKafkaParams | None = None  # strategy=kafka params
    query_id: str | None = None  # optional stable_id of the persisted query to run
    outputs: list[LiveOutputConfig] = Field(default_factory=list)


class Table(
    BaseModel
):  # REQ-013, REQ-014, REQ-016, REQ-119, REQ-133, REQ-135, REQ-204, REQ-237, REQ-240, REQ-260, REQ-393
    model_config = ConfigDict(populate_by_name=True)

    source_id: str
    domain_id: str
    schema_name: str = Field(
        validation_alias=AliasChoices("schema", "schema_name"),
        serialization_alias="schema",
        default="default",
    )
    table_name: str = Field(
        validation_alias=AliasChoices("table", "table_name"),
        serialization_alias="table",
    )
    columns: list[Column]
    column_presets: list[ColumnPreset] = Field(default_factory=list)
    # REQ-1093: table-level UNIQUE constraints (single-column or composite), seeded from
    # source introspection and editable in the admin "Uniques" panel. Distinct from
    # is_alternate_key (FK-derived, per-column); this is the declared-constraint source of truth.
    unique_constraints: list[UniqueConstraint] = Field(default_factory=list)
    # REQ-119: JSONB field promotions → PG generated columns. Each entry:
    # {jsonb_column, field (dot-path), target_column, target_type}.
    promotions: list[dict] = Field(default_factory=list)
    alias: str | None = None  # GraphQL type/field name override
    description: str | None = None  # GraphQL type description
    cache_ttl: int | None = None  # overrides source-level; None = inherit
    prefer_materialized: bool | None = None  # overrides source-level; None = inherit (REQ-826)
    gql_naming_convention: str | None = None  # overrides source; None = inherit
    hot: bool | None = None  # None = auto-detect, True = force hot, False = opt out
    warm: bool | None = (
        None  # REQ-240: None = auto by query frequency, True = force, False = opt out
    )
    relay_pagination: bool | None = None  # None = inherit from source/global NamingConfig
    live: LiveDeliveryConfig | None = None  # live query delivery config (Phase AM)
    # REQ-924/926/927: the single watermark column (an existing column). Set → append landing +
    # incremental refresh (WHERE wm > cursor) + poll-path subscription (insert/update grain, no
    # hard deletes). Unset → replace landing, full refresh, no poll-path subscription.
    watermark_column: str | None = None
    # REQ-929: change signal for this table; None = inherit the source's. Push variants (debezium/
    # kafka) carry the rows and form the CDC subscription path (deletes included) with no watermark.
    change_signal: str | None = None
    # REQ-929: source-native freshness probe for change_signal in {probe, ttl_probe}; the query
    # returns one comparable token. None → derive MAX(watermark_column) when a watermark exists.
    probe_query: str | None = None
    # REQ-982: input-side change-detection method ∈ {watermark, hash, count, none}. Gated by the
    # source's capability class (probe_capabilities) and implies the landing shape (watermark → append,
    # else replace). None → resolved per class at wiring time (ttl forces none). Validated at parse.
    probe_type: str | None = None
    # REQ-930: cache_ttl is the SINGLE per-table TTL. change_signal in {ttl, ttl_probe} requires it
    # (the poll/staleness cadence); when materialized it is also the refresh cadence. One value, so
    # the change-detection interval and the materialized-copy lifetime can never diverge.
    view_sql: str | None = None  # when set, table is a Provisa-managed view
    materialize: bool = False  # when True, view_sql is materialized as a physical CTAS in mv_cache
    mv_refresh_interval: int = 300  # seconds between MV refreshes (only used when materialize=True)
    # REQ-963: live-MV debounce. deadline = min(last_change+quiet, first_change+max_delay). A burst
    # of upstream changes collapses into one recompute-to-current. quiet=0 disables debounce (pure
    # real-time); max_delay is the mandatory staleness-SLA cap. Consumed by the event loop (Phase 3).
    mv_debounce_quiet: float = 0.0  # seconds of quiet before firing; 0 = real-time (no debounce)
    mv_debounce_max_delay: float = (
        5.0  # hard cap: never more than this stale under continuous churn
    )
    # REQ-962: temporal-window boundary source. calendar names a registered, versioned calendar;
    # grain ∈ daily/weekly/monthly/quarterly/annual. Declaring a calendar makes the MV PERIODIC
    # (calendar-boundary trigger, REQ-961) instead of live-debounce — the two are mutually exclusive.
    mv_calendar: str | None = None
    mv_grain: str | None = None
    mv_business_day_grain: bool = False  # gate window existence on the calendar's business days
    # REQ-961: allowed_lateness (seconds) extends the claim deadline past window.end.
    mv_allowed_lateness: float = 0.0
    # REQ-961: the freshness-contract inputs — the inputs that must be fresh-through window.end for
    # the periodic output to be trusted. None = default to ALL SQL-lineage inputs (extract_inputs,
    # REQ-939); [] = calendar-only (verify nothing).
    mv_expected_events: list[str] | None = None
    # REQ-879: cross-instance MV consistency tier. "shared" = one fleet-coordinated copy (CAS on
    # the shared materialized_views catalog; one instance refreshes at a time). "distributed" =
    # per-instance materialization, the distributed tier (eventually consistent).
    mv_consistency: str = "shared"
    data_product: bool = False  # publish as a Data Product (catalog export)
    enable_aggregates: bool = False  # REQ-653: opt-in for _aggregate root field
    enable_group_by: bool = False  # REQ-653: opt-in for _group_by root field
    approval_hook: bool = False  # REQ-204/247: scope the ABAC approval hook to this table
    kafka_sink: KafkaSinkAttachment | None = None  # REQ-176: per-table Kafka sink config


class HotTablesConfig(BaseModel):  # REQ-544
    auto_threshold: int = 1_000  # max rows for auto-detection
    refresh_interval: int = 300  # seconds between refreshes
    max_rows: int | None = None  # REQ-230: own ceiling; None = fall back to auto_threshold
    max_bytes: int = 10 * 1024 * 1024  # REQ-230: serialized-blob ceiling (10 MB)


class WarmTablesConfig(BaseModel):  # REQ-544
    # REQ-240: tier promotion thresholds + the engine filesystem (SSD) read-cache settings.
    query_threshold: int = 100  # promote a table after this many queries
    max_rows: int = 10_000_000  # do not promote tables larger than this
    refresh_interval: int = 60  # seconds between promotion/demotion sweeps
    fs_cache_enabled: bool = False  # REQ-238: emit fs.cache.* on the Iceberg catalog
    fs_cache_directories: str = "/tmp/engine-cache"  # nosec B108 - engine-node cache dir, configurable
    fs_cache_max_sizes: str = "10GB"


class MaterializedViewsConfig(BaseModel):  # REQ-543
    # REQ-199: default TTL / refresh interval (seconds) for materialized views that do not
    # specify their own. Materialization is opt-in (per-table/relationship materialize flag);
    # there is no cost-based auto-materialization.
    default_ttl: int = 300


class Relationship(BaseModel):  # REQ-019, REQ-020, REQ-158, REQ-159, REQ-399, REQ-400
    id: str
    source_table_id: str
    target_table_id: str = ""  # empty for computed (function-target) relationships
    source_column: str
    target_column: str = ""  # empty for computed relationships
    cardinality: Cardinality
    materialize: bool = False  # auto-create MV for cross-source joins
    refresh_interval: int = 300  # MV refresh interval in seconds
    target_function_name: str | None = None  # computed relationship: DB function name
    function_arg: str | None = None  # which function arg receives source_column value
    alias: str | None = (
        None  # human-readable relationship type (e.g. WORKS_FOR); unique per source table
    )
    graphql_alias: str | None = (
        None  # persisted GraphQL field name override (computed from target+cardinality when absent)
    )
    disable_cypher: bool = False  # when True, exclude this relationship from Cypher graph edges
    source_json_key: str | None = (
        None  # when set, JOIN extracts this key from source column as JSON object
    )
    # REQ-020: defining-steward ownership, version, and re-review flag.
    owner: str | None = None  # role/user id of the defining steward
    version: int = 1  # bumped on every upsert and on re-review flagging
    needs_review: bool = False  # set when a join-field schema change may have stale-d the join


class RoleRateLimit(BaseModel):
    """Per-role rate limits (REQ-369). None = unlimited for that dimension."""

    requests_per_second: int | None = None
    max_sse_subscriptions: int | None = None
    max_flight_streams: int | None = None


class Role(BaseModel):  # REQ-003, REQ-005, REQ-042, REQ-059, REQ-060, REQ-369
    id: str
    capabilities: list[str]
    domain_access: list[str]
    rate_limit: RoleRateLimit | None = None  # REQ-369
    parent_role_id: str | None = None  # inherit capabilities + domain_access from parent
    relationship_guard: bool = (
        True  # when False (+ SQL opt-out), V002 join approval check is skipped
    )
    max_rows: int | None = None  # REQ-005: per-role result-size ceiling (LIMIT injected by Stage 2)


def flatten_roles(roles: list[Role]) -> list[Role]:  # REQ-003, REQ-005, REQ-042
    """Flatten inherited role hierarchy: merge parent capabilities + domain_access.

    Returns new Role objects with inherited permissions merged in.
    """
    by_id = {r.id: r for r in roles}
    cache: dict[str, tuple[set[str], set[str], int | None]] = {}

    def _resolve(role_id: str) -> tuple[set[str], set[str], int | None]:
        if role_id in cache:
            return cache[role_id]
        role = by_id[role_id]
        caps = set(role.capabilities)
        domains = set(role.domain_access)
        max_rows = role.max_rows
        if role.parent_role_id and role.parent_role_id in by_id:
            p_caps, p_domains, p_max_rows = _resolve(role.parent_role_id)
            caps.update(p_caps)
            domains.update(p_domains)
            # Ceiling: child inherits parent's when unset; most restrictive wins when both set.
            if max_rows is None:
                max_rows = p_max_rows
            elif p_max_rows is not None:
                max_rows = min(max_rows, p_max_rows)
        cache[role_id] = (caps, domains, max_rows)
        return caps, domains, max_rows

    result: list[Role] = []
    for r in roles:
        caps, domains, max_rows = _resolve(r.id)
        result.append(
            Role(
                id=r.id,
                capabilities=sorted(caps),
                domain_access=["*"] if "*" in domains else sorted(domains),
                parent_role_id=r.parent_role_id,
                relationship_guard=r.relationship_guard,
                max_rows=max_rows,
                rate_limit=r.rate_limit,
            )
        )
    return result


class RLSRule(BaseModel):  # REQ-041, REQ-402
    table_id: str | None = None
    domain_id: str | None = None
    role_id: str
    filter: str


class EventTrigger(BaseModel):  # REQ-565
    """Database event trigger: PG LISTEN/NOTIFY → webhook POST."""

    table_id: str  # table name or schema.table
    operations: list[str] = Field(default_factory=lambda: ["insert", "update", "delete"])
    webhook_url: str
    retry_max: int = 3  # max retry attempts
    retry_delay: float = 1.0  # base delay in seconds (exponential backoff)
    enabled: bool = True


class FunctionArgument(BaseModel):
    """Argument definition for a tracked DB function."""

    name: str
    type: str  # GraphQL scalar type name: String, Int, Float, Boolean, DateTime
    # REQ-885: relation-argument kind for Provisa-hosted functions.
    #   column_value = scalar, passed row-wise (default; existing behaviour)
    #   table_ref    = lazy reference (relation name/metadata passed, not materialized)
    #   result_set   = eager, referenced relation materialized to an Arrow buffer
    arg_kind: str = "column_value"


class InlineType(BaseModel):
    """Inline return type field for webhooks (no registered table)."""

    name: str
    type: str  # GraphQL scalar type name


class Function(BaseModel):  # REQ-205, REQ-206, REQ-207, REQ-208
    """Tracked DB function exposed as a GraphQL query or mutation."""

    name: str  # exposed field name
    source_id: str
    schema_name: str = Field(
        validation_alias=AliasChoices("schema", "schema_name"),
        serialization_alias="schema",
        default="public",
    )
    function_name: str
    returns: str  # registered table id (source_id.schema.table)
    arguments: list[FunctionArgument] = Field(default_factory=list)
    visible_to: list[str] = Field(default_factory=list)
    writable_by: list[str] = Field(default_factory=list)
    domain_id: str = ""
    description: str | None = None
    kind: str = "mutation"  # "mutation" or "query"
    # REQ-885: implementation-kind dimension. Addressing (name/function_name) is decoupled
    # from binding (transport + location, swappable). ``source_procedure`` is the existing
    # REQ-205–208 path; the others are Provisa-hosted / external implementations.
    #   source_procedure | script | http | grpc | python
    impl_kind: str = "source_procedure"
    # Per-kind transport+location. Never a fallback: dispatch fails loud when a hosted kind
    # is registered without the binding keys its transport requires (REQ-885).
    #   script: {"argv": [...]}   http: {"url", "method"?}   grpc: {"target", "method"}
    #   python: {"callable": "module:attr"}   source_procedure: {} (uses source_id/function_name)
    binding: dict = Field(default_factory=dict)
    # REQ-885 identity model: True ⇒ admin/DEFINER, output-governed; False ⇒ user/INVOKER,
    # input-governed. Selects the identity context stamped into the invocation trace (REQ-886).
    materialize: bool = False

    model_config = ConfigDict(populate_by_name=True)


class Webhook(BaseModel):  # REQ-209, REQ-210, REQ-211
    """External HTTP webhook exposed as a GraphQL query or mutation."""

    name: str  # exposed field name
    url: str
    method: str = "POST"
    timeout_ms: int = 5000
    returns: str | None = None  # registered table id, or None for inline type
    inline_return_type: list[InlineType] = Field(default_factory=list)
    arguments: list[FunctionArgument] = Field(default_factory=list)
    visible_to: list[str] = Field(default_factory=list)
    domain_id: str = ""
    description: str | None = None
    kind: str = "mutation"  # "mutation" or "query"
    governance: str | None = None  # e.g. "requires_approval" (REQ-209)


class ScheduledTrigger(BaseModel):
    """Time-based trigger for webhooks or internal functions."""

    id: str
    cron: str  # cron expression (e.g. "0 * * * *" for hourly)
    url: str | None = None  # webhook URL (mutually exclusive with function)
    webhook_name: str | None = None  # display name for the webhook
    args: dict = Field(default_factory=dict)  # arg name → value for webhook POST body
    function: str | None = None  # internal function name
    # REQ-1003: SQL statement executed against the federated engine on the cron schedule.
    # Mutually exclusive with url/function. REQ-1004: the text may contain {{date-token}}
    # placeholders substituted with the run's execution date/time before execution.
    sql: str | None = None
    enabled: bool = True


class AuthConfig(
    BaseModel
):  # REQ-120, REQ-121, REQ-122, REQ-123, REQ-124, REQ-125, REQ-203, REQ-247
    provider: str = "none"  # none, firebase, keycloak, oauth, oidc, simple
    firebase: dict | None = None
    keycloak: dict | None = None
    oauth: dict | None = None
    oidc: dict | None = (
        None  # REQ-890: generic OIDC (discovery_url, client_id, audience, role_claim)
    )
    simple: dict | None = None
    allow_simple_auth: bool = False  # REQ-124: production guard — simple auth refused unless true
    superuser: dict | None = None
    role_mapping: list[dict] = Field(default_factory=list)
    default_role: str = "analyst"
    assignments_source: str = "claims"  # claims | provisa
    default_assignments: list[dict] = Field(default_factory=list)
    trust_upstream: bool = False
    approval_hook: dict | None = None  # REQ-247: ABAC approval hook config block


class OtelConfig(BaseModel):  # REQ-545
    """OpenTelemetry tracing configuration.

    endpoint: OTLP gRPC collector address (e.g. http://otel-collector:4317).
    Empty string means spans are generated but silently dropped.
    Overridden at runtime by OTEL_EXPORTER_OTLP_ENDPOINT env var.

    service_name: reported service name. Overridden by OTEL_SERVICE_NAME env var.
    sample_rate: fraction of traces to sample (1.0 = 100%).
    log_level: Python log level name forwarded to the OTLP log exporter (default WARNING).
    compact_cron: cron expression for the Parquet→Iceberg compaction job (default every minute).
    compact_batch_size: rows per INSERT batch during compaction; reduce for low-memory engines.
    compact_file_chunk: Parquet files processed per compaction chunk; reduce for low-memory environments.
    ops_snapshot_retention_hours: if set, expire Iceberg snapshots and orphan files older than
        this many hours on each startup. None (default) disables expiry.
    span_export_delay_millis: how often the BatchSpanProcessor flushes spans to the collector
        (milliseconds). Lower values reduce trace latency; default 1000.
    otlp2parquet_max_age_secs: max age before otlp2parquet flushes a Parquet batch to S3
        (seconds). Lower values reduce trace latency; default 5.
    collector_batch_timeout_ms: OTel Collector batch processor timeout (milliseconds).
        Lower values reduce trace latency; default 200.
    s3_endpoint: S3/MinIO endpoint for Parquet→Iceberg compaction. Default http://minio:9000
        works in Docker; set to http://localhost:9000 when running the backend locally.
        Overridden at runtime by PROVISA_OTEL_S3_ENDPOINT env var.
    """

    endpoint: str = ""
    service_name: str = "provisa"
    sample_rate: float = 1.0
    log_level: str = "WARNING"
    compact_cron: str = "* * * * *"
    compact_batch_size: int = 10
    compact_file_chunk: int = 50
    ops_snapshot_retention_hours: int | None = None
    span_export_delay_millis: int = 1000
    otlp2parquet_max_age_secs: int = 5
    collector_batch_timeout_ms: int = 200
    s3_endpoint: str = "http://minio:9000"


class GraphQLRemoteConfig(BaseModel):
    max_object_depth: int = 5
    max_list_depth: int = 2
    max_list_items: int = 100


class VectorModelConfig(BaseModel):  # REQ-500
    """A registered embedding model (REQ-419). The registry is an allowlist."""

    id: str  # provider model id, e.g. "text-embedding-3-small"
    provider: str  # "openai" | "ollama" | "huggingface"
    dimensions: int
    api_key_env: str | None = None  # env var holding the API key
    base_url: str | None = None  # provider base URL override
    enabled: bool = True


class AIModelsConfig(BaseModel):  # REQ-464
    """AI model configuration for various operations.

    Each field accepts either:
    - A string (shorthand): model name (e.g. "claude-haiku-4-5-20251001")
    - A dict (full form): {"vendor": str, "model": str, "fallback": dict | null}
    """

    table_description: str | dict = "claude-haiku-4-5-20251001"
    column_description: str | dict = "claude-haiku-4-5-20251001"
    relationship_inference: str | dict = "claude-haiku-4-5-20251001"
    sql_generation: str | dict = "claude-opus-4-6"
    table_selection: str | dict = "claude-haiku-4-5-20251001"


class NlConfig(BaseModel):  # REQ-464
    """Natural-language query service config."""

    rate_limit: int | None = None  # REQ-370: requests per minute per role (None = unlimited)


class ServerConfig(BaseModel):
    """Server network configuration.

    Set ``hostname`` to the publicly reachable hostname or IP of the Provisa
    server.  Used by gRPC, Arrow Flight, SSE, and any self-referential URLs
    (e.g. redirect presign base URLs).  Defaults to ``localhost`` for desktop
    development; set to the pod/VM hostname or load-balancer address in production.

    Override via ``PROVISA_HOSTNAME`` environment variable at runtime.

    Ports can be overridden via env vars: ``PROVISA_PORT``, ``GRPC_PORT``,
    ``FLIGHT_PORT``.
    """

    hostname: str = "localhost"
    port: int = 8000
    grpc_port: int = 50051
    flight_port: int = 8815


class GovDataSubject(str, Enum):
    """Subscription subject groupings for GovData schemas.

    ALL is equivalent to subscribing to the full GOVDATA service.
    """

    all = "ALL"
    commerce = "COMMERCE"
    economy = "ECONOMY"
    education = "EDUCATION"
    health = "HEALTH"
    cyber = "CYBER"
    public_safety = "PUBLIC_SAFETY"
    environment = "ENVIRONMENT"
    weather = "WEATHER"
    energy = "ENERGY"
    government = "GOVERNMENT"
    demographics = "DEMOGRAPHICS"


# Maps each subject to the GovData schema names it covers.
# ALL expands to every schema at access-check time.
# "ref" and "geo" are always included as linker schemas — not listed here.
GOVDATA_SUBJECT_SCHEMAS: dict[str, list[str]] = {
    "COMMERCE": ["sec", "patents"],
    "ECONOMY": ["econ", "econ_reference"],
    "EDUCATION": ["census", "edu"],
    "HEALTH": ["health"],
    "CYBER": ["cyber_threat", "cyber_vuln"],
    "PUBLIC_SAFETY": ["crime"],
    "ENVIRONMENT": ["lands"],
    "WEATHER": ["weather"],
    "ENERGY": ["energy"],
    "GOVERNMENT": ["fedregister", "fec"],
}


class GovDataSource(BaseModel):  # REQ-540
    """A GovData dataset group exposed via the askamerica JDBC adapter.

    Each entry corresponds to one set of GovData schemas sharing a subject tag.
    At query time Provisa connects via askamerica.engine.get_connection(api_key).
    """

    id: str
    subject: GovDataSubject
    govdata_schemas: list[str]
    domain_id: str
    description: str = ""
    api_key: str = ""
    start_year: int | None = None
    end_year: int | None = None
    ciks: str | None = None


class GovDataSubscription(BaseModel):  # REQ-540
    """Per-tenant list of allowed GovData subjects.

    ``subjects`` containing GovDataSubject.all grants access to every subject.
    An empty list means no GovData access.
    """

    subjects: list[GovDataSubject] = Field(default_factory=list)


class ControlPlaneConfig(BaseModel):
    """Database connections for the two control planes.

    The ONLY place database connection details enter Provisa. Both planes are
    driven purely by SQLAlchemy, each by its own URI. Values may embed
    ``${env:VAR}`` / ``${env:VAR:-default}`` references resolved through the
    secrets provider — config is the single chokepoint that reads the
    environment; everything else reads this resolved config.

      * tenant plane  — per-org control plane, schema-scoped at runtime.
      * platform plane — global registry (orgs/users/invites) + billing.
    """

    tenant_url: str = (
        "${env:TENANT_DATABASE_URL:-postgresql+asyncpg://provisa:provisa@localhost:5432/provisa}"
    )
    # REQ-837: PLATFORM_DATABASE_URL is required at startup with no fallback —
    # a missing var raises in resolve_secrets rather than silently defaulting.
    platform_url: str = "${env:PLATFORM_DATABASE_URL}"
    org_id: str = "${env:ORG_ID:-default}"
    pool_min: int = 2
    pool_max: int = 10

    @property
    def max_overflow(self) -> int:
        return max(self.pool_max - self.pool_min, 0)

    def resolved_tenant_url(self) -> str:
        from provisa.core.secrets import resolve_secrets

        return resolve_secrets(self.tenant_url)

    def resolved_platform_url(self) -> str:
        from provisa.core.secrets import resolve_secrets

        return resolve_secrets(self.platform_url)

    def resolved_org_id(self) -> str:
        from provisa.core.secrets import resolve_secrets

        return resolve_secrets(self.org_id)

    def tenant_parts(self) -> tuple[str | None, int, str | None, str | None, str | None]:
        """(host, port, database, username, password) parsed from the tenant URL —
        drives the raw asyncpg tenant pool and the engine self-catalog.

        For a unix-socket URL (the embedded/native tier — no host in the netloc, the
        socket directory carried in ``?host=/dir``), the host lives in the query, so
        read it there when the netloc host is absent. asyncpg treats a directory host
        as a unix socket, so this feeds ``create_pool(host=/dir)`` directly."""
        from sqlalchemy import make_url

        u = make_url(self.resolved_tenant_url())
        q_host = u.query.get("host")  # SQLAlchemy multi-valued query params can be a tuple
        socket_host: str | None = q_host[0] if isinstance(q_host, tuple) else q_host
        return u.host or socket_host, u.port or 5432, u.database, u.username, u.password


class SecurityConfig(BaseModel):  # REQ-693
    """Platform security posture.

    ``mode=high`` is the zero-trust posture: the pgwire server is not started, REST
    and GraphQL data-API endpoints return 403, and JDBC/Python connections without a
    ``kms_key_arn`` (client-side decrypt configured) are rejected at auth. Only clients
    that decrypt locally may reach data. ``mode=standard`` is the default posture.
    """

    mode: str = "standard"  # "standard" | "high"

    @property
    def high(self) -> bool:
        return self.mode.lower() == "high"


class ProvisaConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    control_plane: ControlPlaneConfig = Field(default_factory=ControlPlaneConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)  # REQ-693
    multitenancy: bool = False
    # Federation engine selection + connection config (set via the admin UI; applied on restart).
    # The selected engine's own implementation reads these — generic code never branches on the value.
    federation_engine: str = "duckdb"  # REQ-989: zero-config default is the embedded DuckDB engine
    federation_engine_url: str | None = None  # DSN for sqlalchemy/clickhouse/pg engines
    federation_engine_host: str = "localhost"  # coordinator host (engine)
    federation_engine_port: int = 8080  # coordinator port (engine)
    # This embedded-MPP instance's role in the cluster: "coordinator" (also schedules work on itself)
    # or "worker". Drives which template write_trino_config renders into config.properties.
    node_role: str = "coordinator"
    jvm_heap_gb: int = 8
    query_max_memory: str = "4GB"
    query_max_memory_per_node: str = "2GB"
    query_max_total_memory: str = "8GB"
    # Fault-tolerant execution (replaces the previous spill-to-disk). All sizes/locations
    # are config-driven and engine-agnostic — no execution-engine identifiers leak
    # into the config vocabulary.
    fault_tolerant_execution: bool = True
    fault_tolerant_task_memory: str = "1GB"
    exchange_spool_dir: str = "/data/provisa/exchange"
    exchange_spool_bucket: str = "provisa-exchange"
    # S3-backed spool for multi-host deployments (a local dir is not shared across
    # hosts). Empty endpoint → local filesystem spool at exchange_spool_dir.
    exchange_spool_s3_endpoint: str = ""
    exchange_spool_s3_region: str = "us-east-1"
    exchange_spool_s3_access_key: str = ""
    exchange_spool_s3_secret_key: str = ""
    default_org_id: str = "root"
    sources: list[Source]
    domains: list[Domain]
    naming: NamingConfig = Field(default_factory=NamingConfig)
    tables: list[Table]
    relationships: list[Relationship] = Field(default_factory=list)
    vector_models: list[VectorModelConfig] = Field(default_factory=list)  # REQ-419
    roles: list[Role]
    rls_rules: list[RLSRule] = Field(default_factory=list)
    event_triggers: list[EventTrigger] = Field(default_factory=list)
    scheduled_triggers: list[ScheduledTrigger] = Field(default_factory=list)
    functions: list[Function] = Field(default_factory=list)
    webhooks: list[Webhook] = Field(default_factory=list)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    hot_tables: HotTablesConfig = Field(default_factory=HotTablesConfig)
    # PostgreSQL DSN where non-attachable sources LAND for native (duckdb/…) engines; set via the
    # admin UI. Empty → the native materialize path is unavailable. Applied on restart.
    materialize_store_url: str | None = None
    # REQ-931: Provisa-level Kafka consumer group for inbound CDC (Debezium/Kafka). Receiver-side —
    # one consumer identity across all sources; a source's cdc.consumer_group_id overrides it.
    cdc_consumer_group_id: str = "provisa-debezium"
    warm_tables: WarmTablesConfig = Field(default_factory=WarmTablesConfig)
    materialized_views: MaterializedViewsConfig = Field(default_factory=MaterializedViewsConfig)
    observability: OtelConfig = Field(default_factory=OtelConfig)
    graphql_remote: GraphQLRemoteConfig = Field(default_factory=GraphQLRemoteConfig)
    ai_models: AIModelsConfig = Field(default_factory=AIModelsConfig)
    nl: NlConfig = Field(default_factory=NlConfig)
    govdata_sources: list[GovDataSource] = Field(default_factory=list)
    govdata_subscriptions: list[GovDataSubscription] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_domain_policy(self) -> "ProvisaConfig":
        # Inert when the feature is not engaged (use_domains absent).
        if self.naming.use_domains is None:
            return self
        if self.naming.use_domains is False:
            if self.domains:
                raise ValueError(
                    "naming.use_domains=false is mutually exclusive with a non-empty `domains:` list"
                )
            allowed = self.naming.default_domain
            offenders: list[str] = []
            for t in self.tables:
                if t.domain_id and t.domain_id != allowed:
                    offenders.append(f"table {t.source_id}.{t.table_name}={t.domain_id!r}")
            for fn in self.functions:
                if fn.domain_id and fn.domain_id != allowed:
                    offenders.append(f"function {fn.name}={fn.domain_id!r}")
            for wh in self.webhooks:
                if wh.domain_id and wh.domain_id != allowed:
                    offenders.append(f"webhook {wh.name}={wh.domain_id!r}")
            for gd in self.govdata_sources:
                if gd.domain_id and gd.domain_id != allowed:
                    offenders.append(f"govdata_source {gd.id}={gd.domain_id!r}")
            if offenders:
                raise ValueError(
                    f"naming.use_domains=false permits only domain {allowed!r}; offending: "
                    + ", ".join(offenders)
                )
        return self
