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


class SourceType(str, Enum):
    # RDBMS
    postgresql = "postgresql"
    mysql = "mysql"
    singlestore = "singlestore"
    mariadb = "mariadb"
    sqlserver = "sqlserver"
    oracle = "oracle"
    duckdb = "duckdb"
    # Cloud DW
    snowflake = "snowflake"
    bigquery = "bigquery"
    databricks = "databricks"
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
    hive_s3 = "hive_s3"  # S3-backed Hive via Trino hive connector (TRINO_ONLY)
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
    # Push receiver — external services POST JSON events (Phase AS)
    ingest = "ingest"
    # U.S. government open data via Apache Calcite / GovData JDBC adapter
    govdata = "govdata"


class Cardinality(str, Enum):
    many_to_one = "many-to-one"
    one_to_many = "one-to-many"


# Map source types to Trino connector names
SOURCE_TO_CONNECTOR: dict[str, str] = {
    "postgresql": "postgresql",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "singlestore": "singlestore",
    "sqlserver": "sqlserver",
    "oracle": "oracle",
    "mongodb": "mongodb",
    "cassandra": "cassandra",
    "duckdb": "memory",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "redshift": "redshift",
    "databricks": "delta_lake",
    "hive": "hive",
    "druid": "druid",
    "exasol": "exasol",
    # TRINO_ONLY lake sources — connector-only, no direct driver, no SQLGlot dialect (REQ-229)
    "iceberg": "iceberg",
    "hive_s3": "hive",
    "delta_lake": "delta_lake",
    # NoSQL/non-relational connectors driven by the mapping DSL (REQ-250/251)
    "redis": "redis",
    "elasticsearch": "elasticsearch",
    "prometheus": "prometheus",
    "kafka": "kafka",
}

# Map source types to SQLGlot dialect names (enables direct-route single-source queries)
SOURCE_TO_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "mysql": "mysql",
    "mariadb": "mysql",
    "singlestore": "singlestore",
    "sqlserver": "tsql",
    "oracle": "oracle",
    "duckdb": "duckdb",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "redshift": "redshift",
    "databricks": "databricks",
    "hive": "hive",
    "druid": "druid",
    "exasol": "exasol",
}

# Source types that are TRINO_ONLY — no direct driver, no SQLGlot dialect (REQ-229)
TRINO_ONLY_SOURCES: set[str] = {"iceberg", "hive_s3", "delta_lake"}

# Source types that support time-travel via Trino FOR TIMESTAMP/VERSION AS OF (REQ-372)
TIME_TRAVEL_SOURCES: set[str] = {"iceberg", "delta_lake"}


_SAFE_ID_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")


class Source(BaseModel):
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
    use_pgbouncer: bool = Field(default=False, alias="use_pgbouncer")
    pgbouncer_port: int = Field(default=6432, alias="pgbouncer_port")
    cache_enabled: bool = True
    cache_ttl: int | None = None  # overrides global default; None = inherit
    cache_catalog: str | None = None  # Trino catalog for API cache; None = source's own catalog
    cache_schema: str = "api_cache"  # schema within that catalog
    gql_naming_convention: str | None = None  # overrides global; None = inherit
    federation_hints: dict[str, str] = Field(default_factory=dict)  # Trino session props
    # REQ-251: type-specific mapping DSL for NoSQL/non-relational sources
    # (redis/elasticsearch/prometheus). Holds {"tables": [...]} plus connector options.
    mapping: dict = Field(default_factory=dict)
    approval_hook: bool = False  # REQ-204/247: scope the ABAC approval hook to this source
    allowed_domains: list[str] = Field(
        default_factory=list
    )  # restrict this source to specific domains; empty = unrestricted
    description: str = ""

    @property
    def connector(self) -> str:
        return SOURCE_TO_CONNECTOR[self.type.value]

    @property
    def dialect(self) -> str | None:
        """SQLGlot dialect name, or None for NoSQL sources."""
        return SOURCE_TO_DIALECT.get(self.type.value)

    @property
    def catalog_name(self) -> str:
        """Trino catalog name — sanitized source id."""
        return self.id.replace("-", "_")

    def jdbc_url(self, host: str | None = None, port: int | None = None) -> str:
        prefix = {
            "postgresql": "jdbc:postgresql",
            "mysql": "jdbc:mysql",
            "sqlserver": "jdbc:sqlserver",
            "oracle": "jdbc:oracle:thin",
            "mariadb": "jdbc:mariadb",
        }
        p = prefix.get(self.type.value)
        if p is None:
            return ""
        h = host or self.host
        po = port or self.port
        if self.type == SourceType.sqlserver:
            return f"{p}://{h}:{po};databaseName={self.database}"
        if self.type == SourceType.oracle:
            return f"{p}:@{h}:{po}/{self.database}"
        if self.type == SourceType.postgresql:
            # autosave=conservative prevents pgjdbc from crashing on the
            # pg_type_typname_nsp_index duplicate-key race when two concurrent
            # queries register the same anonymous composite type.
            return f"{p}://{h}:{po}/{self.database}?autosave=conservative"
        return f"{p}://{h}:{po}/{self.database}"


class Domain(BaseModel):
    id: str
    description: str = ""
    graphql_alias: str | None = None
    ddl_catalog: str | None = None  # Trino catalog for DDL; defaults to system Iceberg catalog
    ddl_schema: str | None = None  # schema within ddl_catalog; defaults to domain id


class NamingRule(BaseModel):
    pattern: str
    replace: str


class NamingConfig(BaseModel):
    convention: str = "apollo_graphql"
    sql_convention: str = "snake"
    rules: list[NamingRule] = Field(default_factory=list)
    relay_pagination: bool = False  # opt-in: generate _connection fields + Edge/PageInfo types
    # REQ-415: FK-derived relationship names follow Hasura V2 conventions —
    # singular object (many-to-one), plural array (one-to-many) via inflection.
    hasura_v2_relationship_style: bool = False
    domain_prefix: bool = False  # prepend domain initials to GraphQL names (namespaced mode)
    # Tri-state domain feature. None = legacy (inert); False = single stored default_domain,
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


class Column(BaseModel):
    name: str
    data_type: str | None = (
        None  # source column type (e.g. "varchar", "integer"); lets startup skip Trino introspection
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


class ColumnPreset(BaseModel):
    """Auto-set a column value on insert/update from session or built-in source."""

    column: str
    source: str  # "header", "now", "literal"
    name: str | None = None  # header name (for source=header)
    value: str | None = None  # literal value (for source=literal)
    data_type: str | None = None  # Trino data type of the column (for coercion)


class LiveOutputConfig(BaseModel):
    """Single output destination for a live query (SSE fanout or Kafka sink)."""

    type: str  # "sse" | "kafka"
    topic: str | None = None  # Kafka topic (required when type="kafka")
    key_column: str | None = None  # Kafka message key column


class LiveDeliveryConfig(BaseModel):
    """Live query delivery configuration attached to a table."""

    query_id: str  # stable_id of the approved persisted query to run
    watermark_column: str  # column whose max value is tracked as the watermark
    poll_interval: int = 10  # seconds between polls
    outputs: list[LiveOutputConfig] = Field(default_factory=list)


class Table(BaseModel):
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
    # REQ-119: JSONB field promotions → PG generated columns. Each entry:
    # {jsonb_column, field (dot-path), target_column, target_type}.
    promotions: list[dict] = Field(default_factory=list)
    alias: str | None = None  # GraphQL type/field name override
    description: str | None = None  # GraphQL type description
    cache_ttl: int | None = None  # overrides source-level; None = inherit
    gql_naming_convention: str | None = None  # overrides source; None = inherit
    hot: bool | None = None  # None = auto-detect, True = force hot, False = opt out
    relay_pagination: bool | None = None  # None = inherit from source/global NamingConfig
    live: LiveDeliveryConfig | None = None  # live query delivery config (Phase AM)
    watermark_column: str | None = None  # column used by polling subscription provider
    view_sql: str | None = None  # when set, table is a Provisa-managed view
    materialize: bool = False  # when True, view_sql is materialized as a physical CTAS in mv_cache
    mv_refresh_interval: int = 300  # seconds between MV refreshes (only used when materialize=True)
    data_product: bool = False  # publish as a Data Product (catalog export)
    approval_hook: bool = False  # REQ-204/247: scope the ABAC approval hook to this table


class HotTablesConfig(BaseModel):
    auto_threshold: int = 1_000  # max rows for auto-detection
    refresh_interval: int = 300  # seconds between refreshes


class Relationship(BaseModel):
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


class Role(BaseModel):
    id: str
    capabilities: list[str]
    domain_access: list[str]
    rate_limit: RoleRateLimit | None = None  # REQ-369
    parent_role_id: str | None = None  # inherit capabilities + domain_access from parent
    relationship_guard: bool = (
        True  # when False (+ SQL opt-out), V002 join approval check is skipped
    )
    max_rows: int | None = None  # REQ-005: per-role result-size ceiling (LIMIT injected by Stage 2)
    allow_aggregations: bool = True  # REQ-197: when False, aggregate root fields are not exposed


def flatten_roles(roles: list[Role]) -> list[Role]:
    """Flatten inherited role hierarchy: merge parent capabilities + domain_access.

    Returns new Role objects with inherited permissions merged in.
    """
    by_id = {r.id: r for r in roles}
    cache: dict[str, tuple[set[str], set[str], int | None, bool]] = {}

    def _resolve(role_id: str) -> tuple[set[str], set[str], int | None, bool]:
        if role_id in cache:
            return cache[role_id]
        role = by_id[role_id]
        caps = set(role.capabilities)
        domains = set(role.domain_access)
        max_rows = role.max_rows
        allow_agg = role.allow_aggregations
        if role.parent_role_id and role.parent_role_id in by_id:
            p_caps, p_domains, p_max_rows, p_allow_agg = _resolve(role.parent_role_id)
            caps.update(p_caps)
            domains.update(p_domains)
            # Ceiling: child inherits parent's when unset; most restrictive wins when both set.
            if max_rows is None:
                max_rows = p_max_rows
            elif p_max_rows is not None:
                max_rows = min(max_rows, p_max_rows)
            # Aggregate gating: most restrictive wins — a parent that disallows disallows the child.
            allow_agg = allow_agg and p_allow_agg
        cache[role_id] = (caps, domains, max_rows, allow_agg)
        return caps, domains, max_rows, allow_agg

    result: list[Role] = []
    for r in roles:
        caps, domains, max_rows, allow_agg = _resolve(r.id)
        result.append(
            Role(
                id=r.id,
                capabilities=sorted(caps),
                domain_access=["*"] if "*" in domains else sorted(domains),
                parent_role_id=r.parent_role_id,
                relationship_guard=r.relationship_guard,
                max_rows=max_rows,
                rate_limit=r.rate_limit,
                allow_aggregations=allow_agg,
            )
        )
    return result


class RLSRule(BaseModel):
    table_id: str | None = None
    domain_id: str | None = None
    role_id: str
    filter: str


class EventTrigger(BaseModel):
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


class InlineType(BaseModel):
    """Inline return type field for webhooks (no registered table)."""

    name: str
    type: str  # GraphQL scalar type name


class Function(BaseModel):
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

    model_config = ConfigDict(populate_by_name=True)


class Webhook(BaseModel):
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


class ScheduledTrigger(BaseModel):
    """Time-based trigger for webhooks or internal functions."""

    id: str
    cron: str  # cron expression (e.g. "0 * * * *" for hourly)
    url: str | None = None  # webhook URL (mutually exclusive with function)
    function: str | None = None  # internal function name
    enabled: bool = True


class AuthConfig(BaseModel):
    provider: str = "none"  # none, firebase, keycloak, oauth, simple
    firebase: dict | None = None
    keycloak: dict | None = None
    oauth: dict | None = None
    simple: dict | None = None
    allow_simple_auth: bool = False  # REQ-124: production guard — simple auth refused unless true
    superuser: dict | None = None
    role_mapping: list[dict] = Field(default_factory=list)
    default_role: str = "analyst"
    assignments_source: str = "claims"  # claims | provisa
    default_assignments: list[dict] = Field(default_factory=list)
    trust_upstream: bool = False
    approval_hook: dict | None = None  # REQ-247: ABAC approval hook config block


class OtelConfig(BaseModel):
    """OpenTelemetry tracing configuration.

    endpoint: OTLP gRPC collector address (e.g. http://otel-collector:4317).
    Empty string means spans are generated but silently dropped.
    Overridden at runtime by OTEL_EXPORTER_OTLP_ENDPOINT env var.

    service_name: reported service name. Overridden by OTEL_SERVICE_NAME env var.
    sample_rate: fraction of traces to sample (1.0 = 100%).
    log_level: Python log level name forwarded to the OTLP log exporter (default WARNING).
    compact_cron: cron expression for the Parquet→Iceberg compaction job (default every minute).
    compact_batch_size: rows per INSERT batch during compaction; reduce for low-memory Trino.
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


class VectorModelConfig(BaseModel):
    """A registered embedding model (REQ-419). The registry is an allowlist."""

    id: str  # provider model id, e.g. "text-embedding-3-small"
    provider: str  # "openai" | "ollama" | "huggingface"
    dimensions: int
    api_key_env: str | None = None  # env var holding the API key
    base_url: str | None = None  # provider base URL override
    enabled: bool = True


class AIModelsConfig(BaseModel):
    """AI model configuration for various operations.

    Each field accepts either:
    - A string (legacy): model name (e.g. "claude-haiku-4-5-20251001")
    - A dict (new): {"vendor": str, "model": str, "fallback": dict | null}
    """

    table_description: str | dict = "claude-haiku-4-5-20251001"
    column_description: str | dict = "claude-haiku-4-5-20251001"
    relationship_inference: str | dict = "claude-haiku-4-5-20251001"
    sql_generation: str | dict = "claude-opus-4-6"
    table_selection: str | dict = "claude-haiku-4-5-20251001"


class NlConfig(BaseModel):
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


class GovDataSource(BaseModel):
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


class GovDataSubscription(BaseModel):
    """Per-tenant list of allowed GovData subjects.

    ``subjects`` containing GovDataSubject.all grants access to every subject.
    An empty list means no GovData access.
    """

    subjects: list[GovDataSubject] = Field(default_factory=list)


class ProvisaConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    multitenancy: bool = False
    jvm_heap_gb: int = 8
    spill_enabled: bool = True
    spill_path: str = "/tmp/provisa-spill"  # nosec B108 - Trino spill dir default, config-overridable
    query_max_memory: str = "4GB"
    query_max_memory_per_node: str = "2GB"
    query_max_total_memory: str = "8GB"
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
