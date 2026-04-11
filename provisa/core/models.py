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

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    rss = "rss"             # RSS 2.0 / Atom feed — poll, watermark by pubDate/updated
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


class GovernanceLevel(str, Enum):
    pre_approved = "pre-approved"
    registry_required = "registry-required"


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
            raise ValueError(
                f"Source id must be alphanumeric with hyphens/underscores, got: {v!r}"
            )
        return v
    type: SourceType
    host: str = ""
    port: int = 0
    database: str = ""
    username: str = ""
    password: str = ""  # Secret reference e.g. ${env:PG_PASSWORD}
    path: str | None = None  # File path or URL for file-based sources (csv, parquet, sqlite)
    pool_min: int = Field(default=1, alias="pool_min")
    pool_max: int = Field(default=5, alias="pool_max")
    use_pgbouncer: bool = Field(default=False, alias="use_pgbouncer")
    pgbouncer_port: int = Field(default=6432, alias="pgbouncer_port")
    cache_enabled: bool = True
    cache_ttl: int | None = None  # overrides global default; None = inherit
    naming_convention: str | None = None  # overrides global; None = inherit
    federation_hints: dict[str, str] = Field(default_factory=dict)  # Trino session props

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

    def jdbc_url(self) -> str:
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
        if self.type == SourceType.sqlserver:
            return f"{p}://{self.host}:{self.port};databaseName={self.database}"
        if self.type == SourceType.oracle:
            return f"{p}:@{self.host}:{self.port}/{self.database}"
        return f"{p}://{self.host}:{self.port}/{self.database}"


class Domain(BaseModel):
    id: str
    description: str = ""


class NamingRule(BaseModel):
    pattern: str
    replace: str


class NamingConfig(BaseModel):
    convention: str = "snake_case"  # none, snake_case, camelCase, PascalCase
    rules: list[NamingRule] = Field(default_factory=list)
    relay_pagination: bool = False  # opt-in: generate _connection fields + Edge/PageInfo types


class Column(BaseModel):
    name: str
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


class ColumnPreset(BaseModel):
    """Auto-set a column value on insert/update from session or built-in source."""

    column: str
    source: str  # "header", "now", "literal"
    name: str | None = None  # header name (for source=header)
    value: str | None = None  # literal value (for source=literal)


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
    schema_name: str = Field(alias="schema")
    table_name: str = Field(alias="table")
    governance: GovernanceLevel
    columns: list[Column]
    column_presets: list[ColumnPreset] = Field(default_factory=list)
    alias: str | None = None  # GraphQL type/field name override
    description: str | None = None  # GraphQL type description
    cache_ttl: int | None = None  # overrides source-level; None = inherit
    naming_convention: str | None = None  # overrides source; None = inherit
    hot: bool | None = None  # None = auto-detect, True = force hot, False = opt out
    relay_pagination: bool | None = None  # None = inherit from source/global NamingConfig
    live: LiveDeliveryConfig | None = None  # live query delivery config (Phase AM)
    watermark_column: str | None = None  # column used by polling subscription provider


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


class Role(BaseModel):
    id: str
    capabilities: list[str]
    domain_access: list[str]
    parent_role_id: str | None = None  # inherit capabilities + domain_access from parent


def flatten_roles(roles: list[Role]) -> list[Role]:
    """Flatten inherited role hierarchy: merge parent capabilities + domain_access.

    Returns new Role objects with inherited permissions merged in.
    """
    by_id = {r.id: r for r in roles}
    cache: dict[str, tuple[set[str], set[str]]] = {}

    def _resolve(role_id: str) -> tuple[set[str], set[str]]:
        if role_id in cache:
            return cache[role_id]
        role = by_id[role_id]
        caps = set(role.capabilities)
        domains = set(role.domain_access)
        if role.parent_role_id and role.parent_role_id in by_id:
            p_caps, p_domains = _resolve(role.parent_role_id)
            caps.update(p_caps)
            domains.update(p_domains)
        cache[role_id] = (caps, domains)
        return caps, domains

    result: list[Role] = []
    for r in roles:
        caps, domains = _resolve(r.id)
        result.append(Role(
            id=r.id,
            capabilities=sorted(caps),
            domain_access=["*"] if "*" in domains else sorted(domains),
            parent_role_id=r.parent_role_id,
        ))
    return result


class RLSRule(BaseModel):
    table_id: str
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
    schema_name: str = Field(alias="schema", default="public")
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
    superuser: dict | None = None
    role_mapping: list[dict] = Field(default_factory=list)
    default_role: str = "analyst"


class OtelConfig(BaseModel):
    """OpenTelemetry tracing configuration.

    endpoint: OTLP gRPC collector address (e.g. http://otel-collector:4317).
    Empty string means spans are generated but silently dropped.
    Overridden at runtime by OTEL_EXPORTER_OTLP_ENDPOINT env var.

    service_name: reported service name. Overridden by OTEL_SERVICE_NAME env var.
    sample_rate: fraction of traces to sample (1.0 = 100%).
    """

    endpoint: str = ""
    service_name: str = "provisa"
    sample_rate: float = 1.0


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


class ProvisaConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    sources: list[Source]
    domains: list[Domain]
    naming: NamingConfig = Field(default_factory=NamingConfig)
    tables: list[Table]
    relationships: list[Relationship] = Field(default_factory=list)
    roles: list[Role]
    rls_rules: list[RLSRule] = Field(default_factory=list)
    event_triggers: list[EventTrigger] = Field(default_factory=list)
    scheduled_triggers: list[ScheduledTrigger] = Field(default_factory=list)
    functions: list[Function] = Field(default_factory=list)
    webhooks: list[Webhook] = Field(default_factory=list)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    hot_tables: HotTablesConfig = Field(default_factory=HotTablesConfig)
    observability: OtelConfig = Field(default_factory=OtelConfig)
