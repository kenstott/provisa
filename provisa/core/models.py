# Copyright (c) 2025 Kenneth Stott
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
    # NoSQL
    mongodb = "mongodb"
    cassandra = "cassandra"
    redis = "redis"
    kudu = "kudu"
    accumulo = "accumulo"
    # Streaming
    kafka = "kafka"
    # Other
    google_sheets = "google_sheets"
    prometheus = "prometheus"


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
    host: str
    port: int
    database: str
    username: str
    password: str  # Secret reference e.g. ${env:PG_PASSWORD}
    pool_min: int = Field(default=1, alias="pool_min")
    pool_max: int = Field(default=5, alias="pool_max")
    use_pgbouncer: bool = Field(default=False, alias="use_pgbouncer")
    pgbouncer_port: int = Field(default=6432, alias="pgbouncer_port")
    cache_enabled: bool = True
    cache_ttl: int | None = None  # overrides global default; None = inherit
    naming_convention: str | None = None  # overrides global; None = inherit

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


class ColumnPreset(BaseModel):
    """Auto-set a column value on insert/update from session or built-in source."""

    column: str
    source: str  # "header", "now", "literal"
    name: str | None = None  # header name (for source=header)
    value: str | None = None  # literal value (for source=literal)


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


class HotTablesConfig(BaseModel):
    auto_threshold: int = 10_000  # max rows for auto-detection
    refresh_interval: int = 300  # seconds between refreshes


class Relationship(BaseModel):
    id: str
    source_table_id: str
    target_table_id: str
    source_column: str
    target_column: str
    cardinality: Cardinality
    materialize: bool = False  # auto-create MV for cross-source joins
    refresh_interval: int = 300  # MV refresh interval in seconds


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
    """Tracked DB function exposed as a GraphQL mutation."""

    name: str  # exposed mutation name
    source_id: str
    schema_name: str = Field(alias="schema", default="public")
    function_name: str
    returns: str  # registered table id (source_id.schema.table)
    arguments: list[FunctionArgument] = Field(default_factory=list)
    visible_to: list[str] = Field(default_factory=list)
    writable_by: list[str] = Field(default_factory=list)
    domain_id: str = ""
    description: str | None = None

    model_config = ConfigDict(populate_by_name=True)


class Webhook(BaseModel):
    """External HTTP webhook exposed as a GraphQL mutation."""

    name: str  # exposed mutation name
    url: str
    method: str = "POST"
    timeout_ms: int = 5000
    returns: str | None = None  # registered table id, or None for inline type
    inline_return_type: list[InlineType] = Field(default_factory=list)
    arguments: list[FunctionArgument] = Field(default_factory=list)
    visible_to: list[str] = Field(default_factory=list)
    domain_id: str = ""
    description: str | None = None


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


class ProvisaConfig(BaseModel):
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
