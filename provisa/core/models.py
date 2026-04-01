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
    "sqlserver": "sqlserver",
    "oracle": "oracle",
    "mongodb": "mongodb",
    "cassandra": "cassandra",
    "duckdb": "memory",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
}

# Map source types to SQLGlot dialect names
SOURCE_TO_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "mysql": "mysql",
    "sqlserver": "tsql",
    "oracle": "oracle",
    "duckdb": "duckdb",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
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
    rules: list[NamingRule] = Field(default_factory=list)


class MaskingRuleConfig(BaseModel):
    """Per-role masking rule for a column."""

    type: str  # regex, constant, truncate
    pattern: str | None = None
    replace: str | None = None
    value: object = None
    precision: str | None = None

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in ("regex", "constant", "truncate"):
            raise ValueError(f"Invalid masking type: {v!r}")
        return v


class Column(BaseModel):
    name: str
    visible_to: list[str]
    masking: dict[str, MaskingRuleConfig] | None = None  # role_id → rule
    alias: str | None = None  # GraphQL field name override
    description: str | None = None  # GraphQL field description


class Table(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    source_id: str
    domain_id: str
    schema_name: str = Field(alias="schema")
    table_name: str = Field(alias="table")
    governance: GovernanceLevel
    columns: list[Column]
    alias: str | None = None  # GraphQL type/field name override
    description: str | None = None  # GraphQL type description


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


class RLSRule(BaseModel):
    table_id: str
    role_id: str
    filter: str


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
    auth: AuthConfig = Field(default_factory=AuthConfig)
