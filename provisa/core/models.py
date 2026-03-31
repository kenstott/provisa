# Copyright (c) 2025 Kenneth Stott
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
    postgresql = "postgresql"
    mysql = "mysql"
    sqlserver = "sqlserver"
    duckdb = "duckdb"
    snowflake = "snowflake"
    bigquery = "bigquery"
    mongodb = "mongodb"


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
    "mongodb": "mongodb",
    "duckdb": "memory",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
}

# Map source types to SQLGlot dialect names
SOURCE_TO_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "mysql": "mysql",
    "sqlserver": "tsql",
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

    @property
    def connector(self) -> str:
        return SOURCE_TO_CONNECTOR[self.type.value]

    @property
    def dialect(self) -> str:
        return SOURCE_TO_DIALECT[self.type.value]

    @property
    def catalog_name(self) -> str:
        """Trino catalog name — sanitized source id."""
        return self.id.replace("-", "_")

    def jdbc_url(self) -> str:
        prefix = {
            "postgresql": "jdbc:postgresql",
            "mysql": "jdbc:mysql",
            "sqlserver": "jdbc:sqlserver",
        }
        p = prefix.get(self.type.value)
        if p is None:
            return ""
        if self.type == SourceType.sqlserver:
            return f"{p}://{self.host}:{self.port};databaseName={self.database}"
        return f"{p}://{self.host}:{self.port}/{self.database}"


class Domain(BaseModel):
    id: str
    description: str = ""


class NamingRule(BaseModel):
    pattern: str
    replace: str


class NamingConfig(BaseModel):
    rules: list[NamingRule] = Field(default_factory=list)


class Column(BaseModel):
    name: str
    visible_to: list[str]


class Table(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    source_id: str
    domain_id: str
    schema_name: str = Field(alias="schema")
    table_name: str = Field(alias="table")
    governance: GovernanceLevel
    columns: list[Column]


class Relationship(BaseModel):
    id: str
    source_table_id: str
    target_table_id: str
    source_column: str
    target_column: str
    cardinality: Cardinality


class Role(BaseModel):
    id: str
    capabilities: list[str]
    domain_access: list[str]


class RLSRule(BaseModel):
    table_id: str
    role_id: str
    filter: str


class ProvisaConfig(BaseModel):
    sources: list[Source]
    domains: list[Domain]
    naming: NamingConfig = Field(default_factory=NamingConfig)
    tables: list[Table]
    relationships: list[Relationship] = Field(default_factory=list)
    roles: list[Role]
    rls_rules: list[RLSRule] = Field(default_factory=list)
