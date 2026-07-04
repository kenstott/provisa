# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c60-7b19-4d54-9e02-1c7a0d6f8b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Connector abstraction for the federation engine (REQ-842).

A Connector is indexed by ``(federation_engine, source_type)`` and encapsulates the
engine-specific catalog operations for that source type: it projects a source (asset)
into a persisted engine ``CatalogEntry`` and declares its capability and mechanism.

The mechanism is a fixed property of the connector, not chosen per query (REQ-841):
- ATTACH — reference the source in place, no data movement (Trino catalog, DuckDB ATTACH).
- LAND   — materialize the source into the engine's own reachable store (warehouse-native
           engines land into self; broad/partial federators land where no attach exists).

``CatalogEntry`` is derived, rebuildable engine state (REQ-843) — never a migrated table.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.models import Source


class Mechanism(str, Enum):  # REQ-841
    ATTACH = "attach"
    LAND = "land"


@dataclass(frozen=True)
class Capability:  # REQ-842
    """What a connector's engine can do with a source of this type."""

    predicate_pushdown: bool = False
    join_pushdown: bool = False
    aggregate_pushdown: bool = False


@dataclass(frozen=True)
class CatalogEntry:  # REQ-842, REQ-843
    """A derived engine-catalog row projected from a registry asset.

    ``details`` is engine+source_type specific (Trino .properties, DuckDB ATTACH dsn or
    scanner view DDL, or empty for a warehouse-native land-into-self).
    """

    name: str
    engine: str
    source_type: str
    mechanism: Mechanism
    details: dict = field(default_factory=dict)


class Connector(ABC):  # REQ-842
    """Engine-specific catalog operations for one ``(engine, source_type)`` pair."""

    engine: str
    source_type: str
    mechanism: Mechanism

    @abstractmethod
    def capability(self) -> Capability: ...

    @abstractmethod
    def details(self, source: Source) -> dict:
        """The engine+source_type specific catalog payload for ``source``."""

    def catalog_entry(self, source: Source) -> CatalogEntry:  # REQ-842 catalog_add projection
        """Project a registry asset into its derived engine-catalog entry."""
        return CatalogEntry(
            name=source.id,
            engine=self.engine,
            source_type=self.source_type,
            mechanism=self.mechanism,
            details=self.details(source),
        )


# --- Trino: a broad federator (many source types, all ATTACH via catalogs) ---


class _TrinoAttachConnector(Connector):
    engine = "trino"
    mechanism = Mechanism.ATTACH
    _jdbc_scheme: str = ""

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, join_pushdown=True, aggregate_pushdown=True)

    def details(self, source: Source) -> dict:
        # Trino connector .properties: a JDBC connection-url plus credentials.
        return {
            "connection-url": f"jdbc:{self._jdbc_scheme}://{source.host}:{source.port}/{source.database}",
            "connection-user": source.username,
            "connection-password": source.password,
        }


class TrinoPostgresConnector(_TrinoAttachConnector):
    source_type = "postgresql"
    _jdbc_scheme = "postgresql"


class TrinoMysqlConnector(_TrinoAttachConnector):
    source_type = "mysql"
    _jdbc_scheme = "mysql"


class TrinoSqlServerConnector(_TrinoAttachConnector):
    source_type = "sqlserver"
    _jdbc_scheme = "sqlserver"


# --- DuckDB: a partial federator (postgres via ATTACH; files via scanner views) ---


class DuckDBPostgresConnector(Connector):
    engine = "duckdb"
    source_type = "postgresql"
    mechanism = Mechanism.ATTACH

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        dsn = (
            f"host={source.host} port={source.port} dbname={source.database} user={source.username}"
        )
        return {"attach": f"ATTACH '{dsn}' AS {source.id} (TYPE postgres)"}


class DuckDBCsvConnector(Connector):
    engine = "duckdb"
    source_type = "csv"
    mechanism = Mechanism.ATTACH  # a scanner view references the file in place

    def capability(self) -> Capability:
        return Capability()

    def details(self, source: Source) -> dict:
        return {
            "view_ddl": f"CREATE VIEW {source.id} AS SELECT * FROM read_csv_auto('{source.path}')"
        }


class DuckDBParquetConnector(Connector):
    engine = "duckdb"
    source_type = "parquet"
    mechanism = Mechanism.ATTACH  # a scanner view references the file in place

    def capability(self) -> Capability:
        return Capability(
            predicate_pushdown=True
        )  # parquet supports predicate + projection pushdown

    def details(self, source: Source) -> dict:
        return {
            "view_ddl": f"CREATE VIEW {source.id} AS SELECT * FROM read_parquet('{source.path}')"
        }


class DuckDBSqliteConnector(Connector):
    engine = "duckdb"
    source_type = "sqlite"
    mechanism = Mechanism.ATTACH  # the sqlite extension attaches the file in place

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        return {"attach": f"ATTACH '{source.path}' AS {source.id} (TYPE sqlite)"}


# --- Warehouse-native (Snowflake): self-only, land-into-self is a no-op ---


class WarehouseNativeConnector(Connector):
    """A self-only engine reaches only its own store; the asset is already native."""

    mechanism = Mechanism.LAND

    def __init__(self, engine: str, source_type: str) -> None:
        self.engine = engine
        self.source_type = source_type

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, join_pushdown=True, aggregate_pushdown=True)

    def details(self, source: Source) -> dict:
        return {}  # land-into-self: nothing to attach; the table is already native
