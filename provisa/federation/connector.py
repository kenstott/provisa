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


# --- Postgres: a single-node federator that ATTACHes remote sources via postgres_fdw (SQL/MED) ---


class PostgresFdwConnector(Connector):  # REQ-893
    """Attach a remote PostgreSQL source into a Postgres engine via postgres_fdw (SQL/MED).

    A remote source is referenced in place through a foreign server + imported foreign schema — the
    SQL-standard analog of a Trino catalog / DuckDB ATTACH. ``details`` carries the ordered DDL the
    engine issues once to attach the source; per-query the engine just reads the foreign tables.
    """

    engine = "postgres"
    source_type = "postgresql"
    mechanism = Mechanism.ATTACH

    def capability(self) -> Capability:
        # postgres_fdw pushes down predicates, joins between same-server foreign tables, and (PG14+)
        # aggregates; a cross-SERVER join still materializes locally (single-node — REQ-894).
        return Capability(predicate_pushdown=True, join_pushdown=True, aggregate_pushdown=True)

    def details(self, source: Source) -> dict:
        server = f"fdw_{source.id}"
        local_schema = f"fdw_{source.id}"
        remote_schema = getattr(source, "schema", None) or "public"
        return {
            "attach_ddl": [
                "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
                f"CREATE SERVER IF NOT EXISTS {server} FOREIGN DATA WRAPPER postgres_fdw "
                f"OPTIONS (host '{source.host}', port '{source.port}', dbname '{source.database}')",
                f"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER {server} "
                f"OPTIONS (user '{source.username}', password '{source.password}')",
                f"CREATE SCHEMA IF NOT EXISTS {local_schema}",
                f"IMPORT FOREIGN SCHEMA {remote_schema} FROM SERVER {server} INTO {local_schema}",
            ],
            "local_schema": local_schema,
        }


class FileFdwConnector(Connector):  # REQ-893
    """Attach a CSV file into a Postgres engine via file_fdw (a stock/core PG contrib FDW).

    file_fdw needs an explicit column list, so the per-table CREATE FOREIGN TABLE is completed by the
    engine runtime from the registry's column metadata; ``details`` carries the column-independent
    server setup plus the file OPTIONS the foreign table binds.
    """

    engine = "postgres"
    source_type = "csv"
    mechanism = Mechanism.ATTACH

    def capability(self) -> Capability:
        return Capability()  # file_fdw is a plain sequential scan — no pushdown

    def details(self, source: Source) -> dict:
        return {
            "server_ddl": [
                "CREATE EXTENSION IF NOT EXISTS file_fdw",
                "CREATE SERVER IF NOT EXISTS fdw_file_srv FOREIGN DATA WRAPPER file_fdw",
            ],
            "server": "fdw_file_srv",
            "table_options": f"OPTIONS (filename '{source.path}', format 'csv', header 'true')",
        }


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
