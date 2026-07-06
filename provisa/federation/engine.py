# Copyright (c) 2026 Kenneth Stott
# Canary: 8b2d4c71-6a09-4f53-9e12-3c7a0d4f8b61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pluggable federation engine and its derived engine catalog (REQ-840, REQ-841, REQ-843).

A ``FederationEngine`` INSTANCE owns its connector collection keyed by source_type. The
three driver classes are defined purely by the collection's contents (REQ-840):
- broad federator     — many source types (Trino)
- partial federator   — a subset (DuckDB: postgres + file scanners)
- self-only warehouse — only its own store (Snowflake)

Reachability is a lookup, binary and connector-presence-defined (REQ-840):
``reachable(source_type) == source_type in engine.connectors``. Swapping the engine
swaps the connector collection; planner/cache/freshness logic is unchanged.

The engine catalog is DERIVED, rebuildable state (REQ-843): connectors project the asset
registry into it on create/drop and on a full startup reconcile. A missing or stale entry
re-projects from the registry — never a fallback or error-and-continue.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from provisa.federation.connector import CatalogEntry, Connector

if TYPE_CHECKING:
    from provisa.core.models import Source


class UnreachableSource(Exception):  # REQ-841
    """Raised when a source has no connector for the selected engine."""

    def __init__(self, engine: str, source_type: str) -> None:
        self.engine = engine
        self.source_type = source_type
        super().__init__(f"engine {engine!r} cannot reach source type {source_type!r}")


class DriverClass(str, Enum):  # REQ-840
    BROAD = "broad"  # reaches many external source types (Trino)
    PARTIAL = "partial"  # reaches a subset (DuckDB)
    SELF_ONLY = "self_only"  # reaches only its own store (Snowflake)


class EngineCatalog:  # REQ-843
    """Derived, rebuildable engine-catalog state — a name -> CatalogEntry projection."""

    def __init__(self) -> None:
        self._entries: dict[str, CatalogEntry] = {}

    def add(self, entry: CatalogEntry) -> None:
        self._entries[entry.name] = entry

    def remove(self, name: str) -> bool:
        return self._entries.pop(name, None) is not None

    def get(self, name: str) -> CatalogEntry | None:
        return self._entries.get(name)

    def entries(self) -> list[CatalogEntry]:
        return list(self._entries.values())

    def refresh(self, entries: list[CatalogEntry]) -> None:
        """Replace the whole projection (full reconcile)."""
        self._entries = {e.name: e for e in entries}


class FederationEngine:  # REQ-840
    """A federation engine: a named connector collection plus its derived catalog."""

    def __init__(
        self,
        name: str,
        connectors: list[Connector],
        *,
        native_store: str | None = None,
        driver_class: DriverClass | None = None,
        mpp: bool = False,
    ) -> None:
        self.name = name
        # Prebuilt candidate connectors in PRECEDENCE order (first per source_type wins). Optimistic
        # until discover() probes them; connectors is the active set queried by reachable().
        self._candidates: list[Connector] = list(connectors)
        self.connectors: dict[str, Connector] = {}
        for c in self._candidates:
            self.connectors.setdefault(c.source_type, c)
        self.catalog = EngineCatalog()
        # The source_type of the engine's OWN store, into which it materializes natively
        # (DuckDB → "duckdb", Snowflake → "snowflake"); None for a pure federator (Trino).
        self.native_store = native_store
        # An engine DECLARES its class (REQ-894/895). When unset, fall back to the count/mechanism
        # heuristic for ad-hoc engines.
        self._driver_class = driver_class
        # Whether the engine distributes execution across a cluster. ORTHOGONAL to reach/driver_class
        # (REQ-894/895): Snowflake is SELF_ONLY reach yet MPP; DuckDB/Postgres reach several sources
        # yet are single-node. Informational today; a planner input later (e.g. push a large
        # federated join to an MPP engine, or gate the single-node→MPP tier graduation).
        self.mpp = mpp

    # -- reachability (REQ-840) ------------------------------------------------

    def reachable(self, source_type: str) -> bool:
        return source_type in self.connectors

    def connector_for(self, source_type: str) -> Connector:
        connector = self.connectors.get(source_type)
        if connector is None:
            raise UnreachableSource(self.name, source_type)
        return connector

    # -- capability discovery (REQ-904) ----------------------------------------

    async def discover(self, fetch, *, disabled: frozenset[str] = frozenset()) -> dict:
        """Probe candidate connectors against the live engine and set the active connector set.

        A connector STRUCK by the override (its key in ``disabled``) is skipped entirely — not probed.
        Every other candidate is probed; only those whose probe is available become active. For each
        source_type the first available candidate (precedence order) wins, so a richer connector can be
        preferred with a stock one as fallback. ``fetch(sql)`` is an async rows-returning callable.

        Returns a per-connector report {key: ProbeResult}. A source whose connectors are all
        unavailable is simply unreachable — resolve() raises UnreachableSource (explicit, no fallback).
        """
        from provisa.federation.connector import ProbeResult

        report: dict = {}
        active: dict[str, Connector] = {}
        for c in self._candidates:
            key = c.key or c.source_type
            if key in disabled:  # struck from the list -> do not probe
                report[key] = ProbeResult(False, "disabled by override config (not probed)")
                continue
            result = await c.probe(fetch)
            report[key] = result
            if result.available:
                active.setdefault(c.source_type, c)  # first available per type wins (precedence)
        self.connectors = active
        return report

    def driver_class(self) -> DriverClass:
        """The engine's declared class, or the mechanism/count heuristic when undeclared (REQ-840)."""
        if self._driver_class is not None:
            return self._driver_class
        from provisa.federation.connector import Mechanism

        if all(c.mechanism is Mechanism.LAND for c in self.connectors.values()):
            return DriverClass.SELF_ONLY
        # Undeclared ad-hoc engine: infer from breadth. BROAD-by-count is only a fallback — real
        # engines declare their class since BROAD means MPP, which connector count cannot imply.
        return (
            DriverClass.BROAD if len(self.connectors) >= _BROAD_THRESHOLD else DriverClass.PARTIAL
        )

    # -- exposure (REQ-841) ----------------------------------------------------

    def resolve(self, source: Source) -> CatalogEntry:
        """Expose a source by its connector's mechanism, or reject it as unreachable."""
        return self.connector_for(source.type.value).catalog_entry(source)

    def federate(self, source: Source, *, prefer_materialized: bool = False):
        """Resolve this source's federation strategy on this engine (REQ-826)."""
        from provisa.federation.strategy import federate as _federate

        return _federate(source, self, prefer_materialized=prefer_materialized)

    # -- catalog projection / reconcile (REQ-843) ------------------------------

    def on_asset_create(self, source: Source) -> CatalogEntry:
        """Project a newly-registered asset into the engine catalog."""
        entry = self.resolve(source)
        self.catalog.add(entry)
        return entry

    def on_asset_drop(self, name: str) -> None:
        self.catalog.remove(name)

    def reconcile(self, sources: list[Source]) -> list[CatalogEntry]:
        """Rebuild the engine catalog from the registry (REQ-843 full reconcile).

        Only reachable sources project an entry; unreachable ones are omitted (a query
        against them is rejected at resolve time). The registry is the source of truth.
        """
        entries = [self.resolve(s) for s in sources if self.reachable(s.type.value)]
        self.catalog.refresh(entries)
        return entries

    def ensure_entry(self, source: Source) -> CatalogEntry:
        """Return the catalog entry, re-projecting from the registry if missing/stale (REQ-843)."""
        fresh = self.resolve(source)
        current = self.catalog.get(source.id)
        if current != fresh:  # missing or stale → re-project, never fall back
            self.catalog.add(fresh)
        return fresh


_BROAD_THRESHOLD = 3  # a connector collection reaching >= this many source types is "broad"


def build_trino_engine() -> FederationEngine:  # REQ-840 broad federator
    from provisa.federation.connector import (
        TrinoMysqlConnector,
        TrinoPostgresConnector,
        TrinoSqlServerConnector,
    )

    return FederationEngine(
        "trino",
        [TrinoPostgresConnector(), TrinoMysqlConnector(), TrinoSqlServerConnector()],
        driver_class=DriverClass.BROAD,  # many external source types
        mpp=True,  # distributes across a Trino worker cluster
    )


def build_duckdb_engine() -> FederationEngine:  # REQ-840 partial federator
    from provisa.federation.connector import (
        DuckDBAirportConnector,
        DuckDBBigQueryConnector,
        DuckDBCsvConnector,
        DuckDBFirebirdConnector,
        DuckDBGsheetsConnector,
        DuckDBIcebergConnector,
        DuckDBMongoConnector,
        DuckDBMssqlConnector,
        DuckDBParquetConnector,
        DuckDBPostgresConnector,
        DuckDBSnowflakeConnector,
        DuckDBSqliteConnector,
    )

    return FederationEngine(
        "duckdb",
        [
            DuckDBPostgresConnector(),
            DuckDBSqliteConnector(),
            DuckDBCsvConnector(),
            DuckDBParquetConnector(),
            # REQ-899 community-extension connectors: external DB / warehouse / SaaS reach in place.
            DuckDBMssqlConnector(),
            DuckDBMongoConnector(),
            DuckDBSnowflakeConnector(),
            DuckDBBigQueryConnector(),
            DuckDBFirebirdConnector(),
            DuckDBGsheetsConnector(),
            DuckDBAirportConnector(),
            DuckDBIcebergConnector(),  # core `iceberg` extension — iceberg_scan (REQ-899)
        ],
        native_store="duckdb",
        driver_class=DriverClass.PARTIAL,
        mpp=False,  # single-node embedded engine (REQ-894)
    )


def build_pg_engine(name: str = "postgres") -> FederationEngine:  # REQ-904
    """A Postgres-family engine with ALL prebuilt connector defs registered; discover() prunes to what
    actually works.

    No static "is it installed" config: the candidate set is fixed and each connector's probe() reports
    functional truth against the live Postgres (an FDW/extension present but not loaded is disabled).
    Candidates are ordered by PRECEDENCE — pg_duckdb is registered before file_fdw so that, when both
    probe available, pg_duckdb owns ``csv`` (richer scanner) while file_fdw remains the fallback if
    pg_duckdb is unavailable. An operator override may STRIKE connectors from the candidate list by key
    (they are then never probed — see FederationEngine.discover(disabled=...)).
    """
    from provisa.federation.connector import (
        FileFdwConnector,
        MysqlFdwConnector,
        PgDuckdbCsvConnector,
        PgDuckdbIcebergConnector,
        PgDuckdbJsonConnector,
        PgDuckdbParquetConnector,
        PostgresFdwConnector,
        SqliteFdwConnector,
    )

    return FederationEngine(
        name,
        [
            PostgresFdwConnector(),  # postgresql
            PgDuckdbCsvConnector(),  # csv (preferred over file_fdw)
            FileFdwConnector(),  # csv (fallback)
            PgDuckdbParquetConnector(),  # parquet
            PgDuckdbJsonConnector(),  # json
            PgDuckdbIcebergConnector(),  # iceberg (DuckDB iceberg ext; probe verifies it's compiled in)
            SqliteFdwConnector(),  # sqlite (system libsqlite3)
            MysqlFdwConnector(),  # mysql (needs a bundled client lib; probe-gated)
        ],
        native_store="postgres",  # its own tables are native; attached sources reference in place
        driver_class=DriverClass.PARTIAL,
        mpp=False,  # single-node: cross-server joins materialize locally (REQ-894)
    )


# Demo source types LANDed into the sqlalchemy self-only engine (no attach/FDW).
_LAND_TYPES = ("postgresql", "csv", "sqlite", "parquet", "openapi", "graphql_remote")


def build_sqlalchemy_engine(  # REQ-905: any SQLAlchemy-reachable store, zero connectors
    url: str | None = None, name: str = "sqlalchemy"
) -> FederationEngine:
    """A self-only engine defined SOLELY by a SQLAlchemy URL — zero federation
    connectors. Every source LANDs into the target store and is federated with plain
    SQL, so ANY SQLAlchemy-reachable database (Postgres, MySQL, Oracle, SQL Server,
    ClickHouse, ...) is a usable engine with no per-source connector. The URL comes
    from the arg or ``$PROVISA_ENGINE_URL``; its scheme names the native store."""
    import os

    from provisa.federation.connector import WarehouseNativeConnector

    dsn = url or os.environ.get("PROVISA_ENGINE_URL")
    if not dsn:
        raise ValueError("sqlalchemy engine requires a URL ($PROVISA_ENGINE_URL)")
    backend = dsn.split("://", 1)[0].split("+", 1)[0]  # postgresql+psycopg2 -> postgresql
    return FederationEngine(
        name,
        [WarehouseNativeConnector(name, t) for t in _LAND_TYPES],
        native_store=backend,
        driver_class=DriverClass.SELF_ONLY,  # reaches only its own store; everything lands in
        mpp=False,
    )


# The four federation engines. embedded PostgreSQL is NOT a separate engine — it is the
# ``pg`` engine on a bundled instance (its FDW/pg_duckdb connectors are probed by discover).
# Snowflake is a SOURCE reached by an engine's connector, not an engine.
_ENGINE_BUILDERS = {
    "trino": build_trino_engine,  # broad federator (needs a Trino cluster)
    "pg": build_pg_engine,  # PostgreSQL (BYO or embedded) — FDW/pg_duckdb federation
    "duckdb": build_duckdb_engine,  # native in-process partial federator
    "sqlalchemy": build_sqlalchemy_engine,  # any SQLAlchemy URL, zero connectors (self-only)
}


def build_engine(name: str | None = None) -> FederationEngine:  # REQ-840/893/904
    """Select the federation engine by name — the one place the runtime picks an
    engine. Default is ``$PROVISA_ENGINE`` (else trino); the four engines are
    trino / pg / duckdb / sqlalchemy."""
    import os

    key = (name or os.environ.get("PROVISA_ENGINE") or "trino").lower().replace("_", "-")
    if key not in _ENGINE_BUILDERS:
        raise ValueError(f"unknown PROVISA_ENGINE {key!r}; valid: {sorted(_ENGINE_BUILDERS)}")
    return _ENGINE_BUILDERS[key]()
