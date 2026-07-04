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

    def __init__(self, name: str, connectors: list[Connector]) -> None:
        self.name = name
        self.connectors: dict[str, Connector] = {c.source_type: c for c in connectors}
        self.catalog = EngineCatalog()

    # -- reachability (REQ-840) ------------------------------------------------

    def reachable(self, source_type: str) -> bool:
        return source_type in self.connectors

    def connector_for(self, source_type: str) -> Connector:
        connector = self.connectors.get(source_type)
        if connector is None:
            raise UnreachableSource(self.name, source_type)
        return connector

    def driver_class(self) -> DriverClass:
        """Classify by connector-collection breadth (REQ-840)."""
        from provisa.federation.connector import Mechanism

        if all(c.mechanism is Mechanism.LAND for c in self.connectors.values()):
            return DriverClass.SELF_ONLY
        # More than one distinct reachable source type ⇒ broad; a small set ⇒ partial.
        return (
            DriverClass.BROAD if len(self.connectors) >= _BROAD_THRESHOLD else DriverClass.PARTIAL
        )

    # -- exposure (REQ-841) ----------------------------------------------------

    def resolve(self, source: Source) -> CatalogEntry:
        """Expose a source by its connector's mechanism, or reject it as unreachable."""
        return self.connector_for(source.type.value).catalog_entry(source)

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
    )


def build_duckdb_engine() -> FederationEngine:  # REQ-840 partial federator
    from provisa.federation.connector import DuckDBCsvConnector, DuckDBPostgresConnector

    return FederationEngine("duckdb", [DuckDBPostgresConnector(), DuckDBCsvConnector()])


def build_snowflake_engine() -> FederationEngine:  # REQ-840 self-only warehouse
    from provisa.federation.connector import WarehouseNativeConnector

    return FederationEngine("snowflake", [WarehouseNativeConnector("snowflake", "snowflake")])
