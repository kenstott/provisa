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
from dataclasses import dataclass
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from provisa.federation.connector_base import CatalogEntry, Connector

if TYPE_CHECKING:
    from provisa.core.models import Source
    from provisa.federation.connector_base import Capability


class UnreachableSource(Exception):  # REQ-841
    """Raised when a source has no connector for the selected engine."""

    def __init__(self, engine: str, source_type: str) -> None:
        self.engine = engine
        self.source_type = source_type
        super().__init__(f"engine {engine!r} cannot reach source type {source_type!r}")


class MaterializeStoreUnconfigured(Exception):  # REQ-826
    """No materialization store exists — neither an explicit ``materialize_store_url`` nor a default
    declared by the engine. A store MUST exist; this is a hard error, never a fallback."""

    def __init__(self, engine: str) -> None:
        self.engine = engine
        super().__init__(
            f"engine {engine!r} has no materialization store: set materialize_store_url or use an "
            "engine that declares a default store"
        )


class DriverClass(str, Enum):  # REQ-840
    BROAD = "broad"  # reaches many external source types (Trino)
    PARTIAL = "partial"  # reaches a subset (DuckDB)
    SELF_ONLY = "self_only"  # reaches only its own store (Snowflake)


class UndeclaredTrait(Exception):  # REQ-897
    """A DECLARED capability trait was read where a planner decision needs it, but the engine never
    declared it. Traits are planner INPUTS (REQ-897); a decision on an unset trait is a declaration
    gap, never a silently-guessed default — fail loud (CLAUDE.md: no fallback on a missing value)."""

    def __init__(self, engine: str, trait: str) -> None:
        self.engine = engine
        self.trait = trait
        super().__init__(f"engine {engine!r} did not declare capability trait {trait!r}")


@dataclass(frozen=True)
class EngineTraits:  # REQ-897
    """The DECLARED capability traits of a federation engine — a first-class descriptor the planner
    reads as INPUTS (REQ-897), not inferred from incidental plumbing (connector count / driver class
    alone). Orthogonal dimensions: reach (driver_class), scale (mpp), storage (file_native / pooled /
    transactional), transport (streaming). Connector-level pushdown is per (engine, source_type) and
    lives on connector ``Capability`` (REQ-842) — read via ``FederationEngine.connector_pushdown()``,
    the same Capability promote.should_promote / plan_mask_evaluation read (no duplicate trait)."""

    reach: DriverClass  # reach dimension — BROAD (native federation) / PARTIAL / SELF_ONLY
    mpp: bool  # scale dimension — distributes execution across a cluster
    file_native: bool  # storage — scans file/object sources IN PLACE (no landing)
    pooled: bool  # storage — server-side connection pooling for the engine's own store
    transactional: bool  # storage — the engine's own store supports transactional writes
    streaming: bool  # transport — lazy Arrow record-batch streaming (EngineCapability.ARROW_STREAM)


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
        connectors: Sequence[Connector],
        *,
        native_store: str | None = None,
        driver_class: DriverClass | None = None,
        mpp: bool = False,
        file_native: bool | None = None,
        pooled: bool | None = None,
        transactional: bool | None = None,
        backend_factory: Any = None,
        capabilities: Any = None,
        default_materialize_store: Any = None,
    ) -> None:
        self.name = name
        # A zero-arg callable returning this engine's DECLARED default materialization-store DSN (or
        # None). Set per engine in build_*_engine — the ONE place an engine names its own default.
        self._default_store_fn = default_materialize_store
        # Transports this engine advertises (REQ-825), e.g. Arrow Flight. The engine declares its own
        # capabilities here — the generic seam reads them and never hardcodes a per-engine table.
        self._capabilities = capabilities
        # The engine's concrete terminal implementation (execute/dialect/lifecycle/introspection).
        # A Trino engine attaches TrinoBackend; native engines use the default EngineBackend. This
        # is what keeps every Trino reference inside the Trino engine instance and out of the seam.
        self._backend_factory = backend_factory
        self._backend: Any = None
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
        # DECLARED storage traits (REQ-897) — first-class planner INPUTS, not inferred from plumbing.
        # None ⇒ undeclared: reading one where a decision needs it raises UndeclaredTrait (fail loud).
        # file_native: the engine scans file/object sources in place (a SCAN reach) with no landing.
        # pooled: the engine's own store does server-side connection pooling. transactional: that
        # store supports transactional (atomic) writes. Consolidated with reach/mpp/streaming into the
        # ``traits`` descriptor the planner reads.
        self._file_native = file_native
        self._pooled = pooled
        self._transactional = transactional

    # -- backend (REQ-825/840): the engine's concrete terminal implementation ---

    @property
    def backend(self) -> Any:
        """The engine's backend — built once from its factory (Trino → TrinoBackend) or the default
        native EngineBackend. Every engine-specific terminal (execute/dialect/lifecycle/introspect)
        lives here, so the generic runtime seam never branches on the engine name."""
        if self._backend is None:
            from provisa.federation.backend import EngineBackend

            self._backend = (self._backend_factory or EngineBackend)(self)
        return self._backend

    @property
    def dialect(self) -> str:
        """The physical SQL dialect the engine speaks (transpile target)."""
        return self.backend.dialect

    @property
    def capabilities(self) -> Any:
        """The transports this engine advertises (REQ-825). Defaults to row-oriented only."""
        from provisa.federation.runtime import EngineCapability

        return self._capabilities or frozenset({EngineCapability.ROWS})

    # -- DECLARED capability traits (REQ-897): first-class planner inputs -------

    def _trait(self, value: bool | None, name: str) -> bool:
        """Read a DECLARED trait, failing loud when a decision needs one the engine never set."""
        if value is None:
            raise UndeclaredTrait(self.name, name)
        return value

    @property
    def file_native(self) -> bool:
        """DECLARED: the engine scans file/object sources in place (a SCAN reach), no landing."""
        return self._trait(self._file_native, "file_native")

    @property
    def pooled(self) -> bool:
        """DECLARED: the engine's own store does server-side connection pooling."""
        return self._trait(self._pooled, "pooled")

    @property
    def transactional(self) -> bool:
        """DECLARED: the engine's own store supports transactional (atomic) writes."""
        return self._trait(self._transactional, "transactional")

    @property
    def streaming(self) -> bool:
        """DERIVED transport trait: the engine advertises lazy Arrow record-batch streaming."""
        from provisa.federation.runtime import EngineCapability

        return EngineCapability.ARROW_STREAM in self.capabilities

    @property
    def traits(self) -> EngineTraits:
        """The engine's DECLARED capability descriptor (REQ-897) — the consolidated planner input.
        Fails loud if any storage trait is undeclared (UndeclaredTrait)."""
        return EngineTraits(
            reach=self.driver_class(),
            mpp=self.mpp,
            file_native=self.file_native,
            pooled=self.pooled,
            transactional=self.transactional,
            streaming=self.streaming,
        )

    def connector_pushdown(self, source_type: str) -> Capability:
        """The DECLARED connector-level pushdown capability (predicate/join/aggregate) for a source
        type (REQ-897/842) — the planner's pushdown INPUT. Reconciles with promote.should_promote and
        plan_mask_evaluation, which read the same connector ``Capability``; no duplicate trait is
        introduced. Fails loud (UnreachableSource) when the engine cannot reach the type."""
        return self.connector_for(source_type).capability()

    # -- reachability (REQ-840) ------------------------------------------------

    def reachable(self, source_type: str) -> bool:
        return source_type in self.connectors or self._is_landable(source_type)

    def _is_landable(self, source_type: str) -> bool:
        """A non-attachable remote source (openapi/graphql_remote/grpc/NoSQL/stream) is reachable
        by LANDing into the tenant materialization store — every engine can do this. This mirrors
        strategy.federate()'s ``_MATERIALIZE_ONLY`` gate so catalog projection and query strategy
        agree on which sources land instead of attach (REQ-826/841)."""
        from provisa.federation.strategy import _MATERIALIZE_ONLY

        return source_type in _MATERIALIZE_ONLY

    def complete_reach(self) -> FederationEngine:  # REQ-947
        """Fill ``connectors`` with a land-reach connector for EVERY source type the engine can offer
        that it does not already ATTACH/SCAN live — the Provisa-direct DRIVERS (executor/drivers) and
        the adapter/materialize-only + connector-pgwire-replica FETCH types (source_adapters +
        strategy). After this, ``connectors`` is the complete reach and the source-creation dropdown is
        a PURE PROJECTION of it (no parallel-map union). A live-attach connector always wins — a land
        connector is added only where the engine has none for that type. Idempotent."""
        from provisa.executor.drivers.registry import _DRIVER_FACTORIES
        from provisa.source_adapters.registry import _ADAPTER_MAP
        from provisa.federation.connector import WarehouseNativeConnector
        from provisa.federation.connector_base import Mechanism
        from provisa.federation.strategy import _CONNECTOR_PGWIRE_REPLICA, _MATERIALIZE_ONLY

        direct = frozenset(_DRIVER_FACTORIES)
        fetch = frozenset(_ADAPTER_MAP) | _MATERIALIZE_ONLY | _CONNECTOR_PGWIRE_REPLICA
        for source_type in sorted(direct | fetch):
            if source_type in self.connectors:
                continue  # already reached live (ATTACH/SCAN) — attach wins over a landed replica
            mechanism = Mechanism.DIRECT if source_type in direct else Mechanism.FETCH
            connector = WarehouseNativeConnector(self.name, source_type, mechanism)
            self.connectors[source_type] = connector
            self._candidates.append(
                connector
            )  # survive discover(): land connectors have no dep-probe
        return self

    def connector_for(self, source_type: str) -> Connector:
        connector = self.connectors.get(source_type)
        if connector is not None:
            return connector
        # No attach/scan connector. A non-attachable remote source is not unreachable — it LANDs
        # into the materialization store (the user rule: "remote schemas are all required to be
        # landed"). Hand back a land-into-store connector so resolve() projects a LAND entry.
        if self._is_landable(source_type):
            from provisa.federation.connector import WarehouseNativeConnector

            return WarehouseNativeConnector(self.name, source_type)
        raise UnreachableSource(self.name, source_type)

    # -- materialization store (REQ-826) ---------------------------------------

    @property
    def materialize_stores(self) -> frozenset[str]:
        """The materialized-store backends this engine can USE (REQ-846) — DERIVED from its
        connectors (connectors are what the engine can reach): the source_types of the reachable
        connectors flagged ``materialized_store`` (read-back + a write face to land into). Selecting
        an engine constrains the store choice to this set. Today the proven set is ``{postgresql}``;
        it expands as connectors are flagged once each is validated end-to-end."""
        return frozenset(
            c.source_type
            for c in self.connectors.values()
            if getattr(c, "materialized_store", False)
        )

    def default_materialize_store(self) -> str | None:
        """The store this engine offers ITSELF as its materialization target, absent explicit config —
        the DECLARED default set at construction (never a silent derive-from-whatever). An engine that
        declares none returns None (then a store must be explicitly configured, else error)."""
        return self._default_store_fn() if self._default_store_fn is not None else None

    def materialize_store(self) -> str:
        """The materialization store DSN. A store MUST exist: the explicitly-configured
        ``materialize_store_url`` wins, else the engine's declared default; if neither exists that is
        a hard error (never a fallback to inline / the engine's own runtime)."""
        dsn = configured_materialize_url() or self.default_materialize_store()
        if dsn is None:
            raise MaterializeStoreUnconfigured(self.name)
        return dsn

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
        from provisa.federation.connector_base import ProbeResult

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

        if self.native_store is not None and not any(
            c.reads_in_place for c in self.connectors.values()
        ):
            return DriverClass.SELF_ONLY  # cannot read anything live — all materialized into self
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
    from provisa.federation.backend import TrinoBackend
    from provisa.federation.trino_connectors import build_trino_connectors
    from provisa.federation.runtime import EngineCapability

    return FederationEngine(
        "trino",
        build_trino_connectors(),  # the complete Trino reach — one connector per catalogable type
        driver_class=DriverClass.BROAD,  # many external source types
        mpp=True,  # distributes across a Trino worker cluster
        file_native=True,  # Hive/Iceberg/Delta lakehouse catalogs scan files in place (REQ-897)
        pooled=True,  # coordinator holds server-side pools per catalog
        transactional=False,  # DML is connector-dependent; not a general transactional store
        backend_factory=TrinoBackend,  # the only backend that references Trino
        capabilities=frozenset(
            {EngineCapability.ROWS, EngineCapability.ARROW, EngineCapability.ARROW_STREAM}
        ),
    )


def build_duckdb_engine() -> FederationEngine:  # REQ-840 partial federator
    from provisa.federation.duckdb_backend import DuckDBBackend
    from provisa.federation.runtime import EngineCapability
    from provisa.federation.connector_duckdb import (
        DuckDBAirportConnector,
        DuckDBBigQueryConnector,
        DuckDBCsvConnector,
        DuckDBDeltaConnector,
        DuckDBDuckdbConnector,
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
    from provisa.federation.custom_connectors import load_custom_connectors

    return FederationEngine(
        "duckdb",
        [
            DuckDBPostgresConnector(),
            DuckDBSqliteConnector(),
            DuckDBDuckdbConnector(),  # DuckDB attaches another DuckDB database in place (core)
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
            DuckDBDeltaConnector(),  # core `delta` extension — delta_scan (REQ-899)
            # REQ-1177: operator-declared custom ATTACH/SCAN extensions (config/custom_connectors.yaml).
            *load_custom_connectors("duckdb"),
        ],
        native_store="duckdb",
        driver_class=DriverClass.PARTIAL,
        mpp=False,  # single-node embedded engine (REQ-894)
        file_native=True,  # read_csv/read_parquet/iceberg_scan read files in place (REQ-897)
        pooled=False,  # embedded single-writer connection — no server-side pool
        transactional=True,  # DuckDB is ACID
        backend_factory=DuckDBBackend,  # in-process execution terminal (the engine's own model)
        capabilities=frozenset(
            {
                EngineCapability.ROWS,
                EngineCapability.ARROW,  # fetch_arrow_table
                EngineCapability.ARROW_STREAM,  # lazy record-batch streaming via Flight (REQ-986)
            }
        ),
        # DECLARED default: an embedded DuckDB file — the fully-embedded zero-config store (REQ-989),
        # not the platform tenant DB. An explicit materialize_store_url still overrides it.
        default_materialize_store=_embedded_duckdb_materialize_default,
    )


def _embedded_duckdb_materialize_default() -> str | None:  # REQ-989
    """The DECLARED default materialization store for the zero-config embedded stack: an embedded
    DuckDB file under the data dir (``$PROVISA_DATA_DIR`` else ``~/.provisa``). Fully in-process, no
    external database — the DuckDB engine attaches it and lands into it through its own connection
    (DuckDB is single-writer). Explicit ``materialize_store_url`` still overrides it."""
    import os
    from pathlib import Path

    data_dir = Path(os.environ.get("PROVISA_DATA_DIR") or (Path.home() / ".provisa"))
    data_dir.mkdir(parents=True, exist_ok=True)
    return f"duckdb:///{data_dir / 'materialize.duckdb'}"


def _platform_db_materialize_default() -> str | None:
    """The DECLARED default materialization store for an ephemeral in-process engine: the platform's
    own tenant database (``TENANT_DATABASE_URL``) — the persistent store the platform always
    provisions. Explicit ``materialize_store_url`` still overrides it. Normalized to a driver-agnostic
    DSN (the SQLAlchemy ``+driver`` suffix stripped) for the store attach / asyncpg land. None when the
    platform DB is unset — then a store must be configured explicitly, else a hard error."""
    import os

    url = os.environ.get("TENANT_DATABASE_URL")
    if not url:
        return None
    scheme, sep, rest = url.partition("://")
    return f"{scheme.split('+', 1)[0]}://{rest}" if sep else None


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
    from provisa.federation.connector_duckdb import (
        FileFdwConnector,
        MysqlFdwConnector,
        OracleFdwConnector,
        PgDuckdbCsvConnector,
        PgDuckdbDeltaConnector,
        PgDuckdbIcebergConnector,
        PgDuckdbJsonConnector,
        PgDuckdbParquetConnector,
        PostgresFdwConnector,
        SqliteFdwConnector,
        TdsFdwConnector,
    )
    from provisa.federation.custom_connectors import load_custom_connectors
    from provisa.federation.pg_backend import PgBackend

    return FederationEngine(
        name,
        [
            PostgresFdwConnector(),  # postgresql
            PgDuckdbCsvConnector(),  # csv (preferred over file_fdw)
            FileFdwConnector(),  # csv (fallback)
            PgDuckdbParquetConnector(),  # parquet
            PgDuckdbJsonConnector(),  # json
            PgDuckdbIcebergConnector(),  # iceberg (DuckDB iceberg ext; probe verifies it's compiled in)
            PgDuckdbDeltaConnector(),  # delta_lake (DuckDB delta ext; probe verifies it's compiled in)
            SqliteFdwConnector(),  # sqlite (system libsqlite3)
            MysqlFdwConnector(),  # mysql (needs a bundled client lib; probe-gated)
            TdsFdwConnector(),  # sqlserver via tds_fdw (bundled freetds; probe-gated)
            OracleFdwConnector(),  # oracle via oracle_fdw (operator-supplied Instant Client; probe-gated)
            # REQ-1177: operator-declared custom FDWs (config/custom_connectors.yaml) — probe-gated last.
            *load_custom_connectors("postgres"),
        ],
        native_store="postgres",  # its own tables are native; attached sources reference in place
        driver_class=DriverClass.PARTIAL,
        mpp=False,  # single-node: cross-server joins materialize locally (REQ-894)
        file_native=True,  # pg_duckdb / file_fdw scan csv/parquet/iceberg in place (REQ-897)
        pooled=True,  # server-side connection pooling
        transactional=True,  # PostgreSQL is ACID
        backend_factory=PgBackend,  # in-process terminal driving PgFederationRuntime
        default_materialize_store=_platform_db_materialize_default,
    )


def build_clickhouse_engine() -> FederationEngine:  # REQ-909 OLAP partial federator
    from provisa.federation.runtime import EngineCapability

    """A ClickHouse engine that ATTACHes external sources via native integration engines.

    Relational sources mount as a DATABASE engine (PostgreSQL/MySQL) that auto-exposes every remote
    table; file sources (csv/parquet) and MongoDB mount as a per-table TABLE engine. Everything is
    referenced in place (Mechanism.ATTACH_RW) — nothing lands. ClickHouse is its own native store, so a
    source of type ``clickhouse`` is already native. Single-node reach model like DuckDB/Postgres.
    """
    from provisa.federation.clickhouse_connectors import (
        ClickHouseCsvConnector,
        ClickHouseDeltaLakeConnector,
        ClickHouseHudiConnector,
        ClickHouseIcebergConnector,
        ClickHouseMongoConnector,
        ClickHouseMysqlConnector,
        ClickHouseParquetConnector,
        ClickHousePostgresConnector,
        ClickHouseSqliteConnector,
    )
    from provisa.federation.clickhouse_backend import ClickHouseBackend
    from provisa.federation.custom_connectors import load_custom_connectors

    # ATTACH connectors reach external sources in place via ClickHouse's native integration/table
    # engines (zero-copy); every other readable source lands. Reach is derived, not a fixed list.
    return FederationEngine(
        "clickhouse",
        _warehouse_connectors(
            "clickhouse",
            attach=[
                ClickHousePostgresConnector(),  # postgresql — CREATE DATABASE ENGINE=PostgreSQL
                ClickHouseMysqlConnector(),  # mysql — CREATE DATABASE ENGINE=MySQL
                ClickHouseSqliteConnector(),  # sqlite — CREATE DATABASE ENGINE=SQLite (file, REQ-1178)
                ClickHouseMongoConnector(),  # mongodb — MongoDB table engine (columns from registry)
                ClickHouseCsvConnector(),  # csv — S3/URL/File engine by path scheme
                ClickHouseParquetConnector(),  # parquet — S3/URL/File engine by path scheme
                ClickHouseIcebergConnector(),  # iceberg — IcebergS3 lakehouse engine (zero-copy)
                ClickHouseDeltaLakeConnector(),  # delta_lake — DeltaLake lakehouse engine (zero-copy)
                ClickHouseHudiConnector(),  # hudi — Hudi lakehouse engine (zero-copy, REQ-1178)
                # Config-declared ClickHouse connectors (JDBC/ODBC bridge, Redis, HDFS, URL, …) — REQ-1178
                *load_custom_connectors("clickhouse"),
            ],
        ),
        native_store="clickhouse",  # its own tables are native; attached sources reference in place
        driver_class=DriverClass.PARTIAL,
        mpp=True,  # ClickHouse distributes across shards/replicas
        file_native=True,  # S3/URL/File + Iceberg/DeltaLake table engines scan in place (REQ-897)
        pooled=True,  # server-side connection handling
        transactional=False,  # OLAP store — no general multi-statement transactions
        backend_factory=ClickHouseBackend,  # in-process terminal driving ClickHouseFederationRuntime
        default_materialize_store=_platform_db_materialize_default,
        capabilities=frozenset(
            {EngineCapability.ROWS, EngineCapability.ARROW, EngineCapability.ARROW_STREAM}
        ),  # query_arrow + query_arrow_stream over HTTP / chdb ArrowStream (REQ-909, REQ-986)
    )


def _warehouse_connectors(name: str, *, attach: Sequence[Connector] = ()) -> list[Connector]:
    """A warehouse engine's FULL connector set (REQ-897): the explicit ATTACH connectors it links in
    place, PLUS a land connector for EVERY OTHER readable source type. The land reach is DERIVED from
    what Provisa can actually read — its direct drivers/adapters, the materialize-only feeds, and the
    pgwire-replica types — never a curated demo subset: any readable source the engine cannot attach
    is materialized into the warehouse, and each connector's declared mechanism drives strategy
    (ATTACH_*→SCAN/VIRTUAL zero-copy, DIRECT/FETCH→LAND). The engine's own native store type is
    excluded (a source already in the warehouse is native, not landed)."""
    from provisa.federation.connector import WarehouseNativeConnector
    from provisa.federation.strategy import _CONNECTOR_PGWIRE_REPLICA, _MATERIALIZE_ONLY

    attach_types = {c.source_type for c in attach}
    readable = _provisa_direct_types() | set(_MATERIALIZE_ONLY) | set(_CONNECTOR_PGWIRE_REPLICA)
    land = sorted(readable - attach_types - {name})
    return list(attach) + [WarehouseNativeConnector(name, t) for t in land]


def build_sqlalchemy_engine(  # REQ-905: any SQLAlchemy-reachable store, zero connectors
    url: str | None = None, name: str = "sqlalchemy"
) -> FederationEngine:
    """A self-only engine defined SOLELY by a SQLAlchemy URL — zero federation
    connectors. Every source LANDs into the target store and is federated with plain
    SQL, so ANY SQLAlchemy-reachable database (Postgres, MySQL, Oracle, SQL Server,
    ClickHouse, ...) is a usable engine with no per-source connector. The URL comes
    from the arg or ``$PROVISA_ENGINE_URL``; its scheme names the native store."""
    from provisa.federation.sqlalchemy_backend import SqlAlchemyBackend

    dsn = url or configured_engine_url()
    if not dsn:
        raise ValueError("sqlalchemy engine requires a URL ($PROVISA_ENGINE_URL)")
    backend = dsn.split("://", 1)[0].split("+", 1)[0]  # postgresql+psycopg2 -> postgresql
    return FederationEngine(
        name,
        _warehouse_connectors(name),  # lands every readable source into the store (no attach)
        native_store=backend,
        driver_class=DriverClass.SELF_ONLY,  # reaches only its own store; everything lands in
        mpp=False,
        file_native=False,  # no file scanner — every source lands into the store (REQ-897)
        pooled=True,  # SQLAlchemy engine holds a server-side connection pool
        transactional=True,  # a generic RDB store is transactional
        backend_factory=SqlAlchemyBackend,  # in-process terminal driving SqlAlchemyFederationRuntime
        default_materialize_store=_platform_db_materialize_default,
    )


def build_snowflake_engine() -> FederationEngine:  # REQ-988 self-only MPP warehouse
    """Snowflake as a first-class engine (not a source reached via Trino). A self-only warehouse:
    every source LANDs into Snowflake; governed SQL runs against it in the Snowflake dialect, with
    Arrow-native read transport (fetch_arrow_all / fetch_arrow_batches) surfaced through Arrow Flight."""
    from provisa.federation.runtime import EngineCapability
    from provisa.federation.snowflake_backend import SnowflakeBackend
    from provisa.federation.snowflake_connectors import snowflake_object_link_connectors

    return FederationEngine(
        "snowflake",
        # ATTACH connectors link object/lake sources on cloud storage in place (zero-copy SCAN via an
        # external stage + external table); every other readable source lands. Reach is derived.
        _warehouse_connectors("snowflake", attach=snowflake_object_link_connectors()),
        native_store="snowflake",
        driver_class=DriverClass.PARTIAL,  # attaches cloud object/lake sources live + lands the rest
        mpp=True,  # Snowflake distributes across virtual-warehouse compute
        file_native=True,  # external tables/stages scan cloud object/lake sources in place (REQ-897)
        pooled=True,  # server-side connection pooling
        transactional=True,  # Snowflake is ACID
        backend_factory=SnowflakeBackend,
        capabilities=frozenset(
            {
                EngineCapability.ROWS,
                EngineCapability.ARROW,  # fetch_arrow_all
                EngineCapability.ARROW_STREAM,  # fetch_arrow_batches (lazy) via Flight (REQ-988)
            }
        ),
        default_materialize_store=_platform_db_materialize_default,
    )


def build_databricks_engine() -> FederationEngine:  # REQ-987 self-only MPP warehouse
    """Databricks SQL warehouse as a first-class engine (not a source reached via Trino). A self-only
    warehouse: every source LANDs into Databricks; governed SQL runs against it in the Databricks
    dialect, with Arrow-native read transport (Cloud Fetch) surfaced through Arrow Flight."""
    from provisa.federation.databricks_backend import DatabricksBackend
    from provisa.federation.databricks_connectors import databricks_object_link_connectors
    from provisa.federation.runtime import EngineCapability

    return FederationEngine(
        "databricks",
        # ATTACH connectors link object/lake sources on cloud storage in place (zero-copy SCAN via UC
        # external tables); every other readable source lands. Reach is derived, not a demo tuple.
        _warehouse_connectors("databricks", attach=databricks_object_link_connectors()),
        native_store="databricks",
        driver_class=DriverClass.PARTIAL,  # attaches cloud object/lake sources live + lands the rest
        mpp=True,  # Databricks distributes across its SQL-warehouse cluster
        file_native=True,  # UC external tables scan cloud object/lake sources in place (REQ-897)
        pooled=True,  # SQL-warehouse holds server-side pools
        transactional=True,  # Delta Lake ACID
        backend_factory=DatabricksBackend,
        capabilities=frozenset(
            {
                EngineCapability.ROWS,
                EngineCapability.ARROW,  # fetchall_arrow (Cloud Fetch)
                EngineCapability.ARROW_STREAM,  # lazy record-batch streaming via Flight (REQ-987)
            }
        ),
        default_materialize_store=_platform_db_materialize_default,
    )


def build_bigquery_engine() -> FederationEngine:  # REQ — BigQuery federation engine
    """BigQuery as a first-class engine. A partial-federator warehouse: object/lake sources on GCS
    (and cross-cloud via BigLake) ATTACH as zero-copy external tables (SCAN); every other readable
    source LANDs into a per-source dataset. Governed SQL runs in the BigQuery dialect with Arrow-native
    reads (Storage Read API) surfaced through Arrow Flight."""
    from provisa.federation.bigquery_backend import BigQueryBackend
    from provisa.federation.bigquery_connectors import bigquery_object_link_connectors
    from provisa.federation.runtime import EngineCapability

    return FederationEngine(
        "bigquery",
        _warehouse_connectors("bigquery", attach=bigquery_object_link_connectors()),
        native_store="bigquery",
        driver_class=DriverClass.PARTIAL,  # attaches cloud object/lake sources live + lands the rest
        mpp=True,  # Dremel distributes across BigQuery slots
        file_native=True,  # external tables (BigLake) scan GCS object/lake sources in place (REQ-897)
        pooled=True,  # BigQuery API multiplexes server-side
        transactional=False,  # analytics warehouse — no general multi-statement transactions
        backend_factory=BigQueryBackend,
        capabilities=frozenset(
            {
                EngineCapability.ROWS,
                EngineCapability.ARROW,  # to_arrow (Storage Read API)
                EngineCapability.ARROW_STREAM,  # to_arrow_iterable — lazy record batches via Flight
            }
        ),
        default_materialize_store=_platform_db_materialize_default,
    )


def _build_mssql_warehouse_engine(name: str) -> FederationEngine:  # Fabric / Synapse
    """Microsoft Fabric Warehouse / Azure Synapse — a T-SQL MPP partial-federator warehouse. Object/
    lake sources on OneLake/ADLS ATTACH as zero-copy views over ``OPENROWSET`` (SCAN); every other
    readable source LANDs into a per-source schema. Governed SQL runs in the T-SQL dialect; reads are
    Arrow (built from the ODBC cursor). Azure AD auth (azure-identity)."""
    from provisa.federation.mssql_warehouse_backend import FabricBackend, SynapseBackend
    from provisa.federation.mssql_warehouse_connectors import openrowset_link_connectors
    from provisa.federation.runtime import EngineCapability

    return FederationEngine(
        name,
        _warehouse_connectors(name, attach=openrowset_link_connectors(name)),
        native_store=name,
        driver_class=DriverClass.PARTIAL,  # attaches OneLake/ADLS object/lake sources live + lands the rest
        mpp=True,  # Fabric/Synapse distribute across their compute
        file_native=True,  # OPENROWSET scans OneLake/ADLS object/lake sources in place (REQ-897)
        pooled=True,  # server-side connection pooling over TDS/ODBC
        transactional=(
            name == "fabric"
        ),  # Fabric Warehouse is transactional; Synapse serverless is read-only
        backend_factory=(FabricBackend if name == "fabric" else SynapseBackend),
        capabilities=frozenset(
            {
                EngineCapability.ROWS,
                EngineCapability.ARROW,  # built from the ODBC cursor
                EngineCapability.ARROW_STREAM,
            }
        ),
        default_materialize_store=_platform_db_materialize_default,
    )


def build_fabric_engine() -> FederationEngine:
    return _build_mssql_warehouse_engine("fabric")


def build_synapse_engine() -> FederationEngine:
    return _build_mssql_warehouse_engine("synapse")


# The federation engines. embedded PostgreSQL is NOT a separate engine — it is the ``pg`` engine on a
# bundled instance (its FDW/pg_duckdb connectors are probed by discover). Snowflake and Databricks are
# now first-class self-only warehouse engines (REQ-987/988), not merely sources reached via Trino.
_ENGINE_BUILDERS = {
    "trino": build_trino_engine,  # embedded MPP federator (Provisa-managed Trino cluster)
    "trino-byo": build_trino_engine,  # external Trino coordinator (same runtime; connection only)
    "pg": build_pg_engine,  # PostgreSQL (BYO or embedded) — FDW/pg_duckdb federation
    "duckdb": build_duckdb_engine,  # native in-process partial federator
    "clickhouse": build_clickhouse_engine,  # embedded chdb OLAP federator (REQ-909)
    "clickhouse-server": build_clickhouse_engine,  # external ClickHouse server/cloud (same runtime, URL-driven)
    "snowflake": build_snowflake_engine,  # self-only MPP warehouse, Arrow-native (REQ-988)
    "databricks": build_databricks_engine,  # partial-federator warehouse, Arrow-native (REQ-987)
    "bigquery": build_bigquery_engine,  # partial-federator warehouse, Arrow-native (GCS external links)
    "fabric": build_fabric_engine,  # Microsoft Fabric Warehouse — T-SQL, OneLake OPENROWSET links
    "synapse": build_synapse_engine,  # Azure Synapse serverless SQL — T-SQL, ADLS OPENROWSET links
    "sqlalchemy": build_sqlalchemy_engine,  # any SQLAlchemy URL, zero connectors (self-only)
}


# Selectable-engine registry (REQ-916): metadata + config schema the admin UI renders to pick and
# configure the federation engine. ``config_fields[].config_key`` names the ProvisaConfig field the
# value persists to; the selected engine's own implementation reads it. Applied on service restart.
# Execution-engine tuning for the embedded (Provisa-managed) Trino cluster. These persist as
# top-level config keys and are read by write_trino_config to regenerate the cluster's jvm.config /
# config.properties on restart. A bring-your-own coordinator is unmanaged, so it does NOT expose them.
_TRINO_EXEC_FIELDS: list[dict] = [
    {
        "config_key": "jvm_heap_gb",
        "label": "JVM Heap (GB)",
        "type": "number",
        "required": False,
        "placeholder": "8",
    },
    {
        "config_key": "query_max_memory",
        "label": "Query Max Memory",
        "type": "string",
        "required": False,
        "placeholder": "4GB",
    },
    {
        "config_key": "query_max_memory_per_node",
        "label": "Query Max Memory / Node",
        "type": "string",
        "required": False,
        "placeholder": "2GB",
    },
    {
        "config_key": "query_max_total_memory",
        "label": "Query Max Total Memory",
        "type": "string",
        "required": False,
        "placeholder": "8GB",
    },
    {
        "config_key": "fault_tolerant_execution",
        "label": "Fault-tolerant execution",
        "type": "boolean",
        "required": False,
    },
    {
        "config_key": "fault_tolerant_task_memory",
        "label": "Fault-tolerant Task Memory",
        "type": "string",
        "required": False,
        "placeholder": "1GB",
    },
    {
        "config_key": "exchange_spool_dir",
        "label": "Exchange Spool Directory",
        "type": "string",
        "required": False,
        "placeholder": "/data/provisa/exchange",
    },
    # S3-backed spool for multi-host clusters (a local dir is not shared across hosts). A blank
    # endpoint keeps the local-filesystem spool at exchange_spool_dir.
    {
        "config_key": "exchange_spool_s3_endpoint",
        "label": "Exchange Spool S3 Endpoint",
        "type": "string",
        "required": False,
        "placeholder": "blank = local filesystem spool",
    },
    {
        "config_key": "exchange_spool_bucket",
        "label": "Exchange Spool S3 Bucket",
        "type": "string",
        "required": False,
        "placeholder": "provisa-exchange",
    },
    {
        "config_key": "exchange_spool_s3_region",
        "label": "Exchange Spool S3 Region",
        "type": "string",
        "required": False,
        "placeholder": "us-east-1",
    },
    {
        "config_key": "exchange_spool_s3_access_key",
        "label": "Exchange Spool S3 Access Key",
        "type": "string",
        "required": False,
    },
    {
        "config_key": "exchange_spool_s3_secret_key",
        "label": "Exchange Spool S3 Secret Key",
        "type": "string",
        "required": False,
    },
]

ENGINE_REGISTRY: list[dict] = [
    {
        "key": "trino",
        "label": "Provisa Federation Engine",
        "description": "Embedded distributed MPP engine, managed by Provisa (bundled Trino cluster). Reaches many external source types. Each instance runs as a coordinator or a worker; memory sizing and fault-tolerant execution below regenerate the cluster config and take effect on restart.",
        "config_fields": [
            {
                "config_key": "node_role",
                "label": "Node role",
                "type": "select",
                "required": False,
                "options": [
                    {"value": "coordinator", "label": "Coordinator (schedules + serves queries)"},
                    {"value": "worker", "label": "Worker (executes tasks only)"},
                ],
            },
            *_TRINO_EXEC_FIELDS,
        ],
    },
    {
        "key": "trino-byo",
        "label": "Trino (bring-your-own)",
        "description": "Connect to an external Trino coordinator you operate. Provisa does not manage its process, JVM, or memory — only the connection.",
        "config_fields": [
            {
                "config_key": "federation_engine_host",
                "label": "Coordinator host",
                "type": "string",
                "required": False,
                "placeholder": "localhost",
            },
            {
                "config_key": "federation_engine_port",
                "label": "Coordinator port",
                "type": "number",
                "required": False,
                "placeholder": "8080",
            },
        ],
    },
    {
        "key": "duckdb",
        "label": "DuckDB",
        "description": "In-process partial federator. Reaches postgres/sqlite/files and cloud sources via DuckDB extensions. No external service.",
        "config_fields": [],
    },
    {
        "key": "pg",
        "label": "PostgreSQL",
        "description": "PostgreSQL (embedded or bring-your-own) with FDW / pg_duckdb federation. Leave the URL empty to use the embedded instance.",
        "config_fields": [
            {
                "config_key": "federation_engine_url",
                "label": "PostgreSQL URL",
                "type": "string",
                "required": False,
                "placeholder": "postgresql://user:pass@host:5432/db",
            },
        ],
    },
    {
        "key": "clickhouse",
        "label": "ClickHouse (embedded)",
        "description": "In-process chdb — the ClickHouse engine linked into Provisa. No external service. An optional data directory persists the store; blank keeps it in-memory.",
        "config_fields": [
            {
                "config_key": "federation_engine_url",
                "label": "Data directory (chdb)",
                "type": "string",
                "required": False,
                "placeholder": "chdb:///var/lib/provisa/chdb — blank = in-memory",
            },
        ],
    },
    {
        "key": "clickhouse-server",
        "label": "ClickHouse (Server / Cloud)",
        "description": "OLAP federator against an external ClickHouse server or ClickHouse Cloud, via native integration engines.",
        "config_fields": [
            {
                "config_key": "federation_engine_url",
                "label": "ClickHouse URL",
                "type": "string",
                "required": True,
                "placeholder": "clickhouse://user:pass@host:9000/db",
            },
        ],
    },
    {
        "key": "snowflake",
        "label": "Snowflake",
        "description": "Snowflake as a first-class self-only MPP warehouse with Arrow-native transport. Every source lands into Snowflake; queries run in the Snowflake dialect.",
        "config_fields": [
            {
                "config_key": "federation_engine_url",
                "label": "Snowflake URL",
                "type": "string",
                "required": True,
                "placeholder": "snowflake://user:pass@account/db/schema?warehouse=WH",
            },
        ],
    },
    {
        "key": "databricks",
        "label": "Databricks",
        "description": "Databricks SQL warehouse as a first-class self-only MPP engine with Arrow-native (Cloud Fetch) transport. Every source lands into Databricks; queries run in the Databricks dialect.",
        "config_fields": [
            {
                "config_key": "federation_engine_url",
                "label": "Databricks URL",
                "type": "string",
                "required": True,
                "placeholder": "databricks://token:TOKEN@host?http_path=/sql/1.0/warehouses/xxxx",
            },
        ],
    },
    {
        "key": "bigquery",
        "label": "BigQuery",
        "description": "Google BigQuery as a first-class partial-federator engine with Arrow-native transport (Storage Read API). Object/lake sources on GCS attach as zero-copy external tables; every other source lands into a per-source dataset; queries run in the BigQuery dialect. Auth via a service-account key (GOOGLE_APPLICATION_CREDENTIALS).",
        "config_fields": [
            {
                "config_key": "federation_engine_url",
                "label": "BigQuery URL",
                "type": "string",
                "required": False,
                "placeholder": "bigquery://<project>?location=US (blank = $GOOGLE_CLOUD_PROJECT)",
            },
        ],
    },
    {
        "key": "fabric",
        "label": "Microsoft Fabric",
        "description": "Microsoft Fabric Warehouse as a first-class partial-federator engine (T-SQL over TDS/ODBC, Azure AD auth). Object/lake sources on OneLake attach as zero-copy OPENROWSET views; every other source lands into a per-source schema. Set FABRIC_SQL_SERVER / FABRIC_DATABASE.",
        "config_fields": [
            {
                "config_key": "federation_engine_url",
                "label": "Fabric SQL connection string",
                "type": "string",
                "required": False,
                "placeholder": "fabric://<workspace>.datawarehouse.fabric.microsoft.com/<warehouse> (blank = FABRIC_SQL_SERVER)",
            },
        ],
    },
    {
        "key": "synapse",
        "label": "Azure Synapse",
        "description": "Azure Synapse serverless SQL as a first-class partial-federator engine (T-SQL over TDS/ODBC, Azure AD auth). Object/lake sources on ADLS attach as zero-copy OPENROWSET / external-table views; every other source lands. Set SYNAPSE_SQL_SERVER / SYNAPSE_DATABASE.",
        "config_fields": [
            {
                "config_key": "federation_engine_url",
                "label": "Synapse SQL endpoint",
                "type": "string",
                "required": False,
                "placeholder": "synapse://<workspace>-ondemand.sql.azuresynapse.net/<database> (blank = SYNAPSE_SQL_SERVER)",
            },
        ],
    },
    {
        "key": "sqlalchemy",
        "label": "SQLAlchemy (any RDB)",
        "description": "Any SQLAlchemy-reachable database as a self-only warehouse. Every source lands into the target store.",
        "config_fields": [
            {
                "config_key": "federation_engine_url",
                "label": "SQLAlchemy URL",
                "type": "string",
                "required": True,
                "placeholder": "postgresql+psycopg2://user:pass@host:5432/db",
            },
        ],
    },
]


def _provisa_direct_types() -> frozenset[str]:
    """Source types Provisa reads directly — native drivers (DIRECT) + source adapters (FETCH) —
    and lands into the engine's store. Reachable on ANY engine because Provisa, not the engine,
    obtains the rows, then materializes a refreshed replica the engine reads (REQ-947)."""
    from provisa.executor.drivers.registry import _DRIVER_FACTORIES
    from provisa.source_adapters.registry import _ADAPTER_MAP

    return frozenset(_DRIVER_FACTORIES) | frozenset(_ADAPTER_MAP)


def live_source_types(engine_key: str) -> list[str]:
    """Source types the given engine reads LIVE via a live-attach connector (ATTACH_*): queried in
    place, no replica, always fresh (REQ-947). The dropdown tags these ``LIVE``; everything else
    reachable is a Provisa-landed ``REPLICA``."""
    builder = _ENGINE_BUILDERS.get(engine_key)
    if builder is None:
        return []
    # Building reads only the in-memory connector registry; a URL-driven engine that cannot build
    # without config contributes no attach connectors (its replica reach still applies).
    try:
        engine = builder()
    except ValueError:
        # A URL-driven engine (sqlalchemy) cannot build without config; it contributes no attach
        # connectors, so its live set is empty until configured.
        return []
    # LIVE = read in place with no replica (ATTACH_* or SCAN) — always fresh (REQ-951).
    return sorted(t for t, c in engine.connectors.items() if c.reads_in_place)


def reachable_source_types(engine_key: str) -> list[str]:
    """Every source type CONFIGURABLE on the given engine (REQ-947) — a PURE PROJECTION of the engine's
    COMPLETED connector registry (``complete_reach``): live-attach/scan connectors plus the land-reach
    connectors for every Provisa-direct driver / adapter / materialize-only / pgwire-replica type. This
    drives the source-creation dropdown; types outside the current engine's set are shown disabled with
    the engine(s) that do reach them. No parallel-map union — the registry IS the reach."""
    builder = _ENGINE_BUILDERS.get(engine_key)
    if builder is None:
        return []
    try:
        engine = builder().complete_reach()
    except ValueError:
        # A URL-driven engine (sqlalchemy) cannot build without config, so it contributes no live
        # connectors; its direct/materialize land reach is engine-independent and still applies.
        return sorted(_provisa_direct_land_types())
    return sorted(engine.connectors)


def _provisa_direct_land_types() -> frozenset[str]:
    """The engine-INDEPENDENT land reach — Provisa-direct drivers + adapters + materialize-only +
    pgwire-replica — used when an engine cannot build (unconfigured URL engine) so its dropdown still
    lists what Provisa lands regardless of the engine (REQ-947)."""
    from provisa.federation.strategy import _CONNECTOR_PGWIRE_REPLICA, _MATERIALIZE_ONLY

    return _provisa_direct_types() | _MATERIALIZE_ONLY | _CONNECTOR_PGWIRE_REPLICA


def engine_registry() -> list[dict]:
    """The selectable-engine registry (metadata + config schema + reach faces) for the admin UI.
    Per entry, ``reachable_source_types`` gates the source-creation dropdown to the selected engine
    and ``live_source_types`` distinguishes LIVE (attach) from REPLICA (landed) reach, educating on
    the impact of the engine choice (REQ-947)."""
    return [
        {
            **e,
            "reachable_source_types": reachable_source_types(e["key"]),
            "live_source_types": live_source_types(e["key"]),
        }
        for e in ENGINE_REGISTRY
    ]


def _engine_config() -> dict:
    """The persisted platform config, for engine selection/URL fallback. Empty if unreadable
    (e.g. very early boot before a config file exists)."""
    try:
        from provisa.api.admin._config_io import read_config
    except ImportError:  # api layer not importable at very early boot (module-load ordering)
        return {}
    return read_config() or {}


def configured_engine_url() -> str | None:
    """The engine DSN for sqlalchemy/clickhouse/pg: ``$PROVISA_ENGINE_URL`` then the persisted
    ``federation_engine_url`` config field."""
    import os

    return os.environ.get("PROVISA_ENGINE_URL") or _engine_config().get("federation_engine_url")


def configured_materialize_url() -> str | None:
    """The PostgreSQL DSN where non-attachable sources land for native engines:
    ``$PROVISA_MATERIALIZE_URL`` then the persisted ``materialize_store_url`` config field."""
    import os

    return os.environ.get("PROVISA_MATERIALIZE_URL") or _engine_config().get(
        "materialize_store_url"
    )


def configured_engine_endpoint() -> tuple[str, int]:
    """The engine coordinator host/port (Trino): ``$TRINO_HOST``/``$TRINO_PORT`` env then the
    persisted ``federation_engine_host``/``federation_engine_port`` config fields."""
    import os

    cfg = _engine_config()
    host = os.environ.get("TRINO_HOST") or cfg.get("federation_engine_host") or "localhost"
    port = int(os.environ.get("TRINO_PORT") or cfg.get("federation_engine_port") or 8080)
    return host, port


def build_engine(name: str | None = None) -> FederationEngine:  # REQ-840/893/904/916
    """Select the federation engine by name — the one place the runtime picks an engine. Precedence:
    explicit arg > ``$PROVISA_ENGINE`` env > persisted ``federation_engine`` config > ``duckdb``. The
    zero-config default is the fully-embedded in-process DuckDB engine (REQ-989); external engines
    (trino / pg / clickhouse / sqlalchemy / …) are selected via any of the above."""
    import os

    selected = name or os.environ.get("PROVISA_ENGINE") or _engine_config().get("federation_engine")
    key = (selected or "duckdb").lower().replace("_", "-")
    if key not in _ENGINE_BUILDERS:
        raise ValueError(f"unknown federation engine {key!r}; valid: {sorted(_ENGINE_BUILDERS)}")
    # Complete the reach so the runtime engine's connectors include every configurable type (REQ-947):
    # live-attach plus the Provisa-direct/adapter land connectors. connector_for/federate/reconcile then
    # resolve landable sources from the registry directly rather than synthesizing per call.
    return _ENGINE_BUILDERS[key]().complete_reach()
