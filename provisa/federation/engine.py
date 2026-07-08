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
from typing import TYPE_CHECKING, Any

from provisa.federation.connector import CatalogEntry, Connector

if TYPE_CHECKING:
    from provisa.core.models import Source


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
    from provisa.federation.backend import TrinoBackend
    from provisa.federation.connector import (
        TrinoMysqlConnector,
        TrinoPostgresConnector,
        TrinoSqlServerConnector,
    )
    from provisa.federation.runtime import EngineCapability

    return FederationEngine(
        "trino",
        [TrinoPostgresConnector(), TrinoMysqlConnector(), TrinoSqlServerConnector()],
        driver_class=DriverClass.BROAD,  # many external source types
        mpp=True,  # distributes across a Trino worker cluster
        backend_factory=TrinoBackend,  # the only backend that references Trino
        capabilities=frozenset(
            {EngineCapability.ROWS, EngineCapability.ARROW, EngineCapability.ARROW_STREAM}
        ),
    )


def build_duckdb_engine() -> FederationEngine:  # REQ-840 partial federator
    from provisa.federation.duckdb_backend import DuckDBBackend
    from provisa.federation.runtime import EngineCapability
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
        backend_factory=DuckDBBackend,  # in-process execution terminal (the engine's own model)
        capabilities=frozenset({EngineCapability.ROWS, EngineCapability.ARROW}),
        default_materialize_store=_platform_db_materialize_default,  # DECLARED default: platform DB
    )


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
            SqliteFdwConnector(),  # sqlite (system libsqlite3)
            MysqlFdwConnector(),  # mysql (needs a bundled client lib; probe-gated)
        ],
        native_store="postgres",  # its own tables are native; attached sources reference in place
        driver_class=DriverClass.PARTIAL,
        mpp=False,  # single-node: cross-server joins materialize locally (REQ-894)
        backend_factory=PgBackend,  # in-process terminal driving PgFederationRuntime
        default_materialize_store=_platform_db_materialize_default,
    )


def build_clickhouse_engine() -> FederationEngine:  # REQ-909 OLAP partial federator
    from provisa.federation.runtime import EngineCapability

    """A ClickHouse engine that ATTACHes external sources via native integration engines.

    Relational sources mount as a DATABASE engine (PostgreSQL/MySQL) that auto-exposes every remote
    table; file sources (csv/parquet) and MongoDB mount as a per-table TABLE engine. Everything is
    referenced in place (Mechanism.ATTACH) — nothing lands. ClickHouse is its own native store, so a
    source of type ``clickhouse`` is already native. Single-node reach model like DuckDB/Postgres.
    """
    from provisa.federation.connector import (
        ClickHouseCsvConnector,
        ClickHouseMongoConnector,
        ClickHouseMysqlConnector,
        ClickHouseParquetConnector,
        ClickHousePostgresConnector,
    )
    from provisa.federation.clickhouse_backend import ClickHouseBackend

    return FederationEngine(
        "clickhouse",
        [
            ClickHousePostgresConnector(),  # postgresql — CREATE DATABASE ENGINE=PostgreSQL
            ClickHouseMysqlConnector(),  # mysql — CREATE DATABASE ENGINE=MySQL
            ClickHouseMongoConnector(),  # mongodb — MongoDB table engine (columns from registry)
            ClickHouseCsvConnector(),  # csv — S3/URL/File engine by path scheme
            ClickHouseParquetConnector(),  # parquet — S3/URL/File engine by path scheme
        ],
        native_store="clickhouse",  # its own tables are native; attached sources reference in place
        driver_class=DriverClass.PARTIAL,
        mpp=True,  # ClickHouse distributes across shards/replicas
        backend_factory=ClickHouseBackend,  # in-process terminal driving ClickHouseFederationRuntime
        default_materialize_store=_platform_db_materialize_default,
        capabilities=frozenset(
            {EngineCapability.ROWS, EngineCapability.ARROW}
        ),  # query_arrow (REQ-909)
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
    from provisa.federation.connector import WarehouseNativeConnector
    from provisa.federation.sqlalchemy_backend import SqlAlchemyBackend

    dsn = url or configured_engine_url()
    if not dsn:
        raise ValueError("sqlalchemy engine requires a URL ($PROVISA_ENGINE_URL)")
    backend = dsn.split("://", 1)[0].split("+", 1)[0]  # postgresql+psycopg2 -> postgresql
    return FederationEngine(
        name,
        [WarehouseNativeConnector(name, t) for t in _LAND_TYPES],
        native_store=backend,
        driver_class=DriverClass.SELF_ONLY,  # reaches only its own store; everything lands in
        mpp=False,
        backend_factory=SqlAlchemyBackend,  # in-process terminal driving SqlAlchemyFederationRuntime
        default_materialize_store=_platform_db_materialize_default,
    )


# The five federation engines. embedded PostgreSQL is NOT a separate engine — it is the
# ``pg`` engine on a bundled instance (its FDW/pg_duckdb connectors are probed by discover).
# Snowflake is a SOURCE reached by an engine's connector, not an engine.
_ENGINE_BUILDERS = {
    "trino": build_trino_engine,  # embedded MPP federator (Provisa-managed Trino cluster)
    "trino-byo": build_trino_engine,  # external Trino coordinator (same runtime; connection only)
    "pg": build_pg_engine,  # PostgreSQL (BYO or embedded) — FDW/pg_duckdb federation
    "duckdb": build_duckdb_engine,  # native in-process partial federator
    "clickhouse": build_clickhouse_engine,  # embedded chdb OLAP federator (REQ-909)
    "clickhouse-server": build_clickhouse_engine,  # external ClickHouse server/cloud (same runtime, URL-driven)
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


def engine_registry() -> list[dict]:
    """The selectable-engine registry (metadata + config schema) for the admin UI."""
    return ENGINE_REGISTRY


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
    explicit arg > ``$PROVISA_ENGINE`` env > persisted ``federation_engine`` config > ``trino``. The
    engines are trino / pg / duckdb / clickhouse / sqlalchemy."""
    import os

    selected = name or os.environ.get("PROVISA_ENGINE") or _engine_config().get("federation_engine")
    key = (selected or "trino").lower().replace("_", "-")
    if key not in _ENGINE_BUILDERS:
        raise ValueError(f"unknown federation engine {key!r}; valid: {sorted(_ENGINE_BUILDERS)}")
    return _ENGINE_BUILDERS[key]()
