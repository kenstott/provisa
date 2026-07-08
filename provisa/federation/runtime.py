# Copyright (c) 2026 Kenneth Stott
# Canary: 9d3e1a72-5c1a-4e86-9f23-4d8b1e5c0d28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live execution binding for a FederationEngine — the terminal-route dispatch (REQ-825).

The planner (REQ-825) produces an ordered plan whose terminal step is DIRECT (a single
reachable source, executed on its native driver) or ENGINE (hand to the federation engine).
``EngineRuntime`` is where that hand-off actually happens: it binds a ``FederationEngine`` to
its live backend and owns the DIRECT-vs-ENGINE dispatch that was previously duplicated at every
call site as a hardcoded engine execution / ``execute_direct(state.source_pools, ...)``.

The ENGINE terminal delegates to the bound engine's backend (which owns its own connection
and reconnect against ``state.engine_conn_kwargs``) and the DIRECT terminal delegates
to ``execute_direct`` — so behavior is byte-identical to the pre-swap hardcoded path. Swapping the
bound engine (DuckDB/Snowflake) swaps only this terminal dispatch; routing/governance/cache are
unchanged (REQ-840, REQ-841).
"""

from __future__ import annotations

from contextlib import contextmanager
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

    from provisa.executor.result import QueryResult
    from provisa.federation.engine import FederationEngine
    from provisa.transpiler.router import RouteDecision


class EngineCapability(str, Enum):  # REQ-825, REQ-840
    """A transport an engine advertises. Consumer-side features gate on these — they are
    federation-engine-specific, not universally available (e.g. Arrow Flight is the engine feature)."""

    ROWS = "rows"  # row-oriented result (dbapi cursor) — every engine
    ARROW = "arrow"  # materialized columnar Arrow table
    ARROW_STREAM = "arrow_stream"  # lazily-streamed Arrow record batches


class UnsupportedCapabilityError(Exception):  # REQ-825
    """Raised when a consumer-side feature requires a transport the bound engine does not offer."""

    def __init__(self, engine: str, capability: EngineCapability) -> None:
        self.engine = engine
        self.capability = capability
        super().__init__(f"engine {engine!r} does not support transport {capability.value!r}")


class EngineRuntime:  # REQ-825, REQ-840
    """Binds a FederationEngine to AppState and owns terminal-route execution."""

    def __init__(self, engine: FederationEngine, state: Any) -> None:
        self.engine = engine
        self._state = state
        self._backend = engine.backend  # the engine's concrete implementation of every terminal

    @property
    def name(self) -> str:
        return self.engine.name

    @property
    def dialect(self) -> str:
        """The physical SQL dialect the bound engine speaks — the transpile target for generic
        callers, so they never hardcode a specific engine's dialect."""
        return self._backend.dialect

    def transpile_physical(self, pg_sql: str) -> str:
        """Transpile governed PostgreSQL-dialect SQL to the bound engine's physical dialect —
        the single seam generic callers use instead of hardcoding a specific engine's dialect."""
        return self._backend.transpile_physical(pg_sql)

    # -- capability introspection (REQ-825): consumer-side features gate on these -----------

    @property
    def capabilities(self) -> frozenset[EngineCapability]:
        return self.engine.capabilities

    def supports(self, capability: EngineCapability) -> bool:
        return capability in self.capabilities

    def require(self, capability: EngineCapability) -> None:
        """Fail closed when a required transport is not advertised by the bound engine."""
        if capability not in self.capabilities:
            raise UnsupportedCapabilityError(self.engine.name, capability)

    @property
    def native_conn(self) -> Any:
        """The reference-engine connection backing the ENGINE terminal (the engine dbapi conn)."""
        return self._state.engine_conn

    async def execute_engine(
        self,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,
        fresh: bool = False,
        conn_kwargs: dict | None = None,
        span_attrs: dict[str, str] | None = None,
        extra_table_attrs: list[dict[str, str]] | None = None,
    ) -> QueryResult:
        """ENGINE terminal (REQ-825): execute federated SQL on the bound engine.

        ``fresh=True`` requests a private, freshly-reconnected terminal connection instead of the
        shared one (used by concurrent API-cache materialization that must not share a session) —
        the engine supplies its own reconnection parameters, so callers never touch a raw
        connection."""
        return await self._backend.execute(
            self._state,
            sql,
            params,
            session_hints=session_hints,
            fresh=fresh,
            conn_kwargs=conn_kwargs,
            span_attrs=span_attrs,
            extra_table_attrs=extra_table_attrs,
        )

    def execute_engine_sync(self, sql: str, params: list | None = None) -> QueryResult:
        """SYNCHRONOUS ENGINE terminal — for callers already on a worker thread (Arrow
        Flight, API-response materialization, OTEL compaction) that must not touch the
        event loop."""
        return self._backend.execute_sync(self._state, sql, params)

    @contextmanager
    def isolated_sync(self):
        """A FRESH, thread-isolated engine connection for background materialization
        (API-response caching) that runs off the event loop and must not share the main
        connection's session across threads. The engine owns provisioning and teardown, so
        callers never open a concrete connection directly."""
        with self._backend.isolated_sync(self._state) as conn:
            yield conn

    async def execute_native(
        self, source_pools: Any, source_id: str, sql: str, params: list | None = None
    ) -> QueryResult:
        """DIRECT terminal (REQ-825): execute on a single reachable source's native driver."""
        from provisa.executor.direct import execute_direct

        return await execute_direct(source_pools, source_id, sql, params)

    # -- engine-native metadata (REQ-825/840): introspection through the abstraction ----------

    def introspect_by_catalog(self, catalog: str, schema: str, table: str) -> dict[str, str]:
        """Column types keyed by the engine's PHYSICAL catalog name (not source id) — the
        sync introspection seam used by the compile-time type cache."""
        return self._backend.introspect_by_catalog(self._state, catalog, schema, table)

    def introspect_columns(self, source: Any, schema_name: str, table_name: str) -> dict[str, str]:
        """Column types as the BOUND ENGINE reports them for a registered table — the single
        introspection seam. Every engine answers in its own type system, and all engine-specific
        access lives in the engine's backend, so callers never reference a concrete engine. Returns
        ``{column_name: type_name}``; an engine that cannot introspect live returns ``{}``."""
        return self._backend.introspect_columns(self._state, source, schema_name, table_name)

    # -- source lifecycle (REQ-825/840): registration/analyze through the abstraction --------

    def register_source(self, source: Any, resolved_password: str) -> None:
        """Provision a registered source ON THE BOUND ENGINE (the engine creates a dynamic catalog;
        native engines attach lazily). The only place source→engine provisioning happens."""
        self._backend.register_source(self._state, source, resolved_password)

    def drop_source(self, source_id: str) -> None:
        """Deprovision a source on the bound engine."""
        self._backend.drop_source(self._state, source_id)

    def analyze(self, source: Any, tables: list) -> None:
        """Refresh engine statistics for a source's tables (best-effort, behind the seam)."""
        self._backend.analyze(self._state, source, tables)

    # -- engine lifecycle (REQ-825/840): boot / watchdog / reload / readiness through the seam --

    def is_connected(self) -> bool:
        """Whether the bound engine's terminal connection is live. Generic readiness gate — native
        engines run in-process and are always connected."""
        return self._backend.is_connected(self._state)

    def cache_catalog(self) -> str | None:
        """The catalog the API-result cache lives in for the bound engine (``None`` = the source's
        own engine catalog; an ephemeral engine returns its attached materialization-store catalog)."""
        return self._backend.cache_catalog(self._state)

    def materialize_store_dsn(self) -> str:
        """The materialization-store DSN — where landed source data is WRITTEN, through the write
        face (store_writer.land), never through the engine. A store MUST exist (engine invariant);
        this raises if none is configured. The engine only READS the landed replica back."""
        return self.engine.materialize_store()

    def provision(self, ops_views: list, retention_hours: int | None) -> None:
        """Boot-time: connect the engine terminal and seed the OTel ops store (no-op for native
        engines, whose telemetry lands in the dedicated ops store)."""
        self._backend.provision(self._state, ops_views, retention_hours)

    async def reconcile_landed_tables(self) -> list[tuple[str, str]]:
        """Converge the store's landing schema for MATERIALIZED tables and attach their read views
        (REQ-846/932) — the schema-currency controller. Driven at boot and after (re)registration;
        convergent + idempotent. No-op on a broad federator. Returns the reconciled (source, table)."""
        return await self._backend.reconcile_landed_tables(self._state)

    async def provision_infra(self) -> None:
        """Boot-time engine-terminal infra (Arrow Flight proxy, object store, results schema).
        No-op for a native engine."""
        await self._backend.provision_infra(self._state)

    async def watchdog(self) -> None:
        """Liveness watchdog for the engine terminal (no-op when there is no external process)."""
        await self._backend.watchdog(self._state)

    async def reload_catalog(
        self, catalog: str, ops_views: list, retention_hours: int | None
    ) -> dict:
        """Reload an engine catalog without a restart (native engines have no dynamic catalog)."""
        return await self._backend.reload_catalog(self._state, catalog, ops_views, retention_hours)

    def classify_error(self, exc: Exception) -> str | None:
        """Map an engine driver exception to an engine-agnostic category (``"connection"`` → 503,
        ``"query"`` → 400, ``None`` → caller default) so generic request handlers select an HTTP
        status without importing engine-specific exception types."""
        return self._backend.classify_error(exc)

    def write_config(self, config_path: str) -> None:
        """Lifecycle: render the engine's cluster config from platform config (no-op for native)."""
        self._backend.write_config(self._state, config_path)

    def configure_session(self, server_cfg: dict) -> None:
        """Lifecycle: set engine session hints from server config (no-op for native)."""
        self._backend.configure_session(self._state, server_cfg)

    def polling_provider(self, catalog: str, schema: str, table: str, watermark_column: str):
        """A change-data polling provider for the engine, or ``None`` when it offers none."""
        return self._backend.polling_provider(self._state, catalog, schema, table, watermark_column)

    def close(self) -> None:
        """Lifecycle: tear down the engine terminal (no-op for native)."""
        self._backend.close(self._state)

    def register_kafka_catalog(self, kafka_source: dict) -> None:
        """Register a Kafka source as an engine catalog (no-op for native engines)."""
        self._backend.register_kafka_catalog(self._state, kafka_source)

    def reseed_ops(self, ops_views: list, retention_hours: int | None) -> None:
        """Idempotently re-seed the OTel ops store (self-heal after reconcile); no-op for native."""
        self._backend.reseed_ops(self._state, ops_views, retention_hours)

    def cluster_diagnostics(self) -> tuple[bool, int, int]:
        """Engine health for the admin system-health view: ``(connected, workers, active_workers)``."""
        return self._backend.cluster_diagnostics(self._state)

    def ctas_redirect(self, physical_sql: str, output_format: str) -> dict:
        """Execute a query as CTAS-to-object-store and return the redirect manifest (engine-specific)."""
        return self._backend.ctas_redirect(self._state, physical_sql, output_format)

    # -- engine-specific transports (REQ-825): designed, capability-gated ENGINE terminals ----

    def execute_engine_arrow(self, sql: str, params: list | None = None) -> pa.Table:
        """ENGINE terminal returning a materialized Arrow table (requires ARROW capability).

        Synchronous: consumers (Flight server, COPY-to) call it from handler threads and off-load
        to executors themselves. The transport call is blocking (REQ-143, REQ-144).
        """
        self.require(EngineCapability.ARROW)
        return self._backend.execute_arrow(self._state, sql, params)

    def execute_engine_stream(self, sql: str, params: list | None = None):
        """ENGINE terminal returning ``(schema, RecordBatch generator)`` for lazy streaming.

        Requires the ARROW_STREAM capability. Synchronous: the caller drives the lazy reader, so
        the full result is never materialized (REQ-145).
        """
        self.require(EngineCapability.ARROW_STREAM)
        return self._backend.execute_stream(self._state, sql, params)

    async def execute(
        self,
        decision: RouteDecision,
        sql: str,
        params: list | None = None,
        *,
        source_pools: Any,
        session_hints: dict[str, str] | None = None,
        conn_kwargs: dict | None = None,
        span_attrs: dict[str, str] | None = None,
        extra_table_attrs: list[dict[str, str]] | None = None,
    ) -> QueryResult:
        """Dispatch a decided route to its terminal: DIRECT native driver, else ENGINE (REQ-825)."""
        from provisa.transpiler.router import Route

        if (
            decision.route == Route.DIRECT
            and decision.source_id
            and source_pools.has(decision.source_id)
        ):
            return await self.execute_native(source_pools, decision.source_id, sql, params)
        return await self.execute_engine(
            sql,
            params,
            session_hints=session_hints,
            conn_kwargs=conn_kwargs,
            span_attrs=span_attrs,
            extra_table_attrs=extra_table_attrs,
        )
