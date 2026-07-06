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
call site as ``execute_trino(state.trino_conn, ...)`` / ``execute_direct(state.source_pools, ...)``.

For the Trino reference engine the ENGINE terminal delegates to the existing ``execute_trino``
(which owns Trino reconnect against ``state.trino_conn_kwargs``) and the DIRECT terminal delegates
to ``execute_direct`` — so behavior is byte-identical to the pre-swap hardcoded path. Swapping the
bound engine (DuckDB/Snowflake) swaps only this terminal dispatch; routing/governance/cache are
unchanged (REQ-840, REQ-841).
"""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import pyarrow as pa

    from provisa.executor.trino import QueryResult
    from provisa.federation.engine import FederationEngine
    from provisa.transpiler.router import RouteDecision


class EngineCapability(str, Enum):  # REQ-825, REQ-840
    """A transport an engine advertises. Consumer-side features gate on these — they are
    federation-engine-specific, not universally available (e.g. Arrow Flight is a Trino feature)."""

    ROWS = "rows"  # row-oriented result (dbapi cursor) — every engine
    ARROW = "arrow"  # materialized columnar Arrow table
    ARROW_STREAM = "arrow_stream"  # lazily-streamed Arrow record batches


# Transport capabilities per reference engine. A designed escape hatch (REQ-825): the contract is
# uniform, but engine-specific transports (Arrow Flight off Trino) are advertised capabilities that
# callers query and route through — never an undesigned reach-around to a raw connection. An engine
# that lacks a transport simply omits it, and require() fails closed.
_ENGINE_CAPABILITIES: dict[str, frozenset[EngineCapability]] = {
    "trino": frozenset(
        {EngineCapability.ROWS, EngineCapability.ARROW, EngineCapability.ARROW_STREAM}
    ),
    "duckdb": frozenset({EngineCapability.ROWS, EngineCapability.ARROW}),
    # pg and sqlalchemy are row-oriented (no Arrow transport); omitted keys default to ROWS.
}


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

    @property
    def name(self) -> str:
        return self.engine.name

    # -- capability introspection (REQ-825): consumer-side features gate on these -----------

    @property
    def capabilities(self) -> frozenset[EngineCapability]:
        return _ENGINE_CAPABILITIES.get(self.engine.name, frozenset({EngineCapability.ROWS}))

    def supports(self, capability: EngineCapability) -> bool:
        return capability in self.capabilities

    def require(self, capability: EngineCapability) -> None:
        """Fail closed when a required transport is not advertised by the bound engine."""
        if capability not in self.capabilities:
            raise UnsupportedCapabilityError(self.engine.name, capability)

    @property
    def native_conn(self) -> Any:
        """The reference-engine connection backing the ENGINE terminal (Trino dbapi conn)."""
        return self._state.trino_conn

    async def execute_engine(
        self,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,
        conn_kwargs: dict | None = None,
        span_attrs: dict[str, str] | None = None,
        extra_table_attrs: list[dict[str, str]] | None = None,
    ) -> QueryResult:
        """ENGINE terminal (REQ-825): execute federated SQL on the bound engine."""
        from provisa.executor.trino import execute_trino

        conn = self._state.trino_conn
        if conn is None and conn_kwargs is None:
            raise RuntimeError(f"engine {self.engine.name!r} connection not available")
        # When conn_kwargs is set, execute_trino reconnects and ignores `conn`; None is safe there.
        _conn = cast("Any", conn)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: execute_trino(
                _conn,
                sql,
                params=params,
                session_hints=session_hints,
                conn_kwargs=conn_kwargs,
                span_attrs=span_attrs,
                extra_table_attrs=extra_table_attrs,
            ),
        )

    async def execute_native(
        self, source_pools: Any, source_id: str, sql: str, params: list | None = None
    ) -> QueryResult:
        """DIRECT terminal (REQ-825): execute on a single reachable source's native driver."""
        from provisa.executor.direct import execute_direct

        return await execute_direct(source_pools, source_id, sql, params)

    # -- engine-native metadata (REQ-825/840): introspection through the abstraction ----------

    def introspect_columns(self, source: Any, schema_name: str, table_name: str) -> dict[str, str]:
        """Column types as the BOUND ENGINE reports them for a registered table — the
        single introspection seam. Every engine answers in its own type system: Trino
        reads its normalized ``information_schema`` over the dbapi conn; DuckDB DESCRIBEs
        the attached source. All engine-specific access (Trino conn, DuckDB attach) lives
        behind this method, so callers never reference a concrete engine. Returns
        ``{column_name: type_name}``; an engine that cannot introspect live returns ``{}``
        (registration then keeps the declared types — introspection only fills nulls)."""
        name = self.engine.name
        if name == "trino":
            conn = self._state.trino_conn
            if conn is None:
                return {}
            from provisa.compiler.introspect import introspect_column_types
            from provisa.compiler.naming import source_to_catalog

            return introspect_column_types(
                conn, source_to_catalog(source.id), schema_name, table_name
            )
        if self.engine.native_store == "duckdb":
            from types import SimpleNamespace

            from provisa.core.secrets import resolve_secrets
            from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

            def _rs(v: Any) -> Any:  # resolve ${env:..}/${secret:..} in connection strings
                return resolve_secrets(v) if isinstance(v, str) else v

            merged = SimpleNamespace(
                id=source.id,
                type=source.type,
                host=_rs(getattr(source, "host", None)),
                port=getattr(source, "port", None),
                database=_rs(getattr(source, "database", None)),
                username=_rs(getattr(source, "username", None)),
                password=_rs(getattr(source, "password", None)),
                path=_rs(getattr(source, "path", None)),
                schema_name=schema_name,
                table_name=table_name,
            )
            import duckdb

            runtime = DuckDBFederationRuntime()
            try:
                return runtime.introspect_columns(merged)
            except duckdb.Error:
                # Engine can't reach the source right now (e.g. offline extension install,
                # source down): keep declared types. Introspection only augments — logged.
                import logging

                logging.getLogger(__name__).warning(
                    "duckdb introspection of %s.%s failed; keeping declared types",
                    schema_name,
                    table_name,
                    exc_info=True,
                )
                return {}
            finally:
                runtime.close()
        return {}

    # -- source lifecycle (REQ-825/840): registration/analyze through the abstraction --------

    def register_source(self, source: Any, resolved_password: str) -> None:
        """Provision a registered source ON THE BOUND ENGINE. Trino creates a dynamic
        catalog; DuckDB/pg/sqlalchemy attach lazily at query time, so this is a no-op
        for them. The only place source→engine provisioning happens — callers never
        touch a Trino connection."""
        if self.engine.name == "trino" and self._state.trino_conn is not None:
            from provisa.core import catalog

            catalog.create_catalog(self._state.trino_conn, source, resolved_password)

    def drop_source(self, source_id: str) -> None:
        """Deprovision a source on the bound engine (Trino: drop catalog; else no-op)."""
        if self.engine.name == "trino" and self._state.trino_conn is not None:
            from provisa.core import catalog

            catalog.drop_catalog(self._state.trino_conn, source_id)

    def analyze(self, source: Any, tables: list) -> None:
        """Refresh engine statistics for a source's tables (Trino: ANALYZE; else no-op —
        DuckDB/pg gather stats implicitly or not at all). Best-effort, behind the seam."""
        if self.engine.name == "trino" and self._state.trino_conn is not None:
            from provisa.core import catalog

            catalog.analyze_source_tables(self._state.trino_conn, source, tables)

    # -- engine-specific transports (REQ-825): designed, capability-gated ENGINE terminals ----

    def _flight_transport(self) -> Any:
        """The engine's Arrow-Flight transport client (Trino → Zaychik Flight SQL proxy).

        Distinct from the dbapi ``trino_conn``: the Flight transport is a separate, engine-specific
        connection. Absent configuration fails closed — the capability is advertised statically but
        unavailable at runtime until the proxy is wired.
        """
        client = self._state.flight_client
        if client is None:
            raise RuntimeError(
                f"engine {self.engine.name!r} Arrow Flight transport is not configured "
                "(set ZAYCHIK_HOST/ZAYCHIK_PORT and ensure the proxy is running)"
            )
        return client

    def execute_engine_arrow(self, sql: str, params: list | None = None) -> pa.Table:
        """ENGINE terminal returning a materialized Arrow table (requires ARROW capability).

        Synchronous: consumers (Flight server, COPY-to) call it from handler threads and off-load
        to executors themselves. The Flight call is blocking (REQ-143, REQ-144).
        """
        self.require(EngineCapability.ARROW)
        from provisa.executor.trino_flight import execute_trino_flight_arrow

        return execute_trino_flight_arrow(self._flight_transport(), sql, params)

    def execute_engine_stream(self, sql: str, params: list | None = None):
        """ENGINE terminal returning ``(schema, RecordBatch generator)`` for lazy streaming.

        Requires the ARROW_STREAM capability. Synchronous: the caller drives the lazy reader, so
        the full result is never materialized (REQ-145).
        """
        self.require(EngineCapability.ARROW_STREAM)
        from provisa.executor.trino_flight import execute_trino_flight_stream

        return execute_trino_flight_stream(self._flight_transport(), sql, params)

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
