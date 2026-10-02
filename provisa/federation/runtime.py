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

import logging
from contextlib import contextmanager
from enum import Enum
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

    from provisa.executor.result import QueryResult, ResultStream
    from provisa.federation.engine import FederationEngine
    from provisa.federation.execution_auth import ExecutionAuthorization
    from provisa.transpiler.router import RouteDecision

log = logging.getLogger(__name__)


def _strip_driver_suffix(url: str) -> str:
    """A SQLAlchemy-style ``scheme+driver://`` URL, driver-agnostic for asyncpg's ``dsn=`` kwarg
    — the same stripping ``PgBackend._new_runtime`` already does for its own psycopg2 connection."""
    scheme, sep, rest = url.partition("://")
    return f"{scheme.split('+', 1)[0]}://{rest}" if sep else url


def quoted_name(key: tuple[str | None, str, str]) -> str:
    """An engine table name ``(catalog | None, schema, table)`` as a quoted SQL reference."""
    return ".".join('"' + part.replace('"', '""') + '"' for part in key if part is not None)


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
    def selected_key(self) -> str:
        """The engine-registry key this process was booted under — the LIVE selection, which is what
        callers must report rather than the persisted config field (an env pin overrides it)."""
        return self.engine.selected_key

    @property
    def dialect(self) -> str:
        """The physical SQL dialect the bound engine speaks — the transpile target for generic
        callers, so they never hardcode a specific engine's dialect."""
        return self._backend.dialect

    def transpile_physical(self, pg_sql: str) -> str:
        """Transpile governed PostgreSQL-dialect SQL to the bound engine's physical dialect —
        the single seam generic callers use instead of hardcoding a specific engine's dialect.

        REQ-1912: ``pg_sql`` names its tables as the engine addresses them. Each table served
        from its replica is renamed here to its replica's address, before the dialect transpile:
        the one place every statement bound for the engine passes, so no surface addresses a
        replica differently and none reads the source of a table the operator floors."""
        return self._backend.transpile_physical(self.address_replicas(pg_sql))

    def _replica_routes(self) -> Any:
        """The routes published with the registry (``replica_routing.replica_routes``) for the
        bound engine, or None when no table is served from a replica."""
        from provisa.federation.replica_address import ReplicaRoutes

        # The routes are published on the org's runtime with its registry (``_rebuild_schemas``).
        # A state that carries none — an engine runtime built outside the app, with no registry —
        # has no registered table, so none is served from a replica.
        routes = getattr(self._state, "replica_routes", None)
        if routes is None:
            return None
        if not isinstance(routes, ReplicaRoutes):
            raise TypeError(f"replica_routes is a {type(routes).__name__}, not ReplicaRoutes")
        if not routes:
            return None
        if routes.engine_name != self.engine.name:
            raise RuntimeError(
                f"replica routes were published for engine {routes.engine_name!r}; the bound "
                f"engine is {self.engine.name!r}"
            )
        return routes

    def address_replicas(self, pg_sql: str) -> str:
        """``pg_sql`` with each replica-served table at its replica's address (REQ-1912), from the
        routes published with the registry (``replica_routing.replica_routes``)."""
        from provisa.federation.replica_address import address_replicas

        routes = self._replica_routes()
        return pg_sql if routes is None else address_replicas(pg_sql, routes)

    def read_address(
        self, catalog: str | None, schema: str, table: str
    ) -> tuple[str | None, str, str]:
        """Where the bound engine reads the table a statement names ``catalog.schema.table``
        (REQ-1912): its replica's address when it is served from its replica, the name itself
        when it is read live. The same published routes and the same refusals as
        :meth:`address_replicas`, for a statement that is not a query (``ANALYZE``, a catalog
        listing of one table)."""
        from provisa.federation.replica_address import read_address

        routes = self._replica_routes()
        key = (catalog, schema, table)
        return key if routes is None else read_address(key, routes)

    async def registered_key(self, table_name: str) -> tuple[str | None, str, str]:
        """The catalog-physical name the bound engine gives the table registered as
        ``table_name`` (``replica_routing.registered_table_key``): its registered address, before
        the address seam. Refused for an unknown name and for one two sources register."""
        from provisa.federation.replica_routing import registered_table_key

        return await registered_table_key(self.engine, self._state, table_name)

    async def read_ref(self, table_name: str) -> str:
        """The quoted engine name a statement reads the table registered as ``table_name`` by:
        its registered address resolved from the registry, then the address seam — its replica's
        address when it is served from its replica (REQ-1912). For a statement built from
        registered table names rather than lowered by the query pipeline."""
        address = self.read_address(*await self.registered_key(table_name))
        return quoted_name(address)

    def engine_physical(self, pg_sql: str) -> str:
        """Catalog-physical (``"catalog"."schema"."table"``) PostgreSQL-dialect SQL as the bound
        engine executes it: tables in the engine's own addressing, then its dialect.

        REQ-1730: an engine whose SQL cannot express a catalog-qualified reference (``pg`` —
        PostgreSQL has no cross-database queries, ``catalog_qualified=False``) addresses a
        source's table with the catalog folded into the schema name; every other engine keeps
        the three-part name. The one lowering for a caller that names registered tables itself
        rather than through a compiled plan."""
        if not self.engine.catalog_qualified:
            from provisa.compiler.sql_rewrite import fold_catalog_into_schema

            pg_sql = fold_catalog_into_schema(pg_sql)
        return self.transpile_physical(pg_sql)

    def connector_pushdown(self, source_type: str):
        """The bound engine's declared pushdown Capability for ``source_type`` — the same
        passthrough pattern as ``dialect``/``transpile_physical``, so callers reach the engine's
        planner input through the runtime seam rather than needing ``.engine`` themselves."""
        return self.engine.connector_pushdown(source_type)

    @property
    def has_otel_catalog(self) -> bool:
        """Whether the bound engine exposes the ``otel`` telemetry catalog — the seam the ops
        seeding and catalog-name map use instead of branching on the engine name."""
        return self._backend.has_otel_catalog

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
        authorization: "ExecutionAuthorization | None" = None,
    ) -> QueryResult:
        """ENGINE terminal (REQ-825): execute federated SQL on the bound engine.

        ``fresh=True`` requests a private, freshly-reconnected terminal connection instead of the
        shared one (used by concurrent API-cache materialization that must not share a session) —
        the engine supplies its own reconnection parameters, so callers never touch a raw
        connection.

        ``authorization`` (REQ-1760): a GovernedPlanAuth or SystemAuth (provisa.federation.
        execution_auth) naming what authorizes this call. Verified when supplied. Optional for
        now — most of this terminal's ~45 call sites across the codebase predate REQ-1760 and
        are not yet migrated; making it required is a separate, dedicated migration, not bundled
        into this change. A caller with no authorization is logged, not silently normalized as
        trusted, so real usage can be inventoried before that migration."""
        if authorization is not None:
            from provisa.federation.execution_auth import verify_execution_authorization

            verify_execution_authorization(authorization, sql)
        else:
            log.debug(
                "execute_engine called with no authorization (REQ-1760 migration pending): %s",
                sql[:200],
            )
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

    def execute_engine_sync(
        self,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,
    ) -> ResultStream:
        """SYNCHRONOUS ENGINE terminal — for callers already on a worker thread (Arrow
        Flight, pgwire socketserver, API-response materialization, OTEL compaction) that must
        not touch the event loop. Returns a :class:`ResultStream`: the DuckDB engine streams
        lazily (batched cursor), Trino materializes; consumers that call ``.rows`` buffer
        explicitly. ``session_hints`` carries per-plan session properties (e.g. the FTE
        ``retry_policy`` for non-replayable sources); Trino injects them, native engines ignore
        them exactly as their async ``execute`` does."""
        return self._backend.execute_sync(self._state, sql, params, session_hints=session_hints)

    def describe_engine_sync(self, sql: str, params: list | None = None) -> ResultStream | None:
        """The result SHAPE of governed physical SQL — column names and declared types, zero rows —
        without running the statement (the pgwire Describe, REQ-589). ``None`` when this engine
        cannot describe a statement without running it; the caller then runs it (documented)."""
        return self._backend.describe_sync(self._state, sql, params)

    @contextmanager
    def isolated_sync(self):
        """A FRESH, thread-isolated engine connection for background materialization
        (API-response caching) that runs off the event loop and must not share the main
        connection's session across threads. The engine owns provisioning and teardown, so
        callers never open a concrete connection directly."""
        with self._backend.isolated_sync(self._state) as conn:
            yield conn

    async def execute_native(
        self,
        source_pools: Any,
        source_id: str,
        sql: str,
        params: list | None = None,
        span_attrs: dict[str, str] | None = None,
    ) -> QueryResult:
        """DIRECT terminal (REQ-825): execute on a single reachable source's native driver.

        ``span_attrs`` carries the governed plan's OTel attributes so the DIRECT terminal is
        observable on the same footing as ENGINE (REQ-1425).
        """
        from provisa.executor.direct import execute_direct

        return await execute_direct(source_pools, source_id, sql, params, span_attrs)

    def execute_native_stream(
        self,
        source_pools: Any,
        source_id: str,
        sql: str,
        params: list | None,
        *,
        run: Callable[[Coroutine[Any, Any, Any]], Any],
    ) -> ResultStream:
        """DIRECT STREAMING terminal (REQ-1190): a lazily-drained row :class:`ResultStream` over a
        single reachable source's server-side cursor.

        SYNCHRONOUS, for the streaming surfaces (pgwire, Flight SQL) that drive it on a worker thread:
        the async source cursor is opened and pumped through ``run`` — which runs a coroutine to
        completion on the caller's event loop (pgwire/Flight: the connection thread's own loop, on
        that thread, REQ-1882) — one fetch batch at a time, so a large DIRECT scan never fully
        materializes (streaming-uniformity Defect 1). The connection/transaction is held for the
        stream's life and released when the row iterator drains (``on_close``). Only valid when
        ``source_pools.supports_stream(source_id)``."""
        from provisa.executor.direct import open_direct_stream
        from provisa.executor.result import StreamingQueryResult
        from provisa.federation.runtime_support import _STREAM_BATCH_ROWS

        ds = run(open_direct_stream(source_pools, source_id, sql, params))

        released = [False]

        def _release() -> None:
            # Free the eagerly-opened server-side cursor exactly once — invoked by the stream's
            # on_release both on drain-to-exhaustion (via _finish) and on an early schema-only
            # close(). The generator's finally alone is insufficient: a schema probe never iterates
            # it, so its finally never runs.
            if released[0]:
                return
            released[0] = True
            run(ds.close())

        def _batches() -> Any:
            try:
                while True:
                    chunk = run(ds.fetch(_STREAM_BATCH_ROWS))
                    if not chunk:
                        return
                    yield chunk
            finally:
                _release()

        return StreamingQueryResult(
            _batches(),
            column_names=ds.column_names,
            column_types=ds.column_types,
            on_release=_release,
        )

    def execute_pg_passthrough(
        self,
        source_pools: Any,
        source_id: str,
        sql: str,
        params: list | None,
        result_formats: list[int],
        *,
        described_oids: list[int] | None,
    ) -> ResultStream:
        """REQ-1863: like :meth:`execute_native_stream`, but for a DIRECT-route source that is
        itself PostgreSQL — each "row" the returned stream yields is a :class:`RawDataRowBytes`
        (a complete, wire-framed DataRow message from the source, forwarded unmodified) instead of
        a decoded tuple. The read runs on ONE connection borrowed from the source's own pool, on
        the calling thread. ``described_oids`` are the type OIDs the client was told at Describe
        (None when the statement was not described). Only valid when
        ``source_pools.dialect_for(source_id)`` is postgres; callers catch :class:`PassthroughError`
        (the passthrough does not apply) and take :meth:`execute_native_stream`."""
        from provisa.pgwire.pg_passthrough import open_passthrough

        driver = source_pools.get(source_id)
        return self._pg_passthrough_stream(
            open_passthrough(
                driver.borrow_raw, sql, list(params or []), result_formats, described_oids
            )
        )

    def execute_pg_engine_passthrough(
        self,
        sql: str,
        params: list | None,
        result_formats: list[int],
        *,
        described_oids: list[int] | None,
    ) -> ResultStream:
        """Like :meth:`execute_pg_passthrough`, but for the ENGINE route when the bound federation
        engine ITSELF is Postgres (REQ-904, ``PROVISA_ENGINE=pg``) — pgwire and the engine both
        speak real Postgres wire protocol end to end, so the same raw-DataRow-forwarding mechanism
        applies, on one connection borrowed from the engine runtime's own read pool. Callers catch
        :class:`PassthroughError` (the passthrough does not apply) and take the
        ``execute_engine_sync`` decode/re-encode path; a :class:`PassthroughFailure` fails the
        request (REQ-1863)."""
        from provisa.pgwire.pg_passthrough import open_passthrough

        return self._pg_passthrough_stream(
            open_passthrough(
                lambda: self._backend.borrow_raw_pg_connection(self._state),
                sql,
                list(params or []),
                result_formats,
                described_oids,
            )
        )

    def _pg_passthrough_stream(self, pr: Any) -> ResultStream:
        """An opened passthrough's cursor as a lazily-drained :class:`ResultStream` of
        :class:`RawDataRowBytes` batches — shared by the DIRECT and ENGINE passthrough entrypoints."""
        from provisa.executor.result import StreamingQueryResult
        from provisa.federation.runtime_support import _STREAM_BATCH_ROWS
        from buenavista.core import RawDataRowBytes

        def _batches() -> Any:
            try:
                while True:
                    try:
                        chunk = pr.cursor.fetch(_STREAM_BATCH_ROWS)
                    except Exception:
                        # A mid-stream failure reaches the client as its request's error; log it
                        # here too so it is never visible only on the client's side.
                        log.exception("[PASSTHROUGH] mid-stream fetch failed")
                        raise
                    if not chunk:
                        return
                    yield [RawDataRowBytes(msg) for msg in chunk]
            finally:
                pr.cursor.close()

        return StreamingQueryResult(
            _batches(),
            column_names=pr.column_names,
            column_types=pr.column_types,
            on_release=pr.cursor.close,
        )

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

    def introspect_schemas(self, source: Any) -> list[str] | None:
        """REQ-1673: the schemas of a source's database as the bound engine sees them, with no table
        registered yet (a native engine attaches the raw source). ``None`` = no seam on this engine;
        the caller lists through the engine's catalog SQL."""
        return self._backend.introspect_schemas(self._state, source)

    def introspect_tables(self, source: Any, schema_name: str) -> list[str] | None:
        """REQ-1673: the tables of one schema of a source's database (see introspect_schemas)."""
        return self._backend.introspect_tables(self._state, source, schema_name)

    # -- source lifecycle (REQ-825/840): registration/analyze through the abstraction --------

    def register_source(
        self, source: Any, resolved_password: str, catalog_name: str | None = None
    ) -> None:
        """Provision a registered source ON THE BOUND ENGINE (the engine creates a dynamic catalog;
        native engines attach lazily). The only place source→engine provisioning happens.

        ``catalog_name`` (REQ-1266) is the physical catalog name to register under — supplied,
        org-prefixed, for a non-default org so identically-named sources across orgs never collide.
        ``None`` → the engine derives the bare name from ``source.id`` (default-org behavior)."""
        self._backend.register_source(self._state, source, resolved_password, catalog_name)

    def drop_source(self, source_id: str, catalog_name: str | None = None) -> None:
        """Deprovision a source on the bound engine. ``catalog_name`` (REQ-1266): the org-prefixed
        physical catalog to drop; ``None`` → bare name from ``source_id``."""
        self._backend.drop_source(self._state, source_id, catalog_name)

    def analyze(self, source: Any, tables: list, catalog_name: str | None = None) -> None:
        """Refresh engine statistics for a source's tables (best-effort, behind the seam).
        ``catalog_name`` (REQ-1266): the org-prefixed physical catalog; ``None`` → bare."""
        self._backend.analyze(self._state, source, tables, catalog_name)

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

    def materialize_store_target(self, org_id: str) -> tuple[str, str]:
        """The (catalog, schema) an MV materializes into for the bound engine — delegated to the
        backend. Native engines (DuckDB/Databricks/BigQuery) target their attached store; own-store
        engines (Postgres/Trino) the Postgres store default. Never a hardcoded catalog."""
        return self._backend.materialize_store_target(self._state, org_id)

    def provision(self, ops_views: list) -> None:
        """Boot-time: connect the engine terminal and seed the OTel ops store (no-op for native
        engines, whose telemetry lands in the dedicated ops store)."""
        self._backend.provision(self._state, ops_views)

    def connect_terminal(self) -> None:  # REQ-1900
        """Per worker: connect this process to an engine terminal its launch has already
        provisioned (see ``provisa.core.boot_lock``) — the connection without the catalog and
        ops-store seeding ``provision`` does. No-op for native engines."""
        self._backend.connect_terminal(self._state)

    async def reconcile_landed_tables(self) -> list[tuple[str, str]]:
        """Converge the store's landing schema for MATERIALIZED tables and attach their read views
        (REQ-846/932) — the schema-currency controller. Driven at boot and after (re)registration;
        convergent + idempotent. No-op on a broad federator. Returns the reconciled (source, table).

        REQ-1730: followed by ``refresh_landed_views`` — an engine whose reconcile wrote the change
        by dialing its store directly, bypassing the engine's own connection (Trino/store_writer),
        may cache that connector's metadata and need an explicit nudge to see it. No-op on an
        engine (DuckDB) whose reconcile writes through its OWN connection, which already sees its
        own state live. In a ``finally``, not sequenced after: one table's landing_worklist entry
        raising (a stale row, a schema drift the store rejects) must not skip refresh_landed_views
        for every OTHER table already reconciled before it — the two are independent concerns, and
        every caller of this method already treats the whole call as best-effort (REQ-846/932's
        own callers log-and-continue on failure here, never let it abort boot or registration)."""
        from provisa.federation.registry_view import registered_sources
        from provisa.federation.source_vault import org_vault

        try:
            # REQ-1695: the reconcile runs the engine's attach walk, which dials every registered
            # source; it runs at boot and after a registration, outside any statement, so the
            # vault of the org the sources are registered in is bound here.
            async with org_vault(self._state, await registered_sources(self._state)):
                reconciled = await self._backend.reconcile_landed_tables(self._state)
        finally:
            await self._backend.refresh_landed_views(self._state)
        return reconciled

    async def land_source_table(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        change_signal: str = "ttl",
        watermark_column: str | None = None,
        pk_columns: list[str] | None = None,
        match_floor: float = 0.0,
        shape: str | None = None,
    ) -> str:
        """Land a source's ``rows`` into the materialization store through the write face — delegated
        to the backend so an embedded single-connection store (DuckDB, REQ-989) writes through the
        engine's own connection instead of a second connection onto the same file."""
        return await self._backend.land_source_table(
            self._state,
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            change_signal=change_signal,
            watermark_column=watermark_column,
            pk_columns=pk_columns,
            match_floor=match_floor,
            shape=shape,
        )

    def replica_address(self, *, source_id: str, schema_name: str, table_name: str) -> Any:
        """Where the replica of a source table lives in the bound engine's store (REQ-1912) —
        delegated to the backend, for a caller that holds the runtime and not the backend."""
        return self._backend.replica_address(
            self._state, source_id=source_id, schema_name=schema_name, table_name=table_name
        )

    async def analyze_landed_table(self, *, catalog: str | None, schema: str, table: str) -> None:
        """Planner statistics on a landed table, collected where the table lives (REQ-1688) —
        delegated to the backend."""
        await self._backend.analyze_landed_table(
            self._state, catalog=catalog, schema=schema, table=table
        )

    async def apply_cdc_events(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
        events: list,
        row_materialize: bool = False,
        node: str | None = None,
    ) -> dict[str, int]:
        """Apply CDC change events (insert/update -> upsert by PK, delete -> tombstone) to a landed
        table (REQ-1733) — delegated to the backend so an embedded single-connection store (DuckDB,
        REQ-989) writes through the engine's own connection instead of a second connection onto the
        same file.

        REQ-1865: when the target table is ``row_materialize`` (``row_materialize=True``, ``node``
        its registered ``schema.table``), this does NOT call the ordinary upsert-every-event path —
        see design doc section 5. Instead it filters ``events`` to PKs already present in the row
        cache and posts a background ``row_refresh`` work item for exactly those keys; a key not
        already cached is dropped before any fetch is even considered."""
        if row_materialize:
            assert node is not None  # caller-side invariant: row_materialize implies a known node
            from provisa.federation.row_materialize_cdc import handle_row_materialize_cdc

            return await handle_row_materialize_cdc(
                self._state,
                schema=schema,
                table=table,
                pk_columns=pk_columns,
                events=events,
                node=node,
            )
        return await self._backend.apply_cdc_events(
            self._state,
            schema=schema,
            table=table,
            columns=columns,
            pk_columns=pk_columns,
            events=events,
        )

    async def reconcile_mv_table(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str] | None = None,
    ) -> str:
        """Converge an MV's OWN store table to its output ``columns`` (REQ-970) — delegated to the
        backend."""
        return await self._backend.reconcile_mv_table(
            self._state, schema=schema, table=table, columns=columns, pk_columns=pk_columns
        )

    async def reconcile_mv_metadata(
        self, *, schema: str, table: str, pk_columns: list[str] | None = None
    ) -> None:
        """REQ-1652/1654/1655: converge an MV store table's keys, descriptions and tags right after
        the table itself is created or refreshed -- delegated to the backend; a store that holds no
        informational constraints does nothing."""
        await self._backend.reconcile_mv_metadata(
            self._state, schema=schema, table=table, pk_columns=pk_columns
        )

    def mv_store_broker(self) -> Any:
        """The store broker an MV refresh writes through instead of engine SQL, or ``None``
        (REQ-1901) — delegated to the backend."""
        return self._backend.mv_store_broker(self._state)

    async def persist_mv_table(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        persist: str,
        pk_columns: list[str] | None = None,
        match_floor: float = 0.0,
    ) -> str:
        """Land an MV's recomputed ``rows`` into its OWN store table under the declared PERSISTENCE
        outcome (REQ-965) — delegated to the backend."""
        return await self._backend.persist_mv_table(
            self._state,
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            persist=persist,
            pk_columns=pk_columns,
            match_floor=match_floor,
        )

    async def provision_infra(self) -> None:
        """Boot-time engine-terminal infra (Arrow Flight proxy, object store, results schema).
        No-op for a native engine."""
        await self._backend.provision_infra(self._state)

    async def watchdog(self) -> None:
        """Liveness watchdog for the engine terminal (no-op when there is no external process)."""
        await self._backend.watchdog(self._state)

    async def reload_catalog(self, catalog: str, ops_views: list) -> dict:
        """Reload an engine catalog without a restart (native engines have no dynamic catalog)."""
        return await self._backend.reload_catalog(self._state, catalog, ops_views)

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

    def bind_terminal(self) -> None:
        """Lifecycle (REQ-1043/REQ-1244): store the terminal's connection parameters WITHOUT
        connecting, so a sleeping dedicated cluster is woken only by the first real query
        (wake-on-traffic). No-op for in-process engines."""
        self._backend.bind_terminal(self._state)

    def close(self) -> None:
        """Lifecycle: tear down the engine terminal (no-op for native)."""
        self._backend.close(self._state)

    def register_kafka_catalog(self, kafka_source: dict) -> None:
        """Register a Kafka source as an engine catalog (no-op for native engines)."""
        self._backend.register_kafka_catalog(self._state, kafka_source)

    def reseed_ops(self, ops_views: list) -> None:
        """Idempotently re-seed the OTel ops store (self-heal after reconcile); no-op for native."""
        self._backend.reseed_ops(self._state, ops_views)

    def cluster_diagnostics(self) -> tuple[bool, int, int]:
        """Engine health for the admin system-health view: ``(connected, workers, active_workers)``."""
        return self._backend.cluster_diagnostics(self._state)

    def ctas_redirect(self, physical_sql: str, output_format: str, params: list | None) -> dict:
        """Execute a query as CTAS-to-object-store and return the redirect manifest
        (engine-specific). ``params``: the statement's bound values, None when it binds none."""
        return self._backend.ctas_redirect(self._state, physical_sql, output_format, params)

    # -- engine-specific transports (REQ-825): designed, capability-gated ENGINE terminals ----

    def execute_engine_arrow(self, sql: str, params: list | None = None) -> pa.Table:
        """ENGINE terminal returning a materialized Arrow table (requires ARROW capability).

        Synchronous: consumers (Flight server, COPY-to) call it from handler threads and off-load
        to executors themselves. The transport call is blocking (REQ-143, REQ-144).
        """
        self.require(EngineCapability.ARROW)
        return self._backend.execute_arrow(self._state, sql, params)

    def execute_engine_stream(
        self,
        sql: str,
        params: list | None = None,
        *,
        authorization: "ExecutionAuthorization | None" = None,
    ):
        """ENGINE terminal returning ``(schema, RecordBatch generator)`` for lazy streaming.

        Requires the ARROW_STREAM capability. Synchronous: the caller drives the lazy reader, so
        the full result is never materialized (REQ-145). ``authorization`` (REQ-1760) is verified
        when supplied, exactly as on ``execute_engine`` — the system-authorized path (an MV refresh
        streaming its SELECT) proves its identity the same way.
        """
        self.require(EngineCapability.ARROW_STREAM)
        if authorization is not None:
            from provisa.federation.execution_auth import verify_execution_authorization

            verify_execution_authorization(authorization, sql)
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
            return await self.execute_native(
                source_pools, decision.source_id, sql, params, span_attrs
            )
        return await self.execute_engine(
            sql,
            params,
            session_hints=session_hints,
            conn_kwargs=conn_kwargs,
            span_attrs=span_attrs,
            extra_table_attrs=extra_table_attrs,
        )
