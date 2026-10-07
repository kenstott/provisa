# Copyright (c) 2026 Kenneth Stott
# Canary: 7af90b07-3f44-46a1-af0a-52965cc3470c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""NativeEngineBackend — the shared in-process execution + materialization-store cache terminal for
every native federation engine (duckdb / clickhouse / pg / sqlalchemy) (REQ-825/840/844).

A native engine holds ONE persistent runtime into which every registered table is exposed, and runs
governed physical SQL against it. API results a source cannot reach live are cached into the engine's
materialization store (attached through the runtime) — never a transient store, never inline-as-
fallback; a missing store is the engine's hard invariant error.

This base owns the entire lifecycle. A subclass provides only its engine-specific runtime via
``_new_runtime()`` (and the driver error type it raises on an unreachable table). The runtime is a
small protocol:

    connection                        -> the underlying DBAPI-ish connection (cache terminal writes)
    run(sql, params) -> QueryResult   -> execute physical SQL (async)
    run_sync(sql, params)             -> the same, synchronous
    ensure_materialize_attached()     -> attach the materialization store; return its catalog alias
    attach_source(source)             -> expose a registered table at its catalog-physical name
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from provisa.federation.backend import EngineBackend
from provisa.federation.engine import UnreachableSource
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.otel_compat import stage as _stage

_tracer = _get_tracer(__name__)

if TYPE_CHECKING:
    from sqlalchemy.engine import URL

    from provisa.executor.result import QueryResult, ResultStream

_log = logging.getLogger(__name__)

# Seeded system sources that name no attachable remote, so attach_source must never see them.
#
# - provisa-admin is the control plane itself; on the native tier its catalog comes from
#   attach_control_plane (below), which owns the whole `provisa_admin` catalog for both the SQLite
#   and Postgres backends. The seeded row carries the control plane's dialect and address but no
#   `path`, so a SQLite deployment attached the literal string "None" as a database file — DuckDB
#   created that file, and every later information_schema scan (which reads every attached catalog)
#   failed with "file is not a database".
# - __derived__ is the virtual-view sentinel: "the sentinel has no address of its own"
#   (api/startup_seed.py). Its rows are compiler-emitted views, never a remote database.
_NO_REMOTE_SOURCE_IDS = frozenset({"provisa-admin", "__derived__"})

# Attach refusals that are the declared state of an (engine, source type) pair — the engine has no
# connector for the type (UnreachableSource, REQ-841). A source whose connector simply has no live
# attach is now decided up front from its reach (``reads_in_place``) and never attempts an attach,
# so a missing details key is a real bug, not a declared refusal. They cannot change until the
# registry does.
_DECLARED_REFUSALS: tuple[type[BaseException], ...] = (UnreachableSource,)


def libpq_dsn(url: "URL") -> str:
    """A SQLAlchemy PostgreSQL URL as the libpq keyword DSN DuckDB's postgres extension takes.

    The embedded control plane of REQ-1535 has no host in the netloc: pgserver listens on a unix
    socket, and the socket directory and the port it names the socket file after are carried in the
    query (``postgresql+psycopg:///provisa?host=/dir&port=54321``) because that is the only place
    asyncpg reads them from. Reading the netloc alone yields ``host=None port=None`` and libpq
    refuses the DSN outright, so both places are read here — the query first, since a URL that
    carries them there is the one that means them.
    """

    def _q(name: str) -> str | None:
        value = url.query.get(name)  # a SQLAlchemy multi-valued query param can be a tuple
        return value[0] if isinstance(value, tuple) else value

    host = _q("host") or url.host
    port = _q("port") or url.port
    if not host:
        raise ValueError(f"control-plane URL names no host to attach: {url.render_as_string()}")
    if not port:
        raise ValueError(f"control-plane URL names no port to attach: {url.render_as_string()}")
    parts = [f"host={host}", f"port={port}", f"dbname={url.database}", f"user={url.username}"]
    if url.password:
        parts.append(f"password={url.password}")
    return " ".join(parts)


def _same_registry(
    walked: tuple[Any, Any, Any, Any] | None, registry: tuple[Any, Any, Any, Any]
) -> bool:
    return walked is not None and all(a is b for a, b in zip(walked, registry, strict=True))


def _file_glob_tables(tables: list[Any]) -> dict[str, list[dict]]:  # REQ-788
    """{source_id: [the file adapter's glob-table spec, ...]} for every registered table that
    declares a ``file_glob`` -- a config Table or a registered-table row, one spec per table."""
    by_source: dict[str, list[dict]] = {}
    for table in tables:
        row = table if isinstance(table, dict) else vars(table)
        if not row.get("file_glob"):
            continue
        specs = by_source.setdefault(row["source_id"], [])
        if all(spec["name"] != row["table_name"] for spec in specs):
            specs.append(
                {
                    "name": row["table_name"],
                    "file_glob": row["file_glob"],
                    "source_file_column": row.get("source_file_column"),
                }
            )
    return by_source


class NativeEngineBackend(EngineBackend):
    """In-process execution terminal shared by all native engines. ``is_connected`` is inherited True
    — a native engine is live once built. Subclasses supply ``_new_runtime`` and, if the runtime
    raises a driver-specific error when a source is unreachable, extend ``_attach_errors``."""

    # Errors from attach_source that mean "this table is not queryable" (offline source, or a LAND
    # source not yet materialized) — logged and skipped so one bad table never fails other queries.
    # A subclass ORs in its driver error type. Anything else is a real bug and propagates.
    #
    # UnreachableSource belongs here: reachability is binary and engine-scoped (REQ-841), so on a
    # PARTIAL/SELF_ONLY engine some registered source types simply have no connector — the seeded
    # provisa-otel iceberg store on the Synapse engine, for example. That is the declared state of
    # that pair, not a failure of the attach loop, and reconcile_landed_tables already skips the
    # same condition the same way (see its `except UnreachableSource: continue`). KeyError is NOT
    # here: a source whose connector has no attach face is decided up front from the connector's
    # reach (`reads_in_place`) and never reaches attach_source, so a missing details key is now a
    # real bug that propagates rather than a swallowed "not queryable" for every API table.
    _attach_errors: tuple[type[BaseException], ...] = (UnreachableSource,)

    # The marker the runtime connection's driver binds a value at, for the API-result cache
    # terminal (``isolated_sync``). None: the engine's subclass declares none, and the cache
    # renders each value as a literal of the engine's dialect instead.
    _cache_bind_placeholder: str | None = None

    def __init__(self, engine: Any) -> None:
        super().__init__(engine)
        self._runtime: Any = None
        # Each table by its identity (catalog, source_id, schema, table): two sources may both hold
        # ``schema.table``, and each has its own attach; and one engine serves several org
        # environments, each attaching the same source under its own catalog (REQ-1266, REQ-1529).
        self._attached: set[tuple[str, str, str, str]] = set()
        # Tables whose live attach this process has removed because their reads moved to the
        # replica (REQ-1912) — removed once, whichever process created it.
        self._detached: set[tuple[str, str, str, str]] = set()
        # The registry state the last complete walk covered: the identities of (config,
        # runtime_sources, tables, model_db). A schema rebuild REPLACES those objects (app.py
        # publishes a new source map and a new table list; nothing mutates them in place), so an
        # unchanged identity means there is nothing new to attach and the walk is skipped.
        self._walked: tuple[Any, Any, Any, Any] | None = None
        # Tables whose attach was refused for a DECLARED reason in the walked registry state (a
        # source type this engine lands instead of attaching, REQ-841). Not retried until the
        # registry changes. A driver error is not remembered: an offline source is retried.
        self._refused: set[tuple[str, str, str, str]] = set()
        self._refused_in: tuple[Any, Any, Any, Any] | None = None
        self._walk_lock = threading.Lock()

    # -- runtime (subclass hook) ----------------------------------------------

    def _new_runtime(self) -> Any:
        """Build this engine's persistent runtime. Native engines that have not wired a runtime yet
        cannot execute — an explicit error, never a silent fallback to another engine."""
        raise NotImplementedError(
            f"engine {self.engine.name!r} has not wired a native runtime (execution/cache terminal)"
        )

    def _runtime_for(self, state: Any) -> Any:
        """The persistent runtime with every registered table attached (idempotent, lazy)."""
        runtime = self._store_runtime()
        self._attach_registered(state)
        return runtime

    def _store_runtime(self) -> Any:
        """The persistent runtime, built if this process has none yet, with no source attached by
        this call: for what is answered from the engine and its store alone."""
        if self._runtime is None:
            with self._walk_lock:
                if self._runtime is None:
                    self._runtime = self._new_runtime()
        return self._runtime

    def _store_catalog(self, state: Any, org_id: str) -> str:
        """The catalog the store is read under, from the runtime's store attach alone (REQ-1912):
        where a replica is read is a property of the engine and its store, so no registered source
        is dialed for it (the attach walk is not run)."""
        del state, org_id  # the store's catalog is the runtime's, whichever org is served
        return self._store_runtime().ensure_materialize_attached()

    def _attach_registered(self, state: Any) -> None:
        """ATTACH every registered table into the runtime: one walk per registry state, not one per
        query. A table whose source cannot be attached (offline, or a LAND source not yet
        materialized) is logged and skipped."""
        config = getattr(state, "config", None)
        if config is None or self._runtime is None:
            return

        # REQ-1266: the native engines share ONE process-wide runtime (self._runtime lives on the
        # global federation engine), and their physical-catalog / raw-attach aliases are derived
        # bare from source.id — they are NOT org-namespaced. Two orgs seeding identical source ids
        # would collide into one attach. Multi-org isolation on the shared coordinator is
        # implemented for the Trino tier (org-prefixed CREATE CATALOG); the native tier is single-
        # org only. Rather than silently serve another org's rows, refuse a non-default org here.
        #
        # REQ-1418: the collision this guards against is SHARING one runtime, not being a non-default
        # org. An org on the isolated/external lane carries its OWN EngineRuntime — hence its own
        # backend instance and its own ``self._runtime`` — so its bare attach aliases live in a
        # namespace nothing else writes to. That org runs a native kind (Databricks, Snowflake,
        # BigQuery, ClickHouse, …) of its own legitimately; ``active_isolated_org`` is exactly the
        # seam that says so.
        from provisa.core.request_context import current_org

        _active_org = current_org.get()
        _owns_engine = getattr(state, "active_isolated_org", None) == _active_org
        _default_org = getattr(state, "org_id", None)
        if _active_org is not None and not _owns_engine and _active_org != _default_org:
            raise RuntimeError(
                f"native federation engine {self.engine.name!r} is single-org; org "
                f"{_active_org!r} requires the Trino tier for per-org catalog isolation (REQ-1266)"
            )

        self._refresh_control_plane_snapshot(state)
        registry = self._registry_of(state)
        if self._covers(registry):
            return
        with self._walk_lock:
            if self._covers(registry):
                return  # a concurrent query walked this registry state while this one waited
            if not _same_registry(self._refused_in, registry):
                self._refused.clear()  # a new registry state: each refused attach gets one retry
                self._refused_in = registry
            self._walked = registry if self._walk_registry(state, config) else None

    def _covers(self, registry: tuple[Any, Any, Any, Any]) -> bool:
        return _same_registry(self._walked, registry)

    @staticmethod
    def _registry_of(state: Any) -> tuple[Any, Any, Any, Any]:
        """The registry state one walk covers: replaced as a whole by a schema rebuild."""
        return (
            getattr(state, "config", None),
            getattr(state, "runtime_sources", None),
            getattr(state, "tables", None),
            getattr(state, "model_db", None),
        )

    async def _attached_runtime(self, state: Any) -> Any:
        """``_runtime_for`` off the event loop (its walk runs blocking attach DDL, REQ-1882).

        REQ-1695: a pending walk dials every registered source, resolving each one's
        ``${secret:...}``. It runs here whichever surface sent the statement — the GraphQL engine
        route reaches this without having bound a vault — so the vault of the org the sources are
        registered in is bound for the walk. No walk pending: nothing is resolved, nothing bound.
        """
        import asyncio

        if self._runtime is not None and self._covers(self._registry_of(state)):
            # to_thread carries the bound org into the worker: the walk reads its registry.
            return await asyncio.to_thread(self._runtime_for, state)
        from provisa.federation.registry_view import registered_sources
        from provisa.federation.source_vault import org_vault

        async with org_vault(state, await registered_sources(state)):
            # to_thread, not run_in_executor: the walk's thread must see the binding.
            return await asyncio.to_thread(self._runtime_for, state)

    def _refresh_control_plane_snapshot(self, state: Any) -> None:
        """A SQLite control plane is read through a snapshot the runtime re-takes when something
        was committed since the last one (``attach_control_plane``: a table registered after
        startup is visible to the very next query). That check is per query by design and is not
        part of the registry walk; a PostgreSQL control plane is attached live, once, by the walk."""
        tdb = getattr(state, "model_db", None)
        if (
            tdb is not None
            and getattr(tdb, "dialect", None) == "sqlite"
            and hasattr(self._runtime, "attach_control_plane")
        ):
            _org_id = getattr(state, "org_id", "default")
            self._runtime.attach_control_plane(
                str(tdb.engine.url.database or ""), f"org_{_org_id}", dialect="sqlite"
            )

    def _walk_registry(self, state: Any, config: Any) -> bool:
        """Attach every registered table not yet attached. True when the walk is complete for this
        registry state — every table attached, skipped by design, or refused for a declared reason;
        False when a driver error left a table to retry on the next query. Caller holds
        ``_walk_lock``."""
        from provisa.core.operator_floor import floor_setting
        from provisa.core.secrets import resolve_secrets

        complete = True
        # A table listed by both the config and the registry: one attempt. Keyed by the table's
        # identity — two sources may both hold ``schema.table``, and each is attached.
        tried: set[tuple[str, str, str, str]] = set()
        sources = {s.id: s for s in config.sources}

        # Merge in dynamically created sources that exist in the DB but not in the YAML config.
        # state.runtime_sources is populated by _rebuild_schemas from the DB sources table; it
        # carries full source rows for sources registered via create_source after startup, whose
        # tables would otherwise never be attached (config.tables is YAML-only, never updated at
        # runtime). Using SimpleNamespace keeps the attribute-access shape identical to config
        # source model objects so the merged creation below works for both.
        runtime_sources = getattr(state, "runtime_sources", None) or {}
        for _rs_id, _rs_dict in runtime_sources.items():
            if _rs_id not in sources:
                sources[_rs_id] = SimpleNamespace(
                    id=_rs_id,
                    type=SimpleNamespace(value=(_rs_dict.get("type") or "")),
                    path=_rs_dict.get("path"),
                    host=_rs_dict.get("host"),
                    port=_rs_dict.get("port"),
                    # REQ-1746: forwarded so _attach_tbl's merged SimpleNamespace below (which
                    # reads it via getattr(src, "base_url", ...)) can actually see it — see that
                    # merge's own comment for the connector-side failure this fixes.
                    base_url=_rs_dict.get("base_url"),
                    database=_rs_dict.get("database"),
                    username=_rs_dict.get("username"),
                    # REQ-1695: the row's password reference is the source's password.
                    password=_rs_dict["password_ref"],
                    federation_hints={},
                    # REQ-1742: forwarded so _attach_tbl's merged SimpleNamespace below (which reads
                    # it via getattr(src, "mapping", {})) can actually see it — same gap base_url
                    # above (REQ-1746) already documents and fixes for a different connector
                    # attribute (DuckDBGsheetsConnector.details() reads mapping["credentials_json"]).
                    mapping=_rs_dict.get("mapping") or {},
                )

        def _rs(v: Any) -> Any:
            return resolve_secrets(v) if isinstance(v, str) else v

        # REQ-788: each files source's file_glob tables, read off the registered tables (the model
        # the store holds, REQ-1919): the attach builds the adapter's merged glob table from them.
        glob_tables = _file_glob_tables([*config.tables, *(getattr(state, "tables", None) or [])])

        def _attach_tbl(src: Any, schema_name: str, table_name: str) -> None:
            """Attach one table into the runtime; skip if already attached or attach fails."""
            nonlocal complete
            key = (state.source_catalogs[src.id], src.id, schema_name, table_name)
            if key in tried:
                return
            if floor_setting(src) is not None:
                # REQ-030/826/1141: the operator's floor. A floored source is read only from its
                # replica, which a read addresses in the store's replicas schema (REQ-1912). It
                # has no live attach: nothing on the engine can then read the source, whatever a
                # statement names. One attached before the setting was turned on is removed.
                # An earlier process of a persistent engine may have left one too, so it is removed
                # once per process whether or not this process attached it.
                tried.add(key)
                if key in self._attached or key not in self._detached:
                    self._runtime.detach_source(
                        SimpleNamespace(
                            id=src.id,
                            type=src.type,
                            catalog=state.source_catalogs[src.id],
                            schema_name=schema_name,
                            table_name=table_name,
                        )
                    )
                    self._attached.discard(key)
                    self._detached.add(key)
                return
            if key in self._attached or key in self._refused:
                return
            tried.add(key)
            if getattr(src, "id", None) in _NO_REMOTE_SOURCE_IDS:
                return
            # Only a source the engine reads LIVE in place (attach/scan) is attached here. A
            # FETCH/DIRECT source (API/push adapter or native driver -- openapi, graphql_remote,
            # the dq sources) is read from its replica or the API cache, never attached, so the
            # walk must not attempt an attach its connector has no face for: that raised a
            # KeyError('attach') caught as "table not queryable" for every such table at startup,
            # though the table is queryable from its replica (REQ-947/951). Decide from the
            # connector's declared reach, not by catching the failure.
            # A registered source always carries its type; one without fails here, by name.
            stype: str = src.type.value
            try:
                if not self.engine.connector_for(stype).reads_in_place:
                    return
            except UnreachableSource:
                return
            merged = SimpleNamespace(
                id=getattr(src, "id", None),
                type=getattr(src, "type", SimpleNamespace(value="")),
                # REQ-1266/1529: the source's catalog name, the one the compiler emits; the
                # engine names what it keeps for the source after it (org and environment).
                catalog=state.source_catalogs[src.id],
                host=_rs(getattr(src, "host", None)),
                port=getattr(src, "port", None),
                # REQ-1746: DuckDBAirportConnector.details() (connector_duckdb.py) reads
                # source.base_url directly (not via getattr) — omitting it here raises
                # AttributeError inside the connector at attach time, the same failure mode
                # backend.py's _merged_source (REQ-1693) already documents and fixes for the
                # non-runtime-source path; this merge (runtime_sources registered after startup)
                # needed the identical field.
                base_url=_rs(getattr(src, "base_url", None)),
                database=_rs(getattr(src, "database", None)),
                username=_rs(getattr(src, "username", None)),
                password=_rs(getattr(src, "password", None)),
                path=_rs(getattr(src, "path", None)),
                # Connection extras (e.g. object-store credentials for a warehouse external link) —
                # secrets resolved so a connector's attach can read them (REQ-987).
                federation_hints={
                    k: _rs(v) for k, v in (getattr(src, "federation_hints", {}) or {}).items()
                },
                # REQ-1742: DuckDBGsheetsConnector.details() (connector_duckdb.py) reads
                # source.mapping["credentials_json"] directly (not via getattr) — omitting it here
                # raised AttributeError inside the connector at real query-time attach, the exact
                # same failure mode this merge's base_url field above (REQ-1746) already documents
                # and fixes for a different connector attribute. backend.py's introspection-time
                # _merged_source already carries mapping; this query-time merge needed it too.
                mapping=getattr(src, "mapping", {}) or {},
                # REQ-788: the source's file_glob table specs drive the file adapter's merged
                # glob tables (pgwire_replica._files_operand). Dropping them here builds the
                # endpoint without the merged table, so the glob table is never queryable.
                file_glob_tables=glob_tables.get(src.id, []),
                schema_name=schema_name,
                table_name=table_name,
            )
            try:
                self._runtime.attach_source(merged)
                self._attached.add(key)
                self._detached.discard(key)
            except self._attach_errors as _ae:
                if isinstance(_ae, _DECLARED_REFUSALS):
                    self._refused.add(key)
                else:
                    complete = False
                _log.warning(
                    "%s attach of %s failed; table not queryable: %s", self.engine.name, key, _ae
                )

        for tbl in config.tables:
            src = sources.get(tbl.source_id)
            if src is not None:
                _attach_tbl(src, tbl.schema_name, tbl.table_name)

        # Also attach tables registered dynamically after startup via registerTable. These live in
        # state.tables (set by _rebuild_schemas from the DB) but not in config.tables (YAML-only).
        _state_tables = getattr(state, "tables", None) or []
        for tbl_dict in _state_tables:
            _sid = tbl_dict.get("source_id")
            src = sources.get(_sid)
            if src is not None:
                _attach_tbl(src, tbl_dict.get("schema_name", ""), tbl_dict.get("table_name", ""))

        # Native DuckDB path: attach a PostgreSQL control-plane DB as the provisa_admin catalog so
        # meta/ops entities resolve (parity with Trino, where provisa_admin is a real catalog). The
        # attach is live, so once is enough; the SQLite control plane is a snapshot and is handled
        # per query by _refresh_control_plane_snapshot.
        tdb = getattr(state, "model_db", None)
        if (
            tdb is not None
            and getattr(tdb, "dialect", None) == "postgresql"
            and hasattr(self._runtime, "attach_control_plane")
        ):
            _org_id = getattr(state, "org_id", "default")
            self._runtime.attach_control_plane(
                libpq_dsn(tdb.engine.url), f"org_{_org_id}", dialect="postgresql"
            )
        return complete

    # -- residency prep (REQ-825 stage-4b / REQ-932) ---------------------------

    async def reconcile_landed_tables(self, state: Any) -> list[tuple[str, str]]:
        """Reconcile the store's replica of every REGISTERED table that is served from one
        (REQ-846/932) — the schema-currency controller. DDL only: no data is copied (that is the
        refresh's job); an existing matching table is KEPT (survives restart), a drifted one
        RECREATED. Convergent + idempotent.

        The work list (which registered tables own a replica, with what shape) is the shared
        ``landing_worklist``; what this adds is the native terminal — converge the table at its
        replica address (REQ-1912). Nothing is created at the table's registered name: a read
        reaches the replica by its address. Returns the (source_id, table_name) reconciled."""
        from provisa.federation.landed_keys import LandedTable, key_plan_for
        from provisa.federation.replica_routing import landing_worklist

        runtime = self._runtime_for(state)
        if not hasattr(runtime, "reconcile_replica"):
            return []  # this engine's runtime has no replica terminal
        reconciled: list[tuple[str, str]] = []
        landed: list[LandedTable] = []
        addresses: dict[str, tuple[str, str]] = {}
        views: dict[str, tuple[str, str]] = {}
        for src, schema_name, table_name, columns, pk_columns in await landing_worklist(
            self.engine, state
        ):
            address = self.replica_address(
                state, source_id=src.id, schema_name=schema_name, table_name=table_name
            )
            try:
                outcome = await runtime.reconcile_replica(
                    schema=address.schema,
                    table=address.table,
                    columns=columns,
                    pk_columns=pk_columns,
                )
                view = None
                if hasattr(runtime, "publish_replica_view"):
                    # A store whose catalog export shares a view of each replica (Snowflake). It
                    # is a published object in the export schema, not a read path, and never at
                    # the table's registered address (REQ-1912).
                    view = self.export_view_address(
                        state, source_id=src.id, schema_name=schema_name, table_name=table_name
                    )
                    await runtime.publish_replica_view(
                        view_schema=view.schema,
                        view_table=view.table,
                        schema=address.schema,
                        table=address.table,
                        replace=outcome == "recreated",
                    )
            except Exception as exc:  # allow-ble: any failure to reconcile IS this table's state — recorded, logged, and raised to every read that names it (replica_address.address_replicas); the other tables still reconcile
                self._unreconciled[(src.id, table_name)] = exc
                _log.error(
                    "%s: the replica of %s.%s.%s could not be reconciled; reads of it are "
                    "refused until it is: %s",
                    self.engine.name,
                    src.id,
                    schema_name,
                    table_name,
                    exc,
                    exc_info=exc,
                )
                continue
            self._unreconciled.pop((src.id, table_name), None)
            reconciled.append((src.id, table_name))
            entry = LandedTable(src.id, schema_name, table_name, tuple(pk_columns))
            landed.append(entry)
            addresses[entry.identity] = (address.schema, address.table)
            if view is not None:
                views[entry.identity] = (view.schema, view.table)
        # REQ-1652/REQ-1654: the keys and descriptions are part of the replicated model's shape and
        # converge here, with the tables, on a store that can hold informational constraints. A
        # runtime without the hook is an enforcing store, where a FOREIGN KEY would refuse every
        # REPLACE land -- by design.
        if landed and hasattr(runtime, "reconcile_landed_metadata"):
            plan = await key_plan_for(state, landed)
            store_catalog = runtime.ensure_materialize_attached()
            # each replica's store address: named by the replica rule, not by its registration
            plan.store_parts = {
                ident: (store_catalog, schema, table)
                for ident, (schema, table) in addresses.items()
            }
            # each replica's export view, where the store publishes one: named by the export rule
            plan.view_parts = {
                ident: (store_catalog, schema, table) for ident, (schema, table) in views.items()
            }
            for what, reason in plan.withheld:
                _log.info("%s: key withheld for %s: %s", self.engine.name, what, reason)
            applied = await runtime.reconcile_landed_metadata(plan)
            if applied:
                _log.info(
                    "%s: %d metadata statement(s) applied to replicas",
                    self.engine.name,
                    applied,
                )
        return reconciled

    # -- store write face --------------------------------------------------

    async def analyze_landed_table(
        self, state: Any, *, catalog: str | None, schema: str, table: str
    ) -> None:  # REQ-280, REQ-1688
        """An embedded DuckDB store is a DuckDB table: the engine's own ANALYZE is the store's (and
        the single connection is the only writer). Every other store is analyzed through its own
        connection (base), because DuckDB's ANALYZE on an attached table is a VACUUM it refuses."""
        runtime = self._runtime_for(state)
        if getattr(runtime, "_store_is_duckdb", lambda: False)():
            with self.isolated_sync(state) as conn:
                conn.execute(f'ANALYZE {catalog}.{schema}."{table}"')
                conn.fetchall()
            return
        await super().analyze_landed_table(state, catalog=catalog, schema=schema, table=table)

    async def land_source_table(
        self,
        state: Any,
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
        """Land ``rows`` through the runtime when it holds the store's own connection (DuckDB, REQ-989
        — a second connection cannot open a file the engine already ATTACHed); otherwise the base
        ``store_writer`` DSN path (every other native store) applies unchanged."""
        runtime = self._runtime_for(state)
        if hasattr(runtime, "land_table"):
            return await runtime.land_table(
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
        return await super().land_source_table(
            state,
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

    async def apply_cdc_events(
        self,
        state: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
        events: list,
    ) -> dict[str, int]:
        """Apply CDC events through the runtime when it holds the store's own connection (DuckDB,
        REQ-989 — a second connection cannot open a file the engine already ATTACHed); otherwise the
        base ``store_writer`` DSN path (every other native store) applies unchanged."""
        runtime = self._runtime_for(state)
        if hasattr(runtime, "apply_cdc_events"):
            return await runtime.apply_cdc_events(
                schema=schema,
                table=table,
                columns=columns,
                pk_columns=pk_columns,
                events=events,
            )
        # pyright reports this as an unresolved attribute on `object` despite EngineBackend
        # (backend.py) defining apply_cdc_events and NativeEngineBackend.__mro__ resolving
        # exactly as NativeEngineBackend -> EngineBackend -> object (confirmed live) — a static-
        # analysis false positive, not a real gap.
        return await super().apply_cdc_events(  # pyright: ignore[reportAttributeAccessIssue]
            state,
            schema=schema,
            table=table,
            columns=columns,
            pk_columns=pk_columns,
            events=events,
        )

    async def reconcile_mv_table(
        self,
        state: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str] | None = None,
    ) -> str:
        """Converge an MV's store table through the runtime's own connection when it holds one
        (DuckDB, REQ-989); otherwise the base ``store_writer`` DSN path applies unchanged."""
        runtime = self._runtime_for(state)
        if hasattr(runtime, "reconcile_mv_table"):
            outcome = await runtime.reconcile_mv_table(
                schema=schema, table=table, columns=columns, pk_columns=pk_columns
            )
            await self._reconcile_mv_metadata(state, runtime, schema, table, pk_columns)
            return outcome
        return await super().reconcile_mv_table(
            state, schema=schema, table=table, columns=columns, pk_columns=pk_columns
        )

    async def reconcile_mv_metadata(
        self, state: Any, *, schema: str, table: str, pk_columns: list[str] | None = None
    ) -> None:
        """The refresh's hook (REQ-1652/1654/1655): after an MV store table is created or refreshed
        through the engine, its declared metadata converges onto it."""
        await self._reconcile_mv_metadata(
            state, self._runtime_for(state), schema, table, pk_columns
        )

    async def _reconcile_mv_metadata(
        self, state: Any, runtime: Any, schema: str, table: str, pk_columns: list[str] | None
    ) -> None:
        """REQ-1652/1654/1655 for an MV: the keys, descriptions and tags its ``__derived__``
        registration declares converge onto its store table right after that table is converged --
        the "on MV creation" hook. A store without the metadata hook (enforcing) gets none."""
        if not hasattr(runtime, "reconcile_landed_metadata"):
            return
        tdb = getattr(state, "model_db", None)
        if tdb is None:
            return
        from provisa.api.admin.db_queries import fetch_tables
        from provisa.federation.landed_keys import (
            LandedTable,
            derived_registration,
            key_plan_for,
        )

        async with tdb.acquire() as conn:
            row = derived_registration(await fetch_tables(conn), table)
        if row is None:
            return  # a store table no registration describes: nothing declared to apply
        landed = LandedTable(
            "__derived__", row["schema_name"], row["table_name"], tuple(pk_columns or ())
        )
        plan = await key_plan_for(state, [landed])
        # the MV's store address differs from its registration name (mv_<id> in the cache schema)
        plan.store_parts = {landed.identity: (runtime.ensure_materialize_attached(), schema, table)}
        await runtime.reconcile_landed_metadata(plan)

    def mv_store_broker(self, state: Any) -> Any:
        """The runtime's store broker when MV writes cannot run as engine SQL (REQ-1901: an embedded
        DuckDB-file store), else ``None`` — the runtime decides; one without a broker has none."""
        runtime = self._runtime_for(state)
        return runtime.mv_store_broker() if hasattr(runtime, "mv_store_broker") else None

    async def persist_mv_table(
        self,
        state: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        persist: str,
        pk_columns: list[str] | None = None,
        match_floor: float = 0.0,
    ) -> str:
        """Persist an MV's recomputed rows through the runtime's own connection when it holds one
        (DuckDB, REQ-989); otherwise the base ``store_writer`` DSN path applies unchanged."""
        runtime = self._runtime_for(state)
        if hasattr(runtime, "persist_mv_table"):
            return await runtime.persist_mv_table(
                schema=schema,
                table=table,
                columns=columns,
                rows=rows,
                persist=persist,
                pk_columns=pk_columns,
                match_floor=match_floor,
            )
        return await super().persist_mv_table(
            state,
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            persist=persist,
            pk_columns=pk_columns,
            match_floor=match_floor,
        )

    # -- execution -------------------------------------------------------------

    async def execute(
        self,
        state: Any,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,  # pyright: ignore[reportUnusedParameter]
        fresh: bool = False,  # pyright: ignore[reportUnusedParameter]
        conn_kwargs: dict | None = None,  # pyright: ignore[reportUnusedParameter]
        span_attrs: dict[str, str] | None = None,
        extra_table_attrs: list[dict[str, str]] | None = None,  # pyright: ignore[reportUnusedParameter]
    ) -> QueryResult:
        # session_hints/fresh/conn_kwargs/extra_table_attrs are part of the polymorphic execute()
        # signature EngineBackend's callers share across engines (session_hints/conn_kwargs steer a
        # per-connection driver session Trino's terminal needs; fresh/extra_table_attrs are cache-
        # freshness and report-column hints other terminals consult) — a native engine has one
        # single in-process connection with no such session to steer, so it accepts and ignores
        # them rather than breaking the shared call site every engine's execute() must match.
        # The ops `queries` report reads spans named provisa.query.* and lifts their provisa.*
        # attributes into the trace table (TRACE_ATTR_COLS). The Trino terminal names its span
        # that way in execute_trino; a native engine has no such executor, so the terminal names
        # it here — otherwise every native-engine org's report is blank.
        span_name = f"provisa.query.{self.dialect}" if span_attrs else f"{self.dialect}.execute"
        with _stage(_tracer, span_name, name="execute") as span:
            span.set_attribute("db.system", self.dialect)
            span.set_attribute("db.statement", sql[:1000])
            if span_attrs:
                for k, v in span_attrs.items():
                    span.set_attribute(k, v)
            # _runtime_for's lazy _attach_registered() is a plain synchronous method: on a
            # source's FIRST attach it runs blocking DDL (IMPORT FOREIGN SCHEMA) + ANALYZE
            # against the remote FDW source directly -- for a ClickHouse-backed foreign table,
            # live-confirmed ANALYZE alone can take 100s of seconds. Called un-wrapped, this ran
            # straight on the shared governance event loop (REQ-1882's exact pattern), starving
            # every other concurrent request for the whole attach+ANALYZE duration and surfacing
            # as an empty-message ~120s timeout with zero server-side trace. Off-loaded via the
            # default executor, matching every other REQ-1882 fix site.
            runtime = await self._attached_runtime(state)
            return await runtime.run(sql, params)

    def describe_sync(
        self, state: Any, sql: str, params: list | None = None
    ) -> ResultStream | None:
        runtime = self._runtime_for(state)
        describe = getattr(runtime, "describe_sync", None)
        return None if describe is None else describe(sql, params)

    def execute_sync(
        self,
        state: Any,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,
    ) -> ResultStream:
        # A native runtime ignores session_hints exactly as its async ``execute`` does — the
        # hints (FTE retry_policy etc.) are Trino session properties with no native analogue.
        del session_hints
        return self._runtime_for(state).run_sync(sql, params)

    def borrow_raw_pg_connection(self, state: Any) -> Any:
        """One connection from the runtime's own read pool, for pgwire's raw-DataRow passthrough
        (REQ-1863). Only a runtime that pools genuine Postgres connections offers one; for any
        other runtime the passthrough does not apply, and the statement is decoded."""
        from provisa.pgwire.pg_passthrough import PassthroughError

        runtime = self._runtime_for(state)
        if not hasattr(runtime, "borrow_raw"):
            raise PassthroughError(
                f"engine {self.engine.name!r} has no pooled Postgres connection to read through"
            )
        return runtime.borrow_raw()

    # -- engine-specific transports (Arrow) (REQ-986, REQ-1219) ----------------
    # Routed here only for engines whose capabilities declare ARROW / ARROW_STREAM (the runtime gates
    # on capability before dispatch). A runtime with a NATIVE Arrow reader (duckdb / snowflake) uses
    # it directly (zero-copy). A ROWS-only runtime (pg / sqlalchemy) has no ``run_arrow*`` method, so
    # its lazy row stream is packed into Arrow batches by the generic adapter (REQ-1219): bounded, not
    # zero-copy. This is a genuine strategy choice, not a silent row fallback — the engine DECLARES
    # ARROW/ARROW_STREAM only because this adapter backs it.

    def execute_arrow(self, state: Any, sql: str, params: list | None = None):
        rt = self._runtime_for(state)
        if hasattr(rt, "run_arrow"):
            return rt.run_arrow(sql, params)
        import pyarrow as pa

        from provisa.federation.runtime_support import arrow_batches_from_rows

        schema, batches = arrow_batches_from_rows(rt.run_sync(sql, params))
        return pa.Table.from_batches(list(batches), schema=schema)

    def execute_stream(self, state: Any, sql: str, params: list | None = None):
        rt = self._runtime_for(state)
        if hasattr(rt, "run_arrow_stream"):
            return rt.run_arrow_stream(sql, params)
        from provisa.federation.runtime_support import arrow_batches_from_rows

        return arrow_batches_from_rows(rt.run_sync(sql, params))

    # -- cache terminal (materialization store) --------------------------------

    @contextmanager
    def isolated_sync(self, state: Any):
        """The API-result cache terminal: the runtime connection with the materialization store
        attached. Cache writes land in the store — never the engine's transient storage. A missing
        store errors at attach (the engine invariant). Yields an :class:`EngineSession`, never the
        raw physical-driver connection — the runtime's connection is shared/persistent, so the
        session is not closed on exit.

        REQ-1901: an embedded DuckDB-file store is never ATTACHed on the runtime connection, so
        ``mat_store`` does not exist there; the session then runs every cache statement against
        the store through the store broker instead."""
        from provisa.executor.session import EngineSession, StoreBrokerSession

        rt = self._runtime_for(state)
        rt.ensure_materialize_attached()
        broker = self.mv_store_broker(state)
        if broker is not None:
            yield StoreBrokerSession(broker)
            return
        yield EngineSession(
            rt.connection, dialect=self.dialect, placeholder=self._cache_bind_placeholder
        )

    def _materialize_store_ref(self, state: Any) -> str | None:
        """A native engine's source exposure is not itself a durable catalog, so API results a source
        cannot reach live are cached in the materialization store, attached under its alias. A missing
        store is a hard error (raised by the runtime)."""
        return self._runtime_for(state).ensure_materialize_attached()

    def materialize_store_target(self, state: Any, org_id: str) -> tuple[str, str]:
        """A native engine writes MVs into its OWN materialization store — the catalog it attaches the
        store under (``ensure_materialize_attached``: DuckDB → ``mat_store``, Databricks → its Unity
        catalog, BigQuery → its project) and the runtime's declared MV schema — NOT the Postgres
        store-engine default. Hardcoding ``postgresql`` here failed the refresh with "Catalog with name
        postgresql does not exist" on a DuckDB deployment. An engine whose store terminal is not wired
        (ClickHouse) raises from ``ensure_materialize_attached`` — explicit, never a wrong target."""
        rt = self._runtime_for(state)
        return rt.ensure_materialize_attached(), rt.mv_store_schema(org_id)
