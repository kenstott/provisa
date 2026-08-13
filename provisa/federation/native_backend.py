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
from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from opentelemetry import trace

from provisa.federation.backend import EngineBackend
from provisa.federation.engine import UnreachableSource

_tracer = trace.get_tracer(__name__)

if TYPE_CHECKING:
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


class NativeEngineBackend(EngineBackend):
    """In-process execution terminal shared by all native engines. ``is_connected`` is inherited True
    — a native engine is live once built. Subclasses supply ``_new_runtime`` and, if the runtime
    raises a driver-specific error when a source is unreachable, extend ``_attach_errors``."""

    # Errors from attach_source that mean "this table is not queryable" (offline source, or a LAND
    # source not yet materialized) — logged and skipped so one bad table never fails other queries.
    # A subclass ORs in its driver error type. Anything else is a real bug and propagates.
    #
    # UnreachableSource belongs here for the same reason: reachability is binary and engine-scoped
    # (REQ-841), so on a PARTIAL/SELF_ONLY engine some registered source types simply have no
    # connector — the seeded provisa-otel iceberg store on the Synapse engine, for example. That is
    # the declared state of that pair, not a failure of the attach loop, and reconcile_landed_tables
    # already skips the same condition the same way (see its `except UnreachableSource: continue`).
    _attach_errors: tuple[type[BaseException], ...] = (KeyError, UnreachableSource)

    def __init__(self, engine: Any) -> None:
        super().__init__(engine)
        self._runtime: Any = None
        self._attached: set[str] = set()

    # -- runtime (subclass hook) ----------------------------------------------

    def _new_runtime(self) -> Any:
        """Build this engine's persistent runtime. Native engines that have not wired a runtime yet
        cannot execute — an explicit error, never a silent fallback to another engine."""
        raise NotImplementedError(
            f"engine {self.engine.name!r} has not wired a native runtime (execution/cache terminal)"
        )

    def _runtime_for(self, state: Any) -> Any:
        """The persistent runtime with every registered table attached (idempotent, lazy)."""
        if self._runtime is None:
            self._runtime = self._new_runtime()
        self._attach_registered(state)
        return self._runtime

    def _attach_registered(self, state: Any) -> None:
        """ATTACH every registered table into the runtime once. A table whose source cannot be
        attached (offline, or a LAND source not yet materialized) is logged and skipped."""
        from provisa.core.secrets import resolve_secrets

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
        from provisa.api.org_runtime import current_org

        _active_org = current_org.get()
        _owns_engine = getattr(state, "active_isolated_org", None) == _active_org
        _default_org = getattr(state, "org_id", None)
        if _active_org is not None and not _owns_engine and _active_org != _default_org:
            raise RuntimeError(
                f"native federation engine {self.engine.name!r} is single-org; org "
                f"{_active_org!r} requires the Trino tier for per-org catalog isolation (REQ-1266)"
            )

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
                    database=_rs_dict.get("database"),
                    username=_rs_dict.get("username"),
                    password=_rs_dict.get("password"),
                    federation_hints={},
                )

        def _rs(v: Any) -> Any:
            return resolve_secrets(v) if isinstance(v, str) else v

        def _attach_tbl(src: Any, schema_name: str, table_name: str) -> None:
            """Attach one table into the runtime; skip if already attached or attach fails."""
            key = f"{schema_name}.{table_name}"
            if key in self._attached:
                return
            if getattr(src, "id", None) in _NO_REMOTE_SOURCE_IDS:
                return
            merged = SimpleNamespace(
                id=getattr(src, "id", None),
                type=getattr(src, "type", SimpleNamespace(value="")),
                host=_rs(getattr(src, "host", None)),
                port=getattr(src, "port", None),
                database=_rs(getattr(src, "database", None)),
                username=_rs(getattr(src, "username", None)),
                password=_rs(getattr(src, "password", None)),
                path=_rs(getattr(src, "path", None)),
                # Connection extras (e.g. object-store credentials for a warehouse external link) —
                # secrets resolved so a connector's attach can read them (REQ-987).
                federation_hints={
                    k: _rs(v) for k, v in (getattr(src, "federation_hints", {}) or {}).items()
                },
                schema_name=schema_name,
                table_name=table_name,
            )
            try:
                self._runtime.attach_source(merged)
                self._attached.add(key)
            except self._attach_errors as _ae:
                _log.warning("%s attach of %s failed; table not queryable: %s", self.engine.name, key, _ae)

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

        # Native DuckDB path: attach the control-plane DB as the provisa_admin catalog so
        # meta/ops entities resolve (parity with Trino, where provisa_admin is a real catalog).
        # Supports both SQLite (file path) and Postgres (libpq DSN) tenant DBs. Idempotent — the
        # runtime guards with a flag.
        tdb = getattr(state, "tenant_db", None)
        _dialect = getattr(tdb, "dialect", None)
        if (
            tdb is not None
            and _dialect in ("sqlite", "postgresql")
            and hasattr(self._runtime, "attach_control_plane")
        ):
            _db_url = tdb.engine.url
            _org_id = getattr(state, "org_id", "default")
            if _dialect == "postgresql":
                _pw = f" password={_db_url.password}" if _db_url.password else ""
                _db_path = (
                    f"host={_db_url.host} port={_db_url.port} dbname={_db_url.database} "
                    f"user={_db_url.username}{_pw}"
                )
            else:
                _db_path = str(_db_url.database or "")
            self._runtime.attach_control_plane(_db_path, f"org_{_org_id}", dialect=_dialect)

    # -- residency prep (REQ-825 stage-4b / REQ-932) ---------------------------

    async def materialize_pending(
        self,
        state: Any,
        *,
        loader: Any,
        is_stale: Any,
        prefer_materialized_of: Any = None,
        materialization_backend: str | None = None,
        freshness_subject_of: Any = None,
        now: float | None = None,
    ) -> list[tuple[str, str]]:
        """Land every MATERIALIZED source table that is stale, before execute (REQ-825/932).

        Builds the residency plan over the configured sources (``build_execution_plan`` decides
        which federate to MATERIALIZED and, via ``is_stale``, which need a refresh) and carries it
        out through ``run_prep`` — fetching rows with the injected ``loader`` and landing them via
        the runtime's store write face. The engine is only the reader; it never writes. Returns the
        (source_id, table_name) pairs landed. A no-op when there is no config or nothing is stale."""
        from provisa.federation.plan import build_execution_plan
        from provisa.federation.residency import run_prep

        config = getattr(state, "config", None)
        if config is None:
            return []
        sources = list(config.sources)
        sources_by_id = {s.id: s for s in sources}
        tables_by_source: dict[str, list] = {}
        for t in config.tables:
            tables_by_source.setdefault(t.source_id, []).append(t)
        plan = build_execution_plan(
            sources,
            self.engine,
            is_stale,
            prefer_materialized_of=prefer_materialized_of,
            materialization_backend=materialization_backend,
            freshness_subject_of=freshness_subject_of,
            now=now,
        )
        if not plan.prep:
            return []
        runtime = self._runtime_for(state)
        return await run_prep(
            plan,
            sources_by_id=sources_by_id,
            tables_by_source=tables_by_source,
            runtime=runtime,
            loader=loader,
        )

    async def reconcile_landed_tables(self, state: Any) -> list[tuple[str, str]]:
        """Reconcile the store's landing SCHEMA to the REGISTERED tables for every MATERIALIZED
        source, then attach the engine's read view — the schema-currency controller (REQ-846/932).
        DDL only: no data is landed (that is the refresh's job); an existing matching table is KEPT
        (survives restart), a drifted one RECREATED. Convergent + idempotent.

        The work list (which registered tables land, with what shape) is the shared
        ``landing_worklist``; what this adds is the native terminal — converge the store table and
        expose the engine's read view over it. Returns the (source_id, table_name) reconciled."""
        from provisa.federation.backend import landing_worklist

        runtime = self._runtime_for(state)
        if not hasattr(runtime, "attach_landed_source"):
            return []  # this engine's runtime has no eager-landing terminal
        reconciled: list[tuple[str, str]] = []
        for src, schema_name, table_name, columns, pk_columns in await landing_worklist(
            self.engine, state
        ):
            merged = SimpleNamespace(
                id=src.id, type=src.type, schema_name=schema_name, table_name=table_name
            )
            await runtime.attach_landed_source(merged, columns, pk_columns=pk_columns)
            reconciled.append((src.id, table_name))
        return reconciled

    # -- store write face --------------------------------------------------

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
            return await runtime.reconcile_mv_table(
                schema=schema, table=table, columns=columns, pk_columns=pk_columns
            )
        return await super().reconcile_mv_table(
            state, schema=schema, table=table, columns=columns, pk_columns=pk_columns
        )

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
        session_hints: dict[str, str] | None = None,
        fresh: bool = False,
        conn_kwargs: dict | None = None,
        span_attrs: dict[str, str] | None = None,
        extra_table_attrs: list[dict[str, str]] | None = None,
    ) -> QueryResult:
        # The ops `queries` report reads spans named provisa.query.* and lifts their provisa.*
        # attributes into the trace table (TRACE_ATTR_COLS). The Trino terminal names its span
        # that way in execute_trino; a native engine has no such executor, so the terminal names
        # it here — otherwise every native-engine org's report is blank.
        span_name = f"provisa.query.{self.dialect}" if span_attrs else f"{self.dialect}.execute"
        with _tracer.start_as_current_span(span_name) as span:
            span.set_attribute("db.system", self.dialect)
            span.set_attribute("db.statement", sql[:1000])
            if span_attrs:
                for k, v in span_attrs.items():
                    span.set_attribute(k, v)
            return await self._runtime_for(state).run(sql, params)

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
        session is not closed on exit."""
        from provisa.executor.session import EngineSession

        rt = self._runtime_for(state)
        rt.ensure_materialize_attached()
        yield EngineSession(rt.connection)

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
