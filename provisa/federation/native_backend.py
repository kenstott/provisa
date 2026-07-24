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

from provisa.federation.backend import EngineBackend

if TYPE_CHECKING:
    from provisa.executor.result import QueryResult, ResultStream

_log = logging.getLogger(__name__)


class NativeEngineBackend(EngineBackend):
    """In-process execution terminal shared by all native engines. ``is_connected`` is inherited True
    — a native engine is live once built. Subclasses supply ``_new_runtime`` and, if the runtime
    raises a driver-specific error when a source is unreachable, extend ``_attach_errors``."""

    # Errors from attach_source that mean "this table is not queryable" (offline source, or a LAND
    # source not yet materialized) — logged and skipped so one bad table never fails other queries.
    # A subclass ORs in its driver error type. Anything else is a real bug and propagates.
    _attach_errors: tuple[type[BaseException], ...] = (KeyError,)

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
        sources = {s.id: s for s in config.sources}

        def _rs(v: Any) -> Any:
            return resolve_secrets(v) if isinstance(v, str) else v

        for tbl in config.tables:
            key = f"{tbl.schema_name}.{tbl.table_name}"
            if key in self._attached:
                continue
            src = sources.get(tbl.source_id)
            if src is None:
                continue
            merged = SimpleNamespace(
                id=src.id,
                type=src.type,
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
                schema_name=tbl.schema_name,
                table_name=tbl.table_name,
            )
            try:
                self._runtime.attach_source(merged)
                self._attached.add(key)
            except self._attach_errors:
                _log.warning("%s attach of %s failed; table not queryable", self.engine.name, key)

        # Native DuckDB path: attach the control-plane SQLite DB as the provisa_admin catalog so
        # meta/ops entities resolve (parity with Trino, where provisa_admin is a real catalog).
        # Only runs when: the runtime supports it (DuckDB), the tenant DB is SQLite (native), and
        # the DB URL points at a real file. Idempotent — the runtime guards with a flag.
        tdb = getattr(state, "tenant_db", None)
        if (
            tdb is not None
            and getattr(tdb, "dialect", None) == "sqlite"
            and hasattr(self._runtime, "attach_control_plane")
        ):
            _db_url = tdb.engine.url
            _db_path = str(_db_url.database or "")
            _org_id = getattr(state, "org_id", "default")
            self._runtime.attach_control_plane(_db_path, f"org_{_org_id}")

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

        Drives off the control-plane REGISTERED tables — not the raw YAML config — because
        registration is the design-time source of truth: it holds the sql-normalized physical names
        (the same names the compiler emits) AND the resolved column types. A registered data column
        with no type is a registration/config gap and the table is skipped (logged), never guessed.
        Returns the (source_id, table_name) reconciled."""
        from provisa.core.ir_types import to_ir
        from provisa.federation.engine import UnreachableSource
        from provisa.federation.strategy import Strategy, federate

        config = getattr(state, "config", None)
        tdb = getattr(state, "tenant_db", None)
        if config is None or tdb is None:
            return []
        runtime = self._runtime_for(state)
        if not hasattr(runtime, "attach_landed_source"):
            return []  # this engine's runtime has no eager-landing terminal
        from provisa.api.admin.db_queries import fetch_tables

        async with tdb.acquire() as conn:
            registered = await fetch_tables(conn)
        sources = {s.id: s for s in config.sources}
        reconciled: list[tuple[str, str]] = []
        for reg in registered:
            src = sources.get(reg["source_id"])
            if src is None:
                continue
            try:
                if federate(src, self.engine) is not Strategy.MATERIALIZED:
                    continue  # live/scan → attached live, not eager-landed
            except UnreachableSource:
                continue
            # A PARAMETERIZED source — one with native-filter (query/path-param) columns — is a
            # function f(args) -> rows with no unparameterized snapshot. It is fetched real-time at
            # query time (never materialized), so it never lands a replica.
            if any(c["native_filter_type"] is not None for c in reg["columns"]):
                continue
            # Native-filter columns are synthetic query args, not landed data — excluded from the
            # landing shape (defensive: none remain past the guard above).
            data_cols = [c for c in reg["columns"] if c["native_filter_type"] is None]
            if any(c["data_type"] is None for c in data_cols):
                _log.warning(
                    "%s: skip eager reconcile of %s.%s — a registered column has no resolved type",
                    self.engine.name,
                    reg["schema_name"],
                    reg["table_name"],
                )
                continue
            columns = [(c["column_name"], to_ir(c["data_type"])) for c in data_cols]
            pk_columns = [c["column_name"] for c in data_cols if c["is_primary_key"]]
            merged = SimpleNamespace(
                id=src.id,
                type=src.type,
                schema_name=reg["schema_name"],
                table_name=reg["table_name"],
            )
            await runtime.attach_landed_source(merged, columns, pk_columns=pk_columns)
            reconciled.append((src.id, reg["table_name"]))
        return reconciled

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
        store errors at attach (the engine invariant)."""
        rt = self._runtime_for(state)
        rt.ensure_materialize_attached()
        yield rt.connection

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
