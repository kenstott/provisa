# Copyright (c) 2026 Kenneth Stott
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
    from provisa.executor.result import QueryResult

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
                schema_name=tbl.schema_name,
                table_name=tbl.table_name,
            )
            try:
                self._runtime.attach_source(merged)
                self._attached.add(key)
            except self._attach_errors:
                _log.warning("%s attach of %s failed; table not queryable", self.engine.name, key)

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

    def execute_sync(self, state: Any, sql: str, params: list | None = None) -> QueryResult:
        return self._runtime_for(state).run_sync(sql, params)

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
