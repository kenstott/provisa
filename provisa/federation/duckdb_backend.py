# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDBBackend — the DuckDB engine's in-process execution terminal (REQ-840/844).

Kept beside the generic EngineBackend seam and the DuckDBFederationRuntime it drives. The engine's
MODEL (its connector set) decides how each source is exposed; this backend only owns the terminal
lifecycle: build one persistent runtime, attach every registered table into it, run governed SQL.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import duckdb

from provisa.executor.result import QueryResult
from provisa.federation.backend import EngineBackend
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

_log = logging.getLogger(__name__)


class DuckDBBackend(EngineBackend):
    """An in-process federation terminal. Every registered table ATTACHes into ONE persistent
    ``DuckDBFederationRuntime`` — postgres/sqlite/csv/parquet in place, non-attachable remote sources
    LAND into the materialization store. Governed physical SQL (already transpiled to the DuckDB
    dialect by ``transpile_physical``) runs against that connection, whose catalog-physical views
    resolve the names the compiler emits. ``is_connected`` is inherited True — native, live once built.
    """

    def __init__(self, engine: Any) -> None:
        super().__init__(engine)
        self._runtime: DuckDBFederationRuntime | None = None
        self._attached: set[str] = set()

    def _runtime_for(self, state: Any) -> DuckDBFederationRuntime:
        """The persistent runtime with every registered table attached (idempotent, lazy)."""
        if self._runtime is None:
            self._runtime = DuckDBFederationRuntime()
        self._attach_registered(state)
        return self._runtime

    def _attach_registered(self, state: Any) -> None:
        """ATTACH every registered table into the runtime once. A source that cannot be attached
        (offline → duckdb.Error, or a LAND source with no attach DDL / no rows materialized yet →
        KeyError) is logged and skipped: that table is not queryable until reachable, rather than
        failing every other table's query. Any other error is a real bug and propagates."""
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
            except (duckdb.Error, KeyError):
                _log.warning("duckdb attach of %s failed; table not queryable", key, exc_info=True)

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

    @contextmanager
    def isolated_sync(self, state: Any):
        """The API-result cache terminal: yields the runtime's connection with the materialization
        store attached. The engine cache writes CREATE TABLE/INSERT against ``mat_store.*``, which
        land in the store (not DuckDB's own storage)."""
        rt = self._runtime_for(state)
        rt.ensure_materialize_attached()
        yield rt.connection

    def _materialize_store_ref(self, state: Any) -> str | None:
        """DuckDB's source exposure is ephemeral (in-memory attaches), so API results a source cannot
        reach live are cached in the EXTERNAL materialization store, attached under its alias — never
        DuckDB's own storage. Unconfigured store → error (raised by the runtime)."""
        return self._runtime_for(state).ensure_materialize_attached()
