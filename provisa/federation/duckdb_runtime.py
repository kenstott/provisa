# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7b2a19-6d3c-4f81-9b25-8a1e5c9d2f47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB federation runtime — ties the connectors, materialize store, and execution together.

One in-process DuckDB connection acts as the single-node federation engine. Each registered source
is exposed at its PHYSICAL ``schema.table`` name (what rewrite_semantic_to_physical emits) so
the query executes unchanged:

- ATTACH sources (postgres/sqlite/csv/parquet) are referenced in place via the (duckdb, source_type)
  connector's DDL, then wrapped in a physical-named view.
- NON-attachable sources (openapi/graphql_remote) are LANDED into the relational materialization
  store (via materialize_exec, through the SQLAlchemy write face), which DuckDB ATTACHes, then
  wrapped in a physical-named view.

execute() runs governed semantic SQL through rewrite_semantic_to_physical -> transpile("duckdb").
This is the engine primitive a live EngineRuntime dispatch would call; routing/HTTP wiring is separate.
"""

from __future__ import annotations

import asyncio
from typing import Any

import duckdb

from provisa.executor.result import QueryResult
from provisa.federation import store_writer
from provisa.federation.engine import build_duckdb_engine
from provisa.federation.runtime_support import columns_from_describe, result_from_dbapi
from provisa.transpiler.transpile import transpile


def _mat_table_name(source: Any) -> str:
    """The internal ``mat`` schema table name for a landed (source, physical table). Keyed by the
    source id AND its physical schema/table so a multi-table materialize-only source lands each
    table in its own store table instead of colliding on the source id. Only the runtime references
    it (through the physical-named view it creates); the compiler never sees it."""
    return f"{source.id}__{source.schema_name}__{source.table_name}"


class DuckDBFederationRuntime:  # REQ-825, REQ-840, REQ-844
    def __init__(self, *, materialize_dsn: str | None = None) -> None:
        self._con = duckdb.connect()
        self._engine = build_duckdb_engine()
        # An explicit materialize-store DSN override (tests). When None it is resolved lazily via the
        # engine's invariant (configured store → declared default → error) only when a materialize
        # operation actually needs it — the runtime is also built for introspection, which does not.
        self._materialize_dsn = materialize_dsn
        self._sqlite_loaded = False
        self._pg_ext_loaded = False  # postgres DuckDB extension INSTALL/LOAD (source ATTACH)
        self._store_attached = False  # materialization-store ATTACH (distinct from source attaches)
        self._phys_catalogs: set[str] = set()  # in-memory catalogs holding the physical views
        self._raw_attached: set[str] = set()  # source ids whose remote DB is already ATTACHed

    # -- source exposure -------------------------------------------------------

    def _phys_name(self, source: Any) -> str:
        """The catalog-qualified physical name the compiler emits: ``"catalog"."schema"."table"``.
        The engine's catalog for a source is its id with hyphens normalized (see core.catalog)."""
        from provisa.core.catalog import _to_catalog_name

        catalog = _to_catalog_name(source.id)
        if catalog not in self._phys_catalogs:
            # A writable in-memory catalog so the 3-part physical name resolves (an ATTACHed remote
            # DB is read-only and cannot host the schema/view the compiler references).
            self._con.execute(f"ATTACH ':memory:' AS \"{catalog}\"")
            self._phys_catalogs.add(catalog)
        self._con.execute(f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."{source.schema_name}"')
        return f'"{catalog}"."{source.schema_name}"."{source.table_name}"'

    def attach_source(self, source: Any) -> None:
        """Expose an ATTACH source at its catalog-physical name via the engine's connector."""
        entry = self._engine.resolve(source)  # picks the (duckdb, source_type) connector
        details = entry.details
        phys = self._phys_name(source)
        if "view_ddl" in details:  # csv / parquet scanner
            scan = details["view_ddl"].split(" AS ", 1)[1]
            self._con.execute(f"CREATE VIEW IF NOT EXISTS {phys} AS {scan}")
        else:  # ATTACH postgres / sqlite once, then view the remote table
            if source.type.value == "sqlite" and not self._sqlite_loaded:
                self._con.execute("INSTALL sqlite")
                self._con.execute("LOAD sqlite")
                self._sqlite_loaded = True
            elif source.type.value == "postgresql" and not self._pg_ext_loaded:
                self._con.execute("INSTALL postgres")
                self._con.execute("LOAD postgres")
                self._pg_ext_loaded = True
            # The connector attaches the raw remote under a private alias (distinct from the physical
            # catalog) and declares WHERE it exposes the table: postgres keeps its own (registered)
            # schema; sqlite lands everything under ``main``. The runtime composes the reference with
            # the actual table, so no per-source-type layout is hardcoded here.
            raw_alias = details.get("raw_alias", source.id)
            if raw_alias not in self._raw_attached:
                self._con.execute(details["attach"])
                self._raw_attached.add(raw_alias)
            remote_schema = details.get("remote_schema", source.schema_name)
            remote = f'"{raw_alias}"."{remote_schema}"."{source.table_name}"'
            self._con.execute(f"CREATE VIEW IF NOT EXISTS {phys} AS SELECT * FROM {remote}")

    # The materialization store, attached under this backend-neutral alias. A store MUST exist (the
    # engine's invariant); its backend/dialect is taken from the store URL scheme, never assumed.
    _MAT_STORE = "mat_store"
    _ATTACH_TYPE_BY_SCHEME = {"postgresql": "postgres", "postgres": "postgres"}

    def _store_dsn(self) -> str:
        """The materialization-store DSN: the explicit constructor override, else the engine's
        invariant resolution (configured → declared default → error). Never a fallback."""
        return (
            self._materialize_dsn
            if self._materialize_dsn is not None
            else (self._engine.materialize_store())
        )

    def ensure_materialize_attached(self) -> str:
        """ATTACH the materialization store under ``mat_store`` (idempotent); return the alias. The
        DuckDB ATTACH type is derived from the store URL scheme; the driver parses the URL and owns
        its own defaults — the runtime injects none. A missing store is a hard error (via _store_dsn)."""
        dsn = self._store_dsn()
        if not self._store_attached:
            from urllib.parse import urlparse

            scheme = urlparse(dsn).scheme.split("+", 1)[0]
            store_type = self._ATTACH_TYPE_BY_SCHEME.get(scheme)
            if store_type is None:
                raise RuntimeError(f"materialize store scheme {scheme!r} is not attachable")
            self._con.execute(f"INSTALL {store_type}")
            self._con.execute(f"LOAD {store_type}")
            self._con.execute(f"ATTACH '{dsn}' AS {self._MAT_STORE} (TYPE {store_type})")
            self._store_attached = True
        return self._MAT_STORE

    @property
    def connection(self):
        """The underlying DuckDB connection — the backend's cache terminal writes the API-result
        cache through it against ``mat_store.*``, landing in the store (not DuckDB's own storage)."""
        return self._con

    async def materialize_source(
        self,
        source: Any,
        columns: list[tuple[str, str]],
        rows: list[dict],
        *,
        change_signal: str = "ttl",
        watermark_column: str | None = None,
        pk_columns: list[str] | None = None,
    ) -> None:
        """LAND a source with no connector into the materialization store, then expose it at its
        catalog-physical name through the store attach.

        The batch land shape is chosen from the effective change_signal (REQ-932): a poll signal
        with a watermark AMENDS (append the watermark-filtered delta); every other batch is a full
        REPLACE. Hard-delete CDC is the separate streaming path (subscriptions.cdc_landing) — a push
        signal's one-shot materialize is a full snapshot seed."""
        store = self.ensure_materialize_attached()  # errors if the store is not configured
        mat_table = _mat_table_name(source)  # unique per (source, physical table) — no collision
        # Land through the ONE write face (store_writer.land) — the engine never writes the store.
        await store_writer.land(
            self._store_dsn(),
            schema="mat",
            table=mat_table,
            columns=columns,
            rows=rows,
            change_signal=change_signal,
            watermark_column=watermark_column,
            pk_columns=pk_columns,
        )
        self._expose_landed(source, store, mat_table)  # the engine only READS the landed replica

    async def attach_landed_source(
        self, source: Any, columns: list[tuple[str, str]], *, pk_columns: list[str] | None = None
    ) -> None:
        """Eager reconcile + attach (boot / (re)registration): converge the landing table in the
        store to ``columns`` — survives restart, recreated on a config drift — and expose the
        engine's READ view over it, WITHOUT landing data (that is the refresh's job). Splitting the
        DDL from the DML makes the catalog complete at startup. The engine never writes the store."""
        store = self.ensure_materialize_attached()
        mat_table = _mat_table_name(source)
        await store_writer.reconcile_table(
            self._store_dsn(), schema="mat", table=mat_table, columns=columns, pk_columns=pk_columns
        )
        self._expose_landed(source, store, mat_table)

    def _expose_landed(self, source: Any, store: str, mat_table: str) -> None:
        """Create the engine's physical-named READ view over the landed store table (idempotent)."""
        phys = self._phys_name(source)
        self._con.execute(
            f'CREATE VIEW IF NOT EXISTS {phys} AS SELECT * FROM {store}.mat."{mat_table}"'
        )

    # -- metadata --------------------------------------------------------------

    def introspect_columns(self, source: Any) -> dict[str, str]:
        """Column types as the DuckDB engine reports them for a registered source —
        the engine's metadata view (attach the source, DESCRIBE the physical relation).
        Returns {column_name: duckdb_type_name}. This is the DuckDB implementation of
        the engine-introspection seam (REQ-825/840); callers reach it via EngineRuntime."""
        self.attach_source(source)
        phys = self._phys_name(source)
        res = self._con.execute(f"DESCRIBE {phys}")
        # DESCRIBE rows: (column_name, column_type, null, key, default, extra)
        return columns_from_describe(res.fetchall())

    # -- execution -------------------------------------------------------------

    async def execute(self, physical_or_governed_sql: str) -> QueryResult:
        """Execute physical SQL (post-governance) on the engine (transpiled to DuckDB)."""
        return await self.run(transpile(physical_or_governed_sql, "duckdb"))

    async def run(self, duck_sql: str, params: list | None = None) -> QueryResult:
        """Execute SQL ALREADY in the DuckDB dialect (the backend transpiled it via the seam) against
        the connection, whose attached sources expose every physical ``schema.table`` view."""
        loop = asyncio.get_event_loop()

        def _run() -> QueryResult:
            res = self._con.execute(duck_sql, params) if params else self._con.execute(duck_sql)
            cols = [d[0] for d in res.description] if res.description else []
            return QueryResult(rows=res.fetchall(), column_names=cols)

        return await loop.run_in_executor(None, _run)

    def run_sync(self, duck_sql: str, params: list | None = None) -> QueryResult:
        """Synchronous variant of run() for callers already on a worker thread (Arrow Flight, etc.)."""
        res = self._con.execute(duck_sql, params) if params else self._con.execute(duck_sql)
        return result_from_dbapi(res)

    def close(self) -> None:
        self._con.close()
