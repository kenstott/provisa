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
import os
import shutil
import sqlite3
import tempfile
import threading
from contextlib import contextmanager
from typing import Any

import duckdb

from provisa.executor.result import QueryResult, ResultStream
from provisa.federation import store_writer
from provisa.federation.engine import build_duckdb_engine
from provisa.federation.runtime_support import columns_from_describe, stream_from_dbapi
from provisa.transpiler.transpile import transpile

# Rows per Arrow record batch when lazily streaming the engine result (REQ-1214). Larger than the
# DBAPI row-stream batch (1000) because Arrow batches carry columnar overhead per batch; still bounds
# peak memory to one batch rather than the whole result.
_ARROW_STREAM_BATCH_ROWS = 65_536


def build_vss_index_connection(
    dim: int, rows: list[tuple[str, str, str | None, str | None, list[float]]]
) -> duckdb.DuckDBPyConnection:
    """Embed catalog chunks into a fresh in-process DuckDB VSS (HNSW) index (REQ-1008, MCP catalog
    search — ``api.mcp.search.CatalogSearchIndex``). ``rows`` are pre-computed
    (level, schema, table, column, embedding) tuples; this owns the connect + extension load +
    index DDL so the MCP module has no direct DuckDB dependency. VSS is genuinely
    duckdb-specific (no cross-engine equivalent), hence its own connection rather than the shared
    federation runtime's."""
    from provisa.federation.duckdb_extensions import connect, install_and_load

    con = connect()
    install_and_load(con, "vss", from_community=False)
    con.execute(
        f"CREATE TABLE chunks (level VARCHAR, schema VARCHAR, tbl VARCHAR, "
        f"col VARCHAR, embedding FLOAT[{dim}])"
    )
    con.executemany("INSERT INTO chunks VALUES (?, ?, ?, ?, ?)", rows)
    # Cosine HNSW: query with array_cosine_distance; smaller = more similar.
    con.execute("CREATE INDEX chunk_hnsw ON chunks USING HNSW (embedding) WITH (metric = 'cosine')")
    return con


def _mat_table_name(source: Any) -> str:
    """The internal ``mat`` schema table name for a landed (source, physical table). Keyed by the
    source id AND its physical schema/table so a multi-table materialize-only source lands each
    table in its own store table instead of colliding on the source id. Only the runtime references
    it (through the physical-named view it creates); the compiler never sees it."""
    return f"{source.id}__{source.schema_name}__{source.table_name}"


class _CatalogGate:
    """Readers-writer gate over the shared DuckDB connection's catalog.

    The control-plane refresh (attach_control_plane, SQLite dialect) rebuilds ``provisa_admin`` in
    place: it DROPs the org schema CASCADE, DETACHes the snapshot alias, replaces the snapshot file
    and re-creates every view. A query that binds or scans during that window does not fail — it
    comes back with ZERO ROWS, because the views it resolved point at an alias whose file was swapped
    out from under it. Under the e2e suite that surfaced as meta/ops Cypher queries intermittently
    returning nothing on every worker while /data/graph-counts on the same backend reported the real
    counts. Queries therefore hold the gate for read; the rebuild holds it for write.

    Reader-priority, deliberately: a waiting writer must NOT block new readers. Execution paths that
    stream (run_sync, run_arrow_stream) keep their read hold until the cursor drains, and the thread
    that opened such a stream may itself be the next one to trigger a refresh — writer preference
    would let that thread block on its own outstanding read. A refresh happens only when the control
    plane has actually committed something, so writer starvation is not a live concern."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._readers = 0
        self._writing = False

    def acquire_read(self) -> None:
        with self._cond:
            while self._writing:
                self._cond.wait()
            self._readers += 1

    def release_read(self) -> None:
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    @contextmanager
    def read(self):
        self.acquire_read()
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write(self):
        with self._cond:
            while self._writing or self._readers:
                self._cond.wait()
            self._writing = True
        try:
            yield
        finally:
            with self._cond:
                self._writing = False
                self._cond.notify_all()


class DuckDBFederationRuntime:  # REQ-825, REQ-840, REQ-844
    def __init__(self, *, materialize_dsn: str | None = None) -> None:
        # When PROVISA_DUCKDB_EXT_DIR is set (the embedded tier stages the pinned extension blobs there
        # from the provisa-duckdb-ext PyPI package), load extensions from it and DISABLE network
        # autoinstall — an air-gapped/enterprise install must never silently reach extensions.duckdb.org;
        # a missing extension fails loud instead. Unset (dev/server) keeps DuckDB's default network path.
        _ext_dir = os.environ.get("PROVISA_DUCKDB_EXT_DIR")
        _cfg: dict[str, str | bool | int | float | list[str]] = (
            {"extension_directory": _ext_dir, "autoinstall_known_extensions": False}
            if _ext_dir
            else {}
        )
        self._con = duckdb.connect(config=_cfg)
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
        self._control_plane_attached = False  # provisa_admin catalog (native path only)
        # SQLite control plane only: the engine attaches a private snapshot of the tenant DB, never
        # the live file (see _refresh_control_plane_snapshot). These track that snapshot.
        self._cp_probe: sqlite3.Connection | None = None  # read-only handle on the LIVE file
        self._cp_snapshot_dir = ""  # created with the probe, on the first SQLite control plane
        self._cp_snapshot_path = ""
        self._cp_data_version: int | None = None  # PRAGMA data_version at the last snapshot
        # attach_control_plane runs on whatever worker thread serves the request, and its
        # DETACH -> os.replace -> ATTACH sequence leaves the catalog momentarily unbound. Two
        # threads interleaving there would query a detached alias, so the whole refresh is
        # serialized. The same lock covers _cp_probe, whose PRAGMA data_version caching requires a
        # single long-lived connection (a per-call probe would report a fresh version every time).
        # It is taken BEFORE _catalog_gate on the refresh path; nothing takes them the other way.
        self._cp_lock = threading.Lock()
        # ...and the rebuild is invisible to concurrent queries only if they are excluded from it —
        # see _CatalogGate. Held for read by every execution path, for write by the rebuild.
        self._catalog_gate = _CatalogGate()

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

    def attach_control_plane(self, db_path: str, schema_name: str, dialect: str = "sqlite") -> None:
        """Attach the tenant control-plane DB as the ``provisa_admin`` catalog.

        Trino parity: on Trino, ``provisa_admin`` is a real catalog backed by the Postgres
        control-plane DB (configured via a catalog file). On the native DuckDB tier there is no
        such catalog, so this method provides it by ATTACHing the tenant control-plane DB itself.

        Two dialects:
        - sqlite: ATTACH a private SNAPSHOT of the tenant file READ_ONLY (never the live file —
          see _refresh_control_plane_snapshot), then wrap every table under the schema the
          compiler emits (``org_<id>``), since a SQLite ATTACH flattens everything into ``main``
          with no real multi-schema support. Re-entrant: each call re-snapshots and rebuilds the
          views if the control plane has committed anything since the last one, so a table
          registered after startup is visible to the very next query.
        - postgresql: ATTACH the connection DSN directly (READ_ONLY). DuckDB's postgres extension
          maps every real Postgres schema (including ``schema_name``, and the meta views it
          already holds per api._meta_views) 1:1 under the catalog, so no per-table view-wrapping
          is needed.

        All tables/schemas found are exposed — not a hardcoded subset — so future control-plane
        schema additions are automatically visible without touching this method. Called from
        NativeEngineBackend._attach_registered."""
        if not db_path or db_path == ":memory:":
            return  # in-memory tenant DB (tests/CI without a file): no-op, not an error
        catalog = "provisa_admin"
        if dialect == "postgresql":
            if self._control_plane_attached:
                return
            if not self._pg_ext_loaded:
                self._con.execute("INSTALL postgres")
                self._con.execute("LOAD postgres")
                self._pg_ext_loaded = True
            if catalog not in self._phys_catalogs:
                self._con.execute(f"ATTACH '{db_path}' AS \"{catalog}\" (TYPE postgres, READ_ONLY)")
                self._phys_catalogs.add(catalog)
            self._control_plane_attached = True
            return
        with self._cp_lock:
            scratch = self._refresh_control_plane_snapshot(db_path)
            if scratch is None:
                return  # nothing committed since the last snapshot; the attached views are current
            # Only a real rebuild excludes queries. The no-change path above is the common one — this
            # method runs before EVERY query (NativeEngineBackend._attach_registered), so taking the
            # write gate unconditionally would serialize the whole engine behind one query at a time.
            with self._catalog_gate.write():
                self._rebuild_control_plane(catalog, schema_name, scratch)

    def _rebuild_control_plane(self, catalog: str, schema_name: str, scratch: str) -> None:
        """Swap in a freshly snapshotted control plane. Caller holds _cp_lock and the write gate."""
        if not self._sqlite_loaded:
            self._con.execute("INSTALL sqlite")
            self._con.execute("LOAD sqlite")
            self._sqlite_loaded = True
        raw_alias = "_raw_provisa_admin"
        if catalog not in self._phys_catalogs:
            self._con.execute(f"ATTACH ':memory:' AS \"{catalog}\"")
            self._phys_catalogs.add(catalog)
        else:
            # Rebuild from scratch: the refreshed snapshot may have gained or dropped tables, and
            # the views must be unbound before the stale snapshot file can be detached.
            self._con.execute(f'DROP SCHEMA IF EXISTS "{catalog}"."{schema_name}" CASCADE')
        if raw_alias in self._raw_attached:
            self._con.execute(f'DETACH "{raw_alias}"')
            self._raw_attached.discard(raw_alias)
        # Only now that DuckDB released the previous snapshot can the fresh copy take its place.
        os.replace(scratch, self._cp_snapshot_path)
        self._con.execute(
            f"ATTACH '{self._cp_snapshot_path}' AS \"{raw_alias}\" (TYPE sqlite, READ_ONLY)"
        )
        self._raw_attached.add(raw_alias)
        self._con.execute(f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."{schema_name}"')
        # Enumerate every table from the SQLite file via SHOW TABLES (sqlite_master is not
        # accessible at the 3-part name DuckDB expects after a TYPE sqlite ATTACH).
        for (tbl,) in self._con.execute(f'SHOW TABLES FROM "{raw_alias}"').fetchall():
            view = f'"{catalog}"."{schema_name}"."{tbl}"'
            remote = f'"{raw_alias}"."main"."{tbl}"'
            self._con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {remote}")
        self._control_plane_attached = True

    def _refresh_control_plane_snapshot(self, db_path: str) -> str | None:
        """Copy the live control-plane SQLite file into a private snapshot; return the path of the
        fresh copy, or None if the control plane has committed nothing since the last one.

        DuckDB's sqlite extension cannot read a SQLite file that another connection is writing:
        doing so corrupts the database and kills the process with SIGBUS. Verified against both
        READ_ONLY and read-write ATTACH, and both with and without explicit WAL checkpoints — a
        plain sqlite3 read-only reader survives the identical workload, so this is specific to the
        extension and not something WAL mode can make safe. The control-plane file is written
        continuously by aiosqlite (SQLAlchemy), so the engine attaches a copy and never the
        original.

        ``sqlite3.Connection.backup`` is SQLite's supported online-backup API: it yields a
        consistent point-in-time copy while a writer is active. ``PRAGMA data_version`` on the
        long-lived reader changes exactly when another connection commits, so an unchanged value
        means the existing snapshot is still current and no copy is needed."""
        if self._cp_probe is None:
            # check_same_thread=False: the probe outlives the request that created it and is read
            # from whichever worker thread later serves a query. _cp_lock is what makes that safe —
            # every use of the probe happens under it.
            self._cp_probe = sqlite3.connect(
                f"file:{db_path}?mode=ro", uri=True, check_same_thread=False
            )
            self._cp_snapshot_dir = tempfile.mkdtemp(prefix="provisa-control-plane-snapshot-")
            self._cp_snapshot_path = os.path.join(self._cp_snapshot_dir, "control_plane.sqlite")
        version = self._cp_probe.execute("PRAGMA data_version").fetchone()[0]
        if version == self._cp_data_version:
            return None
        # The caller detaches the previous snapshot only after this returns, so write the new copy
        # to a scratch path for it to move into place — never overwrite a file DuckDB has open.
        scratch = f"{self._cp_snapshot_dir}/control_plane.sqlite.new"
        dst = sqlite3.connect(scratch)
        try:
            self._cp_probe.backup(dst)
        finally:
            dst.close()
        self._cp_data_version = version
        return scratch

    # The materialization store, attached under this backend-neutral alias. A store MUST exist (the
    # engine's invariant); its backend/dialect is taken from the store URL scheme, never assumed.
    _MAT_STORE = "mat_store"
    # DuckDB ATTACH type per store URL scheme. postgres/sqlite attach via their extensions; duckdb
    # is core (a DuckDB file attaches directly, no extension). sqlite/duckdb attach a FILE PATH,
    # postgres attaches the full connection URL. A duckdb store has its own sync write face
    # (federation.store_connection) since it lacks an async SQLAlchemy driver — this is the
    # fully-embedded zero-config store (REQ-989).
    _ATTACH_TYPE_BY_SCHEME = {
        "postgresql": "postgres",
        "postgres": "postgres",
        "sqlite": "sqlite",
        "duckdb": "duckdb",
    }
    _FILE_ATTACH_TYPES = frozenset({"sqlite", "duckdb"})  # ATTACH a file path, not a URL
    _NO_EXTENSION_TYPES = frozenset({"duckdb"})  # core store type: no INSTALL/LOAD needed

    def mv_store_schema(self, org_id: str) -> str:
        """The schema MVs materialize into — the SAME store schema source-landing writes to (``mat`` /
        ``main``). The embedded store is already the org's isolated store, so ``org_id`` is unused
        (no org-scoped namespace needed, unlike a shared Postgres store-engine)."""
        del org_id
        return self._store_schema()

    def _store_schema(self) -> str:
        """The schema the landed replicas live in WITHIN the store. Schema-capable stores (postgres)
        isolate them under ``mat``; a schema-less store (sqlite) has no namespaces, so they land in
        its default ``main`` schema. Landing, reconcile, and the engine's READ view all use this."""
        from urllib.parse import urlparse

        scheme = urlparse(self._store_dsn()).scheme.split("+", 1)[0]
        return "main" if scheme == "sqlite" else "mat"

    def _store_dsn(self) -> str:
        """The materialization-store DSN: the explicit constructor override, else the engine's
        invariant resolution (configured → declared default → error). Never a fallback."""
        return (
            self._materialize_dsn
            if self._materialize_dsn is not None
            else (self._engine.materialize_store())
        )

    def _store_is_duckdb(self) -> bool:
        """True when the materialization store is an embedded DuckDB file (REQ-989). A DuckDB store is
        single-writer, so it is landed through THIS engine's own connection (which already holds it
        attached), not the separate server-relational write face."""
        from urllib.parse import urlparse

        return urlparse(self._store_dsn()).scheme.split("+", 1)[0] == "duckdb"

    def ensure_materialize_attached(self) -> str:
        """ATTACH the materialization store under ``mat_store`` (idempotent); return the alias. The
        DuckDB ATTACH type is derived from the store URL scheme; the driver parses the URL and owns
        its own defaults — the runtime injects none. A missing store is a hard error (via _store_dsn)."""
        dsn = self._store_dsn()
        if not self._store_attached:
            from sqlalchemy import make_url

            url = make_url(dsn)
            scheme = url.get_backend_name()
            store_type = self._ATTACH_TYPE_BY_SCHEME.get(scheme)
            if store_type is None:
                raise RuntimeError(f"materialize store scheme {scheme!r} is not attachable")
            if store_type not in self._NO_EXTENSION_TYPES:
                self._con.execute(f"INSTALL {store_type}")
                self._con.execute(f"LOAD {store_type}")
            # A file-backed store (sqlite/duckdb) attaches the FILESYSTEM PATH; postgres attaches the
            # full URL. Use make_url(...).database, not urlparse(...).path: on Windows a
            # ``duckdb:///C:\...`` DSN parses to ``/C:\...`` under urlparse (leading slash), which
            # DuckDB then reads as a ``//C:`` UNC network path and fails ("network path not found").
            # SQLAlchemy's URL parser strips the leading slash for a drive-letter path on every OS.
            target = url.database if store_type in self._FILE_ATTACH_TYPES else dsn
            self._con.execute(f"ATTACH '{target}' AS {self._MAT_STORE} (TYPE {store_type})")
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
        if self._store_is_duckdb():
            # DuckDB store is single-writer: land through THIS engine's own connection (REQ-989).
            from provisa.federation.store_connection import land_duckdb_native

            land_duckdb_native(
                self._con,
                catalog=store,
                schema=self._store_schema(),
                table=mat_table,
                columns=columns,
                rows=rows,
                change_signal=change_signal,
                watermark_column=watermark_column,
            )
        else:
            # Land through the ONE server-store write face — the engine never writes that store.
            await store_writer.land(
                self._store_dsn(),
                schema=self._store_schema(),
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
        if self._store_is_duckdb():
            from provisa.federation.store_connection import reconcile_duckdb_native

            reconcile_duckdb_native(
                self._con,
                catalog=store,
                schema=self._store_schema(),
                table=mat_table,
                columns=columns,
            )
        else:
            await store_writer.reconcile_table(
                self._store_dsn(),
                schema=self._store_schema(),
                table=mat_table,
                columns=columns,
                pk_columns=pk_columns,
            )
        self._expose_landed(source, store, mat_table)

    async def land_table(
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
        """Land ``rows`` into a store table already named ``schema.table`` (the per-fire source
        refresh path — no source-object physical-name exposure, that is boot-time reconcile's job).
        Duckdb-native dispatch mirroring ``materialize_source``: through the engine's own connection
        for an embedded DuckDB store (REQ-989), else the server-store write face."""
        store = self.ensure_materialize_attached()
        if self._store_is_duckdb():
            from provisa.federation.store_connection import land_duckdb_native

            return land_duckdb_native(
                self._con,
                catalog=store,
                schema=schema,
                table=table,
                columns=columns,
                rows=rows,
                change_signal=change_signal,
                watermark_column=watermark_column,
            )
        return await store_writer.land(
            self._store_dsn(),
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
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str] | None = None,
    ) -> str:
        """Converge an MV's OWN store table to its output ``columns`` (REQ-970). Duckdb-native
        dispatch mirroring ``attach_landed_source``: through the engine's own connection for an
        embedded DuckDB store (REQ-989), else the server-store write face."""
        store = self.ensure_materialize_attached()
        if self._store_is_duckdb():
            from provisa.federation.store_connection import reconcile_duckdb_native

            return reconcile_duckdb_native(
                self._con, catalog=store, schema=schema, table=table, columns=columns
            )
        return await store_writer.reconcile_table(
            self._store_dsn(), schema=schema, table=table, columns=columns, pk_columns=pk_columns
        )

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
        outcome (REQ-965). Duckdb-native dispatch mirroring ``materialize_source``: through the
        engine's own connection for an embedded DuckDB store (REQ-989), else the server-store write
        face."""
        store = self.ensure_materialize_attached()
        if self._store_is_duckdb():
            from provisa.federation.store_connection import persist_duckdb_native

            return persist_duckdb_native(
                self._con,
                catalog=store,
                schema=schema,
                table=table,
                columns=columns,
                rows=rows,
                persist=persist,
                pk_columns=pk_columns,
            )
        return await store_writer.persist_land(
            self._store_dsn(),
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            persist=persist,
            pk_columns=pk_columns,
            match_floor=match_floor,
        )

    def _expose_landed(self, source: Any, store: str, mat_table: str) -> None:
        """Create the engine's physical-named READ view over the landed store table (idempotent)."""
        phys = self._phys_name(source)
        self._con.execute(
            f"CREATE VIEW IF NOT EXISTS {phys} AS "
            f'SELECT * FROM {store}."{self._store_schema()}"."{mat_table}"'
        )

    # -- metadata --------------------------------------------------------------

    def introspect_columns(self, source: Any) -> dict[str, str]:
        """Column types as the DuckDB engine reports them for a registered source —
        the engine's metadata view (attach the source, DESCRIBE the physical relation).
        Returns {column_name: duckdb_type_name}. This is the DuckDB implementation of
        the engine-introspection seam (REQ-825/840); callers reach it via EngineRuntime."""
        self.attach_source(source)
        phys = self._phys_name(source)
        # PRIVATE cursor: introspection runs on request threads concurrently with queries, and the
        # shared connection holds only one pending result (see run()).
        cur = self._con.cursor()
        try:
            res = cur.execute(f"DESCRIBE {phys}")
            # DESCRIBE rows: (column_name, column_type, null, key, default, extra)
            return columns_from_describe(res.fetchall())
        finally:
            cur.close()

    # -- execution -------------------------------------------------------------

    async def execute(self, physical_or_governed_sql: str) -> QueryResult:
        """Execute physical SQL (post-governance) on the engine (transpiled to DuckDB)."""
        return await self.run(transpile(physical_or_governed_sql, "duckdb"))

    async def run(self, duck_sql: str, params: list | None = None) -> QueryResult:
        """Execute SQL ALREADY in the DuckDB dialect (the backend transpiled it via the seam) against
        the connection, whose attached sources expose every physical ``schema.table`` view."""
        loop = asyncio.get_event_loop()

        def _run() -> QueryResult:
            # Read gate: a control-plane rebuild swaps the provisa_admin snapshot out from under any
            # query already bound to it, which returns zero rows rather than failing (_CatalogGate).
            with self._catalog_gate.read():
                # A PRIVATE cursor, never the shared connection: run() is dispatched to an executor
                # thread, so two queries overlap routinely. A DuckDB connection holds ONE pending
                # result — the second execute() replaces the first, and the first thread's fetchall()
                # then returns an EMPTY list rather than raising. That is what made meta/admin
                # queries intermittently come back with no rows under the parallel e2e suite while
                # the control plane plainly held the data (run_sync and run_arrow_stream already
                # took a cursor for this reason).
                cur = self._con.cursor()
                try:
                    res = cur.execute(duck_sql, params) if params else cur.execute(duck_sql)
                    cols = [d[0] for d in res.description] if res.description else []
                    types = [str(d[1]) for d in res.description] if res.description else []
                    return QueryResult(rows=res.fetchall(), column_names=cols, column_types=types)
                finally:
                    cur.close()

        return await loop.run_in_executor(None, _run)

    def run_sync(self, duck_sql: str, params: list | None = None) -> ResultStream:
        """Synchronous variant of run() for callers already on a worker thread (Arrow Flight, etc.).

        Streams rows lazily over a PRIVATE cursor (batched ``fetchmany``) so a large result never
        fully materializes; the cursor is closed when the stream drains (REQ-028). A private cursor
        (not the shared connection) keeps concurrent worker-thread queries from corrupting each
        other's fetch state. Consumers that call ``.rows`` still get the full list — the buffering
        is then explicit at their call site."""
        # The read gate is held until the stream drains, not just past execute(): the rows are pulled
        # from the cursor lazily, so the scan is still live and a rebuild mid-drain would empty it.
        self._catalog_gate.acquire_read()
        try:
            cur = self._con.cursor()
            cur.execute(duck_sql, params) if params else cur.execute(duck_sql)
        except BaseException:
            self._catalog_gate.release_read()
            raise

        def _close(*_: Any) -> None:
            cur.close()
            self._catalog_gate.release_read()

        return stream_from_dbapi(cur, on_close=_close)

    # -- Arrow transport (REQ-986) ---------------------------------------------

    def run_arrow(self, duck_sql: str, params: list | None = None):
        """Execute dialect-DuckDB SQL and return a ``pyarrow.Table`` — DuckDB produces Arrow natively
        (``fetch_arrow_table``), so no Python rows are materialized for the Flight transport."""
        with self._catalog_gate.read():
            # PRIVATE cursor for the same reason as run_sync/run_arrow_stream: a concurrent query on
            # the shared connection replaces this one's pending result, and the fetch then yields
            # nothing instead of raising.
            cur = self._con.cursor()
            try:
                res = cur.execute(duck_sql, params) if params else cur.execute(duck_sql)
                return res.to_arrow_table()
            finally:
                cur.close()

    def run_arrow_stream(self, duck_sql: str, params: list | None = None):
        """Execute dialect-DuckDB SQL and return ``(schema, batch_generator)`` for lazy record-batch
        streaming through the Flight server's GeneratorStream (REQ-986, REQ-1214).

        Truly lazy: the batches are pulled from a PRIVATE cursor's Arrow record-batch reader
        (``fetch_record_batch``) on demand, so the full result never materializes — peak memory is
        bounded by one record batch, not the total result size. A private cursor (not the shared
        connection) keeps concurrent worker-thread streams from corrupting each other's fetch state;
        it is closed when the generator drains or the consumer stops early."""
        # Held for the whole stream — same reason as run_sync: the batches are scanned on demand.
        self._catalog_gate.acquire_read()
        try:
            cur = self._con.cursor()
            cur.execute(duck_sql, params) if params else cur.execute(duck_sql)
            reader = cur.fetch_record_batch(_ARROW_STREAM_BATCH_ROWS)
            schema = reader.schema
        except BaseException:
            self._catalog_gate.release_read()
            raise

        def _batches():
            try:
                for batch in reader:
                    yield batch
            finally:
                cur.close()
                self._catalog_gate.release_read()

        return schema, _batches()

    def close(self) -> None:
        self._con.close()
        with self._cp_lock:  # never tear the probe/snapshot out from under a refresh in flight
            if self._cp_probe is not None:
                self._cp_probe.close()
                self._cp_probe = None
            if self._cp_snapshot_dir:
                shutil.rmtree(self._cp_snapshot_dir)
                self._cp_snapshot_dir = ""
                self._cp_snapshot_path = ""
