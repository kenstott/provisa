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
import logging
import os
import shutil
import sqlite3
import tempfile
import re
import threading
from contextlib import contextmanager
from typing import Any

import duckdb

from provisa.executor.result import QueryResult, ResultStream
from provisa.federation import store_writer
from provisa.federation.engine import build_duckdb_engine
from provisa.core import request_deadline
from provisa.federation.land_guard import LandGuard
from provisa.federation.runtime_support import columns_from_describe, stream_from_dbapi
from provisa.transpiler.transpile import transpile

log = logging.getLogger(__name__)

# Rows per Arrow record batch when lazily streaming the engine result (REQ-1214). Larger than the
# DBAPI row-stream batch (1000) because Arrow batches carry columnar overhead per batch; still bounds
# peak memory to one batch rather than the whole result.
_ARROW_STREAM_BATCH_ROWS = 65_536


def _file_stat_pair(path: str) -> tuple[int, int] | None:
    """(mtime_ns, size) for *path*, or None if it does not exist (e.g. no ``-wal`` sidecar yet —
    a WAL-mode db has none until its first write). Used as the control-plane refresh canary; see
    DuckDBFederationRuntime._refresh_control_plane_snapshot for why a kernel-level file stat
    replaced PRAGMA data_version there."""
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


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


# REQ-1901: a `mat_store.<schema>.<table>` read on the engine connection (quoted or bare parts).
_MAT_STORE_REF = re.compile(
    r'(?<![\w"])"?mat_store"?\s*\.\s*(?:"([^"]+)"|([^".\s]+))'
    r'\s*\.\s*(?:"([^"]+)"|([^".\s,;()]+))'
)


def _store_ref(match: re.Match[str]) -> tuple[str, str]:
    """The (schema, table) a ``_MAT_STORE_REF`` match names."""
    return match.group(1) or match.group(2), match.group(3) or match.group(4)


_LOCAL_STORE_SCHEMA = "_mat_store_local"


def _local_store_name(schema: str, table: str) -> str:
    """The local table a `mat_store.<schema>.<table>` read is served from on this connection."""
    return f'"{_LOCAL_STORE_SCHEMA}"."{schema}__{table}"'


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
        self._httpfs_loaded = False  # httpfs INSTALL/LOAD for S3-compatible (e.g. R2) sources
        self._store_attached = False  # materialization-store ATTACH (distinct from source attaches)
        # REQ-1901: set instead of a `mat_store` ATTACH when the store is embedded DuckDB — see
        # ensure_materialize_attached. A statement's `mat_store.<schema>.<table>` reads (a replica,
        # a materialized view) are served by the query-time refresh in run/run_sync/run_arrow/
        # run_arrow_stream, which copies a fresh snapshot from the broker before the query runs.
        self._store_broker: Any = None
        # Guards the register()+CREATE TABLE pair in _refresh_store_relations: run()/run_arrow()
        # dispatch to a thread pool, so two concurrent queries touching the same store table
        # would otherwise race registering under the same temp name.
        self._store_relation_lock = threading.Lock()
        # local copy target -> the store canary it was copied at (see _copy_store_table).
        self._store_copy_canary: dict[str, Any] = {}
        self._phys_catalogs: set[str] = set()  # in-memory catalogs holding the physical views
        # REQ-899: ClickHouse tables read live over ClickHouse's HTTP interface — physical-name key
        # (lowercased catalog, schema, table) -> relation. No view stands at that name: every
        # statement naming one is rewritten to a read_parquet() of it (clickhouse_http_scan).
        self._ch_relations: dict[tuple[str, str, str], Any] = {}
        # attach_source (request/prepare threads) writes it while statements on other threads read
        # it; readers take a snapshot under the lock.
        self._ch_lock = threading.Lock()
        self._raw_attached: set[str] = set()  # source ids whose remote DB is already ATTACHed
        self._ext_loaded: set[str] = (
            set()
        )  # community/extension connectors LOADed on this connection
        self._control_plane_attached = False  # provisa_admin catalog (native path only)
        # SQLite control plane only: the engine attaches a private snapshot of the tenant DB, never
        # the live file (see _refresh_control_plane_snapshot). These track that snapshot.
        self._cp_snapshot_dir = ""  # created lazily, on the first SQLite control plane refresh
        self._cp_snapshot_path = ""
        # (main-file, wal-file) os.stat (mtime_ns, size) pair as of the last snapshot — see
        # _refresh_control_plane_snapshot for why this replaced PRAGMA data_version (and why the
        # backup source connection it drives is opened fresh every time, never kept long-lived).
        self._cp_canary: tuple[tuple[int, int] | None, tuple[int, int] | None] | None = None
        # attach_control_plane runs on whatever worker thread serves the request, and its
        # DETACH -> os.replace -> ATTACH sequence leaves the catalog momentarily unbound. Two
        # threads interleaving there would query a detached alias, so the whole refresh is
        # serialized. It is taken BEFORE _catalog_gate on the refresh path; nothing takes them the
        # other way.
        self._cp_lock = threading.Lock()
        # ...and the rebuild is invisible to concurrent queries only if they are excluded from it —
        # see _CatalogGate. Held for read by every execution path, for write by the rebuild.
        self._catalog_gate = _CatalogGate()
        # One store connection, so one write on it at a time (two lands interleaving on it was a
        # confirmed regression). A lock serializes it, and each write runs on the thread that asked
        # for it — a read-triggered land stays on its request's thread (REQ-1882), where a
        # one-worker pool took it off.
        self._land_guard = LandGuard("DuckDB store")

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
        if "clickhouse_http" in details:
            self._attach_clickhouse(source, details)
        elif "view_ddl" in details:  # csv / parquet scanner, or another view_ddl-based connector
            # REQ-1742 gap: this branch only ever installed httpfs (needed by csv/parquet's own
            # secret_ddl) — a scanner connector with its OWN DuckDB extension (e.g.
            # DuckDBGsheetsConnector's `extension = "gsheets"`) never got that extension
            # installed/loaded on THIS connection, so its view_ddl's table function (read_gsheet)
            # raised a Catalog Error ("... does not exist") on a fresh runtime that never probed
            # it — silently caught by introspect_columns' duckdb.Error handler, so the Register
            # Table form's column list was just empty with no visible error. Mirrors _attach_raw's
            # already-generic extension-loading below, for the view_ddl-based connectors too.
            connector = self._engine.connector_for(source.type.value)
            ext = getattr(connector, "extension", None)
            if ext and ext not in self._ext_loaded:
                if getattr(connector, "install_from_community", False):
                    self._con.execute(f"INSTALL {ext} FROM community")
                else:
                    self._con.execute(f"INSTALL {ext}")
                self._con.execute(f"LOAD {ext}")
                self._ext_loaded.add(ext)
            secret_ddl = details.get("secret_ddl")
            if secret_ddl and not self._httpfs_loaded:
                self._con.execute("INSTALL httpfs")
                self._con.execute("LOAD httpfs")
                self._httpfs_loaded = True
            if secret_ddl:
                self._con.execute(secret_ddl)
            scan = details["view_ddl"].split(" AS ", 1)[1]
            self._con.execute(f"CREATE OR REPLACE VIEW {phys} AS {scan}")
        else:  # ATTACH postgres / sqlite / extension source once, then view the remote table
            raw_alias = self._attach_raw(source, details)
            remote_schema = details.get("remote_schema", source.schema_name)
            remote = f'"{raw_alias}"."{remote_schema}"."{source.table_name}"'
            self._con.execute(f"CREATE OR REPLACE VIEW {phys} AS SELECT * FROM {remote}")

    def detach_source(self, source: Any) -> None:
        """Remove the live exposure of ``source``'s table from this engine: the view at its
        catalog-physical name (checked against the catalog, so nothing else is dropped) and any
        ClickHouse HTTP relation registered for it. Called when the table's reads move to its
        replica (REQ-1912): nothing on the engine may then read the source."""
        from provisa.core.catalog import _to_catalog_name

        parts = [_to_catalog_name(source.id), source.schema_name, source.table_name]
        with self._ch_lock:
            self._ch_relations.pop((parts[0].lower(), parts[1].lower(), parts[2].lower()), None)
        if parts[0] not in self._phys_catalogs:
            return  # nothing of this source was ever exposed on this connection
        is_view = self._con.execute(
            "SELECT 1 FROM duckdb_views() WHERE database_name = ? AND schema_name = ? "
            "AND view_name = ?",
            parts,
        ).fetchone()
        if is_view is not None:
            self._con.execute(f'DROP VIEW "{parts[0]}"."{parts[1]}"."{parts[2]}"')

    def _attach_clickhouse(self, source: Any, details: dict) -> None:
        """REQ-899: register a ClickHouse table for the query-time HTTP read. Loads httpfs, creates
        the source's http secret (credentials only there — never in a URL or a log line), and reads
        the table's (name, type) list from ClickHouse's system.columns over the same interface; a
        table ClickHouse does not have raises here, at attach."""
        from provisa.core.catalog import _to_catalog_name
        from provisa.federation.clickhouse_http_scan import (
            ClickHouseRelation,
            columns_query,
            read_url,
        )

        if not self._httpfs_loaded:
            self._con.execute("INSTALL httpfs")
            self._con.execute("LOAD httpfs")
            self._httpfs_loaded = True
        self._con.execute(details["secret_ddl"])
        base_url = details["clickhouse_http"]
        url = read_url(
            base_url,
            columns_query(source.schema_name, source.table_name),
            deadline_s=request_deadline.remaining(),
        )
        cur = self._open_cursor(live_http=True)
        try:
            with request_deadline.cancel_on_deadline(cur.interrupt):
                rows = cur.execute(f"SELECT name, type FROM read_parquet('{url}')").fetchall()
        finally:
            cur.close()
        if not rows:
            raise ValueError(
                f"ClickHouse table {source.schema_name}.{source.table_name} (source "
                f"{source.id!r}) does not exist or has no columns"
            )
        key = (
            _to_catalog_name(source.id).lower(),
            source.schema_name.lower(),
            source.table_name.lower(),
        )
        relation = ClickHouseRelation(
            base_url=base_url,
            database=source.schema_name,
            table=source.table_name,
            columns=tuple((str(n), str(t)) for n, t in rows),
        )
        with self._ch_lock:
            self._ch_relations[key] = relation

    def _rewrite_clickhouse_relations(
        self,
        duck_sql: str,
        params: list | None,
        *,
        deadline_s: float | None,
        describe: bool = False,
    ) -> tuple[str, bool]:
        """REQ-899: ``duck_sql`` with every registered ClickHouse table replaced by its live HTTP
        read (projection + pushed literal predicates), and whether any was. The hook sits beside
        _refresh_store_relations in every execution path: it runs on the governed engine statement,
        so RLS/masking are already in it and the outer predicates stay — a read only narrows. A
        failed read raises from execute; nothing reroutes to row_materialize or a landing."""
        with self._ch_lock:
            relations = dict(self._ch_relations)
        if not relations:
            return duck_sql, False
        from provisa.federation.clickhouse_http_scan import rewrite

        rewritten = rewrite(duck_sql, params, relations, deadline_s=deadline_s, describe=describe)
        return (duck_sql, False) if rewritten is None else (rewritten, True)

    def _open_cursor(self, *, live_http: bool) -> Any:
        """A private cursor; for a statement reading ClickHouse over HTTP it downloads each result
        whole (``force_download``): ClickHouse answers per request with no range support, and
        DuckDB's default HEAD + ranged GETs made ClickHouse execute the query twice. It also turns
        httpfs retries off: a retry re-runs the whole ClickHouse query with a fresh
        max_execution_time, so three retries carried a 2s-budget read to 13s (integration test); a
        failed read raises instead. Both settings are session-scoped, so they stay on this cursor
        (verified: a sibling cursor still reads the defaults)."""
        cur = self._con.cursor()
        if live_http:
            cur.execute("SET force_download = true")
            cur.execute("SET http_retries = 0")
        return cur

    def _attach_raw(self, source: Any, details: dict) -> str:
        """ATTACH the source's remote database under its private alias (once), loading the DuckDB
        extension its connector rides on first, and return the alias. The connector declares WHERE
        it exposes tables (postgres keeps its own schema, sqlite lands everything under ``main``,
        mongo maps databases to schemas); the runtime never hardcodes a per-type layout."""
        if source.type.value == "sqlite" and not self._sqlite_loaded:
            self._con.execute("INSTALL sqlite")
            self._con.execute("LOAD sqlite")
            self._sqlite_loaded = True
        elif source.type.value == "postgresql" and not self._pg_ext_loaded:
            self._con.execute("INSTALL postgres")
            self._con.execute("LOAD postgres")
            self._pg_ext_loaded = True
        else:
            # REQ-1673: a community-extension connector (mongo, mssql, firebird, …) needs its
            # extension loaded on THIS connection — the startup probe loaded it on another one.
            connector = self._engine.connector_for(source.type.value)
            ext = getattr(connector, "extension", None)
            if ext and ext not in self._ext_loaded:
                if getattr(connector, "install_from_community", False):
                    self._con.execute(f"INSTALL {ext} FROM community")
                else:
                    self._con.execute(f"INSTALL {ext}")
                self._con.execute(f"LOAD {ext}")
                self._ext_loaded.add(ext)
        raw_alias = details.get("raw_alias", source.id)
        if raw_alias not in self._raw_attached:
            self._con.execute(details["attach"])
            self._raw_attached.add(raw_alias)
        return raw_alias

    # -- source introspection without a registered table (REQ-1673) -------------------------------

    def _attached_alias(self, source: Any) -> str | None:
        """The raw-attached alias for an ATTACH source; None for a scanner (view_ddl) source, which
        has no database to list schemas and tables from."""
        details = self._engine.resolve(source).details
        if "view_ddl" in details or "attach" not in details:
            return None
        return self._attach_raw(source, details)

    def introspect_schemas(self, source: Any) -> list[str]:
        """The schemas of the source's remote database, read from the attached catalog —
        what Register Table lists for a source with no table registered yet.

        Tries duckdb_schemas() first, not information_schema.schemata — profiled live (REQ-1730
        engine-swap investigation, same cost store_connection.py::_existing_columns had):
        information_schema's cross-catalog views scale with the TOTAL number of tables/schemas
        across every attached catalog, not just the one being filtered for, so this call got
        dramatically slower as more sources registered during a session, timing out a
        not-yet-registered source's schema/table picker once enough OTHER sources had landed.
        duckdb_schemas() doesn't have that cost — but it's blind to an extension-backed VIRTUAL
        source (verified live against DuckDB's mongo extension: duckdb_tables() returns nothing
        for an attached Mongo-style catalog, since that extension apparently only plumbs its
        virtual collections through the information_schema compatibility view, not DuckDB's own
        internal catalog table functions). An attached-but-genuinely-empty database is not a
        real-world case for Register Table (nothing to pick would mean nothing to register), so
        falling back to information_schema only when the fast path comes back empty gets the
        speed win for real catalogs (firebird, airport, the SQL warehouses) without going wrong
        for virtual ones (mongo, and presumably redis/elasticsearch/cassandra the same way).
        """
        self._require_discovery_ready(source)
        alias = self._attached_alias(source)
        if alias is None:
            return []
        cur = self._con.cursor()
        try:
            fast = cur.execute(
                "SELECT schema_name FROM duckdb_schemas() WHERE database_name = ? "
                "AND schema_name NOT IN ('information_schema', 'pg_catalog') ORDER BY schema_name",
                [alias],
            ).fetchall()
            if fast:
                return [r[0] for r in fast]
            res = cur.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ? "
                "AND schema_name NOT IN ('information_schema', 'pg_catalog') ORDER BY schema_name",
                [alias],
            )
            return [r[0] for r in res.fetchall()]
        finally:
            cur.close()

    def _require_discovery_ready(self, source: Any) -> None:
        """REQ-1824: for a files/sharepoint/splunk source, `_attached_alias` below would otherwise
        block for up to SERVER_READY_SECONDS (a bundled JVM's full startup — for a large `files`
        directory, Calcite's eager schema scan can take much longer than that) inside what is meant
        to be a quick discovery call for the Register Table form. Fail fast with
        SourceStillStartingError instead of hanging the whole HTTP request; the caller
        (schema_query.py's available_schemas/available_tables) lets that propagate as a
        recognizable error the frontend polls on. A real query's own attach (attach_source, used at
        actual SELECT time) is UNCHANGED — it still waits the full SERVER_READY_SECONDS, since a
        query has no useful way to proceed without the attach either way."""
        from provisa.federation.pgwire_replica import (
            PGWIRE_REPLICA_TYPES,
            _source_type as _pgwire_source_type,
            ensure_endpoint_for_discovery,
        )

        if _pgwire_source_type(source) not in PGWIRE_REPLICA_TYPES:
            return
        ensure_endpoint_for_discovery(source)

    def introspect_tables(self, source: Any, schema_name: str) -> list[str]:
        """The tables of one schema of the source's remote database (see introspect_schemas)."""
        self._require_discovery_ready(source)
        alias = self._attached_alias(source)
        if alias is None:
            return []
        cur = self._con.cursor()
        try:
            fast = cur.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name = ? "
                "AND schema_name = ? ORDER BY table_name",
                [alias, schema_name],
            ).fetchall()
            if fast:
                return [r[0] for r in fast]
            res = cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = ? "
                "AND table_schema = ? ORDER BY table_name",
                [alias, schema_name],
            )
            return [r[0] for r in res.fetchall()]
        finally:
            cur.close()

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
        continuously by the control plane's SQLAlchemy engine, so the engine attaches a copy and never the
        original.

        ``sqlite3.Connection.backup`` is SQLite's supported online-backup API: it yields a
        consistent point-in-time copy while a writer is active.

        Change detection uses OS-level file metadata (mtime + size of the main db file and its
        ``-wal`` sidecar) rather than ``PRAGMA data_version`` on the long-lived probe. That pragma
        is SQLite's own documented signal and normally tracks external commits correctly, but a
        long-lived ``mode=ro`` reader was observed (REQ-1771's ingest catalog wiring, which for the
        first time made a query's CORRECTNESS — not just an admin dashboard's eventual consistency —
        depend on this refresh firing on every single write) to simply STOP advancing after enough
        external commits/checkpoints against this control-plane file: a WAL reader-snapshot edge
        case, not something a plain ``sqlite3`` connection reconnect can safely paper over (a freshly
        reopened reader's first ``PRAGMA data_version`` read reports a low session-relative baseline
        regardless of the file's real history, so "reconnect and recheck" can't distinguish "nothing
        changed" from "reconnected too late to see it"). Once data_version got stuck, every later
        write was silently invisible forever — a landed ingest row (or ANY control-plane write) never
        appeared under ``provisa_admin`` again for the life of the process. mtime/size are kernel
        facts about the file itself; they cannot get stuck the way a reader's cached session state
        can, and unlike data_version they stay meaningful across a reconnect.

        The backup SOURCE connection is opened fresh for every copy, not reused (REQ-1771): a
        single long-lived ``mode=ro`` reader's ``.backup()`` was ALSO observed going stale under
        sustained write churn — after enough prior backups on the same connection, it kept copying
        an old MVCC snapshot even once the mtime/size canary above correctly noticed the file had
        changed and asked for a fresh copy, so the row a landed source had actually committed
        stayed invisible no matter how many later writes triggered another rebuild attempt. A new
        connection has no stale snapshot to be stuck on; the per-refresh cost of opening one is
        negligible next to the backup + DROP/CREATE VIEW work already done on every real change."""
        if self._cp_snapshot_dir == "":
            self._cp_snapshot_dir = tempfile.mkdtemp(prefix="provisa-control-plane-snapshot-")
            self._cp_snapshot_path = os.path.join(self._cp_snapshot_dir, "control_plane.sqlite")
        canary = (_file_stat_pair(db_path), _file_stat_pair(f"{db_path}-wal"))
        if canary == self._cp_canary and self._cp_canary is not None:
            return None
        # The caller detaches the previous snapshot only after this returns, so write the new copy
        # to a scratch path for it to move into place — never overwrite a file DuckDB has open.
        scratch = f"{self._cp_snapshot_dir}/control_plane.sqlite.new"
        src = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        dst = sqlite3.connect(scratch)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        self._cp_canary = canary
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
        """The schema materialized views are written to: one that holds nothing else (REQ-1912).
        Replicas have their own (``replica_address.replica_schema``)."""
        from provisa.federation.replica_address import mv_schema

        return mv_schema(org_id)

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
        its own defaults — the runtime injects none. A missing store is a hard error (via _store_dsn).

        REQ-1901: a `duckdb`-scheme (embedded, single-writer) store is NEVER attached on THIS
        connection at all — DuckDB's file lock is exclusive regardless of requested access mode
        (confirmed empirically across three topologies: full mesh, hub-and-spoke, plain
        writer/reader pair), so under `--workers N` any second connection to the same file, in any
        mode, deadlocks or crashes against the first. Instead this runtime routes every operation
        against the store through `materialize_broker.get_broker()` — the one process-wide (or, if
        elected, cross-process) singleton connection (see that module). This runtime's own `self._con`
        never holds `mat_store` attached; `self._store_broker` is set instead, and every duckdb-store
        call site below (reconcile_replica/land_table/apply_cdc_events/reconcile_mv_table/
        persist_mv_table, plus the query-time relation refresh in run/run_sync/run_arrow/
        run_arrow_stream) goes through it."""
        dsn = self._store_dsn()
        if not self._store_attached:
            from sqlalchemy import make_url

            url = make_url(dsn)
            scheme = url.get_backend_name()
            store_type = self._ATTACH_TYPE_BY_SCHEME.get(scheme)
            if store_type is None:
                raise RuntimeError(f"materialize store scheme {scheme!r} is not attachable")
            # A file-backed store (sqlite/duckdb) attaches the FILESYSTEM PATH; postgres attaches the
            # full URL. Use make_url(...).database, not urlparse(...).path: on Windows a
            # ``duckdb:///C:\...`` DSN parses to ``/C:\...`` under urlparse (leading slash), which
            # DuckDB then reads as a ``//C:`` UNC network path and fails ("network path not found").
            # SQLAlchemy's URL parser strips the leading slash for a drive-letter path on every OS.
            target = url.database if store_type in self._FILE_ATTACH_TYPES else dsn
            if store_type == "duckdb":
                if not target:
                    raise RuntimeError(f"materialize store DSN {dsn!r} has no file path")
                from provisa.federation.materialize_broker import get_broker

                self._store_broker = get_broker(target)
            else:
                if store_type not in self._NO_EXTENSION_TYPES:
                    self._con.execute(f"INSTALL {store_type}")
                    self._con.execute(f"LOAD {store_type}")
                self._con.execute(f"ATTACH '{target}' AS {self._MAT_STORE} (TYPE {store_type})")
            self._store_attached = True
        return self._MAT_STORE

    @property
    def connection(self):
        """The underlying DuckDB connection — the backend's cache terminal writes the API-result
        cache through it against ``mat_store.*``, landing in the store (not DuckDB's own storage)."""
        return self._con

    async def reconcile_replica(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str] | None = None,
    ) -> str:
        """Eager reconcile (boot / (re)registration): converge the replica ``schema.table`` in the
        store to ``columns`` — it survives restart and is recreated on a drift — WITHOUT copying
        data (that is the refresh's job). Nothing is created on the engine: a read addresses the
        replica in the store (REQ-1912). The engine never writes the store."""
        self.ensure_materialize_attached()
        if self._store_is_duckdb():
            return self._store_broker.reconcile(schema, table, columns)
        return await store_writer.reconcile_table(
            self._store_dsn(), schema=schema, table=table, columns=columns, pk_columns=pk_columns
        )

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
        """Land ``rows`` into the store table ``schema.table`` (the per-fire source refresh path).
        Duckdb-native dispatch: through the store broker for an embedded DuckDB store (REQ-989),
        else the server-store write face."""
        self.ensure_materialize_attached()
        if self._store_is_duckdb():
            # REQ-1901: the land goes through the store broker, a synchronous, blocking call. It
            # runs on the caller's own thread (REQ-1882) — see LandGuard.run for the one place a
            # land is handed to another thread, and why.
            return await self._land_guard.run(
                lambda: self._store_broker.land(
                    schema,
                    table,
                    columns,
                    rows,
                    change_signal,
                    watermark_column,
                ),
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

    async def apply_cdc_events(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
        events: list,
    ) -> dict[str, int]:
        """Apply CDC change events to a landed table (REQ-1733). Duckdb-native dispatch mirroring
        ``land_table``: through the engine's own connection for an embedded DuckDB store (REQ-989 —
        a second connection cannot open a file the engine already ATTACHed), else the server-store
        write face."""
        self.ensure_materialize_attached()
        if self._store_is_duckdb():
            return self._store_broker.apply_cdc(schema, table, columns, pk_columns, events)
        from sqlalchemy.schema import CreateSchema

        from provisa.federation.materialize_exec import apply_cdc, build_table
        from provisa.federation.store_writer import store_connection

        tbl = build_table(schema, table, columns, tuple(pk_columns))
        async with store_connection(self._store_dsn()) as conn:
            if schema and conn.capabilities.schemas:
                await conn.execute_core(CreateSchema(schema, if_not_exists=True))
            return await apply_cdc(conn, tbl, pk_columns, events)

    async def reconcile_mv_table(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str] | None = None,
    ) -> str:
        """Converge an MV's OWN store table to its output ``columns`` (REQ-970). Duckdb-native
        dispatch mirroring ``reconcile_replica``: through the engine's own connection for an
        embedded DuckDB store (REQ-989), else the server-store write face."""
        self.ensure_materialize_attached()
        if self._store_is_duckdb():
            return self._store_broker.reconcile(schema, table, columns)
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
        outcome (REQ-965). Duckdb-native dispatch mirroring ``land_table``: through the
        engine's own connection for an embedded DuckDB store (REQ-989), else the server-store write
        face."""
        self.ensure_materialize_attached()
        if self._store_is_duckdb():
            return self._store_broker.persist(schema, table, columns, rows, persist, pk_columns)
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

    def mv_store_broker(self) -> Any:
        """The broker MV refresh must write through, or ``None`` when the store is attached on this
        connection. REQ-1901: an embedded DuckDB-file store is never ATTACHed here, so an MV's
        CTAS / DELETE+INSERT / bitemporal append cannot run as engine SQL against ``mat_store`` —
        the refresh hands the fresh rows to the broker instead (``provisa.mv.refresh``)."""
        self.ensure_materialize_attached()
        return self._store_broker if self._store_is_duckdb() else None

    def _refresh_store_relations(self, duck_sql: str) -> str:
        """REQ-1901: rehydrate, from the broker singleton, every embedded-DuckDB-store table
        `duck_sql` reads, and return the SQL to execute.

        A read of the store is a `mat_store.<schema>.<table>` reference — a replica addressed in
        the replicas schema (REQ-1912), or a materialized view in its own. Each is copied ONLY when
        the statement names it (never the whole store), under `_store_relation_lock`, into a local
        table, and the reference is rewritten to that table. This connection never ATTACHes the
        store file (the exclusive DuckDB file lock), and no stand-in `mat_store` catalog is
        attached either: a WRITE aimed at `mat_store` on this connection must keep failing loudly,
        not land in memory and vanish.

        A real TABLE, not a VIEW over a `register()`-ed Python object: `register()` binds a
        "replacement scan" scoped to the connection object it was called on, but every query runs
        on a PRIVATE `self._con.cursor()` — a VIEW built that way raised ``Catalog Error: Table ...
        does not exist`` from a cursor. A materialized TABLE is ordinary catalog data any cursor of
        this connection can read, so the registration is only a transient staging step.

        Matching is textual — a false positive (a name inside an unrelated literal) only costs one
        extra broker round-trip; a false negative cannot happen, since a read of the store must
        name it."""
        if self._store_broker is None:
            return duck_sql
        refs = {_store_ref(m) for m in _MAT_STORE_REF.finditer(duck_sql)}
        if not refs:
            return duck_sql
        with self._store_relation_lock:
            self._con.execute(f'CREATE SCHEMA IF NOT EXISTS "{_LOCAL_STORE_SCHEMA}"')
            for schema, table in refs:
                self._copy_store_table(schema, table, _local_store_name(schema, table))
        return _MAT_STORE_REF.sub(lambda m: _local_store_name(*_store_ref(m)), duck_sql)

    def _copy_store_table(self, schema: str, table: str, target: str) -> None:
        """Make `target` on this connection current with the store's `schema.table` (lock held).

        Copies only when the store changed since this target was last copied: the broker returns
        the store-file canary its read was current as of, and any process's write changes it.
        Re-copying the whole table on every statement made a landed 1M-row table cost a full copy
        per query (live on the perf bench: large_federated_join spent its 120s budget copying)."""
        if self._store_copy_canary.get(target) == self._store_broker.canary():
            return
        arrow_tbl, canary = self._store_broker.fetch_arrow(schema, table)
        reg_name = f"_matbroker_{table}"
        # A PRIVATE cursor, never the shared connection: register() makes a view holding a Python
        # reference, and DuckDB destroys it (needing the GIL) while holding the owning client
        # context's lock. On the shared connection another thread can hold the GIL while waiting
        # for that same context lock -- a deadlock, caught live on the perf bench (a request stuck
        # 10+ min in this CREATE's commit while a peer's self._con.execute waited in LockContext).
        # A cursor's own context is locked by no other thread.
        cur = self._con.cursor()
        try:
            cur.register(reg_name, arrow_tbl)
            try:
                cur.execute(f'CREATE OR REPLACE TABLE {target} AS SELECT * FROM "{reg_name}"')
            finally:
                cur.unregister(reg_name)
        finally:
            cur.close()
        self._store_copy_canary[target] = canary

    # -- metadata --------------------------------------------------------------

    def introspect_columns(self, source: Any) -> dict[str, str]:
        """Column types as the DuckDB engine reports them for a registered source —
        the engine's metadata view (attach the source, DESCRIBE the physical relation).
        Returns {column_name: duckdb_type_name}. This is the DuckDB implementation of
        the engine-introspection seam (REQ-825/840); callers reach it via EngineRuntime."""
        self.attach_source(source)
        phys = self._phys_name(source)
        # REQ-899: a ClickHouse table has no view at its physical name — DESCRIBE its HTTP read.
        target, live_http = self._rewrite_clickhouse_relations(
            f"SELECT * FROM {phys}", None, deadline_s=request_deadline.remaining(), describe=True
        )
        if not live_http:
            target = phys
        # PRIVATE cursor: introspection runs on request threads concurrently with queries, and the
        # shared connection holds only one pending result (see run()).
        cur = self._open_cursor(live_http=live_http)
        try:
            res = cur.execute(f"DESCRIBE {target}")
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
        # Read here, on the request's own context: run_in_executor does not carry the contextvar.
        deadline_s = request_deadline.remaining()

        def _run() -> QueryResult:
            # Read gate: a control-plane rebuild swaps the provisa_admin snapshot out from under any
            # query already bound to it, which returns zero rows rather than failing (_CatalogGate).
            with self._catalog_gate.read():
                sql = self._refresh_store_relations(duck_sql)
                sql, live_http = self._rewrite_clickhouse_relations(
                    sql, params, deadline_s=deadline_s
                )
                # A PRIVATE cursor, never the shared connection: run() is dispatched to an executor
                # thread, so two queries overlap routinely. A DuckDB connection holds ONE pending
                # result — the second execute() replaces the first, and the first thread's fetchall()
                # then returns an EMPTY list rather than raising. That is what made meta/admin
                # queries intermittently come back with no rows under the parallel e2e suite while
                # the control plane plainly held the data (run_sync and run_arrow_stream already
                # took a cursor for this reason).
                cur = self._open_cursor(live_http=live_http)
                try:
                    with request_deadline.cancel_on_deadline(cur.interrupt):
                        res = cur.execute(sql, params) if params else cur.execute(sql)
                        cols = [d[0] for d in res.description] if res.description else []
                        types = [str(d[1]) for d in res.description] if res.description else []
                        rows = res.fetchall()
                    return QueryResult(rows=rows, column_names=cols, column_types=types)
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
            duck_sql = self._refresh_store_relations(duck_sql)
            duck_sql, live_http = self._rewrite_clickhouse_relations(
                duck_sql, params, deadline_s=request_deadline.remaining()
            )
            cur = self._open_cursor(live_http=live_http)
            with request_deadline.cancel_on_deadline(cur.interrupt):
                cur.execute(duck_sql, params) if params else cur.execute(duck_sql)
        except BaseException:
            self._catalog_gate.release_read()
            raise

        def _close(*_: Any) -> None:
            cur.close()
            self._catalog_gate.release_read()

        # DuckDB's description carries each column's declared DuckDBPyType ("BIGINT",
        # "DECIMAL(18,2)"), even for a zero-row result — the pgwire Describe relies on it.
        return stream_from_dbapi(
            cur, on_close=_close, type_names=lambda codes: [str(c) for c in codes]
        )

    def describe_sync(self, duck_sql: str, params: list | None = None) -> ResultStream:
        """The statement's result shape without running it: DuckDB's ``DESCRIBE <query>`` binds and
        plans it and returns each column's name and declared type — the same names and type strings
        run_sync's cursor description reports, duplicates included (a subquery wrapper would rename
        a duplicate ``a`` to ``a_1``). REQ-589."""
        with self._catalog_gate.read():
            duck_sql = self._refresh_store_relations(duck_sql)
            duck_sql, live_http = self._rewrite_clickhouse_relations(
                duck_sql, params, deadline_s=request_deadline.remaining(), describe=True
            )
            cur = self._open_cursor(live_http=live_http)
            try:
                with request_deadline.cancel_on_deadline(cur.interrupt):
                    res = (
                        cur.execute(f"DESCRIBE {duck_sql}", params)
                        if params
                        else cur.execute(f"DESCRIBE {duck_sql}")
                    )
                    described = res.fetchall()
            finally:
                cur.close()
        return QueryResult(
            rows=[],
            column_names=[r[0] for r in described],
            column_types=[r[1] for r in described],
        )

    # -- Arrow transport (REQ-986) ---------------------------------------------

    def run_arrow(self, duck_sql: str, params: list | None = None):
        """Execute dialect-DuckDB SQL and return a ``pyarrow.Table`` — DuckDB produces Arrow natively
        (``fetch_arrow_table``), so no Python rows are materialized for the Flight transport."""
        with self._catalog_gate.read():
            duck_sql = self._refresh_store_relations(duck_sql)
            duck_sql, live_http = self._rewrite_clickhouse_relations(
                duck_sql, params, deadline_s=request_deadline.remaining()
            )
            # PRIVATE cursor for the same reason as run_sync/run_arrow_stream: a concurrent query on
            # the shared connection replaces this one's pending result, and the fetch then yields
            # nothing instead of raising.
            cur = self._open_cursor(live_http=live_http)
            try:
                with request_deadline.cancel_on_deadline(cur.interrupt):
                    res = cur.execute(duck_sql, params) if params else cur.execute(duck_sql)
                    return res.to_arrow_table()
            finally:
                cur.close()

    def run_arrow_stream(self, duck_sql: str, params: list | None = None):
        """Execute dialect-DuckDB SQL and return ``(schema, batch_generator)`` for lazy record-batch
        streaming through the Flight server's GeneratorStream (REQ-986, REQ-1214).

        Truly lazy: the batches are pulled from a PRIVATE cursor's Arrow record-batch reader
        (``to_arrow_reader`` — ``fetch_record_batch`` is deprecated as of duckdb 1.x, same
        batch_size arg and RecordBatchReader return) on demand, so the full result never
        materializes — peak memory is bounded by one record batch, not the total result size. A
        private cursor (not the shared connection) keeps concurrent worker-thread streams from
        corrupting each other's fetch state; it is closed when the generator drains or the
        consumer stops early."""
        # Held for the whole stream — same reason as run_sync: the batches are scanned on demand.
        self._catalog_gate.acquire_read()
        try:
            duck_sql = self._refresh_store_relations(duck_sql)
            duck_sql, live_http = self._rewrite_clickhouse_relations(
                duck_sql, params, deadline_s=request_deadline.remaining()
            )
            cur = self._open_cursor(live_http=live_http)
            with request_deadline.cancel_on_deadline(cur.interrupt):
                cur.execute(duck_sql, params) if params else cur.execute(duck_sql)
                reader = cur.to_arrow_reader(_ARROW_STREAM_BATCH_ROWS)
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
        with self._cp_lock:  # never tear the snapshot out from under a refresh in flight
            if self._cp_snapshot_dir:
                shutil.rmtree(self._cp_snapshot_dir)
                self._cp_snapshot_dir = ""
                self._cp_snapshot_path = ""
