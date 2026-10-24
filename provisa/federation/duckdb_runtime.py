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
import hashlib
import logging
import os
import re
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, TypeVar

import duckdb

from provisa.federation.duckdb_extensions import install_and_load
from provisa.federation.file_locks import ReadersWriterGate
from provisa.executor.drivers.duckdb_driver import fetch_rows
from provisa.executor.result import QueryResult, ResultStream
from provisa.federation import store_writer
from provisa.federation.engine import build_duckdb_engine
from provisa.core import request_deadline
from provisa.core.read_refusal import ReadRefused
from provisa.federation.land_guard import LandGuard
from provisa.federation.replica_address import is_replicas_schema
from provisa.federation.runtime_support import (
    close_cursor,
    columns_from_describe,
    open_cursor,
    stream_from_dbapi,
    stream_rows_from_arrow,
)
from provisa.transpiler.transpile import transpile

log = logging.getLogger(__name__)

_T = TypeVar("_T")

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
    from provisa.federation.duckdb_extensions import connect

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


def _sql_text(value: str) -> str:
    """``value`` for a single-quoted SQL literal."""
    return value.replace("'", "''")


def _quoted(name: str) -> str:
    """``name`` as a double-quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def _attach_url(url: Any) -> str:
    """A server store's URL as DuckDB's ATTACH takes it: a store is declared with its SQLAlchemy
    driver (``postgresql+psycopg://``, config/capabilities.yaml), which libpq does not read as a
    URI, so the driver is dropped and the rest is kept as written."""
    return url.set(drivername=url.get_backend_name()).render_as_string(hide_password=False)


class _CatalogGate(ReadersWriterGate):
    """Readers-writer gate over the shared DuckDB connection's catalog.

    A local DuckDB file this connection holds is DETACHed while another writer takes it (REQ-1997,
    ``file_locks``) and re-attached after. A query that binds or scans during that window does not
    fail -- it comes back with ZERO ROWS, its relations pointing at an alias that is gone. Queries
    therefore hold the gate for read; the pause holds it for write.

    Reader-priority, deliberately: a waiting writer must NOT block new readers. Execution paths that
    stream (run_sync, run_arrow_stream) keep their read hold until the cursor drains, and the thread
    that opened such a stream may itself be the next one to pause a file -- writer preference would
    let that thread block on its own outstanding read."""


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


class WriteConflict(RuntimeError):
    """REQ-2006: a statement collided with another transaction's write to the same rows, and DuckDB
    refused it. Refused by name, never retried here: the caller decides whether to write again."""

    code = "engine.write_conflict"

    def __init__(self, detail: str) -> None:
        super().__init__(
            f"Another change to the same data was written at the same time; retry. ({detail})"
        )
        self.params = {"detail": detail}


class SourceUnreachable(ReadRefused):
    """A read of a source whose server does not answer (#256): its attach was set aside so
    nothing else fails with it, and attaching it again for this read failed too. Refused by the
    source's name; the server's own error, which carries its connection string, stays in the
    log."""

    code = "query.source_unreachable"

    def __init__(self, source_id: str) -> None:
        self.params = {"source": source_id}
        super().__init__(
            f"source {source_id!r} cannot be reached; its reads are refused until its server "
            f"answers."
        )


@contextmanager
def named_write_conflict() -> Iterator[None]:
    """DuckDB's write-write conflict, as :class:`WriteConflict`; every other error unchanged."""
    try:
        yield
    except duckdb.TransactionException as exc:
        first = str(exc).splitlines()[0]
        if "conflict" not in first.lower():
            raise
        raise WriteConflict(first) from exc


class DuckDBFederationRuntime:  # REQ-825, REQ-840, REQ-844
    def __init__(
        self, *, materialize_dsn: str | None = None, store_engine: Any | None = None
    ) -> None:
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
        # REQ-2050: the engine's zone is UTC, so an instant (TIMESTAMPTZ) reads as UTC and a
        # literal with no zone compared to one is UTC -- never the host's zone.
        _cfg["TimeZone"] = "UTC"
        # REQ-2006: an org's engine may be capped (a demo sandbox's, sandbox.memory_limit), and the
        # deployment may cap every engine (engine.duckdb_memory_limit); unset is DuckDB's own.
        # DuckDB spills to disk past it and refuses, by name, what cannot spill.
        _limit = getattr(store_engine, "memory_limit", None)
        if _limit is None:
            from provisa.core import settings_registry

            _limit = settings_registry.value("engine.duckdb_memory_limit")
        if _limit is not None:
            _cfg["memory_limit"] = f"{_limit // (1024 * 1024)}MiB"
        # Spilled data stays in the org's folder (engine.temp_org). A runtime built for no org --
        # one introspecting a source's catalog, which reads metadata and spills nothing -- has no
        # folder to name, and keeps DuckDB's own.
        _temp_org = getattr(store_engine, "temp_org", None)
        if _temp_org is not None:
            from provisa.core import org_files

            _cfg["temp_directory"] = str(org_files.org_tmp(_temp_org))
        self._con = duckdb.connect(config=_cfg)
        from provisa.fakes.duckdb_functions import register as register_fake_functions

        register_fake_functions(self._con)  # REQ-1494: provisa_digest, provisa_fake_method
        self._engine = build_duckdb_engine()
        # REQ-1922: the FederationEngine whose store this runtime lands in — the backend's own, so
        # the store an org lane pins on it (``pin_materialize_store``) is the one attached. A
        # runtime built on its own (a test, introspection) lands where its own engine resolves.
        self._store_engine = store_engine if store_engine is not None else self._engine
        # An explicit materialize-store DSN override (tests). When None it is resolved lazily via the
        # engine's invariant (configured store → declared default → error) only when a materialize
        # operation actually needs it — the runtime is also built for introspection, which does not.
        self._materialize_dsn = materialize_dsn
        self._sqlite_loaded = False
        self._pg_ext_loaded = False  # postgres DuckDB extension INSTALL/LOAD (source ATTACH)
        self._httpfs_loaded = False  # httpfs INSTALL/LOAD for S3-compatible (e.g. R2) sources
        self._store_attached = False  # materialization-store ATTACH (distinct from source attaches)
        self._attached_store_dsn: str | None = None  # the store that attach was for
        # REQ-1922: the other regions' replicas stores this connection has attached, by alias.
        self._region_stores: set[str] = set()
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
        # The source each attach of this connection was made for -- raw remote databases and the
        # in-memory catalogs its views stand in -- so a deleted source's attaches go with it.
        self._attach_source: dict[str, str] = {}
        # REQ-899: ClickHouse tables read live over ClickHouse's HTTP interface — physical-name key
        # (lowercased catalog, schema, table) -> relation. No view stands at that name: every
        # statement naming one is rewritten to a read_parquet() of it (clickhouse_http_scan).
        self._ch_relations: dict[tuple[str, str, str], Any] = {}
        # attach_source (request/prepare threads) writes it while statements on other threads read
        # it; readers take a snapshot under the lock.
        self._ch_lock = threading.Lock()
        # REQ-1867: Databricks tables read in place — physical name -> (source, iceberg_scan
        # arguments). Their view is re-pointed before every statement that names one
        # (_refresh_databricks_relations), never left on an old snapshot or expired credentials.
        self._dbx_relations: dict[str, tuple[Any, str]] = {}
        # #266: each attached view's columns named apart from their source fields, by its physical
        # name, as the attach walk gave them (native_backend._mongo_column_fields).
        self._column_fields: dict[str, list[tuple[str, str]]] = {}
        self._dbx_lock = threading.Lock()
        self._raw_attached: set[str] = set()  # source ids whose remote DB is already ATTACHed
        # REQ-1529: the ATTACH each raw alias was made with, so an environment reaching the same
        # source id through a different connection gets an attach of its own.
        self._raw_attach_ddl: dict[str, str] = {}
        # Raw attaches set aside because their server stopped answering (_set_aside_unreachable):
        # alias -> its ATTACH, run again by the next statement that reads the source.
        self._unreachable: dict[str, str] = {}
        self._ext_loaded: set[str] = (
            set()
        )  # community/extension connectors LOADed on this connection
        self._control_plane_attached = False  # provisa_admin catalog (native path only)
        # A region's own stores, attached read-only for the ops views over them, by alias.
        self._region_stores_attached: set[str] = set()
        # A held file's pause is invisible to concurrent queries only if they are excluded from it
        # -- see _CatalogGate. Held for read by every execution path, for write by the pause.
        self._catalog_gate = _CatalogGate()
        # REQ-1997: local DuckDB files held by this connection, by attach alias: (path, token).
        self._file_holders: dict[str, tuple[str, int]] = {}
        # REQ-1998: a digest of the MotherDuck token this connection is signed in with, if any.
        self._motherduck_account: str | None = None
        # One store connection, so one write on it at a time (two lands interleaving on it was a
        # confirmed regression). A lock serializes it, and each write runs on the thread that asked
        # for it — a read-triggered land stays on its request's thread (REQ-1882), where a
        # one-worker pool took it off.
        self._land_guard = LandGuard("DuckDB store")

    # -- source exposure -------------------------------------------------------

    @staticmethod
    def _catalog_of(source: Any) -> str:
        """The catalog the compiler names ``source``'s tables under. The attach walk hands each
        source over with the catalog of the org environment being served (REQ-1266, REQ-1529:
        each environment its own); an introspection attach hands none and is prod's, whose catalog
        is the source id with hyphens normalized (see core.catalog)."""
        from provisa.compiler.naming import attach_catalog
        from provisa.core.catalog import _to_catalog_name

        if "catalog" in vars(source):
            return attach_catalog(source)
        return _to_catalog_name(source.id)

    def _phys_name(self, source: Any) -> str:
        """The catalog-qualified physical name the compiler emits: ``"catalog"."schema"."table"``."""
        catalog = self._catalog_of(source)
        if catalog not in self._phys_catalogs:
            # A writable in-memory catalog so the 3-part physical name resolves (an ATTACHed remote
            # DB is read-only and cannot host the schema/view the compiler references).
            self._con.execute(f"ATTACH ':memory:' AS \"{catalog}\"")
            self._phys_catalogs.add(catalog)
            self._attach_source[catalog] = source.id
        self._con.execute(f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."{source.schema_name}"')
        return f'"{catalog}"."{source.schema_name}"."{source.table_name}"'

    def attach_source(self, source: Any) -> None:
        """Expose an ATTACH source at its catalog-physical name via the engine's connector."""
        entry = self._engine.resolve(source)  # picks the (duckdb, source_type) connector
        details = entry.details
        phys = self._phys_name(source)
        if "clickhouse_http" in details:
            self._attach_clickhouse(source, details)
        elif "databricks_iceberg" in details:
            self._attach_databricks(source, details["databricks_iceberg"], phys)
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
                install_and_load(
                    self._con,
                    ext,
                    from_community=getattr(connector, "install_from_community", False),
                )
                self._ext_loaded.add(ext)
            secret_ddl = details.get("secret_ddl")
            if secret_ddl and not self._httpfs_loaded:
                install_and_load(self._con, "httpfs", from_community=False)
                self._httpfs_loaded = True
            self._reach_location(details)
            scan = details["view_ddl"].split(" AS ", 1)[1]
            self._con.execute(f"CREATE OR REPLACE VIEW {phys} AS {scan}")
        else:  # ATTACH postgres / sqlite / extension source once, then view the remote table
            raw_alias = self._attach_raw(source, details)
            remote_schema = details.get("remote_schema", source.schema_name)
            remote = f'"{raw_alias}"."{remote_schema}"."{source.table_name}"'
            # #266: a column named apart from the field it reads (a MongoDB column's path) is
            # exposed under its own name beside the source's.
            # An attach from no table model (the introspection seam's) carries none and keeps
            # those the walk gave, so it never re-creates the view without them.
            fields = getattr(source, "column_fields", None)
            if fields is None:
                fields = self._column_fields.get(phys, [])
            else:
                self._column_fields[phys] = fields
            renamed = "".join(f", {_quoted(field)} AS {_quoted(name)}" for field, name in fields)
            self._con.execute(f"CREATE OR REPLACE VIEW {phys} AS SELECT *{renamed} FROM {remote}")

    def detach_source(self, source: Any) -> None:
        """Remove the live exposure of ``source``'s table from this engine: the view at its
        catalog-physical name (checked against the catalog, so nothing else is dropped) and any
        ClickHouse HTTP relation registered for it. Called when the table's reads move to its
        replica (REQ-1912): nothing on the engine may then read the source."""
        parts = [self._catalog_of(source), source.schema_name, source.table_name]
        with self._ch_lock:
            self._ch_relations.pop((parts[0].lower(), parts[1].lower(), parts[2].lower()), None)
        with self._dbx_lock:
            self._dbx_relations.pop(f'"{parts[0]}"."{parts[1]}"."{parts[2]}"', None)
        if parts[0] not in self._phys_catalogs:
            return  # nothing of this source was ever exposed on this connection
        is_view = self._listing(
            lambda: self._con.execute(
                "SELECT 1 FROM duckdb_views() WHERE database_name = ? AND schema_name = ? "
                "AND view_name = ?",
                parts,
            ).fetchone()
        )
        if is_view is not None:
            self._con.execute(f'DROP VIEW "{parts[0]}"."{parts[1]}"."{parts[2]}"')

    def drop_source(self, source_id: str) -> None:
        """Let go of everything this connection attached for the deleted source ``source_id``:
        its remote databases and the catalogs its views stand in. DuckDB reads every attached
        database for any catalog listing (``information_schema``, ``duckdb_columns()``), so an
        attach left behind to a server that has gone fails those listings for every other
        source. Under the catalogue gate: a statement in flight finishes first."""
        aliases = [a for a, sid in self._attach_source.items() if sid == source_id]
        with self._catalog_gate.write():
            for alias in aliases:
                self._con.execute(f'DETACH "{alias}"')
                del self._attach_source[alias]
                self._raw_attached.discard(alias)
                self._raw_attach_ddl.pop(alias, None)
                self._phys_catalogs.discard(alias)
                self._let_file_go(alias)

    def _listing(self, run: Callable[[], _T]) -> _T:
        """``run`` -- a catalog listing on this connection -- with any raw attach whose server
        has stopped answering set aside first. DuckDB reads every attached database for any
        catalog listing, even one filtered to another database, so one unreachable server fails
        them all (#256). Not to be called holding the catalogue gate."""
        try:
            return run()
        except duckdb.IOException:
            if not self._set_aside_unreachable():
                raise
        return run()

    def _set_aside_unreachable(self) -> list[str]:
        """DETACH each raw attach whose server does not answer, keeping its ATTACH to run again
        (``_bring_back``); the aliases set aside. A missing schema binds in the one database
        named, so the probe reaches no other: a live one answers that the schema does not
        exist, an unreachable one fails to connect."""
        gone: list[str] = []
        with self._catalog_gate.write():
            for alias in sorted(self._raw_attached):
                try:
                    self._con.execute(f'SELECT 1 FROM "{alias}"."__provisa_probe__"."x" LIMIT 0')
                except duckdb.IOException as exc:
                    self._con.execute(f'DETACH "{alias}"')
                    self._raw_attached.discard(alias)
                    self._unreachable[alias] = self._raw_attach_ddl[alias]
                    gone.append(alias)
                    log.warning(
                        "source %s is unreachable; its attach is set aside until a read of it: %s",
                        self._attach_source[alias],
                        exc,
                    )
                except duckdb.Error:
                    continue  # it answered
        return gone

    def _bring_back(self, duck_sql: str) -> None:
        """ATTACH again each set-aside source ``duck_sql`` reads (by any catalog it is read
        under); one whose server still does not answer refuses the statement by its name."""
        for alias, attach in list(self._unreachable.items()):
            source_id = self._attach_source[alias]
            catalogs = [c for c, sid in self._attach_source.items() if sid == source_id]
            if not any(f'"{c}"'.lower() in duck_sql.lower() for c in catalogs):
                continue
            try:
                self._con.execute(attach)
            except duckdb.IOException as exc:
                log.warning("source %s is still unreachable: %s", source_id, exc)
                raise SourceUnreachable(source_id) from exc
            self._raw_attached.add(alias)
            del self._unreachable[alias]

    def _attach_databricks(self, source: Any, scan_args: str, phys: str) -> None:
        """REQ-1867: register a Databricks table read in place, and point its view at the table's
        current snapshot now (so it can be described). The core iceberg and httpfs extensions are
        loaded once on this connection."""
        for ext in ("httpfs", "iceberg"):
            if ext not in self._ext_loaded:
                self._con.execute(f"INSTALL {ext}")
                self._con.execute(f"LOAD {ext}")
                self._ext_loaded.add(ext)
        self._httpfs_loaded = True
        with self._dbx_lock:
            self._dbx_relations[phys] = (source, scan_args)
            self._point_databricks_view(phys, source, scan_args)

    def _point_databricks_view(self, phys: str, source: Any, scan_args: str) -> None:
        """Resolve the table's current metadata pointer and vended storage credentials, issue the
        credentials as DuckDB secrets scoped to their storage prefix, and re-point the view.
        Called with ``_dbx_lock`` held."""
        from provisa.federation.databricks_iceberg_resolver import (
            resolve_databricks_iceberg_table,
            s3_secret_options,
        )

        table = resolve_databricks_iceberg_table(source)
        for i, credential in enumerate(table.credentials):
            options = ", ".join(
                f"{k} '{_sql_text(v)}'" for k, v in s3_secret_options(credential).items()
            )
            self._con.execute(
                f'CREATE OR REPLACE SECRET "_dbx_{source.id}_{i}" (TYPE s3, {options})'
            )
        self._con.execute(
            f"CREATE OR REPLACE VIEW {phys} AS SELECT * FROM "
            f"iceberg_scan('{_sql_text(table.metadata_location)}'{scan_args})"
        )

    def _refresh_databricks_relations(self, duck_sql: str) -> None:
        """REQ-1867: before a statement runs, re-point the view of every in-place Databricks table
        it names at the table's current snapshot, with fresh credentials. Matching is textual on
        the physical name: a false positive costs one extra Unity Catalog round-trip; a read of the
        table must name it."""
        with self._dbx_lock:
            if not self._dbx_relations:
                return
            lowered = duck_sql.lower()
            for phys, (source, scan_args) in self._dbx_relations.items():
                if phys.lower() in lowered:
                    self._point_databricks_view(phys, source, scan_args)

    def refresh_live_relations(self, sql: str) -> None:
        """REQ-1867: the same re-pointing for a statement this runtime does not run itself."""
        self._refresh_databricks_relations(sql)

    def _prepare_sql(self, duck_sql: str) -> str:
        """The per-statement preparation every execution path runs: in-place Databricks views
        re-pointed (REQ-1867), then the store relations the statement reads rehydrated
        (REQ-1901). A source set aside as unreachable is attached again first."""
        self._bring_back(duck_sql)
        self._refresh_databricks_relations(duck_sql)
        return self._refresh_store_relations(duck_sql)

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
            install_and_load(self._con, "httpfs", from_community=False)
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
            close_cursor(cur)
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
        cur = open_cursor(self._con)
        if live_http:
            try:
                cur.execute("SET force_download = true")
                cur.execute("SET http_retries = 0")
            except BaseException:
                close_cursor(cur)
                raise
        return cur

    def _ensure_postgres_loaded(self) -> None:
        """The postgres extension on this connection, once, with the settings every postgres load
        has (``duckdb_extensions.install_and_load``; #241)."""
        if not self._pg_ext_loaded:
            install_and_load(self._con, "postgres", from_community=False)
            self._pg_ext_loaded = True

    def _sign_in_motherduck(self, source: Any) -> None:
        """REQ-1998: sign this connection in to MotherDuck with the source's token, given as a
        bound value so it is in no statement's text. The instance is signed in to one account: a
        source signed in with another token is refused by name."""
        self._sign_in_motherduck_as(source.password, f"Source {source.id!r}")

    def _sign_in_motherduck_as(self, token: str, who: str) -> None:
        """Sign this connection in to MotherDuck with ``token`` (a bound value), once; ``who`` --
        a source, or the materialization store (REQ-1999) -- signing in with another account's
        token is refused by name."""
        import hashlib

        account = hashlib.sha256(token.encode()).hexdigest()
        if self._motherduck_account is None:
            self._con.execute("SET GLOBAL motherduck_token = ?", [token])
            self._motherduck_account = account
        elif self._motherduck_account != account:
            raise ValueError(
                f"{who}: this engine is signed in to another MotherDuck account; "
                "one DuckDB engine reads one MotherDuck account"
            )

    def _reach_location(self, details: dict) -> None:
        """Load the extensions a file source's location needs on this connection and create its
        secret (``connector_duckdb.file_location_details``, REQ-1997)."""
        for ext in details.get("extensions", ()):
            if ext == "httpfs":
                if not self._httpfs_loaded:
                    install_and_load(self._con, "httpfs", from_community=False)
                    self._httpfs_loaded = True
            elif ext not in self._ext_loaded:
                install_and_load(self._con, ext, from_community=False)
                self._ext_loaded.add(ext)
        if details.get("secret_ddl"):
            self._con.execute(details["secret_ddl"])

    def _attach_raw(self, source: Any, details: dict) -> str:
        """ATTACH the source's remote database under its private alias (once), loading the DuckDB
        extension its connector rides on first, and return the alias. The connector declares WHERE
        it exposes tables (postgres keeps its own schema, sqlite lands everything under ``main``,
        mongo maps databases to schemas); the runtime never hardcodes a per-type layout."""
        if source.type.value == "sqlite" and not self._sqlite_loaded:
            install_and_load(self._con, "sqlite", from_community=False)
            self._sqlite_loaded = True
        elif source.type.value == "postgresql" and not self._pg_ext_loaded:
            self._ensure_postgres_loaded()
        else:
            # REQ-1673: a community-extension connector (mongo, mssql, firebird, …) needs its
            # extension loaded on THIS connection — the startup probe loaded it on another one.
            connector = self._engine.connector_for(source.type.value)
            ext = getattr(connector, "extension", None)
            if ext and ext not in self._ext_loaded:
                install_and_load(
                    self._con,
                    ext,
                    from_community=getattr(connector, "install_from_community", False),
                )
                self._ext_loaded.add(ext)
        self._reach_location(details)  # REQ-1997: a file database in a remote location
        if details.get("signs_in") == "motherduck":
            self._sign_in_motherduck(source)  # REQ-1998: before its ATTACH
        raw_alias = details.get("raw_alias", source.id)
        attach = details["attach"]
        if raw_alias in self._raw_attached and self._raw_attach_ddl[raw_alias] != attach:
            # REQ-1529, REQ-1942: one source id reached through another connection -- an
            # environment that binds the source to a database of its own (a dev lane repointed,
            # a synthetic lane bound to its store). It gets an attach of its own, named for the
            # catalog it is read under; an environment inheriting the connection shares the one
            # already attached, the ATTACH being the same.
            catalog_alias = f"{raw_alias}__{self._catalog_of(source)}"
            attach = attach.replace(f'"{raw_alias}"', f'"{catalog_alias}"')
            if (
                catalog_alias in self._raw_attached
                and self._raw_attach_ddl[catalog_alias] != attach
            ):
                # The environment's binding changed: its old connection is let go.
                self._con.execute(f'DETACH "{catalog_alias}"')
                self._raw_attached.discard(catalog_alias)
                self._let_file_go(catalog_alias)
            raw_alias = catalog_alias
        if raw_alias not in self._raw_attached:
            self._con.execute(attach)
            if "configure_pool" in details:  # REQ-1909: the source's share of its live-read cap
                # The pool is the catalog's as attached: an environment's own attach is named for
                # its catalog above, and so is the pool its statement configures.
                self._con.execute(
                    details["configure_pool"].replace(
                        f"catalog_name='{details.get('raw_alias', source.id)}'",
                        f"catalog_name='{raw_alias}'",
                    )
                )
            self._raw_attached.add(raw_alias)
            self._unreachable.pop(raw_alias, None)
            self._raw_attach_ddl[raw_alias] = attach
            self._attach_source[raw_alias] = source.id
            self._hold_file(source, raw_alias)
        return raw_alias

    def _hold_file(self, source: Any, raw_alias: str) -> None:
        """REQ-1997: a local DuckDB file this connection attaches (read-only) is let go while a
        write opens it read-write (``federation.file_locks``): detached under the catalogue gate,
        so a query in flight finishes first and none starts until it is attached again."""
        from provisa.federation import file_locks
        from provisa.federation.connector_duckdb import location_kind

        path = getattr(source, "path", None)
        if source.type.value != "duckdb" or not path or location_kind(path) is not None:
            return
        from provisa.core.org_files import resolve_path

        path = resolve_path(path)  # the file a write opens, as the driver resolves it

        @contextmanager
        def paused() -> Iterator[None]:
            with self._catalog_gate.write():
                self._con.execute(f'DETACH "{raw_alias}"')
                try:
                    yield
                finally:
                    self._con.execute(self._raw_attach_ddl[raw_alias])

        self._file_holders[raw_alias] = (path, file_locks.register(path, paused))

    def _let_file_go(self, raw_alias: str) -> None:
        from provisa.federation import file_locks

        held = self._file_holders.pop(raw_alias, None)
        if held is not None:
            file_locks.unregister(*held)

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
        return self._listing(lambda: self._schemas_of(alias))

    def _schemas_of(self, alias: str) -> list[str]:
        cur = open_cursor(self._con)
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
            close_cursor(cur)

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
        return self._listing(lambda: self._tables_of(alias, schema_name))

    def _tables_of(self, alias: str, schema_name: str) -> list[str]:
        cur = open_cursor(self._con)
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
            close_cursor(cur)

    def attach_control_plane(self, dsn: str) -> None:
        """Attach the org's control plane (REQ-2014: PostgreSQL) as the ``provisa_admin`` catalog,
        read-only and live, once: DuckDB's postgres extension maps every schema of it -- the org's
        ``org_<id>`` and the meta views it holds -- 1:1 under the catalog, so a row it commits is
        read by the next statement. Trino parity: there ``provisa_admin`` is a real catalog over
        the same database. Called from NativeEngineBackend's registry walk."""
        if self._control_plane_attached:
            return
        catalog = self._CONTROL_PLANE
        self._ensure_postgres_loaded()
        if catalog not in self._phys_catalogs:
            self._con.execute(f"ATTACH '{dsn}' AS \"{catalog}\" (TYPE postgres, READ_ONLY)")
            self._phys_catalogs.add(catalog)
        self._control_plane_attached = True

    def attach_region_ops(
        self, catalog: str, schema: str, stores: dict[str, str], views: dict[str, str]
    ) -> None:
        """Define ``views`` -- each ops view's SELECT, by name -- in ``catalog``.``schema``, over
        ``stores`` (alias -> libpq DSN): the stores a region keeps its record and state in, each
        attached read-only once, as ``attach_control_plane`` attaches the model's
        (``api.region_ops_views``, REQ-1922). The views are defined again on each call, so a
        definition the model's reload changed is the one read."""
        self._ensure_postgres_loaded()
        for alias, dsn in stores.items():
            if alias not in self._region_stores_attached:
                self._con.execute(f"ATTACH '{dsn}' AS \"{alias}\" (TYPE postgres, READ_ONLY)")
                self._region_stores_attached.add(alias)
        if catalog not in self._phys_catalogs:
            self._con.execute(f"ATTACH ':memory:' AS \"{catalog}\"")
            self._phys_catalogs.add(catalog)
        self._con.execute(f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."{schema}"')
        for name, select in views.items():
            self._con.execute(f'CREATE OR REPLACE VIEW "{catalog}"."{schema}"."{name}" AS {select}')

    # The materialization store, attached under this backend-neutral alias. A store MUST exist (the
    # engine's invariant); its backend/dialect is taken from the store URL scheme, never assumed.
    _MAT_STORE = "mat_store"
    # The catalog the control plane is read under (attach_control_plane).
    _CONTROL_PLANE = "provisa_admin"
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

    def _see_store_writes(self) -> None:
        """Make what the server store's own connection just wrote readable here. This connection
        reads the store through its ATTACH, which caches the store's catalog: a schema created
        since is missing from it, and a read of a table in it fails "schema ... does not exist"
        (a view's first build, staged into an org schema the store did not hold yet). A Postgres
        store's cache is cleared; an attached SQLite file has none."""
        if self._ATTACH_TYPE_BY_SCHEME[self._store_scheme()] == "postgres":
            self._con.cursor().execute("CALL pg_clear_cache()")

    def mv_store_schema(self, org_id: str) -> str:
        """The schema materialized views are written to: one that holds nothing else (REQ-1912).
        Replicas have their own (``replica_address.replica_schema``)."""
        from provisa.federation.replica_address import mv_schema

        return mv_schema(org_id)

    def _store_dsn(self) -> str:
        """The materialization-store DSN: the explicit constructor override, else the engine's
        invariant resolution (configured → declared default → error). Never a fallback."""
        if self._attached_store_dsn is not None:
            # The store this runtime attached is its store for as long as it lives: "is it
            # attached" and "which store" are answered from the same fact.
            return self._attached_store_dsn
        return (
            self._materialize_dsn
            if self._materialize_dsn is not None
            else self._store_engine.materialize_store()
        )

    def _store_scheme(self) -> str:
        from urllib.parse import urlparse

        return urlparse(self._store_dsn()).scheme.split("+", 1)[0]

    def _store_is_duckdb(self) -> bool:
        """True when the materialization store is written with DuckDB's own statements through the
        store broker: an embedded DuckDB file (REQ-989), single-writer, or a MotherDuck database
        the engine holds attached (REQ-1999) -- not the separate server-relational write face."""
        return self._store_scheme() in ("duckdb", "motherduck")

    def _store_is_file(self) -> bool:
        """True when the store is the embedded DuckDB file, which this connection never attaches
        (REQ-1901): a statement reads a per-statement copy of each store table it names. A
        MotherDuck store is attached here and read in place."""
        return self._store_scheme() == "duckdb"

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
            if scheme == "motherduck":
                self._attach_motherduck_store(url)
                self._store_attached = True
                self._attached_store_dsn = dsn
                return self._MAT_STORE
            store_type = self._ATTACH_TYPE_BY_SCHEME.get(scheme)
            if store_type is None:
                raise RuntimeError(f"materialize store scheme {scheme!r} is not attachable")
            # A file-backed store (sqlite/duckdb) attaches the FILESYSTEM PATH; postgres attaches the
            # full URL. Use make_url(...).database, not urlparse(...).path: on Windows a
            # ``duckdb:///C:\...`` DSN parses to ``/C:\...`` under urlparse (leading slash), which
            # DuckDB then reads as a ``//C:`` UNC network path and fails ("network path not found").
            # SQLAlchemy's URL parser strips the leading slash for a drive-letter path on every OS.
            target = url.database if store_type in self._FILE_ATTACH_TYPES else _attach_url(url)
            if store_type == "duckdb":
                if not target:
                    raise RuntimeError(f"materialize store DSN {dsn!r} has no file path")
                from provisa.federation.materialize_broker import get_broker

                self._store_broker = get_broker(target)
            else:
                if store_type not in self._NO_EXTENSION_TYPES:
                    # A Postgres store loads the extension with its settings too (#241).
                    install_and_load(self._con, store_type, from_community=False)
                self._con.execute(f"ATTACH '{target}' AS {self._MAT_STORE} (TYPE {store_type})")
            self._store_attached = True
            self._attached_store_dsn = dsn
        return self._MAT_STORE

    def _attach_motherduck_store(self, url: Any) -> None:
        """REQ-1999: the store is a MotherDuck database (``motherduck:///<db>?motherduck_token=``),
        ATTACHed on this connection as ``mat_store`` once the connection is signed in with the
        store's token; its writes run on cursors of this connection (``EngineAttachedStore``)."""
        from provisa.compiler.sql_literals import sql_literal
        from provisa.federation.materialize_broker import EngineAttachedStore

        from provisa.core.secrets import resolve_secrets

        written = url.query.get("motherduck_token")
        if not written:
            raise RuntimeError("the MotherDuck materialize store URL names no motherduck_token")
        token = resolve_secrets(written)  # a ${env:..}/${secret:..} reference, resolved at use
        if not url.database:
            raise RuntimeError("the MotherDuck materialize store URL names no database")
        if "motherduck" not in self._ext_loaded:
            install_and_load(self._con, "motherduck", from_community=False)
            self._ext_loaded.add("motherduck")
        self._sign_in_motherduck_as(token, "The materialize store")
        md = sql_literal("md:" + url.database, "duckdb")
        self._con.execute(f"ATTACH {md} AS {self._MAT_STORE}")
        self._store_broker = EngineAttachedStore(self._con, self._listing)

    def region_table_address(self, name: str, schema: str, table: str) -> tuple[str, str, str]:
        """Where a statement reads ``schema.table`` of another region's store (REQ-1922): under
        the store's ATTACH, ``name`` (``replica_address.region_read_name``)."""
        return name, schema, table

    def attach_region_read(
        self, name: str, dsn: str, schema: str, table: str, build: object
    ) -> None:
        """REQ-1922: the store is ATTACHed once; it reads that store's tables as they are."""
        del schema, table, build
        self.attach_region_store(name, dsn)

    def attach_region_store(self, name: str, dsn: str) -> str:
        """ATTACH another region's replicas store READ_ONLY under ``name`` — the org
        environment's name for that region's store (``replica_address.region_read_name``) —
        idempotent, and return it (REQ-1922): a table that region names is read from its replica
        there. Only a server store attaches: an embedded DuckDB file belongs to the nodes of its
        own region (refused at load — ``regions.validate_regions``)."""
        from sqlalchemy import make_url

        alias = name
        if alias in self._region_stores:
            return alias
        url = make_url(dsn)
        store_type = self._ATTACH_TYPE_BY_SCHEME.get(url.get_backend_name())
        if store_type is None or store_type in self._FILE_ATTACH_TYPES:
            raise RuntimeError(
                f"{name!r}: that region's replicas store is not a server store this engine can "
                "attach"
            )
        if store_type not in self._NO_EXTENSION_TYPES:
            # A Postgres store loads the extension with its settings too (#241).
            install_and_load(self._con, store_type, from_community=False)
        self._con.execute(
            f"ATTACH '{_attach_url(url)}' AS \"{alias}\" (TYPE {store_type}, READ_ONLY)"
        )
        self._region_stores.add(alias)
        return alias

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
        from provisa.federation.replica_target import text_vectors

        # REQ-424: this engine reads a server store through its connector, so an embedding is
        # held there as its text (as the build writes it, ``PostgresStoreTarget``).
        written = await store_writer.reconcile_table(
            self._store_dsn(),
            schema=schema,
            table=table,
            columns=text_vectors(columns),
            pk_columns=pk_columns,
            key_after_landing=True,
        )
        self._see_store_writes()
        return written

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
        written = await store_writer.land(
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
        self._see_store_writes()
        return written

    async def apply_change_group(self, group: list[Any]) -> None:
        """Apply a group of changes (``federation.change_group``) in one transaction: of the
        embedded DuckDB store through the engine's own connection, else of the server store's."""
        self.ensure_materialize_attached()
        if self._store_is_duckdb():
            self._store_broker.apply_change_group(group)
            return
        from sqlalchemy import and_
        from sqlalchemy.schema import CreateSchema, CreateTable

        from provisa.federation.materialize_exec import apply_cdc, build_table
        from provisa.federation.store_writer import store_connection

        async with store_connection(self._store_dsn()) as conn:
            async with conn.transaction():
                for changes in group:
                    if changes.schema and conn.capabilities.schemas:
                        await conn.execute_core(CreateSchema(changes.schema, if_not_exists=True))
                    table = build_table(
                        changes.schema, changes.table, changes.columns, tuple(changes.pk_columns)
                    )
                    await conn.execute_core(CreateTable(table, if_not_exists=True))
                    for match in changes.delete_where:
                        await conn.execute_core(
                            table.delete().where(and_(*[table.c[k] == v for k, v in match.items()]))
                        )
                    await apply_cdc(conn, table, changes.pk_columns, changes.events)
        self._see_store_writes()

    async def apply_cdc_events(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
        events: list,
    ) -> dict[str, Any]:
        """Apply CDC change events to a landed table (REQ-1733). Duckdb-native dispatch mirroring
        ``land_table``: through the engine's own connection for an embedded DuckDB store (REQ-989 —
        a second connection cannot open a file the engine already ATTACHed), else the server-store
        write face."""
        self.ensure_materialize_attached()
        if self._store_is_duckdb():
            return self._store_broker.apply_cdc(
                schema, table, columns, pk_columns, events, report_unchanged=True
            )
        from sqlalchemy.schema import CreateSchema

        from provisa.federation.materialize_exec import apply_cdc, build_table
        from provisa.federation.store_writer import store_connection

        tbl = build_table(schema, table, columns, tuple(pk_columns))
        async with store_connection(self._store_dsn()) as conn:
            if schema and conn.capabilities.schemas:
                await conn.execute_core(CreateSchema(schema, if_not_exists=True))
            applied = await apply_cdc(conn, tbl, pk_columns, events, report_unchanged=True)
        self._see_store_writes()
        return applied

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
        written = await store_writer.reconcile_table(
            self._store_dsn(), schema=schema, table=table, columns=columns, pk_columns=pk_columns
        )
        self._see_store_writes()
        return written

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
        written = await store_writer.persist_land(
            self._store_dsn(),
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            persist=persist,
            pk_columns=pk_columns,
            match_floor=match_floor,
        )
        self._see_store_writes()
        return written

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
        if self._store_broker is None or not self._store_is_file():
            return duck_sql  # no store, or one attached here and read in place (REQ-1999)
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
        cur = open_cursor(self._con)
        try:
            cur.register(reg_name, arrow_tbl)
            try:
                cur.execute(f'CREATE OR REPLACE TABLE {target} AS SELECT * FROM "{reg_name}"')
            finally:
                cur.unregister(reg_name)
            if is_replicas_schema(schema):
                self._index_replica_vectors(cur, target, f"{schema}__{table}")
        finally:
            close_cursor(cur)
        self._store_copy_canary[target] = canary

    def _index_replica_vectors(self, cur: Any, target: str, name: str) -> None:
        """REQ-424: index each embedding column of the copied replica ``target`` with HNSW for
        cosine distance, so a top-k search reads the index. A replica holds an embedding as
        ``FLOAT[]`` (``ir_types.Embedding``) and nothing else as a float list; the column is
        fixed at its vectors' length, and vectors of two lengths in one column fail the read,
        naming it. A column with no vector has nothing to index."""
        # DESCRIBE binds the one table: a catalog listing (information_schema) would read every
        # attached database, and fail on any whose server is down.
        columns = [
            row[0] for row in cur.execute(f"DESCRIBE {target}").fetchall() if row[1] == "FLOAT[]"
        ]
        if not columns:
            return
        if "vss" not in self._ext_loaded:
            install_and_load(cur, "vss", from_community=False)
            self._ext_loaded.add("vss")
        for column in columns:
            quoted = '"' + column.replace('"', '""') + '"'
            found = cur.execute(
                f"SELECT len({quoted}) FROM {target} WHERE {quoted} IS NOT NULL LIMIT 1"
            ).fetchone()
            if found is None:
                continue
            cur.execute(f"ALTER TABLE {target} ALTER {quoted} TYPE FLOAT[{int(found[0])}]")
            index = "hnsw__" + hashlib.sha256(f"{name}.{column}".encode()).hexdigest()[:24]
            cur.execute(
                f'CREATE INDEX "{index}" ON {target} USING HNSW ({quoted}) '
                "WITH (metric = 'cosine')"
            )

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
            close_cursor(cur)

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
            # Read gate: a held file paused under a query already bound to it returns zero rows
            # rather than failing (_CatalogGate).
            with self._catalog_gate.read():
                sql = self._prepare_sql(duck_sql)
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
                        with named_write_conflict():
                            res = cur.execute(sql, params) if params else cur.execute(sql)
                        cols = [d[0] for d in res.description] if res.description else []
                        types = [str(d[1]) for d in res.description] if res.description else []
                        rows = fetch_rows(res)
                    return QueryResult(rows=rows, column_names=cols, column_types=types)
                finally:
                    close_cursor(cur)

        return await loop.run_in_executor(None, _run)

    def write_result(self, copy_sql: str, params: list | None, *, secret_sql: str | None) -> int:
        """Run ``copy_sql`` (``result_sink.duckdb_copy``: the statement's query wrapped in
        ``COPY ... TO`` the results object store, or ``duckdb_copy_file``: to a file in the org's
        folder) with the statement's bound values, and return the rows written (REQ-1194,
        REQ-1195). ``secret_sql`` is the S3 secret the results bucket is written with
        (``result_sink.duckdb_secret``); it is (re)created first, so a changed results store is
        the one written. None: the result is written to a file on this host and needs no
        secret. Read as any statement is: under the catalog gate, on a private cursor,
        cancelled at the request's deadline."""
        self._catalog_gate.acquire_read()
        try:
            copy_sql = self._prepare_sql(copy_sql)
            copy_sql, live_http = self._rewrite_clickhouse_relations(
                copy_sql, params, deadline_s=request_deadline.remaining()
            )
            if secret_sql is not None:
                if not self._httpfs_loaded:
                    install_and_load(self._con, "httpfs", from_community=False)
                    self._httpfs_loaded = True
                self._con.execute(secret_sql)
            cur = self._open_cursor(live_http=live_http)
            try:
                with request_deadline.cancel_on_deadline(cur.interrupt):
                    cur.execute(copy_sql, params) if params else cur.execute(copy_sql)
                written = cur.fetchone()
            finally:
                close_cursor(cur)
        finally:
            self._catalog_gate.release_read()
        assert written is not None  # COPY ... TO returns exactly one row: the count written
        return int(written[0])

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
            duck_sql = self._prepare_sql(duck_sql)
            duck_sql, live_http = self._rewrite_clickhouse_relations(
                duck_sql, params, deadline_s=request_deadline.remaining()
            )
            cur = self._open_cursor(live_http=live_http)
            try:
                with request_deadline.cancel_on_deadline(cur.interrupt):
                    with named_write_conflict():
                        cur.execute(duck_sql, params) if params else cur.execute(duck_sql)
            except BaseException:
                # No stream will own the cursor: it is closed here (REQ-1905).
                close_cursor(cur)
                raise
        except BaseException:
            self._catalog_gate.release_read()
            raise

        def _close(*_: Any) -> None:
            close_cursor(cur)
            self._catalog_gate.release_read()

        # DuckDB's description carries each column's declared DuckDBPyType ("BIGINT",
        # "DECIMAL(18,2)"), even for a zero-row result — the pgwire Describe relies on it.
        types = [str(d[1]) for d in cur.description] if cur.description else []
        if "INTERVAL" in types:
            # DuckDB's own Python rows fold an interval's months into days; its Arrow batches keep
            # them (duckdb_driver.fetch_rows).
            reader = cur.to_arrow_reader(_ARROW_STREAM_BATCH_ROWS)
            return stream_rows_from_arrow(
                reader.schema, iter(reader), on_close=_close, column_types=types
            )
        return stream_from_dbapi(
            cur, on_close=_close, type_names=lambda codes: [str(c) for c in codes]
        )

    def describe_sync(self, duck_sql: str, params: list | None = None) -> ResultStream:
        """The statement's result shape without running it: DuckDB's ``DESCRIBE <query>`` binds and
        plans it and returns each column's name and declared type — the same names and type strings
        run_sync's cursor description reports, duplicates included (a subquery wrapper would rename
        a duplicate ``a`` to ``a_1``). REQ-589."""
        with self._catalog_gate.read():
            duck_sql = self._prepare_sql(duck_sql)
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
                close_cursor(cur)
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
            duck_sql = self._prepare_sql(duck_sql)
            duck_sql, live_http = self._rewrite_clickhouse_relations(
                duck_sql, params, deadline_s=request_deadline.remaining()
            )
            # PRIVATE cursor for the same reason as run_sync/run_arrow_stream: a concurrent query on
            # the shared connection replaces this one's pending result, and the fetch then yields
            # nothing instead of raising.
            cur = self._open_cursor(live_http=live_http)
            try:
                with request_deadline.cancel_on_deadline(cur.interrupt):
                    with named_write_conflict():
                        res = cur.execute(duck_sql, params) if params else cur.execute(duck_sql)
                    return res.to_arrow_table()
            finally:
                close_cursor(cur)

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
            duck_sql = self._prepare_sql(duck_sql)
            duck_sql, live_http = self._rewrite_clickhouse_relations(
                duck_sql, params, deadline_s=request_deadline.remaining()
            )
            cur = self._open_cursor(live_http=live_http)
            try:
                with request_deadline.cancel_on_deadline(cur.interrupt):
                    with named_write_conflict():
                        cur.execute(duck_sql, params) if params else cur.execute(duck_sql)
                    reader = cur.to_arrow_reader(_ARROW_STREAM_BATCH_ROWS)
                schema = reader.schema
            except BaseException:
                # No stream will own the cursor: it is closed here (REQ-1905).
                close_cursor(cur)
                raise
        except BaseException:
            self._catalog_gate.release_read()
            raise

        def _batches():
            try:
                for batch in reader:
                    yield batch
            finally:
                close_cursor(cur)
                self._catalog_gate.release_read()

        return schema, _batches()

    def close(self) -> None:
        for raw_alias in list(self._file_holders):
            self._let_file_go(raw_alias)
        self._con.close()
