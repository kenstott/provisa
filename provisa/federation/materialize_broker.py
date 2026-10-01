# Copyright (c) 2026 Kenneth Stott
# Canary: 3d1f6e4e-5ce5-4b79-9502-c273ddd9e3e7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Serializes every operation against the embedded DuckDB materialize store across `--workers N`
processes (REQ-1901) -- WITHOUT any persistent cross-process broker, election, or socket.

A dedicated SENTINEL LOCK FILE (`<db_path>.lock`, `fcntl.flock`) is the synchronization primitive:
every operation acquires it (a blocking OS-level mutex -- no polling, no backoff, no parsing
DuckDB's own error text to guess whether a failure means "busy" or "broken"), ATTACHes the actual
store file (now guaranteed uncontested), does its one operation, DETACHes (closes the connection),
and releases the lock. No process ever holds either the lock or the file open longer than a single
call, and `flock` is tied to the file descriptor -- a process that dies mid-operation (crash,
SIGKILL) releases it automatically, with no stale-socket or stale-lock cleanup logic needed at all.

This replaces an earlier persistent-broker-process design (one process elected via a Unix socket +
`multiprocessing.managers`, serving every other worker's calls over that connection for the
runtime's whole lifetime). That design was correct in principle but added a large amount of
incidental complexity for a problem this file solves directly: election races, a same-process
self-loopback connect that broke on reconnect, and — confirmed live under a real `--workers 8`
benchmark run — a genuine split-brain recurrence (`Unique file handle conflict`) in a scenario
(an established broker plus one respawned worker rejoining) the earlier design's own tests never
covered. None of that exists here: there is no persistent state to split, and no connection to
lose and reconnect, because no connection ever outlives one call.
"""

from __future__ import annotations

import fcntl
import os
import threading
import time
from collections.abc import Callable
from typing import Any

import duckdb

_MAT_STORE_ALIAS = "mat_store"
_LOCK_SUFFIX = ".lock"
_LOCK_POLL_S = 0.005


def _lock_exclusive(fd: int) -> None:
    """Take the exclusive `flock`, bounded by the current request's remaining budget (REQ-1882).

    Outside a request there is no budget and the wait is unbounded, as before. Inside one, a
    blocking `flock` could not be pre-empted by the request deadline (it is not a statement with a
    cancel), so the lock is polled non-blocking until it frees or the budget runs out."""
    from provisa.core import request_deadline

    if request_deadline.remaining() is None:
        fcntl.flock(fd, fcntl.LOCK_EX)
        return
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            left = request_deadline.remaining()
            if left is None or left <= 0:
                raise TimeoutError("request budget spent waiting for the materialize store lock")
            time.sleep(min(_LOCK_POLL_S, left))


_GEN_SUFFIX = ".gen"
StoreCanary = int

_MEMORY_PATH = ":memory:"


class _MemoryStore:
    """The in-memory store (``duckdb:///:memory:``): one database for the life of the process.

    It has no file, so nothing beside a file either — no sentinel lock file, no generation file.
    No other process can reach it, so the lock is a thread lock and the generation a counter. And
    because a database attached from ``:memory:`` lives only as long as the connection that
    attached it, that connection stays open: a connection opened and closed per call, as the file
    store's is, would hand every call a new empty database."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.generation: StoreCanary = 0
        self.db = duckdb.connect(_MEMORY_PATH)
        self.db.execute(f"ATTACH '{_MEMORY_PATH}' AS {_MAT_STORE_ALIAS}")


_memory_store: _MemoryStore | None = None
_memory_store_guard = threading.Lock()


def _memory() -> _MemoryStore:
    global _memory_store
    with _memory_store_guard:
        if _memory_store is None:
            _memory_store = _MemoryStore()
        return _memory_store


def _with_memory_store(fn: Any, *, read_only: bool, canary: bool) -> Any:
    """``_with_store`` for the in-memory store: the same one-operation-at-a-time contract and the
    same generation semantics, held in this process instead of in files."""
    from provisa.core import request_deadline

    store = _memory()
    with store.lock:
        # A cursor is its own connection onto the same database: the operation's temporary
        # registrations go away with it, as they do when the file store's connection closes.
        con = store.db.cursor()
        try:
            with request_deadline.cancel_on_deadline(con.interrupt):
                result = fn(con)
        finally:
            con.close()
            if not read_only:
                store.generation += 1
        return (result, store.generation) if canary else result


def store_canary(db_path: str) -> StoreCanary:
    """The store's write generation: incremented (under the sentinel lock) by every non-read-only
    store operation from any process, so a reader holding a copy taken at generation G knows it is
    still current while the generation still reads G. An exact counter, not a file mtime — mtime
    ticks coarsely enough that a same-size write inside one tick would look unchanged."""
    if db_path == _MEMORY_PATH:
        return _memory().generation
    try:
        with open(db_path + _GEN_SUFFIX) as f:
            return int(f.read())
    except FileNotFoundError:
        return 0


def _bump_generation(db_path: str) -> None:
    """Advance the write generation. Caller holds the sentinel lock; the replace is atomic, so an
    unlocked reader sees either the old or the new value, never a torn one."""
    tmp = db_path + _GEN_SUFFIX + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(store_canary(db_path) + 1))
    os.replace(tmp, db_path + _GEN_SUFFIX)


def _with_store(db_path: str, fn: Any, *, read_only: bool = False, canary: bool = False) -> Any:
    """Acquire the sentinel lock for `db_path` (waiting at most the request's remaining budget),
    ATTACH it under the `mat_store` alias, run `fn(con)` (cancellable at the request deadline), then
    DETACH (close) and release the lock -- see module docstring. The lock file itself is never read
    or written to; its only role is to be an `flock`-able handle, created if missing.

    ``read_only`` ATTACHes READ_ONLY; every other call may write, so it advances the store's write
    generation (even on failure — a partial write must not look unchanged). ``canary`` returns
    ``(result, generation)`` read while the lock is still held, so no writer can land between the
    read and the generation it is tagged with."""
    if db_path == _MEMORY_PATH:
        return _with_memory_store(fn, read_only=read_only, canary=canary)

    from provisa.core import request_deadline

    lock_path = db_path + _LOCK_SUFFIX
    mode = ", READ_ONLY" if read_only else ""
    with open(lock_path, "a+") as lockfile:
        _lock_exclusive(lockfile.fileno())
        try:
            con = duckdb.connect(":memory:")
            con.execute(f"ATTACH '{db_path}' AS {_MAT_STORE_ALIAS} (TYPE duckdb{mode})")
            try:
                with request_deadline.cancel_on_deadline(con.interrupt):
                    result = fn(con)
            finally:
                con.close()
                if not read_only:
                    _bump_generation(db_path)
            return (result, store_canary(db_path)) if canary else result
        finally:
            fcntl.flock(lockfile.fileno(), fcntl.LOCK_UN)


def _table_columns(con: Any, schema: str, table: str) -> list[str] | None:
    rows = con.execute(
        "SELECT column_name FROM duckdb_columns() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ? "
        "ORDER BY column_index",
        [_MAT_STORE_ALIAS, schema, table],
    ).fetchall()
    return [r[0] for r in rows] or None


class _SyncedStore:
    """Per-runtime handle exposing the same method surface `duckdb_runtime.py`/
    `query_residency.py` already call — each method opens the file, does its one operation, and
    closes it (see `_with_store`); nothing here is stateful or held open between calls."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def reconcile(self, schema: str, table: str, columns: list[tuple[str, str]]) -> str:
        from provisa.federation.store_connection import reconcile_duckdb_native

        return _with_store(
            self._db_path,
            lambda con: reconcile_duckdb_native(
                con, catalog=_MAT_STORE_ALIAS, schema=schema, table=table, columns=columns
            ),
        )

    def land(
        self,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        change_signal: str,
        watermark_column: str | None,
    ) -> str:
        from provisa.federation.store_connection import land_duckdb_native

        return _with_store(
            self._db_path,
            lambda con: land_duckdb_native(
                con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                columns=columns,
                rows=rows,
                change_signal=change_signal,
                watermark_column=watermark_column,
            ),
        )

    def upsert_arrow(
        self,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
        data: Any,
    ) -> int:
        """Upsert an Arrow table by ``pk_columns`` under one lock hold (REQ-1865 row cache) --
        handed over columnar like ``write_mv``'s ``fresh``, never as per-row events."""
        from provisa.federation.store_connection import upsert_arrow_duckdb_native

        return _with_store(
            self._db_path,
            lambda con: upsert_arrow_duckdb_native(
                con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                columns=columns,
                pk_columns=pk_columns,
                data=data,
            ),
        )

    def apply_cdc(
        self,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
        events: list,
    ) -> dict[str, int]:
        from provisa.federation.store_connection import apply_cdc_duckdb_native

        return _with_store(
            self._db_path,
            lambda con: apply_cdc_duckdb_native(
                con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                columns=columns,
                pk_columns=pk_columns,
                events=events,
            ),
        )

    def ensure_row_cache_table(
        self, schema: str, table: str, columns: list[tuple[str, str]]
    ) -> None:
        from provisa.federation.store_connection import ensure_row_cache_table_duckdb_native

        _with_store(
            self._db_path,
            lambda con: ensure_row_cache_table_duckdb_native(
                con, catalog=_MAT_STORE_ALIAS, schema=schema, table=table, columns=columns
            ),
        )

    def ensure_and_read_row_cache(
        self,
        schema: str,
        table: str,
        ensure_columns: list[tuple[str, str]],
        pk_columns: list[str],
        keys: list[tuple[Any, ...]],
    ) -> dict[tuple[Any, ...], Any]:
        """Reconcile the cache table's columns and read it back, under ONE lock hold.

        Doing this as two separate ``_with_store`` calls (ensure, then read) leaves a window
        between them: with ``--workers N``, another worker's ordinary whole-table materialize
        land can recreate this same table (without the cache's ``_row_cached_at``/
        ``_row_expires_at`` columns) in that gap, so the read that follows hits a table the
        reconcile step just fixed and now finds broken again -- confirmed live (``Binder Error:
        Referenced column "_row_expires_at" not found``) under a real 8-worker benchmark run.
        Holding the lock across both closes the window: no other worker's call can touch this
        file between the reconcile and the read that depends on it."""
        from provisa.federation.store_connection import (
            ensure_row_cache_table_duckdb_native,
            read_row_cache_duckdb_native,
        )

        def _do(con: Any) -> dict[tuple[Any, ...], Any]:
            ensure_row_cache_table_duckdb_native(
                con, catalog=_MAT_STORE_ALIAS, schema=schema, table=table, columns=ensure_columns
            )
            return read_row_cache_duckdb_native(
                con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                pk_columns=pk_columns,
                keys=keys,
            )

        return _with_store(self._db_path, _do)

    def read_row_cache(
        self,
        schema: str,
        table: str,
        pk_columns: list[str],
        keys: list[tuple[Any, ...]],
    ) -> dict[tuple[Any, ...], Any]:
        from provisa.federation.store_connection import read_row_cache_duckdb_native

        return _with_store(
            self._db_path,
            lambda con: read_row_cache_duckdb_native(
                con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                pk_columns=pk_columns,
                keys=keys,
            ),
        )

    def tombstone_row_cache(
        self,
        schema: str,
        table: str,
        pk_columns: list[str],
        keys: list[tuple[Any, ...]],
    ) -> None:
        from provisa.federation.store_connection import tombstone_row_cache_duckdb_native

        _with_store(
            self._db_path,
            lambda con: tombstone_row_cache_duckdb_native(
                con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                pk_columns=pk_columns,
                keys=keys,
            ),
        )

    def persist(
        self,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        persist: str,
        pk_columns: list[str] | None,
    ) -> str:
        from provisa.federation.store_connection import persist_duckdb_native

        return _with_store(
            self._db_path,
            lambda con: persist_duckdb_native(
                con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                columns=columns,
                rows=rows,
                persist=persist,
                pk_columns=pk_columns,
            ),
        )

    def table_columns(self, schema: str, table: str) -> list[str] | None:
        """The store table's column names in ordinal order, or ``None`` when it does not exist."""
        return _with_store(self._db_path, lambda con: _table_columns(con, schema, table))

    def execute(self, sql: str) -> list[tuple]:
        """Run one statement against the store (``mat_store.*`` names resolve) and return its rows.
        For the MV-maintenance statements that act on the store alone (reclaim/orphan DROP, SHOW
        TABLES) — the engine connection never ATTACHes this file (REQ-1901)."""
        return _with_store(self._db_path, lambda con: con.execute(sql).fetchall())

    def write_mv(
        self,
        schema: str,
        table: str,
        fresh: Any,
        plan: Callable[[list[str] | None], list[str]],
    ) -> int:
        """Write an MV refresh into the store under ONE lock hold; return the target's row count.

        ``fresh`` is the MV SELECT's result, computed by the ENGINE (which reads the sources) and
        handed over as Arrow (a ``RecordBatchReader`` streams: it is staged into a temp table batch
        by batch, never materialized in Python). ``plan(existing_columns)`` returns the store-side
        statements, written against ``_mv_fresh`` for the fresh rows and ``mat_store.<schema>.<table>``
        for the target — the same CTAS / DELETE+INSERT / bitemporal-append statements the engine
        path runs, so both paths share one set of semantics. ``existing_columns`` is read inside the
        same lock hold the statements run under."""

        def _do(con: Any) -> int:
            con.register("_mv_fresh_src", fresh)
            try:
                con.execute("CREATE TEMP TABLE _mv_fresh AS SELECT * FROM _mv_fresh_src")
            finally:
                con.unregister("_mv_fresh_src")
            con.execute(f'CREATE SCHEMA IF NOT EXISTS {_MAT_STORE_ALIAS}."{schema}"')
            for stmt in plan(_table_columns(con, schema, table)):
                con.execute(stmt)
            row = con.execute(
                f'SELECT COUNT(*) FROM {_MAT_STORE_ALIAS}."{schema}"."{table}"'
            ).fetchone()
            assert row is not None  # COUNT(*) always yields a row
            return int(row[0])

        return _with_store(self._db_path, _do)

    def fetch_arrow(self, schema: str, table: str) -> tuple[Any, StoreCanary]:
        """Full current contents of a landed/materialized table, as Arrow, plus the store canary
        it is current as of -- the read half of this module's contract. A worker copies the table
        into its own connection before a query that references it, and re-copies only once
        ``canary()`` no longer matches (some process wrote the store since)."""
        return _with_store(
            self._db_path,
            lambda con: con.execute(
                f'SELECT * FROM {_MAT_STORE_ALIAS}."{schema}"."{table}"'
            ).fetch_arrow_table(),
            read_only=True,
            canary=True,
        )

    def canary(self) -> StoreCanary:
        """The store's current canary (no lock: a stat is atomic, and a mismatch only re-copies)."""
        return store_canary(self._db_path)


def get_broker(db_path: str) -> _SyncedStore:
    """A handle for `db_path` whose every method call independently opens, uses, and closes the
    file (see `_SyncedStore`/`_with_store`) — cheap to call repeatedly; no state to share or
    reconnect, so every caller gets a fresh, equally-valid handle."""
    return _SyncedStore(db_path)
