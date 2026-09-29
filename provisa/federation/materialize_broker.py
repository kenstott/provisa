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
from typing import Any

import duckdb

_MAT_STORE_ALIAS = "mat_store"
_LOCK_SUFFIX = ".lock"


def _with_store(db_path: str, fn: Any) -> Any:
    """Acquire the sentinel lock for `db_path` (blocks until available), ATTACH it under the
    `mat_store` alias, run `fn(con)`, then DETACH (close) and release the lock -- see module
    docstring. The lock file itself is never read or written to; its only role is to be an
    `flock`-able handle, created if missing."""
    lock_path = db_path + _LOCK_SUFFIX
    with open(lock_path, "a+") as lockfile:
        fcntl.flock(lockfile.fileno(), fcntl.LOCK_EX)
        try:
            con = duckdb.connect(":memory:")
            con.execute(f"ATTACH '{db_path}' AS {_MAT_STORE_ALIAS} (TYPE duckdb)")
            try:
                return fn(con)
            finally:
                con.close()
        finally:
            fcntl.flock(lockfile.fileno(), fcntl.LOCK_UN)


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

    def fetch_arrow(self, schema: str, table: str) -> Any:
        """Full current contents of a landed/materialized table, as Arrow -- the read half of
        this module's contract. Called by a worker immediately before it executes any query
        referencing this table, so the worker's own connection can register the result as a local
        relation and join it against its OTHER (live-attached) sources; always current as of the
        call, since it opens the file fresh every time rather than caching anything."""
        return _with_store(
            self._db_path,
            lambda con: con.execute(
                f'SELECT * FROM {_MAT_STORE_ALIAS}."{schema}"."{table}"'
            ).fetch_arrow_table(),
        )


def get_broker(db_path: str) -> _SyncedStore:
    """A handle for `db_path` whose every method call independently opens, uses, and closes the
    file (see `_SyncedStore`/`_with_store`) — cheap to call repeatedly; no state to share or
    reconnect, so every caller gets a fresh, equally-valid handle."""
    return _SyncedStore(db_path)
