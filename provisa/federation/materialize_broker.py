# Copyright (c) 2026 Kenneth Stott
# Canary: 3d1f6e4e-5ce5-4b79-9502-c273ddd9e3e7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Singleton broker owning the ONE embedded-DuckDB materialize-store connection (REQ-1901).

DuckDB's file lock is exclusive regardless of the requested access mode -- confirmed empirically
across three isolated topologies (full mesh of cross-attached files, hub-and-spoke, and a plain
two-process writer/reader pair): whichever connection opens the file FIRST, in ANY mode, blocks
every OTHER connection from opening it in ANY mode (including READ_ONLY) for as long as the first
stays open. There is no multi-connection topology that works for one physical file under
`--workers N` -- pooling separate files instead just trades the crash for silent per-worker
divergence (zero cross-worker visibility of landed data).

The only correct fix: exactly ONE process ever opens the file. This module elects that one
process ("first to bind the broker socket wins" -- the same pattern REQ-1900 already uses for the
Flight port pool) and serves every operation against the store -- reads AND writes -- through it,
over a local Unix-domain-socket RPC (`multiprocessing.managers.BaseManager`). A worker process,
including the elected one, never touches the file directly; it always goes through this handle.
Applies uniformly regardless of worker count -- a single-worker deployment just pays one local
loopback IPC hop per operation instead of a direct call, which keeps this module's contract
identical (no worker-count detection, no separate code path) in the only tier this store exists
for (the embedded, dev-desktop DuckDB store, REQ-989) -- production's multi-worker target
(Trino/PG materialize store) has no analogous lock at all and never goes through this module.
"""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
import threading
import time
from multiprocessing.managers import BaseManager
from typing import Any

import duckdb

log = logging.getLogger(__name__)

_AUTHKEY = b"provisa-materialize-broker-v1"
_MAT_STORE_ALIAS = "mat_store"


def _socket_path(db_path: str) -> str:
    """A deterministic per-file broker address, so distinct materialize files (tests, multiple
    tenants running the embedded store) each get their own broker, and every worker pointed at the
    SAME file independently derives the SAME address to race for."""
    digest = hashlib.sha1(os.path.abspath(db_path).encode(), usedforsecurity=False).hexdigest()[:16]
    return os.path.join(tempfile.gettempdir(), f"provisa_matbroker_{digest}.sock")


class _Broker:
    """The elected process's real state: one DuckDB connection with the store file ATTACHed under
    the same `mat_store` alias every store_connection.py function already expects, and one lock
    serializing every operation -- reads included, per this module's docstring: no other holder of
    this object may let two operations touch `self._con` concurrently, since the underlying
    guarantee this whole module exists for is "at most one open handle to the file, at most one
    operation in flight against it at a time.\""""

    def __init__(self, db_path: str) -> None:
        self._lock = threading.RLock()
        self._con = duckdb.connect(":memory:")
        self._con.execute(f"ATTACH '{db_path}' AS {_MAT_STORE_ALIAS} (TYPE duckdb)")

    def reconcile(self, schema: str, table: str, columns: list[tuple[str, str]]) -> str:
        from provisa.federation.store_connection import reconcile_duckdb_native

        with self._lock:
            return reconcile_duckdb_native(
                self._con, catalog=_MAT_STORE_ALIAS, schema=schema, table=table, columns=columns
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

        with self._lock:
            return land_duckdb_native(
                self._con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                columns=columns,
                rows=rows,
                change_signal=change_signal,
                watermark_column=watermark_column,
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

        with self._lock:
            return apply_cdc_duckdb_native(
                self._con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                columns=columns,
                pk_columns=pk_columns,
                events=events,
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

        with self._lock:
            return persist_duckdb_native(
                self._con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                columns=columns,
                rows=rows,
                persist=persist,
                pk_columns=pk_columns,
            )

    def fetch_arrow(self, schema: str, table: str) -> Any:
        """Full current contents of a landed/materialized table, as Arrow -- the read half of this
        module's contract. Called by a worker immediately before it executes any query referencing
        this table, so the worker's own connection can register the result as a local relation and
        join it against its OTHER (live-attached) sources; always current as of the call, since it
        is re-fetched from the one real connection every time rather than cached."""
        with self._lock:
            return self._con.execute(
                f'SELECT * FROM {_MAT_STORE_ALIAS}."{schema}"."{table}"'
            ).fetch_arrow_table()


_local_cache: dict[str, "_Broker"] = {}
_local_cache_lock = threading.Lock()


def get_broker(db_path: str) -> _Broker:
    """Return a handle to THE broker for `db_path` -- either a direct reference (this process won
    election) or a `multiprocessing.managers` proxy to whichever other process did. Every method on
    `_Broker` above is reachable identically either way (BaseManager auto-generates proxy methods
    matching the registered class's public methods).

    Election: try to CONNECT to an existing broker socket first; if none answers, try to BIND it
    (become the broker) instead. A bind can lose a narrow race against another process doing the
    same thing at the same instant -- caught as `OSError` (address already in use) and retried as a
    connect, bounded, since the loser's correct next move is simply "the winner is up now, go be a
    client.\""""
    resolved = os.path.abspath(db_path)
    with _local_cache_lock:
        cached = _local_cache.get(resolved)
    if cached is not None:
        return cached

    sock = _socket_path(resolved)

    class _ProvisaMatBrokerManager(BaseManager):
        pass

    _ProvisaMatBrokerManager.register("Broker")

    for _ in range(10):
        try:
            mgr = _ProvisaMatBrokerManager(address=sock, authkey=_AUTHKEY)
            mgr.connect()
            handle = getattr(mgr, "Broker")()
            with _local_cache_lock:
                _local_cache[resolved] = handle
            return handle
        except ConnectionRefusedError:
            # The socket FILE exists but nothing is listening behind it -- definitively a stale
            # entry left by a prior process that exited without cleanup, never a live peer (a live
            # listener would have accepted). Safe to remove and try to become the broker ourselves.
            try:
                os.remove(sock)
            except OSError:
                pass
        except FileNotFoundError:
            pass  # nothing there at all yet -- go straight to attempting to bind
        except OSError:
            # Ambiguous (e.g. a peer mid-listen(), or a transient local resource error) -- NOT a
            # confirmed-dead socket, so never remove it: doing so could rip a live peer's listener
            # out from under it, letting a second process bind its own independent broker and
            # silently split the singleton (confirmed live: exactly this happened once before this
            # branch existed -- two workers each landed data the other never saw). Just back off
            # and retry the connect, without attempting to bind at all this iteration.
            time.sleep(0.05)
            continue

        # Lazy construction: the DuckDB connect+ATTACH happens on the FIRST real `.Broker()` call
        # (ours or a peer's), never here -- constructing it before we know we've actually won the
        # bind would otherwise open (and never close) a connection to the file on every losing
        # attempt, corrupting this SAME process's own next retry.
        holder: list[_Broker] = []

        def _make(_holder: list[_Broker] = holder, _path: str = resolved) -> _Broker:
            if not _holder:
                _holder.append(_Broker(_path))
            return _holder[0]

        class _ProvisaMatBrokerServerManager(BaseManager):
            pass

        _ProvisaMatBrokerServerManager.register("Broker", callable=_make)
        try:
            server_mgr = _ProvisaMatBrokerServerManager(address=sock, authkey=_AUTHKEY)
            server = server_mgr.get_server()
        except OSError:
            # Lost the bind race to another process between our check above and this bind. Its
            # listener is now (or about to be) up -- loop back and connect to it instead.
            time.sleep(0.05)
            continue

        thread = threading.Thread(
            target=server.serve_forever, name="provisa-materialize-broker", daemon=True
        )
        thread.start()
        time.sleep(0.05)  # let the listener actually start accepting before we connect to it

        client_mgr = _ProvisaMatBrokerManager(address=sock, authkey=_AUTHKEY)
        client_mgr.connect()
        handle = getattr(client_mgr, "Broker")()
        with _local_cache_lock:
            _local_cache[resolved] = handle
        return handle

    raise RuntimeError(f"could not establish a materialize broker for {resolved!r}")
