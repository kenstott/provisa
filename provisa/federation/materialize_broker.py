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

    def ensure_row_cache_table(
        self, schema: str, table: str, columns: list[tuple[str, str]]
    ) -> None:
        from provisa.federation.store_connection import ensure_row_cache_table_duckdb_native

        with self._lock:
            ensure_row_cache_table_duckdb_native(
                self._con, catalog=_MAT_STORE_ALIAS, schema=schema, table=table, columns=columns
            )

    def read_row_cache(
        self,
        schema: str,
        table: str,
        pk_columns: list[str],
        keys: list[tuple[Any, ...]],
    ) -> dict[tuple[Any, ...], Any]:
        from provisa.federation.store_connection import read_row_cache_duckdb_native

        with self._lock:
            return read_row_cache_duckdb_native(
                self._con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                pk_columns=pk_columns,
                keys=keys,
            )

    def tombstone_row_cache(
        self,
        schema: str,
        table: str,
        pk_columns: list[str],
        keys: list[tuple[Any, ...]],
    ) -> None:
        from provisa.federation.store_connection import tombstone_row_cache_duckdb_native

        with self._lock:
            tombstone_row_cache_duckdb_native(
                self._con,
                catalog=_MAT_STORE_ALIAS,
                schema=schema,
                table=table,
                pk_columns=pk_columns,
                keys=keys,
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


_local_cache: dict[str, Any] = {}
_local_cache_lock = threading.Lock()

# Connection-level errors meaning "the peer end of this proxy is gone" -- never a real
# application error the broker itself would raise (those come back as ordinary Python
# exceptions from the CALLED method, not these). Retrying a call after ANY of these means
# re-electing first (see _ResilientBrokerHandle), since the whole point of the cache this
# guards is "reuse the same connection" -- once it is confirmed dead, the cached handle is not
# just stale, it can never succeed again.
_DEAD_CONNECTION_ERRORS = (ConnectionError, EOFError)


class _ResilientBrokerHandle:
    """Wraps whatever `_connect_or_elect` returns (a direct `_Broker` reference or a
    `multiprocessing.managers` proxy to a peer) and re-elects transparently if the underlying
    connection dies mid-run.

    Confirmed live (REQ-1901 full-sweep testing): the elected broker's OWN worker process can be
    killed and respawned by uvicorn (its own crash, unrelated to the broker) independently of
    every OTHER worker's already-cached proxy to it -- every one of those workers then got
    `BrokenPipeError` on EVERY subsequent call, forever, since `get_broker()`'s cache was never
    invalidated and nothing ever re-elected. A dead broker is not a permanent failure: some worker
    (possibly this one) simply needs to win election again, exactly as at startup -- so a dead-
    connection error here evicts the cache and retries via `_connect_or_elect`, the same
    "connect first, elect if nobody answers" path every worker already runs at its own startup --
    bounded (a few attempts, short backoff) rather than a single retry: confirmed live that a
    freshly re-elected handle can itself fail on its very first real call in some same-process
    self-loopback timing windows, so one retry is not always enough, but the failure clears on a
    subsequent attempt once that transient window passes."""

    _MAX_ATTEMPTS = 4
    _RETRY_BACKOFF_S = 0.1

    def __init__(self, resolved_path: str) -> None:
        self._resolved = resolved_path

    def __getattr__(self, name: str) -> Any:
        def _call(*args: Any, **kwargs: Any) -> Any:
            last_exc: BaseException | None = None
            for attempt in range(self._MAX_ATTEMPTS):
                with _local_cache_lock:
                    handle = _local_cache.get(self._resolved)
                if handle is None:
                    handle = _connect_or_elect(self._resolved)
                    with _local_cache_lock:
                        _local_cache[self._resolved] = handle
                try:
                    return getattr(handle, name)(*args, **kwargs)
                except _DEAD_CONNECTION_ERRORS as exc:
                    last_exc = exc
                    log.warning(
                        "[REQ-1901] materialize broker connection lost (attempt %d/%d); "
                        "re-electing and retrying %r",
                        attempt + 1,
                        self._MAX_ATTEMPTS,
                        name,
                    )
                    with _local_cache_lock:
                        _local_cache.pop(self._resolved, None)
                    time.sleep(self._RETRY_BACKOFF_S)
            assert last_exc is not None
            raise last_exc

        return _call


def get_broker(db_path: str) -> _ResilientBrokerHandle:
    """Return a handle to THE broker for `db_path` — self-healing across the underlying
    connection dying mid-run (see `_ResilientBrokerHandle`). Cheap to call repeatedly; every
    caller in this codebase already does (`ensure_materialize_attached` re-resolves it whenever
    `_store_attached` is false, which is only ever once per runtime instance)."""
    return _ResilientBrokerHandle(os.path.abspath(db_path))


_process_used_manager_before = False


def _connect_or_elect(resolved: str) -> Any:
    """Either a direct reference (this process won election) or a `multiprocessing.managers`
    proxy to whichever other process did. Every method on `_Broker` above is reachable
    identically either way (BaseManager auto-generates proxy methods matching the registered
    class's public methods).

    Election: try to CONNECT to an existing broker socket first; if none answers, try to BIND it
    (become the broker) instead. A bind can lose a narrow race against another process doing the
    same thing at the same instant -- caught as `OSError` (address already in use) and retried as a
    connect, bounded, since the loser's correct next move is simply "the winner is up now, go be a
    client.\"

    `_process_used_manager_before` gates HOW a winning bind connects to its own new server:
    confirmed live, a process making its FIRST-EVER `multiprocessing.managers` connection in this
    scenario (the real concurrent-startup race, N workers electing simultaneously with no prior
    connections) self-loop-connects safely -- verified across 25+ concurrent 4-process race
    trials, zero split-brain. But a process that already held an EARLIER connection (to a peer
    that has since died -- the resilience/reconnect path, REQ-1901's `_ResilientBrokerHandle`)
    and then self-loop-connects to its OWN newly bound server gets a handle whose first real RPC
    succeeds but every call after immediately raises `BrokenPipeError` -- some same-process
    client+server threading/socket state left over from the earlier connection. Bypassing the
    self-connect (constructing the `_Broker` directly, no socket round-trip for OUR OWN use) genuinely
    is NOT racy in itself for a single already-differentiated winner, but confirmed live it
    reliably CAUSES split-brain when used unconditionally on the FRESH-election path with several
    processes racing at once (not yet root-caused precisely; empirically 12/15 trials failed with
    the bypass applied unconditionally, 0/25 failed with self-loopback). So the bypass is used
    ONLY on this narrower, already-differentiated resilience path, never on a process's first
    election."""
    global _process_used_manager_before
    sock = _socket_path(resolved)

    class _ProvisaMatBrokerManager(BaseManager):
        pass

    _ProvisaMatBrokerManager.register("Broker")

    for _ in range(10):
        try:
            mgr = _ProvisaMatBrokerManager(address=sock, authkey=_AUTHKEY)
            mgr.connect()
            handle = getattr(mgr, "Broker")()
            _process_used_manager_before = True
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

        if _process_used_manager_before:
            # Resilience/reconnect path (see docstring): skip the self-loopback socket round-trip
            # entirely for our own use -- a REAL peer still reaches this exact object normally,
            # via its own genuinely separate `mgr.connect()`, unaffected by this.
            return _make()

        time.sleep(0.05)  # let the listener actually start accepting before we connect to it
        client_mgr = _ProvisaMatBrokerManager(address=sock, authkey=_AUTHKEY)
        client_mgr.connect()
        handle = getattr(client_mgr, "Broker")()
        _process_used_manager_before = True
        return handle

    raise RuntimeError(f"could not establish a materialize broker for {resolved!r}")
