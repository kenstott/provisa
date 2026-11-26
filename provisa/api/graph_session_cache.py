# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e4a92-3b58-4f0d-9e26-d84a1f05b6c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each worker's copy of the graph sessions it serves (REQ-2176, amended 2026-11-23: sessions
cached per worker).

A worker keeps what each principal's ``graph_sessions`` row held when it last read it, so a
statement with no write clause on a cached session touches no control-plane row. Every write of a
row -- a statement writing its session back, a session begun or begun anew, a clear, a sweep --
sends a NOTIFY on :data:`CHANNEL` in the transaction that writes it, so it is delivered when that
commits; each worker LISTENs on one connection of its own per control-plane database and drops the
copy the notification names. A write made on another worker is so seen once its notification
arrives. A worker that loses the channel drops every copy and reads rows again (says so once, in
its log) until it reconnects.

Within a worker a statement on a session holding objects keeps the order the row lock gave it: a
read waits for a write of the same session this worker is making, and a write for its reads
(:class:`_Gate`). Across workers the row lock still orders writes; a read on another worker's copy
sees a write once its notification arrives.
"""

# Requirements: REQ-2176

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from sqlalchemy import event, func, select
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

#: The control plane's channel for graph-session writes. Its payload names the row written: the
#: org schema it is in, its principal, and the generation it now holds (null once deleted).
CHANNEL = "provisa_graph_session"

#: The listening connection's application name, by which it is told apart in ``pg_stat_activity``.
APPLICATION_NAME = "provisa-graph-session-listen"

#: How often the listening connection is checked alive while no notification comes, and how long
#: a lost one waits before it connects again.
_BEAT_S = 1.0
_RECONNECT_S = 1.0

Key = tuple[str, str]  # (org schema, principal)


@dataclass(frozen=True)
class Held:
    """What a principal's ``graph_sessions`` row held when this worker read it."""

    generation: int
    rights: str
    used_at: datetime
    tables: Any
    graph: Any
    stored: bool
    #: It holds no object: a read of it needs no order against a write (``graph_session``).
    empty: bool

    @classmethod
    def of(cls, row: Any, empty: bool) -> Held:
        return cls(
            row.generation, row.rights, row.used_at, row.tables, row.graph, row.stored, empty
        )


def notify(schema: str, user_id: str, generation: int | None) -> Any:
    """The statement that tells every worker the row of ``user_id`` in ``schema`` was written:
    sent in the transaction that writes it, it is delivered when that commits."""
    payload = json.dumps({"s": schema, "u": user_id, "g": generation})
    return select(func.pg_notify(CHANNEL, payload))


class _Gate:
    """A worker's order between the statements of one session holding objects: reads together,
    a write alone; a waiting write goes before reads that come after it."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._reading = 0
        self._writing = False
        self._waiting = 0

    @contextmanager
    def reading(self) -> Generator[None]:
        with self._cond:
            while self._writing or self._waiting:
                self._cond.wait()
            self._reading += 1
        try:
            yield
        finally:
            with self._cond:
                self._reading -= 1
                self._cond.notify_all()

    def _enter_writing(self, wait: bool) -> bool:
        with self._cond:
            if not wait and (self._writing or self._reading):
                return False
            self._waiting += 1
            while self._writing or self._reading:
                self._cond.wait()
            self._waiting -= 1
            self._writing = True
            return True

    def _leave_writing(self) -> None:
        with self._cond:
            self._writing = False
            self._cond.notify_all()


class Sessions:
    """One worker's copies of the graph sessions in one control-plane database, and the
    connection that hears of every write of them."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._lock = threading.Lock()
        self._held: dict[Key, Held] = {}
        # Each key's count of writes heard of, and every copy's count of channel losses and
        # reconnections: a row read before either moved is not kept (:meth:`put`).
        self._heard: dict[Key, int] = {}
        self._epoch = 0
        self._connected = False
        self._gates: dict[Key, _Gate] = {}
        self._stopping = threading.Event()
        self._thread = threading.Thread(
            target=self._listen, name="provisa-graph-session-listen", daemon=True
        )
        self._thread.start()

    # --- the copies ------------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._connected

    def get(self, key: Key) -> Held | None:
        """The copy of ``key``'s row, while the channel is heard."""
        with self._lock:
            return self._held.get(key) if self._connected else None

    def ticket(self, key: Key) -> tuple[int, int] | None:
        """Taken before ``key``'s row is read: :meth:`put` keeps what was read only if nothing was
        heard of it since. None while the channel is not heard: nothing is kept."""
        with self._lock:
            return (self._epoch, self._heard.get(key, 0)) if self._connected else None

    def put(self, key: Key, ticket: tuple[int, int] | None, held: Held) -> None:
        with self._lock:
            if ticket is not None and ticket == (self._epoch, self._heard.get(key, 0)):
                self._held[key] = held

    def invalidate(self, key: Key) -> None:
        """Drop ``key``'s copy: this worker wrote its row."""
        with self._lock:
            self._heard[key] = self._heard.get(key, 0) + 1
            self._held.pop(key, None)

    def renewed(self, key: Key, held: Held, used_at: datetime) -> None:
        """``held``'s last-use mark renewed in its row; the copy says so if it is still ``held``."""
        with self._lock:
            if self._held.get(key) is held:
                self._held[key] = replace(held, used_at=used_at)

    def _gate(self, key: Key) -> _Gate:
        with self._lock:
            gate = self._gates.get(key)
            if gate is None:
                gate = self._gates[key] = _Gate()
            return gate

    def reading(self, key: Key) -> Any:
        return self._gate(key).reading()

    @contextmanager
    def writing(self, key: Key) -> Generator[None]:
        """A write of ``key``'s row on this worker: after this worker's reads of it, before the
        ones that come after; its copy dropped when it is done."""
        gate = self._gate(key)
        gate._enter_writing(wait=True)
        try:
            yield
        finally:
            self.invalidate(key)
            gate._leave_writing()

    @contextmanager
    def writing_if_free(self, key: Key) -> Generator[bool]:
        """:meth:`writing`, unless this worker is reading or writing ``key`` now: then False and
        nothing is held (a sweep leaves a session in use for its next pass)."""
        gate = self._gate(key)
        if not gate._enter_writing(wait=False):
            yield False
            return
        try:
            yield True
        finally:
            self.invalidate(key)
            gate._leave_writing()

    # --- the channel -----------------------------------------------------------------------

    def _heard_of(self, payload: str) -> None:
        said = json.loads(payload)
        key = (said["s"], said["u"])
        with self._lock:
            self._heard[key] = self._heard.get(key, 0) + 1
            held = self._held.get(key)
            # Drop the copy only when the row was deleted (g is None) or when a generation
            # strictly newer than the copy was written. A notification for an older generation
            # (the race: G0 arrives after the reader already cached G0+1) leaves the fresher
            # copy in place. Generations only increase per principal (REQ-2176), so g < held
            # means this worker already has the more recent state.
            if held is not None and (said["g"] is None or said["g"] > held.generation):
                del self._held[key]

    def _joined(self) -> None:
        with self._lock:
            self._epoch += 1
            self._connected = True

    def _lost(self) -> bool:
        with self._lock:
            was = self._connected
            self._epoch += 1
            self._connected = False
            self._held.clear()
            return was

    def _connect(self) -> Any:
        cargs, cparams = self._engine.dialect.create_connect_args(self._engine.url)
        conn = self._engine.dialect.loaded_dbapi.connect(
            *cargs, **{**cparams, "application_name": APPLICATION_NAME}
        )
        conn.autocommit = True
        conn.execute(f'LISTEN "{CHANNEL}"')
        return conn

    def _listen(self) -> None:
        said = False  # a loss has been logged, and its reconnection not yet
        while not self._stopping.is_set():
            conn = None
            try:
                conn = self._connect()
                self._joined()
                if said:
                    log.info("graph-session channel heard again; this worker keeps copies again")
                    said = False
                while not self._stopping.is_set():
                    for n in conn.notifies(timeout=_BEAT_S):
                        self._heard_of(n.payload)
                    conn.execute("SELECT 1")
            except Exception as exc:  # noqa: BLE001 — the loss is logged, the copies dropped
                self._lost()
                if not said and not self._stopping.is_set():
                    log.warning(
                        "graph-session channel lost (%s); this worker's copies are dropped and "
                        "every statement reads its session's row until it reconnects",
                        exc,
                    )
                    said = True
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001 — a lost connection may refuse its close
                        log.debug("graph-session channel connection did not close", exc_info=True)
            self._stopping.wait(_RECONNECT_S)
        self._lost()

    def stop(self) -> None:
        self._stopping.set()


_registry_lock = threading.Lock()
_registry: dict[Engine, Sessions] = {}


def of(db: Any) -> Sessions:
    """This worker's copies of the graph sessions in ``db``'s control-plane database, heard of on
    one connection however many handles share its engine."""
    assert db.dialect == "postgresql", "graph sessions live in a PostgreSQL control plane"
    engine = db.engine
    with _registry_lock:
        sessions = _registry.get(engine)
        if sessions is None:
            sessions = _registry[engine] = Sessions(engine)
            event.listen(engine, "engine_disposed", _disposed)
        return sessions


def listen(db: Any) -> None:
    """Start this worker hearing of the graph sessions in ``db``'s control-plane database as the
    worker binds it, so no request starts the listening thread (REQ-1882: every stage of a
    request runs on its own thread, and a request starts none). Graph sessions live only in a
    PostgreSQL control plane (REQ-2176): any other holds none to hear of."""
    if db.dialect == "postgresql":
        of(db)


def _disposed(engine: Engine) -> None:
    with _registry_lock:
        sessions = _registry.pop(engine, None)
    if sessions is not None:
        sessions.stop()
