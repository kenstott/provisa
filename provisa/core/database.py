# Copyright (c) 2026 Kenneth Stott
# Canary: e03b3787-4d63-421e-bfd5-eef8845e74c5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Control-plane database abstraction backed by SQLAlchemy Core.

This is the ``AdminDatabase`` contract that decouples the Provisa control plane
from any one driver. It wraps ONE shared, synchronous SQLAlchemy :class:`Engine`
per store per worker (REQ-828, amended 2026-09-29 by REQ-1882): every request
runs on its own thread, the engine's pool is thread-safe, and a borrower waits
for a free pooled connection — bounded by the request's remaining budget — when
the pool is exhausted. It exposes an asyncpg-shaped awaitable API so the ~586
existing call sites keep working unchanged:

    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM sources WHERE id = $1", sid)
        await conn.execute("DELETE FROM sources WHERE id = $1", sid)

The ``async def`` methods keep that awaitable call-site contract only; their
bodies run synchronously on the calling request thread. Every blocking statement
is registered with the request deadline (``provisa.core.request_deadline``), whose
watchdog cancels it through the driver when the budget expires.

Two instances back the split control plane (see ``schema_admin`` /
``schema_org``): the **platform control plane** (``admin``) and the **tenant
control plane** (``org``, per-org).

Semantics deliberately mirror asyncpg:

- Positional ``$1``/``$2`` placeholders are translated to SQLAlchemy ``:pN``
  named binds; call sites pass positional args unchanged.
- Statements outside an explicit :meth:`Connection.transaction` are committed
  immediately (asyncpg's default autocommit). Inside ``transaction()`` they are
  grouped and committed/rolled back together; nested blocks use savepoints.
- :meth:`Connection.execute` returns an asyncpg-style status string
  (``"DELETE 1"``, ``"UPDATE 3"``, ``"INSERT 0 1"``) so status parsing at call
  sites (e.g. ``repositories/source.py``) is preserved.
- ``jsonb``/``json`` columns decode to Python objects, and on PostgreSQL a
  ``dict``/``list`` bound to a raw ``$N`` placeholder is sent as JSON — the
  control-plane schema has no array columns, so every such value targets a
  ``jsonb`` column, exactly as the former asyncpg jsonb codec encoded it.

Portability (Tier-2: SQLite >=3.35, MySQL 8) is layered on via
:class:`Capabilities` gating.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import select
import threading
import urllib.parse
import weakref
from collections.abc import Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncGenerator, NamedTuple

import sqlalchemy as sa
from sqlalchemy import Table, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool, QueuePool, SingletonThreadPool, StaticPool

from provisa.core import request_deadline

if TYPE_CHECKING:
    from provisa.core.model_change import ModelPlane

log = logging.getLogger(__name__)

# Bounded wait for a pooled control-plane connection when every one is checked out (REQ-1882: the
# (max+1)th borrower waits, never fails with "pool exhausted"); capped further by the request's
# remaining budget when one is bound.
_POOL_WAIT_S = 30.0


# --------------------------------------------------------------------------- #
# capabilities
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Capabilities:
    """Per-dialect feature flags. PostgreSQL supports everything; Tier-2
    backends gate PG-only features (LISTEN/NOTIFY, advisory locks, arrays,
    append-only RULEs, RETURNING) so callers can branch.

    ``schemas`` is the org-isolation axis. Schema-capable backends (PG/Oracle/
    MySQL) scope an org as a namespace switched on a single connection — see
    ``enter_org_sql``. Non-schema-capable backends (SQLite) carry the org in the
    database *file*, so an org is a distinct engine/connection, not a statement;
    ``enter_org_sql`` returns None and org selection happens when the engine is
    built (file-per-org)."""

    dialect: str
    listen_notify: bool
    advisory_lock: bool
    arrays: bool
    rules: bool
    returning: bool
    schemas: bool
    # SAVEPOINT / nested-transaction support. PostgreSQL poisons the whole transaction
    # when a statement raises, so an INSERT whose unique violation we intend to catch must be
    # isolated in a savepoint. DuckDB's parser has no SAVEPOINT at all, and it does not abort the
    # surrounding transaction on error, so the nested scope is both unsupported and unnecessary.
    savepoints: bool

    def enter_org_sql(self, schema: str) -> str | None:
        """The statement that scopes a connection to ``schema`` (org namespace),
        or None for non-schema-capable backends. Semantics differ per dialect:
        PG search_path, MySQL current database, Oracle current schema."""
        if not self.schemas:
            return None
        if self.dialect == "postgresql":
            return f'SET search_path TO "{schema}"'
        if self.dialect in ("mysql", "mariadb"):
            return f"USE `{schema}`"
        if self.dialect == "oracle":
            return f'ALTER SESSION SET CURRENT_SCHEMA = "{schema}"'
        return None

    @classmethod
    def for_dialect(cls, dialect: str) -> "Capabilities":
        d = dialect.split("+", 1)[0]
        if d == "postgresql":
            return cls(
                d,
                listen_notify=True,
                advisory_lock=True,
                arrays=True,
                rules=True,
                returning=True,
                schemas=True,
                savepoints=True,
            )
        if d == "sqlite":
            return cls(
                d,
                listen_notify=False,
                advisory_lock=False,
                arrays=False,
                rules=False,
                returning=True,
                schemas=False,
                savepoints=True,
            )
        if d in ("mysql", "mariadb"):
            return cls(
                d,
                listen_notify=False,
                advisory_lock=True,
                arrays=False,
                rules=False,
                returning=False,
                schemas=True,
                savepoints=True,
            )
        if d == "duckdb":
            # An embedded DuckDB file used as a materialization store (REQ-989): schema-capable,
            # RETURNING-capable; no LISTEN/NOTIFY or advisory locks (single-process file store).
            return cls(
                d,
                listen_notify=False,
                advisory_lock=False,
                arrays=True,
                rules=False,
                returning=True,
                schemas=True,
                savepoints=False,
            )
        if d == "oracle":
            return cls(
                d,
                listen_notify=False,
                advisory_lock=False,
                arrays=False,
                rules=False,
                returning=True,
                schemas=True,
                savepoints=True,
            )
        return cls(
            d,
            listen_notify=False,
            advisory_lock=False,
            arrays=False,
            rules=False,
            returning=False,
            schemas=False,
            savepoints=False,
        )


# --------------------------------------------------------------------------- #
# row adapter
# --------------------------------------------------------------------------- #
class Row:
    """Wraps a SQLAlchemy ``Row`` to mimic ``asyncpg.Record``: ``row['col']``,
    ``row[0]``, ``dict(row)``, ``.get()``, ``.keys()``, and value-iteration."""

    __slots__ = ("_row", "_mapping")

    def __init__(self, row: Any) -> None:
        self._row = row
        self._mapping = row._mapping

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return self._row[key]
        return self._mapping[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._mapping.get(key, default)

    def keys(self):
        return self._mapping.keys()

    def values(self):
        return self._mapping.values()

    def items(self):
        return self._mapping.items()

    def __iter__(self):
        # asyncpg.Record iterates values; dict(row) uses keys()+__getitem__.
        return iter(self._mapping.values())

    def __contains__(self, key: Any) -> bool:
        return key in self._mapping

    def __len__(self) -> int:
        return len(self._mapping)

    def __repr__(self) -> str:
        return f"Row({dict(self._mapping)!r})"


# --------------------------------------------------------------------------- #
# placeholder translation
# --------------------------------------------------------------------------- #
_PLACEHOLDER = re.compile(r"\$(\d+)")
# A PG cast (``::jsonb``, ``::text[]`` …) applied directly to a placeholder.
# SQLAlchemy's text() bind regex has a ``(?!:)`` lookahead, so it refuses to
# bind ``:pN`` when ``::`` follows. We drop the cast on the placeholder: PG
# infers the param type from the target column/context, and a JSON-wrapped
# dict/list (see _translate) lands in a jsonb column unchanged. Standalone
# casts like ``col::text`` are left untouched.
_CAST_ON_BIND = re.compile(r"(:p\d+)::\w+(?:\[\])?")


def _translate(sql: str, args: tuple, dialect: str = "") -> tuple[str, dict[str, Any]]:
    """Convert asyncpg ``$1``-style SQL + positional args to SQLAlchemy
    ``:pN``-style SQL + a param dict.

    On PostgreSQL a ``dict``/``list`` argument is wrapped as JSON: asyncpg learned each
    placeholder's type from the server and its jsonb codec JSON-encoded these, whereas psycopg
    dumps a list as an ARRAY and cannot dump a dict at all. Every control-plane column such a
    value lands in is ``jsonb`` (the schema has no array columns), so JSONB is the old semantics."""
    if not args:
        return sql, {}
    if dialect == "postgresql":
        from psycopg.types.json import Jsonb

        args = tuple(Jsonb(a) if isinstance(a, (dict, list)) else a for a in args)
    params = {f"p{i + 1}": a for i, a in enumerate(args)}
    sql = _PLACEHOLDER.sub(lambda m: f":p{m.group(1)}", sql)
    # Strip ``::type`` casts on binds — SQLAlchemy text() would misread the ``::``
    # as another bind. Array-typed binds that need a cast (e.g. unnest) must use
    # CAST(:p AS type[]) form instead, which survives this and SQLAlchemy parsing.
    sql = _CAST_ON_BIND.sub(lambda m: m.group(1), sql)
    return sql, params


_DOLLAR_QUOTE = re.compile(r"\$\$.*?\$\$", re.DOTALL)
# A string literal or a quoted identifier, matched in one pass so a quote character of one kind
# inside the other is never read as a delimiter.
_SQL_QUOTED = re.compile(r"""'(?:[^']|'')*'|"(?:[^"]|"")*\"""")
_LINE_COMMENT = re.compile(r"--[^\n]*")


def _is_multi_statement(sql: str) -> bool:
    """True if *sql* contains more than one top-level statement (separated by
    ``;``), ignoring ``$$``-quoted blocks, string literals, quoted identifiers, and line
    comments.

    asyncpg's extended/prepared protocol (what SQLAlchemy ``text()`` uses)
    rejects multiple commands with "cannot insert multiple commands into a
    prepared statement"; such scripts must run on the raw driver connection.
    A single ``DO $$ ... $$`` block is NOT multi-statement."""
    s = _DOLLAR_QUOTE.sub("", sql)
    s = _SQL_QUOTED.sub("", s)
    s = _LINE_COMMENT.sub("", s)
    s = s.strip().rstrip(";").strip()
    return ";" in s


_VERB = re.compile(r"^\s*(\w+)")


def _status(sql: str, rowcount: int) -> str:
    """Synthesize an asyncpg-style command status tag from the verb + rowcount."""
    m = _VERB.match(sql)
    verb = m.group(1).upper() if m else ""
    if verb == "INSERT":
        return f"INSERT 0 {max(rowcount, 0)}"
    if verb in ("UPDATE", "DELETE", "SELECT"):
        return f"{verb} {max(rowcount, 0)}"
    return verb


# --------------------------------------------------------------------------- #
# statement cancel (request deadline)
# --------------------------------------------------------------------------- #
def _mysql_kill(engine: Engine, thread_id: int) -> None:
    """Cancel a MySQL/MariaDB statement by ``KILL QUERY`` from a short-lived side connection.

    Opened directly through the dialect, never borrowed from the pool: the pool may be exhausted
    by the very requests whose statements need cancelling."""
    cargs, cparams = engine.dialect.create_connect_args(engine.url)
    side = engine.dialect.loaded_dbapi.connect(*cargs, **cparams)
    try:
        cur = side.cursor()
        cur.execute(f"KILL QUERY {int(thread_id)}")
        cur.close()
    finally:
        side.close()


def statement_cancel(sc: sa.Connection) -> Callable[[], None]:
    """The driver call that aborts the statement in flight on ``sc`` from another thread."""
    dialect = sc.dialect.name
    dbapi_conn = sc.connection.dbapi_connection
    if dbapi_conn is None:
        raise RuntimeError("control-plane connection has no live DBAPI connection")
    if dialect == "postgresql":
        return dbapi_conn.cancel  # psycopg 3: sends a cancel request on a separate socket
    if dialect in ("sqlite", "duckdb"):
        return dbapi_conn.interrupt
    if dialect == "oracle":
        return dbapi_conn.cancel  # python-oracledb: Connection.cancel() from another thread
    if dialect in ("mysql", "mariadb"):
        engine = sc.engine
        thread_id = dbapi_conn.thread_id()
        return lambda: _mysql_kill(engine, thread_id)
    raise ValueError(f"no statement cancel for control-plane dialect {dialect!r}")


# --------------------------------------------------------------------------- #
# LISTEN/NOTIFY (PostgreSQL) — one listener thread per Database
# --------------------------------------------------------------------------- #
_NotifyCallback = Callable[[Any, int, str, str], None]


class _PgListener:
    """Delivers PostgreSQL NOTIFY payloads to registered callbacks.

    A LISTEN outlives the request that registered it, so it runs on a dedicated daemon thread over
    its own autocommit psycopg 3 connection (opened directly, not from the shared pool, which it
    would otherwise hold forever). The thread waits on the connection socket plus a wake pipe with
    ``select()``, drains what arrived with ``conn.notifies(timeout=0)``, and hands each payload to
    its callback on the event loop that registered it (``call_soon_threadsafe``) — the callbacks
    feed ``asyncio.Queue``s owned by that loop. LISTEN/UNLISTEN are issued on the listener thread
    (the only user of that connection), and ``subscribe`` returns only once the LISTEN is in
    effect."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._lock = threading.Lock()
        self._callbacks: dict[str, list[tuple[_NotifyCallback, Any, Any]]] = {}
        self._commands: list[tuple[str, str, threading.Event, list[BaseException]]] = []
        self._thread: threading.Thread | None = None
        self._wake_r, self._wake_w = os.pipe()
        self._stopping = False
        self._conn: Any = None

    def _connect(self) -> Any:
        cargs, cparams = self._engine.dialect.create_connect_args(self._engine.url)
        conn = self._engine.dialect.loaded_dbapi.connect(*cargs, **cparams)
        conn.autocommit = True
        return conn

    def _ensure_started(self) -> None:
        if self._thread is not None:
            return
        self._conn = self._connect()
        self._thread = threading.Thread(target=self._run, name="provisa-pg-listen", daemon=True)
        self._thread.start()

    def _submit(self, verb: str, channel: str) -> None:
        done = threading.Event()
        errors: list[BaseException] = []
        with self._lock:
            self._ensure_started()
            self._commands.append((verb, channel, done, errors))
        os.write(self._wake_w, b"x")
        if not done.wait(_POOL_WAIT_S):
            raise TimeoutError(f"LISTEN thread did not apply {verb} {channel!r}")
        if errors:
            raise errors[0]

    def subscribe(self, channel: str, callback: _NotifyCallback, owner: Any, loop: Any) -> None:
        with self._lock:
            entries = self._callbacks.setdefault(channel, [])
            first = not entries
            entries.append((callback, owner, loop))
        if first:
            self._submit("LISTEN", channel)

    def unsubscribe(self, channel: str, callback: _NotifyCallback) -> None:
        with self._lock:
            entries = self._callbacks.get(channel, [])
            # Equality, not identity: a bound-method callback is a new object on every access.
            remaining = [e for e in entries if e[0] != callback]
            if len(remaining) == len(entries):
                raise KeyError(f"callback not registered on channel {channel!r}")
            if remaining:
                self._callbacks[channel] = remaining
                return
            del self._callbacks[channel]
        self._submit("UNLISTEN", channel)

    def _apply_commands(self) -> None:
        with self._lock:
            pending, self._commands = self._commands, []
        for verb, channel, done, errors in pending:
            try:
                cur = self._conn.cursor()
                cur.execute(f'{verb} "{channel}"')
                cur.close()
            except BaseException as exc:  # handed to the waiting subscriber, which re-raises it
                errors.append(exc)
            finally:
                done.set()

    def _dispatch(self) -> None:
        # timeout=0: consume what the socket already holds (select() reported it readable) and
        # return, so the loop goes back to waiting on the socket AND the wake pipe.
        for n in self._conn.notifies(timeout=0):
            with self._lock:
                entries = list(self._callbacks.get(n.channel, []))
            for callback, owner, loop in entries:
                try:
                    loop.call_soon_threadsafe(callback, owner, n.pid, n.channel, n.payload)
                except RuntimeError:
                    # The registering loop is closed: its request ended without removing the
                    # listener. Reported and dropped so it is not retried on every notify.
                    log.error(
                        "LISTEN %s: registering loop is closed; dropping its callback", n.channel
                    )
                    with self._lock:
                        self._callbacks[n.channel] = [
                            e for e in self._callbacks.get(n.channel, []) if e[0] != callback
                        ]

    def _run(self) -> None:
        while not self._stopping:
            sock = self._conn.fileno()
            ready, _, _ = select.select([sock, self._wake_r], [], [], 5.0)
            if self._wake_r in ready:
                os.read(self._wake_r, 4096)
                self._apply_commands()
            if sock in ready:
                self._dispatch()

    def close(self) -> None:
        self._stopping = True
        os.write(self._wake_w, b"x")
        if self._thread is not None:
            self._thread.join(timeout=10)
        if self._conn is not None:
            self._conn.close()
        os.close(self._wake_r)
        os.close(self._wake_w)


# --------------------------------------------------------------------------- #
# pool gate — bounded, deadline-aware wait for a pooled connection
# --------------------------------------------------------------------------- #
class _PoolGate:
    """Bounds concurrent checkouts from one engine and makes the extra borrower wait.

    A sized ``QueuePool`` takes a semaphore of ``size + overflow`` slots, waited on for the
    request's remaining budget (SQLAlchemy's own ``pool_timeout`` is shared engine state and cannot
    be set per caller). A single-connection pool (``StaticPool``/``SingletonThreadPool`` — an
    in-memory SQLite/DuckDB store) takes a re-entrant lock instead: that one connection is not safe
    for concurrent use, so request threads use it one at a time, while a nested acquire on the
    thread already holding it proceeds (the pool hands back the same connection). ``NullPool``
    opens a connection per checkout and has nothing to exhaust."""

    def __init__(self, engine: Engine) -> None:
        pool = engine.pool
        self._lock: Any = None
        if isinstance(pool, (StaticPool, SingletonThreadPool)):
            self._lock = threading.RLock()
        elif isinstance(pool, QueuePool):
            self._lock = threading.BoundedSemaphore(pool.size() + max(pool._max_overflow, 0))
        elif not isinstance(pool, NullPool):
            raise TypeError(f"unsupported control-plane pool {type(pool).__name__}")

    @contextmanager
    def slot(self) -> Iterator[None]:
        if self._lock is None:
            yield
            return
        budget = request_deadline.remaining()
        wait = _POOL_WAIT_S if budget is None else min(_POOL_WAIT_S, budget)
        shield = request_deadline.shielded()
        held = False
        try:
            # Taking the slot and giving it back are each a section the request's deadline does
            # not interrupt (REQ-1905): a raise landing after the slot was taken and before this
            # block owned its release would keep it for good.
            with shield.lock:
                shield.settle()
                held = self._lock.acquire(timeout=wait)
            if not held:
                raise TimeoutError(f"no control-plane connection freed within {wait:.1f}s")
            yield
        finally:
            with shield.lock:
                shield.settle()
                if held:
                    self._lock.release()


_GATES: "weakref.WeakKeyDictionary[Engine, _PoolGate]" = weakref.WeakKeyDictionary()
_GATES_LOCK = threading.Lock()


def _gate_for(engine: Engine) -> _PoolGate:
    with _GATES_LOCK:
        gate = _GATES.get(engine)
        if gate is None:
            gate = _GATES[engine] = _PoolGate(engine)
        return gate


@contextmanager
def bounded_connection(engine: Engine, *, begin: bool = False) -> Iterator[sa.Connection]:
    """A pooled connection from a shared sync ``engine``: the checkout waits for a free slot
    bounded by the request's remaining budget, and statements run on it are cancellable by the
    request deadline via :func:`deadline_execute`. ``begin`` wraps it in a transaction committed
    on exit (``engine.begin()`` semantics)."""
    shield = request_deadline.shielded()
    with _gate_for(engine).slot():
        scope: Any = None
        sc: Any = None  # the sa.Connection, once checked out
        failure: BaseException | None = None
        try:
            # REQ-1905: checking the connection out, and ending its transaction and handing it
            # back, are each a section the request's deadline does not interrupt.
            with shield.lock:
                shield.settle()
                scope = engine.begin() if begin else engine.connect()
                sc = scope.__enter__()
            yield sc
        except BaseException as exc:
            failure = exc
            raise
        finally:
            with shield.lock:
                shield.settle()
                if sc is None:
                    pass  # the checkout itself failed: nothing is held
                elif failure is None:
                    scope.__exit__(None, None, None)
                else:
                    # A connection whose statement the deadline's raise cut short is in an
                    # unknown protocol state: invalidated, so the pool closes it, not reuses it.
                    if request_deadline.interrupted(failure):
                        sc.invalidate()
                    scope.__exit__(type(failure), failure, failure.__traceback__)


def _buffered(result: Any) -> Any:
    """Fetch every row of ``result`` now and hand back the same ``CursorResult`` replaying them.

    The driver cursor is drained before the statement's autocommit, as SQLAlchemy's async layer
    buffered every result — SQLite refuses to commit with a statement still in progress, and
    callers read rows after the commit. ``rowcount``/``lastrowid``/``keys()`` are preserved.
    ``_rewind`` is SQLAlchemy's own replay of a fetched rowset onto its result (used for
    ``return_defaults`` + supplemental returning)."""
    if not result.returns_rows:
        return result
    rowcount = result.rowcount
    rows = result.fetchall()
    result._rewind(rows)
    result.__dict__["rowcount"] = rowcount
    return result


def deadline_execute(sc: sa.Connection, stmt: Any, params: Any = None) -> Any:
    """``sc.execute`` registered with the request deadline (when one is bound), which cancels it
    through the driver when the budget expires. The result is fully buffered (see
    :func:`_buffered`)."""
    if request_deadline.current() is None:
        return _buffered(sc.execute(stmt, params))
    with request_deadline.cancel_on_deadline(statement_cancel(sc)):
        return _buffered(sc.execute(stmt, params))


# --------------------------------------------------------------------------- #
# connection
# --------------------------------------------------------------------------- #
def _copy_text(value: Any) -> str:
    """A DBAPI value rendered for a PostgreSQL text/CSV ``COPY`` field."""
    import datetime as _dt

    from psycopg import Binary
    from psycopg.types.json import Json, JsonDumper, Jsonb

    if isinstance(value, Binary):  # a DBAPI Binary wrapper around bytes
        value = value.obj
    if isinstance(value, (Json, Jsonb)):  # the dialect's JSON/JSONB bind processor wraps values
        # psycopg's own text dumper renders the wrapper exactly as it would on the wire.
        return bytes(JsonDumper(Json).dump(value)).decode()  # type: ignore[arg-type]
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(value).hex()
    if isinstance(value, (list, tuple)):
        return "{" + ",".join(_array_elem(v) for v in value) + "}"
    if isinstance(value, dict):
        return json.dumps(value)
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    return str(value)


def _array_elem(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (list, tuple)):
        return _copy_text(value)
    body = _copy_text(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{body}"'


class StoreSideViolation(RuntimeError):
    """A statement on one store's handle touched another store's tables (REQ-1922): the model
    store (``model_db``), an org region's state store (``tenant_db``) and its record
    (``record_db``) are separate databases in a region deployment, and the deployment's own state
    (``platform_state_db``) is no org's, so such a statement could not run there."""

    def __init__(self, handle: str, holds: str, tables: frozenset[str]) -> None:
        from provisa.core.store_sides import HANDLE, PLATFORM_STATE_SIDE, side_of

        def _use(side: str) -> str:
            # The deployment has one platform-state handle; every other side is an org's.
            whose = "the deployment's" if side == PLATFORM_STATE_SIDE else "the org's"
            return f"{whose} {HANDLE[side]}"

        said = "; ".join(
            f"{t} is a {side_of(t)} table, use {_use(side_of(t))}" for t in sorted(tables)
        )
        super().__init__(
            f"{said} (read through the {handle!r} handle, which holds the {holds} store)"
        )


class Connection:
    """asyncpg-shaped wrapper over a synchronous SQLAlchemy :class:`sqlalchemy.Connection`.

    Every method is ``async def`` only to keep the awaitable call-site contract; each body runs
    synchronously on the calling request thread."""

    def __init__(
        self, sc: sa.Connection, caps: Capabilities, database: "Database | None" = None
    ) -> None:
        self._sc = sc
        self.capabilities = caps
        self._tx_depth = 0
        self._cancel: Callable[[], None] | None = None
        # REQ-1524: the Database this connection came from, when it holds an environment's model.
        self._model_db = database if database is not None and database.model is not None else None
        # REQ-1922: the store this connection's handle holds ("model" / "state"), or None.
        self._holds = database.holds if database is not None else None
        self._handle = database.name if database is not None else ""

    def _record_write(self, target: tuple[str, str, str | None] | None, rowcount: int) -> None:
        """REQ-1524: a write that changed a row of the model is recorded for its commit
        (``provisa.core.model_change``). A rowcount the driver does not report (-1) counts as a
        change; 0 does not."""
        if target is None or self._model_db is None or rowcount == 0:
            return
        from provisa.core import model_change

        verb, table, schema = target
        assert self._model_db.model is not None
        model_change.record(self._model_db, self._model_db.model, verb, table, schema)

    def _statement_cancel(self) -> Callable[[], None]:
        if self._cancel is None:
            self._cancel = statement_cancel(self._sc)
        return self._cancel

    @contextmanager
    def _cancellable(self) -> Iterator[None]:
        """Register the statement about to run with the request deadline, if one is bound.
        Outside a request (startup, background work) there is no budget and nothing to cancel.

        A statement that fails OUTSIDE an explicit ``transaction()`` is rolled back at once: the
        asyncpg contract this class keeps is per-statement autocommit, where a caught failure
        (a duplicate-key INSERT a caller treats as its success case) leaves the connection usable.
        Without the rollback PostgreSQL keeps the implicit transaction aborted and refuses every
        later statement on this connection with "current transaction is aborted"."""
        try:
            if request_deadline.current() is None:
                yield
            else:
                with request_deadline.cancel_on_deadline(self._statement_cancel()):
                    yield
        except BaseException:
            if self._tx_depth == 0:
                self._sc.rollback()
            raise

    def _exec(self, stmt: Any, params: Any = None) -> Any:
        if self._holds is not None:
            from provisa.core.store_sides import foreign_tables

            foreign = foreign_tables(self._holds, stmt)
            if foreign:
                raise StoreSideViolation(self._handle, self._holds, foreign)
        with self._cancellable():
            result = _buffered(self._sc.execute(stmt, params))
        if self._model_db is not None:
            from provisa.core.model_change import core_target, raw_target

            target = raw_target(stmt.text) if isinstance(stmt, sa.TextClause) else core_target(stmt)
            rowcount = getattr(result, "rowcount", -1)
            self._record_write(target, -1 if rowcount is None else rowcount)
        return result

    def _run(self, sql: str, args: tuple) -> Any:
        stmt, params = _translate(sql, args, self.capabilities.dialect)
        return self._exec(text(stmt), params)

    def _commit_if_autocommit(self) -> None:
        if self._tx_depth == 0:
            self._sc.commit()

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def execute(self, sql: str, *args: Any) -> str:
        # No-arg DDL scripts (multiple statements) run as one simple-protocol script.
        # Parameterized statements never reach here as multi-statement (they carry args).
        if not args and _is_multi_statement(sql):
            self._execute_script(sql)
            self._commit_if_autocommit()
            return ""
        result = self._run(sql, args)
        rowcount = result.rowcount if result.rowcount is not None else -1
        self._commit_if_autocommit()
        return _status(sql, rowcount)

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def executemany(self, sql: str, args_seq: list) -> None:
        dialect = self.capabilities.dialect
        stmt, _ = _translate(sql, tuple(args_seq[0]) if args_seq else (), dialect)
        param_list = [_translate(sql, tuple(row), dialect)[1] for row in args_seq]
        if param_list:
            self._exec(text(stmt), param_list)
        self._commit_if_autocommit()

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def fetch(self, sql: str, *args: Any) -> list[Row]:
        result = self._run(sql, args)
        rows = [Row(r) for r in result.fetchall()]
        self._commit_if_autocommit()
        return rows

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def fetch_with_columns(self, sql: str, *args: Any) -> tuple[list[str], list[Row]]:
        """Rows plus the result's column names — known even when no row comes back."""
        result = self._run(sql, args)
        columns = list(result.keys())
        rows = [Row(r) for r in result.fetchall()]
        self._commit_if_autocommit()
        return columns, rows

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def fetchrow(self, sql: str, *args: Any) -> Row | None:
        result = self._run(sql, args)
        r = result.fetchone()
        self._commit_if_autocommit()
        return Row(r) if r is not None else None

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def fetchval(self, sql: str, *args: Any, column: int = 0) -> Any:
        result = self._run(sql, args)
        r = result.fetchone()
        self._commit_if_autocommit()
        return r[column] if r is not None else None

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def reflect_columns(self, table: str, schema: str | None = None) -> list[dict]:
        """Portable column reflection via the SQLAlchemy Inspector — replaces raw
        ``information_schema`` queries so it runs on any backend. Returns one dict per
        column: ``{column_name, data_type, is_primary_key}``. ``data_type`` is the
        lowercased SQL type name with array/json collapsed to ``text`` (mirroring the
        engine surface, which sees those columns as text).

        ``schema`` is the org-isolation schema; it is honoured only on schema-capable
        backends (``capabilities.schemas``) and ignored on schema-less ones (SQLite),
        so callers pass the org schema unconditionally and the dialect decision stays
        inside the abstraction."""
        from sqlalchemy import inspect as _sa_inspect

        eff_schema = schema if self.capabilities.schemas else None
        is_sqlite = self.capabilities.dialect == "sqlite"

        # SQLite does not type VIEW columns, so the Inspector reports NullType (→ "null") for the
        # meta-table views. Rather than default such a column to string, analyze the actual data
        # in-line with SQL (``typeof``) and pick the best match; storage classes map cleanly.
        _SQLITE_STORAGE = {"integer": "integer", "real": "double", "text": "text", "blob": "blob"}
        sync_conn = self._sc
        insp = _sa_inspect(sync_conn)
        pk = set(insp.get_pk_constraint(table, schema=eff_schema).get("constrained_columns") or [])
        ref = f'"{table}"' if eff_schema is None else f'"{eff_schema}"."{table}"'

        def _infer_sqlite(col: str) -> str:
            # A column with no non-null sample (empty table / all-null) yields no row — the
            # storage class is genuinely undetermined, so "text" is the neutral class. This is
            # the design-mandated default (REQ-947 design-time typing), not error-swallowing.
            row = sync_conn.exec_driver_sql(
                f'SELECT typeof("{col}") FROM {ref} WHERE "{col}" IS NOT NULL LIMIT 1'
            ).fetchone()
            return _SQLITE_STORAGE.get(row[0], "text") if row else "text"

        cols: list[dict] = []
        for c in insp.get_columns(table, schema=eff_schema):
            type_name = str(c["type"]).split("(")[0].strip().lower()
            if "[]" in type_name or type_name in ("array", "json", "jsonb"):
                type_name = "text"
            elif is_sqlite and type_name in ("null", "nulltype", ""):
                type_name = _infer_sqlite(c["name"])
            cols.append(
                {
                    "column_name": c["name"],
                    "data_type": type_name,
                    "is_primary_key": c["name"] in pk,
                }
            )
        return cols

    # -- advisory locks (dialect-portable; a no-op where the backend has none) --
    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def advisory_xact_lock(self, key: int) -> None:
        """Take a transaction-scoped advisory lock keyed by ``key`` (auto-released at commit).
        A no-op on backends without advisory locks — single-writer file DBs (SQLite) need none."""
        caps = self.capabilities
        if not caps.advisory_lock:
            return
        if caps.dialect == "postgresql":
            await self.execute(f"SELECT pg_advisory_xact_lock({key})")
        elif caps.dialect in ("mysql", "mariadb"):
            await self.execute(f"SELECT GET_LOCK('{key}', -1)")

    @asynccontextmanager
    async def advisory_lock(self, key: int) -> "AsyncGenerator[Connection]":
        """Hold a session advisory lock keyed by ``key`` for the ``with`` block, released on exit.
        A no-op on backends without advisory locks."""
        caps = self.capabilities
        held = caps.advisory_lock and caps.dialect in ("postgresql", "mysql", "mariadb")
        if held:
            take = (
                f"SELECT pg_advisory_lock({key})"
                if caps.dialect == "postgresql"
                else f"SELECT GET_LOCK('{key}', -1)"
            )
            await self.execute(take)
        try:
            yield self
        finally:
            if held:
                release = (
                    f"SELECT pg_advisory_unlock({key})"
                    if caps.dialect == "postgresql"
                    else f"SELECT RELEASE_LOCK('{key}')"
                )
                await self.execute(release)

    # -- portable Core helpers (dialect-agnostic; used by migrated repositories) --
    def _execute_core(self, stmt: Any) -> Any:
        from provisa.core.env_secrets import guard_statement
        from provisa.core.meta_rls import apply_meta_tenant_guard

        result = self._exec(apply_meta_tenant_guard(guard_statement(stmt)))
        self._commit_if_autocommit()
        return result

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def execute_core(self, stmt: Any) -> Any:
        """Execute a SQLAlchemy Core statement (select/insert/update/delete)
        and return the CursorResult. Autocommits outside a transaction.

        REQ-828: every control-plane statement passes through the app-layer meta-RLS guard here —
        the single, un-bypassable seam that enforces tenant isolation store-independently (a no-op
        when no tenant is in scope).

        REQ-1525: and through the credential-literal guard, which refuses a write that would put a
        credential into a carried field. It sits here rather than at the commit because REQ-1524
        forbids a failed commit from failing the change it observes — refusing at commit time would
        leave the secret in the database and the repository permanently behind."""
        return self._execute_core(stmt)

    # Async only to keep the awaitable call-site contract; runs synchronously on the calling thread.
    async def execute_core_many(self, stmt: Any, rows: list[dict[str, Any]]) -> None:
        """Execute a Core INSERT once for every parameter set in ``rows`` (executemany): the
        statement is compiled once whatever the batch size, where ``insert().values([...])``
        compiles every row's binds into the statement. Passes the same guards as
        :meth:`execute_core`. Autocommits outside a transaction; a no-op for an empty batch."""
        if not rows:
            return
        from provisa.core.env_secrets import guard_statement
        from provisa.core.meta_rls import apply_meta_tenant_guard

        self._exec(apply_meta_tenant_guard(guard_statement(stmt)), rows)
        self._commit_if_autocommit()

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def bulk_copy(self, table: Table, rows: list[dict[str, Any]]) -> int:
        """Bulk-ingest ``rows`` into ``table`` via the store's fastest columnar / bulk path (REQ-990).

        The path is chosen from the dialect capability — explicit, never a silent fallback:
        - PostgreSQL: ``COPY ... FROM STDIN`` (CSV) — one statement streaming every row, no
          per-row statement. Each value is first passed through its column type's bind processor
          (JSON columns serialize, arrays stay lists), then rendered as a COPY field.
        - Every other relational backend: a single ``executemany`` Core INSERT (one prepared
          statement, N parameter sets) — still a bulk path, never a per-row loop.

        Rows are normalized to the table's column order; a key absent from a row lands as NULL.
        Returns the number of rows ingested. A no-op for an empty batch."""
        if not rows:
            return 0
        colnames = [c.name for c in table.columns]
        if self.capabilities.dialect == "postgresql":
            dialect = self._sc.dialect
            # The dialect's own implementation of each type — what the statement compiler binds
            # with (generic Numeric's processor would round-trip Decimal through float).
            procs = [c.type.dialect_impl(dialect).bind_processor(dialect) for c in table.columns]
            buf = io.StringIO()
            for r in rows:
                fields: list[str] = []
                for cn, proc in zip(colnames, procs):
                    v = r.get(cn)
                    if v is not None and proc is not None:
                        v = proc(v)
                    # CSV COPY: NULL is an unquoted empty field; every value is quoted, so an
                    # empty string ("") stays distinct from NULL.
                    fields.append("" if v is None else '"' + _copy_text(v).replace('"', '""') + '"')
                buf.write(",".join(fields) + "\n")
            qualified = f'"{table.schema}"."{table.name}"' if table.schema else f'"{table.name}"'
            cols_sql = ", ".join(f'"{cn}"' for cn in colnames)
            copy_sql = f"COPY {qualified} ({cols_sql}) FROM STDIN WITH (FORMAT csv)"
            buf.seek(0)
            dbapi_conn = self._sc.connection.dbapi_connection
            assert dbapi_conn is not None
            cur = dbapi_conn.cursor()
            try:
                with self._cancellable():
                    with cur.copy(copy_sql) as copy:
                        copy.write(buf.getvalue())
            finally:
                cur.close()
            self._commit_if_autocommit()
            return len(rows)
        param_list = [{cn: r.get(cn) for cn in colnames} for r in rows]
        self._exec(table.insert(), param_list)
        self._commit_if_autocommit()
        return len(param_list)

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def upsert(
        self,
        table: Table,
        values: dict[str, Any],
        *,
        index_elements: list[str],
        update_columns: list[str] | None = None,
        set_extra: dict[str, Any] | None = None,
    ) -> None:
        """Upsert a row, dialect-AGNOSTICALLY: UPDATE by the conflict keys, and INSERT if no row
        matched. Uses only generic Core (update/insert/select) — no dialect-specific ON CONFLICT /
        MERGE / ON DUPLICATE KEY, so it works on EVERY SQLAlchemy backend, not an enumerated few.

        ``update_columns`` defaults to all inserted columns except the conflict keys; an empty list
        means DO NOTHING (insert-if-absent). ``set_extra`` adds/overrides set assignments with Core
        expressions (e.g. ``{"version": table.c.version + 1}``)."""
        from sqlalchemy import (
            and_,
            insert as _insert,
            literal,
            select as _select,
            update as _update,
        )
        from sqlalchemy.exc import IntegrityError

        from provisa.core.env_secrets import guard_statement
        from provisa.core.meta_rls import apply_meta_tenant_guard

        cols = (
            update_columns
            if update_columns is not None
            else [c for c in values if c not in index_elements]
        )
        set_map: dict[str, Any] = {c: values[c] for c in cols}
        set_map.update(set_extra or {})
        where = and_(*[table.c[k] == values[k] for k in index_elements])

        if set_map:
            res = self._execute_core(_update(table).where(where).values(**set_map))
            if (res.rowcount or 0) > 0:
                return
        else:
            exists = self._execute_core(_select(literal(1)).select_from(table).where(where))
            if exists.fetchone() is not None:
                return  # DO NOTHING — row already present
        # Isolate the INSERT in a SAVEPOINT: on PostgreSQL a unique-violation aborts the whole
        # surrounding transaction, so without the nested scope the caught IntegrityError would leave
        # the connection in a failed state and the next statement (e.g. upsert_returning's SELECT)
        # would raise InFailedSQLTransaction. begin_nested auto-begins the outer txn if none is
        # active; rolling back the savepoint keeps the connection usable.
        # REQ-1525: this INSERT is executed directly rather than through execute_core (it needs
        # the savepoint), so the credential guard that lives there is applied here too — a seam
        # with one hole in it is not a seam.
        insert_stmt = apply_meta_tenant_guard(guard_statement(_insert(table).values(**values)))
        try:
            if self.capabilities.savepoints:
                with self._sc.begin_nested():
                    self._exec(insert_stmt)
            else:
                # No savepoint support (DuckDB has no SAVEPOINT keyword). Safe because these
                # backends do not abort the surrounding transaction when a statement raises, so
                # the caught IntegrityError leaves the connection usable without a nested scope.
                self._exec(insert_stmt)
        except IntegrityError:
            # Lost an insert race with a concurrent writer — fall back to the update. Only a race
            # leaves a row to update: when the update matches nothing, the INSERT was refused by a
            # constraint (a CHECK, a NOT NULL, a foreign key), and swallowing that turned a schema
            # defect into a silently missing row (REQ-1668: api_sources.type refused 'neo4j').
            if set_map:
                res = self._execute_core(_update(table).where(where).values(**set_map))
                if (res.rowcount or 0) > 0:
                    self._commit_if_autocommit()
                    return
            raise
        self._commit_if_autocommit()

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def upsert_returning(
        self,
        table: Table,
        values: dict[str, Any],
        *,
        index_elements: list[str],
        returning: str,
        update_columns: list[str] | None = None,
        set_extra: dict[str, Any] | None = None,
    ) -> Any:
        """Upsert (see :meth:`upsert`) then return one column of the row (e.g. its id), via a plain
        SELECT on the conflict keys — dialect-agnostic, no RETURNING dependency."""
        from sqlalchemy import and_, select as _select

        await self.upsert(
            table,
            values,
            index_elements=index_elements,
            update_columns=update_columns,
            set_extra=set_extra,
        )
        where = and_(*[table.c[k] == values[k] for k in index_elements])
        res = self._execute_core(_select(table.c[returning]).where(where))
        row = res.fetchone()
        return row[0] if row is not None else None

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def insert_returning(self, table: Table, values: dict[str, Any], returning: str) -> Any:
        """INSERT and return one generated column value, portably.

        Uses ``RETURNING`` where supported (PostgreSQL, SQLite >=3.35); falls
        back to ``lastrowid`` on MySQL 8, which lacks RETURNING."""
        from sqlalchemy import insert as _insert

        if self.capabilities.returning:
            stmt = _insert(table).values(**values).returning(table.c[returning])
            result = self._execute_core(stmt)
            row = result.fetchone()
            return row[0] if row is not None else None
        result = self._execute_core(_insert(table).values(**values))
        return result.lastrowid

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        """Group statements; commit on success, roll back on exception.

        Mirrors ``asyncpg.Connection.transaction``: the outermost block is a
        real transaction; nested blocks use savepoints.
        """
        if self._tx_depth == 0:
            self._tx_depth += 1
            try:
                yield
                self._sc.commit()
            except BaseException:
                self._sc.rollback()
                raise
            finally:
                self._tx_depth -= 1
        else:
            self._tx_depth += 1
            sp = self._sc.begin_nested()
            try:
                yield
                sp.commit()
            except BaseException:
                sp.rollback()
                raise
            finally:
                self._tx_depth -= 1

    def _execute_script(self, sql: str) -> None:
        if self.capabilities.dialect == "postgresql":
            # psycopg with no parameters and prepare=False sends the script on the simple query
            # protocol, which accepts multiple statements and ``DO $$`` blocks in one call (a
            # prepared statement cannot hold more than one).
            dbapi_conn = self._sc.connection.dbapi_connection
            assert dbapi_conn is not None
            # The raw cursor bypasses SQLAlchemy's transaction tracking, so outside an explicit
            # transaction() the script's own DBAPI transaction is committed or rolled back here:
            # Connection.commit()/rollback() would be no-ops for work SQLAlchemy never saw begin,
            # leaving a failed script's transaction aborted (poisoning the next statement) or a
            # successful one uncommitted (lost on pool checkin).
            cur: Any = (
                dbapi_conn.cursor()
            )  # a psycopg cursor (the DBAPI protocol type lacks prepare=)
            try:
                with self._cancellable():
                    cur.execute(sql, prepare=False)
            except BaseException:
                if self._tx_depth == 0:
                    dbapi_conn.rollback()
                raise
            finally:
                cur.close()
            if self._tx_depth == 0:
                dbapi_conn.commit()
            for statement in sql.split(";"):
                self._record_script(statement.strip())
            return
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                with self._cancellable():
                    self._sc.exec_driver_sql(stmt)
                self._record_script(stmt)

    def _record_script(self, sql: str) -> None:
        """REQ-1524: a script's write to the model is recorded like any other statement's."""
        if self._model_db is None:
            return
        from provisa.core.model_change import raw_target

        self._record_write(raw_target(sql), -1)

    # Async only to keep the awaitable call-site contract; runs synchronously on the request thread.
    async def execute_script(self, sql: str) -> None:
        """Run a multi-statement SQL script (DDL bootstrap).

        PostgreSQL: the whole script goes to the driver unparameterized, on the simple query
        protocol, which allows multiple statements and ``DO $$`` blocks.

        Other dialects: split into individual statements and run each through the
        SQLAlchemy connection via ``exec_driver_sql`` (dialect-agnostic — no driver-
        specific method). The non-PG scripts we emit are plain DDL (e.g. meta-view
        ``DROP``/``CREATE``) with no procedural blocks or embedded statement separators;
        tables come from ``metadata.create_all``, not this path."""
        self._execute_script(sql)


# --------------------------------------------------------------------------- #
# database
# --------------------------------------------------------------------------- #
class Database:
    """A control-plane database handle backed by one shared, synchronous SQLAlchemy Engine.

    The engine's pool is thread-safe and shared by every request thread (REQ-1882); ``acquire``
    waits for a free pooled connection, bounded by the request's remaining budget.

    ``search_path`` scopes every acquired connection to the org namespace on
    schema-capable backends, preserving the isolation the former asyncpg pool
    provided via its ``setup`` callback. The scoping statement is dialect-
    dispatched (``Capabilities.enter_org_sql``): PG search_path, MySQL current
    database, Oracle current schema. Non-schema-capable backends (SQLite) carry
    the org in the file, so this is a no-op there — org = which engine.
    """

    def __init__(
        self,
        engine: Engine,
        name: str,
        search_path: str | None = None,
        model: "ModelPlane | None" = None,
        holds: str | None = None,
    ) -> None:
        self._engine = engine
        self.name = name
        self.search_path = search_path
        # REQ-1922: which store this handle holds — "model" (the org's model, shared across its
        # regions) or "state" (an org region's operating state) — and so which tables it refuses
        # (provisa/core/store_sides.py). None for a handle that is neither (the platform plane).
        if holds is not None:
            from provisa.core.store_sides import TABLES as _SIDES

            if holds not in _SIDES:
                raise ValueError(f"a database handle holds one of {sorted(_SIDES)}, not {holds!r}")
        self.holds = holds
        # REQ-1524: the environment whose model this handle holds; its writes are committed.
        self.model = model
        self.dialect = engine.dialect.name
        self.capabilities = Capabilities.for_dialect(self.dialect)
        self._listener: _PgListener | None = None
        self._listener_lock = threading.Lock()

    @property
    def engine(self) -> Engine:
        """The shared synchronous engine."""
        return self._engine

    def _get_listener(self) -> _PgListener:
        with self._listener_lock:
            if self._listener is None:
                self._listener = _PgListener(self._engine)
            return self._listener

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[Connection]:
        shield = request_deadline.shielded()
        with bounded_connection(self._engine) as sc:
            try:
                # Inside the try: once the org's search_path is set on the connection, the
                # reset below runs whatever ends the block, a request timeout included.
                if self.search_path and (sql := self.capabilities.enter_org_sql(self.search_path)):
                    sc.execute(text(sql))
                    sc.commit()
                yield Connection(sc, self.capabilities, self)
            except BaseException:
                # A statement that failed inside the block (a duplicate-key INSERT a caller
                # catches as its success case) leaves a PostgreSQL transaction aborted, and an
                # aborted transaction refuses every later statement -- the RESET below
                # included, which then surfaces as "current transaction is aborted" in place of
                # the caller's own error. Roll it back first; the caller's exception still
                # propagates.
                if self.dialect == "postgresql" and not sc.invalidated:
                    sc.rollback()
                raise
            finally:
                # PG session state (search_path) survives pool checkin — SQLAlchemy's
                # reset_on_return only rolls back an open transaction, and callers that
                # scope a connection with a raw "SET search_path" (e.g. per-acquire org
                # scoping) commit it. Without this, the next checkout of this pooled
                # connection inherits the wrong schema regardless of its own Database's
                # search_path setting. Reset unconditionally so every acquire starts the
                # role's default search_path, matching the guarantee this class documents.
                # A release section (REQ-1905): the request's deadline does not interrupt it.
                # An invalidated connection (its statement was cut short) is closed, not reset.
                with shield.lock:
                    shield.settle()
                    if self.dialect == "postgresql" and not sc.invalidated:
                        sc.execute(text("RESET search_path"))
                        sc.commit()

    # -- PG-only LISTEN/NOTIFY: served by the listener thread's own connection, so a listener
    #    (an SSE stream, the event-trigger manager) never holds a pooled connection. --
    def _pg_listener(self) -> _PgListener:
        if not self.capabilities.listen_notify:
            raise NotImplementedError(
                f"LISTEN/NOTIFY is not available on the {self.dialect} control plane"
            )
        return self._get_listener()

    # Async only to keep the awaitable call-site contract; the LISTEN runs on the listener thread.
    async def add_listener(self, channel: str, callback: _NotifyCallback) -> None:
        """Deliver NOTIFYs on ``channel`` to ``callback(db, pid, channel, payload)``, called on
        the running event loop (asyncpg's callback signature and delivery point)."""
        import asyncio

        self._pg_listener().subscribe(channel, callback, self, asyncio.get_running_loop())

    # Async only to keep the awaitable call-site contract; the UNLISTEN runs on the listener thread.
    async def remove_listener(self, channel: str, callback: _NotifyCallback) -> None:
        self._pg_listener().unsubscribe(channel, callback)

    # Pool-style passthrough (asyncpg pools proxy connection methods). Used by
    # the few call sites that call db.execute(...) / db.fetch(...) directly.
    async def execute(self, sql: str, *args: Any) -> str:
        async with self.acquire() as conn:
            return await conn.execute(sql, *args)

    async def fetch(self, sql: str, *args: Any) -> list[Row]:
        async with self.acquire() as conn:
            return await conn.fetch(sql, *args)

    async def fetchrow(self, sql: str, *args: Any) -> Row | None:
        async with self.acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def fetchval(self, sql: str, *args: Any, column: int = 0) -> Any:
        async with self.acquire() as conn:
            return await conn.fetchval(sql, *args, column=column)

    def get_size(self) -> int:
        """Current pool size (checked-out + idle connections).

        Returns -1 when the engine uses a pool that doesn't track connection
        counts (NullPool/StaticPool — e.g. some SQLite configs), which has no
        meaningful size.
        """
        pool = self._engine.pool
        if isinstance(pool, QueuePool):
            return pool.checkedout() + pool.checkedin()
        return -1

    def get_idle_size(self) -> int:
        """Idle (checked-in) connections, or -1 for a non-sized pool (see get_size)."""
        pool = self._engine.pool
        if isinstance(pool, QueuePool):
            return pool.checkedin()
        return -1

    # Async only to keep the awaitable call-site contract; disposes the pool synchronously.
    async def close(self) -> None:
        with self._listener_lock:
            listener, self._listener = self._listener, None
        if listener is not None:
            listener.close()
        self._engine.dispose()


# --------------------------------------------------------------------------- #
# factory
# --------------------------------------------------------------------------- #
def build_url(
    dialect: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> str:
    """Build a SQLAlchemy URL for the control plane (mirrors ingest/engine.py::_build_url)."""
    if not dialect:
        dialect = "postgresql+psycopg"
    if not host:
        host = "localhost"
    if not port:
        port = 5432
    pw = urllib.parse.quote_plus(password or "")
    return f"{dialect}://{username}:{pw}@{host}:{port}/{database}"


def create_engine(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    pool_size: int = 5,
    pool_min: int = 0,
    dialect: str = "postgresql+psycopg",
) -> Engine:
    """Create the shared control-plane Engine. ``max_overflow`` is derived from
    ``pool_size - pool_min``."""
    url = build_url(dialect, host, port, database, user, password)
    return create_engine_from_url(
        url, pool_size=pool_size, max_overflow=max(pool_size - pool_min, 0)
    )


# Control-plane store backends selectable by SQLAlchemy URI (REQ-828). The value is the sync
# driver each backend runs on (REQ-1882: one shared, thread-safe engine per worker); an embedded
# engine (sqlite/duckdb) gives the desktop deployment model zero external infra, Postgres backs
# production — one abstraction, same schema.
_ADMIN_DRIVER: dict[str, str] = {
    "postgresql": "psycopg",
    "sqlite": "pysqlite",
    "duckdb": "provisa",
    "mysql": "pymysql",
    "mariadb": "pymysql",
}


def sync_store_url(url: str) -> str:
    """Resolve a relational store URI (control plane or materialization store) to the sync driver
    Provisa runs it on, failing loud on an unsupported or misconfigured backend (REQ-828 — no
    silent fallback to a default store).

    A bare backend (``duckdb://…``) is pinned to the supported sync driver; the sync driver itself
    passes through; any other driver (including the async drivers used before REQ-1882) or an
    unknown backend is rejected."""
    from sqlalchemy import make_url
    from sqlalchemy.exc import ArgumentError

    try:
        parsed = make_url(url)
    except ArgumentError as exc:
        raise ValueError(f"invalid store URI {url!r}: {exc}") from exc

    backend = parsed.get_backend_name()
    # ``drivername`` is the raw ``backend[+driver]`` token; ``get_driver_name()`` would
    # substitute the dialect's default driver, hiding that none was requested.
    driver = parsed.drivername.split("+", 1)[1] if "+" in parsed.drivername else ""
    if backend not in _ADMIN_DRIVER:
        raise ValueError(
            f"unsupported store backend {backend!r} in URI {url!r}; "
            f"supported: {', '.join(sorted(_ADMIN_DRIVER))}"
        )
    sync_driver = _ADMIN_DRIVER[backend]
    if driver not in ("", sync_driver):
        raise ValueError(
            f"store {backend!r} runs on {backend}+{sync_driver}, "
            f"got {backend}+{driver} in URI {url!r}"
        )
    # ``str(url)``/``URL.__str__`` renders with the password masked (``***``) — the right
    # default for logging, wrong here since this string becomes the actual connect URI.
    return parsed.set(drivername=f"{backend}+{sync_driver}").render_as_string(hide_password=False)


def _pool_kwargs_for(url: str) -> dict[str, Any]:
    """SQLAlchemy pool kwargs for a single-writer file store (DuckDB/SQLite) vs a server
    backend, for :func:`sync_engine_from_url`'s single-writer callers. DuckDB/SQLite are
    single-writer file stores: a one-connection pool serializes writes so concurrent callers
    can't corrupt the single writable handle. Server backends get ``pool_pre_ping`` instead, to
    guard against stale/dropped connections."""
    from sqlalchemy import make_url

    if make_url(url).get_backend_name() in ("duckdb", "sqlite"):
        return {"poolclass": QueuePool, "pool_size": 1, "max_overflow": 0}
    return {"pool_pre_ping": True}


def sync_engine_from_url(url: str, *, pool_pre_ping: bool = True) -> Engine:
    """A **sync** SQLAlchemy engine for single-writer sync callers (e.g. otlp2sql, whose
    inserts run on a worker of its own single-loop process) — applies
    :func:`_pool_kwargs_for`'s single-writer guard."""
    kwargs: dict[str, Any] = {"future": True, **_pool_kwargs_for(url)}
    if "pool_pre_ping" in kwargs:
        kwargs["pool_pre_ping"] = pool_pre_ping
    return sa.create_engine(url, **kwargs)


def create_engine_from_url(
    url: str,
    *,
    pool_size: int = 5,
    max_overflow: int = 5,
) -> Engine:
    """Create the shared control-plane Engine from a SQLAlchemy URI (REQ-828, REQ-1882).

    The platform and tenant control planes are each configured by an independent
    SQLAlchemy URI (``postgresql://…``, ``sqlite:///…``, ``duckdb:///…``, ``mysql://…``), so
    neither is tied to PostgreSQL. The URI selects the backend; an embedded engine
    (SQLite/DuckDB) runs the store with zero external infra on a developer desktop, Postgres in
    production — same schema, same behavior. An unsupported/misconfigured URI fails loud (no
    default store).

    The engine is synchronous and shared by every request thread: its ``QueuePool`` is
    thread-safe, and :class:`Database` bounds each checkout wait by the request's budget. An
    in-memory store (sqlite/duckdb ``:memory:``) exists only inside one connection, so it gets a
    ``StaticPool`` — one connection, which ``Database`` hands to one request thread at a time.
    """
    from sqlalchemy import make_url

    parsed, use_pgbouncer = _pgbouncer_flag(make_url(sync_store_url(url)))
    normalized = parsed.render_as_string(hide_password=False)
    backend = parsed.get_backend_name()
    if backend == "duckdb":
        # Registers the duckdb+provisa control-plane dialect before the engine is built.
        import provisa.core.duckdb_store  # noqa: F401

    kwargs: dict[str, Any] = {"pool_pre_ping": True}
    if backend == "sqlite":
        # The pooled sqlite3 connection is handed between request threads (one at a time).
        kwargs["connect_args"] = {"check_same_thread": False}
    if parsed.database in (None, "", ":memory:"):
        kwargs["poolclass"] = StaticPool
    else:
        kwargs["pool_size"] = pool_size
        kwargs["max_overflow"] = max_overflow
        kwargs["pool_timeout"] = _POOL_WAIT_S
    if backend == "postgresql":
        return pg_engine(normalized, use_pgbouncer=use_pgbouncer, **kwargs)
    engine = sa.create_engine(normalized, **kwargs)
    if backend == "sqlite":
        event.listen(engine, "connect", _on_sqlite_connect)
    return engine


def pg_engine(url: str, *, use_pgbouncer: bool, **kwargs: Any) -> Engine:
    """A SQLAlchemy engine on ``postgresql+psycopg`` with Provisa's prepared-statement policy — the
    one place it is set, for the control plane and the ingest write engines alike.

    ``prepare_threshold=0``: each pooled connection prepares a statement server-side on its first
    execution and reuses that plan on every later one (the asyncpg pool's statement cache did the
    same); the cache is bounded by :data:`_PREPARED_MAX`. Behind PgBouncer in transaction mode a
    session-level prepared statement does not survive the per-transaction server binding, so
    preparing is off. The choice is recorded on the engine (:func:`pg_uses_pgbouncer`) so an engine
    that mirrors this one's database inherits it."""
    connect_args = {
        **kwargs.pop("connect_args", {}),
        "prepare_threshold": None if use_pgbouncer else 0,
    }
    engine = sa.create_engine(
        url,
        connect_args=connect_args,
        execution_options={_PGBOUNCER_OPTION: use_pgbouncer},
        **kwargs,
    )
    event.listen(engine, "connect", _on_pg_connect)
    return engine


def pg_uses_pgbouncer(engine: Engine) -> bool:
    """Whether *engine* (built by :func:`pg_engine`) reaches PostgreSQL through PgBouncer."""
    return engine.get_execution_options()[_PGBOUNCER_OPTION]


_PGBOUNCER_OPTION = "provisa_use_pgbouncer"


# asyncpg's default per-connection statement cache size, which the psycopg pool replaced.
_PREPARED_MAX = 100


def _pgbouncer_flag(url: Any) -> tuple[Any, bool]:
    """Strip Provisa's ``use_pgbouncer=true|false`` query flag off a store URL (libpq does not know
    it) and return it: the URL names PgBouncer's endpoint when the store sits behind one, which only
    the operator knows. Any other value, or the flag on a non-PostgreSQL store, fails loud."""
    raw = url.query.get("use_pgbouncer")
    if raw is None:
        return url, False
    if raw not in ("true", "false"):
        raise ValueError(f"use_pgbouncer must be 'true' or 'false', got {raw!r} in store URI")
    if url.get_backend_name() != "postgresql":
        raise ValueError("use_pgbouncer applies only to a postgresql store URI")
    return url.difference_update_query(["use_pgbouncer"]), raw == "true"


def _on_pg_connect(dbapi_conn: Any, connection_record: Any) -> None:
    """SQLAlchemy ``connect`` listener: bound each connection's prepared-statement cache (LRU;
    evicted statements are DEALLOCATEd). psycopg 3's own loaders already return the types the former
    asyncpg pool returned — uuid as ``uuid.UUID``, bytea as ``bytes``, json/jsonb as Python
    objects."""
    del connection_record
    dbapi_conn.prepared_max = _PREPARED_MAX


def _on_sqlite_connect(dbapi_conn: Any, connection_record: Any) -> None:
    """SQLAlchemy ``connect`` listener: put the control-plane SQLite file in WAL mode.

    WAL is what lets the app's own readers run alongside its writer: rollback-journal mode (the
    SQLite default) blocks readers during a write commit and yields transient ``database is
    locked``; WAL lets one writer proceed alongside concurrent readers. ``busy_timeout`` absorbs
    the brief checkpoint/commit windows. journal_mode=WAL persists in the file; the pragma is
    idempotent. A no-op on an in-memory DB (which cannot be WAL).

    WAL does NOT extend that guarantee to the DuckDB federation engine: DuckDB's sqlite extension
    corrupts a file another connection is writing and dies with SIGBUS regardless of journal mode,
    so the engine attaches a snapshot copy of this file rather than the file itself (see
    federation.duckdb_runtime.DuckDBFederationRuntime._refresh_control_plane_snapshot)."""
    del connection_record
    cur = dbapi_conn.cursor()
    try:
        cur.execute("PRAGMA journal_mode=WAL")
        # 30s, not sqlite3's 5s: a table registration holds the writer through a full schema
        # rebuild and MV activation, and an admin mutation arriving meanwhile (the e2e lane's
        # registerFact) failed with "database is locked" rather than waiting it out.
        cur.execute("PRAGMA busy_timeout=30000")
    finally:
        cur.close()


# --------------------------------------------------------------------------- #
# org router — multi-tenant on not-schema-capable backends
# --------------------------------------------------------------------------- #
class OrgRouter:
    """Maps ``org_id`` -> :class:`Database`, one engine/file per org.

    The multi-tenant mechanism for **not-schema-capable** backends (SQLite,
    DuckDB), where an org cannot be a namespace switched on a shared connection
    (``Capabilities.schemas`` is False) and instead lives in its own database
    file. Schema-capable backends (PG/Oracle/MySQL) do NOT need this — a single
    shared :class:`Database` scopes orgs via ``Capabilities.enter_org_sql`` — so
    construct a router only when ``Capabilities.for_dialect(...).schemas`` is
    False. Engines are built lazily and cached, so each org keeps one pool."""

    def __init__(self, base_url: str, *, pool_size: int = 5, max_overflow: int = 5) -> None:
        from sqlalchemy import make_url

        self._base_url = make_url(base_url)
        if Capabilities.for_dialect(self._base_url.get_dialect().name).schemas:
            raise ValueError(
                "OrgRouter is for not-schema-capable backends (file-per-org); "
                f"{self._base_url.get_backend_name()} scopes orgs on a shared engine — "
                "use a single Database with Capabilities.enter_org_sql instead."
            )
        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self._cache: dict[str, Database] = {}

    def _org_url(self, org_id: str) -> str:
        """Per-org file URL: a sibling of the base file named ``org_<id>.db``."""
        from pathlib import PurePosixPath

        base_db = self._base_url.database
        if not base_db:
            raise ValueError(f"base URL has no database path: {self._base_url!r}")
        parent = PurePosixPath(base_db).parent
        org_path = str(parent / f"org_{org_id}{PurePosixPath(base_db).suffix or '.db'}")
        return str(self._base_url.set(database=org_path))

    def database_for(self, org_id: str) -> "Database":
        from provisa.core.db import _validate_org_id

        _validate_org_id(org_id)
        if org_id not in self._cache:
            engine = create_engine_from_url(
                self._org_url(org_id), pool_size=self._pool_size, max_overflow=self._max_overflow
            )
            self._cache[org_id] = Database(engine, name=f"org_{org_id}")
        return self._cache[org_id]

    async def close(self) -> None:
        for db in self._cache.values():
            await db.close()
        self._cache.clear()


class OrgStores(NamedTuple):
    """An org's three control-plane handles in this region (REQ-1919, REQ-1920, REQ-1922)."""

    model_db: "Database"  # its model, shared by every region it selects
    # This region's operating state and its request record. None only between building a runtime
    # in a region deployment and loading the model that names their stores (region_stores.py).
    tenant_db: "Database | None"
    record_db: "Database | None"


def org_store_handles(
    search_path: str,
    model: "ModelPlane",
    *,
    model_engine: Engine,
    state_engine: Engine,
    record_engine: Engine,
) -> OrgStores:
    """An org's handles, each over the engine of the store that keeps its side: the MODEL store
    (its writes committed to the environment's model, REQ-1524), the region's STATE store and its
    RECORD. Each refuses the others' tables (``provisa/core/store_sides.py``)."""
    return OrgStores(
        Database(
            model_engine, name="org-model", search_path=search_path, model=model, holds="model"
        ),
        Database(state_engine, name="org-state", search_path=search_path, holds="state"),
        Database(record_engine, name="org-record", search_path=search_path, holds="record"),
    )
