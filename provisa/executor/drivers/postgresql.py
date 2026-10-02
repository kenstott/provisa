# Copyright (c) 2026 Kenneth Stott
# Canary: 7b73f4d0-6a4a-481e-aa89-9e177a9c8afa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL direct driver over one shared, thread-safe psycopg 3 pool per source.

Supports direct PG connections and PgBouncer (transaction pool mode).

Prepared statements: psycopg 3 speaks the extended protocol, so each pooled connection prepares a
statement server-side on its first execution (``prepare_threshold=0``) and reuses the plan for every
later execution of the same SQL, up to ``prepared_max`` statements per connection (LRU; evicted
ones are DEALLOCATEd). This matches what the asyncpg driver did before (its per-connection
statement cache prepared on first use, 100 entries). A PgBouncer'd source never prepares
(``prepare_threshold=None``): a session-level prepared statement does not survive PgBouncer's
per-transaction server binding — the same reason the asyncpg driver ran it with
``statement_cache_size=0``. It also cannot stream (a server-side cursor outlives that binding).

Connections run in autocommit: a read is one round trip (no BEGIN/COMMIT around it), as it was on
asyncpg. A stream opens an explicit transaction for its server-side cursor's life.

REQ-1882 (amended 2026-09-29): every request runs on its own thread, so the pool is a shared
resource used by many request threads; a borrower waits (bounded by the request's remaining budget)
when all connections are out, and every blocking statement is cancelled via ``conn.cancel()`` when
the request's deadline fires.
"""

# Requirements: REQ-052, REQ-053, REQ-068, REQ-550, REQ-1882

from __future__ import annotations

import re
import threading
import weakref
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

import psycopg
from psycopg.abc import QueryNoTemplate
from psycopg.rows import dict_row
from psycopg.types.string import TextLoader
from psycopg_pool import ConnectionPool

from provisa.core import request_deadline
from provisa.executor.drivers.base import DirectDriver, DirectResultStream
from provisa.executor.result import QueryResult

if TYPE_CHECKING:
    from provisa.pgwire.pg_passthrough import BorrowedPgConnection

_PLACEHOLDER = re.compile(r"\$(\d+)")

# asyncpg's default per-connection statement cache size, which this driver replaced.
_PREPARED_MAX = 100


def _exec_args(sql: str, params: list | None) -> tuple[str, dict[str, Any] | None]:
    """Rewrite PG-native ``$N`` placeholders to psycopg's ``%(pN)s``.

    psycopg sends them to the server as ``$N`` bound parameters (never interpolated); with params
    present it %-parses the statement, so a literal ``%`` (``LIKE 'a%'``) is escaped first. A named
    dict binds a repeated ``$N`` to the same value."""
    if not params:
        return sql, None
    converted = _PLACEHOLDER.sub(lambda m: f"%(p{m.group(1)})s", sql.replace("%", "%%"))
    return converted, {f"p{i + 1}": v for i, v in enumerate(params)}


def _configure(conn: psycopg.Connection[Any]) -> None:
    """Match the value types the asyncpg driver returned: json/jsonb as text (uuid as UUID, inet as
    ipaddress objects, bytea as bytes and numeric as Decimal are psycopg's own defaults), and bound
    the per-connection prepared-statement cache."""
    conn.adapters.register_loader("json", TextLoader)
    conn.adapters.register_loader("jsonb", TextLoader)
    conn.prepared_max = _PREPARED_MAX


def _q(sql: str) -> QueryNoTemplate:
    """psycopg types its query parameter as a LiteralString to catch string-built SQL; this SQL is
    the governed pipeline's output with its values bound as parameters, not interpolated."""
    return cast(QueryNoTemplate, sql)


def _wait_s(default: float) -> float:
    """The pool wait: its own bound, or less when the request has less budget left."""
    budget = request_deadline.remaining()
    return default if budget is None else min(default, budget)


# A trailing ``LIMIT n [OFFSET m]`` — the last clause of the outermost query, so it bounds the whole
# result (a subquery's LIMIT is followed by its closing parenthesis, never by the end of the text).
_TRAILING_LIMIT = re.compile(
    r"\bLIMIT\s+(?:\$(\d+)|(\d+))(?:\s+OFFSET\s+(?:\$\d+|\d+))?\s*;?\s*\Z", re.IGNORECASE
)
# The one server-side cursor a pooled connection holds at a time. A fixed name, so DECLARE / FETCH
# / CLOSE are the same text for every stream on the connection and are prepared once.
_CURSOR = '"provisa_direct"'
# Rows converted per fetch of a materialized result, between two looks at the request's deadline.
_FETCH_CHUNK_ROWS = 50_000


def _row_bound(sql: str, params: list) -> int | None:
    """The most rows ``sql`` can return, when its own text says so: its trailing LIMIT — a literal,
    or a bound parameter whose value is here. None when the statement states no bound."""
    match = _TRAILING_LIMIT.search(sql)
    if match is None:
        return None
    if match.group(2) is not None:
        return int(match.group(2))
    index = int(match.group(1)) - 1
    if 0 <= index < len(params) and isinstance(params[index], int):
        return params[index]
    return None


class _PgDirectStream(DirectResultStream):  # REQ-1190
    """A DIRECT read handed back in bounded batches.

    A statement whose own trailing LIMIT fits one stream batch (``_STREAM_BATCH_ROWS``, the most
    any consumer pulls at once) is BOUNDED: a cursor would only add a transaction and four more
    statements around a result that is one batch anyway, so it is executed directly — the prepared
    statement the pooled connection already holds, one Bind/Execute — and its connection goes
    straight back to the pool.

    Anything else keeps a server-side cursor inside a transaction held for the stream's life and
    fetched in bounded batches, so a large DIRECT scan never materializes (streaming-uniformity
    Defect 1). The cursor has one fixed name per connection: BEGIN / DECLARE / FETCH / CLOSE /
    COMMIT are then the same text for every stream of the same statement, and the connection's
    prepared-statement cache applies to them as to any other statement. Closing commits the
    read-only transaction and returns the connection to the pool."""

    def __init__(self, driver: PostgreSQLDriver, sql: str, params: list) -> None:
        self._driver = driver
        self._sql = sql
        self._params = params
        self._conn: psycopg.Connection[Any] | None = None
        self._first: list[tuple] | None = None
        self.column_names = []
        self.column_types = None

    def _open(self, first_batch: int) -> None:
        from provisa.federation.runtime_support import _STREAM_BATCH_ROWS

        bound = _row_bound(self._sql, self._params)
        if bound is not None and bound <= _STREAM_BATCH_ROWS:
            self._open_bounded()
        else:
            self._open_cursor(first_batch)

    def _open_bounded(self) -> None:
        sql, args = _exec_args(self._sql, self._params)
        with self._driver._borrow() as conn:
            with conn.cursor() as cur:
                with request_deadline.cancel_on_deadline(conn.cancel):
                    cur.execute(_q(sql), args)
                    self._first = [tuple(r) for r in cur.fetchall()] if cur.description else []
                desc = cur.description or []
                self.column_names = [d.name for d in desc]
                self.column_types = self._driver._type_names(conn, [d.type_code for d in desc])

    def _open_cursor(self, first_batch: int) -> None:
        pool = self._driver._require_pool()
        shield = request_deadline.shielded()
        conn: psycopg.Connection[Any] | None = None
        try:
            with shield.lock:  # taken inside the shield too: see BlockingPool.connection
                shield.settle()
                conn = pool.getconn(timeout=_wait_s(self._driver._ACQUIRE_TIMEOUT))
            # A server-side cursor lives inside a transaction; the connection is autocommit, so the
            # transaction is opened explicitly and committed in close().
            conn.execute(_q("BEGIN"))
            sql, args = _exec_args(
                f"DECLARE {_CURSOR} NO SCROLL CURSOR FOR {self._sql}", self._params
            )
            with request_deadline.cancel_on_deadline(conn.cancel):
                conn.execute(_q(sql), args)
                cur = conn.execute(_q(f"FETCH FORWARD {int(first_batch)} FROM {_CURSOR}"))
                self._first = [tuple(r) for r in cur.fetchall()]
            desc = cur.description or []
            self.column_names = [d.name for d in desc]
            self.column_types = self._driver._type_names(conn, [d.type_code for d in desc])
        except BaseException as exc:
            # putconn rolls back an open transaction and discards a broken connection. A release
            # section: the request's deadline does not interrupt it (REQ-1905).
            with shield.lock:
                shield.settle()
                if conn is not None:
                    if request_deadline.interrupted(exc):
                        conn.close()
                    pool.putconn(conn)
            raise
        self._conn = conn

    def __del__(self) -> None:
        # A stream dropped without close() — its request ended between open and whatever would
        # have closed it — still gives its connection back: putconn rolls the cursor's
        # transaction back.
        conn, self._conn = self._conn, None
        if conn is not None:
            self._driver._require_pool().putconn(conn)

    # Async only for the DirectResultStream awaitable contract; fetches synchronously in-thread.
    async def fetch(self, size: int) -> list[tuple]:
        if self._first is not None:
            first, self._first = self._first, None
            if first:
                return first
        if self._conn is None:
            return []  # a bounded read: its one batch has been handed over
        with request_deadline.cancel_on_deadline(self._conn.cancel):
            cur = self._conn.execute(_q(f"FETCH FORWARD {int(size)} FROM {_CURSOR}"))
            return [tuple(r) for r in cur.fetchall()]

    # Async only for the DirectResultStream awaitable contract; releases synchronously in-thread.
    async def close(self) -> None:
        self._first = None
        if self._conn is None:
            return
        conn, self._conn = self._conn, None
        pool = self._driver._require_pool()
        shield = request_deadline.shielded()
        # The whole close is a release section (REQ-1905): the request's deadline does not
        # interrupt it, so the cursor's transaction ends and the connection goes back.
        with shield.lock:
            shield.settle()
            try:
                conn.execute(_q(f"CLOSE {_CURSOR}"))
                conn.execute(_q("COMMIT"))
            finally:
                # A failed close leaves the transaction open or the connection broken: putconn
                # rolls it back or discards it; the close error still propagates.
                pool.putconn(conn)


class _RowFetcher:
    """The ``await conn.fetch(sql)`` surface ``fetch_enum_registry`` reads through."""

    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self._conn = conn

    # Async only for fetch_enum_registry's awaitable contract; runs synchronously in-thread.
    async def fetch(self, sql: str) -> list[dict[str, Any]]:
        with self._conn.cursor(row_factory=dict_row) as cur:
            with request_deadline.cancel_on_deadline(self._conn.cancel):
                cur.execute(_q(sql))
            return list(cur.fetchall())


class PostgreSQLDriver(DirectDriver):  # REQ-052, REQ-053, REQ-068, REQ-550
    # Bounded wait for a pooled connection when all are checked out (request deadline permitting).
    _ACQUIRE_TIMEOUT = 10.0
    # Rows pulled when a stream opens (so its column metadata is known up front); later batches use
    # the caller's fetch size.
    _FIRST_BATCH_ROWS = 1000

    def __init__(self, use_pgbouncer: bool = False) -> None:
        self._pool: ConnectionPool[psycopg.Connection[Any]] | None = None
        self._use_pgbouncer = use_pgbouncer
        self._connect_kwargs: dict[str, Any] = {}
        self._typnames: dict[int, str] = {}
        self._typnames_lock = threading.Lock()
        # What the raw passthrough has prepared on each pooled connection (REQ-1863), kept for as
        # long as the connection object lives.
        self._raw_statements: weakref.WeakKeyDictionary[Any, dict[str, Any]] = (
            weakref.WeakKeyDictionary()
        )
        self._raw_statements_lock = threading.Lock()

    def _conn_kwargs(self) -> dict[str, Any]:
        """psycopg connect() kwargs for a pooled connection."""
        kw = dict(self._connect_kwargs)
        kw["dbname"] = kw.pop("database")
        kw["autocommit"] = True
        # 0: prepare on the first execution and reuse from the second (asyncpg's behavior).
        # PgBouncer transaction mode cannot keep a session-level prepared statement: never.
        kw["prepare_threshold"] = None if self._use_pgbouncer else 0
        return kw

    # Async only for the DirectDriver awaitable contract; connects synchronously in-thread.
    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:  # REQ-052, REQ-053
        self._connect_kwargs = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }
        min_size = max(min_pool, 1)
        pool: ConnectionPool[psycopg.Connection[Any]] = ConnectionPool(
            kwargs=self._conn_kwargs(),
            min_size=min_size,
            max_size=max(max_pool, min_size),
            configure=_configure,
            timeout=self._ACQUIRE_TIMEOUT,
            name=f"postgresql:{host}:{port}/{database}",
            open=True,
        )
        try:
            # min_pool connections open now, so an unreachable source fails at registration.
            pool.wait(timeout=self._ACQUIRE_TIMEOUT)
        except BaseException:
            pool.close()
            raise
        self._pool = pool

    def _require_pool(self) -> ConnectionPool[psycopg.Connection[Any]]:
        if self._pool is None:
            raise RuntimeError("PostgreSQLDriver is not connected")
        return self._pool

    @contextmanager
    def _borrow(self) -> Generator[psycopg.Connection[Any]]:
        """A pooled connection, waiting at most the request's remaining budget for one.

        Returning it is a release section (REQ-1905): the request's deadline does not interrupt
        it. ``putconn`` rolls back an open transaction and discards a connection that is not
        idle, so one whose statement the deadline's raise cut short is closed, not pooled."""
        pool = self._require_pool()
        shield = request_deadline.shielded()
        conn: psycopg.Connection[Any] | None = None
        failure: BaseException | None = None
        try:
            with shield.lock:  # taken inside the shield too: see BlockingPool.connection
                shield.settle()
                conn = pool.getconn(timeout=_wait_s(self._ACQUIRE_TIMEOUT))
            yield conn
        except BaseException as exc:
            failure = exc
            raise
        finally:
            with shield.lock:
                shield.settle()
                if conn is not None:
                    if failure is not None and request_deadline.interrupted(failure):
                        conn.close()  # putconn drops a closed connection; the pool opens another
                    pool.putconn(conn)

    def borrow_raw(self) -> BorrowedPgConnection:
        """One pooled connection for a raw-DataRow passthrough (REQ-1863): the passthrough drives
        the extended-query exchange on the connection's own socket and hands it back idle."""

        pool = self._require_pool()
        shield = request_deadline.shielded()
        with shield.lock:  # until the caller holds the release (the returned object)
            shield.settle()
            return self._borrow_raw(pool)

    def _borrow_raw(self, pool: "ConnectionPool[psycopg.Connection[Any]]") -> BorrowedPgConnection:
        from provisa.pgwire.pg_passthrough import BorrowedPgConnection

        conn = pool.getconn(timeout=_wait_s(self._ACQUIRE_TIMEOUT))

        def _release(discard: bool) -> None:
            shield = request_deadline.shielded()
            with shield.lock:  # a release section: not interrupted by the request's deadline
                shield.settle()
                if discard:
                    conn.close()  # putconn drops a closed connection and the pool opens a new one
                pool.putconn(conn)

        with self._raw_statements_lock:
            statements = self._raw_statements.setdefault(conn, {})
        return BorrowedPgConnection(
            fileno=conn.pgconn.socket,
            ssl_in_use=bool(conn.pgconn.ssl_in_use),
            cancel=conn.cancel,
            release=_release,
            statements=statements,
        )

    def _type_names(self, conn: psycopg.Connection[Any], oids: list[int]) -> list[str]:
        """REQ-883: the source's real PG result-column type names (pg_type.typname) so downstream
        binary encoders tag each field with the OID the catalog advertised."""
        with self._typnames_lock:
            missing = [o for o in set(oids) if o not in self._typnames]
        if missing:
            with conn.cursor() as cur:
                cur.execute("SELECT oid::int, typname FROM pg_type WHERE oid = ANY(%s)", (missing,))
                found = {int(oid): name for oid, name in cur.fetchall()}
            unknown = set(missing) - set(found)
            if unknown:
                raise RuntimeError(f"pg_type has no entry for result column OID(s) {unknown}")
            with self._typnames_lock:
                self._typnames.update(found)
        with self._typnames_lock:
            return [self._typnames[o] for o in oids]

    # Async only for the DirectDriver awaitable contract; executes synchronously in-thread.
    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        exec_sql, args = _exec_args(sql, params)
        with self._borrow() as conn:
            with conn.cursor() as cur:
                with request_deadline.cancel_on_deadline(conn.cancel):
                    cur.execute(_q(exec_sql), args)
                    rows: list[tuple] = []
                    if cur.description:
                        # In chunks, not one fetchall(): converting a large result is a single
                        # C call the request's deadline cannot end until it returns (REQ-1905,
                        # measured at 0.9-3.5 s for 6M rows). Between chunks it can.
                        while chunk := cur.fetchmany(_FETCH_CHUNK_ROWS):
                            rows.extend(tuple(r) for r in chunk)
                            request_deadline.check()
                desc = cur.description or []
                columns = [d.name for d in desc]
                col_types = self._type_names(conn, [d.type_code for d in desc])
                # A statement with no result set (a data write without RETURNING) reports how
                # many rows it changed; the driver's count is the only place that is known.
                affected = cur.rowcount if cur.description is None and cur.rowcount >= 0 else None
        return QueryResult(
            rows=rows, column_names=columns, column_types=col_types, rowcount=affected
        )

    @property
    def supports_streaming(self) -> bool:  # REQ-1190
        # PgBouncer transaction-pool mode forbids the long-lived server-side cursor a stream needs, so
        # only a direct connection streams; a pgbouncer'd source materializes via execute().
        return not self._use_pgbouncer

    # Async only for the DirectDriver awaitable contract; opens synchronously in-thread.
    async def open_stream(
        self, sql: str, params: list | None = None
    ) -> _PgDirectStream:  # REQ-1190
        stream = _PgDirectStream(self, sql, list(params or []))
        stream._open(self._FIRST_BATCH_ROWS)
        return stream

    # Async only for the DirectDriver awaitable contract; executes synchronously in-thread.
    async def execute_ddl(self, sql: str) -> None:
        # Connections are autocommit, so DDL that cannot run in a transaction (CREATE INDEX
        # CONCURRENTLY, ...) runs as-is.
        with self._borrow() as conn:
            with conn.cursor() as cur:
                with request_deadline.cancel_on_deadline(conn.cancel):
                    cur.execute(_q(sql))

    # Async only for fetch_enum_registry's awaitable contract; queries synchronously in-thread.
    async def fetch_enums(self) -> dict[str, list[str]]:  # REQ-636
        from provisa.compiler.enum_detect import fetch_enum_registry

        with self._borrow() as conn:
            return await fetch_enum_registry(_RowFetcher(conn))

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.close()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
