# Copyright (c) 2026 Kenneth Stott
# Canary: 16f749bb-b448-49a1-ba7b-6f679817be81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse direct source driver (REQ-986).

Makes a ClickHouse server a first-class NAMED SOURCE reachable on ANY engine: Provisa reads it
directly (HTTP via clickhouse-connect) then lands a replica. The same client family the ClickHouse
federation engine uses. Port defaults to the HTTP interface (8123); ``secure`` (TLS) is an optional
``federation_hints`` flag. Reads use ClickHouse's native columnar output (REQ-986): every result is
read as Arrow (``query_arrow``, ``query_arrow_stream``), never as clickhouse-connect's Python rows
(REQ-2194). Arrow output needs ClickHouse 26 or later -- earlier servers refuse UUID in it -- so a
client that reaches an older server is refused as it opens (:class:`ClickHouseServerTooOld`).

One clickhouse-connect client is one ClickHouse SESSION, and a session runs one query at a time:
a second query on it is refused ("Attempt to execute concurrent queries within the same
session"). Every request runs on its own thread (REQ-1882), so the driver holds a bounded pool of
clients — one per checkout, used by one request at a time — like every other direct driver, and
runs the query on the request's own thread.

The clients name NO session at all (``autogenerate_session_id=False``). One client per request
keeps two requests off one session, but not the next request off the last one's: the server
releases a session a moment after the client has read its answer, and a pooled client handed
straight to the next request was refused in that moment ("Session ... is locked by a concurrent
client", SESSION_IS_LOCKED -- seen on the engine's client, #189). Nothing here keeps state in a
session (no temporary table, no SET).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator, Sequence
from typing import Any

from provisa.core import request_deadline
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.arrow_result import arrow_table_rows, source_arrow_result
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult

#: The oldest ClickHouse major whose Arrow output holds every column type (REQ-2194): earlier
#: servers refuse UUID in Arrow output.
MINIMUM_SERVER_MAJOR = 26


class ClickHouseServerTooOld(RuntimeError):  # REQ-2194
    """A ClickHouse source's server is older than :data:`MINIMUM_SERVER_MAJOR`: its results are
    read as Arrow, which an older server cannot write for every type."""

    def __init__(self, version: str) -> None:
        self.version = version
        self.minimum = MINIMUM_SERVER_MAJOR
        super().__init__(
            f"ClickHouse server {version} is older than {MINIMUM_SERVER_MAJOR}: a ClickHouse "
            f"source is read as Arrow, which needs ClickHouse {MINIMUM_SERVER_MAJOR} or later "
            "(earlier servers refuse UUID in Arrow output)"
        )


def require_arrow_server(client: Any) -> None:  # noqa: ANN401 — a clickhouse-connect client
    """Refuse ``client`` by name when its server is older than :data:`MINIMUM_SERVER_MAJOR`.
    clickhouse-connect reads the server's version as the client opens (``server_version``)."""
    version: str = client.server_version
    if int(version.split(".", 1)[0]) < MINIMUM_SERVER_MAJOR:
        raise ClickHouseServerTooOld(version)


def _is_broken(exc: BaseException) -> bool:
    """A failure after which the client's connection cannot be trusted: it is closed, not pooled."""
    from clickhouse_connect.driver.exceptions import InterfaceError, OperationalError

    return isinstance(exc, (OperationalError, InterfaceError))


class ClickHouseDriver(DirectDriver):  # REQ-986
    # Bounded wait for a pooled client when all are checked out (request deadline permitting).
    _ACQUIRE_TIMEOUT = 10.0

    def __init__(self) -> None:
        self._pool: BlockingPool[Any] | None = None
        self._open: Callable[[], Any] | None = None
        self._extra: dict[str, str] = {}

    def configure(self, extra: dict[str, str]) -> None:
        """Optional ``secure`` (TLS 'true'/'false') from federation_hints."""
        self._extra = dict(extra)

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
    ) -> None:
        import clickhouse_connect

        secure = self._extra.get("secure", "").lower() in ("1", "true", "yes")

        def _open() -> Any:
            client = clickhouse_connect.get_client(
                host=host,
                port=port or (8443 if secure else 8123),
                username=user or "default",
                password=password or "",
                database=database or "default",
                secure=secure,
                # No server session (see the module note): a pooled client is taken by the next
                # request the moment the last one returns it.
                autogenerate_session_id=False,
            )
            try:
                require_arrow_server(client)
            except ClickHouseServerTooOld:
                client.close()
                raise
            return client

        self._open = _open
        # min_pool clients open now, so an unreachable source, or a server too old to write
        # Arrow, fails at registration.
        self._pool = BlockingPool(
            _open,
            lambda c: c.close(),
            minsize=max(min_pool, 1),
            maxsize=max_pool,
            wait_s=self._ACQUIRE_TIMEOUT,
            name=f"clickhouse:{host}:{port}/{database}",
        )

    def _require_pool(self) -> BlockingPool[Any]:
        if self._pool is None:
            raise RuntimeError("ClickHouseDriver is not connected")
        return self._pool

    def _kill_query(self, query_id: str) -> None:
        """Cancel the query ``query_id`` from a second client (the first is blocked in it)."""
        assert self._open is not None
        killer = self._open()
        try:
            # query_id is this driver's own uuid4 hex, never caller text.
            killer.command(f"KILL QUERY WHERE query_id = '{query_id}' ASYNC")
        finally:
            killer.close()

    # Async only for the DirectDriver awaitable contract; executes synchronously in-thread.
    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # Confirmed live (federated_join via GraphQL against a real ClickHouse server): the
        # governed pipeline's `@N` positional placeholders reach here UNSUBSTITUTED for a
        # cross-engine push-down, the same shape trino.py/trino_flight.py already handle for
        # Trino — "arrives fully formed" only held for the single-source path this driver was
        # first written against. `@N` -> ClickHouse's own `%(pN)s` dict-substitution placeholder
        # (clickhouse_connect.driver.binding.finalize_query does real value escaping, not raw
        # string interpolation); see substitute_positional_placeholders for why the reverse-order
        # replacement is centralized but the placeholder spelling isn't.
        from provisa.compiler.params import (
            extract_params_comment,
            substitute_positional_placeholders,
        )

        exec_sql, embedded = extract_params_comment(sql)
        effective_params = params if params is not None else embedded
        ch_params: dict[str, Any] | None = None
        if effective_params:
            exec_sql = substitute_positional_placeholders(
                exec_sql, effective_params, lambda i: f"%(p{i})s"
            )
            ch_params = {f"p{i}": v for i, v in enumerate(effective_params, start=1)}

        # REQ-2194: the server's Arrow output, handed on as the table it reads as. Arrow names a
        # result's columns even when it has no row.
        return source_arrow_result(await self.execute_arrow(exec_sql, ch_params))

    # Async only for the awaitable call-site contract; executes synchronously in-thread.
    @property
    def supports_streaming(self) -> bool:  # REQ-1190
        return True

    async def open_stream(self, sql: str, params: list | None = None) -> Any:  # REQ-1190
        """The result a batch at a time: ClickHouse's Arrow stream (REQ-2194), read batch by batch
        as a fetch asks, on a pooled client; ``KILL QUERY`` by the statement's id stops one still
        sending batches."""
        from provisa.compiler.params import (
            extract_params_comment,
            substitute_positional_placeholders,
        )
        from provisa.executor.drivers.cursor_stream import PooledArrowCursorStream

        exec_sql, embedded = extract_params_comment(sql)
        effective_params = params if params is not None else embedded
        ch_params: dict[str, Any] | None = None
        if effective_params:
            exec_sql = substitute_positional_placeholders(
                exec_sql, effective_params, lambda i: f"%(p{i})s"
            )
            ch_params = {f"p{i}": v for i, v in enumerate(effective_params, start=1)}
        query_id = uuid.uuid4().hex
        return PooledArrowCursorStream(
            self._require_pool(),
            open_cursor=_ArrowStreamCursor,
            execute=lambda cur: cur.execute(exec_sql, ch_params, query_id),
            columns=lambda cur: list(cur.schema.names),
            cancel=lambda _client, _cur: self._kill_query(query_id),
            to_rows=arrow_table_rows,
        )

    async def execute_arrow(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        """Run ``sql`` and return the result as a ``pyarrow.Table`` in ClickHouse's native Arrow
        format -- no per-row Python objects (REQ-1865: a keyed row-materialize fetch lands
        millions of rows columnar). ClickHouse ``String`` arrives as Arrow ``string``, not
        ``binary``. ``params`` fill the statement's ``%(name)s`` placeholders through the
        driver's own binding, which escapes each value for ClickHouse."""
        query_id = uuid.uuid4().hex
        with self._require_pool().connection(is_broken=_is_broken) as client:
            with request_deadline.cancel_on_deadline(lambda: self._kill_query(query_id)):
                return client.query_arrow(
                    sql, parameters=params, use_strings=True, settings={"query_id": query_id}
                )

    def iter_arrow_batches(self, sql: str) -> Iterator[Any]:
        """Run fully formed ``sql`` and yield its result as Arrow record batches while the
        server streams them (REQ-1915): one block is held at a time. The pooled client is kept
        for the life of the iteration and returned when it ends or is closed."""
        import pyarrow as pa

        query_id = uuid.uuid4().hex
        with self._require_pool().connection(is_broken=_is_broken) as client:
            with client.query_arrow_stream(
                sql, use_strings=True, settings={"query_id": query_id}
            ) as stream:
                for chunk in stream:
                    if isinstance(chunk, pa.Table):
                        yield from chunk.to_batches()
                    else:
                        yield chunk

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.closeall()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None


def result_names(
    client: Any, sql: str, parameters: dict[str, Any] | None, answered: Sequence[str]
) -> list[str]:
    """The statement's result columns. ClickHouse can answer a query that matches no row with no
    block at all, and clickhouse-connect then reports no column names; a SELECT with no rows still
    has its columns, so they are read from ClickHouse's own ``DESCRIBE (query)``."""
    if answered:
        return list(answered)
    described = client.query(f"DESCRIBE ({sql})", parameters=parameters)
    return [row[0] for row in described.result_rows]


class _ArrowStreamCursor:
    """A ClickHouse Arrow stream read for :class:`PooledArrowCursorStream` (REQ-2194):
    ``schema`` is the stream's Arrow schema, and ``fetch_batch`` hands on the server's next record
    batch, taken from the server only when it is asked for; never clickhouse-connect's rows."""

    def __init__(self, client: Any) -> None:  # noqa: ANN401 — a clickhouse-connect client
        self._client = client
        self._context: Any = None
        self._batches: Iterator[Any] | None = None
        self.schema: Any = None
        self.description: list[tuple[str]] | None = None

    def execute(self, sql: str, parameters: dict[str, Any] | None, query_id: str) -> None:
        self._context = self._client.query_arrow_stream(
            sql, parameters=parameters, use_strings=True, settings={"query_id": query_id}
        )
        stream = self._context.__enter__()
        # clickhouse-connect's Arrow stream is pyarrow's stream reader over the response
        # (``gen``), which has read the schema message as it opened.
        self.schema = stream.gen.schema
        self.description = [(name,) for name in self.schema.names]
        self._batches = iter(stream)

    def fetch_batch(self) -> Any:  # noqa: ANN401 — a pyarrow RecordBatch, or None once exhausted
        if self._batches is None:
            return None
        batch = next(self._batches, None)
        if batch is None:
            self._batches = None
        return batch

    def close(self) -> None:
        context, self._context = self._context, None
        self._batches = None
        if context is not None:
            context.__exit__(None, None, None)
