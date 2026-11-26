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


def _bare_type(ch_type: str) -> str:
    """A ClickHouse column type without its Nullable / LowCardinality wrappers."""
    t = ch_type.strip()
    for wrapper in ("Nullable(", "LowCardinality("):
        while t.startswith(wrapper) and t.endswith(")"):
            t = t[len(wrapper) : -1].strip()
    return t


def _encoded_temporals(
    client: Any, sql: str, parameters: dict[str, Any] | None, schema: Any
) -> dict[str, Any]:  # REQ-2194
    """The result columns ClickHouse's Arrow output writes as integers that are temporals, by
    name, each with the Arrow temporal it encodes: ``DateTime`` is written as uint32 epoch
    seconds and ``Date`` as uint16 epoch days (no output setting changes this; ``DateTime64``
    alone arrives as an Arrow timestamp). Which columns those are is the server's own answer
    (``DESCRIBE (query)``), asked only when the result holds a uint32 or uint16 column."""
    import pyarrow as pa

    if not any(pa.types.is_uint32(f.type) or pa.types.is_uint16(f.type) for f in schema):
        return {}
    described = client.query(f"DESCRIBE ({sql})", parameters=parameters)
    found: dict[str, Any] = {}
    for row in described.result_rows:
        name, ch_type = row[0], _bare_type(row[1])
        field = schema.field(name) if name in schema.names else None
        if field is None:
            continue
        if (ch_type == "DateTime" or ch_type.startswith("DateTime(")) and pa.types.is_uint32(
            field.type
        ):
            found[name] = pa.timestamp("s")
        elif ch_type == "Date" and pa.types.is_uint16(field.type):
            found[name] = pa.date32()
    return found


def _as_temporals(data: Any, temporals: dict[str, Any]) -> Any:  # REQ-2194
    """``data`` (a record batch or table) with each column of ``temporals`` as the Arrow temporal
    its integers encode: naive UTC ``timestamp[s]`` from epoch seconds, ``date32`` from epoch
    days."""
    import pyarrow as pa

    if not temporals:
        return data
    columns = list(data.columns)
    fields = list(data.schema)
    for i, field in enumerate(fields):
        target = temporals.get(field.name)
        if target is None:
            continue
        via = pa.int64() if pa.types.is_timestamp(target) else pa.int32()
        columns[i] = columns[i].cast(via).cast(target)
        fields[i] = field.with_type(target)
    schema = pa.schema(fields, metadata=data.schema.metadata)
    if isinstance(data, pa.RecordBatch):
        return pa.RecordBatch.from_arrays(columns, schema=schema)
    return pa.Table.from_arrays(columns, schema=schema)


def _schema_as_temporals(schema: Any, temporals: dict[str, Any]) -> Any:
    import pyarrow as pa

    return pa.schema(
        [f.with_type(temporals[f.name]) if f.name in temporals else f for f in schema],
        metadata=schema.metadata,
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
        ``binary``; a String whose bytes are not UTF-8 is converted to binary (REQ-2194, matches
        the ENGINE path's bytes_where_not_text). ``params`` fill the statement's ``%(name)s``
        placeholders through the driver's own binding, which escapes each value for ClickHouse."""
        # Local import breaks the clickhouse_runtime → clickhouse.py circular import at module
        # level (clickhouse_runtime imports result_names from here).
        import pyarrow as pa

        from provisa.federation.clickhouse_runtime import bytes_where_not_text

        query_id = uuid.uuid4().hex
        with self._require_pool().connection(is_broken=_is_broken) as client:
            with request_deadline.cancel_on_deadline(lambda: self._kill_query(query_id)):
                table: pa.Table = client.query_arrow(
                    sql, parameters=params, use_strings=True, settings={"query_id": query_id}
                )
                temporals = _encoded_temporals(client, sql, params, table.schema)
        batches = [bytes_where_not_text(_as_temporals(b, temporals)) for b in table.to_batches()]
        if batches:
            return pa.Table.from_batches(batches, schema=batches[0].schema)
        return _as_temporals(table, temporals)

    def iter_arrow_batches(self, sql: str) -> Iterator[Any]:
        """Run fully formed ``sql`` and yield its result as Arrow record batches while the
        server streams them (REQ-1915): one block is held at a time. The pooled client is kept
        for the life of the iteration and returned when it ends or is closed. A String column
        whose bytes are not UTF-8 is converted to binary (REQ-2194)."""
        import pyarrow as pa

        # Local import: clickhouse_runtime imports result_names from this module at module level.
        from provisa.federation.clickhouse_runtime import bytes_where_not_text

        query_id = uuid.uuid4().hex
        with self._require_pool().connection(is_broken=_is_broken) as client:
            with client.query_arrow_stream(
                sql, use_strings=True, settings={"query_id": query_id}
            ) as stream:
                temporals = _encoded_temporals(client, sql, None, stream.gen.schema)
                for chunk in stream:
                    if isinstance(chunk, pa.Table):
                        for batch in chunk.to_batches():
                            yield bytes_where_not_text(_as_temporals(batch, temporals))
                    else:
                        yield bytes_where_not_text(_as_temporals(chunk, temporals))

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
        self._temporals: dict[str, Any] = {}
        self.schema: Any = None
        self.description: list[tuple[str]] | None = None

    def execute(self, sql: str, parameters: dict[str, Any] | None, query_id: str) -> None:
        self._context = self._client.query_arrow_stream(
            sql, parameters=parameters, use_strings=True, settings={"query_id": query_id}
        )
        stream = self._context.__enter__()
        # clickhouse-connect's Arrow stream is pyarrow's stream reader over the response
        # (``gen``), which has read the schema message as it opened.
        self._temporals = _encoded_temporals(self._client, sql, parameters, stream.gen.schema)
        self.schema = _schema_as_temporals(stream.gen.schema, self._temporals)
        self.description = [(name,) for name in self.schema.names]
        self._batches = iter(stream)

    def fetch_batch(self) -> Any:  # noqa: ANN401 — a pyarrow RecordBatch, or None once exhausted
        if self._batches is None:
            return None
        batch = next(self._batches, None)
        if batch is None:
            self._batches = None
            return None
        # A String column whose bytes are not UTF-8 is returned as binary (REQ-2194): matches
        # the ENGINE path's bytes_where_not_text applied in run_sync.
        # Local import: clickhouse_runtime imports result_names from clickhouse.py at module level.
        from provisa.federation.clickhouse_runtime import bytes_where_not_text

        return bytes_where_not_text(_as_temporals(batch, self._temporals))

    def close(self) -> None:
        context, self._context = self._context, None
        self._batches = None
        if context is not None:
            context.__exit__(None, None, None)
