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
``federation_hints`` flag. Reads use ClickHouse's native columnar output (REQ-986).

One clickhouse-connect client is one ClickHouse SESSION, and a session runs one query at a time:
a second query on it is refused ("Attempt to execute concurrent queries within the same
session"). Every request runs on its own thread (REQ-1882), so the driver holds a bounded pool of
clients — one per checkout, used by one request at a time — like every other direct driver, and
runs the query on the request's own thread.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

from provisa.core import request_deadline
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


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
            return clickhouse_connect.get_client(
                host=host,
                port=port or (8443 if secure else 8123),
                username=user or "default",
                password=password or "",
                database=database or "default",
                secure=secure,
            )

        self._open = _open
        # min_pool clients open now, so an unreachable source fails at registration.
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

        query_id = uuid.uuid4().hex
        with self._require_pool().connection(is_broken=_is_broken) as client:
            with request_deadline.cancel_on_deadline(lambda: self._kill_query(query_id)):
                res = client.query(exec_sql, parameters=ch_params, settings={"query_id": query_id})
        return QueryResult(
            rows=[tuple(r) for r in res.result_rows], column_names=list(res.column_names)
        )

    # Async only for the awaitable call-site contract; executes synchronously in-thread.
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

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.closeall()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
