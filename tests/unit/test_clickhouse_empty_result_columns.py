# Copyright (c) 2026 Kenneth Stott
# Canary: deec73e8-1292-444e-9c1f-07ea60af3c6a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A ClickHouse answer with no rows can carry no block at all, and so no column names
(clickhouse-connect returns QueryResult([]) for it). A SELECT with no rows is still a result with
columns, and pgwire otherwise answered it as a command (the client's "didn't produce records").

The DIRECT driver reads Arrow (REQ-2194), whose schema names the columns with no row; the engine
runtime, which reads rows, names them from ClickHouse's own DESCRIBE of the query."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pyarrow as pa

from provisa.executor.drivers.clickhouse import ClickHouseDriver, _ArrowStreamCursor


class _Result:
    def __init__(self, rows, cols):
        self.result_rows = rows
        self.column_names = tuple(cols)


class _ArrowStream:
    """clickhouse-connect's Arrow StreamContext: ``gen`` is pyarrow's stream reader."""

    def __init__(self, schema, batches):
        self.gen = SimpleNamespace(schema=schema)
        self._batches = batches

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __iter__(self):
        return iter(self._batches)


class _Client:
    """Answers a query with no row: as Arrow with its schema, and DESCRIBE with its columns."""

    server_version = "26.8.46.6"

    def __init__(self, described):
        self.described = described
        self.queries: list[tuple[str, object]] = []

    def _schema(self):
        return pa.schema([(n, pa.string()) for n, _t in self.described])

    def query(self, sql, parameters=None, settings=None):
        self.queries.append((sql, parameters))
        if sql.startswith("DESCRIBE ("):
            return _Result([(n, t, "", "", "", "", "") for n, t in self.described], [])
        return _Result([], [])

    def query_arrow(self, sql, parameters=None, settings=None, use_strings=None):
        self.queries.append((sql, parameters))
        return self._schema().empty_table()

    def query_arrow_stream(self, sql, parameters=None, settings=None, use_strings=None):
        self.queries.append((sql, parameters))
        return _ArrowStream(self._schema(), [])

    def close(self):
        pass


SQL = "SELECT event_type FROM perf_bench.order_events WHERE customer_id = %(p1)s LIMIT 1"


def test_a_stream_with_no_batch_names_the_statements_columns():
    client = _Client([("event_type", "String")])
    cur = _ArrowStreamCursor(client)
    cur.execute(SQL, {"p1": 44}, "q1")
    assert cur.description == [("event_type",)]
    assert cur.fetch_batch() is None
    assert client.queries == [(SQL, {"p1": 44})]  # nothing more is asked


def test_a_query_with_no_rows_names_the_statements_columns():
    """A whole result is read as Arrow (REQ-2194), and ClickHouse's Arrow answer of a SELECT
    that matches no row still carries the statement's schema: its columns are named from it,
    with no DESCRIBE."""
    import pyarrow as pa
    from clickhouse_connect.driver.query import QueryContext

    from provisa.core.sync_pool import BlockingPool

    class _Arrow(_Client):
        def create_query_context(self, query=None, parameters=None, **_kw):
            return QueryContext(query=query or "", parameters=parameters)

        def query_arrow(self, sql, parameters=None, use_strings=None, settings=None):
            self.queries.append((sql, parameters))
            return pa.schema([("event_type", pa.string()), ("order_id", pa.uint64())]).empty_table()

    client = _Arrow([])
    driver = ClickHouseDriver()
    driver._pool = BlockingPool(
        lambda: client, lambda c: c.close(), minsize=1, maxsize=1, wait_s=5.0, name="ch:test"
    )
    result = asyncio.run(driver.execute("SELECT event_type, order_id FROM t WHERE 1 = 0"))
    assert result.rows == []
    assert result.column_names == ["event_type", "order_id"]
    assert [sql for sql, _ in client.queries] == ["SELECT event_type, order_id FROM t WHERE 1 = 0"]


def test_the_engine_runtimes_query_with_no_block_names_the_statements_columns(monkeypatch):
    import clickhouse_connect

    from provisa.federation.clickhouse_runtime import _ServerBackend

    client = _Client([("event_type", "String")])
    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **_k: client)
    backend = _ServerBackend(host="ch", port=8123, username="default", password="")
    rows, names = backend.query(
        "SELECT event_type FROM t WHERE customer_id = {p1:Int64}", {"p1": 44}
    )
    assert rows == []
    assert names == ["event_type"]
    assert client.queries[-1] == (
        "DESCRIBE (SELECT event_type FROM t WHERE customer_id = {p1:Int64})",
        {"p1": 44},
    )
