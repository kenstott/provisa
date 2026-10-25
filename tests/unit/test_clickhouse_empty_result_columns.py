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
(clickhouse-connect returns QueryResult([]) for it). The driver still names the statement's
columns, from ClickHouse's own DESCRIBE of the query: a SELECT with no rows is a result with
columns, and pgwire otherwise answered it as a command (the client's "didn't produce records")."""

from __future__ import annotations

import asyncio

from provisa.executor.drivers.clickhouse import ClickHouseDriver, _BlockStreamCursor


class _Result:
    def __init__(self, rows, cols):
        self.result_rows = rows
        self.column_names = tuple(cols)


class _Stream:
    def __init__(self, blocks, cols):
        self._blocks = blocks
        self.source = _Result([], cols)

    def __iter__(self):
        return iter(self._blocks)


class _StreamContext:
    def __init__(self, stream):
        self._stream = stream

    def __enter__(self):
        return self._stream

    def __exit__(self, *exc):
        return False


class _Client:
    """Answers a query with no block at all, and DESCRIBE with the query's columns."""

    def __init__(self, described):
        self.described = described
        self.queries: list[tuple[str, object]] = []

    def query(self, sql, parameters=None, settings=None):
        self.queries.append((sql, parameters))
        if sql.startswith("DESCRIBE ("):
            return _Result([(n, t, "", "", "", "", "") for n, t in self.described], [])
        return _Result([], [])

    def query_row_block_stream(self, sql, parameters=None, settings=None):
        self.queries.append((sql, parameters))
        return _StreamContext(_Stream([], []))

    def close(self):
        pass


SQL = "SELECT event_type FROM perf_bench.order_events WHERE customer_id = %(p1)s LIMIT 1"


def test_a_stream_with_no_block_names_the_statements_columns():
    client = _Client([("event_type", "String")])
    cur = _BlockStreamCursor(client)
    cur.execute(SQL, {"p1": 44}, "q1")
    assert cur.description == [("event_type",)]
    assert cur.fetchmany(10) == []
    assert client.queries[-1] == (f"DESCRIBE ({SQL})", {"p1": 44})


def test_a_stream_with_a_header_asks_nothing_more():
    class _Header(_Client):
        def query_row_block_stream(self, sql, parameters=None, settings=None):
            self.queries.append((sql, parameters))
            return _StreamContext(_Stream([], ["event_type"]))

    client = _Header([])
    cur = _BlockStreamCursor(client)
    cur.execute(SQL, None, "q1")
    assert cur.description == [("event_type",)]
    assert len(client.queries) == 1


def test_a_query_with_no_block_names_the_statements_columns():
    from provisa.core.sync_pool import BlockingPool

    client = _Client([("event_type", "String"), ("order_id", "UInt64")])
    driver = ClickHouseDriver()
    driver._pool = BlockingPool(
        lambda: client, lambda c: c.close(), minsize=1, maxsize=1, wait_s=5.0, name="ch:test"
    )
    result = asyncio.run(driver.execute("SELECT event_type, order_id FROM t WHERE 1 = 0"))
    assert result.rows == []
    assert result.column_names == ["event_type", "order_id"]


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
