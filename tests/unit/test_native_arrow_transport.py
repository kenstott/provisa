# Copyright (c) 2026 Kenneth Stott
# Canary: 92445ad3-29c6-4106-9dc9-68588b428463
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-986: native federation engines surface Arrow through the runtime — ``run_arrow`` returns a
pyarrow.Table and ``run_arrow_stream`` returns ``(schema, record-batch generator)`` for the Flight
server's RecordBatchStream / GeneratorStream, with no Python row materialization. Covers DuckDB (the
zero-config default) and embedded ClickHouse (chdb); the remote ClickHouse backends and warehouse
engines share the same runtime contract, exercised live in integration."""

from __future__ import annotations

import pyarrow as pa
import pytest


def test_duckdb_run_arrow_returns_table():
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = DuckDBFederationRuntime()
    try:
        table = rt.run_arrow("SELECT 1 AS id, 'a' AS s UNION ALL SELECT 2, 'b'")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert table.column_names == ["id", "s"]
    finally:
        rt.close()


def test_duckdb_run_arrow_stream_yields_batches():
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = DuckDBFederationRuntime()
    try:
        schema, batches = rt.run_arrow_stream("SELECT n FROM range(5) t(n)")
        assert isinstance(schema, pa.Schema)
        assert schema.names == ["n"]
        rows = sum(b.num_rows for b in batches)
        assert rows == 5
    finally:
        rt.close()


def test_clickhouse_embedded_run_arrow_returns_table():
    pytest.importorskip("chdb")
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    rt = ClickHouseFederationRuntime.embedded()
    try:
        table = rt.run_arrow("SELECT number AS n FROM numbers(3)")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
        assert table.column_names == ["n"]
    finally:
        rt.close()


def test_clickhouse_embedded_run_arrow_stream_yields_batches():
    pytest.importorskip("chdb")
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    rt = ClickHouseFederationRuntime.embedded()
    try:
        schema, batches = rt.run_arrow_stream("SELECT number AS n FROM numbers(5)")
        assert isinstance(schema, pa.Schema)
        assert sum(b.num_rows for b in batches) == 5
    finally:
        rt.close()


# --- REQ-1216/REQ-1217: warehouse engines that declare ARROW_STREAM must stream GENUINELY ----------
# Databricks/BigQuery/MSSQL previously called the full-materialize primitive then re-emitted its
# batches — declaring a lazy capability the terminal faked. These tests drive each run_arrow_stream
# with a fake driver and assert (a) the full-materialize primitive is NEVER called, and (b) chunks are
# pulled from the server incrementally as the generator drains (peek locks the schema; one fetch so
# far), not all up front. No live warehouse needed — the laziness is a property of the call sequence.


class _Field:
    def __init__(self, name: str) -> None:
        self.name = name


def test_databricks_run_arrow_stream_is_lazy():
    from provisa.federation.databricks_runtime import DatabricksFederationRuntime

    chunk = pa.table({"n": pa.array([1, 2], type=pa.int64())})

    class _Cursor:
        def __init__(self) -> None:
            self._chunks = [chunk, chunk]
            self.fetch_calls = 0
            self.closed = False

        def execute(self, sql, params=None):
            self.executed = sql

        def fetchmany_arrow(self, size):
            self.fetch_calls += 1
            if self._chunks:
                return self._chunks.pop(0)
            return pa.table({"n": pa.array([], type=pa.int64())})  # exhausted — schema, no rows

        def fetchall_arrow(self):
            raise AssertionError("materialized full result — run_arrow_stream is not lazy")

        def close(self):
            self.closed = True

    cur = _Cursor()
    rt = object.__new__(DatabricksFederationRuntime)
    rt._conn = type("C", (), {"cursor": lambda self: cur})()  # type: ignore[assignment]

    schema, gen = rt.run_arrow_stream("SELECT n FROM t")
    assert schema.names == ["n"]
    assert cur.fetch_calls == 1  # only the peek — result not drained yet
    batches = iter(gen)
    first = next(batches)
    assert cur.fetch_calls == 1  # first chunk was the peeked one; no new fetch
    rest = list(batches)
    assert cur.closed
    assert first.num_rows + sum(b.num_rows for b in rest) == 4


def test_bigquery_run_arrow_stream_is_lazy():
    from provisa.federation.bigquery_runtime import BigQueryFederationRuntime

    batch = pa.record_batch({"n": pa.array([1, 2], type=pa.int64())})
    produced: list[int] = []

    class _RowIter:
        schema = [_Field("n")]

        def to_arrow_iterable(self):
            for b in (batch, batch):
                produced.append(1)
                yield b

    class _Job:
        def result(self):
            return _RowIter()

        def to_arrow(self):
            raise AssertionError("materialized full result — run_arrow_stream is not lazy")

    rt = object.__new__(BigQueryFederationRuntime)
    rt._client = type("Cl", (), {"query": lambda self, sql: _Job()})()  # type: ignore[assignment]

    schema, gen = rt.run_arrow_stream("SELECT n FROM t")
    assert schema.names == ["n"]
    assert produced == [1]  # only the peeked batch pulled so far
    total = sum(b.num_rows for b in gen)
    assert produced == [1, 1]  # remaining batch pulled only as the generator drained
    assert total == 4


def test_mssql_run_arrow_stream_is_lazy(monkeypatch):
    from provisa.federation import mssql_warehouse_runtime as mod

    monkeypatch.setattr(mod, "_ARROW_CHUNK_ROWS", 2)  # small chunks to exercise multi-fetch on 5 rows

    class _Cursor:
        def __init__(self) -> None:
            self._rows = [(i,) for i in range(5)]
            self.description = [("n",)]
            self.fetch_calls = 0
            self.closed = False
            self._pos = 0

        def execute(self, sql):
            self.executed = sql

        def fetchmany(self, size):
            self.fetch_calls += 1
            chunk = self._rows[self._pos : self._pos + size]
            self._pos += size
            return chunk

        def fetchall(self):
            raise AssertionError("materialized full result — run_arrow_stream is not lazy")

        def close(self):
            self.closed = True

    cur = _Cursor()
    rt = object.__new__(mod.MssqlWarehouseRuntime)
    rt._conn = type("C", (), {"cursor": lambda self: cur})()

    schema, gen = rt.run_arrow_stream("SELECT n FROM t")
    assert schema.names == ["n"]
    assert cur.fetch_calls == 1  # only the peek chunk
    total = sum(b.num_rows for b in gen)
    assert total == 5
    assert cur.closed
    assert cur.fetch_calls > 2  # drained in multiple server round-trips, not one materialization


# --- REQ-1217 Defect 3: warehouse run_sync STREAMS (pgwire ENGINE route stays bounded) -------------
# run_sync is the ROWS terminal the pgwire ENGINE route drains. It previously returned a fully
# materialized QueryResult; now it is built on the engine's lazy Arrow primitive via
# stream_rows_from_arrow, so it returns a StreamingQueryResult that pulls one batch at a time.


def test_clickhouse_embedded_run_sync_streams():
    pytest.importorskip("chdb")
    from provisa.executor.result import StreamingQueryResult
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    rt = ClickHouseFederationRuntime.embedded()
    try:
        rs = rt.run_sync("SELECT number AS n, toString(number) AS s FROM numbers(5)")
        assert isinstance(rs, StreamingQueryResult)  # not a materialized QueryResult
        assert rs.column_names == ["n", "s"]
        assert list(rs.iter_rows()) == [(i, str(i)) for i in range(5)]
    finally:
        rt.close()


def test_databricks_run_sync_streams_lazily():
    from provisa.executor.result import StreamingQueryResult
    from provisa.federation.databricks_runtime import DatabricksFederationRuntime

    chunk = pa.table({"n": pa.array([1, 2], type=pa.int64())})

    class _Cursor:
        def __init__(self) -> None:
            self._chunks = [chunk, chunk]
            self.fetch_calls = 0
            self.closed = False

        def execute(self, sql, params=None):
            self.executed = sql

        def fetchmany_arrow(self, size):
            self.fetch_calls += 1
            return self._chunks.pop(0) if self._chunks else pa.table({"n": pa.array([], pa.int64())})

        def fetchall_arrow(self):
            raise AssertionError("run_sync materialized the full result — not streaming")

        def close(self):
            self.closed = True

    cur = _Cursor()
    rt = object.__new__(DatabricksFederationRuntime)
    rt._conn = type("C", (), {"cursor": lambda self: cur})()  # type: ignore[assignment]

    rs = rt.run_sync("SELECT n FROM t")
    assert isinstance(rs, StreamingQueryResult)
    assert rs.column_names == ["n"]
    assert cur.fetch_calls == 1  # only the peek — the ENGINE route has not drained it yet
    assert list(rs.iter_rows()) == [(1,), (2,), (1,), (2,)]
    assert cur.closed


def test_mssql_run_sync_streams_lazily(monkeypatch):
    from provisa.executor.result import StreamingQueryResult
    from provisa.federation import mssql_warehouse_runtime as mod

    monkeypatch.setattr(mod, "_ARROW_CHUNK_ROWS", 2)

    class _Cursor:
        def __init__(self) -> None:
            self._rows = [(i,) for i in range(5)]
            self.description = [("n",)]
            self.fetch_calls = 0
            self.closed = False
            self._pos = 0

        def execute(self, sql):
            self.executed = sql

        def fetchmany(self, size):
            self.fetch_calls += 1
            chunk = self._rows[self._pos : self._pos + size]
            self._pos += size
            return chunk

        def fetchall(self):
            raise AssertionError("run_sync materialized the full result — not streaming")

        def close(self):
            self.closed = True

    cur = _Cursor()
    rt = object.__new__(mod.MssqlWarehouseRuntime)
    rt._conn = type("C", (), {"cursor": lambda self: cur})()  # type: ignore[assignment]

    rs = rt.run_sync("SELECT n FROM t")
    assert isinstance(rs, StreamingQueryResult)
    assert rs.column_names == ["n"]
    assert cur.fetch_calls == 1  # only the peek chunk
    assert list(rs.iter_rows()) == [(i,) for i in range(5)]
    assert cur.fetch_calls > 2 and cur.closed  # drained across multiple server round-trips


def test_mssql_run_arrow_builds_table_from_stream(monkeypatch):
    # run_arrow is built directly on run_arrow_stream (Arrow batches → Table), not round-tripped
    # through run_sync's Python rows. pyodbc has no native Arrow, so the forward-only cursor is the
    # only source; the fetchall materializer must never be touched.
    from provisa.federation import mssql_warehouse_runtime as mod

    monkeypatch.setattr(mod, "_ARROW_CHUNK_ROWS", 2)

    class _Cursor:
        def __init__(self) -> None:
            self._rows = [(i,) for i in range(5)]
            self.description = [("n",)]
            self.closed = False
            self._pos = 0

        def execute(self, sql):
            self.executed = sql

        def fetchmany(self, size):
            chunk = self._rows[self._pos : self._pos + size]
            self._pos += size
            return chunk

        def fetchall(self):
            raise AssertionError("run_arrow materialized the full result — not built on the stream")

        def close(self):
            self.closed = True

    cur = _Cursor()
    rt = object.__new__(mod.MssqlWarehouseRuntime)
    rt._conn = type("C", (), {"cursor": lambda self: cur})()  # type: ignore[assignment]

    table = rt.run_arrow("SELECT n FROM t")
    assert isinstance(table, pa.Table)
    assert table.column_names == ["n"]
    assert table.column("n").to_pylist() == [0, 1, 2, 3, 4]
    assert cur.closed


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
