# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b2d9e-8a41-4c7e-b5d0-2e9a7c1f4b68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DIRECT streamed read from a driver that fetches Arrow carries Arrow end to end (REQ-2194):
the driver's stream hands on its record batches with their declared types, the DIRECT streaming
terminal (``EngineRuntime.execute_native_stream``) passes them on as an ``ArrowStreamResult``,
and an encoder reading Arrow gets them with no Python row built in between. A row consumer still
reads rows, each batch turned into them as it pulls it."""

# Requirements: REQ-2194, REQ-2195, REQ-1190

from __future__ import annotations

import asyncio
from decimal import Decimal

import pyarrow as pa
import pytest

from provisa.executor.drivers import duckdb_driver
from provisa.executor.drivers.base import DirectArrowStream
from provisa.executor.drivers.duckdb_driver import DuckDBDriver
from provisa.executor.result import ArrowStreamResult, StreamingQueryResult, is_arrow
from provisa.federation.runtime import EngineRuntime

_TYPED_SQL = (
    "SELECT range::INTEGER AS i, 'v' || range::VARCHAR AS s, "
    "(range * 1.25)::DECIMAL(18,2) AS d, DATE '2026-01-01' + range::INTEGER AS dt "
    "FROM range(5)"
)

_DECLARED = pa.schema(
    [
        pa.field("i", pa.int32()),
        pa.field("s", pa.string()),
        pa.field("d", pa.decimal128(18, 2)),
        pa.field("dt", pa.date32()),
    ]
)


@pytest.fixture
def driver():
    d = DuckDBDriver()
    asyncio.run(d.connect("", 0, ":memory:", "", ""))
    yield d
    asyncio.run(d.close())


class _Pools:
    """The one source's pool, as ``executor.direct.open_direct_stream`` reads it."""

    def __init__(self, driver: DuckDBDriver) -> None:
        self._driver = driver

    async def open_stream(self, _source_id: str, sql: str, params: list | None):
        return await self._driver.open_stream(sql, params)


def _runtime() -> EngineRuntime:
    return EngineRuntime.__new__(EngineRuntime)  # only execute_native_stream is exercised


def _native_stream(driver: DuckDBDriver, sql: str):
    loop = asyncio.new_event_loop()
    stream = _runtime().execute_native_stream(
        _Pools(driver), "src", sql, [], run=loop.run_until_complete
    )
    return stream, loop


@pytest.fixture
def no_rows_built(monkeypatch):
    """Records every batch the DuckDB driver turns into Python rows."""
    built: list[int] = []
    real = duckdb_driver.arrow_rows

    def _recording(batch):
        built.append(batch.num_rows)
        return real(batch)

    monkeypatch.setattr(duckdb_driver, "arrow_rows", _recording)
    return built


def test_a_driver_stream_hands_on_its_record_batches_with_their_declared_types(driver):
    async def run():
        stream = await driver.open_stream(_TYPED_SQL)
        batches = []
        while (batch := await stream.fetch_arrow()) is not None:
            batches.append(batch)
        return stream, batches

    stream, batches = asyncio.run(run())
    assert isinstance(stream, DirectArrowStream)
    assert stream.schema == _DECLARED
    assert all(isinstance(b, pa.RecordBatch) and b.schema == _DECLARED for b in batches)
    table = pa.Table.from_batches(batches, schema=_DECLARED)
    assert table.column("i").to_pylist() == [0, 1, 2, 3, 4]
    assert table.column("d").to_pylist()[1] == Decimal("1.25")


def test_the_direct_terminal_passes_a_driver_arrow_stream_on_as_arrow(driver, no_rows_built):
    stream, loop = _native_stream(driver, _TYPED_SQL)
    try:
        assert isinstance(stream, ArrowStreamResult) and is_arrow(stream)
        assert stream.schema == _DECLARED
        assert stream.column_names == ["i", "s", "d", "dt"]
        batches = list(stream.arrow_batches())
    finally:
        loop.close()
    assert batches and all(b.schema == _DECLARED for b in batches)
    assert sum(b.num_rows for b in batches) == 5
    assert stream.stats.done and stream.stats.row_count == 5
    assert no_rows_built == []


def test_an_arrow_encoder_reads_a_direct_stream_with_no_row_round_trip(driver, no_rows_built):
    from provisa.executor.json_encoders import result_batches

    stream, loop = _native_stream(driver, _TYPED_SQL)
    try:
        encoded = result_batches(stream)
    finally:
        loop.close()
    assert isinstance(encoded, list)
    assert all(isinstance(b, pa.RecordBatch) and b.schema == _DECLARED for b in encoded)
    assert sum(b.num_rows for b in encoded) == 5
    assert no_rows_built == []


def test_a_row_consumer_reads_a_direct_arrow_stream_as_rows(driver):
    stream, loop = _native_stream(driver, "SELECT range AS id, 'x' AS s FROM range(3)")
    try:
        rows = list(stream.iter_rows())
    finally:
        loop.close()
    assert rows == [(0, "x"), (1, "x"), (2, "x")]
    assert stream.stats.done


def test_a_direct_arrow_stream_closed_for_its_schema_frees_its_connection(driver):
    pool = driver._require_pool()
    stream, loop = _native_stream(driver, "SELECT range AS id FROM range(1000)")
    try:
        assert stream.schema.names == ["id"]
        assert pool.counts()["checked_out"] == 1
        stream.close()
    finally:
        loop.close()
    assert pool.counts()["checked_out"] == 0


def test_a_drained_direct_arrow_stream_gives_its_connection_back(driver):
    pool = driver._require_pool()
    stream, loop = _native_stream(driver, "SELECT range AS id FROM range(10)")
    try:
        assert sum(b.num_rows for b in stream.arrow_batches()) == 10
    finally:
        loop.close()
    counts = pool.counts()
    assert counts["checked_out"] == 0 and counts["open"] == 1  # given back, not discarded


def test_a_row_driver_stays_a_row_stream():
    """A driver that fetches no Arrow is a row stream: its rows go to the encoders as rows, never
    built into Arrow to be encoded (REQ-2195)."""
    from provisa.executor.drivers.base import DirectResultStream

    class _RowStream(DirectResultStream):
        column_names = ["id"]
        column_types = None

        def __init__(self) -> None:
            self._rows = [[(1,), (2,)]]

        async def fetch(self, size: int) -> list[tuple]:
            return self._rows.pop() if self._rows else []

        async def close(self) -> None:
            return None

    class _RowPools:
        async def open_stream(self, _source_id, _sql, _params):
            return _RowStream()

    loop = asyncio.new_event_loop()
    try:
        stream = _runtime().execute_native_stream(
            _RowPools(), "src", "SELECT id FROM t", [], run=loop.run_until_complete
        )
        assert type(stream) is StreamingQueryResult and not is_arrow(stream)
        assert stream.rows == [(1,), (2,)]
    finally:
        loop.close()
