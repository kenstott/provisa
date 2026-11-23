# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b1f4a-7c2d-4e9b-a5f6-0d1c2b3a4e5f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A response-cache hit is its miss, byte for byte, on Flight SQL and pgwire (REQ-1867, amended
2026-11-23).

The entry holds the record batches the miss produced, so a hit hands Flight the same Arrow IPC
stream and pgwire's encoder the same batches: the bytes each writes are the miss's bytes. A miss
that drained Python rows is kept as Arrow; pgwire's Arrow encoder writes the DataRows its row
encoder wrote for those rows. And the hit does no per-value work: a large_scan-sized hit is not
slower than its miss on any surface.
"""

from __future__ import annotations

import datetime as dt
import time
from decimal import Decimal

import pyarrow as pa
import pytest

from provisa._encoders import pgwire as encoder
from provisa.executor.result import ArrowResult, ArrowStreamResult, StreamingQueryResult
from provisa.pgwire._pipeline import (
    check_response_cache,
    check_response_cache_arrow,
    response_cache_tee,
)
from tests.unit.test_raw_sql_response_cache import _plan, _state
from tests.unit.test_response_cache_shared import FakeCacheStore

UTC = dt.timezone.utc


@pytest.fixture(autouse=True)
def _bound_to_org_a(bind_org):
    bind_org("org-a")


@pytest.fixture(autouse=True)
def _no_audit_writes(monkeypatch):
    async def _noop(pending, status_code, state=None, **outcome):
        return None

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", _noop)


@pytest.fixture(autouse=True)
def _bound(monkeypatch):
    monkeypatch.setattr("provisa.pgwire._pipeline._response_cache_bound", lambda: 1_000_000)


# The engine's Arrow, as DuckDB hands an ENGINE-route read to pgwire: every type the retired
# msgpack codec carried.
_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("amount", pa.decimal128(18, 2)),
        pa.field("at", pa.timestamp("us", tz="UTC")),
        pa.field("local", pa.timestamp("us")),
        pa.field("day", pa.date32()),
        pa.field("gap", pa.month_day_nano_interval()),
        pa.field("blob", pa.binary()),
        pa.field("tags", pa.list_(pa.int32())),
        pa.field("name", pa.string()),
    ]
)
_BV = ["BIGINT", "DECIMAL", "TIMESTAMPTZ", "TIMESTAMP", "DATE", "INTERVAL", "BYTES",
       "INTEGERARRAY", "TEXT"]  # fmt: skip
_TYPES = ["BIGINT", "DECIMAL(18,2)", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP", "DATE", "INTERVAL",
          "BLOB", "INTEGER[]", "VARCHAR"]  # fmt: skip
_DATE_INF = 2**31 - 1
_TS_INF = 2**63 - 1


def _engine_batches(n: int = 3, per: int = 2) -> list[pa.RecordBatch]:
    out = []
    for b in range(n):
        base = b * per
        out.append(
            pa.RecordBatch.from_arrays(
                [
                    pa.array([base + i for i in range(per)], pa.int64()),
                    pa.array(
                        [Decimal("12.34"), None][:per] + [Decimal("-0.05")] * (per - 2),
                        pa.decimal128(18, 2),
                    ),  # fmt: skip
                    pa.array(
                        [_TS_INF, 1_700_000_000_000_000][:per] + [0] * (per - 2), pa.int64()
                    ).cast(pa.timestamp("us", tz="UTC")),  # fmt: skip
                    pa.array([-_TS_INF, 86_400_000_000][:per] + [0] * (per - 2), pa.int64()).cast(
                        pa.timestamp("us")
                    ),  # fmt: skip
                    pa.array([_DATE_INF, -_DATE_INF][:per] + [1] * (per - 2), pa.int32()).cast(
                        pa.date32()
                    ),  # fmt: skip
                    pa.array(
                        [(14, 3, 5_000_000_000), None][:per] + [(0, 1, 0)] * (per - 2),
                        pa.month_day_nano_interval(),
                    ),  # fmt: skip
                    pa.array([b"\x00\xff", None][:per] + [b""] * (per - 2), pa.binary()),
                    pa.array([[1, None, 3], []][:per] + [[7]] * (per - 2), pa.list_(pa.int32())),
                    pa.array(["a", None][:per] + ["z"] * (per - 2)),
                ],
                schema=_SCHEMA,
            )
        )
    return out


def _to_rows(batch):
    from provisa.cache.raw_sql import _table_rows

    return _table_rows(pa.Table.from_batches([batch]))


def _ipc(schema: pa.Schema, batches) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _pgwire(batches) -> bytes:
    return b"".join(encoder.encode_batch(b, _BV, [0] * len(_BV), None) for b in batches)


async def _miss_through_pgwire(state, batches) -> list[pa.RecordBatch]:
    """pgwire's ENGINE terminal: the engine's Arrow stream teed, drained as Arrow."""
    tee = response_cache_tee(_plan(), state, run=None)
    assert tee is not None
    stream = ArrowStreamResult(_SCHEMA, iter(batches), _TYPES, to_rows=_to_rows)
    served = list(tee.rows(stream).arrow_batches())
    await tee.commit()
    return served


@pytest.mark.asyncio
async def test_pgwire_hit_writes_the_miss_datarows_byte_for_byte():
    state = _state(FakeCacheStore())
    batches = _engine_batches()
    served = await _miss_through_pgwire(state, batches)
    miss_bytes = _pgwire(served)

    hit = await check_response_cache(_plan(), state)
    assert isinstance(hit, ArrowResult) and hit.table is not None
    assert hit.column_types == _TYPES  # RowDescription is the miss's
    assert _pgwire(hit.arrow_batches()) == miss_bytes


@pytest.mark.asyncio
async def test_flight_hit_streams_the_miss_ipc_byte_for_byte():
    state = _state(FakeCacheStore())
    batches = _engine_batches()
    tee = response_cache_tee(_plan(), state, run=None)
    assert tee is not None
    served = list(tee.arrow(_SCHEMA, iter(batches)))
    await tee.commit()
    miss_bytes = _ipc(_SCHEMA, served)

    table = await check_response_cache_arrow(_plan(), state)
    assert table is not None
    assert _ipc(table.schema, table.to_batches()) == miss_bytes


@pytest.mark.asyncio
async def test_a_pgwire_miss_served_to_flight_is_the_engine_ipc():
    """An entry pgwire's ENGINE terminal wrote is the engine's batches: Flight's hit streams them
    (no registered shape here, so nothing is cast) exactly as Flight's own miss would."""
    state = _state(FakeCacheStore())
    batches = _engine_batches()
    await _miss_through_pgwire(state, batches)
    table = await check_response_cache_arrow(_plan(), state)
    assert table is not None
    assert _ipc(table.schema, table.to_batches()) == _ipc(_SCHEMA, batches)


_ROW_TYPES = ["BIGINT", "NUMERIC", "TIMESTAMP WITH TIME ZONE", "DATE", "INTERVAL", "BYTEA",
              "INTEGER[]", "VARCHAR", "BOOLEAN"]  # fmt: skip
_ROW_BV = ["BIGINT", "DECIMAL", "TIMESTAMPTZ", "DATE", "INTERVAL", "BYTES", "INTEGERARRAY",
           "TEXT", "BOOL"]  # fmt: skip


def _source_rows() -> list[tuple]:
    from dateutil.relativedelta import relativedelta

    from provisa.core.ir_types import infinite_date, infinite_timestamp

    return [
        (1, Decimal("12.340"), dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC), dt.date(2026, 1, 2),
         relativedelta(months=14, days=3, seconds=5), b"\x00\xff", [1, None, 3], "a", True),
        (2, Decimal("-0.001"), infinite_timestamp(1, aware=True), infinite_date(-1),
         dt.timedelta(days=2), None, [], None, False),
        (3, None, None, None, None, b"", None, "z", None),
    ]  # fmt: skip


@pytest.mark.asyncio
async def test_a_row_stream_miss_and_its_arrow_hit_write_the_same_datarows():
    """A DIRECT stream drains Python rows and pgwire's row encoder writes them; the entry keeps
    them as Arrow and the hit goes to pgwire's Arrow encoder. The DataRows are the same bytes."""
    state = _state(FakeCacheStore())
    rows = _source_rows()
    names = [f"c{i}" for i in range(len(_ROW_TYPES))]
    tee = response_cache_tee(_plan(), state, run=None)
    assert tee is not None
    stream = StreamingQueryResult(iter([rows]), column_names=names, column_types=_ROW_TYPES)
    served = [r for b in tee.rows(stream).batches() for r in b]
    await tee.commit()
    formats = [0] * len(_ROW_BV)
    miss_bytes = encoder.encode_rows(served, _ROW_BV, formats, None, _ROW_TYPES)

    hit = await check_response_cache(_plan(), state)
    assert isinstance(hit, ArrowResult)
    assert hit.column_types == _ROW_TYPES
    hit_bytes = b"".join(
        encoder.encode_batch(b, _ROW_BV, formats, None) for b in hit.arrow_batches()
    )
    assert hit_bytes == miss_bytes
    binary = [1] * len(_ROW_BV)
    assert b"".join(
        encoder.encode_batch(b, _ROW_BV, binary, None) for b in hit.arrow_batches()
    ) == encoder.encode_rows(served, _ROW_BV, binary, None, _ROW_TYPES)


def _large_scan_batches() -> list[pa.RecordBatch]:
    """80,050 rows of the order_items shape the mini bench's large_scan reads, in the engine's
    65,536-row batches."""
    n = 80_050
    table = pa.table(
        {
            "item_id": pa.array(range(n), pa.int64()),
            "order_id": pa.array([i // 3 for i in range(n)], pa.int64()),
            "sku": pa.array([f"SKU-{i % 999_983:06d}" for i in range(n)]),
            "product_name": pa.array([f"Product {i % 5000}" for i in range(n)]),
            "category": pa.array([("Home", "Toys", "Books")[i % 3] for i in range(n)]),
            "quantity": pa.array([i % 20 for i in range(n)], pa.int32()),
            "unit_price": pa.array(range(n), pa.int64()).cast(pa.decimal128(10, 2)),
            "line_total": pa.array(range(n), pa.int64()).cast(pa.decimal128(12, 2)),
            "backorder_flag": pa.array([i % 10 == 0 for i in range(n)]),
            "lead_time_days": pa.array([i % 60 for i in range(n)], pa.int32()),
        }
    )
    return table.to_batches(max_chunksize=65_536)


def _best_ms(fn, rounds: int = 5) -> float:
    times = []
    for _ in range(rounds):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return min(times) * 1000


@pytest.mark.asyncio
async def test_a_large_scan_hit_is_not_slower_than_its_miss_on_each_surface():
    """The hit's own work (read the entry, hand its batches on) against the miss's serving work
    alone -- the engine's read is left out of the miss, so the miss is the faster side of a fair
    race. pgwire and gRPC encode the batches either way: the hit adds only the entry's decode,
    which must cost less than the per-value row build the msgpack entry made each hit pay. Flight
    writes the batches either way."""
    import asyncio

    from provisa.cache.raw_sql import decode_entry, encode_arrow_entry, entry_as_arrow
    from provisa.cache.raw_sql import entry_as_result

    batches = _large_scan_batches()
    schema = batches[0].schema
    bv = ["BIGINT", "BIGINT", "TEXT", "TEXT", "TEXT", "INTEGER", "DECIMAL", "DECIMAL", "BOOL",
          "INTEGER"]  # fmt: skip
    data = encode_arrow_entry(schema, batches, None)

    def pg(bs) -> int:
        return sum(len(encoder.encode_batch(b, bv, [0] * len(bv), None)) for b in bs)

    def rows_then_pg() -> None:
        # What a hit cost before: every value built into Python, then the row encoder.
        from provisa.cache.raw_sql import _table_rows

        rows = _table_rows(pa.Table.from_batches(batches))
        encoder.encode_rows(rows, bv, [0] * len(bv), None, None)

    miss_pg = _best_ms(lambda: pg(batches))
    hit_pg = _best_ms(lambda: pg(entry_as_result(decode_entry(data)).arrow_batches()))
    old_hit_pg = _best_ms(rows_then_pg, rounds=2)
    miss_flight = _best_ms(lambda: _ipc(schema, batches))
    hit_flight = _best_ms(
        lambda: (lambda t: _ipc(t.schema, t.to_batches()))(entry_as_arrow(decode_entry(data), None))
    )
    decode = _best_ms(lambda: decode_entry(data))
    print(
        f"\nlarge_scan 80,050 rows: entry {len(data):,} bytes; decode {decode:.1f} ms; "
        f"pgwire miss {miss_pg:.1f} / hit {hit_pg:.1f} (old hit {old_hit_pg:.1f}) ms; "
        f"flight miss {miss_flight:.1f} / hit {hit_flight:.1f} ms"
    )
    await asyncio.sleep(0)
    # The hit's decode is the only work the miss did not do; it is a small fraction of the
    # serving work and far below the per-value row build the old entry cost every hit.
    assert hit_pg - miss_pg < old_hit_pg / 5
    assert hit_pg < old_hit_pg / 3
    assert hit_flight - miss_flight < decode * 1.5 + 5
