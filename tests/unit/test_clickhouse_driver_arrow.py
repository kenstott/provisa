# Copyright (c) 2026 Kenneth Stott
# Canary: 2bc4762a-6446-4512-9c29-a4798ca0c7f6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2194: a ClickHouse DIRECT result is read as Arrow (clickhouse-connect's ``query_arrow`` and
``query_arrow_stream``), never as clickhouse-connect's Python rows. A column whose Arrow type names
no single ClickHouse type is typed by ``DESCRIBE`` and holds the row read's value: a UUID as its
text, a DateTime64 as a microsecond timestamp in its zone; Decimal stays decimal128 and an Array a
list. Arrow output needs ClickHouse 26 or later (earlier servers refuse UUID in it), so an older
server is refused at connect, by name, with its version and the minimum.

The clients are fakes standing in for clickhouse-connect's: ``query_arrow`` hands back the pyarrow
table the server's Arrow output reads as, and ``query`` answers only ``DESCRIBE``."""

# Requirements: REQ-2194

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.clickhouse import ClickHouseDriver, ClickHouseServerTooOld
from provisa.executor.result import ArrowResult, is_arrow

_ZONE = "Europe/Berlin"
_ID = uuid.UUID("61f0c404-5cb3-11e7-907b-a6006ad3dba0")
_TYPED = pa.table(
    {
        # ClickHouse 26 writes a UUID as Arrow's canonical uuid extension type.
        "id": pa.ExtensionArray.from_storage(
            pa.uuid(), pa.array([_ID.bytes, None], type=pa.binary(16))
        ),
        "amount": pa.array([Decimal("12345.6789"), None], type=pa.decimal128(18, 4)),
        "at": pa.array([1_700_000_000_123, None], type=pa.timestamp("ms", tz=_ZONE)),
        "tags": pa.array([["a", "b"], []], type=pa.list_(pa.string())),
        "name": pa.array(["x", "y"], type=pa.string()),
    }
)


# What ClickHouse's DESCRIBE answers for _TYPED's statement: (name, type).
_TYPED_DESCRIBED = [
    ["id", "Nullable(UUID)"],
    ["amount", "Nullable(Decimal(18, 4))"],
    ["at", f"Nullable(DateTime64(3, '{_ZONE}'))"],
    ["tags", "Array(String)"],
    ["name", "String"],
]


def _query_context(query=None, parameters=None, **_kw):
    from clickhouse_connect.driver.query import QueryContext

    return QueryContext(
        query=query or "", parameters=parameters, server_tz=timezone.utc, apply_server_tz=True
    )


class _Client:
    """clickhouse-connect's client: ``query_arrow`` answers with ``table``; ``query`` answers
    ``DESCRIBE`` with ``described`` and must not be called to read a result's rows."""

    server_version = "26.8.46.6"
    create_query_context = staticmethod(_query_context)

    def __init__(self, table: pa.Table, described: list[list[str]] | None = None) -> None:
        self._table = table
        self._described = described
        self.arrow_calls: list[tuple[str, Any, dict]] = []
        self.described_calls: list[str] = []

    def query_arrow(self, sql, parameters=None, settings=None, use_strings=None, **_kw):
        self.arrow_calls.append((sql, parameters, {"use_strings": use_strings, **settings}))
        return self._table

    def query(self, sql, parameters=None, **_k):
        if not sql.startswith("DESCRIBE (") or self._described is None:
            raise AssertionError("a DIRECT read built clickhouse-connect's Python rows")
        self.described_calls.append(sql)
        return SimpleNamespace(result_rows=self._described, column_names=("name", "type"))

    def close(self):
        pass


def _driver(client: Any) -> ClickHouseDriver:
    driver = ClickHouseDriver()
    driver._pool = BlockingPool(  # noqa: SLF001
        lambda: client, lambda c: c.close(), minsize=1, maxsize=1, wait_s=5.0, name="ch:test"
    )
    return driver


def test_a_result_is_arrow_with_each_ambiguous_column_typed_by_describe():
    client = _Client(_TYPED, _TYPED_DESCRIBED)
    result = asyncio.run(_driver(client).execute("SELECT id, amount, at, tags, name FROM t"))

    assert isinstance(result, ArrowResult) and is_arrow(result)
    # UUID (an extension type) and the timestamp name no single ClickHouse type: DESCRIBE types them.
    assert client.described_calls == ["DESCRIBE (SELECT id, amount, at, tags, name FROM t)"]
    assert result.table.schema == pa.schema(
        [
            ("id", pa.string()),
            ("amount", pa.decimal128(18, 4)),
            ("at", pa.timestamp("us", tz=_ZONE)),
            ("tags", pa.list_(pa.string())),
            ("name", pa.string()),
        ]
    )
    assert result.table.column("id").to_pylist() == [str(_ID), None]
    assert result.column_names == ["id", "amount", "at", "tags", "name"]
    assert result.rows == [
        (
            _ID,
            Decimal("12345.6789"),
            datetime(2023, 11, 14, 23, 13, 20, 123000, tzinfo=ZoneInfo(_ZONE)),
            ["a", "b"],
            "x",
        ),
        (None, None, None, [], "y"),
    ]
    assert result.rows[0][2].utcoffset() is not None
    # String reads as Arrow string, not binary.
    assert client.arrow_calls[0][2]["use_strings"] is True


def test_positional_params_are_bound_through_the_arrow_read():
    client = _Client(pa.table({"v": pa.array(["x"])}))
    asyncio.run(_driver(client).execute("SELECT @1 AS v, @2 AS w", params=["x", 7]))
    sql, parameters, _settings = client.arrow_calls[0]
    assert "@1" not in sql and "@2" not in sql
    assert parameters == {"p1": "x", "p2": 7}


@pytest.mark.parametrize("version", ["24.3.18.7", "25.12.1.1"])
def test_a_server_older_than_26_is_refused_at_connect_by_name(monkeypatch, version):
    import clickhouse_connect

    closed: list[bool] = []

    class _Old:
        server_version = version

        def close(self):
            closed.append(True)

    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **_k: _Old())
    driver = ClickHouseDriver()
    with pytest.raises(ClickHouseServerTooOld) as refused:
        asyncio.run(driver.connect("h", 8123, "default", "u", "p"))
    message = str(refused.value)
    assert version in message and "26" in message
    assert refused.value.version == version and refused.value.minimum == 26
    assert closed == [True]  # the refused client is not left open
    assert not driver.is_connected


def test_a_server_of_26_or_later_connects(monkeypatch):
    import clickhouse_connect

    monkeypatch.setattr(
        clickhouse_connect,
        "get_client",
        lambda **_k: SimpleNamespace(server_version="26.8.46.6", close=lambda: None),
    )
    driver = ClickHouseDriver()
    asyncio.run(driver.connect("h", 8123, "default", "u", "p"))
    assert driver.is_connected


def test_a_stream_hands_on_the_servers_record_batches_and_kills_one_closed_early(monkeypatch):
    batches_read: list[int] = []
    schema = pa.schema([("id", pa.int64()), ("amount", pa.decimal128(10, 2))])
    blocks = [
        pa.record_batch(
            [pa.array([1, 2]), pa.array([Decimal("1.50"), None], schema[1].type)], schema=schema
        ),
        pa.record_batch(
            [pa.array([3, 4]), pa.array([Decimal("2.00"), Decimal("3.25")], schema[1].type)],
            schema=schema,
        ),
        pa.record_batch(
            [pa.array([5]), pa.array([Decimal("4.00")], schema[1].type)], schema=schema
        ),
    ]

    class _Stream:
        # clickhouse-connect's StreamContext: ``gen`` is pyarrow's stream reader.
        gen = SimpleNamespace(schema=schema)
        exited = False

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            _Stream.exited = True

        def __iter__(self):
            for n, block in enumerate(blocks, start=1):
                batches_read.append(n)
                yield block

    class _StreamClient:
        server_version = "26.8.46.6"
        settings: dict | None = None
        create_query_context = staticmethod(_query_context)

        def query_arrow_stream(self, sql, parameters=None, settings=None, use_strings=None):
            _StreamClient.settings = {"use_strings": use_strings, **settings}
            return _Stream()

        def query_row_block_stream(self, *_a, **_k):
            raise AssertionError("a DIRECT stream read clickhouse-connect's Python row blocks")

        def close(self):
            pass

    driver = _driver(_StreamClient())
    killed: list[str] = []
    monkeypatch.setattr(driver, "_kill_query", lambda query_id: killed.append(query_id))

    async def run():
        stream = await driver.open_stream("SELECT id, amount FROM t")
        first = await stream.fetch_arrow()
        rows = await stream.fetch(2)
        await stream.close()
        return stream, first, rows

    stream, first, rows = asyncio.run(run())
    assert stream.column_names == ["id", "amount"]
    assert stream.schema == schema
    assert first.equals(blocks[0])
    assert rows == [(3, Decimal("2.00")), (4, Decimal("3.25"))]
    assert batches_read == [1, 2]  # the third batch was never asked for
    assert _StreamClient.settings is not None and _StreamClient.settings["use_strings"] is True
    assert len(killed) == 1
