# Copyright (c) 2026 Kenneth Stott
# Canary: 0cc40b0a-9623-47e9-ab3a-eb154090f40a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2194: ClickHouse reads a DIRECT result through clickhouse-connect's ``query_arrow`` and
hands the executor the Arrow table, each value what the row read (``client.query``) gives today.

The client here answers as a ClickHouse server over HTTP does, from chdb (ClickHouse itself):
``query_arrow`` with ClickHouse's Arrow output, ``DESCRIBE`` with its answer. The row read is
clickhouse-connect's own Native decoder over ClickHouse's Native output of the same statement,
in a UTC session as the test server runs."""

# Requirements: REQ-2194

from __future__ import annotations

import asyncio
import io
import ipaddress
import json
import uuid
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import chdb
import pyarrow as pa
import pytest

from provisa.core.ir_arrow import column_values, text_of
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.clickhouse import ClickHouseDriver
from provisa.executor.result import ArrowResult, is_arrow

_UTC = "session_timezone = 'UTC'"


def _chdb(sql: str, fmt: str, settings: dict[str, str] | None = None) -> bytes:
    extra = "".join(f", {k} = {v!r}" for k, v in (settings or {}).items())
    return chdb.query(f"{sql} SETTINGS {_UTC}{extra}", fmt).bytes()


class _Described:
    def __init__(self, rows: list[list[Any]]) -> None:
        self.result_rows = rows
        self.column_names = ()


class _ChdbClient:
    """A clickhouse-connect client answered by chdb, recording what it was asked."""

    def __init__(self, server_settings: dict[str, str] | None = None) -> None:
        self.server_settings = server_settings or {}
        self.calls: list[tuple[str, str, Any]] = []

    def create_query_context(self, query: str | None = None, parameters: Any = None, **_kw):
        from clickhouse_connect.driver.query import QueryContext

        return QueryContext(
            query=query or "", parameters=parameters, server_tz=timezone.utc, apply_server_tz=True
        )

    def query_arrow(self, sql, parameters=None, use_strings=None, settings=None):
        self.calls.append(("arrow", sql, {"use_strings": use_strings, "settings": settings}))
        pinned = {k: v for k, v in (settings or {}).items() if k != "query_id"}
        raw = _chdb(sql, "Arrow", {**self.server_settings, **pinned})
        return pa.ipc.open_file(io.BytesIO(raw)).read_all()

    def query(self, sql, parameters=None, settings=None):
        self.calls.append(("query", sql, parameters))
        if not sql.startswith("DESCRIBE ("):
            raise AssertionError(f"rows read for {sql!r}")
        return _Described(json.loads(_chdb(sql, "JSONCompact"))["data"])

    def close(self) -> None:
        pass


def _driver(client: Any) -> ClickHouseDriver:
    driver = ClickHouseDriver()
    driver._pool = BlockingPool(
        lambda: client, lambda c: c.close(), minsize=1, maxsize=1, wait_s=5.0, name="ch:test"
    )
    return driver


def _row_read(sql: str) -> list[tuple]:
    """What ``client.query`` gives today: clickhouse-connect's Native decoder over ClickHouse's
    Native output of ``sql``, for a UTC server."""
    from clickhouse_connect.driver.query import QueryContext, QueryResult
    from clickhouse_connect.driver.transform import NativeTransform
    from clickhouse_connect.driverc.buffer import ResponseBuffer

    class _Source:
        def __init__(self, data: bytes) -> None:
            self.gen = iter([data])

        def close(self) -> None:
            pass

    context = QueryContext(query=sql, server_tz=timezone.utc, apply_server_tz=True)
    res = NativeTransform.parse_response(ResponseBuffer(_Source(_chdb(sql, "Native"))), context)
    assert isinstance(res, QueryResult)
    return [tuple(r) for r in res.result_rows]


def _typed(rows: list[tuple]) -> list[tuple]:
    """Each value with its type, so an equal value of another type is a difference."""
    return [tuple((type(v), v) for v in row) for row in rows]


def _execute(sql: str, client: _ChdbClient | None = None) -> ArrowResult:
    result = asyncio.run(_driver(client or _ChdbClient()).execute(sql))
    assert is_arrow(result)
    assert isinstance(result, ArrowResult)
    return result


# -- Arrow, not rows -----------------------------------------------------------------------------


def test_clickhouse_hands_the_executor_its_arrow_table():
    client = _ChdbClient()
    result = _execute("SELECT toInt64(number) AS id, toString(number) AS s FROM numbers(3)", client)
    assert result._rows is None  # no Python row built at the fetch
    assert result.stats.row_count == 3
    assert result.table.column("id").to_pylist() == [0, 1, 2]
    (kind, _sql, kwargs), *rest = client.calls
    assert kind == "arrow" and rest == []  # no DESCRIBE: no column needs its ClickHouse type
    assert kwargs["use_strings"] is True
    assert kwargs["settings"]["output_format_arrow_low_cardinality_as_dictionary"] == "0"
    assert kwargs["settings"]["output_format_arrow_fixed_string_as_fixed_byte_array"] == "1"
    assert kwargs["settings"]["query_id"]
    assert result.rows == [(0, "0"), (1, "1"), (2, "2")]


def test_an_empty_result_names_its_columns_from_the_arrow_schema():
    client = _ChdbClient()
    result = _execute("SELECT toInt64(1) AS id, 'x' AS s WHERE 0", client)
    assert result.column_names == ["id", "s"]
    assert result.rows == []
    assert [c[0] for c in client.calls] == ["arrow"]


def test_a_statement_with_no_result_set_is_run_as_a_command():
    class _Commands(_ChdbClient):
        def query(self, sql, parameters=None, settings=None):
            self.calls.append(("query", sql, parameters))
            return _Described([[""]]) if not sql.startswith("DESCRIBE") else _Described([])

        def query_arrow(self, *a, **k):
            raise AssertionError("a command read as Arrow")

    client = _Commands()
    result = asyncio.run(_driver(client).execute("CREATE TABLE t (id Int64) ENGINE = Memory"))
    assert not is_arrow(result)
    assert client.calls[0][:2] == ("query", "CREATE TABLE t (id Int64) ENGINE = Memory")


# -- Each value as the row read gives it ---------------------------------------------------------

_UUID = "61f0c404-5cb3-11e7-907b-a6006ad3dba0"

_PARITY = {
    "uuid": f"CAST([toUUID('{_UUID}'), generateUUIDv4(), NULL] AS Array(Nullable(UUID)))",
    "ipv4": "CAST(['1.2.3.4', '0.0.0.0', '255.255.255.255', NULL] AS Array(Nullable(IPv4)))",
    "ipv6": "CAST(['2001:db8::1', '::', '::1', '::ffff:1.2.3.4', '::1.2.3.4', '1:0:0:2:0:0:0:3',"
    " 'fe80::1:0:0:0', NULL] AS Array(Nullable(IPv6)))",
    "datetime": "CAST(['2024-01-02 03:04:05', '1970-01-01 00:00:00', NULL]"
    " AS Array(Nullable(DateTime)))",
    "datetime64_3": "CAST(['2024-01-02 03:04:05.123', '1960-05-06 07:08:09.999', NULL]"
    " AS Array(Nullable(DateTime64(3))))",
    "datetime64_9": "CAST(['2024-01-02 03:04:05.123456789', '1960-05-06 07:08:09.000000001']"
    " AS Array(DateTime64(9)))",
    "decimal": "CAST([12.345, -0.001, NULL] AS Array(Nullable(Decimal(18, 3))))",
    "decimal256": "CAST(['123456789012345678901234567890.12', NULL]"
    " AS Array(Nullable(Decimal(76, 2))))",
    "nullable_int": "CAST([1, NULL] AS Array(Nullable(Int32)))",
    "array_uuid": f"[[toUUID('{_UUID}')], []]",
    "array_ipv4": "[CAST(['1.2.3.4', NULL] AS Array(Nullable(IPv4)))]",
    "array_datetime": "[[toDateTime('2024-01-02 03:04:05')]]",
    "map_ipv4": "[map('a', toIPv4('1.2.3.4'), 'b', toIPv4('5.6.7.8')), map()]",
    "map_array_uuid": f"[map('k', [toUUID('{_UUID}')])]",
    "enum8": "CAST(['a', 'b'] AS Array(Enum8('a' = 1, 'b' = -2)))",
    "enum16": "CAST(['x', NULL] AS Array(Nullable(Enum16('x' = -300, 'y' = 2))))",
    "int128": "[toInt128('-170141183460469231731687303715884105728'), toInt128(5)]",
    "uint256": f"[toUInt256('{2**255 - 1}'), toUInt256(0)]",
    "tuple": "[tuple(toIPv4('1.2.3.4'), 'x')]",
    "date": "CAST(['2024-01-02', '1970-01-01', NULL] AS Array(Nullable(Date)))",
    "date32": "[toDate32('1900-01-02')]",
    "low_cardinality": "CAST(['a', 'b'] AS Array(LowCardinality(String)))",
    "fixed_string": "[toFixedString('ab', 3)]",
    "bool": "[true, false]",
}


@pytest.mark.parametrize("values", list(_PARITY.values()), ids=list(_PARITY))
def test_each_value_is_the_row_reads(values):
    sql = f"SELECT arrayJoin({values}) AS v"
    if "generateUUIDv4" in values:  # a fresh UUID per statement: the reads compare one statement
        sql = f"SELECT arrayJoin(CAST([toUUID('{uuid.uuid4()}'), NULL] AS Array(Nullable(UUID)))) AS v"
    assert _typed(_execute(sql).rows) == _typed(_row_read(sql))


def test_a_uint256_beyond_a_signed_256_bit_decimal_is_refused():
    """decimal256 holds a signed 256-bit value; a UInt256 at or above 2**255 has none."""
    with pytest.raises(ValueError, match="UInt256"):
        _execute(f"SELECT toUInt256('{2**255}') AS v")


def test_an_empty_result_of_nested_uuids_is_typed():
    result = _execute(f"SELECT map('k', [toUUID('{_UUID}')]) AS v WHERE 0")
    assert result.rows == []
    assert pa.types.is_map(result.table.column("v").type)


def test_a_date_written_as_days_is_a_date():
    """ClickHouse before 26 writes Date to Arrow as uint16 days; 26 does when asked."""
    client = _ChdbClient({"output_format_arrow_date_as_uint16": "1"})
    sql = "SELECT arrayJoin(CAST(['2024-01-02', NULL] AS Array(Nullable(Date)))) AS v"
    result = _execute(sql, client)
    assert _typed(result.rows) == _typed(_row_read(sql))
    assert result.table.column("v").type == pa.date32()


@pytest.mark.parametrize(
    ("expr", "zone", "wall", "unit"),
    [
        (
            "toDateTime('2024-01-02 03:04:05', 'Asia/Tokyo')",
            "Asia/Tokyo",
            (2024, 1, 2, 3, 4, 5, 0),
            "s",
        ),
        (
            "toDateTime64('2024-07-02 03:04:05.123456', 6, 'America/New_York')",
            "America/New_York",
            (2024, 7, 2, 3, 4, 5, 123456),
            "us",
        ),
        (
            "toDateTime64('2024-01-02 03:04:05.1234567', 7, 'Asia/Kolkata')",
            "Asia/Kolkata",
            (2024, 1, 2, 3, 4, 5, 123456),
            "ns",
        ),
    ],
)
def test_a_datetime_in_its_own_zone_is_that_zones(expr, zone, wall, unit):
    """clickhouse-connect gives a column's own zone (chdb's Native header drops it, so the row
    read's value is written out: the wall time in that zone, at its offset)."""
    result = _execute(f"SELECT {expr} AS v")
    expected = datetime(*wall, tzinfo=ZoneInfo(zone))
    (value,) = result.rows[0]
    assert value == expected and value.utcoffset() == expected.utcoffset()
    # The unit of the server's type; the row holds the microsecond, a finer value floored.
    assert result.table.column("v").type == pa.timestamp(unit, tz=zone)


def test_a_naive_datetime_is_utc_wall_time_in_arrow():
    result = _execute("SELECT toDateTime64('2024-01-02 03:04:05.5', 1) AS a, toDateTime(0) AS b")
    assert result.table.schema.types == [pa.timestamp("ms"), pa.timestamp("s")]
    assert result.rows == [(datetime(2024, 1, 2, 3, 4, 5, 500000), datetime(1970, 1, 1))]


def test_an_address_or_uuid_in_arrow_is_the_text_postgresql_writes():
    """An Arrow reader (an encoder) reads the text the row result's address gives
    (``text_of``); the rows hold the address itself."""
    sql = (
        "SELECT toIPv4('1.2.3.4') AS a, toIPv6('::ffff:1.2.3.4') AS b, toIPv6('2001:db8::1') AS c,"
        f" toUUID('{_UUID}') AS u"
    )
    result = _execute(sql)
    (row,) = _row_read(sql)
    assert [column_values(c)[0] for c in result.table.columns] == [text_of(v, None) for v in row]
    assert result.rows == [row]
    assert isinstance(result.rows[0][0], ipaddress.IPv4Address)


# -- A whole read and a streamed read give the same values ---------------------------------------


class _StreamContext:
    """clickhouse-connect's StreamContext over ClickHouse's ArrowStream output: ``gen`` is
    pyarrow's stream reader, iterated a record batch at a time."""

    def __init__(self, raw: bytes) -> None:
        self.gen = pa.ipc.open_stream(io.BytesIO(raw))

    def __enter__(self) -> _StreamContext:
        return self

    def __exit__(self, *_exc: object) -> None:
        pass

    def __iter__(self):
        return iter(self.gen)


class _ChdbStreamClient(_ChdbClient):
    def query_arrow_stream(self, sql, parameters=None, use_strings=None, settings=None):
        self.calls.append(("stream", sql, {"use_strings": use_strings, "settings": settings}))
        pinned = {k: v for k, v in (settings or {}).items() if k != "query_id"}
        return _StreamContext(_chdb(sql, "ArrowStream", {**self.server_settings, **pinned}))


_AMBIGUOUS = (
    "SELECT toUInt32(number) AS n,"
    " if(number % 3 = 0, NULL, toIPv4(toUInt32(number) * 16777619)) AS ip,"
    " toDateTime(1700000000 + number * 3601) AS naive,"
    " toDateTime(1700000000 + number * 3601, 'Asia/Tokyo') AS tokyo,"
    " CAST(if(number % 2 = 0, 'a', 'b') AS Enum8('a' = 1, 'b' = -2)) AS e,"
    " if(number % 4 = 0, NULL, reinterpretAsUUID(reverse(unhex(lpad(hex(number * 2654435761), 32, '0'))))) AS id"
    " FROM numbers(7)"
)


def test_a_streamed_read_gives_what_a_whole_read_gives():
    # Blocks of 2 rows: the stream answers in four batches, the whole read in one table.
    whole_client = _ChdbClient({"max_block_size": "2"})
    whole = _execute(_AMBIGUOUS, whole_client)

    stream_client = _ChdbStreamClient({"max_block_size": "2"})

    async def read_stream() -> tuple[Any, list[pa.RecordBatch]]:
        stream = await _driver(stream_client).open_stream(_AMBIGUOUS)
        batches = []
        while (batch := await stream.fetch_arrow()) is not None:
            batches.append(batch)
        return stream, batches

    stream, batches = asyncio.run(read_stream())
    assert len(batches) == 4
    streamed = pa.Table.from_batches(batches, schema=stream.schema)

    assert stream.schema == whole.table.schema
    assert streamed.equals(whole.table)
    # The schema was resolved once, before the first batch: one DESCRIBE.
    assert [kind for kind, *_ in stream_client.calls] == ["stream", "query"]
    assert stream_client.calls[1][1] == f"DESCRIBE ({_AMBIGUOUS})"

    # The row face of the stream gives the whole read's rows, typed alike.
    async def stream_rows() -> list[tuple]:
        stream = await _driver(_ChdbStreamClient({"max_block_size": "2"})).open_stream(_AMBIGUOUS)
        return await stream.fetch(100)

    assert _typed(asyncio.run(stream_rows())) == _typed(whole.rows)
    assert isinstance(whole.rows[1][1], ipaddress.IPv4Address)
    assert isinstance(whole.rows[1][5], uuid.UUID)
    assert whole.rows[1][4] == "b"


# -- Every read path converts alike --------------------------------------------------------------

_TEMPORALS = (
    "SELECT toDateTime(1700000000 + number) AS dt,"
    " toDateTime64(1700000000.5 + number, 0) AS dt64_0,"
    " toDateTime64(1700000000.123 + number, 3, 'Europe/Berlin') AS dt64_3,"
    " toDateTime64(1700000000.123456 + number, 6) AS dt64_6,"
    " toDateTime64('1960-05-06 07:08:09.123456789', 9) AS dt64_9,"
    " toIPv4(toUInt32(number)) AS ip"
    " FROM numbers(5)"
)


def test_a_datetime_is_the_unit_of_the_servers_type():
    """DateTime is seconds; DateTime64(s) is the unit ClickHouse's Arrow output writes that scale
    in. The row holds the microsecond, a finer value floored, as the row read holds it."""
    result = _execute(_TEMPORALS)
    assert result.table.schema.types[:5] == [
        pa.timestamp("s"),
        pa.timestamp("s"),
        pa.timestamp("ms", tz="Europe/Berlin"),
        pa.timestamp("us"),
        pa.timestamp("ns"),
    ]
    assert _typed(result.rows) == _typed(_row_read(_TEMPORALS))


def test_a_keyed_fetch_and_a_replica_stream_convert_as_execute_does():
    """execute_arrow (a keyed row-materialize fetch) and iter_arrow_batches (a replica stream)
    hold the same table execute does: one conversion for every read path (REQ-2194)."""
    whole = _execute(_TEMPORALS).table

    fetched = asyncio.run(_driver(_ChdbClient()).execute_arrow(_TEMPORALS))
    assert fetched.equals(whole)

    stream_client = _ChdbStreamClient({"max_block_size": "2"})
    batches = list(_driver(stream_client).iter_arrow_batches(_TEMPORALS))
    assert len(batches) == 3
    assert pa.Table.from_batches(batches).equals(whole)
