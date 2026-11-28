# Copyright (c) 2026 Kenneth Stott
# Canary: 0175312f-2c22-497a-aac5-d94e44e4d0cd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: ClickHouse as a first-class NAMED SOURCE (REQ-986), reachable on ANY engine.

Registers ClickHouse through the REAL pool path (``SourcePool.add`` → registry ``create_driver`` →
``configure`` → ``connect`` over clickhouse-connect HTTP → ``execute``) and reads a live table — the
same client family the ClickHouse federation engine uses. The ``requires_clickhouse_26`` marker
makes ``tests/conftest.py`` provision a ClickHouse 26 server (docker-compose.test.yml
``clickhouse-26``) per-test and export CLICKHOUSE_26_PORT, so this runs on every isolated run — no
external creds. ClickHouse-native Arrow reads for the embedded (chdb) engine are covered in
tests/unit/test_native_arrow_transport.py.

REQ-2194: the source's results are read as Arrow, with the server's types, and a server older
than 26 (the ``clickhouse-24`` service, 24.3, ``requires_clickhouse_24``) is refused at connect by
name.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("clickhouse_connect", reason="clickhouse-connect required")

_HOST = os.environ.get("CLICKHOUSE_HOST")

from provisa.executor.pool import SourcePool  # noqa: E402

_SID = "ch_src_e2e"
_TABLE = "provisa_src_e2e_widgets"


def _port() -> int:
    return int(os.environ["CLICKHOUSE_26_PORT"])


@pytest.fixture
def seeded():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=_HOST or "localhost",
        port=_port(),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    client.command(f"DROP TABLE IF EXISTS {_TABLE}")
    client.command(f"CREATE TABLE {_TABLE} (id Int64, name String) ENGINE = MergeTree ORDER BY id")
    client.command(f"INSERT INTO {_TABLE} VALUES (1,'a'),(2,'b'),(3,'c')")
    try:
        yield _TABLE
    finally:
        client.command(f"DROP TABLE IF EXISTS {_TABLE}")
        client.close()


@pytest.mark.requires_clickhouse_26
@pytest.mark.asyncio
async def test_clickhouse_named_source_reads_through_source_pool(seeded):
    pool = SourcePool()
    await pool.add(
        source_id=_SID,
        source_type="clickhouse",
        host=_HOST or "localhost",
        port=_port(),
        database="default",
        user=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    assert pool.has(_SID)
    assert pool.dialect_for(_SID) == "clickhouse"
    driver = pool.get(_SID)
    result = await driver.execute(f"SELECT id, name FROM {seeded} ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
    await driver.close()


@pytest.mark.requires_clickhouse_26
def test_concurrent_requests_on_one_worker_each_get_their_own_session(seeded):
    """REQ-1882: every request runs on its own thread and a connection is used by one request at a
    time. The driver held ONE clickhouse-connect client — one ClickHouse session — for the whole
    worker, so a second in-flight request failed with "Attempt to execute concurrent queries
    within the same session". Eight requests in flight at once on one driver must all succeed."""
    import asyncio
    import threading

    from provisa.core.connection_loop import connection_loop

    pool = SourcePool()
    asyncio.run(
        pool.add(
            source_id=_SID,
            source_type="clickhouse",
            host=_HOST or "localhost",
            port=_port(),
            database="default",
            user=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
            max_size=4,
        )
    )
    n = 8
    start = threading.Barrier(n)
    results: list = [None] * n
    errors: list[BaseException] = []

    def _request(i: int) -> None:
        try:
            start.wait(timeout=30)
            with connection_loop() as cl:
                # sleep() keeps each statement in flight long enough to overlap the others
                results[i] = cl.run(
                    pool.execute(_SID, f"SELECT count() AS n, sleep(0.5) AS s FROM {seeded}")
                )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_request, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    asyncio.run(pool.close_all())
    assert not errors, errors[:2]
    assert [r.rows[0][0] for r in results] == [3] * n


_TYPED_SQL = (
    "SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS id,"
    " toDecimal64('12345.6789', 4) AS amount,"
    " toDateTime64('2024-01-01 12:00:00.123', 3, 'Europe/Berlin') AS at,"
    " ['a', 'b'] AS tags"
)


async def _driver(port: int):
    from provisa.executor.drivers.clickhouse import ClickHouseDriver

    driver = ClickHouseDriver()
    await driver.connect(
        _HOST or "localhost",
        port,
        "default",
        os.environ.get("CLICKHOUSE_USER", "default"),
        os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    return driver


@pytest.mark.requires_clickhouse_26
@pytest.mark.asyncio
async def test_a_result_is_read_as_arrow_with_the_servers_types():
    """REQ-2194: Decimal, DateTime64 in a zone, an Array and a UUID arrive as the server's Arrow."""
    import uuid
    from datetime import datetime
    from decimal import Decimal
    from zoneinfo import ZoneInfo

    import pyarrow as pa

    from provisa.executor.result import ArrowResult

    driver = await _driver(_port())
    try:
        result = await driver.execute(_TYPED_SQL)
        assert isinstance(result, ArrowResult)
        schema = result.table.schema
        assert pa.types.is_decimal(schema.field("amount").type)
        assert schema.field("at").type == pa.timestamp("ms", tz="Europe/Berlin")
        assert pa.types.is_list(schema.field("tags").type)
        assert result.rows == [
            (
                uuid.UUID("61f0c404-5cb3-11e7-907b-a6006ad3dba0"),
                Decimal("12345.6789"),
                datetime(2024, 1, 1, 12, 0, 0, 123000, tzinfo=ZoneInfo("Europe/Berlin")),
                ["a", "b"],
            )
        ]

        empty = await driver.execute(f"SELECT * FROM ({_TYPED_SQL}) WHERE 0")
        assert empty.column_names == ["id", "amount", "at", "tags"]
        assert empty.rows == []

        stream = await driver.open_stream("SELECT number AS n FROM numbers(5)")
        try:
            batch = await stream.fetch_arrow()
            assert stream.column_names == ["n"]
            assert batch.column(0).to_pylist() == [0, 1, 2, 3, 4]
        finally:
            await stream.close()
    finally:
        await driver.close()


@pytest.mark.requires_clickhouse_24
@pytest.mark.asyncio
async def test_a_server_older_than_26_is_refused_at_connect_by_name():
    """The ``clickhouse-24`` test service is 24.3: the DIRECT driver refuses it at connect."""
    from provisa.executor.drivers.clickhouse import ClickHouseServerTooOld

    with pytest.raises(ClickHouseServerTooOld) as refused:
        await _driver(int(os.environ["CLICKHOUSE_24_PORT"]))
    assert refused.value.version.startswith("24.3.")
    assert "26" in str(refused.value)


@pytest.mark.requires_clickhouse_26
@pytest.mark.asyncio
async def test_nonutf8_string_direct_reads_as_bytes_not_text():
    """REQ-2194: a ClickHouse String that is not UTF-8 reads as bytes on the DIRECT path,
    matching what the ENGINE path delivers via bytes_where_not_text. The raw bytes of e.g.
    int4send(255) are \\x00\\x00\\x00\\xff — not valid UTF-8 text."""
    _TABLE_NUB = "provisa_nonutf8_bytes_test"
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=_HOST or "localhost",
        port=_port(),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    try:
        client.command(f"DROP TABLE IF EXISTS {_TABLE_NUB}")
        client.command(
            f"CREATE TABLE {_TABLE_NUB} (id Int64, raw String, name String)"
            " ENGINE = MergeTree ORDER BY id"
        )
        # Insert a row whose 'raw' column holds the bytes of int4send(255): 00 00 00 ff.
        # clickhouse-connect inserts Python bytes as a ClickHouse String's raw bytes.
        client.insert(
            _TABLE_NUB,
            [[1, b"\x00\x00\x00\xff", "hello"]],
            column_names=["id", "raw", "name"],
        )
        driver = await _driver(_port())
        try:
            result = await driver.execute(f"SELECT id, raw, name FROM {_TABLE_NUB} ORDER BY id")
            assert result.column_names == ["id", "raw", "name"]
            rows = result.rows
            assert len(rows) == 1
            _id, raw, name = rows[0]
            assert _id == 1
            # The non-UTF-8 column arrives as bytes, not garbled text.
            assert raw == b"\x00\x00\x00\xff", f"expected bytes, got {raw!r}"
            # The valid-UTF-8 column stays as text.
            assert name == "hello"
        finally:
            await driver.close()
    finally:
        client.command(f"DROP TABLE IF EXISTS {_TABLE_NUB}")
        client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])


# A row of each type whose Arrow form is not the row read's value (REQ-2194). UUID is read as
# Arrow from ClickHouse 26 on, the oldest server the driver accepts.
_PARITY_SQL = """
SELECT
    toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS id,
    CAST(NULL AS Nullable(UUID)) AS id_null,
    [toUUID('00000000-0000-0000-0000-000000000001')] AS ids,
    toIPv4('1.2.3.4') AS ipv4,
    CAST(NULL AS Nullable(IPv4)) AS ipv4_null,
    toIPv6('2001:db8::1') AS ipv6,
    toIPv6('::ffff:1.2.3.4') AS ipv6_mapped,
    toDateTime('2024-01-02 03:04:05') AS dt,
    toDateTime('2024-01-02 03:04:05', 'Asia/Tokyo') AS dt_zoned,
    toDateTime64('2024-01-02 03:04:05.123456', 6) AS dt64,
    toDateTime64('1960-05-06 07:08:09.123456789', 9) AS dt64_ns,
    toDateTime64('2024-07-02 03:04:05.123', 3, 'America/New_York') AS dt64_zoned,
    toDate('2024-01-02') AS d,
    toDecimal64(12.345, 3) AS dec,
    CAST(NULL AS Nullable(Decimal(18, 3))) AS dec_null,
    [toIPv4('5.6.7.8')] AS ipv4s,
    [toDateTime('2024-01-02 03:04:05')] AS dts,
    map('a', toIPv4('1.2.3.4')) AS m,
    CAST('b' AS Enum8('a' = 1, 'b' = 2)) AS e,
    toInt128(-5) AS i128,
    tuple(toIPv4('1.2.3.4'), 'x') AS t
"""


@pytest.mark.requires_clickhouse_26
@pytest.mark.asyncio
async def test_a_result_read_as_arrow_holds_the_row_reads_values(seeded):
    """The driver reads a whole result as ClickHouse's Arrow output; every value equals, in value
    and type, what clickhouse-connect's row read of the same statement gives."""
    import clickhouse_connect

    from provisa.executor.result import ArrowResult

    del seeded
    pool = SourcePool()
    await pool.add(
        source_id=_SID,
        source_type="clickhouse",
        host=_HOST or "localhost",
        port=_port(),
        database="default",
        user=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    driver = pool.get(_SID)
    client = clickhouse_connect.get_client(
        host=_HOST or "localhost",
        port=_port(),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        autogenerate_session_id=False,
    )
    try:
        result = await driver.execute(_PARITY_SQL)
        rows = client.query(_PARITY_SQL)
    finally:
        client.close()
        await driver.close()
    assert isinstance(result, ArrowResult)
    assert result.column_names == list(rows.column_names)
    expected = [tuple(r) for r in rows.result_rows]
    got = result.rows
    for name, g, e in zip(result.column_names, got[0], expected[0], strict=True):
        assert (type(g), g) == (type(e), e), name
        if hasattr(e, "utcoffset"):
            assert g.utcoffset() == e.utcoffset(), name
