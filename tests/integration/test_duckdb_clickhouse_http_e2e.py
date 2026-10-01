# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e7d34-91c2-4a8f-b6d3-2e4f8a1c9d57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the DuckDB engine reads a live ClickHouse table over ClickHouse's HTTP interface
(REQ-899, amended).

Drives DuckDBFederationRuntime directly — attach_source + every execution path — against a real
ClickHouse server self-provisioned by the ``requires_clickhouse`` marker (docker-compose.test.yml,
unique per-run port). Proves: the column types survive the Parquet hop, the statement's projection
and literal predicates (bound $N included) are the query ClickHouse actually ran (its own
system.query_log), credentials authenticate via the http secret, a bad credential raises, and the
request deadline bounds an unpredicated download.
"""

from __future__ import annotations

import datetime
import decimal
import os
import time
import uuid
from types import SimpleNamespace

import duckdb
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_clickhouse]

clickhouse_connect = pytest.importorskip("clickhouse_connect")

from provisa.core import request_deadline  # noqa: E402
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime  # noqa: E402

_TABLE = "itest_duckdb_http_events"
_PHYS = f'"ch_http_src"."default"."{_TABLE}"'
_UUID = uuid.UUID("5b927589-4fa7-49a1-9880-ef2d58461b1d")


def _client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def _source(password: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        id="ch-http-src",
        type=SimpleNamespace(value="clickhouse"),
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "") if password is None else password,
        database=None,
        federation_hints={},
        mapping={},
        base_url=None,
        path=None,
        schema_name="default",
        table_name=_TABLE,
    )


@pytest.fixture
def seeded():
    client = _client()
    client.command(f"DROP TABLE IF EXISTS {_TABLE}")
    client.command(
        f"CREATE TABLE {_TABLE} (order_id UInt64, event_type LowCardinality(String), "
        "customer_id Nullable(Int32), ts DateTime64(3, 'UTC'), amount Decimal(18, 2), "
        "status Enum8('new' = 1, 'done' = 2), event_uuid UUID, tags Array(String), ip IPv4, "
        "day Date) ENGINE = MergeTree ORDER BY order_id"
    )
    client.command(
        f"INSERT INTO {_TABLE} SELECT number, if(number % 2 = 0, 'shipped', 'returned'), "
        "if(number % 3 = 0, NULL, toInt32(number)), toDateTime64('2026-01-02 03:04:05.678', 3, "
        f"'UTC'), number / 4, if(number % 2 = 0, 'new', 'done'), '{_UUID}', ['a', 'b'], "
        "'10.0.0.1', toDate('2026-01-02') FROM numbers(100000)"
    )
    try:
        yield client
    finally:
        client.command(f"DROP TABLE IF EXISTS {_TABLE}")
        client.close()


def _query_log(client, marker: str) -> list[tuple]:
    client.command("SYSTEM FLUSH LOGS")
    return client.query(
        "SELECT http_method, read_rows, query FROM system.query_log "
        "WHERE type = 'QueryFinish' AND query_id LIKE 'provisa-duckdb-%' "
        f"AND query LIKE '%{marker}%' AND query NOT LIKE '%system.query_log%' "
        "ORDER BY event_time_microseconds"
    ).result_rows


@pytest.mark.asyncio
async def test_live_read_types_projection_and_pushdown(seeded):
    rt = DuckDBFederationRuntime()
    try:
        rt.attach_source(_source())
        cols = rt.introspect_columns(_source())
        assert cols == {
            "order_id": "ubigint",
            "event_type": "varchar",
            "customer_id": "integer",
            "ts": "timestamp with time zone",
            "amount": "decimal(18,2)",
            "status": "varchar",
            "event_uuid": "uuid",
            "tags": "varchar[]",
            "ip": "varchar",
            "day": "date",
        }
        res = await rt.run(
            "SELECT e.order_id, e.event_type, e.customer_id, e.amount, e.status, e.event_uuid, "
            f"e.tags, e.ip, e.day, e.ts FROM {_PHYS} AS e "
            "WHERE e.order_id IN ($1, 4, 7) AND e.event_type = 'shipped' ORDER BY e.order_id",
            [2],
        )
        assert res.rows == [
            (
                2,
                "shipped",
                2,
                decimal.Decimal("0.50"),
                "new",
                _UUID,
                ["a", "b"],
                "10.0.0.1",
                datetime.date(2026, 1, 2),
                datetime.datetime(2026, 1, 2, 3, 4, 5, 678000, tzinfo=datetime.UTC),
            ),
            (
                4,
                "shipped",
                4,
                decimal.Decimal("1.00"),
                "new",
                _UUID,
                ["a", "b"],
                "10.0.0.1",
                datetime.date(2026, 1, 2),
                datetime.datetime(2026, 1, 2, 3, 4, 5, 678000, tzinfo=datetime.UTC),
            ),
        ]
        log = _query_log(seeded, '"order_id" IN (2, 4, 7)')
        assert len(log) == 1, log  # one GET — no HEAD re-execution
        method, read_rows, query = log[0]
        assert method == 1  # HTTP GET
        assert 'WHERE "order_id" IN (2, 4, 7) AND "event_type" = \'shipped\'' in query
        assert read_rows < 100000  # ClickHouse's primary key index honored the pushed predicate
        # The credential rides in the http secret's headers — never in the URL ClickHouse logged.
        password = os.environ["CLICKHOUSE_PASSWORD"]
        assert password not in query

        table = rt.run_arrow(f"SELECT count(*) AS n FROM {_PHYS} WHERE status = 'done'")
        assert table.column("n").to_pylist() == [50000]
        schema, batches = rt.run_arrow_stream(f"SELECT order_id FROM {_PHYS} WHERE order_id < 10")
        assert sum(b.num_rows for b in batches) == 10
        shape = rt.describe_sync(
            f"SELECT order_id, event_uuid FROM {_PHYS} WHERE order_id = $1", [1]
        )
        assert shape.column_types == ["UBIGINT", "UUID"]
        stream = rt.run_sync(f"SELECT day FROM {_PHYS} WHERE day = '2026-01-02' AND order_id < 3")
        assert stream.rows == [(datetime.date(2026, 1, 2),)] * 3
    finally:
        rt.close()


def test_bad_credentials_raise(seeded):
    rt = DuckDBFederationRuntime()
    try:
        with pytest.raises(duckdb.Error):
            rt.attach_source(_source(password="wrong-password"))
    finally:
        rt.close()


def test_request_deadline_bounds_an_unpredicated_download(seeded):
    """ClickHouse HTTP is not range-streamable, so the whole result downloads and DuckDB's interrupt
    cannot stop it; the request budget, sent as ClickHouse's max_execution_time, ends it — no row
    cap. A ClickHouse view that sleeps per block stands in for a table too large to finish."""
    seeded.command("DROP VIEW IF EXISTS itest_duckdb_http_slow")
    seeded.command(
        "CREATE VIEW itest_duckdb_http_slow AS SELECT number AS order_id FROM numbers(1000000) "
        "WHERE sleepEachRow(0.0001) = 0 SETTINGS max_block_size = 1000"
    )
    slow = _source()
    slow.table_name = "itest_duckdb_http_slow"
    rt = DuckDBFederationRuntime()
    try:
        rt.attach_source(slow)
        started = time.monotonic()
        with request_deadline.within(2.0):
            with pytest.raises(TimeoutError):
                rt.run_sync('SELECT count(*) FROM "ch_http_src"."default"."itest_duckdb_http_slow"')
        assert time.monotonic() - started < 10
    finally:
        rt.close()
        seeded.command("DROP VIEW IF EXISTS itest_duckdb_http_slow")
