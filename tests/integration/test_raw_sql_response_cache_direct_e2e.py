# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4d6e28-1c7b-4f53-b2e0-8d5f3a9c1e67
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: DIRECT-route streams and pgwire's Postgres passthrough read and write the raw-SQL
response cache (REQ-1897, #127).

A real isolated server (DuckDB engine; pgwire + Arrow Flight; the stack's Redis as the response
cache) over the stack's real Postgres source — a single-source statement takes the DIRECT route:

* this deployment runs under uvloop (main.py, REQ-1867), and pgwire's REQ-1863 raw-socket
  passthrough engages there too — so an extended-protocol client (asyncpg) reading binary gets the
  source's raw DataRows, cached undecoded as ``pg_datarows`` and replayed on the HIT;
* a simple-protocol client (psycopg2) gets the decoded DIRECT stream, cached as ``rows``;
* Flight SQL's DIRECT stream is cached as ``rows`` and served through the same rows->Arrow adapter.

A HIT is proven, not inferred: the entry the MISS wrote is located in Redis and trimmed to its
second row; a read that returns exactly that one row was served from the cache. The server's
large-result threshold is 10 rows, so a 20-row read streams in full and writes nothing.
"""

from __future__ import annotations

import asyncio
import decimal
import json
import os

import pytest

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_ORG = "raw_sql_cache_direct_e2e"
_THRESHOLD = 10


@pytest.fixture(scope="module")
def server():
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    prior = os.environ.get("PROVISA_REDIRECT_THRESHOLD")
    os.environ["PROVISA_REDIRECT_THRESHOLD"] = str(_THRESHOLD)  # the cache bound (REQ-1224 line)
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        enable_pgwire=True,
        await_flight=True,
        config="tests/fixtures/sample_config.yaml",
        control_plane="postgres",
    )
    try:
        srv.start()
        yield srv
    finally:
        srv.stop_process()
        if prior is None:
            os.environ.pop("PROVISA_REDIRECT_THRESHOLD", None)
        else:
            os.environ["PROVISA_REDIRECT_THRESHOLD"] = prior
        asyncio.run(drop_org_schema(_ORG))


def _sql(order: str, limit: int = 3) -> str:
    # Distinct statement text per test so each starts from its own MISS. REQ-544 (amended
    # 2026-09-30): the response cache is per-request opt-in.
    return (
        "-- @provisa cache=true\n"
        "SELECT o.id, o.amount, o.region FROM sales_analytics.orders o "
        f"ORDER BY {order} LIMIT {limit}"
    )


def _redis():
    import redis

    return redis.Redis.from_url(os.environ["REDIS_URL"])


def _entry_keys(org: str) -> set[bytes]:
    """This test's own entries: the store prefixes every key with the org (REQ-595), so a
    concurrent run in another org — sharing the stack's Redis — never shows up here."""
    return {k for k in _redis().scan_iter(f"provisa:cache:{org}:*") if not k.endswith(b":meta")}


def _new_key(org: str, before: set[bytes]) -> bytes:
    new = _entry_keys(org) - before
    assert len(new) == 1, f"expected the MISS to write exactly one entry, got {new}"
    return new.pop()


def _trim_to_second_row(key: bytes) -> str:
    """Keep only the entry's second row (same kind, same bytes); returns the entry kind."""
    from provisa.cache.codec import decode_cache_payload, encode_cache_payload

    r = _redis()
    payload = decode_cache_payload(r.get(key))
    entry = payload["data"]
    field = {"rows": "rows", "pg_datarows": "datarows"}[entry["kind"]]
    entry[field] = entry[field][1:2]
    r.set(key, encode_cache_payload(payload), keepttl=True)
    return entry["kind"]


async def _asyncpg_rows(srv, sql: str) -> list[tuple]:
    import asyncpg

    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=srv.pgwire_port,
        user=_ROLE,
        password="provisa",
        database="provisa",
        ssl=False,
        statement_cache_size=0,
    )
    try:
        return [tuple(r) for r in await conn.fetch(sql)]
    finally:
        await conn.close()


def _psycopg2_rows(srv, sql: str) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1", port=srv.pgwire_port, dbname="provisa", user=_ROLE, password="provisa"
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def _flight(srv, sql: str):
    import pyarrow.flight as flight

    client = flight.FlightClient(f"grpc://127.0.0.1:{srv.flight_port}")
    table = client.do_get(flight.Ticket(json.dumps({"query": sql, "role": _ROLE}).encode()))
    out = table.read_all()
    client.close()
    return out


def test_extended_protocol_direct_read_under_uvloop_caches_raw_datarows(server):
    """Under uvloop the passthrough engages (REQ-1863): the binary read's raw DataRows are cached
    undecoded as ``pg_datarows`` and the HIT replays them byte-for-byte."""
    sql = _sql("o.id")
    before = _entry_keys(server.org_id)
    miss = asyncio.run(_asyncpg_rows(server, sql))
    assert len(miss) == 3 and isinstance(miss[0][1], decimal.Decimal)
    assert _trim_to_second_row(_new_key(server.org_id, before)) == "pg_datarows"
    assert asyncio.run(_asyncpg_rows(server, sql)) == miss[1:2]


def test_simple_protocol_direct_stream_miss_then_hit(server):
    sql = _sql("o.amount")
    before = _entry_keys(server.org_id)
    miss = _psycopg2_rows(server, sql)
    assert len(miss) == 3 and isinstance(miss[0][1], decimal.Decimal)
    assert _trim_to_second_row(_new_key(server.org_id, before)) == "rows"
    assert _psycopg2_rows(server, sql) == miss[1:2]


def test_flight_direct_stream_miss_then_hit_same_shape(server):
    sql = _sql("o.amount DESC")
    before = _entry_keys(server.org_id)
    miss = _flight(server, sql)
    assert miss.num_rows == 3
    assert _trim_to_second_row(_new_key(server.org_id, before)) == "rows"
    hit = _flight(server, sql)
    assert hit.schema == miss.schema  # served through the same rows->Arrow adapter as the miss
    assert hit.to_pylist() == miss.to_pylist()[1:2]


def test_over_bound_reads_stream_in_full_and_write_nothing(server):
    before = _entry_keys(server.org_id)
    rows = asyncio.run(_asyncpg_rows(server, _sql("o.region, o.id", limit=20)))
    assert len(rows) == 20 > _THRESHOLD
    assert len(_psycopg2_rows(server, _sql("o.region DESC, o.id", limit=20))) == 20
    assert _flight(server, _sql("o.id, o.region", limit=20)).num_rows == 20
    assert _entry_keys(server.org_id) == before


def test_a_direct_hit_decimal_is_exact(server):
    sql = "-- @provisa cache_ttl=120\nSELECT o.amount FROM sales_analytics.orders o WHERE o.id = 1"
    first = _psycopg2_rows(server, sql)
    second = _psycopg2_rows(server, sql)
    assert first == second == [(decimal.Decimal("19.99"),)]
