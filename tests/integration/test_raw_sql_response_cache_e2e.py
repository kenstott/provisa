# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7c5a19-8d4f-4b36-9f02-6c1a3e8d7b54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: raw-SQL surfaces write and read their own response-cache namespace (REQ-1897, #127).

A real isolated server (DuckDB engine, pgwire + Arrow Flight, the stack's Redis as the response
cache) over a SQLite source — a VIRTUAL source, so every statement takes the ENGINE route, the
streaming terminal each transport drains itself. A HIT is proven, not inferred: the entry the MISS
wrote is located in Redis and its rows are replaced with marker rows (same kind, same schema); a
read that returns the marker rows was served from the cache, decoded by that surface's reader.

Covers: pgwire miss -> pgwire hit (same columns and wire types); Flight miss -> pgwire hit and
pgwire miss -> Flight hit with a DECIMAL(18,2) and a TIMESTAMP column exact (``Decimal``, not
float); Flight miss -> Flight hit type-identical; a GraphQL read over the same table and a raw-SQL
read never serve each other's entries.
"""

from __future__ import annotations

import datetime
import decimal
import json
import os
import sqlite3
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_ROLE = "org_admin"
_ORG = "raw_sql_cache_e2e"


_ORIGINAL = [
    (1, decimal.Decimal("0.50"), datetime.datetime(2026, 1, 2, 3, 4, 5)),
    (2, decimal.Decimal("123456789.01"), datetime.datetime(2026, 1, 2, 3, 4, 6)),
]
_MARKER = [(42, decimal.Decimal("4242.42"), datetime.datetime(2031, 1, 1, 1, 1, 1))]


def _seed(db: Path) -> None:
    con = sqlite3.connect(db)
    try:
        con.execute("CREATE TABLE events (id INTEGER, amount TEXT, ts TEXT)")
        con.executemany(
            "INSERT INTO events VALUES (?, ?, ?)",
            [(1, "0.50", "2026-01-02 03:04:05"), (2, "123456789.01", "2026-01-02 03:04:06")],
        )
        con.commit()
    finally:
        con.close()


def _start_server(work: Path, org: str, source_extra: dict, **server_opts):
    from tests.integration.isolated_server import IsolatedServer

    db = work / "events.sqlite"
    _seed(db)
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    # Only this test's SQLite source: the fixture's naming + roles, nothing else of its catalog.
    cfg: dict = {"naming": base["naming"], "roles": base["roles"], "relationships": []}
    cfg["sources"] = [{"id": "rc-sqlite", "type": "sqlite", "path": str(db), **source_extra}]
    cfg["domains"] = [{"id": "raw-cache", "description": "raw-SQL cache e2e"}]
    cfg["tables"] = [
        {
            "source_id": "rc-sqlite",
            "domain_id": "raw-cache",
            "schema": "default",
            "table": "events",
            "columns": [
                {"name": n, "data_type": t, "visible_to": [_ROLE]}
                for n, t in (("id", "integer"), ("amount", "varchar"), ("ts", "varchar"))
            ],
        }
    ]
    cfg_path = work / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    srv = IsolatedServer(
        org,
        engine="duckdb",
        enable_pgwire=True,
        await_flight=True,
        config=str(cfg_path),
        control_plane="postgres",
        **server_opts,
    )
    srv.start()
    srv.db_path = db  # type: ignore[attr-defined]
    return srv


def _stop_server(srv, org: str) -> None:
    import asyncio

    from tests.integration.isolated_server import drop_org_schema

    srv.stop_process()
    asyncio.run(drop_org_schema(org))


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = _start_server(tmp_path_factory.mktemp("rawsqlcache"), _ORG, {})
    try:
        yield srv
    finally:
        _stop_server(srv, _ORG)


@pytest.fixture(scope="module")
def landed_server(tmp_path_factory):
    """The same source, but the operator requires it be read from its landed copy
    (``replicate``), refreshed only on its own hour-long cadence."""
    org = _ORG + "_landed"
    srv = _start_server(
        tmp_path_factory.mktemp("rawsqlcache_landed"),
        org,
        {"replicate": 0, "cache_ttl": 3600},
    )
    try:
        yield srv
    finally:
        _stop_server(srv, org)


def _pgwire(srv, sql: str):
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1", port=srv.pgwire_port, dbname="provisa", user=_ROLE, password="provisa"
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        described = [(d.name, d.type_code) for d in cur.description]
        return rows, described
    finally:
        conn.close()


def _flight(srv, sql: str):
    import pyarrow.flight as flight

    client = flight.FlightClient(f"grpc://127.0.0.1:{srv.flight_port}")
    ticket = flight.Ticket(json.dumps({"query": sql, "role": _ROLE}).encode())
    table = client.do_get(ticket).read_all()
    client.close()
    return table


def _flight_rows(table) -> list[tuple]:
    cols = [c.to_pylist() for c in table.columns]
    return [tuple(r) for r in zip(*cols, strict=True)]


def _redis():
    import redis

    return redis.Redis.from_url(os.environ["REDIS_URL"])


def _entry_keys(org: str) -> set[bytes]:
    """This test's own entries: the store prefixes every key with the org (REQ-595), so a
    concurrent run in another org — sharing the stack's Redis — never shows up here."""
    return {k for k in _redis().scan_iter(f"provisa:cache:{org}:*") if not k.endswith(b":meta")}


def _plant_marker(key: bytes) -> str:
    """Replace the entry's rows with _MARKER (same kind and schema); returns the entry kind."""
    import pyarrow as pa

    from provisa.cache.codec import decode_cache_payload, encode_cache_payload

    r = _redis()
    payload = decode_cache_payload(r.get(key))
    entry = payload["data"]
    if entry["kind"] == "rows":
        entry["rows"] = [list(row) for row in _MARKER]
    else:
        schema = pa.ipc.open_stream(entry["ipc"]).schema
        batch = pa.RecordBatch.from_pylist(
            [dict(zip(schema.names, row, strict=True)) for row in _MARKER], schema=schema
        )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(batch)
        entry["ipc"] = sink.getvalue().to_pybytes()
    r.set(key, encode_cache_payload(payload), keepttl=True)
    return entry["kind"]


def _new_key(org: str, before: set[bytes]) -> bytes:
    new = _entry_keys(org) - before
    assert len(new) == 1, f"expected the MISS to write exactly one entry, got {new}"
    return new.pop()


def _plain_sql(order: str) -> str:
    # Distinct statement text per test (the ORDER BY) so each test starts from its own MISS.
    return (
        "SELECT id, CAST(amount AS DECIMAL(18,2)) AS amount, CAST(ts AS TIMESTAMP) AS ts "
        f"FROM raw_cache.events ORDER BY {order}"
    )


def _sql(order: str) -> str:
    # REQ-544 (amended 2026-09-30): the response cache is per-request opt-in.
    return "-- @provisa cache=true\n" + _plain_sql(order)


def test_pgwire_miss_then_pgwire_hit_returns_the_same_columns_and_types(server):
    sql = _sql("id")
    before = _entry_keys(server.org_id)
    miss_rows, miss_desc = _pgwire(server, sql)
    assert miss_rows == _ORIGINAL
    assert isinstance(miss_rows[1][1], decimal.Decimal)
    assert _plant_marker(_new_key(server.org_id, before)) == "rows"
    hit_rows, hit_desc = _pgwire(server, sql)
    assert hit_rows == _MARKER  # only a cache HIT returns the planted rows
    assert hit_desc == miss_desc


def test_flight_miss_then_pgwire_hit_is_exact(server):
    sql = _sql("id DESC")
    before = _entry_keys(server.org_id)
    miss = _flight(server, sql)
    assert _flight_rows(miss) == list(reversed(_ORIGINAL))
    assert _plant_marker(_new_key(server.org_id, before)) == "arrow_ipc"
    hit_rows, _ = _pgwire(server, sql)
    assert hit_rows == _MARKER
    assert isinstance(hit_rows[0][1], decimal.Decimal)
    assert isinstance(hit_rows[0][2], datetime.datetime)


def test_pgwire_miss_then_flight_hit_is_exact(server):
    import pyarrow as pa

    sql = _sql("amount DESC")
    before = _entry_keys(server.org_id)
    miss_rows, _ = _pgwire(server, sql)
    assert miss_rows == list(reversed(_ORIGINAL))
    assert _plant_marker(_new_key(server.org_id, before)) == "rows"
    hit = _flight(server, sql)
    assert hit.schema.field("amount").type == pa.decimal128(18, 2)
    assert _flight_rows(hit) == _MARKER  # Decimal and datetime, exact


def test_flight_miss_then_flight_hit_is_type_identical(server):
    sql = _sql("ts DESC")
    before = _entry_keys(server.org_id)
    miss = _flight(server, sql)
    assert _flight_rows(miss) == list(reversed(_ORIGINAL))
    assert _plant_marker(_new_key(server.org_id, before)) == "arrow_ipc"
    hit = _flight(server, sql)
    assert hit.schema == miss.schema
    assert _flight_rows(hit) == _MARKER


def test_graphql_and_raw_sql_never_serve_each_others_entries(server):
    import httpx

    gql = {"query": "query @cached { rc__events { id } }", "role": _ROLE}
    headers = {"X-Provisa-Role": _ROLE}
    first = httpx.post(f"{server.base_url}/data/graphql", json=gql, headers=headers, timeout=60)
    assert first.status_code == 200, first.text
    assert first.json() == {"data": {"rc__events": [{"id": 1}, {"id": 2}]}}
    rows, _ = _pgwire(server, _sql("amount"))  # a raw-SQL entry over the same table
    assert rows == _ORIGINAL
    again = httpx.post(f"{server.base_url}/data/graphql", json=gql, headers=headers, timeout=60)
    assert again.headers["X-Provisa-Cache"] == "HIT"
    assert again.json() == first.json()  # the GraphQL shape, never a raw-SQL rows entry


def test_without_the_hint_pgwire_and_flight_never_touch_the_cache(server):
    """REQ-544 (amended 2026-09-30): no `-- @provisa cache` comment, no read and no write."""
    before = _entry_keys(server.org_id)
    sql = _plain_sql("id, amount")
    assert _pgwire(server, sql)[0] == _ORIGINAL
    assert _pgwire(server, sql)[0] == _ORIGINAL
    assert _flight_rows(_flight(server, sql)) == _ORIGINAL
    assert _entry_keys(server.org_id) == before


def test_a_hinted_read_of_a_landed_table_is_the_operators_snapshot(landed_server):
    """The hint only accepts staler data: a hinted read of a landed table reads the operator's
    landed copy (never the live source), exactly as an unhinted one does — no per-request way to
    read fresher than the operator allows."""
    srv = landed_server
    assert _pgwire(srv, _sql("ts, id"))[0] == _ORIGINAL  # lands the snapshot
    con = sqlite3.connect(srv.db_path)
    try:
        con.execute("DELETE FROM events")
        con.execute("INSERT INTO events VALUES (99, '9.99', '2030-01-01 00:00:00')")
        con.commit()
    finally:
        con.close()
    hinted = _pgwire(srv, _sql("ts DESC, id"))[0]  # new text: a response-cache MISS
    unhinted = _pgwire(srv, _plain_sql("ts DESC, id"))[0]
    assert hinted == unhinted == list(reversed(_ORIGINAL))  # the landed snapshot, not live
