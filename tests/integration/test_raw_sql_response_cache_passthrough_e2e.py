# Copyright (c) 2026 Kenneth Stott
# Canary: 5f1c8a37-2e9d-4b64-a0c3-7d6e1b9f2a48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: pgwire's Postgres passthrough reads and writes ``pg_datarows`` entries (REQ-1897).

The shared ``pgwire_pg_backend`` fixture — a real ProvisaServer (stdlib event loops, so REQ-1863's
raw-socket passthrough engages) over a REAL Postgres table, DIRECT route — with the stack's Redis
as the response cache. An extended-protocol client's read forwards the source's raw DataRow bytes;
the cache keeps them undecoded under the client's result format codes and a HIT replays them
byte-for-byte on the same undecoded path. A simple-protocol client takes the decoded DIRECT stream
(``rows``). A HIT is proven by trimming the MISS's Redis entry to its second row and reading back
exactly that row. The large-result threshold is 3 rows, so the table's 4-row read writes nothing.
"""

from __future__ import annotations

import asyncio
import os

import pytest

pytestmark = [pytest.mark.integration]

_THRESHOLD = 3


@pytest.fixture
def backend(pgwire_pg_backend, monkeypatch):
    from provisa.cache.store import RedisCacheStore

    monkeypatch.setenv("PROVISA_REDIRECT_THRESHOLD", str(_THRESHOLD))
    state = pgwire_pg_backend["state"]
    state.response_cache_store = RedisCacheStore(os.environ["REDIS_URL"])
    state.response_cache_default_ttl = 300
    state.source_cache = {}
    state.table_cache = {}
    state.settings_overrides = {}  # no org overrides: the threshold is the env's (REQ-1349)
    return pgwire_pg_backend


def _sql(be: dict, order: str, limit: int | None = 2) -> str:
    tail = f" LIMIT {limit}" if limit is not None else ""
    # REQ-544 (amended 2026-09-30): the response cache is per-request opt-in.
    return (
        "-- @provisa cache=true\n"
        f"SELECT id, amount, region FROM {be['schema']}.{be['table']} ORDER BY {order}{tail}"
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


def _trim_to_second_row(key: bytes) -> dict:
    from provisa.cache.codec import decode_cache_payload, encode_cache_payload

    r = _redis()
    payload = decode_cache_payload(r.get(key))
    entry = payload["data"]
    field = {"rows": "rows", "pg_datarows": "datarows"}[entry["kind"]]
    entry[field] = entry[field][1:2]
    r.set(key, encode_cache_payload(payload), keepttl=True)
    return entry


async def _asyncpg(port: int, sql: str) -> list[tuple]:
    import asyncpg

    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=port,
        user="admin",
        password="x",
        database="provisa",
        ssl=False,
        statement_cache_size=0,
    )
    try:
        return [tuple(r) for r in await conn.fetch(sql)]
    finally:
        await conn.close()


def _psycopg3(port: int, sql: str) -> list[tuple]:
    import psycopg

    with psycopg.connect(
        host="127.0.0.1", port=port, dbname="provisa", user="admin", password="x"
    ) as conn:
        return [tuple(r) for r in conn.execute(sql, prepare=False).fetchall()]


def _psycopg2(port: int, sql: str) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1", port=port, dbname="provisa", user="admin", password="x"
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def test_binary_passthrough_miss_then_byte_exact_replay(backend):
    port, sql = backend["port"], _sql(backend, "id")
    before = _entry_keys(backend["state"].org_id)
    miss = asyncio.run(_asyncpg(port, sql))
    assert miss == backend["rows"][:2]
    entry = _trim_to_second_row(_new_key(backend["state"].org_id, before))
    assert entry["kind"] == "pg_datarows"
    assert all(f == 1 for f in entry["wire_formats"])  # asyncpg reads binary
    assert asyncio.run(_asyncpg(port, sql)) == backend["rows"][1:2]


def test_binary_bytes_are_never_replayed_to_a_client_that_asked_for_other_formats(backend):
    """psycopg 3 binds with no result format codes (all text), so its read takes the decoded
    path — a pg_datarows entry written for asyncpg's binary codes is a different key, never served
    to it; it gets (and then hits) its own decoded entry."""
    port, sql = backend["port"], _sql(backend, "id DESC")
    before = _entry_keys(backend["state"].org_id)
    binary = asyncio.run(_asyncpg(port, sql))
    assert _entry_keys(backend["state"].org_id) - before  # the binary pg_datarows entry
    before = _entry_keys(backend["state"].org_id)
    text = _psycopg3(port, sql)
    assert text == binary
    assert _trim_to_second_row(_new_key(backend["state"].org_id, before))["kind"] == "rows"
    assert _psycopg3(port, sql) == text[1:2]
    assert asyncio.run(_asyncpg(port, sql)) == binary  # its own entry, untouched


def test_simple_protocol_read_caches_decoded_rows(backend):
    port, sql = backend["port"], _sql(backend, "amount")
    before = _entry_keys(backend["state"].org_id)
    miss = _psycopg2(port, sql)
    assert _trim_to_second_row(_new_key(backend["state"].org_id, before))["kind"] == "rows"
    assert _psycopg2(port, sql) == miss[1:2]


def test_over_bound_reads_write_nothing(backend):
    port = backend["port"]
    before = _entry_keys(backend["state"].org_id)
    assert len(asyncio.run(_asyncpg(port, _sql(backend, "region, id", limit=None)))) == 4
    assert len(_psycopg3(port, _sql(backend, "region DESC, id", limit=None))) == 4
    assert len(_psycopg2(port, _sql(backend, "id, region", limit=None))) == 4
    assert _entry_keys(backend["state"].org_id) == before
