# Copyright (c) 2026 Kenneth Stott
# Canary: 7d4a2c91-5e38-4f06-b1a9-8c3e6f2d0b57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: temporal columns read and filter as ISO 8601 on every surface, whatever their
physical storage (REQ-1908).

A real server (``uvicorn main:app``, DuckDB engine, SQLite control plane) over the test stack's real
Postgres. ``ptepoch.events`` stores ``happened`` as epoch milliseconds (registered timestamptz),
``logged`` as epoch seconds (registered timestamp) and ``seen`` as a native timestamptz. GraphQL
returns ISO 8601 strings and accepts ISO 8601 filter operands and mutation values; pgwire returns
temporals and accepts ISO 8601 text in a WHERE; a write stores the epoch number at the source.
"""

# Requirements: REQ-1908

from __future__ import annotations

import asyncio
import datetime
import json
import os
import urllib.error
import urllib.request

import pytest

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_SCHEMA = "ptepoch"
_UTC = datetime.timezone.utc
# id 1: 2026-01-02T03:04:05.123Z; id 2: 2026-03-04T05:06:07.000Z
_ROWS = [
    (1, 1767323045123, 1767323045, datetime.datetime(2026, 1, 2, 3, 4, 5, 123000, tzinfo=_UTC)),
    (2, 1772600767000, 1772600767, datetime.datetime(2026, 3, 4, 5, 6, 7, tzinfo=_UTC)),
]


def _source_dsn() -> str:
    return (
        f"postgresql://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@localhost:"
        f"{os.environ.get('PG_PORT', '5432')}/provisa"
    )


async def _source(sql: str, *args) -> list[tuple]:
    import asyncpg

    conn = await asyncpg.connect(_source_dsn())
    try:
        return [tuple(r) for r in await conn.fetch(sql, *args)]
    finally:
        await conn.close()


async def _seed() -> None:
    import asyncpg

    conn = await asyncpg.connect(_source_dsn())
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        await conn.execute(
            f"CREATE TABLE {_SCHEMA}.events "
            "(id int PRIMARY KEY, happened bigint, logged integer, seen timestamptz)"
        )
        for row in _ROWS:
            await conn.execute(f"INSERT INTO {_SCHEMA}.events VALUES ($1, $2, $3, $4)", *row)
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def server():
    from tests.integration.isolated_server import IsolatedServer

    asyncio.run(_seed())
    srv = IsolatedServer(
        "ptepoch_e2e",
        engine="duckdb",
        enable_pgwire=True,
        config="tests/fixtures/pg_epoch_temporal_config.yaml",
        control_plane="sqlite",
    )
    try:
        srv.start()
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(_source(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))


def _graphql(srv, query: str) -> dict:
    req = urllib.request.Request(
        f"{srv.base_url}/data/graphql",
        data=json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json", "X-Provisa-Role": _ROLE},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise AssertionError(
            f"HTTP {exc.code}: {exc.read().decode(errors='replace')}\n"
            f"{srv.dump_stderr_debug()[-4000:]}"
        ) from exc
    assert "errors" not in payload, payload
    return payload["data"]


async def _pgwire(srv, sql: str) -> list[tuple]:
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


def test_graphql_reads_every_temporal_as_iso_8601(server):
    rows = _graphql(server, "{ events(order_by: {id: asc}) { id happened logged seen } }")["events"]
    assert rows[0] == {
        "id": 1,
        "happened": "2026-01-02T03:04:05.123000+00:00",
        "logged": "2026-01-02T03:04:05",
        "seen": "2026-01-02T03:04:05.123000+00:00",
    }
    assert rows[1]["happened"] == "2026-03-04T05:06:07+00:00"


@pytest.mark.parametrize("field", ["happened", "logged", "seen"])
def test_graphql_filters_take_iso_8601_operands(field, server):
    data = _graphql(
        server, f'{{ events(where: {{{field}: {{gte: "2026-02-01T00:00:00Z"}}}}) {{ id }} }}'
    )
    assert [r["id"] for r in data["events"]] == [2]


def test_pgwire_reads_temporals_and_filters_on_iso_text(server):
    rows = asyncio.run(
        _pgwire(
            server,
            f"SELECT id, happened, logged FROM {_SCHEMA}.events "
            "WHERE happened >= '2026-02-01T00:00:00Z' ORDER BY id",
        )
    )
    assert rows == [
        (
            2,
            datetime.datetime(2026, 3, 4, 5, 6, 7, tzinfo=_UTC),
            datetime.datetime(2026, 3, 4, 5, 6, 7),
        )
    ]


def test_a_graphql_write_stores_the_epoch_number(server):
    data = _graphql(
        server,
        "mutation { updateEvents(where: {id: {eq: 1}}, "
        'set: {happened: "2026-05-06T07:08:09.250Z", logged: "2026-05-06T07:08:09"}) '
        "{ affected_rows } }",
    )
    assert data["updateEvents"]["affected_rows"] == 1
    stored = asyncio.run(_source(f"SELECT happened, logged FROM {_SCHEMA}.events WHERE id = 1"))
    assert stored == [(1778051289250, 1778051289)]
