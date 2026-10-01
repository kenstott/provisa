# Copyright (c) 2026 Kenneth Stott
# Canary: 8b3e6c19-4f2a-4d71-9e05-1a7d5c2f9b36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: pgwire's REQ-1863 Postgres passthrough engages on a REAL server process under both
event loops a deployment can run.

* ``uvloop`` — ``uvicorn main:app``, exactly as deployed: main.py installs uvloop (REQ-1867).
* ``stdlib`` — the same app on asyncio's own loops (``tests.integration.stdlib_loop_app``).

The source is the test stack's real Postgres. Its one registered table is a view over every
exact-width type plus a ``reader`` column whose function records the reading connection's
``application_name`` — so the source itself reports which connection executed the statement: the
passthrough's dedicated raw connection (``provisa-pgwire-passthrough``) or the decoded DIRECT
stream's pooled one. asyncpg reads every column back in BINARY, so the forwarded bytes are decoded
strictly by the OIDs pgwire's Describe advertised.
"""

# Requirements: REQ-1863, REQ-1867

from __future__ import annotations

import asyncio
import datetime
import os
import uuid

import pytest

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_SCHEMA = "ptloop"
_PASSTHROUGH_APP = "provisa-pgwire-passthrough"
_SQL = "SELECT si, r, jb, tz, ttz, u, iv, ba, old_ts, old_day, reader FROM ptloop.wide_probe"
_EXPECTED = (
    7,
    1.5,
    '{"k": [1, 2]}',
    datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
    datetime.time(3, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=2))),
    uuid.UUID("12345678-1234-5678-1234-567812345678"),
    datetime.timedelta(days=3, seconds=4, microseconds=5),
    b"\x00\x01\xff",
    datetime.datetime(1999, 12, 31, 23, 59, 59),
    datetime.date(1970, 1, 1),
    1,
)


def _source_dsn() -> str:
    return (
        f"postgresql://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@localhost:"
        f"{os.environ.get('PG_PORT', '5432')}/provisa"
    )


async def _source(sql: str) -> list[tuple]:
    import asyncpg

    conn = await asyncpg.connect(_source_dsn())
    try:
        return [tuple(r) for r in await conn.fetch(sql)]
    finally:
        await conn.close()


async def _seed() -> None:
    import asyncpg

    conn = await asyncpg.connect(_source_dsn())
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        await conn.execute(f"CREATE TABLE {_SCHEMA}.readers (app text)")
        await conn.execute(
            f"CREATE TABLE {_SCHEMA}.wide (si int2, r float4, jb jsonb, tz timestamptz, "
            "ttz timetz, u uuid, iv interval, ba bytea, old_ts timestamp, old_day date)"
        )
        await conn.execute(
            f"INSERT INTO {_SCHEMA}.wide VALUES (7, 1.5, '{{\"k\": [1, 2]}}', "
            "'2026-01-02 03:04:05+00', '03:04:05+02', '12345678-1234-5678-1234-567812345678', "
            "'3 days 00:00:04.000005', '\\x0001ff', '1999-12-31 23:59:59', '1970-01-01')"
        )
        # VOLATILE: evaluated for every row actually read — a Describe's zero-row plan never runs it.
        await conn.execute(
            f"CREATE FUNCTION {_SCHEMA}.note_reader() RETURNS int LANGUAGE sql VOLATILE AS $$ "
            f"INSERT INTO {_SCHEMA}.readers VALUES (current_setting('application_name')); "
            "SELECT 1 $$"
        )
        await conn.execute(
            f"CREATE VIEW {_SCHEMA}.wide_probe AS "
            f"SELECT w.*, {_SCHEMA}.note_reader() AS reader FROM {_SCHEMA}.wide w"
        )
    finally:
        await conn.close()


@pytest.fixture(
    scope="module",
    params=[
        pytest.param(("main:app", "auto"), id="uvloop"),
        pytest.param(("tests.integration.stdlib_loop_app:app", "asyncio"), id="stdlib"),
    ],
)
def server(request):
    from tests.integration.isolated_server import IsolatedServer

    app, loop = request.param
    asyncio.run(_seed())
    srv = IsolatedServer(
        f"ptloop_{request.param_index}",
        engine="duckdb",
        enable_pgwire=True,
        config="tests/fixtures/pg_passthrough_loop_config.yaml",
        control_plane="sqlite",
        app=app,
        loop=loop,
    )
    try:
        srv.start()
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(_source(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))


async def _read_binary(port: int) -> list[tuple]:
    import asyncpg

    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=port,
        user=_ROLE,
        password="provisa",
        database="provisa",
        ssl=False,
        statement_cache_size=0,
    )
    try:
        return [tuple(r) for r in await conn.fetch(_SQL)]
    finally:
        await conn.close()


def test_every_wide_type_reads_back_exactly_through_the_passthrough(server):
    asyncio.run(_source(f"TRUNCATE {_SCHEMA}.readers"))
    assert asyncio.run(_read_binary(server.pgwire_port)) == [_EXPECTED]
    # The source's own record: the statement ran on the passthrough's dedicated raw connection,
    # once — not on the decoded DIRECT stream's pooled connection.
    assert asyncio.run(_source(f"SELECT app FROM {_SCHEMA}.readers")) == [(_PASSTHROUGH_APP,)]


def test_graphql_renders_interval_and_timetz_through_the_same_server(server):
    """The registered interval/timetz/bytea columns serve GraphQL too: an interval is its ISO 8601
    duration (the Interval scalar), timetz its ISO time text, bytea Postgres's own hex text."""
    import json
    import urllib.error
    import urllib.request

    body = json.dumps({"query": "{ wideProbe { iv ttz ba } }"}).encode()
    req = urllib.request.Request(
        f"{server.base_url}/data/graphql",
        data=body,
        headers={"Content-Type": "application/json", "X-Provisa-Role": _ROLE},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise AssertionError(
            f"HTTP {exc.code}: {exc.read().decode(errors='replace')}\n"
            f"{server.dump_stderr_debug()[-4000:]}"
        ) from exc
    assert "errors" not in payload, payload
    (row,) = payload["data"]["wideProbe"]
    assert row["iv"] == "P3DT4.000005S"
    assert row["ttz"] == "03:04:05+02:00"
    assert row["ba"] == "\\x0001ff"
