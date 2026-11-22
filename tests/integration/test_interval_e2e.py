# Copyright (c) 2026 Kenneth Stott
# Canary: 6efc1a17-ff13-4113-bc25-aa170bcb4fc3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: an interval is read as the duration it is, its months kept.

Oracle's INTERVAL YEAR TO MONTH and INTERVAL DAY TO SECOND and Postgres's interval, each holding
1 year 2 months and 3 days 04:05:06, are read through the direct SourcePool (the reader every
landing and direct route uses) and through a DuckDB-engine server, from the replica and (Postgres)
live: every read answers the ISO 8601 durations ``P1Y2M`` and ``P3DT4H5M6S``.

Everything here is the ``test`` instance: the stack's services and an isolated server.
"""

# Requirements: REQ-010, REQ-846

from __future__ import annotations

import json
import os
from datetime import timedelta

import httpx
import pytest
import yaml
from dateutil.relativedelta import relativedelta

from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.requires_oracle]

_ROLE = "org_admin"
_DOMAIN = "durations"
_YM = relativedelta(years=1, months=2)
_DS = timedelta(days=3, seconds=14706)
_ANSWER = [{"id": 1, "ym": "P1Y2M", "ds": "P3DT4H5M6S"}]


def _pg_port() -> int:
    return int(os.environ.get("PG_PORT", "5432"))


def _seed() -> None:
    import oracledb  # pyright: ignore[reportMissingImports]
    import psycopg

    with psycopg.connect(
        host="localhost",
        port=_pg_port(),
        dbname="provisa",
        user="provisa",
        password=os.environ.get("PG_PASSWORD", "provisa"),
        autocommit=True,
    ) as pg:
        for t in ("durations", "durations_live"):
            pg.execute(f"DROP TABLE IF EXISTS public.{t}")
            pg.execute(
                f"CREATE TABLE public.{t} (id integer PRIMARY KEY, ym interval, ds interval)"
            )
            pg.execute(f"INSERT INTO public.{t} VALUES (1, '1 year 2 months', '3 days 04:05:06')")

    ora = oracledb.connect(
        user="system",
        password="provisa",
        dsn=oracledb.makedsn("localhost", int(os.environ["ORACLE_PORT"]), service_name="FREEPDB1"),
    )
    ora.autocommit = True
    with ora.cursor() as cur:
        cur.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE durations'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        cur.execute(
            "CREATE TABLE durations (id NUMBER(10) PRIMARY KEY, "
            "ym INTERVAL YEAR TO MONTH, ds INTERVAL DAY TO SECOND)"
        )
        cur.execute(
            "INSERT INTO durations VALUES (1, INTERVAL '1-2' YEAR TO MONTH, "
            "INTERVAL '3 04:05:06' DAY TO SECOND)"
        )
    ora.close()


# Per test, not per module: Oracle is a heavy service, brought up for each test and torn down
# after it (tests/conftest.py _HEAVY_MARKERS), so each test's container is seeded afresh.
@pytest.fixture
def seeded():
    _seed()


async def _read(source_type, port, database, user, password, sql):
    pool = SourcePool()
    await pool.add(
        source_id=f"durations-{source_type}",
        source_type=source_type,
        host="localhost",
        port=port,
        database=database,
        user=user,
        password=password,
    )
    try:
        return (await pool.execute(f"durations-{source_type}", sql)).rows
    finally:
        await pool.close_all()


@pytest.mark.asyncio
async def test_oracle_reads_its_intervals_with_their_months(seeded):
    rows = await _read(
        "oracle",
        int(os.environ["ORACLE_PORT"]),
        "FREEPDB1",
        "system",
        "provisa",
        "SELECT ym, ds FROM durations",
    )
    assert rows == [(_YM, _DS)]


@pytest.mark.asyncio
async def test_postgres_reads_its_intervals_with_their_months(seeded):
    rows = await _read(
        "postgresql",
        _pg_port(),
        "provisa",
        "provisa",
        os.environ.get("PG_PASSWORD", "provisa"),
        "SELECT ym, ds FROM public.durations",
    )
    assert rows == [(_YM, _DS)]


def _config() -> dict:
    columns = [
        {"name": "id", "data_type": "integer", "visible_to": [_ROLE], "is_primary_key": True},
        {"name": "ym", "data_type": "interval", "visible_to": [_ROLE]},
        {"name": "ds", "data_type": "interval", "visible_to": [_ROLE]},
    ]
    sources = [
        {
            "id": "pg",
            "type": "postgresql",
            "host": "localhost",
            "port": _pg_port(),
            "database": "provisa",
            "username": "provisa",
            "password": "${env:PG_PASSWORD}",
            "cache_ttl": 300,
        },
        {
            "id": "ora",
            "type": "oracle",
            "host": "localhost",
            "port": int(os.environ["ORACLE_PORT"]),
            "database": "FREEPDB1",
            "username": "system",
            "password": "provisa",
            "cache_ttl": 300,
        },
    ]
    # (source, schema, how it is read): the replica, and Postgres live (DuckDB reaches it).
    # One physical table per registration.
    reads = [
        ("pg", "public", "durations", "rep", 0),
        ("pg", "public", "durations_live", "live", -1),
        ("ora", "SYSTEM", "durations", "rep", 0),
    ]
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": _DOMAIN, "description": "interval e2e"}],
        "roles": [],
        "sources": sources,
        "tables": [
            {
                "source_id": sid,
                "domain_id": _DOMAIN,
                "schema": schema,
                "table": table,
                "alias": f"{sid}_{mode}",
                "replicate": replicate,
                "columns": columns,
            }
            for sid, schema, table, mode, replicate in reads
        ],
    }


@pytest.fixture
def server(seeded, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("interval_e2e")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        "interval_e2e",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
    )
    srv.start(timeout=300)
    try:
        yield srv
    finally:
        srv.stop_process()


def test_every_read_answers_the_iso_8601_durations(server):
    answers = {}
    for table in ("pg_rep", "pg_live", "ora_rep"):
        resp = httpx.post(
            f"{server.base_url}/data/sql",
            json={"sql": f"SELECT id, ym, ds FROM {_DOMAIN}.{table}"},
            headers={"X-Provisa-Role": _ROLE},
            timeout=server.request_timeout + 30,
        )
        assert resp.status_code == 200, f"{table}: {resp.text}"
        answers[table] = resp.json()["data"]["sql"]
    assert answers == {t: _ANSWER for t in answers}, json.dumps(answers, indent=1)
