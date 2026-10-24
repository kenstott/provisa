# Copyright (c) 2026 Kenneth Stott
# Canary: 2b10cdf4-a074-41b9-863d-4191a2da025c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a zoned timestamp is an instant end to end (REQ-2050).

Each source family of the test stack holds two tables, ``zoned_rep`` and ``zoned_live``, of one row
(id, ts), its ``ts`` written at
a +02:00 offset (03:04:05+02:00, the instant 01:04:05 UTC): Postgres timestamptz, Oracle TIMESTAMP
WITH TIME ZONE, SQL Server datetimeoffset, MariaDB (the MySQL family) TIMESTAMP, and an
Elasticsearch date. One is read from its replica (``replicate: 0``), the other
live (``replicate: -1``), on DuckDB and on Trino; a live read is registered only where the engine reaches
the source live. Every read answers the instant in UTC with its zone marked, and a filter on an
instant written at another offset finds it.

Everything here is the ``test`` instance: the stack's services and an isolated server per engine.
"""

# Requirements: REQ-2050

from __future__ import annotations

import json
import os

import httpx
import pytest
import yaml

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_trino,
    pytest.mark.requires_oracle,
    pytest.mark.requires_sqlserver,
    pytest.mark.requires_mariadb,
    pytest.mark.requires_elasticsearch,
]

_ROLE = "org_admin"
_DOMAIN = "zoned"
_WRITTEN = "2024-01-02 03:04:05+02:00"
_INSTANT = "2024-01-02T01:04:05+00:00"
# Each source's two tables, by how a read reaches them: the replica, and the source live.
_TABLES = {"rep": "zoned_rep", "live": "zoned_live"}

# (source id, type, host:port as this process reaches it, as Trino's container reaches it,
#  database, username, password, schema)
_SOURCES = {
    "pg": (
        "postgresql",
        ("localhost", lambda: int(os.environ.get("PG_PORT", "5432"))),
        ("postgres", lambda: 5432),
        "provisa",
        "provisa",
        "${env:PG_PASSWORD}",
        "public",
    ),
    "ora": (
        "oracle",
        ("localhost", lambda: int(os.environ["ORACLE_PORT"])),
        ("oracle", lambda: 1521),
        "FREEPDB1",
        "system",
        "provisa",
        "SYSTEM",
    ),
    "mssql": (
        "sqlserver",
        ("localhost", lambda: int(os.environ["SQLSERVER_PORT"])),
        ("sqlserver", lambda: 1433),
        "master",
        "sa",
        "Provisa_2026!",
        "dbo",
    ),
    "maria": (
        "mariadb",
        ("localhost", lambda: int(os.environ["MARIADB_PORT"])),
        ("mariadb", lambda: 3306),
        "provisa",
        "root",
        "provisa",
        "provisa",
    ),
    "es": (
        "elasticsearch",
        ("localhost", lambda: int(os.environ["ELASTICSEARCH_PORT"])),
        ("elasticsearch", lambda: 9200),
        None,
        None,
        None,
        "default",
    ),
}
# The sources an engine reads live; every other is read from its replica only.
_LIVE = {"duckdb": {"pg", "maria"}, "trino": set(_SOURCES)}


def _seed() -> None:
    import oracledb  # pyright: ignore[reportMissingImports]
    import psycopg
    import pymysql
    import pyodbc

    with psycopg.connect(
        host="localhost",
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname="provisa",
        user="provisa",
        password=os.environ.get("PG_PASSWORD", "provisa"),
        autocommit=True,
    ) as pg:
        for t in _TABLES.values():
            pg.execute(f"DROP TABLE IF EXISTS public.{t}")
            pg.execute(f"CREATE TABLE public.{t} (id integer PRIMARY KEY, ts timestamptz)")
            pg.execute(f"INSERT INTO public.{t} VALUES (1, '{_WRITTEN}')")

    ora = oracledb.connect(
        user="system",
        password="provisa",
        dsn=oracledb.makedsn("localhost", int(os.environ["ORACLE_PORT"]), service_name="FREEPDB1"),
    )
    ora.autocommit = True
    with ora.cursor() as cur:
        for t in _TABLES.values():
            cur.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t}'; "
                "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
            )
            cur.execute(
                f"CREATE TABLE {t} (id NUMBER(10) PRIMARY KEY, ts TIMESTAMP WITH TIME ZONE)"
            )
            cur.execute(f"INSERT INTO {t} VALUES (1, TIMESTAMP '2024-01-02 03:04:05 +02:00')")
    ora.close()

    ms = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=localhost,{os.environ['SQLSERVER_PORT']};DATABASE=master;"
        "UID=sa;PWD=Provisa_2026!;TrustServerCertificate=yes",
        autocommit=True,
    )
    for t in _TABLES.values():
        ms.execute(f"DROP TABLE IF EXISTS dbo.{t}")
        ms.execute(f"CREATE TABLE dbo.{t} (id int PRIMARY KEY, ts datetimeoffset)")
        ms.execute(f"INSERT INTO dbo.{t} VALUES (1, '2024-01-02 03:04:05 +02:00')")
    ms.close()

    maria = pymysql.connect(
        host="localhost",
        port=int(os.environ["MARIADB_PORT"]),
        user="root",
        password="provisa",
        database="provisa",
        autocommit=True,
    )
    with maria.cursor() as cur:
        cur.execute("SET time_zone = '+02:00'")
        for t in _TABLES.values():
            cur.execute(f"DROP TABLE IF EXISTS {t}")
            cur.execute(f"CREATE TABLE {t} (id int PRIMARY KEY, ts TIMESTAMP NULL)")
            cur.execute(f"INSERT INTO {t} VALUES (1, '2024-01-02 03:04:05')")
    maria.close()

    base = f"http://localhost:{os.environ['ELASTICSEARCH_PORT']}"
    with httpx.Client(timeout=60) as c:
        for t in _TABLES.values():
            c.delete(f"{base}/{t}")
            made = c.put(
                f"{base}/{t}",
                json={
                    "mappings": {"properties": {"id": {"type": "integer"}, "ts": {"type": "date"}}}
                },
            )
            assert made.status_code == 200, made.text
            put = c.put(
                f"{base}/{t}/_doc/1",
                params={"refresh": "wait_for"},
                json={"id": 1, "ts": "2024-01-02T03:04:05+02:00"},
            )
            assert put.status_code in (200, 201), put.text


def _config(engine: str) -> dict:
    sources, tables = [], []
    for sid, (kind, here, there, database, user, password, schema) in _SOURCES.items():
        host, port = there if engine == "trino" else here
        source = {"id": sid, "type": kind, "host": host, "port": port(), "cache_ttl": 300}
        if database is not None:
            source.update({"database": database, "username": user, "password": password})
        sources.append(source)
        columns = [
            {"name": "id", "data_type": "integer", "visible_to": [_ROLE], "is_primary_key": True},
            {"name": "ts", "data_type": "timestamptz", "visible_to": [_ROLE]},
        ]
        modes = [("rep", 0)] + ([("live", -1)] if sid in _LIVE[engine] else [])
        for mode, replicate in modes:
            tables.append(
                {
                    "source_id": sid,
                    "domain_id": _DOMAIN,
                    "schema": schema,
                    "table": _TABLES[mode],
                    # One SQL address per table in the domain (REQ-1933): the source and how
                    # it is read.
                    "alias": f"{sid}_{mode}",
                    "replicate": replicate,
                    "columns": columns,
                }
            )
    return {
        "federation_engine": engine,
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": _DOMAIN, "description": "zoned timestamps e2e"}],
        "roles": [],
        "sources": sources,
        "tables": tables,
    }


# Per test, not per module: Oracle, SQL Server and MariaDB are brought up for each test and removed
# after it (tests/conftest.py _heavy_db_service), so each test seeds them and boots its own server.
@pytest.fixture(params=["duckdb", "trino"])
def server(tmp_path_factory, request):
    from tests.integration.isolated_server import IsolatedServer

    _seed()
    engine = request.param
    work = tmp_path_factory.mktemp(f"timestamptz_{engine}")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config(engine)))
    if engine == "duckdb":
        srv = IsolatedServer(
            "timestamptz_e2e",
            engine="duckdb",
            config=str(path),
            control_plane="own",
            materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
            enable_pgwire=True,
        )
    else:
        srv = IsolatedServer(
            "timestamptz_e2e_trino", engine="trino", config=str(path), enable_pgwire=True
        )
    srv.start(timeout=300)
    try:
        yield srv, engine
    finally:
        srv.stop_process()


def _tables(engine: str) -> list[str]:
    return [
        f"{sid}_{mode}"
        for sid in _SOURCES
        for mode in (["rep"] + (["live"] if sid in _LIVE[engine] else []))
    ]


def _sql(srv, sql: str) -> list[dict]:
    resp = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 30,
    )
    assert resp.status_code == 200, f"{sql}: {resp.text}"
    return resp.json()["data"]["sql"]


def test_every_family_reads_the_instant_in_utc_live_and_from_its_replica(server):
    srv, engine = server
    answers = {t: _sql(srv, f"SELECT id, ts FROM {_DOMAIN}.{t}") for t in _tables(engine)}
    assert answers == {t: [{"id": 1, "ts": _INSTANT}] for t in _tables(engine)}, json.dumps(
        answers, indent=1
    )


def test_an_instant_written_at_another_offset_finds_it(server):
    srv, engine = server
    for table in _tables(engine):
        rows = _sql(
            srv,
            f"SELECT id FROM {_DOMAIN}.{table} "
            "WHERE ts = CAST('2024-01-01 20:04:05-05:00' AS TIMESTAMPTZ)",
        )
        assert rows == [{"id": 1}], table


def test_the_catalog_lists_an_instant_as_timestamp_with_time_zone(server):
    srv = server[0]
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=srv.pgwire_port,
        dbname="provisa",
        user=_ROLE,
        password="provisa",
    )
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'pg_rep' AND column_name = 'ts'"
        )
        assert cur.fetchall() == [("timestamp with time zone",)]
    finally:
        conn.close()
