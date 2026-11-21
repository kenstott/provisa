# Copyright (c) 2026 Kenneth Stott
# Canary: 3a9f6c20-84d1-4e57-b0c2-9e15d7a84f31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a JDBC source with a postgresql URL and live attach turned on for the DuckDB engine
(REQ-2191) is attached as a built-in postgresql source is, through DuckDB's Postgres attach, and
its rows are read live from the compose Postgres."""

# Requirements: REQ-2191

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.federation.strategy import Strategy, federate
from provisa.jdbc_source.attach import attach_type, attach_view

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

psycopg2 = pytest.importorskip("psycopg2")

_HOST = os.environ.get("PG_HOST", "localhost")
_PORT = int(os.environ.get("PG_PORT", "5432"))
_DB = os.environ.get("PG_DATABASE", "provisa")
_USER = os.environ.get("PG_USER", "provisa")
_PASSWORD = os.environ.get("PG_PASSWORD", "provisa")
_SCHEMA = "jdbc_attach_e2e"


@pytest.fixture
def remote():
    conn = psycopg2.connect(host=_HOST, port=_PORT, dbname=_DB, user=_USER, password=_PASSWORD)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {_SCHEMA}")
        cur.execute(f"CREATE TABLE {_SCHEMA}.orders (id integer PRIMARY KEY, region text)")
        cur.execute(f"INSERT INTO {_SCHEMA}.orders VALUES (1, 'east'), (2, 'west'), (3, 'east')")
    yield conn
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
    conn.close()


async def test_an_attached_jdbc_postgresql_source_reads_through_the_postgres_attach(
    remote, tmp_path
) -> None:
    rt = DuckDBFederationRuntime(materialize_dsn=f"duckdb:///{tmp_path / 'store.duckdb'}")
    engine = rt._engine
    source = Source(
        id="gsql",
        type=SourceType.jdbc,
        password=_PASSWORD,
        federation_hints={
            "url": f"postgresql+psycopg2://{_USER}@{_HOST}:{_PORT}/{_DB}",
            "sql_dialect": "postgres",
            f"capabilities.{engine.name}": "attach",
        },
    )
    assert attach_type(source, engine) == "postgresql"
    assert federate(source, engine) is Strategy.VIRTUAL
    view = attach_view(source, "postgresql")
    table = SimpleNamespace(
        **{name: getattr(view, name) for name in ("id", "type", "host", "port", "database")},
        username=view.username,
        password=view.password,
        path=None,
        catalog="gsql",
        schema_name=_SCHEMA,
        table_name="orders",
        max_live_concurrency=None,
    )
    rt.attach_source(table)
    sql = f"SELECT id FROM {rt._phys_name(table)} WHERE region = 'east' ORDER BY id"
    assert [tuple(r) for r in (await rt.run(sql)).rows] == [(1,), (3,)]
    # Read live: a row written at the source is seen on the next read, nothing landed.
    with remote.cursor() as cur:
        cur.execute(f"INSERT INTO {_SCHEMA}.orders VALUES (4, 'east')")
    assert [tuple(r) for r in (await rt.run(sql)).rows] == [(1,), (3,), (4,)]
