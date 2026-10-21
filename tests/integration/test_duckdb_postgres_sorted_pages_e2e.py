# Copyright (c) 2026 Kenneth Stott
# Canary: 40314801-1da0-43f8-b05c-3929af6e7f7d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""#241: a sorted page of a Postgres-attached relation, read through the DuckDB engine, is the page
Postgres itself would answer.

`ORDER BY <col> … NULLS FIRST LIMIT n` (every DESC sort the UI sends, after the duckdb transpile
writes Postgres's null order out) used to come back holding only the NULL-key rows: DuckDB's Top-N
dynamic filter reached Postgres as `WHERE <col> IS NULL` (upstream duckdb/duckdb-postgres#564).
Checked for the control-plane attach (the ops/meta reports) and for a postgresql source, through
both `run` and `run_sync`, against Postgres's own answer to the same statement, on a column that
holds NULLs so the order of the NULLs is checked too. Self-provisioned embedded Postgres; no docker.
"""

from __future__ import annotations

import tempfile
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pgserver = pytest.importorskip("pixeltable_pgserver")
psycopg2 = pytest.importorskip("psycopg2")

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime  # noqa: E402

_ROWS = [(1, 10, "a"), (2, 30, "b"), (3, None, "c"), (4, 20, "d"), (5, None, "e"), (6, 40, "f")]

# (ORDER BY, LIMIT): the sorts the viewer sends, NULLS FIRST included, each a page shorter than
# the relation so the Top-N path is taken.
_PAGES = [
    ('"score" DESC NULLS FIRST, "id" ASC', 4),
    ('"score" ASC NULLS FIRST, "id" ASC', 4),
    ('"score" DESC NULLS LAST, "id" ASC', 4),
    ('"id" DESC NULLS FIRST', 3),
]


@pytest.fixture(scope="session")
def pg():
    server = pgserver.get_server(tempfile.mkdtemp(prefix="provisa_pg_sorted_"))
    uri = server.get_uri()
    conn = psycopg2.connect(uri)
    conn.autocommit = True
    with conn.cursor() as cur:
        for schema in ("org_default", "remote"):
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cur.execute(f"DROP TABLE IF EXISTS {schema}.items")
            cur.execute(f"CREATE TABLE {schema}.items(id int PRIMARY KEY, score int, name text)")
            cur.executemany(f"INSERT INTO {schema}.items VALUES (%s, %s, %s)", _ROWS)
    yield SimpleNamespace(uri=uri, conn=conn)
    conn.close()


def _postgres_page(pg, schema: str, order_by: str, limit: int) -> list[tuple]:
    with pg.conn.cursor() as cur:
        cur.execute(f"SELECT id, score, name FROM {schema}.items ORDER BY {order_by} LIMIT {limit}")
        return [tuple(r) for r in cur.fetchall()]


def _libpq(uri: str) -> dict:
    u = urlparse(uri)
    q = parse_qs(u.query)
    return {
        "host": q["host"][0] if "host" in q else (u.hostname or "localhost"),
        "port": u.port or 5432,
        "database": u.path.lstrip("/") or "postgres",
        "username": u.username or "postgres",
    }


async def _pages_match(rt: DuckDBFederationRuntime, relation: str, pg, schema: str) -> None:
    for order_by, limit in _PAGES:
        expected = _postgres_page(pg, schema, order_by, limit)
        sql = f"SELECT id, score, name FROM {relation} ORDER BY {order_by} LIMIT {limit} OFFSET 0"
        via_run = [tuple(r) for r in (await rt.run(sql)).rows]
        via_sync = [tuple(r) for r in rt.run_sync(sql).rows]
        assert via_run == expected, (order_by, via_run, expected)
        assert via_sync == expected, (order_by, via_sync, expected)


async def test_the_control_plane_attach_answers_a_sorted_page_as_postgres_does(pg, tmp_path):
    p = _libpq(pg.uri)
    dsn = f"host={p['host']} port={p['port']} dbname={p['database']} user={p['username']}"
    rt = DuckDBFederationRuntime(materialize_dsn=f"duckdb:///{tmp_path / 'store.duckdb'}")
    rt.attach_control_plane(dsn)
    await _pages_match(rt, '"provisa_admin"."org_default"."items"', pg, "org_default")


async def test_a_postgresql_source_answers_a_sorted_page_as_postgres_does(pg, tmp_path):
    p = _libpq(pg.uri)
    src = SimpleNamespace(
        id="pgsrc",
        catalog="pgsrc",
        type=SimpleNamespace(value="postgresql"),
        host=p["host"],
        port=p["port"],
        database=p["database"],
        username=p["username"],
        password=None,
        schema_name="remote",
        table_name="items",
        path=None,
        max_live_concurrency=None,  # REQ-1909: a source always carries its cap; none here
    )
    rt = DuckDBFederationRuntime(materialize_dsn=f"duckdb:///{tmp_path / 'store.duckdb'}")
    rt.attach_source(src)
    await _pages_match(rt, rt._phys_name(src), pg, "remote")
