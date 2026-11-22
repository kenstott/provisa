# Copyright (c) 2026 Kenneth Stott
# Canary: a8b8d36b-035a-4499-b00a-b95b706c1605
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registering sources and tables on a running server, asked as a steward and a client ask
(REQ-012, REQ-013, REQ-433, REQ-638).

One server over the harness's PostgreSQL database, which also holds ``items``, a table nothing
registers at boot, and a SQLite file the test writes. Through ``/admin/graphql``:

- a source whose connection fails is refused when it is created; one that connects is created
  and its tables are read seconds later, with no restart (REQ-012);
- creating a source exposes nothing: its tables are not queryable until each is registered
  (REQ-013);
- ``availableSchemas`` and ``availableTables`` are the one pair of endpoints for a PostgreSQL and
  a SQLite source alike (REQ-638);
- a table name is unique within its domain only: ``orders`` is registered in a second domain, a
  second ``orders`` in the same domain is refused by name, and an ``orders`` removed and
  registered again in its domain is restored with its id (REQ-433).

Lands on the TEST instance only: one real server over a database the harness creates."""

from __future__ import annotations

import json
import os
import sqlite3
import time

import httpx
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot
from tests.port_lease import lease_ports

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ADMIN = "org_admin"
_TIMEOUT = 120.0
# REQ-012: a registered table is available "within seconds".
_AVAILABLE_S = 30.0


@pytest.fixture(scope="module")
def birds(tmp_path_factory):
    """A SQLite file with one table, for a source of a second type."""
    path = tmp_path_factory.mktemp("sqlite") / "birds.sqlite"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE birds (id INTEGER PRIMARY KEY, name TEXT)")
    db.executemany("INSERT INTO birds VALUES (?, ?)", [(1, "owl"), (2, "wren")])
    db.commit()
    db.close()
    return path


@pytest.fixture(scope="module")
def server():
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "domains": [
                {"id": "sales", "description": "boot harness"},
                {"id": "ops", "description": "a second domain"},
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with own.connect() as conn:
            conn.execute(sa.text("CREATE TABLE public.items (id integer PRIMARY KEY, name text)"))
            conn.execute(sa.text("INSERT INTO public.items VALUES (1, 'bolt'), (2, 'nut')"))
        own.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _url(boot, path: str) -> str:
    return f"http://127.0.0.1:{boot.ports['http']}{path}"


def _admin(boot, document: str) -> dict:
    resp = httpx.post(
        _url(boot, "/admin/graphql"),
        json={"query": document},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=_TIMEOUT,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body["errors"]
    return body["data"]


def _mutate(boot, mutation: str) -> dict:
    (result,) = _admin(boot, "mutation { " + mutation + " { success message } }").values()
    return result


def _sql(boot, sql: str, role: str = _ADMIN) -> httpx.Response:
    return httpx.post(
        _url(boot, "/data/sql"),
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=_TIMEOUT,
    )


def _pg_source(source_id: str, port: int, database: str) -> str:
    return (
        f'createSource(input: {{id: "{source_id}", type: "postgresql", host: "{_PG_HOST}", '
        f'port: {port}, database: "{database}", username: "provisa", password: "provisa"}})'
    )


def _register(boot, source_id: str, domain: str, table: str, columns: list[tuple]) -> dict:
    cols = ", ".join(
        f'{{name: "{n}", visibleTo: ["{_ADMIN}"], dataType: "{t}", isPrimaryKey: {json.dumps(pk)}}}'
        for n, t, pk in columns
    )
    return _mutate(
        boot,
        f'registerTable(input: {{sourceId: "{source_id}", domainId: "{domain}", '
        f'schemaName: "public", tableName: "{table}", columns: [{cols}]}})',
    )


def _until_rows(boot, sql: str) -> tuple[list[dict], float]:
    """The rows of ``sql`` as the admin reads them, and how long after the call they were read."""
    started = time.monotonic()
    while True:
        resp = _sql(boot, sql)
        if resp.status_code == 200:
            return resp.json()["data"]["sql"], time.monotonic() - started
        assert time.monotonic() - started < _AVAILABLE_S, resp.text
        time.sleep(0.5)


def _table_ids(boot, domain: str, table: str) -> list[int]:
    tables = _admin(boot, "{ tables { id domainId tableName } }")["tables"]
    return [t["id"] for t in tables if t["domainId"] == domain and t["tableName"] == table]


# --- REQ-012 / REQ-013: a source registered while the server runs -------------------------------


def test_a_source_whose_connection_fails_is_refused_when_it_is_created(server):
    (dead_port,) = lease_ports(1)
    refused = _mutate(server, _pg_source("dead-pg", dead_port, server.database))
    assert not refused["success"], refused
    assert "connection validation failed" in refused["message"], refused
    sources = _admin(server, "{ sources { id } }")["sources"]
    assert "dead-pg" not in [s["id"] for s in sources]


@pytest.fixture(scope="module")
def late_pg(server) -> str:
    """A PostgreSQL source created while the server runs, over the harness's database."""
    created = _mutate(server, _pg_source("late-pg", _PG_PORT, server.database))
    assert created["success"], created
    return "late-pg"


def test_a_created_source_exposes_nothing_until_a_table_is_registered_then_reads_at_once(
    server, late_pg
):
    # REQ-013: the source's tables are not queryable until a steward registers one.
    before = _sql(server, "SELECT id, name FROM ops.items")
    assert before.status_code != 200, before.text

    registered = _register(
        server, late_pg, "ops", "items", [("id", "integer", True), ("name", "varchar", False)]
    )
    assert registered["success"], registered
    # REQ-012: no restart; the table is read seconds after it is registered.
    rows, waited = _until_rows(server, "SELECT id, name FROM ops.items ORDER BY id")
    assert [(r["id"], r["name"]) for r in rows] == [(1, "bolt"), (2, "nut")]
    assert waited < _AVAILABLE_S


# --- REQ-638: one pair of introspection endpoints for every source type -------------------------


def test_available_schemas_and_tables_answer_for_postgresql_and_sqlite_alike(
    server, late_pg, birds
):
    created = _mutate(
        server,
        f'createSource(input: {{id: "birds", type: "sqlite", host: "", port: 0, database: "", '
        f'path: "{birds}"}})',
    )
    assert created["success"], created

    pg_schemas = _admin(server, '{ availableSchemas(sourceId: "late-pg") }')["availableSchemas"]
    assert "public" in pg_schemas, pg_schemas
    pg_tables = _admin(
        server, '{ availableTables(sourceId: "late-pg", schemaName: "public") { name } }'
    )["availableTables"]
    assert {"orders", "items"} <= {t["name"] for t in pg_tables}, pg_tables

    sqlite_schemas = _admin(server, '{ availableSchemas(sourceId: "birds") }')["availableSchemas"]
    assert sqlite_schemas, sqlite_schemas
    sqlite_tables = _admin(
        server,
        f'{{ availableTables(sourceId: "birds", schemaName: "{sqlite_schemas[0]}") {{ name }} }}',
    )["availableTables"]
    assert [t["name"] for t in sqlite_tables] == ["birds"], sqlite_tables


# --- REQ-433: a table name is unique within its domain ------------------------------------------


def test_a_table_name_is_unique_within_its_domain_and_a_removed_one_is_restored(server, late_pg):
    # orders is registered in sales at boot; the same source table registers in ops too.
    in_ops = _register(server, "late-pg", "ops", "orders", [("id", "integer", True)])
    assert in_ops["success"], in_ops
    assert len(_table_ids(server, "sales", "orders")) == 1
    (ops_id,) = _table_ids(server, "ops", "orders")

    # A second orders in sales is refused by name.
    again = _register(server, "late-pg", "sales", "orders", [("id", "integer", True)])
    assert not again["success"], again
    assert "orders" in again["message"], again

    # Removed from ops and registered there again, it is restored with its id.
    removed = _mutate(server, f"deleteTable(id: {ops_id})")
    assert removed["success"], removed
    assert _table_ids(server, "ops", "orders") == []
    restored = _register(server, "late-pg", "ops", "orders", [("id", "integer", True)])
    assert restored["success"], restored
    assert _table_ids(server, "ops", "orders") == [ops_id]
