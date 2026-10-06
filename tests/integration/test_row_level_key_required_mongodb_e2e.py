# Copyright (c) 2026 Kenneth Stott
# Canary: 0c4b7e92-9d1a-4f63-b5e8-2a6d3f8c1e47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a row-level replica table of a MongoDB source on the pg engine (REQ-1915).

The pg engine has no connector for MongoDB, so ``row_materialize`` is the collection's reach — the
perf fragment's ``order_docs``. An unfiltered ``/data/sql`` read of it used to copy the collection
into the worker and upsert it one row at a time (a 20M-document collection never finished and
held the request open). It is refused at planning, naming the table and its key, and the
collection is never read; a read that binds the key fetches exactly those documents.

Everything here is the ``test`` instance: a Postgres and a MongoDB this module starts on leased
ports and removes, and an isolated server whose control plane and replica store are that
Postgres."""

# Requirements: REQ-1915, REQ-1865

from __future__ import annotations

import os
import subprocess
import time

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_TABLE = "rl_order_docs"
_DOCS = [{"order_id": i, "status": "open" if i % 2 else "closed"} for i in range(1, 41)]


class _Stores:
    """A Postgres (engine, control plane, replica store) and a MongoDB (the source)."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.pg_port, self.mongo_port = lease_ports(2)
        self._pg = f"provisa-itest-rlkey-pg-{os.getpid()}"
        self._mongo = f"provisa-itest-rlkey-mongo-{os.getpid()}"

    def pg_url(self, driver: str = "") -> str:
        return f"postgresql{driver}://provisa:provisa@127.0.0.1:{self.pg_port}/provisa"

    def start(self) -> None:
        import psycopg

        subprocess.run(
            ["docker", "run", "-d", "--rm", "--memory", "512m", "--name", self._pg]
            + ["-e", "POSTGRES_USER=provisa", "-e", "POSTGRES_PASSWORD=provisa"]
            + ["-e", "POSTGRES_DB=provisa", "-p", f"127.0.0.1:{self.pg_port}:5432", "postgres:16"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--memory", "512m", "--name", self._mongo]
            + ["-p", f"127.0.0.1:{self.mongo_port}:27017", "mongo:7", "--profile", "2"],
            check=True,
            capture_output=True,
        )
        # The image restarts the server once after initdb: ready means it answers repeatedly.
        deadline, answered = time.monotonic() + 120, 0
        while answered < 3:
            try:
                psycopg.connect(self.pg_url(), connect_timeout=2).close()
                answered += 1
            except psycopg.OperationalError:
                answered = 0
                if time.monotonic() > deadline:
                    raise
            time.sleep(1)
        with self.mongo() as client:
            client.admin.command("ping")
            client["rl"][_TABLE].insert_many([dict(d) for d in _DOCS])

    def mongo(self):
        import pymongo

        return pymongo.MongoClient(
            "127.0.0.1", self.mongo_port, directConnection=True, serverSelectionTimeoutMS=120_000
        )

    def collection_reads(self) -> list[dict]:
        """Every find/aggregate the source served on the collection (profiler level 2)."""
        with self.mongo() as client:
            return list(
                client["rl"]["system.profile"].find(
                    {"ns": f"rl.{_TABLE}", "op": {"$in": ["query", "command", "getmore"]}}
                )
            )

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self._pg, self._mongo], capture_output=True)


@pytest.fixture(scope="module")
def stores():
    s = _Stores()
    try:
        s.start()
        yield s
    finally:
        s.stop()


@pytest.fixture(scope="module")
def server(stores, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    cfg = {
        "federation_engine": "pg",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "docs", "description": "row-level key e2e"}],
        # org_admin is the reserved administrative role (REQ-1349): every org has it and a
        # config file may not declare it.
        "roles": [],
        "sources": [
            {
                "id": "rl-mongo",
                "type": "mongodb",
                "host": "127.0.0.1",
                "port": stores.mongo_port,
                "database": "rl",
                "federation_hints": {"direct_connection": "true"},
            }
        ],
        "tables": [
            {
                "source_id": "rl-mongo",
                "domain_id": "docs",
                "schema": "rl",
                "table": _TABLE,
                "row_materialize": True,
                "cache_ttl": 300,
                "columns": [
                    {
                        "name": "order_id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "visible_to": [_ROLE],
                    },
                    {"name": "status", "data_type": "varchar", "visible_to": [_ROLE]},
                ],
            }
        ],
    }
    path = tmp_path_factory.mktemp("row_level_mongo") / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    control_plane = stores.pg_url("+psycopg")
    srv = IsolatedServer(
        "row_level_key_mongo_e2e",
        engine="pg",
        config=str(path),
        control_plane="postgres",
        env={
            "PLATFORM_DATABASE_URL": control_plane,
            "TENANT_DATABASE_URL": control_plane,
            "PROVISA_MATERIALIZE_URL": stores.pg_url(),
            "PROVISA_REDIS_EMBEDDED": "1",
        },
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _sql(server, sql: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 30,
    )


def test_an_unfiltered_read_is_refused_and_the_collection_is_not_read(stores, server):
    before = len(stores.collection_reads())
    started = time.monotonic()
    resp = _sql(server, f"SELECT order_id FROM {_TABLE} LIMIT 1")
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["code"] == "data.row_level_key_required", body
    assert body["params"] == {"table": _TABLE, "key": "order_id"}
    assert time.monotonic() - started < 10  # refused at planning, not after a copy
    assert len(stores.collection_reads()) == before


def test_a_keyed_read_fetches_exactly_its_documents(stores, server):
    resp = _sql(
        server, f"SELECT order_id, status FROM {_TABLE} WHERE order_id IN (3, 4) ORDER BY 1"
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"]["sql"] == [
        {"order_id": 3, "status": "open"},
        {"order_id": 4, "status": "closed"},
    ]
    import psycopg

    with psycopg.connect(stores.pg_url(), autocommit=True) as conn:
        replicas = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.columns "
            "WHERE column_name = 'order_id' AND table_name LIKE %s",
            (f"%{_TABLE}%",),
        ).fetchall()
        assert len(replicas) == 1, replicas
        schema, name = replicas[0]
        held = conn.execute(f'SELECT order_id FROM "{schema}"."{name}" ORDER BY 1').fetchall()
    assert held == [(3,), (4,)]  # the replica holds the keys asked for, not the collection
    again = _sql(server, f"SELECT count(*) AS n FROM {_TABLE}")
    assert again.status_code == 400 and again.json()["code"] == "data.row_level_key_required"
