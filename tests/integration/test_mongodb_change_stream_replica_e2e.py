# Copyright (c) 2026 Kenneth Stott
# Canary: 33b219b0-fd97-4d0e-8c71-84d76e211a14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: MongoDB change streams keep a replica current (REQ-1861).

The pg engine has no connector for MongoDB, so a MongoDB table is read from its whole-table
replica. A table whose change signal is ``native`` is not rebuilt on a TTL: its source's change
feed is what says it changed. An insert, an update and a delete made in MongoDB each reach a
read through Provisa within seconds, with a cache TTL of an hour, so nothing but the change
stream can have asked for the rebuild.

Everything here is the ``test`` instance: a Postgres and a single-node MongoDB replica set
(change streams need one) this module starts on leased ports and removes, and an isolated
server whose control plane and replica store are that Postgres."""

from __future__ import annotations

import os
import subprocess
import time

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_TABLE = "cs_orders"
_DOCS = [{"order_id": 1, "status": "open"}, {"order_id": 2, "status": "closed"}]
# How long a change may take to reach a read: the listener's debounce, one build, one swap.
_WITHIN_SECONDS = 60


class _Stores:
    """A Postgres (engine, control plane, replica store) and a MongoDB replica set (the source)."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.pg_port, self.mongo_port = lease_ports(2)
        self._pg = f"provisa-itest-cs-pg-{os.getpid()}"
        self._mongo = f"provisa-itest-cs-mongo-{os.getpid()}"

    def pg_url(self, driver: str = "") -> str:
        return f"postgresql{driver}://provisa:provisa@127.0.0.1:{self.pg_port}/provisa"

    def start(self) -> None:
        import psycopg
        import pymongo.errors

        subprocess.run(
            ["docker", "run", "-d", "--rm", "--memory", "512m", "--name", self._pg]
            + ["-e", "POSTGRES_USER=provisa", "-e", "POSTGRES_PASSWORD=provisa"]
            + ["-e", "POSTGRES_DB=provisa", "-p", f"127.0.0.1:{self.pg_port}:5432", "postgres:16"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--memory", "512m", "--name", self._mongo]
            + ["-p", f"127.0.0.1:{self.mongo_port}:27017", "mongo:7"]
            + ["--replSet", "rs0", "--bind_ip_all"],
            check=True,
            capture_output=True,
        )
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
            # A single-node replica set: change streams are served only by one.
            client.admin.command("replSetInitiate")
            deadline = time.monotonic() + 120
            while True:
                try:
                    client["cs"][_TABLE].insert_many([dict(d) for d in _DOCS])
                    break
                except pymongo.errors.PyMongoError:  # no primary elected yet
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(1)

    def mongo(self):
        import pymongo

        return pymongo.MongoClient(
            "127.0.0.1", self.mongo_port, directConnection=True, serverSelectionTimeoutMS=120_000
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
        "domains": [{"id": "docs", "description": "change stream e2e"}],
        "roles": [
            {
                "id": _ROLE,
                "capabilities": ["source_registration", "table_registration", "query_development"],
                "domain_access": ["*"],
            }
        ],
        "sources": [
            {
                "id": "cs-mongo",
                "type": "mongodb",
                "host": "127.0.0.1",
                "port": stores.mongo_port,
                "database": "cs",
                "federation_hints": {"direct_connection": "true"},
            }
        ],
        "tables": [
            {
                "source_id": "cs-mongo",
                "domain_id": "docs",
                "schema": "cs",
                "table": _TABLE,
                # The change feed is what refreshes it; no TTL rebuild can within this test.
                "change_signal": "native",
                "cache_ttl": 3600,
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
    path = tmp_path_factory.mktemp("change_stream_mongo") / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    control_plane = stores.pg_url("+psycopg")
    srv = IsolatedServer(
        "mongodb_change_stream_e2e",
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


def _rows(server) -> list[dict]:
    resp = httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": f"SELECT order_id, status FROM {_TABLE} ORDER BY 1"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 30,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


def _eventually(server, expected: list[dict]) -> float:
    """Seconds until a read through Provisa returns ``expected``; fails after the bound with
    what the last read returned."""
    started = time.monotonic()
    seen: list[dict] = []
    while time.monotonic() - started < _WITHIN_SECONDS:
        seen = _rows(server)
        if seen == expected:
            return time.monotonic() - started
        time.sleep(1)
    raise AssertionError(f"after {_WITHIN_SECONDS}s the read still returns {seen}")


def test_each_change_in_mongodb_reaches_the_replica_without_a_ttl(stores, server):
    assert _eventually(server, _DOCS) < _WITHIN_SECONDS  # the first build

    with stores.mongo() as client:
        client["cs"][_TABLE].insert_one({"order_id": 3, "status": "open"})
    _eventually(server, [*_DOCS, {"order_id": 3, "status": "open"}])

    with stores.mongo() as client:
        client["cs"][_TABLE].update_one({"order_id": 1}, {"$set": {"status": "shipped"}})
    _eventually(
        server,
        [
            {"order_id": 1, "status": "shipped"},
            {"order_id": 2, "status": "closed"},
            {"order_id": 3, "status": "open"},
        ],
    )

    with stores.mongo() as client:
        client["cs"][_TABLE].delete_one({"order_id": 2})
    _eventually(server, [{"order_id": 1, "status": "shipped"}, {"order_id": 3, "status": "open"}])
