# Copyright (c) 2026 Kenneth Stott
# Canary: 43ab9e7e-b14e-40c1-878b-c040bd8b0546
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a source whose cache_ttl is -1 lands once and is never contacted again (REQ-2031).

A pg-engine server declares, in its file, a PostgreSQL source with cache_ttl -1 and one table of
it. Once the table has landed, the source's server is stopped and the Provisa server restarted:
the restart, the reconcile it runs and a read of the table all succeed from the replica alone,
and nothing in the server's log reaches for the stopped source.
"""

# Requirements: REQ-2031

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.embedded_pg import DATABASE, PASSWORD, ROLE, start_local_postgres

pytestmark = [pytest.mark.integration]

_NAME = "never_expires"
_ADMIN = "org_admin"
_ROWS = [("us", 10), ("us", 30), ("eu", 7)]


def _config(pg) -> dict:
    return {
        "federation_engine": "pg",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ADMIN}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "Shop"}],
        "roles": [],
        "sources": [
            {
                "id": "shop_pg",
                "type": "postgresql",
                "host": "127.0.0.1",
                "port": pg.port,
                "database": DATABASE,
                "username": ROLE,
                "password": PASSWORD,
                "cache_ttl": -1,
            }
        ],
        "tables": [
            {
                "source_id": "shop_pg",
                "domain_id": "shop",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {"name": "id", "data_type": "integer", "is_primary_key": True,
                     "visible_to": [_ADMIN]},
                    {"name": "region", "data_type": "varchar", "visible_to": [_ADMIN]},
                    {"name": "amount", "data_type": "integer", "visible_to": [_ADMIN]},
                ],
            }
        ],
    }  # fmt: skip


def _post(srv, path: str, body: dict) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}{path}", json=body, headers={"X-Provisa-Role": _ADMIN}, timeout=120
    )


def _read(srv) -> list[dict]:
    response = _post(
        srv,
        "/data/sql",
        {"sql": 'SELECT region, amount FROM "shop"."orders" ORDER BY id', "role": _ADMIN},
    )
    assert response.status_code == 200, f"{response.text}\n{srv.dump_stderr_debug()[-6000:]}"
    return response.json()["data"]["sql"]


def _landed(srv) -> bool:
    """Whether the table's one landing has completed: until then a read is refused by name."""
    response = _post(
        srv,
        "/data/sql",
        {"sql": 'SELECT COUNT(*) AS n FROM "shop"."orders"', "role": _ADMIN},
    )
    if response.status_code in (503, 504):  # not landed yet, or its landing still running
        assert response.json()["code"] in ("query.replica_not_landed", "data.replica_building")
        return False
    assert response.status_code == 200, f"{response.text}\n{srv.dump_stderr_debug()[-6000:]}"
    return True


@pytest.fixture
def source_pg():
    pg = start_local_postgres()
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        conn.execute("CREATE TABLE orders (id serial PRIMARY KEY, region text, amount int)")
        for row in _ROWS:
            conn.execute("INSERT INTO orders (region, amount) VALUES (%s, %s)", row)
    try:
        yield pg
    finally:
        if (pg.data / "postmaster.pid").exists():
            pg.stop()


def test_a_landed_never_expiring_source_is_never_contacted_again(source_pg):
    from tests.integration.isolated_server import IsolatedServer

    store = start_local_postgres()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            path = Path(workdir) / "config.yaml"
            path.write_text(yaml.safe_dump(_config(source_pg)))

            def _server() -> IsolatedServer:
                return IsolatedServer(
                    _NAME,
                    engine="pg",
                    config=str(path),
                    # The engine runs on the store this test starts, beside its sources: left to the
                    # control plane, it would run in the stack's container, where 127.0.0.1 is not them.
                    env={"PROVISA_MATERIALIZE_URL": store.dsn(), "PROVISA_ENGINE_URL": store.dsn()},
                )

            first = _server()
            try:
                first.start()
                deadline = time.monotonic() + 120
                while not _landed(first):
                    assert time.monotonic() < deadline, first.dump_stderr_debug()[-6000:]
                    time.sleep(1)
                expected = _read(first)
                assert [r["amount"] for r in expected] == [10, 30, 7]
            finally:
                first.stop_process()

            # The source's server goes away for good: its data dir is removed with it.
            source_port = str(source_pg.port)
            source_pg.stop()

            second = _server()
            try:
                second.start()  # boot, reconcile and the model build: none reaches the source
                assert _read(second) == expected  # answered from the replica
                log = second.dump_stderr_debug()
                assert f":{source_port}" not in log and f"port={source_port}" not in log, log[
                    -6000:
                ]
            finally:
                second.stop_process()
    finally:
        store.stop()
