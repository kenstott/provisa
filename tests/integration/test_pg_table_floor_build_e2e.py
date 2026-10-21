# Copyright (c) 2026 Kenneth Stott
# Canary: df5dd8a6-7602-43aa-9022-2552cdf993f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (#269): on the pg engine, a table put on its replica by its own ``replicate: 0`` -- its
source is not -- has its replica built and read.

The build reads the source table through the engine, which names no catalog (PostgreSQL has no
cross-database reference): it reads it at the folded name every statement there uses. A row
inserted at the source reaches the replica through the rebuild its logical replication asks for.
"""

# Requirements: REQ-826, REQ-1730

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.embedded_pg import DATABASE, PASSWORD, ROLE, start_local_postgres

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ADMIN = "org_admin"


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
                "cache_ttl": 3600,
                "change_signal": "native",  # its logical replication says when it changed
            }
        ],
        "tables": [
            {
                "source_id": "shop_pg",
                "domain_id": "shop",
                "schema": "public",
                "table": "orders",
                "replicate": 0,  # the table's own setting; its source has none
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": [_ADMIN]},
                    {"name": "amount", "data_type": "integer", "visible_to": [_ADMIN]},
                ],
            }
        ],
    }


@pytest.fixture
def source_pg():
    pg = start_local_postgres(settings={"wal_level": "logical", "synchronous_commit": "on"})
    try:
        with psycopg.connect(pg.dsn(), autocommit=True) as conn:
            conn.execute("CREATE TABLE orders (id integer, amount integer)")
            conn.execute("INSERT INTO orders VALUES (1, 10), (2, 20)")
        yield pg
    finally:
        pg.stop()


@pytest.fixture
def server(source_pg):
    from tests.integration.isolated_server import IsolatedServer

    store = start_local_postgres()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            path = Path(workdir) / "config.yaml"
            path.write_text(yaml.safe_dump(_config(source_pg)))
            srv = IsolatedServer(
                "pg_table_floor",
                engine="pg",
                config=str(path),
                control_plane="own",
                # The engine runs on the store this test starts, beside its sources: left to the
                # control plane, it would run in the stack's container, where 127.0.0.1 is not them.
                env={"PROVISA_MATERIALIZE_URL": store.dsn(), "PROVISA_ENGINE_URL": store.dsn()},
            )
            try:
                srv.start()
                yield srv
            finally:
                srv.stop_process()
    finally:
        store.stop()


def _read(srv) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": 'SELECT id, amount FROM "shop"."orders" ORDER BY id', "role": _ADMIN},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=120,
    )


def test_a_table_floored_by_its_own_setting_is_built_and_read_from_its_replica(server, source_pg):
    deadline = time.monotonic() + 120
    while True:
        response = _read(server)
        if response.status_code == 200:
            break
        # Until its first build lands a read waits on it, or is answered that it is building.
        assert response.status_code in (503, 504), response.text
        assert time.monotonic() < deadline, f"{response.text}\n{server.dump_stderr_debug()[-6000:]}"
        time.sleep(1)
    assert [r["id"] for r in response.json()["data"]["sql"]] == [1, 2]
    # The feed asks for a rebuild, and the rebuild reads the source again.
    with psycopg.connect(source_pg.dsn(), autocommit=True) as conn:
        conn.execute("INSERT INTO orders VALUES (3, 30)")
    while [r["id"] for r in _read(server).json()["data"]["sql"]] != [1, 2, 3]:
        assert time.monotonic() < deadline + 60, server.dump_stderr_debug()[-6000:]
        time.sleep(1)
    assert "cross-database" not in server.dump_stderr_debug()
