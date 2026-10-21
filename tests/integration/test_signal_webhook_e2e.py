# Copyright (c) 2026 Kenneth Stott
# Canary: 911cfc60-fa4a-4351-88e0-bb2e9cd80d88
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a table on the signal change signal is rebuilt when its webhook says the source changed
(REQ-1149).

A pg-engine server reads a PostgreSQL table, registered through the admin API on the ``signal``
change signal and served from its replica. The first read builds the replica. A row added at
the source stays out of reads -- the table is fresh until a signal says otherwise, and no clock
rebuilds it -- until POST /data/sources/{id}/signal is called; then the rebuild it asked for
brings the row into reads over /data/sql.
"""

# Requirements: REQ-1149

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.embedded_pg import DATABASE, PASSWORD, ROLE, start_local_postgres
from tests.helpers import registered_id, release_field

pytestmark = [pytest.mark.integration]

_NAME = "signal_webhook"
_ADMIN = "org_admin"


def _config() -> dict:
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
        "sources": [],
        "tables": [],
    }


@pytest.fixture
def source_pg():
    pg = start_local_postgres()
    try:
        with psycopg.connect(pg.dsn(), autocommit=True) as conn:
            conn.execute("CREATE TABLE orders (id integer PRIMARY KEY, name text)")
            conn.execute("INSERT INTO orders VALUES (1, 'first')")
        yield pg
    finally:
        pg.stop()


@pytest.fixture
def server():
    from tests.integration.isolated_server import IsolatedServer

    store = start_local_postgres()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            path = Path(workdir) / "config.yaml"
            path.write_text(yaml.safe_dump(_config()))
            srv = IsolatedServer(
                _NAME,
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


def _mutate(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _read(srv) -> list[int]:
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": "SELECT id, name FROM orders ORDER BY id", "role": _ADMIN},
        timeout=120,
    )
    assert response.status_code == 200, f"{response.text}\n{srv.dump_stderr_debug()[-6000:]}"
    body = response.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    return [int(r["id"]) for r in rows]


def test_a_signal_brings_the_sources_change_into_reads(server, source_pg):
    _mutate(
        server,
        f'createSource(input: {{id: "shop_pg", type: "postgresql", host: "127.0.0.1", '
        f'port: {source_pg.port}, database: "{DATABASE}", username: "{ROLE}", '
        f'password: "{PASSWORD}", changeSignal: "signal"}})',
    )
    registered = _mutate(
        server,
        'registerTable(input: {sourceId: "shop_pg", domainId: "shop", schemaName: "public", '
        'tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "name", visibleTo: ["org_admin"], dataType: "varchar"}]})',
    )
    _mutate(server, release_field(registered_id(registered["message"])))
    _mutate(server, 'updateSourceReplicate(sourceId: "shop_pg", replicate: 0)')

    assert _read(server) == [1]
    with psycopg.connect(source_pg.dsn(), autocommit=True) as conn:
        conn.execute("INSERT INTO orders VALUES (2, 'second')")
    time.sleep(5)
    assert _read(server) == [1], "the table was refreshed with no signal"

    signalled = httpx.post(
        f"{server.base_url}/data/sources/shop_pg/signal",
        headers={"X-Provisa-Role": _ADMIN},
        timeout=60,
    )
    assert signalled.status_code == 202, signalled.text
    assert signalled.json() == {"signalled": ["public.orders"]}
    deadline = time.monotonic() + 60
    while _read(server) != [1, 2]:
        assert time.monotonic() < deadline, (
            f"the signal never brought the row in\n{server.dump_stderr_debug()[-6000:]}"
        )
        time.sleep(1)
