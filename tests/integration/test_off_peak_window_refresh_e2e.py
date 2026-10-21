# Copyright (c) 2026 Kenneth Stott
# Canary: 13fd47fa-66ec-4251-be2f-4f8c3a82b867
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a load-protected source is refreshed only in its off-peak window (REQ-1141).

A pg-engine server reads a PostgreSQL table through the admin API. Its source is load-protected
with a five-second cadence and an off-peak window that opens a minute or two from now. The
first read lands the table. A row added at the source then stays out of reads over /data/sql
while the window is closed -- the refresh its cadence asks for is held, and the build records
say why -- and appears once the window opens and the held refresh runs.
"""

# Requirements: REQ-1141

from __future__ import annotations

import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.embedded_pg import DATABASE, PASSWORD, ROLE, start_local_postgres
from tests.helpers import registered_id, release_field

pytestmark = [pytest.mark.integration]

_NAME = "off_peak_window"
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


def _admin(srv, document: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": document},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    return response.json()["data"]


def _mutate(srv, mutation: str) -> dict:
    (result,) = _admin(srv, "mutation { " + mutation + " { success message } }").values()
    assert result["success"], result["message"]
    return result


def _build(srv) -> dict:
    (build,) = [
        b
        for b in _admin(
            srv, "{ replicaBuilds { builds { tableName state completedAt waitingOnCode } } }"
        )["replicaBuilds"]["builds"]
        if b["tableName"] == "orders"
    ]
    return build


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


def _window() -> tuple[datetime, str]:
    """A window opening at the first whole minute at least 75 seconds from now, two minutes
    long, as HH:MM-HH:MM in UTC."""
    start = (datetime.now(UTC) + timedelta(seconds=75)).replace(second=0, microsecond=0)
    start += timedelta(minutes=1)
    end = start + timedelta(minutes=2)
    return start, f"{start:%H:%M}-{end:%H:%M}"


def test_a_load_protected_refresh_waits_for_the_off_peak_window(server, source_pg):
    _mutate(
        server,
        f'createSource(input: {{id: "shop_pg", type: "postgresql", host: "127.0.0.1", '
        f'port: {source_pg.port}, database: "{DATABASE}", username: "{ROLE}", '
        f'password: "{PASSWORD}", changeSignal: "ttl", cacheTtl: 5}})',
    )
    registered = _mutate(
        server,
        'registerTable(input: {sourceId: "shop_pg", domainId: "shop", schemaName: "public", '
        'tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "name", visibleTo: ["org_admin"], dataType: "varchar"}]})',
    )
    _mutate(server, release_field(registered_id(registered["message"])))
    opens, window = _window()
    _mutate(
        server,
        f'updateSourceLoadProtection(sourceId: "shop_pg", loadProtected: true, '
        f'offPeakWindow: "{window}", offPeakTz: "UTC")',
    )

    assert _read(server) == [1]  # the first landing is never held
    with psycopg.connect(source_pg.dsn(), autocommit=True) as conn:
        conn.execute("INSERT INTO orders VALUES (2, 'second')")

    # The cadence asks for a refresh within seconds; the closed window holds it.
    deadline = time.monotonic() + 30
    while _build(server)["waitingOnCode"] != "replication.waiting_off_peak_window":
        assert time.monotonic() < deadline, f"no refresh was held: {_build(server)}"
        time.sleep(1)
    assert datetime.now(UTC) < opens, "the window opened before the hold was seen; rerun"
    assert _read(server) == [1]

    # Once the window opens the held refresh runs.
    deadline = time.monotonic() + (opens - datetime.now(UTC)).total_seconds() + 90
    while _read(server) != [1, 2]:
        assert time.monotonic() < deadline, (
            f"the window opened and the refresh never ran: {_build(server)}\n"
            f"{server.dump_stderr_debug()[-6000:]}"
        )
        time.sleep(2)
    assert datetime.now(UTC) >= opens, "the refresh ran before the window opened"
