# Copyright (c) 2026 Kenneth Stott
# Canary: 5a0c7e39-4d18-4b62-93f7-e8b2d6a1c054
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: Hot promotion sizes a busy table the way the pg engine addresses it (REQ-826,
REQ-239).

A real server on the Postgres federation engine, over a Postgres source it reaches through
postgres_fdw. Postgres has no catalog level — ``"catalog"."schema"."table"`` is refused with
"cross-database references are not implemented" — so the engine exposes a source's table as
``"<catalog>_<schema>"."<table>"``. Once the governed statements that read a table pass its
threshold, the evaluation's size check must name it that way; a table over the size ceiling is
then left live, and the admin summary says why.

One server process and the embedded Redis: the one process sees every statement, so promotion
is decided here (``replica_hot.promotion_runs``).

The Postgres here is the test's own container (engine, control plane and source in one server,
listening on the same port inside the container as on the host, so the engine's foreign server and
the host's direct driver dial the same address). It logs every statement, which is how the test
sees what the sweep actually sent.
"""

# Requirements: REQ-826, REQ-239, REQ-1730

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_ROWS = 50
_SIZE_CHECK = 'SELECT COUNT(*) FROM "warm_src_public"."orders"'


class _Postgres:
    """A throwaway Postgres container that logs every statement it executes."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.port = lease_ports(1)[0]
        self.name = f"provisa-itest-warm-pg-{os.getpid()}"

    def url(self, database: str, driver: str = "") -> str:
        return f"postgresql{driver}://provisa:provisa@127.0.0.1:{self.port}/{database}"

    def start(self) -> None:
        import psycopg

        port = str(self.port)
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", self.name]
            + ["-e", "POSTGRES_USER=provisa", "-e", "POSTGRES_PASSWORD=provisa"]
            + ["-e", "POSTGRES_DB=provisa", "-p", f"127.0.0.1:{port}:{port}", "postgres:16"]
            + ["-c", f"port={port}", "-c", "log_statement=all"],
            check=True,
            capture_output=True,
        )
        # The image restarts the server once after initdb: ready means it answers repeatedly.
        deadline, answered = time.monotonic() + 90, 0
        while answered < 3:
            try:
                psycopg.connect(self.url("provisa"), connect_timeout=2).close()
                answered += 1
            except psycopg.OperationalError:
                answered = 0
                if time.monotonic() > deadline:
                    raise
            time.sleep(1)
        with psycopg.connect(self.url("provisa"), autocommit=True) as conn:
            conn.execute("CREATE DATABASE shop")
        with psycopg.connect(self.url("shop"), autocommit=True) as conn:
            conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount DOUBLE PRECISION)")
            conn.execute(f"INSERT INTO orders SELECT i, i * 1.5 FROM generate_series(1, {_ROWS}) i")

    def log(self) -> str:
        out = subprocess.run(
            ["docker", "logs", self.name], check=True, capture_output=True, text=True
        )
        return out.stdout + out.stderr

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self.name], check=True, capture_output=True)


def _config(pg: _Postgres) -> dict:
    return {
        "federation_engine": "pg",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        # Judged every second against 3 statements per second. The table holds more rows than
        # the ceiling, so each evaluation that finds it busy sizes it and leaves it live.
        "replication": {"hot_threshold": 3, "hot_max_rows": _ROWS - 1, "hot_interval": 1},
        "domains": [{"id": "shop", "description": "Shop"}],
        "roles": [
            {
                "id": _ROLE,
                "capabilities": [
                    "source_registration",
                    "table_registration",
                    "query_development",
                    "observability",
                ],
                "domain_access": ["*"],
            }
        ],
        "sources": [
            {
                "id": "warm-src",
                "type": "postgresql",
                "host": "127.0.0.1",
                "port": pg.port,
                "database": "shop",
                "username": "provisa",
                "password": "provisa",
                # The replication clock a Default table needs before it may be promoted (REQ-1907):
                # without one it is left live however busy, and never sized.
                "cache_ttl": 86400,
            }
        ],
        "tables": [
            {
                "source_id": "warm-src",
                "table": "orders",
                "schema": "public",
                "domain_id": "shop",
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": [_ROLE]},
                    {"name": "amount", "data_type": "double", "visible_to": [_ROLE]},
                ],
            }
        ],
    }


@pytest.fixture(scope="module")
def stack():
    from tests.integration.isolated_server import IsolatedServer

    pg = _Postgres()
    pg.start()
    work = tempfile.TemporaryDirectory()
    config_path = Path(work.name) / "config.yaml"
    config_path.write_text(yaml.safe_dump(_config(pg)))
    control_plane = pg.url("provisa", "+psycopg")
    srv = IsolatedServer(
        "warm_pg_engine",
        engine="pg",
        config=str(config_path),
        control_plane="postgres",
        env={
            "PLATFORM_DATABASE_URL": control_plane,
            "TENANT_DATABASE_URL": control_plane,
            "PROVISA_MATERIALIZE_URL": pg.url("provisa"),
            "PROVISA_REDIS_EMBEDDED": "1",
        },
    )
    try:
        srv.start()
        yield srv, pg
    finally:
        srv.stop_process()
        pg.stop()
        work.cleanup()


def _read(srv, limit: int) -> None:
    response = httpx.post(
        f"{srv.base_url}/data/graphql",
        json={"query": f"{{ orders(limit: {limit}) {{ id amount }} }}"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )
    assert response.status_code == 200, response.text
    assert len(response.json()["data"]["orders"]) == limit


def _admin(srv, query: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert not body.get("errors"), body
    return body["data"]


def test_the_evaluation_sizes_a_busy_table_in_the_pg_engines_own_naming(stack):
    srv, pg = stack
    # Keep the table busy — well past 3 governed statements a second — until an evaluation has
    # found it so and sized it through the engine.
    deadline = time.monotonic() + 60
    while _SIZE_CHECK not in pg.log():
        assert time.monotonic() < deadline, (
            f"the table was never sized:\n{srv.dump_stderr_debug()[-3000:]}"
        )
        for limit in range(1, 9):
            _read(srv, limit)

    log = pg.log()
    assert "cross-database references are not implemented" not in log
    assert '"warm_src"."public"."orders"' not in log
    stderr = srv.dump_stderr_debug()
    assert "could not size the table" not in stderr, stderr[-3000:]
    assert "Hot promotion evaluation failed" not in stderr, stderr[-3000:]

    # Over the ceiling: not promoted, read live, and — while it stays busy — the summary states
    # the reason (the reason is kept for two intervals after the evaluation that found it).
    deadline = time.monotonic() + 60
    while True:
        for limit in range(1, 9):
            _read(srv, limit)  # still answered, from the source
        tables = _admin(srv, "{ tables { tableName refreshPolicySummary { text serving } } }")
        orders = next(t for t in tables["tables"] if t["tableName"] == "orders")
        assert orders["refreshPolicySummary"]["serving"] == "live", orders
        if "replication.hot_max_rows" in orders["refreshPolicySummary"]["text"]:
            break
        assert time.monotonic() < deadline, orders
    assert f"more than {_ROWS - 1} rows" in orders["refreshPolicySummary"]["text"], orders
    kept = _admin(srv, "{ hotTables { tableName kind } }")["hotTables"]
    assert [t for t in kept if t["tableName"] == "orders"] == [], kept
