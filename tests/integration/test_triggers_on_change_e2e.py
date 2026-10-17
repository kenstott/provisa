# Copyright (c) 2026 Kenneth Stott
# Canary: 535994af-06b0-4f81-b857-5f108480d92d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: triggers on a table's change run commands, and the commands chain (REQ-2054).

A pg-engine server declares, in its file, three tables of a PostgreSQL source run with
``wal_level=logical``: ``bronze``, of a source on the native change signal served from its
replicas, and ``silver`` and ``gold``, of a source read live. Two statement commands promote rows a step each, and two
triggers run them: one on bronze's change, one on silver's. The first landing of bronze runs the
first command, whose write to silver runs the second; a row inserted into bronze at the source then
reaches gold the same way. Each trigger's last run is listed with its outcome and what fired it.
"""

# Requirements: REQ-2054

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.embedded_pg import DATABASE, PASSWORD, ROLE, start_local_postgres
from tests.helpers import config_write_grants

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_NAME = "triggers_on_change"
_ADMIN = "org_admin"


def _columns() -> list[dict]:
    return [
        {"name": "id", "data_type": "integer", "visible_to": [_ADMIN]},
        {"name": "amount", "data_type": "integer", "visible_to": [_ADMIN]},
    ]  # fmt: skip


def _table(name: str, source_id: str) -> dict:
    return {
        "source_id": source_id,
        "domain_id": "shop",
        "schema": "public",
        "table": name,
        "columns": _columns(),
        "write_grants": config_write_grants([_ADMIN], ["id", "amount"]),  # REQ-2074
    }


def _promote(name: str, source: str, target: str) -> dict:
    return {
        "name": name,
        "function_name": name,
        "impl_kind": "statement",
        "domain_id": "shop",
        "source_id": "",
        "returns": "",
        "binding": {
            "sql": f'INSERT INTO "shop"."{target}" (id, amount) '
            f'SELECT id, amount FROM "shop"."{source}"'
        },
    }


def _source(pg, source_id: str) -> dict:
    return {
        "id": source_id,
        "type": "postgresql",
        "host": "127.0.0.1",
        "port": pg.port,
        "database": DATABASE,
        "username": ROLE,
        "password": PASSWORD,
        "cache_ttl": 3600,
    }


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
        # One database, two sources: bronze's follows its logical replication and serves it from
        # its replica; the other's tables are read live.
        "sources": [
            {**_source(pg, "bronze_pg"), "change_signal": "native", "replicate": 0},
            _source(pg, "shop_pg"),
        ],
        "tables": [
            _table("bronze", "bronze_pg"),
            _table("silver", "shop_pg"),
            _table("gold", "shop_pg"),
        ],
        "functions": [
            _promote("promote_to_silver", "bronze", "silver"),
            _promote("promote_to_gold", "silver", "gold"),
        ],
        "scheduled_triggers": [
            {
                "id": "bronze_to_silver",
                "on": {"table": "bronze_pg.public.bronze", "event": "change"},
                "command": "promote_to_silver",
                "role": _ADMIN,
            },
            {
                "id": "silver_to_gold",
                "on": {"table": "shop_pg.public.silver", "event": "change"},
                "command": "promote_to_gold",
                "role": _ADMIN,
            },
        ],
    }


@pytest.fixture
def source_pg():
    pg = start_local_postgres(settings={"wal_level": "logical", "synchronous_commit": "on"})
    try:
        with psycopg.connect(pg.dsn(), autocommit=True) as conn:
            for name in ("bronze", "silver", "gold"):
                conn.execute(f"CREATE TABLE {name} (id integer, amount integer)")
            conn.execute("INSERT INTO bronze VALUES (1, 10)")
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
                _NAME,
                engine="pg",
                config=str(path),
                control_plane="own",
                env={"PROVISA_MATERIALIZE_URL": store.dsn()},
            )
            try:
                srv.start()
                yield srv
            finally:
                srv.stop_process()
    finally:
        store.stop()


def _ids(pg, table: str) -> set[int]:
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        return {row[0] for row in conn.execute(f"SELECT id FROM {table}").fetchall()}


def _wait(condition, what: str, srv) -> None:
    deadline = time.monotonic() + 120
    while not condition():
        assert time.monotonic() < deadline, f"{what}\n{srv.dump_stderr_debug()[-8000:]}"
        time.sleep(0.5)


def _last_runs(srv) -> dict[str, dict]:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={
            "query": "{ scheduledTasks { id firesOn onTable onEvent command role "
            "lastOutcome lastError lastFiredBy lastRunAt } }"
        },
        headers={"X-Provisa-Role": _ADMIN},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "errors" not in body, body
    return {t["id"]: t for t in body["data"]["scheduledTasks"]}


def test_a_change_runs_its_trigger_and_the_commands_chain(server, source_pg):
    _wait(lambda: _ids(source_pg, "gold") == {1}, "bronze's landing never reached gold", server)

    with psycopg.connect(source_pg.dsn(), autocommit=True) as conn:
        conn.execute("INSERT INTO bronze VALUES (2, 20)")
    _wait(lambda: _ids(source_pg, "gold") == {1, 2}, "an inserted row never reached gold", server)

    runs = _last_runs(server)
    first, second = runs["bronze_to_silver"], runs["silver_to_gold"]
    assert (first["firesOn"], first["onTable"], first["command"]) == (
        "change",
        "bronze_pg.public.bronze",
        "promote_to_silver",
    )
    assert first["lastOutcome"] == "succeeded" and first["lastError"] is None
    assert first["lastFiredBy"] == "change:bronze_pg.public.bronze"
    assert second["lastOutcome"] == "succeeded"
    assert second["lastFiredBy"] == "change:shop_pg.public.silver"
