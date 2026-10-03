# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e9d15-2c86-4a40-b9f7-0d5e1c4a8f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An audit row names what makes its provenance checkable, on a real server: the model stamp
in force and the commit it equals, what was enforced on the statement (row filters with the
names of the session variables they read, masks by kind, row caps, a write's table and columns),
why it took its route and what it read, and how old the rows it was answered with were."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROLES = ["org_admin", "east_reader"]


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    orders["columns"] = [
        {
            "name": "id",
            "data_type": "integer",
            "visible_to": _ROLES,
            "writable_by": ["org_admin"],
            "is_primary_key": True,
        },
        {
            "name": "region",
            "data_type": "varchar",
            "visible_to": _ROLES,
            "writable_by": ["org_admin"],
            "mask_type": "constant",
            "mask_value": "***",
            "unmasked_to": ["org_admin"],
        },
    ]
    reads = ["query_development", "full_results"]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": [
                {"id": "org_admin", "capabilities": [*reads, "write"], "domain_access": ["*"]},
                {
                    "id": "east_reader",
                    "capabilities": ["query_development"],
                    "domain_access": ["*"],
                },
            ],
            "rls_rules": [
                {
                    "table_id": "orders",
                    "role_id": "east_reader",
                    "filter": "id < 100 AND region <> current_setting('provisa.blocked_region')",
                }
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _post(boot, role: str, path: str, body: dict, headers: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role, **(headers or {})},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return {"status": resp.status, "cache": resp.headers.get("X-Provisa-Cache")}


def _audit_rows(boot, n: int) -> list[dict]:
    """The newest ``n`` audit rows, waiting for the writer (it inserts in batches)."""
    engine = sa.create_engine(boot.url)
    try:
        with engine.connect() as conn:
            schema = conn.execute(
                sa.text(
                    "SELECT table_schema FROM information_schema.tables "
                    "WHERE table_name = 'query_audit_log' AND table_schema LIKE 'org%' LIMIT 1"
                )
            ).scalar_one()
        deadline = time.monotonic() + 60
        while True:
            with engine.connect() as conn:
                rows = [
                    dict(r._mapping)
                    for r in conn.execute(
                        sa.text(
                            f"SELECT role_id, route, model_stamp, model_commit, enforced, "
                            f"route_reason, sources, data_age FROM {schema}.query_audit_log "
                            f"ORDER BY id DESC LIMIT {n}"
                        )
                    )
                ]
            if len(rows) >= n or time.monotonic() > deadline:
                return list(reversed(rows))
            time.sleep(0.5)
    finally:
        engine.dispose()


def test_a_read_a_cache_hit_and_a_write_record_their_provenance(server):
    _post(
        server,
        "east_reader",
        "/data/sql",
        {"sql": "SELECT id, region FROM sales.orders"},
        {"x-provisa-session-blocked_region": "west"},  # a value: never recorded
    )
    cached = {"sql": "-- @provisa cache=true\nSELECT id FROM sales.orders ORDER BY id"}
    _post(server, "org_admin", "/data/sql", cached)
    hit = _post(server, "org_admin", "/data/sql", cached)
    assert hit["cache"] == "HIT"
    _post(
        server,
        "org_admin",
        "/data/sql",
        {"sql": "INSERT INTO sales.orders (id, region) VALUES (90, 'north')"},
    )

    read, _miss, cache_hit, write = _audit_rows(server, 4)

    assert read["role_id"] == "east_reader"
    assert isinstance(read["model_stamp"], int)
    enforced = read["enforced"]
    ((table_id, row_filter),) = enforced["row_filters"].items()
    assert row_filter["session_variables"] == ["blocked_region"]
    assert "west" not in json.dumps(enforced)  # the session variable's value is not recorded
    assert enforced["masks"] == {table_id: {"region": "constant"}}
    assert enforced["row_cap"] is not None  # east_reader has no full_results
    assert read["route_reason"]
    assert read["sources"] == ["sales-pg"]
    assert read["data_age"] is None  # read live

    # The model the statement ran on is named by its commit: a fresh server's model is the one its
    # boot committed, at the stamp it was built at. With the commit, the visible columns are left
    # out of the row (the commit holds them).
    assert read["model_commit"] is not None and len(read["model_commit"]) == 40, read
    assert "visible_columns" not in enforced

    assert cache_hit["route"] == "cache"
    assert isinstance(cache_hit["data_age"]["cache_age_seconds"], int)

    assert write["enforced"]["write"] == {
        "table": int(table_id),
        "columns": ["id", "region"],
        "row_filter": False,
    }
    assert "row_cap" not in write["enforced"]


# --- the SQLite control plane ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def sqlite_server():
    import tempfile
    from pathlib import Path

    from tests.integration.isolated_server import IsolatedServer

    store_dir = tempfile.TemporaryDirectory()
    srv = IsolatedServer(
        "audit_provenance_sqlite",
        engine="duckdb",
        config="tests/fixtures/duckdb_sqlite_config.yaml",
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()
        store_dir.cleanup()


def test_the_provenance_columns_land_on_a_sqlite_control_plane(sqlite_server):
    import sqlite3
    from pathlib import Path

    req = urllib.request.Request(
        f"{sqlite_server.base_url}/data/sql",
        data=json.dumps({"sql": "SELECT pet_id FROM shelter.inquiries"}).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        assert resp.status == 200

    deadline = time.monotonic() + 60
    row = None
    while row is None and time.monotonic() < deadline:
        for db in Path(sqlite_server._tmpdir.name).rglob("*.db"):
            with sqlite3.connect(db) as conn:
                has = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE name = 'query_audit_log'"
                ).fetchone()
                if has:
                    row = conn.execute(
                        "SELECT model_stamp, enforced, route_reason, sources, data_age "
                        "FROM query_audit_log WHERE role_id = 'org_admin' ORDER BY id DESC LIMIT 1"
                    ).fetchone()
                    if row:
                        break
        time.sleep(0.5)
    assert row is not None, "no audit row landed"
    model_stamp, enforced, route_reason, sources, data_age = row
    assert isinstance(model_stamp, int)
    assert set(json.loads(enforced)) >= {"row_filters", "masks"}
    assert route_reason
    assert json.loads(sources) == ["inquiries-sqlite"]
    assert data_age is None
