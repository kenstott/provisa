# Copyright (c) 2026 Kenneth Stott
# Canary: 4d8a2c67-f1b3-4e09-a75d-c3e6b9f02a18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: replicating a table costs the worker a bounded amount of memory, and one copy.

The first read of a replicated source builds its replica. That build read the whole source table
into the worker's heap (a row tuple, a dict per row, then a tuple per row again for the INSERT) —
about 3 KB of resident memory per row of a 26-column table — and every worker that received a
read started its own copy. A 20M-row table took the workers down.

A real pg-engine server over a SOURCE Postgres and an ENGINE Postgres (the test's own containers,
both logging every statement):

* the worker's resident memory during the first replication stays under a fixed bound that does
  not depend on the row count;
* two server processes on one control plane, both asked for the table at once, read the source
  once between them.
"""

# Requirements: REQ-826, REQ-1661

from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path

import httpx
import psutil
import pytest
import yaml

from tests.integration.test_pg_engine_landing_never_writes_source_e2e import (
    _ROLE,
    _SourceAndEngine,
)

pytestmark = [pytest.mark.integration]

_COLUMNS = [f"c{i:02d}" for i in range(25)]
_ROWS = 150_000
# The whole-table-in-memory build grew the worker by ~3 KB per row: ~450 MB for this table.
# A build that moves rows in bounded batches (or not through the worker at all) grows it by the
# size of a batch, whatever the table's size.
_RSS_GROWTH_BOUND = 96 * 1024 * 1024


def _config(pg: _SourceAndEngine) -> dict:
    columns = [
        {"name": "id", "data_type": "integer", "visible_to": [_ROLE], "is_primary_key": True}
    ] + [{"name": name, "data_type": "varchar", "visible_to": [_ROLE]} for name in _COLUMNS]
    return {
        "federation_engine": "pg",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "Shop"}],
        "roles": [
            {
                "id": _ROLE,
                "capabilities": ["source_registration", "table_registration", "query_development"],
                "domain_access": ["*"],
            }
        ],
        "sources": [
            {
                "id": "src",
                "type": "postgresql",
                "host": "127.0.0.1",
                "port": pg.source_port,
                "database": "shop",
                "username": "provisa",
                "password": "provisa",
                "replicate": 0,
                "cache_ttl": 300,
            }
        ],
        "tables": [
            {
                "source_id": "src",
                "table": "wide",
                "schema": "public",
                "domain_id": "shop",
                "columns": columns,
            }
        ],
    }


@pytest.fixture
def databases():
    import psycopg

    pg = _SourceAndEngine()
    pg.start()
    try:
        with psycopg.connect(pg.url(pg.source_port, "shop"), autocommit=True) as conn:
            conn.execute(
                "CREATE TABLE wide (id INTEGER PRIMARY KEY, "
                + ", ".join(f"{name} TEXT" for name in _COLUMNS)
                + ")"
            )
            conn.execute(
                "INSERT INTO wide SELECT i, "
                + ", ".join(f"md5((i + {k})::text)" for k in range(len(_COLUMNS)))
                + f" FROM generate_series(1, {_ROWS}) i"
            )
        yield pg
    finally:
        pg.stop()


def _server(pg: _SourceAndEngine, workdir: str, *, request_timeout: str = "600"):
    from tests.integration.isolated_server import IsolatedServer

    path = Path(workdir) / "config.yaml"
    path.write_text(yaml.safe_dump(_config(pg)))
    control_plane = pg.url(pg.engine_port, "provisa", "+psycopg")
    return IsolatedServer(
        "replication_bounded",
        engine="pg",
        config=str(path),
        control_plane="postgres",
        env={
            "PLATFORM_DATABASE_URL": control_plane,
            "TENANT_DATABASE_URL": control_plane,
            "PROVISA_MATERIALIZE_URL": pg.url(pg.engine_port, "provisa"),
            "PROVISA_REDIS_EMBEDDED": "1",
            "PROVISA_REQUEST_TIMEOUT": request_timeout,
        },
    )


def _read_one(srv) -> None:
    response = httpx.post(
        f"{srv.base_url}/data/graphql",
        json={"query": "{ wide(limit: 1) { id } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=900,
    )
    assert response.status_code == 200, response.text
    assert response.json()["data"]["wide"] == [{"id": 1}]


def _replica_rows(pg: _SourceAndEngine) -> int:
    import psycopg

    with psycopg.connect(pg.url(pg.engine_port, "provisa"), autocommit=True) as conn:
        return conn.execute(
            'SELECT COUNT(*) FROM "org_replication_bounded_replicas"."src__public__wide"'
        ).fetchone()[0]


def _source_table_reads(pg: _SourceAndEngine) -> list[str]:
    """Statements the SOURCE executed that read ``wide`` in full (no key lookups, no metadata)."""
    return [
        line.split("db=shop ", 1)[1]
        for line in pg._source_log()
        if "db=shop " in line
        and "wide" in line
        and "SELECT" in line
        and "information_schema" not in line
        and "generate_series" not in line
        and "pg_catalog" not in line
    ]


def test_replicating_a_table_does_not_grow_the_worker_with_its_row_count(databases):
    pg = databases
    with tempfile.TemporaryDirectory() as workdir:
        srv = _server(pg, workdir)
        try:
            srv.start()
            worker = psutil.Process(srv._proc.pid)
            before = worker.memory_info().rss
            peak = [before]
            done = threading.Event()

            def _watch() -> None:
                while not done.is_set():
                    peak[0] = max(peak[0], worker.memory_info().rss)
                    time.sleep(0.05)

            watcher = threading.Thread(target=_watch, daemon=True)
            watcher.start()
            try:
                _read_one(srv)  # the first read: the replica is built
            finally:
                done.set()
                watcher.join()
            assert _replica_rows(pg) == _ROWS
        finally:
            srv.stop_process()
    growth = peak[0] - before
    assert growth < _RSS_GROWTH_BOUND, (
        f"replicating {_ROWS} rows grew the worker by {growth / 1048576:.0f} MB "
        f"({growth / _ROWS / 1024:.2f} KB per row)"
    )


def test_two_workers_asked_at_once_read_the_source_once(databases):
    pg = databases
    with tempfile.TemporaryDirectory() as dir_a, tempfile.TemporaryDirectory() as dir_b:
        first, second = _server(pg, dir_a), _server(pg, dir_b)
        try:
            first.start()
            second.start()
            errors: list[BaseException] = []

            def _ask(srv) -> None:
                try:
                    _read_one(srv)
                except BaseException as exc:  # noqa: BLE001 - reported below, on the test thread
                    errors.append(exc)

            readers = [threading.Thread(target=_ask, args=(srv,)) for srv in (first, second)]
            for reader in readers:
                reader.start()
            for reader in readers:
                reader.join()
            assert not errors, errors
            assert _replica_rows(pg) == _ROWS
        finally:
            first.stop_process()
            second.stop_process()
    reads = _source_table_reads(pg)
    assert len(reads) == 1, reads


def test_a_read_that_cannot_wait_for_the_build_fails_by_name_and_the_build_goes_on(databases):
    """The source table is locked, so the copy cannot finish inside the read's 3-second deadline.
    The read fails saying the replica is still being built; the build is not cancelled and not
    started again — once the source is released the next read is served, and the source was
    read once."""
    import psycopg

    pg = databases
    with tempfile.TemporaryDirectory() as workdir:
        srv = _server(pg, workdir, request_timeout="3")
        try:
            srv.start()
            blocker = psycopg.connect(pg.url(pg.source_port, "shop"))
            try:
                blocker.execute("LOCK TABLE wide IN ACCESS EXCLUSIVE MODE")
                for _ in range(2):  # the second read joins the running build; it starts no copy
                    refused = httpx.post(
                        f"{srv.base_url}/data/graphql",
                        json={"query": "{ wide(limit: 1) { id } }"},
                        headers={"X-Provisa-Role": _ROLE},
                        timeout=60,
                    )
                    assert refused.status_code >= 500, refused.text
                    assert "the replica of src.public.wide is still being built" in refused.text
            finally:
                blocker.rollback()  # releases the lock: the build can now read the source
                blocker.close()
            deadline = time.monotonic() + 120
            while True:
                served = httpx.post(
                    f"{srv.base_url}/data/graphql",
                    json={"query": "{ wide(limit: 1) { id } }"},
                    headers={"X-Provisa-Role": _ROLE},
                    timeout=60,
                )
                if served.status_code == 200:
                    break
                assert "still being built" in served.text, served.text
                assert time.monotonic() < deadline, "the build never finished"
                time.sleep(1)
            assert served.json()["data"]["wide"] == [{"id": 1}]
            assert _replica_rows(pg) == _ROWS
        finally:
            srv.stop_process()
    assert len(_source_table_reads(pg)) == 1, _source_table_reads(pg)
