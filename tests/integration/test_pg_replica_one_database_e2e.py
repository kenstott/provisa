# Copyright (c) 2026 Kenneth Stott
# Canary: cb66d596-21d8-4560-bccd-76c0ab328dab
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On real PostgreSQL, a pg engine in a database of its own beside the platform's: a row-level
replica and a whole-table replica land where the engine reads them, a land of many rows runs in
sets, and a server given a store the engine cannot read does not start."""

from __future__ import annotations

import asyncio
import os
import subprocess
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from sqlalchemy import event
from sqlalchemy.engine import Engine

pytestmark = [pytest.mark.integration]

_SCHEMA = "org_one_database_replicas"
_TABLE = "src__public__orders"
_COLUMNS = [("id", "bigint"), ("status", "text"), ("doc", "json"), ("at", "timestamp")]
_ROWS = 20_000


class _Postgres:
    """One server holding a platform database and an engine database."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.port = lease_ports(1)[0]
        self._name = f"provisa-itest-onedb-{os.getpid()}"

    def url(self, database: str, driver: str = "") -> str:
        return f"postgresql{driver}://provisa:provisa@127.0.0.1:{self.port}/{database}"

    def start(self) -> None:
        import psycopg

        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", self._name]
            + ["-e", "POSTGRES_USER=provisa", "-e", "POSTGRES_PASSWORD=provisa"]
            + ["-e", "POSTGRES_DB=platform", "-p", f"127.0.0.1:{self.port}:5432", "postgres:16"],
            check=True,
            capture_output=True,
        )
        deadline, answered = time.monotonic() + 120, 0
        while answered < 3:  # the image restarts the server once after initdb
            try:
                psycopg.connect(self.url("platform"), connect_timeout=2).close()
                answered += 1
            except psycopg.OperationalError:
                answered = 0
                if time.monotonic() > deadline:
                    raise
            time.sleep(1)
        with psycopg.connect(self.url("platform"), autocommit=True) as conn:
            conn.execute("CREATE DATABASE engine_db")

    def holds(self, database: str, schema: str, table: str) -> bool:
        import psycopg

        with psycopg.connect(self.url(database), autocommit=True) as conn:
            found = conn.execute("SELECT to_regclass(%s)", (f'"{schema}"."{table}"',)).fetchone()
        return found is not None and found[0] is not None

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self._name], check=True, capture_output=True)


@pytest.fixture(scope="module")
def postgres():
    pg = _Postgres()
    pg.start()
    try:
        yield pg
    finally:
        pg.stop()


@pytest.fixture
def engine(postgres, monkeypatch):
    """The bench's shape: the engine's URL set, no store named."""
    from provisa.federation.engine import build_pg_engine

    monkeypatch.setenv("PROVISA_ENGINE_URL", postgres.url("engine_db"))
    monkeypatch.setenv("TENANT_DATABASE_URL", postgres.url("platform", "+psycopg"))
    monkeypatch.delenv("PROVISA_MATERIALIZE_URL", raising=False)
    built = build_pg_engine()
    built.backend._runtime = built.backend._new_runtime()
    monkeypatch.setattr(built.backend, "_runtime_for", lambda _state: built.backend._runtime)
    return built


def _rows(n: int, status: str) -> list[dict]:
    return [
        {"id": i, "status": status, "doc": {"n": i}, "at": "2026-01-01T00:00:00Z"} for i in range(n)
    ]


def _land(engine, name: str, rows: list[dict]) -> None:
    from provisa.federation import query_residency

    backend = engine.backend
    runtime_face = SimpleNamespace(engine=engine)

    async def _run() -> None:
        table = await query_residency._ensure_row_cache_table(
            runtime_face, backend, None, _SCHEMA, name, _COLUMNS
        )
        await query_residency._land_row_cache(
            runtime_face, backend, None, _SCHEMA, name, table, ["id"], _COLUMNS, rows, 300
        )

    asyncio.run(_run())


def _engine_reads(engine, name: str) -> list[tuple]:
    """Through the engine's own read path, as a query is."""
    sql = f'SELECT id, status FROM "{_SCHEMA}"."{name}" ORDER BY id'
    return [tuple(row) for row in engine.backend._runtime.run_sync(sql).rows]


def test_a_row_level_replica_is_written_and_read_through_one_database(engine, postgres):
    _land(engine, _TABLE, _rows(3, "new"))
    assert postgres.holds("engine_db", _SCHEMA, _TABLE)
    assert not postgres.holds("platform", _SCHEMA, _TABLE)
    assert _engine_reads(engine, _TABLE) == [(0, "new"), (1, "new"), (2, "new")]


def test_a_whole_table_replica_lands_in_that_same_database(engine):
    from provisa.federation.replica_address import ReplicaAddress

    target = engine.backend.replica_target(
        None,
        address=ReplicaAddress(_SCHEMA, "whole"),
        args=SimpleNamespace(columns=_COLUMNS),
        engine=SimpleNamespace(caps=SimpleNamespace(runs=frozenset())),
    )
    assert "/engine_db" in target._dsn and "/engine_db" in engine.materialize_store()


def test_landing_many_rows_runs_in_sets_and_replaces_each_key(engine):
    name = "many"
    _land(engine, name, _rows(_ROWS, "new"))

    seen: list[str] = []

    def _record(_conn, _cursor, statement, _params, _context, _executemany) -> None:
        seen.append(statement)

    event.listen(Engine, "before_cursor_execute", _record)
    try:
        _land(engine, name, _rows(_ROWS, "next"))
    finally:
        event.remove(Engine, "before_cursor_execute", _record)

    held = _engine_reads(engine, name)
    assert len(held) == _ROWS and {status for _id, status in held} == {"next"}
    writes = [s for s in seen if s.lstrip().split()[0] in ("INSERT", "UPDATE", "DELETE")]
    assert not [s for s in writes if s.lstrip().startswith(("UPDATE", "INSERT"))]
    assert len(writes) <= _ROWS // 1000, f"{len(writes)} write statements for {_ROWS} rows"


def test_a_server_given_a_store_the_engine_cannot_read_does_not_start(postgres):
    from tests.integration.isolated_server import IsolatedServer

    control_plane = postgres.url("platform", "+psycopg")
    with tempfile.TemporaryDirectory(prefix="one-database-") as workdir:
        path = Path(workdir) / "config.yaml"
        path.write_text(
            yaml.safe_dump(
                {
                    "federation_engine": "pg",
                    "naming": {"domain_prefix": False, "rules": []},
                    "cache": {"enabled": False},
                    "auth": {"provider": "none"},
                    "domains": [{"id": "shop", "description": "Shop"}],
                    "roles": [],
                    "sources": [],
                    "tables": [],
                }
            )
        )
        srv = IsolatedServer(
            "one_database",
            engine="pg",
            config=str(path),
            control_plane="postgres",
            env={
                "PLATFORM_DATABASE_URL": control_plane,
                "TENANT_DATABASE_URL": control_plane,
                "PROVISA_ENGINE_URL": postgres.url("engine_db"),
                "PROVISA_MATERIALIZE_URL": postgres.url("platform"),
                "PROVISA_REDIS_EMBEDDED": "1",
            },
        )
        try:
            with pytest.raises(RuntimeError) as refused:
                srv.start(timeout=120)
        finally:
            srv.stop_process()
    assert "StoreOutsideEngineDatabase" in str(refused.value)
    assert "engine_db" in str(refused.value)
