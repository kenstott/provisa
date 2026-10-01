# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2b9c41-d05a-4f36-8b17-a94c3e6d2f80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: landing a source on the pg engine never writes to the source (REQ-826, REQ-1141).

On the Postgres federation engine a source read live is exposed as a VIEW named
``"<catalog>_<schema>"."<table>"`` over its postgres_fdw foreign table, and a landed source's copy
is a TABLE at that same name. When a source that has been read live becomes one that lands
(``prefer_materialized`` / ``load_protected`` newly set), the view is still there: the land's
``DELETE`` and ``INSERT`` are then issued against the view, and postgres_fdw carries them to the
SOURCE database.

Two real Postgres servers (the test's own containers): the SOURCE, and the ENGINE (which is also
the control plane and the materialization store). The engine container shares the source
container's network namespace, so the engine's foreign server and the host's direct driver dial
the source at one address. The source logs every statement; the assertions read that log and the
source's rows.
"""

# Requirements: REQ-826, REQ-1141, REQ-030

from __future__ import annotations

import contextlib
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
_ROWS = [(i, i * 1.5, f"n{i}") for i in range(1, 6)]
_TYPES = {"id": "integer", "amount": "double", "note": "varchar"}
_WRITES = ("INSERT INTO", "DELETE FROM", "TRUNCATE", "UPDATE ", "DROP ", "ALTER ")


class _SourceAndEngine:
    """A SOURCE Postgres (database ``shop``) and an ENGINE Postgres (database ``provisa``)."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.source_port, self.engine_port = lease_ports(2)
        self._source = f"provisa-itest-landsrc-{os.getpid()}"
        self._engine = f"provisa-itest-landeng-{os.getpid()}"

    def url(self, port: int, database: str, driver: str = "") -> str:
        return f"postgresql{driver}://provisa:provisa@127.0.0.1:{port}/{database}"

    def start(self) -> None:
        import psycopg

        env = ["-e", "POSTGRES_USER=provisa", "-e", "POSTGRES_PASSWORD=provisa"]
        logging = ["-c", "log_statement=all", "-c", "log_line_prefix=%m [%p] db=%d "]
        publish = [
            f"127.0.0.1:{self.source_port}:{self.source_port}",
            f"127.0.0.1:{self.engine_port}:{self.engine_port}",
        ]
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", self._source, *env]
            + ["-e", "POSTGRES_DB=shop", "-p", publish[0], "-p", publish[1], "postgres:16"]
            + ["-c", f"port={self.source_port}", *logging],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", self._engine, *env]
            + ["-e", "POSTGRES_DB=provisa", "--network", f"container:{self._source}"]
            + ["postgres:16", "-c", f"port={self.engine_port}", *logging],
            check=True,
            capture_output=True,
        )
        for port, database in ((self.source_port, "shop"), (self.engine_port, "provisa")):
            # The image restarts the server once after initdb: ready means it answers repeatedly.
            deadline, answered = time.monotonic() + 120, 0
            while answered < 3:
                try:
                    psycopg.connect(self.url(port, database), connect_timeout=2).close()
                    answered += 1
                except psycopg.OperationalError:
                    answered = 0
                    if time.monotonic() > deadline:
                        raise
                time.sleep(1)
        with psycopg.connect(self.url(self.source_port, "shop"), autocommit=True) as conn:
            conn.execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount DOUBLE PRECISION, note TEXT)"
            )
            with conn.cursor() as cur:
                cur.executemany("INSERT INTO orders VALUES (%s, %s, %s)", _ROWS)

    def source_rows(self) -> list[tuple]:
        import psycopg

        with psycopg.connect(self.url(self.source_port, "shop"), autocommit=True) as conn:
            return conn.execute("SELECT id, amount, note FROM orders ORDER BY id").fetchall()

    def update_source(self, sql: str) -> None:
        import psycopg

        with psycopg.connect(self.url(self.source_port, "shop"), autocommit=True) as conn:
            conn.execute(sql)

    def engine_relation(self, schema: str = "src_public", table: str = "orders") -> str:
        """What stands at the table's name in the ENGINE: view (live), table (replica), absent."""
        import psycopg

        with psycopg.connect(self.url(self.engine_port, "provisa"), autocommit=True) as conn:
            row = conn.execute(
                "SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s",
                (schema, table),
            ).fetchone()
        return {None: "absent", "v": "view", "r": "table"}.get(row[0] if row else None, str(row))

    def source_statement_count(self) -> int:
        return len(self._source_log())

    def source_writes(self, since: int) -> list[str]:
        """The write statements the SOURCE server executed in ``shop`` after log line ``since``."""
        out = []
        for line in self._source_log()[since:]:
            if "db=shop " not in line:
                continue
            statement = line.split("db=shop ", 1)[1]
            if any(word in statement for word in _WRITES):
                out.append(statement)
        return out

    def _source_log(self) -> list[str]:
        done = subprocess.run(
            ["docker", "logs", self._source], check=True, capture_output=True, text=True
        )
        return (done.stdout + done.stderr).splitlines()

    def stop(self) -> None:
        subprocess.run(
            ["docker", "rm", "-f", self._engine, self._source], check=True, capture_output=True
        )


def _config(pg: _SourceAndEngine, *, columns: tuple[str, ...], **source_settings) -> dict:
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
                **source_settings,
            }
        ],
        "tables": [
            {
                "source_id": "src",
                "table": "orders",
                "schema": "public",
                "domain_id": "shop",
                "columns": [
                    {"name": name, "data_type": _TYPES[name], "visible_to": [_ROLE]}
                    | ({"is_primary_key": True} if name == "id" else {})
                    for name in columns
                ],
            }
        ],
    }


@pytest.fixture
def databases():
    pg = _SourceAndEngine()
    pg.start()
    try:
        yield pg
    finally:
        pg.stop()


@contextlib.contextmanager
def _server(pg: _SourceAndEngine, workdir: str, config: dict):
    """A pg-engine server booted on ``config``; yields a function reading the table through it."""
    from tests.integration.isolated_server import IsolatedServer

    path = Path(workdir) / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    control_plane = pg.url(pg.engine_port, "provisa", "+psycopg")
    srv = IsolatedServer(
        "landing_never_writes_source",
        engine="pg",
        config=str(path),
        control_plane="postgres",
        env={
            "PLATFORM_DATABASE_URL": control_plane,
            "TENANT_DATABASE_URL": control_plane,
            "PROVISA_MATERIALIZE_URL": pg.url(pg.engine_port, "provisa"),
            "PROVISA_REDIS_EMBEDDED": "1",
        },
    )

    def _read() -> list[tuple]:
        response = httpx.post(
            f"{srv.base_url}/data/graphql",
            json={"query": "{ orders { id amount } }"},
            headers={"X-Provisa-Role": _ROLE},
            timeout=srv.request_timeout + 10,
        )
        assert response.status_code == 200, response.text
        return sorted((row["id"], row["amount"]) for row in response.json()["data"]["orders"])

    try:
        # The bound on waiting for the boot, not a delay: a boot is ~30 s of work and a busy
        # host stretches it (81 s measured at load average 65), past the harness's 120 s default.
        srv.start(timeout=300.0)
        yield _read
        time.sleep(3)  # the boot's background work (readiness warm-up, reconcile) settles
    finally:
        srv.stop_process()


_ID_AMOUNT = [(i, amount) for i, amount, _note in _ROWS]


def _read_live_once(pg: _SourceAndEngine, workdir: str) -> None:
    """Boot 1: the source is read live, which leaves its live view in the engine."""
    with _server(pg, workdir, _config(pg, columns=("id", "amount", "note"))) as read:
        assert read() == _ID_AMOUNT
    assert pg.engine_relation() == "view"


@pytest.mark.parametrize(
    "setting",
    [{"prefer_materialized": True}, {"load_protected": True, "cache_ttl": 3600}],
    ids=["prefer_materialized", "load_protected"],
)
def test_a_source_that_starts_replicating_after_being_read_live_is_never_written(
    databases, setting
):
    """Boot 1 reads the source live. Boot 2 has the operator's setting newly on, so the source is
    replicated — into the engine's store, never into the source — and reads come from the replica."""
    pg = databases
    with tempfile.TemporaryDirectory() as workdir:
        _read_live_once(pg, workdir)
        since = pg.source_statement_count()
        with _server(pg, workdir, _config(pg, columns=("id", "amount", "note"), **setting)) as read:
            assert read() == _ID_AMOUNT
            assert pg.engine_relation() == "table"  # the replica took the live view's name
            # A change made in the source afterwards is not seen: the read is the replica's.
            pg.update_source("UPDATE orders SET amount = 999 WHERE id = 1")
            assert read() == _ID_AMOUNT
    assert pg.source_writes(since) == [
        "LOG:  statement: UPDATE orders SET amount = 999 WHERE id = 1"
    ]
    assert pg.source_rows() == [(1, 999.0, "n1"), *_ROWS[1:]]


def test_replicating_a_narrower_registration_never_destroys_the_sources_other_columns(databases):
    """The table is registered with fewer columns than the source table has. A replica write that
    reached the source deleted every row and re-inserted only the registered columns."""
    pg = databases
    with tempfile.TemporaryDirectory() as workdir:
        _read_live_once(pg, workdir)
        since = pg.source_statement_count()
        narrow = _config(pg, columns=("id", "amount"), prefer_materialized=True)
        with _server(pg, workdir, narrow) as read:
            assert read() == _ID_AMOUNT
            assert pg.engine_relation() == "table"
    assert pg.source_rows() == _ROWS
    assert pg.source_writes(since) == []


def test_a_source_read_live_again_after_being_replicated_reads_the_source(databases):
    """The reverse switch: the replica table leaves the name and the live view returns, so a read
    sees the source as it is now — not the replica as it was."""
    pg = databases
    with tempfile.TemporaryDirectory() as workdir:
        replicated = _config(pg, columns=("id", "amount", "note"), prefer_materialized=True)
        with _server(pg, workdir, replicated) as read:
            assert read() == _ID_AMOUNT
        assert pg.engine_relation() == "table"
        since = pg.source_statement_count()
        pg.update_source("UPDATE orders SET amount = 999 WHERE id = 1")
        with _server(pg, workdir, _config(pg, columns=("id", "amount", "note"))) as read:
            assert read() == [(1, 999.0), *_ID_AMOUNT[1:]]
        assert pg.engine_relation() == "view"
    assert pg.source_writes(since) == [
        "LOG:  statement: UPDATE orders SET amount = 999 WHERE id = 1"
    ]


async def test_a_replica_write_addressed_to_a_view_over_the_source_is_refused(databases):
    """The guard itself, on the engine's runtime: with the live view standing at the replica's
    name, a land raises the named error and sends nothing to the source."""
    import psycopg

    from provisa.federation.pg_runtime import PgFederationRuntime
    from provisa.federation.replica_guard import ReplicaTargetError

    pg = databases
    with psycopg.connect(pg.url(pg.engine_port, "provisa"), autocommit=True) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        conn.execute(
            "CREATE SERVER guarded FOREIGN DATA WRAPPER postgres_fdw "
            f"OPTIONS (host '127.0.0.1', port '{pg.source_port}', dbname 'shop')"
        )
        conn.execute(
            "CREATE USER MAPPING FOR CURRENT_USER SERVER guarded "
            "OPTIONS (user 'provisa', password 'provisa')"
        )
        conn.execute("CREATE SCHEMA fdw_guarded")
        conn.execute("IMPORT FOREIGN SCHEMA public FROM SERVER guarded INTO fdw_guarded")
        conn.execute("CREATE SCHEMA src_public")
        conn.execute("CREATE VIEW src_public.orders AS SELECT * FROM fdw_guarded.orders")

    since = pg.source_statement_count()
    runtime = PgFederationRuntime(engine_dsn=pg.url(pg.engine_port, "provisa"))
    columns = [("id", "integer"), ("amount", "double")]
    with pytest.raises(ReplicaTargetError) as refused:
        await runtime.land_table(
            schema="src_public",
            table="orders",
            columns=columns,
            rows=[{"id": 1, "amount": 0.0}],
            pk_columns=["id"],
        )
    message = str(refused.value)
    assert '"src_public"."orders" is a view' in message
    assert "fdw_guarded.orders on foreign server guarded" in message and "dbname=shop" in message
    assert pg.source_writes(since) == []
    assert pg.source_rows() == _ROWS
