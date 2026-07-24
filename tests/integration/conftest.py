# Copyright (c) 2026 Kenneth Stott
# Canary: 87c88210-a4bc-4f64-8e50-e46ff9be6a27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from tests._noauth_config import pin_no_auth_config

_PG_BIN = "/Library/PostgreSQL/16/bin"
_SNAPSHOT_PATH = Path.home() / "provisa-test-db-snapshot.dump"
_LIVE_SERVER_URL = os.environ.get("PROVISA_URL", "http://localhost:8000")


require_stack = pytest.mark.usefixtures()


def _pg_env() -> dict:
    env = os.environ.copy()
    env["PGPASSWORD"] = os.environ.get("PG_PASSWORD", "provisa")
    return env


def _pg_args() -> list[str]:
    return [
        "-h",
        os.environ.get("PG_HOST", "localhost"),
        "-p",
        os.environ.get("PG_PORT", "5432"),
        "-U",
        os.environ.get("PG_USER", "provisa"),
        os.environ.get("PG_DATABASE", "provisa"),
    ]


@pytest.fixture(scope="session", autouse=True)
def _db_snapshot_restore():
    """Snapshot the PG DB before the test session and restore it after.

    Tests are free to corrupt DB state — it will be restored at session end.
    After restore the live server is told to rebuild its schema so its in-memory
    state matches the restored DB.
    """
    import time

    # Retry pg_dump up to 5 times — live server DDL may hold AccessExclusiveLock
    # briefly at startup, causing pg_dump's AccessShareLock to deadlock/fail.
    result = None
    for attempt in range(5):
        result = subprocess.run(
            [
                f"{_PG_BIN}/pg_dump",
                "--format=custom",
                "--no-acl",
                "--no-owner",
                "-f",
                str(_SNAPSHOT_PATH),
            ]
            + _pg_args(),
            env=_pg_env(),
            check=False,
        )
        if result.returncode == 0:
            break
        if attempt < 4:
            time.sleep(3)
    else:
        code = result.returncode if result is not None else "unknown"
        raise RuntimeError(f"pg_dump failed after 5 attempts (exit code {code})")

    yield

    subprocess.run(
        [
            f"{_PG_BIN}/pg_restore",
            "--clean",
            "--if-exists",
            "--no-acl",
            "--no-owner",
            "--single-transaction",
            "-h",
            os.environ.get("PG_HOST", "localhost"),
            "-p",
            os.environ.get("PG_PORT", "5432"),
            "-U",
            os.environ.get("PG_USER", "provisa"),
            "-d",
            os.environ.get("PG_DATABASE", "provisa"),
            str(_SNAPSHOT_PATH),
        ],
        env=_pg_env(),
        check=False,
    )

    # Tell the live server to rebuild its in-memory schema from the restored DB
    try:
        import httpx

        httpx.post(
            f"{_LIVE_SERVER_URL}/admin/graphql",
            json={"query": "mutation { rebuildSchemas { success } }"},
            timeout=30,
        )
    except Exception:
        pass


@pytest.fixture(scope="session", autouse=True)
def _require_stack():
    """Docker services are started by _DockerServiceManager before tests run."""
    yield


@pytest.fixture(scope="session", autouse=True)
def _disable_auth_for_integration(tmp_path_factory):
    """Integration tests build the in-process app and call it with a `role` but no
    bearer token; force auth off so create_app() does not install AuthMiddleware."""
    yield from pin_no_auth_config(tmp_path_factory.mktemp("noauth-cfg"))


@pytest.fixture(autouse=True, scope="module")
def _reset_app_state():
    """Reset global app state between test modules.

    The module-level `state` singleton in provisa.api.app accumulates
    auth_config from whichever module-scoped lifespan fixture ran last.
    Resetting before each module ensures create_app() sees a clean slate.
    """
    from provisa.api import app as _app_module

    _app_module.state.auth_config = None
    yield
    _app_module.state.auth_config = None


# --- Real-Postgres-backed pgwire server (REQ-883) ------------------------------
# Shared by test_duckdb_attach_pgwire_real_backend and test_jdbc_introspection_pgwire:
# a live ProvisaServer with a REAL catalog AND a REAL Postgres source — nothing stubbed.
# The scan runs the DIRECT pipeline (execute_direct → asyncpg) against a real table.

_PGW_SOURCE_ID = "sales-pg"
_PGW_SCHEMA = "public"
_PGW_TABLE = "duckattach_orders_e2e"
# Rows the client must read back (covers int4/float8/text + a NULL).
_PGW_ROWS = [
    (1, 19.98, "us-east"),
    (2, 49.99, "us-west"),
    (3, 199.99, "eu-west"),
    (4, 5.0, None),
]

_PGW_TABLES = [
    {
        "id": 1,
        "source_id": _PGW_SOURCE_ID,
        # domain_id drives the catalog SQL schema clients see (pgwire exposes tables under
        # domain_to_sql_name(domain_id)); it must equal the physical Postgres schema so the
        # DIRECT-routed, transpiled SQL resolves against the real table.
        "domain_id": _PGW_SCHEMA,
        "schema_name": _PGW_SCHEMA,
        "table_name": _PGW_TABLE,
        "columns": [
            {"column_name": "id", "visible_to": []},
            {"column_name": "amount", "visible_to": []},
            {"column_name": "region", "visible_to": []},
        ],
    }
]


def _pgw_params() -> dict:
    return {
        "host": os.environ.get("PG_HOST", "localhost"),
        "port": int(os.environ.get("PG_PORT", "5432")),
        "database": os.environ.get("PG_DATABASE", "provisa"),
        "user": os.environ.get("PG_USER", "provisa"),
        "password": os.environ.get("PG_PASSWORD", "provisa"),
    }


def _pgw_free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _pgw_connect(asyncpg, p: dict):
    return await asyncpg.connect(
        host=p["host"],
        port=p["port"],
        database=p["database"],
        user=p["user"],
        password=p["password"],
    )


async def _pgw_seed(asyncpg, p: dict) -> None:
    conn = await _pgw_connect(asyncpg, p)
    try:
        await conn.execute(f"DROP TABLE IF EXISTS {_PGW_SCHEMA}.{_PGW_TABLE}")
        await conn.execute(
            f"CREATE TABLE {_PGW_SCHEMA}.{_PGW_TABLE} "
            "(id integer NOT NULL, amount double precision, region varchar)"
        )
        await conn.executemany(
            f"INSERT INTO {_PGW_SCHEMA}.{_PGW_TABLE} (id, amount, region) VALUES ($1, $2, $3)",
            _PGW_ROWS,
        )
    finally:
        await conn.close()


async def _pgw_drop(asyncpg, p: dict) -> None:
    conn = await _pgw_connect(asyncpg, p)
    try:
        await conn.execute(f"DROP TABLE IF EXISTS {_PGW_SCHEMA}.{_PGW_TABLE}")
    finally:
        await conn.close()


def _pgw_build_state(pool):
    from unittest.mock import MagicMock

    from provisa.compiler import naming as _naming
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput
    from provisa.federation.engine import build_engine
    from provisa.federation.runtime import EngineRuntime

    col_types = {
        1: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
            ColumnMetadata(column_name="amount", data_type="double", is_nullable=True),
            ColumnMetadata(column_name="region", data_type="varchar", is_nullable=True),
        ]
    }
    _naming.configure(gql="snake")
    si = SchemaInput(
        tables=_PGW_TABLES,
        relationships=[],
        column_types=col_types,
        naming_rules=[],
        role={"id": "admin", "domain_access": ["*"], "capabilities": ["ddl"]},
        domains=[{"id": _PGW_SCHEMA, "graphql_alias": None}],
    )
    ctx = build_context(si)

    state = MagicMock()
    state.contexts = {"admin": ctx}
    state.rls_contexts = {}
    state.roles = {"admin": {"id": "admin", "capabilities": ["ddl"], "domain_access": ["*"]}}
    state.schema_build_cache = {"column_types": col_types, "tables": [], "domains": []}
    state.auth_config = {"provider": "none"}  # trust mode: username -> role_id
    state.auth_middleware_active = False
    state.masking_rules = {}
    state.source_types = {_PGW_SOURCE_ID: "postgresql"}
    state.source_dialects = {_PGW_SOURCE_ID: "postgres"}
    state.source_pools = pool
    state.server_limits = {}
    state.engine_conn = None
    from tests.helpers import stub_materialization_noop

    stub_materialization_noop(state)
    # Real federation engine — present for catalog probes; DIRECT execution goes through
    # execute_native → execute_direct on the real source pool, not the engine.
    state.federation_engine = EngineRuntime(build_engine("duckdb"), state)
    return state


@pytest.fixture()
def pgwire_pg_backend(docker_postgres):
    """Live ProvisaServer with a REAL catalog AND a REAL Postgres source — nothing stubbed.

    Yields a dict: ``{port, state, schema, table, rows}``. The scan runs the real DIRECT
    pipeline (govern → route → transpile → execute_direct → asyncpg) against a real table.
    """
    import asyncio
    import socket
    import threading
    import time

    asyncpg = pytest.importorskip("asyncpg", reason="postgres source driver requires asyncpg")

    from provisa.executor.pool import SourcePool
    from provisa.pgwire.server import ProvisaConnection, ProvisaServer
    import provisa.pgwire.server as _srv

    pg = _pgw_params()
    port = _pgw_free_port()
    conn = ProvisaConnection()
    server = ProvisaServer(("127.0.0.1", port), conn)

    loop = asyncio.new_event_loop()
    threading.Thread(target=loop.run_forever, daemon=True).start()

    asyncio.run_coroutine_threadsafe(_pgw_seed(asyncpg, pg), loop).result(timeout=30)

    pool = SourcePool()
    asyncio.run_coroutine_threadsafe(
        pool.add(
            source_id=_PGW_SOURCE_ID,
            source_type="postgresql",
            host=pg["host"],
            port=pg["port"],
            database=pg["database"],
            user=pg["user"],
            password=pg["password"],
        ),
        loop,
    ).result(timeout=30)

    state = _pgw_build_state(pool)

    with (
        patch("provisa.api.app.state", state),
        patch.object(_srv, "state", state, create=True),
    ):
        with _srv._loop_lock:
            _srv._loop = loop
        threading.Thread(target=server.serve_forever, daemon=True).start()
        _deadline = time.time() + 30
        while time.time() < _deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            raise RuntimeError(f"pgwire server did not accept connections on {port} within 30s")
        try:
            yield {
                "port": port,
                "state": state,
                "schema": _PGW_SCHEMA,
                "table": _PGW_TABLE,
                "rows": _PGW_ROWS,
            }
        finally:
            server.shutdown()
            asyncio.run_coroutine_threadsafe(pool.close_all(), loop).result(timeout=10)
            asyncio.run_coroutine_threadsafe(_pgw_drop(asyncpg, pg), loop).result(timeout=10)
            with _srv._loop_lock:
                _srv._loop = None
            loop.call_soon_threadsafe(loop.stop)
