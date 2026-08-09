# Copyright (c) 2026 Kenneth Stott
# Canary: 87c88210-a4bc-4f64-8e50-e46ff9be6a27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import importlib.util
import os
import re
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from tests._noauth_config import pin_no_auth_config

# Ordered (first match wins) filename patterns splitting tests/integration into 5 logical,
# roughly-balanced groups for local/CI parallelization. See analysis in the conversation that
# introduced this: source connectors and native/federation engines are the two heaviest,
# self-provisioning buckets, so they're kept separate from the lighter protocol/admin/pipeline
# tests. New files that don't match anything fall into group_pipeline (never silently unmarked
# — pytest_collection_modifyitems below asserts every collected item gets exactly one group_*
# marker).
_INTEGRATION_GROUP_PATTERNS: list[tuple[str, re.Pattern]] = [
    (
        "group_sources",
        re.compile(
            r"^test_("
            r".+_source_e2e"
            r"|kafka_source|kafka_sink"
            r"|graphql_remote_source|graphql_remote_integration"
            r"|openapi_source|openapi_pet_by_status|openapi_spec"
            r"|govdata_source|nosql_catalogs|elasticsearch_introspect"
            r"|custom_connectors.*"
            r"|debezium_cdc|jsonapi_integration|jsonapi_spec"
            r")\.py$"
        ),
    ),
    (
        "group_engines",
        re.compile(
            r"^test_("
            r".+_federation_engine_e2e"
            r"|duckdb_runtime_e2e|duckdb_attach_pgwire.*"
            r"|postgres_native_engine_e2e|pg_runtime_e2e|clickhouse_runtime_e2e"
            r"|sqlalchemy_dialect|sqlalchemy_runtime_e2e"
            r"|trino_flight_engine_e2e|trino_fte_exchange|trino_tls_flight_e2e|trino_worker_fanout"
            r"|two_org_trino_isolation"
            r"|embedded_pg_duckdb_engine_e2e|embedded_pg_duckdb_iceberg_e2e"
            r"|embedded_pg_fdw_engine_e2e|embedded_pg_sqlite_fdw_e2e"
            r"|duckdb_sqlite_control_plane_e2e|adbc|cross_vendor_parity_e2e"
            r"|engine_runtime_binding|execution_routing|direct_exec"
            r"|federation_integration|databricks_external_link_e2e"
            r")\.py$"
        ),
    ),
    (
        "group_protocols",
        re.compile(
            r"^test_("
            r"bolt_domain_ceiling|bolt_rel_ids|bolt_server"
            r"|pgwire_column_visibility_integration|pgwire_integration|jdbc_introspection_pgwire"
            r"|grpc_execution|grpc_proxy|graphql_execution"
            r"|cypher_endpoint|cypher_integration_extra|cypher_router_api"
            r"|sparql_exec|neo4j_exec|rest_endpoints|sse_subscriptions"
            r"|websocket_rss_integration|live_sse_integration"
            r"|arrow_flight_integration|airport_service_e2e|airport_source_e2e"
            r"|apq_integration|schema_gen|compile_endpoint|nl_endpoint"
            r")\.py$"
        ),
    ),
    (
        "group_auth_org",
        re.compile(
            r"^test_("
            r"auth_integration|api_auth_encryption|audit_auth_integration"
            r"|client_access_integration"
            r"|org_.*|multi_org_onboarding_flow|create_org_onboarding"
            r"|invite_.*|redeem_invite|my_invites"
            r"|first_login_bootstrap_admin|bootstrap_claimant_data_plane|bootstrap_status"
            r"|system_roles_seed"
            r")\.py$"
        ),
    ),
    (
        "group_governance",
        re.compile(
            r"^test_("
            r"admin_api|audit_encryption|abac_hook_endpoint|domain_policy_integration"
            r"|governance_integration|governance_parity_e2e|registration_governance_integration"
            r"|rls_execution|rls_filter_encryption|security_integration_extra"
            r"|rate_limiting_integration|tenancy_role_grants|redirect_encryption_minio"
            r"|settings_router_api|schema_mutation_api|schema_query_api"
            r"|mutations|mutation_parity_integration|mutation_writable_by_preservation"
            r")\.py$"
        ),
    ),
]
_INTEGRATION_GROUP_DEFAULT = "group_pipeline"


def pytest_collection_modifyitems(config, items):
    for item in items:
        path = Path(str(item.fspath))
        if "tests/integration" not in path.as_posix():
            continue
        name = path.name
        group = _INTEGRATION_GROUP_DEFAULT
        for marker_name, pattern in _INTEGRATION_GROUP_PATTERNS:
            if pattern.match(name):
                group = marker_name
                break
        item.add_marker(getattr(pytest.mark, group))


_PG_SERVER_MAJOR = "16"  # matches the postgres:16 image pinned across docker-compose*.yml


def _pg_tool(name: str) -> str:
    """Resolve a PG client binary (pg_dump/pg_restore).

    PATH first — Linux CI and the linux-mem Docker lane ship a pg_* on PATH that matches
    the postgres:16 server image. On local macOS dev, Homebrew's default pg_dump/pg_restore
    tracks Homebrew's latest major version, which drifts ahead of the pinned server image;
    a client built against a newer major emits syntax (e.g. `SET transaction_timeout = 0`)
    the 16 server rejects on restore. When PATH resolves to a mismatched major and the macOS
    EDB 16 installer is present, prefer that instead — it's version-locked to the server.
    """
    edb_tool = Path(f"/Library/PostgreSQL/{_PG_SERVER_MAJOR}/bin/{name}")
    on_path = shutil.which(name)
    if on_path is None:
        return str(edb_tool)
    version = subprocess.run([on_path, "--version"], capture_output=True, text=True, check=False)
    match = re.search(r"\)\s+(\d+)", version.stdout)
    if match and match.group(1) != _PG_SERVER_MAJOR and edb_tool.exists():
        return str(edb_tool)
    return on_path


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


def _reap_stray_pgserver_processes() -> None:
    """Stop postgres processes leaked by pgserver-backed tests in prior sessions.

    pgserver's teardown is registered via atexit (postgres_server.py), which never
    fires when a test session is killed by SIGTERM (e.g. a shell `timeout` wrapper or
    a hard-killed CI job) rather than exiting normally. Leaked instances accumulate
    across sessions and exhaust macOS's small default SysV shared-memory pool
    (kern.sysv.shmall/shmmax), causing unrelated pgserver-based tests deep in a later
    suite run to fail with a shared-memory allocation error. Since this fixture runs
    before this session starts any pgserver instance of its own, any matching process
    found here is necessarily a leftover from a previous session — safe to stop.

    No-ops where pgserver itself is absent (the linux-mem lane's container: pgserver ships no
    linux/aarch64 wheel, which is why that lane exists at all). Nothing there can leak a pgserver
    instance, so there is nothing to reap. psutil is imported only after that check because it
    reaches this process as a pgserver dependency — if pgserver is installed and psutil is not,
    that is a broken environment and must raise rather than silently skip the reap.
    """
    if importlib.util.find_spec("pgserver") is None:
        return

    import psutil

    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            name = proc.info["name"] or ""
            cmdline = proc.info["cmdline"] or []
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if "postgres" not in name:
            continue
        joined = " ".join(cmdline)
        if "pytest-of-" not in joined:
            continue
        pgdata = None
        for i, arg in enumerate(cmdline):
            if arg == "-D" and i + 1 < len(cmdline):
                pgdata = cmdline[i + 1]
                break
        if pgdata is None:
            continue
        result = subprocess.run(
            [_pg_tool("pg_ctl"), "-D", pgdata, "stop", "-m", "fast"],
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            try:
                proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass


@pytest.fixture(scope="session", autouse=True)
def _reap_stray_pgserver():
    _reap_stray_pgserver_processes()
    yield


@pytest.fixture(scope="session", autouse=True)
def _db_snapshot_restore():
    """Snapshot the PG DB before the test session and restore it after.

    Tests are free to corrupt DB state — it will be restored at session end.
    After restore the live server is told to rebuild its schema so its in-memory
    state matches the restored DB.
    """
    import time

    # No isolated stack was provisioned (PYTEST_NO_DOCKER — e.g. the linux-mem lane runs
    # a single pgserver-backed suite in a bare container). There is no app PG at
    # PG_HOST:PG_PORT to snapshot, and pg_dump may be absent — nothing to protect.
    if os.environ.get("PYTEST_NO_DOCKER"):
        yield
        return

    # Retry pg_dump up to 5 times — live server DDL may hold AccessExclusiveLock
    # briefly at startup, causing pg_dump's AccessShareLock to deadlock/fail.
    result = None
    for attempt in range(5):
        result = subprocess.run(
            [
                _pg_tool("pg_dump"),
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
            _pg_tool("pg_restore"),
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
    state.security_high = False  # REQ-693: MagicMock auto-creates attrs as truthy; explicit False
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
