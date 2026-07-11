# Copyright (c) 2026 Kenneth Stott
# Canary: be5aefb1-047c-45bf-bbd3-3d7280b5f906
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import os
import socket
import subprocess
import time

import asyncpg
import pytest
import pytest_asyncio
import trino

from provisa.compiler import naming as _naming

# A native engine caches/lands into a materialization store, which MUST exist (the engine invariant).
# Positive-case tests therefore define one, built from the same PG_* the test PG uses; a negative
# test that asserts the "no store" error overrides it. setdefault so an explicit outer value wins.
os.environ.setdefault(
    "PROVISA_MATERIALIZE_URL",
    "postgresql://{u}:{pw}@{h}:{p}/{db}".format(
        u=os.environ.get("PG_USER", "provisa"),
        pw=os.environ.get("PG_PASSWORD", "provisa"),
        h=os.environ.get("PG_HOST", "localhost"),
        p=os.environ.get("PG_PORT", "5432"),
        db=os.environ.get("PG_DATABASE", "provisa"),
    ),
)

_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
_CORE_COMPOSE = os.path.join(_REPO_ROOT, "docker-compose.core.yml")
_TEST_COMPOSE = os.path.join(_REPO_ROOT, "docker-compose.test.yml")

# The integration tier provisions its OWN isolated stack — a dedicated compose
# project on ephemeral host ports, its own network — so it NEVER touches the local
# dev stack (the `provisa` project on default ports 5432/8080/9000/…). Core and
# marker services share this one project's default network, so Trino reaches
# kafka/mongo/etc. by service name without any external (dev) network.
_ITEST_PROJECT = os.environ.get("PROVISA_ITEST_PROJECT", "provisa-itest")
_ITEST_COMPOSE_ARGS = ["-p", _ITEST_PROJECT, "-f", _CORE_COMPOSE, "-f", _TEST_COMPOSE]

_MARKER_SERVICES: dict[str, list[str]] = {
    "requires_kafka": ["kafka", "schema-registry"],
    "requires_debezium": ["kafka", "schema-registry", "debezium-connect"],
    "requires_mongodb": ["mongodb"],
    "requires_elasticsearch": ["elasticsearch"],
    "requires_neo4j": ["neo4j"],
    "requires_sparql": ["fuseki"],
}
# zaychik is the Arrow Flight terminal the in-process app connects to for Flight/CTAS
# redirects; without it Flight-dependent integration tests fail with connection-refused.
_CORE_SERVICES = ["postgres", "trino", "redis", "pgbouncer", "minio", "zaychik"]

# Host-published services whose ephemeral port the in-process app / test clients
# read from these env vars. compose interpolates the same ${VAR} at `up` time.
_ITEST_PORT_ENV = [
    "PG_PORT",
    "TRINO_PORT",
    "REDIS_PORT",
    "MINIO_PORT",
    "MINIO_CONSOLE_PORT",
    "ZAYCHIK_PORT",
    "MONGO_PORT",
    "NEO4J_HTTP_PORT",
    "NEO4J_BOLT_PORT",
    "KAFKA_HOST_PORT",
    "ELASTICSEARCH_PORT",
    "FUSEKI_PORT",
    "SCHEMA_REGISTRY_PORT",
]


def _reserve_free_ports(n: int) -> list[int]:
    """Return n DISTINCT free TCP ports (sockets held open together so the kernel
    hands out a different port for each)."""
    socks: list[socket.socket] = []
    try:
        for _ in range(n):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def _allocate_itest_ports() -> None:
    """Assign every isolated-stack host port to a fresh ephemeral port and export the
    URL-shaped env the in-process app reads, so the app never hits the dev stack."""
    for name, port in zip(_ITEST_PORT_ENV, _reserve_free_ports(len(_ITEST_PORT_ENV))):
        os.environ[name] = str(port)
    os.environ["REDIS_URL"] = f"redis://localhost:{os.environ['REDIS_PORT']}/0"
    os.environ["PROVISA_OTEL_S3_ENDPOINT"] = f"http://localhost:{os.environ['MINIO_PORT']}"
    # Host-side kafka clients read these; point them at the isolated broker's port.
    _kafka = f"localhost:{os.environ['KAFKA_HOST_PORT']}"
    os.environ["KAFKA_BOOTSTRAP"] = _kafka
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = _kafka


class _DockerServiceManager:
    def pytest_collection_finish(self, session):
        if os.environ.get("PYTEST_NO_DOCKER"):
            return
        integration = [i for i in session.items if "integration" in str(i.fspath)]
        if not integration:
            return

        # Which marker services this run needs (kafka/mongo/neo4j/…). schema-registry
        # and debezium ride along with kafka in the SAME isolated project, so they
        # reach it on the project network by service name — no external dev network.
        needed: set[str] = set(_CORE_SERVICES)
        for item in session.items:
            for marker, services in _MARKER_SERVICES.items():
                if item.get_closest_marker(marker):
                    needed.update(services)

        # Provision an ISOLATED stack: dedicated project, ephemeral host ports, its
        # own network — the dev stack (`provisa` project, default ports) is never
        # touched. Ports are allocated + exported here (before any in-process app is
        # built and before the provisa_server subprocess captures the env), and the
        # compose files interpolate the same ${*_PORT} at `up`.
        _allocate_itest_ports()
        subprocess.run(
            ["docker", "compose", *_ITEST_COMPOSE_ARGS, "up", "-d", "--wait", *sorted(needed)],
            cwd=_REPO_ROOT,
            check=True,
        )

    def pytest_sessionfinish(self, session, exitstatus):  # pyright: ignore
        # Tests own the services they provision — including reaping them. Tear the
        # whole isolated stack down by default so a run never leaks containers (which
        # starve later runs of memory) and never leaves anything touching dev.
        # PYTEST_DOCKER_KEEP=1 keeps it up for local iteration.
        if os.environ.get("PYTEST_DOCKER_KEEP"):
            return
        subprocess.run(
            ["docker", "compose", *_ITEST_COMPOSE_ARGS, "down", "--volumes"],
            cwd=_REPO_ROOT,
            check=False,
        )


def pytest_configure(config):
    config.pluginmanager.register(_DockerServiceManager())
    config.addinivalue_line(
        "markers",
        "requires_provisa_server: skip when Provisa server is not reachable",
    )
    config.addinivalue_line(
        "markers",
        "requires_debezium: skip when Debezium Connect is not reachable",
    )


def pytest_collection_modifyitems(config, items):  # pyright: ignore
    for item in items:
        if item.get_closest_marker("requires_provisa_server"):
            item.fixturenames.insert(0, "provisa_server")
        if item.get_closest_marker("requires_debezium"):
            item.fixturenames.insert(0, "debezium_server")


@pytest.fixture(autouse=True)
def _reset_naming_convention():  # pyright: ignore
    """Reset global naming convention to defaults after each test.

    Tests that call _naming.configure() mutate module-level state. Without
    this reset, convention leaks across test boundaries causing failures in
    tests that rely on the default apollo_graphql (camelCase) convention.
    """
    yield
    _naming.configure(gql="apollo_graphql", sql="snake")


def _server_reachable(url: str) -> bool:
    """True if the server answers liveness within a short retry budget.

    Probes the dependency-free /live endpoint (not /health, which acquires a PG
    connection and can block for several seconds when the pool is saturated by
    concurrent fixture/UI traffic). Retries so a transiently-busy but healthy
    server is not misclassified as dead.
    """
    import urllib.request

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"{url}/live", timeout=5)
            return True
        except Exception:
            time.sleep(1)
    return False


def _tcp_reachable(host: str, port: int) -> bool:
    import socket

    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


def _pgbouncer_auth_ok(host: str, port: int) -> bool:  # pyright: ignore
    import asyncio

    async def _try():
        try:
            conn = await asyncpg.connect(
                host=host,
                port=port,
                user=os.environ.get("PG_USER", "provisa"),
                password=os.environ.get("PG_PASSWORD", "provisa"),
                database=os.environ.get("PG_DATABASE", "provisa"),
                timeout=5,
            )
            await conn.close()
            return True
        except Exception:
            return False

    return asyncio.run(_try())


def _trino_catalog_exists(catalog: str) -> bool:  # pyright: ignore
    import trino

    try:
        conn = trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "localhost"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user="test",
        )
        cur = conn.cursor()
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        cur.fetchone()
        return True
    except Exception:
        return False


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session", autouse=True)
def _wait_for_trino():  # pyright: ignore
    """Block until Trino core catalogs are ready or 6 minutes elapse.

    Set PROVISA_SKIP_TRINO_WAIT=1 when the test session provisions its own
    Trino (e.g. helm/minikube tests) and external Trino is not available.
    """
    if os.environ.get("PROVISA_SKIP_TRINO_WAIT"):
        return
    host = os.environ.get("TRINO_HOST", "localhost")
    port = int(os.environ.get("TRINO_PORT", "8080"))
    deadline = time.monotonic() + 360
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = trino.dbapi.connect(host=host, port=port, user="test")
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.execute("SHOW SCHEMAS FROM sales_pg")
            cur.fetchall()
            conn.close()
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(3)
    raise RuntimeError(f"Trino not ready at {host}:{port} after 360s — last error: {last_exc}")


@pytest.fixture(scope="session", autouse=True)
def _reserve_flight_port():  # pyright: ignore
    """Allocate a free port for the Arrow Flight server before any test starts.

    Multiple integration tests spin up an in-process FastAPI app via
    ASGITransport. Each app instance tries to bind the Arrow Flight gRPC
    server on FLIGHT_PORT (default 8815). If port 8815 is already in use
    (e.g. by a previous test run's zombie process or the live server) the
    lifespan fails and every request returns 400. Setting a random free port
    here ensures every in-process app gets a usable socket.
    """
    port = _free_port()
    os.environ.setdefault("FLIGHT_PORT", str(port))
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    # Limit pool size per in-process app so concurrent module fixtures
    # don't exhaust PostgreSQL max_connections.
    os.environ.setdefault("PG_POOL_MIN", "1")
    os.environ.setdefault("PG_POOL_MAX", "3")
    # Platform control plane URL (global org/user/invite registry + billing).
    # Tests point it at the same Postgres server as the tenant plane but leave it
    # unscoped (default schema) — a separate engine/pool, per the control-plane
    # split. The subprocess server inherits this via {**os.environ}.
    _cp_url = (
        f"postgresql+asyncpg://{os.environ.get('PG_USER', 'provisa')}"
        f":{os.environ.get('PG_PASSWORD', 'provisa')}"
        f"@{os.environ.get('PG_HOST', 'localhost')}"
        f":{os.environ.get('PG_PORT', '5432')}"
        f"/{os.environ.get('PG_DATABASE', 'provisa')}"
    )
    os.environ.setdefault("PLATFORM_DATABASE_URL", _cp_url)
    # Tenant control-plane URL (schema-scoped to org_<id> by the fixtures). Same
    # canonical SQLAlchemy async URL as the platform plane — one place names the
    # driver, so no fixture hand-builds it.
    os.environ.setdefault("TENANT_DATABASE_URL", _cp_url)


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    return (
        f"postgresql://{os.environ.get('PG_USER', 'provisa')}"
        f":{os.environ.get('PG_PASSWORD', 'provisa')}"
        f"@{os.environ.get('PG_HOST', 'localhost')}"
        f":{os.environ.get('PG_PORT', '5432')}"
        f"/{os.environ.get('PG_DATABASE', 'provisa')}"
    )


@pytest_asyncio.fixture(scope="session")
async def tenant_db(_reserve_flight_port):
    # The tenant control plane is the SQLAlchemy-backed Database shim (execute_core,
    # advisory_xact_lock, …), replacing the former bare asyncpg pool. search_path is
    # left unset (role default: public) to match that pool exactly — tests that need
    # the org_default schema set it per-acquire (e.g. test_schema_gen). init_schema
    # scopes its own schema internally, so it does not depend on this. URL (driver
    # included) comes from TENANT_DATABASE_URL, set once in the env.
    from provisa.core.database import Database, create_engine_from_url

    engine = create_engine_from_url(os.environ["TENANT_DATABASE_URL"], pool_size=5, max_overflow=5)
    db = Database(engine, name="org", search_path=None)
    yield db
    await db.close()


@pytest.fixture(scope="session")
def trino_conn():
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="test",
        catalog="sales_pg",
        schema="public",
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def docker_postgres():
    """Ensure the postgres container is running; start it if not.

    Uses `docker compose -f docker-compose.core.yml up postgres -d` which is
    safe on this machine (single named service — never `compose up` with no
    service name, which crashes Docker Engine).
    """
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))

    if not _tcp_reachable(pg_host, pg_port):
        compose_file = os.path.join(os.path.dirname(__file__), "..", "docker-compose.core.yml")
        subprocess.run(
            ["docker", "compose", "-f", compose_file, "up", "postgres", "-d"],
            check=True,
        )
        # Wait up to 30 s for postgres to be ready
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if _tcp_reachable(pg_host, pg_port):
                break
            time.sleep(1)
        else:
            raise RuntimeError(
                f"Postgres did not become reachable at {pg_host}:{pg_port} within 30 s"
            )

    yield {"host": pg_host, "port": pg_port}


@pytest_asyncio.fixture(scope="session")
async def graphql_client(docker_postgres):
    """ASGI test client backed by a real Postgres pool.

    Starts an in-process Provisa app via create_app() with a real asyncpg pool
    so GraphQL queries exercise the full compiler + executor path without
    requiring a separate server process.
    """

    import provisa.api.app as app_mod
    from provisa.api.app import create_app
    from httpx import ASGITransport, AsyncClient

    the_app = create_app()

    from provisa.core.database import (
        Database as _Database,
        create_engine_from_url as _create_engine_from_url,
    )

    org_id = os.environ.get("ORG_ID", "default")
    _tenant_engine = _create_engine_from_url(os.environ["TENANT_DATABASE_URL"], pool_size=3)
    pool = _Database(_tenant_engine, name="org", search_path=f"org_{org_id}")
    from unittest.mock import AsyncMock, MagicMock

    from provisa.executor.pool import SourcePool

    _sp = MagicMock(spec=SourcePool)
    _sp.has.return_value = False
    _sp.get.side_effect = KeyError
    _sp.dialect_for.return_value = None
    _sp.source_ids = []
    _sp.execute = AsyncMock(return_value=MagicMock(rows=[]))
    _sp.execute_ddl = AsyncMock()
    _sp.add = AsyncMock()
    _sp.remove = AsyncMock()
    _sp.close_all = AsyncMock()
    _sp.close = AsyncMock()
    app_mod.state.tenant_db = pool
    app_mod.state.source_pools = _sp

    # Platform control plane (global org/user/invite registry). This fixture
    # bypasses the app lifespan, so build + seed it here the way startup does.
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import init_registry_schema

    admin_engine = create_engine_from_url(os.environ["PLATFORM_DATABASE_URL"], pool_size=3)
    admin_db = Database(admin_engine, name="platform")
    await init_registry_schema(admin_db)
    app_mod.state.admin_db = admin_db

    transport = ASGITransport(app=the_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    await pool.close()
    await admin_db.close()
    app_mod.state.tenant_db = None
    app_mod.state.admin_db = None


@pytest.fixture(scope="session")
def test_client(docker_postgres):  # pyright: ignore[reportUnusedParameter]
    """Synchronous ASGI test client for in-process Provisa app."""
    from unittest.mock import AsyncMock, MagicMock

    import provisa.api.app as app_mod
    from provisa.api.app import create_app
    from provisa.executor.pool import SourcePool
    from starlette.testclient import TestClient

    _sp = MagicMock(spec=SourcePool)
    _sp.has.return_value = False
    _sp.get.side_effect = KeyError
    _sp.dialect_for.return_value = None
    _sp.source_ids = []
    _sp.execute = AsyncMock(return_value=MagicMock(rows=[]))
    _sp.execute_ddl = AsyncMock()
    _sp.add = AsyncMock()
    _sp.remove = AsyncMock()
    _sp.close_all = AsyncMock()
    _sp.close = AsyncMock()
    the_app = create_app()
    app_mod.state.source_pools = _sp
    with TestClient(the_app, raise_server_exceptions=False) as client:
        yield client
    app_mod.state.tenant_db = None


@pytest.fixture(scope="session")
def debezium_server():
    """Wait for Debezium Connect to be reachable — started by _DockerServiceManager."""
    host = os.environ.get("DEBEZIUM_HOST", "localhost")
    port = int(os.environ.get("DEBEZIUM_PORT", "8083"))
    deadline = time.monotonic() + 480
    while time.monotonic() < deadline:
        if _tcp_reachable(host, port):
            yield f"http://{host}:{port}"
            return
        time.sleep(3)
    raise RuntimeError(f"Debezium Connect did not become reachable at {host}:{port} within 480s")


@pytest_asyncio.fixture(scope="session")
async def live_client(provisa_server):
    """AsyncClient that hits the running Provisa server (PROVISA_URL or localhost:8000)."""
    import httpx

    async with httpx.AsyncClient(base_url=provisa_server, timeout=120.0) as client:
        yield client


@pytest.fixture(scope="session")
def provisa_server():
    """Start the Provisa server subprocess if not already running.

    Used by requires_provisa_server tests — injected automatically via
    pytest_collection_modifyitems, not requested directly.
    """
    server_url = os.environ.get("PROVISA_URL", "http://localhost:8000")
    if _server_reachable(server_url):
        # Reusing an externally-managed server: its Flight port is whatever it was started with.
        os.environ["PROVISA_SERVER_FLIGHT_PORT"] = os.environ.get("FLIGHT_PORT", "8815")
        yield server_url
        return

    from urllib.parse import urlparse as _urlparse

    _parsed = _urlparse(server_url)
    _port = _parsed.port or 8000
    _host = _parsed.hostname or "localhost"
    if _tcp_reachable(_host, _port):
        raise RuntimeError(
            f"Port {_port} is already bound by another process but {server_url}/health "
            "is not responding — stop the existing process before running tests that "
            "require a Provisa server."
        )

    # Give the subprocess its OWN free Arrow Flight port. The session-wide FLIGHT_PORT
    # (_reserve_flight_port) is shared by every in-process ASGI app; if the subprocess inherited it,
    # its Flight bind would clash with an already-bound in-process server and silently fail (HTTP
    # comes up, Flight never binds). A dedicated free port makes the live-server Flight isolation-safe.
    _flight_port = _free_port()
    venv_python = os.path.join(_REPO_ROOT, ".venv", "bin", "uvicorn")
    server_env = {
        **os.environ,
        "PG_PASSWORD": os.environ.get("PG_PASSWORD") or "provisa",
        "FLIGHT_PORT": str(_flight_port),
    }
    proc = subprocess.Popen(
        [venv_python, "main:app", "--host", "0.0.0.0", f"--port={_port}"],
        cwd=_REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=server_env,
    )
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if _server_reachable(server_url):
            break
        if proc.poll() is not None:
            raise RuntimeError(f"Provisa server exited early (code {proc.returncode})")
        time.sleep(2)
    else:
        proc.terminate()
        raise RuntimeError(f"Provisa server did not become reachable at {server_url} within 90s")

    # HTTP /health can precede the Flight gRPC bind; wait for the Flight port before yielding so
    # requires_provisa_server tests never race the bind. Publish the port for Flight/ADBC clients.
    _flight_deadline = time.monotonic() + 60
    while time.monotonic() < _flight_deadline:
        if _tcp_reachable(_host, _flight_port):
            break
        if proc.poll() is not None:
            raise RuntimeError(f"Provisa server exited before Flight bind (code {proc.returncode})")
        time.sleep(1)
    else:
        proc.terminate()
        raise RuntimeError(
            f"Provisa Arrow Flight server did not bind {_host}:{_flight_port} within 60s"
        )
    os.environ["PROVISA_SERVER_FLIGHT_PORT"] = str(_flight_port)

    try:
        yield server_url
    finally:
        os.environ.pop("PROVISA_SERVER_FLIGHT_PORT", None)
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture
def otel_spans():
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry import trace

    # set_tracer_provider() is one-shot per process: if any earlier import or
    # fixture already installed a real SDK provider, a second set is silently
    # ignored and our exporter would never receive spans. So attach the exporter
    # to whichever real provider is active; only install a fresh one if the
    # current provider is still the API default (ProxyTracerProvider).
    exporter = InMemorySpanExporter()
    processor = SimpleSpanProcessor(exporter)
    current = trace.get_tracer_provider()
    if isinstance(current, TracerProvider):
        current.add_span_processor(processor)
        yield exporter
        processor.shutdown()  # drains + disables; provider keeps running for other tests
    else:
        provider = TracerProvider()
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        yield exporter
        exporter.shutdown()


@pytest.fixture
def sample_config():
    import yaml
    from pathlib import Path

    config_path = Path(__file__).parent / "fixtures" / "sample_config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)
