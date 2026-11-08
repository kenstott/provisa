# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The smoke check: main can start, and what starts answers (REQ-1989).

One isolated instance of the default demo on the native DuckDB engine, with an embedded PostgreSQL
control plane, its own PROVISA_HOME and its own leased ports. Against it: the browser, one known
query through each front door, a read from each source that needs no external service, and
governance once. Every check asserts the answer, and a skip is a failure (conftest.py).

Run it from the repository root, on a test instance only, with one command:

    .venv/bin/python -m pytest -m isolated tests/smoke/test_main_is_startable.py --no-cov -v

It needs `uv sync --extra dev --extra gx --extra soda`, provisa-client/ installed, and
`npm ci && npx playwright install chromium` in provisa-ui. It starts and removes everything it uses,
and leaves the instance's logs in PROVISA_SMOKE_LOG_DIR when that is set. CI runs the same command
(.github/workflows/smoke.yml).
"""

# Requirements: REQ-1989

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.port_lease import lease_ports

pytestmark = [pytest.mark.isolated]

REPO = Path(__file__).resolve().parents[2]
CONFIG = "tests/smoke/smoke.yaml"
UI = REPO / "provisa-ui"
ADMIN = "org_admin"
RESTRICTED = "pet_analyst"  # the rule in smoke.yaml keeps it to the pets on sale
_BOOT_SECONDS = 300
_REQUEST_SECONDS = 120.0

# Seeded by demo/files/create_demo_files.py. The answer every front door gives.
PET_SQL = "SELECT id, name FROM pet_store.pets ORDER BY id"
PET_CYPHER = "MATCH (n:Pets) RETURN n.name AS name ORDER BY name"
PET_NAMES = ["Cat 1", "Cat 2", "Dog 1", "Lion 1", "Lion 2", "Lion 3", "Rabbit 1"]
PETS = list(enumerate(PET_NAMES, start=1))

_SEED_SOURCE_PG = """
CREATE TABLE smoke_accounts (id integer PRIMARY KEY, owner text NOT NULL);
INSERT INTO smoke_accounts VALUES (1, 'Ada'), (2, 'Grace'), (3, 'Linus');
"""


@dataclass
class Instance:
    api: int
    grpc: int
    flight: int
    pgwire: int
    mcp: int
    bolt: int
    ui: int
    log_dir: Path
    env: dict[str, str] = field(default_factory=dict)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.api}"


def _python(args: list[str], env: dict[str, str] | None = None) -> str:
    """Run this interpreter; a non-zero exit fails the run with what it printed."""
    done = subprocess.run(
        [sys.executable, *args], cwd=REPO, env=env, capture_output=True, text=True, check=False
    )
    assert done.returncode == 0, f"{args}\n{done.stdout}\n{done.stderr}"
    return done.stdout


def _embedded_pg(datadir: Path) -> dict[str, str]:
    out = _python(["-m", "provisa.core.control_plane_pg", "start", str(datadir)])
    fields = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
    return {k: fields[k] for k in ("PG_HOST", "PG_PORT", "PG_PASSWORD")}


def _spawn(name: str, args: list[str], log_dir: Path, env: dict[str, str]) -> subprocess.Popen:
    log = (log_dir / f"{name}.log").open("wb")
    return subprocess.Popen(
        [sys.executable, *args],
        cwd=REPO,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def _wait_for_health(backend: subprocess.Popen, url: str, log: Path) -> None:
    deadline = time.monotonic() + _BOOT_SECONDS
    while True:
        assert backend.poll() is None, f"the backend exited:\n{log.read_text()[-4000:]}"
        try:
            if httpx.get(f"{url}/health", timeout=5).status_code == 200:
                return
        except httpx.TransportError:
            pass  # not listening yet: the deadline below decides
        assert time.monotonic() < deadline, (
            f"no /health in {_BOOT_SECONDS}s:\n{log.read_text()[-4000:]}"
        )
        time.sleep(1)


@pytest.fixture(scope="module")
def instance() -> Iterator[Instance]:
    # A short root: a unix socket's path is limited to about 100 characters on macOS.
    root = Path(tempfile.mkdtemp(prefix="provisa-smoke-", dir="/tmp"))
    home = root / "home"
    (home / "demo").mkdir(parents=True)
    log_dir = Path(os.environ.get("PROVISA_SMOKE_LOG_DIR", str(root / "logs")))
    log_dir.mkdir(parents=True, exist_ok=True)
    api, pet, gql, demo_grpc, grpc_port, flight, pgwire, mcp, bolt, ui = lease_ports(10)
    control_dir, source_dir = home / "demo" / "control-pg", root / "source-pg"
    processes: list[subprocess.Popen] = []
    base = {k: v for k, v in os.environ.items() if not k.startswith(("PROVISA_", "PG_", "TENANT_"))}
    try:
        _python(["demo/files/create_demo_files.py"], base)
        control = _embedded_pg(control_dir)
        source = _embedded_pg(source_dir)
        seed = root / "seed.sql"
        seed.write_text(_SEED_SOURCE_PG)
        _python(
            ["-m", "provisa.core.control_plane_pg", "apply", str(source_dir), "--file", str(seed)]
        )
        host, port, password = control["PG_HOST"], control["PG_PORT"], control["PG_PASSWORD"]
        database_url = f"postgresql+psycopg://provisa:{password}@/provisa?host={host}&port={port}"
        processes.append(
            _spawn(
                "petstore",
                [
                    "-m",
                    "uvicorn",
                    "server:app",
                    "--app-dir",
                    "demo/petstore_server",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(pet),
                ],
                log_dir,
                base,
            )
        )
        processes.append(
            _spawn(
                "graphql-demo",
                [
                    "-m",
                    "uvicorn",
                    "server:app",
                    "--app-dir",
                    "demo/graphql_server",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(gql),
                ],
                log_dir,
                base,
            )
        )
        processes.append(
            _spawn(
                "grpc-demo",
                ["-m", "demo.grpc_server.server"],
                log_dir,
                {**base, "DEMO_GRPC_PORT": str(demo_grpc)},
            )
        )
        env = {
            **base,
            "PROVISA_HOME": str(home),
            "PROVISA_DATA_DIR": str(home / "demo"),
            "PROVISA_CONFIG": CONFIG,
            "PROVISA_CONFIG_REPLACE": "true",
            "PROVISA_DEMO": "true",
            "PROVISA_ENGINE": "duckdb",
            "PROVISA_IDP": "",
            "PROVISA_REDIS_EMBEDDED": "1",
            "PROVISA_ENABLE_TEST_ENDPOINTS": "true",
            "PROVISA_FAKE_KEY_DIR": str(home / "fake-key"),
            "PROVISA_ENGINE_FILES_GID": str(os.getgid()),
            "PROVISA_API_PORT": str(api),
            "PROVISA_LOCAL_ADDRESS": f"http://localhost:{api}",
            "GRPC_PORT": str(grpc_port),
            "FLIGHT_PORT": str(flight),
            "PROVISA_PGWIRE_PORT": str(pgwire),
            "PROVISA_MCP_PORT": str(mcp),
            "PROVISA_MCP_HOST": "127.0.0.1",
            "PROVISA_MCP_ROLE": f"{ADMIN},{RESTRICTED}",
            "PROVISA_BOLT_PORT": str(bolt),
            "PROVISA_AIRPORT_PORT": "0",
            # A real client's route to the service's descriptors on this no-auth demo.
            "GRPC_ALLOW_UNSECURED_REFLECTION": "true",
            "TENANT_DATABASE_URL": database_url,
            "PLATFORM_DATABASE_URL": database_url,
            "PG_HOST": host,
            "PG_PORT": port,
            "PG_DATABASE": "provisa",
            "PG_USER": "provisa",
            "PG_PASSWORD": password,
            "POSTGRES_HOST": host,
            "GRAPHQL_DEMO_ENABLED": "true",
            "GRAPHQL_DEMO_URL": f"http://localhost:{gql}/graphql",
            "PETSTORE_BASE_URL": f"http://localhost:{pet}/api/v3",
            "DEMO_GRPC_PORT": str(demo_grpc),
            "DEMO_GRPC_TARGET": f"localhost:{demo_grpc}",
            "SMOKE_PG_HOST": source["PG_HOST"],
            "SMOKE_PG_PORT": source["PG_PORT"],
            "SMOKE_PG_PASSWORD": source["PG_PASSWORD"],
        }
        backend = _spawn(
            "backend",
            ["-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(api)],
            log_dir,
            env,
        )
        processes.append(backend)
        running = Instance(api, grpc_port, flight, pgwire, mcp, bolt, ui, log_dir, base)
        _wait_for_health(backend, running.url, log_dir / "backend.log")
        yield running
    finally:
        for process in processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        for datadir in (control_dir, source_dir):
            if (datadir / "PG_VERSION").exists():
                _python(["-m", "provisa.core.control_plane_pg", "stop", str(datadir)], base)
        shutil.rmtree(root)


def _headers(role: str = ADMIN) -> dict[str, str]:
    return {"X-Provisa-Role": role}


def _sql_rows(instance: Instance, sql: str, role: str = ADMIN) -> list[dict[str, Any]]:
    resp = httpx.post(
        f"{instance.url}/data/sql",
        json={"sql": sql},
        headers=_headers(role),
        timeout=_REQUEST_SECONDS,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


# --- the browser -----------------------------------------------------------------------------------


def test_the_app_loads_and_explore_runs_a_query(instance: Instance):
    done = subprocess.run(
        ["npx", "playwright", "test", "--config", "playwright.smoke.config.ts"],
        cwd=UI,
        env={
            **instance.env,
            "PROVISA_E2E_UI_PORT": str(instance.ui),
            "PROVISA_API_PORT": str(instance.api),
            "PROVISA_E2E_BACKEND_PORTS": str(instance.api),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, f"{done.stdout}\n{done.stderr}"
    assert "1 passed" in done.stdout, done.stdout


# --- one known query through each front door --------------------------------------------------------


def test_sql_over_http(instance: Instance):
    assert [(r["id"], r["name"]) for r in _sql_rows(instance, PET_SQL)] == PETS


def test_sql_over_pgwire(instance: Instance):
    import psycopg

    with psycopg.connect(
        host="127.0.0.1",
        port=instance.pgwire,
        user=ADMIN,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    ) as conn:
        assert conn.execute(PET_SQL).fetchall() == PETS


def test_graphql(instance: Instance):
    resp = httpx.post(
        f"{instance.url}/data/graphql",
        json={"query": "{ ps__pets { id name } }"},
        headers=_headers(),
        timeout=_REQUEST_SECONDS,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    assert [(p["id"], p["name"]) for p in body["data"]["ps__pets"]] == PETS


def test_cypher_over_http(instance: Instance):
    resp = httpx.post(
        f"{instance.url}/data/cypher",
        json={"query": PET_CYPHER},
        headers=_headers(),
        timeout=_REQUEST_SECONDS,
    )
    assert resp.status_code == 200, resp.text
    assert [r["name"] for r in resp.json()["rows"]] == PET_NAMES


def test_cypher_over_bolt(instance: Instance):
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{instance.bolt}", auth=(ADMIN, ""))
    try:
        with driver.session() as session:
            assert [r["name"] for r in session.run(PET_CYPHER)] == PET_NAMES
    finally:
        driver.close()


def test_sparql(instance: Instance):
    query = (
        f"SELECT ?name WHERE {{ ?p <http://localhost:{instance.api}/vocab/pet-store/pets#name> ?name }} "
        "ORDER BY ?name"
    )
    resp = httpx.post(
        f"{instance.url}/data/sparql",
        content=query,
        headers={
            **_headers(),
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/sparql-query",
        },
        timeout=_REQUEST_SECONDS,
    )
    assert resp.status_code == 200, resp.text
    assert [b["name"]["value"] for b in resp.json()["results"]["bindings"]] == PET_NAMES


def test_grpc(instance: Instance):
    """A streamed Query RPC, its messages from server reflection. The published .proto is not used:
    on this demo it does not compile (SAssignmentsByBreedParams has breed_name beside breedName,
    which protoc refuses as a JSON name conflict), a defect of its own."""
    import grpc
    from google.protobuf import descriptor_pb2
    from google.protobuf.descriptor_pool import DescriptorPool
    from google.protobuf.message_factory import GetMessageClass
    from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

    from provisa.grpc.descriptor import SERVICE_NAME

    metadata = (("x-provisa-role", ADMIN),)
    channel = grpc.insecure_channel(f"127.0.0.1:{instance.grpc}")
    try:
        (described,) = reflection_pb2_grpc.ServerReflectionStub(channel).ServerReflectionInfo(  # pyright: ignore[reportCallIssue]
            iter([reflection_pb2.ServerReflectionRequest(file_containing_symbol=SERVICE_NAME)]),
            metadata=metadata,
            timeout=_REQUEST_SECONDS,
        )
        pool = DescriptorPool()
        # The file comes first and the files it imports after it: added imports first.
        for raw in reversed(described.file_descriptor_response.file_descriptor_proto):
            pool.Add(descriptor_pb2.FileDescriptorProto.FromString(raw))
        service = pool.FindServiceByName(SERVICE_NAME)
        method = next(m for m in service.methods if m.name == "QueryPsPets")
        request_class = GetMessageClass(method.input_type)
        row_class = GetMessageClass(method.output_type)
        rpc = channel.unary_stream(
            f"/{service.full_name}/{method.name}",
            request_serializer=request_class.SerializeToString,
            response_deserializer=row_class.FromString,
        )
        rows = list(rpc(request_class(), metadata=metadata, timeout=_REQUEST_SECONDS))
    finally:
        channel.close()
    assert sorted((r.id, r.name) for r in rows) == PETS  # pyright: ignore[reportAttributeAccessIssue]


def test_flight_sql(instance: Instance):
    import adbc_driver_flightsql.dbapi as flight_sql

    with (
        flight_sql.connect(
            f"grpc://127.0.0.1:{instance.flight}",
            db_kwargs={"adbc.flight.sql.rpc.call_header.x-provisa-role": ADMIN},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute(PET_SQL)
        assert cursor.fetchall() == PETS


def test_rest(instance: Instance):
    resp = httpx.get(
        f"{instance.url}/data/rest/pet-store/pets", headers=_headers(), timeout=_REQUEST_SECONDS
    )
    assert resp.status_code == 200, resp.text
    assert sorted((p["id"], p["name"]) for p in resp.json()["data"]) == PETS


def test_mcp(instance: Instance):
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    async def call() -> dict:
        url = f"http://127.0.0.1:{instance.mcp}/mcp"
        async with streamable_http_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("run_sql", {"sql": PET_SQL, "role": ADMIN})
                text = "".join(getattr(c, "text", "") for c in result.content)
                assert not result.isError, text
                return json.loads(text)

    answer = asyncio.run(call())
    assert [(r["id"], r["name"]) for r in answer["rows"]] == PETS


# --- a read from each source ------------------------------------------------------------------------


def test_source_graphql_remote(instance: Instance):
    rows = _sql_rows(instance, "SELECT id, last_name FROM graphql.employees ORDER BY id LIMIT 2")
    assert rows == [{"id": 1, "last_name": "Alvarez"}, {"id": 2, "last_name": "Patel"}]


def test_source_inquiries_sqlite(instance: Instance):
    rows = _sql_rows(instance, "SELECT id, message FROM default.inquiries ORDER BY id LIMIT 1")
    assert rows == [{"id": 1, "message": "Is Cat 1 still available?"}]


def test_source_pet_store_sqlite(instance: Instance):
    rows = _sql_rows(instance, "SELECT name, breed_name FROM pet_store.pets WHERE id = 3")
    assert rows == [{"name": "Dog 1", "breed_name": "Golden Retriever"}]


def test_source_openapi(instance: Instance):
    rows = _sql_rows(instance, "SELECT count FROM default.get_inventory WHERE status = 'placed'")
    assert rows == [{"count": 100}]


def test_source_parquet(instance: Instance):
    rows = _sql_rows(instance, "SELECT sku, name FROM default.products WHERE id = 1")
    assert rows == [{"sku": "WIDGET-A", "name": "Widget Alpha"}]


def test_source_embedded_postgresql(instance: Instance):
    rows = _sql_rows(instance, "SELECT id, owner FROM public.smoke_accounts ORDER BY id")
    assert rows == [
        {"id": 1, "owner": "Ada"},
        {"id": 2, "owner": "Grace"},
        {"id": 3, "owner": "Linus"},
    ]


# --- governance -------------------------------------------------------------------------------------


def test_the_restricted_role_gets_fewer_rows(instance: Instance):
    query = "SELECT name FROM pet_store.pets ORDER BY id"
    assert [r["name"] for r in _sql_rows(instance, query)] == PET_NAMES
    # Lion 1 is the one pet not on sale (available = 0).
    assert [r["name"] for r in _sql_rows(instance, query, RESTRICTED)] == [
        n for n in PET_NAMES if n != "Lion 1"
    ]
