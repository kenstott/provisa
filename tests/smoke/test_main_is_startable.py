# Copyright (c) 2026 Kenneth Stott
# Canary: 1424d70f-0ae8-42c2-8fcc-c62d850e9128
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

    PYTEST_NO_DOCKER=1 .venv/bin/python -m pytest -m isolated \
        tests/smoke/test_main_is_startable.py --no-cov -v

(PYTEST_NO_DOCKER: tests/conftest.py would otherwise start its compose stack; this check uses none.)

It needs `uv sync --extra dev --extra gx --extra soda`, provisa-client/ installed, and
`npm ci && npx playwright install chromium` in provisa-ui. It starts and removes everything it uses,
and leaves the instance's logs in PROVISA_SMOKE_LOG_DIR when that is set. CI runs the same command
(.github/workflows/smoke.yml).

With PROVISA_SMOKE_INSTALLED=1 the same checks run against a host scripts/install-host.sh installed
(REQ-2269, .github/workflows/release-install-host.yml): its systemd service, its Trino engine and its
control plane, not a backend started here. PROVISA_SMOKE_INSTALL_PREFIX, _CONFIG_DIR, _DATA_DIR,
_API_PORT and _UI_PORT name what the script was given. The harness gives the service the smoke
configuration and the demo's side services as an operator would, through a systemd drop-in, and
needs sudo for that alone.
"""

# Requirements: REQ-1989

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.integration import mcp_pin
from tests.integration.mcp_pin import principal_env, seat_principal
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
CREATE TABLE smoke_unregistered_a (id integer PRIMARY KEY, label text NOT NULL);
INSERT INTO smoke_unregistered_a VALUES (1, 'a1'), (2, 'a2');
CREATE TABLE smoke_unregistered_b (id integer PRIMARY KEY, label text NOT NULL);
INSERT INTO smoke_unregistered_b VALUES (1, 'b1');
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
    # The demo pet store's base URL, built from the port this harness leased for it.
    petstore: str
    env: dict[str, str] = field(default_factory=dict)
    # Stops the backend and starts it again on the same home, control plane and ports; returns
    # once it answers /health. A test reproducing what a restart of the app does calls it.
    restart_backend: Any = None
    # What the backend has logged since it started.
    backend_log: Callable[[], str] = lambda: ""
    # The UI a browser opens: None starts the page server of this checkout against the backend.
    ui_url: str | None = None

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


@dataclass
class _Sides:
    """The demo's side services and the source database the smoke configuration reads."""

    petstore: str
    graphql: str
    demo_grpc: int
    source: dict[str, str]


# How long each side service has to answer before the instance fails to start, naming it.
_SIDE_SECONDS = 60

# The request the backend's graphql-demo registration makes: an introspection POST.
_INTROSPECTION = {"query": "{ __schema { queryType { name } } }"}


def _side_answers(kind: str, port: int) -> bool:
    try:
        if kind == "http":
            url = f"http://127.0.0.1:{port}/api/v3/pet/findByStatus?status=available"
            return httpx.get(url, timeout=2).status_code == 200
        if kind == "graphql":
            url = f"http://127.0.0.1:{port}/graphql"
            return httpx.post(url, json=_INTROSPECTION, timeout=2).status_code == 200
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            return True
    except (httpx.TransportError, OSError):
        return False  # not listening yet: the caller's deadline decides


def _await_side(
    name: str, process: subprocess.Popen, kind: str, port: int, log: Path, seconds: float
) -> None:
    """Return once the side service answers; fail naming it when it exits or does not answer.

    The backend registers the demo's sources once at boot, so it starts only after every side
    service it reads answers."""
    deadline = time.monotonic() + seconds
    while not _side_answers(kind, port):
        tail = log.read_text()[-4000:] if log.exists() else ""
        assert process.poll() is None, f"the {name} side service exited:\n{tail}"
        assert time.monotonic() < deadline, (
            f"the {name} side service did not answer on port {port} in {seconds}s:\n{tail}"
        )
        time.sleep(0.5)


def _spawn_sides(
    log_dir: Path, base: dict[str, str]
) -> tuple[list[subprocess.Popen], int, int, int]:
    """The pet store, the GraphQL demo and the gRPC demo, each on a leased port, each answering
    before this returns."""
    pet, gql, demo_grpc = lease_ports(3)
    processes = []
    for name, app_dir, port in (
        ("petstore", "demo/petstore_server", pet),
        ("graphql-demo", "demo/graphql_server", gql),
    ):
        processes.append(
            _spawn(
                name,
                [
                    "-m",
                    "uvicorn",
                    "server:app",
                    "--app-dir",
                    app_dir,
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
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
    probes = (
        ("petstore", "http", pet),
        ("graphql-demo", "graphql", gql),
        ("grpc-demo", "tcp", demo_grpc),
    )
    try:
        for process, (name, kind, port) in zip(processes, probes, strict=True):
            _await_side(name, process, kind, port, log_dir / f"{name}.log", _SIDE_SECONDS)
    except BaseException:
        # The caller never receives them: stopped here, so a failed start leaves none running.
        _stop(processes)
        raise
    return processes, pet, gql, demo_grpc


def _app_env(
    sides: _Sides, api: int, grpc_port: int, flight: int, pgwire: int, mcp: int, bolt: int
) -> dict[str, str]:
    """What the backend is given beside its home and control plane, in either mode."""
    return {
        "PROVISA_CONFIG_REPLACE": "true",
        "PROVISA_DEMO": "true",
        "PROVISA_IDP": "",
        "PROVISA_ENABLE_TEST_ENDPOINTS": "true",
        "PROVISA_LOCAL_ADDRESS": f"http://localhost:{api}",
        "GRPC_PORT": str(grpc_port),
        "FLIGHT_PORT": str(flight),
        "PROVISA_PGWIRE_PORT": str(pgwire),
        "PROVISA_MCP_PORT": str(mcp),
        "PROVISA_MCP_HOST": "127.0.0.1",
        "PROVISA_MCP_ROLE": f"{ADMIN},{RESTRICTED}",
        # REQ-1105: the principal the MCP agent acts for, seated once the backend is up.
        **principal_env(),
        "PROVISA_BOLT_PORT": str(bolt),
        "PROVISA_AIRPORT_PORT": "0",
        # A real client's route to the service's descriptors on this no-auth demo.
        "GRPC_ALLOW_UNSECURED_REFLECTION": "true",
        "GRAPHQL_DEMO_ENABLED": "true",
        "GRAPHQL_DEMO_URL": sides.graphql,
        "PETSTORE_BASE_URL": sides.petstore,
        "DEMO_GRPC_PORT": str(sides.demo_grpc),
        "DEMO_GRPC_TARGET": f"localhost:{sides.demo_grpc}",
        "SMOKE_PG_HOST": sides.source["PG_HOST"],
        "SMOKE_PG_PORT": sides.source["PG_PORT"],
        "SMOKE_PG_PASSWORD": sides.source["PG_PASSWORD"],
    }


def _stop(processes: list[subprocess.Popen]) -> None:
    for process in processes:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()


@pytest.fixture(scope="module")
def instance() -> Iterator[Instance]:
    if os.environ.get("PROVISA_SMOKE_INSTALLED") == "1":
        yield from _installed_instance()
    else:
        yield from _local_instance()


def _local_instance() -> Iterator[Instance]:
    # A short root: a unix socket's path is limited to about 100 characters on macOS.
    root = Path(tempfile.mkdtemp(prefix="provisa-smoke-", dir="/tmp"))
    home = root / "home"
    (home / "demo").mkdir(parents=True)
    log_dir = Path(os.environ.get("PROVISA_SMOKE_LOG_DIR", str(root / "logs")))
    log_dir.mkdir(parents=True, exist_ok=True)
    api, grpc_port, flight, pgwire, mcp, bolt, ui = lease_ports(7)
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
        spawned, pet, gql, demo_grpc = _spawn_sides(log_dir, base)
        processes.extend(spawned)
        sides = _Sides(
            f"http://localhost:{pet}/api/v3", f"http://localhost:{gql}/graphql", demo_grpc, source
        )
        env = {
            **base,
            **_app_env(sides, api, grpc_port, flight, pgwire, mcp, bolt),
            "PROVISA_HOME": str(home),
            "PROVISA_DATA_DIR": str(home / "demo"),
            "PROVISA_CONFIG": CONFIG,
            "PROVISA_ENGINE": "duckdb",
            "PROVISA_REDIS_EMBEDDED": "1",
            "PROVISA_FAKE_KEY_DIR": str(home / "fake-key"),
            "PROVISA_ENGINE_FILES_GID": str(os.getgid()),
            "PROVISA_API_PORT": str(api),
            "TENANT_DATABASE_URL": database_url,
            "PLATFORM_DATABASE_URL": database_url,
            "PG_HOST": host,
            "PG_PORT": port,
            "PG_DATABASE": "provisa",
            "PG_USER": "provisa",
            "PG_PASSWORD": password,
            "POSTGRES_HOST": host,
        }
        backend = _spawn(
            "backend",
            ["-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(api)],
            log_dir,
            env,
        )
        processes.append(backend)
        restarts = 0

        def _restart_backend() -> None:
            nonlocal backend, restarts
            os.killpg(backend.pid, signal.SIGTERM)
            backend.wait(timeout=60)
            restarts += 1
            # A log of its own: the earlier backends' are kept.
            name = f"backend-restart-{restarts}"
            backend = _spawn(
                name,
                ["-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(api)],
                log_dir,
                env,
            )
            processes.append(backend)
            _wait_for_health(backend, running.url, log_dir / f"{name}.log")

        running = Instance(
            api,
            grpc_port,
            flight,
            pgwire,
            mcp,
            bolt,
            ui,
            log_dir,
            sides.petstore,
            base,
            restart_backend=_restart_backend,
            backend_log=lambda: (log_dir / "backend.log").read_text(errors="replace"),
        )
        _wait_for_health(backend, running.url, log_dir / "backend.log")
        seat_principal(database_url)
        yield running
    finally:
        _stop(processes)
        for datadir in (control_dir, source_dir):
            if (datadir / "PG_VERSION").exists():
                _python(["-m", "provisa.core.control_plane_pg", "stop", str(datadir)], base)
        shutil.rmtree(root)


# --- the installed host (REQ-2269) -----------------------------------------------------------------

_SERVICE = "provisa"
_SERVICE_USER = "provisa"
_DROP_IN = Path(f"/etc/systemd/system/{_SERVICE}.service.d/smoke.conf")


@dataclass
class _Install:
    """What scripts/install-host.sh was given; every value named by the lane."""

    prefix: Path
    config_dir: Path
    data_dir: Path
    api: int
    ui: int

    @classmethod
    def from_env(cls) -> _Install:
        named = {
            n: os.environ.get(f"PROVISA_SMOKE_INSTALL_{n}", "")
            for n in ("PREFIX", "CONFIG_DIR", "DATA_DIR", "API_PORT", "UI_PORT")
        }
        missing = [f"PROVISA_SMOKE_INSTALL_{n}" for n, v in named.items() if not v]
        assert not missing, f"an installed-host smoke run names {', '.join(missing)}"
        return cls(
            Path(named["PREFIX"]),
            Path(named["CONFIG_DIR"]),
            Path(named["DATA_DIR"]),
            int(named["API_PORT"]),
            int(named["UI_PORT"]),
        )

    @property
    def python(self) -> str:
        return str(self.prefix / "current" / "bin" / "python")


def _root(args: list[str], stdin: str | None = None) -> str:
    """Run a command under sudo; a non-zero exit fails the run with what it printed."""
    done = subprocess.run(["sudo", *args], input=stdin, capture_output=True, text=True, check=False)
    assert done.returncode == 0, f"sudo {args}\n{done.stdout}\n{done.stderr}"
    return done.stdout


def _as_service(install: _Install, args: list[str], cwd: Path) -> str:
    """The installed Python, as the service's user: the embedded PostgreSQL servers it reaches
    listen on sockets only their owner opens."""
    done = subprocess.run(
        ["sudo", "-u", _SERVICE_USER, "-H", install.python, *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, f"{args}\n{done.stdout}\n{done.stderr}"
    return done.stdout


def _pg_fields(out: str) -> dict[str, str]:
    fields = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
    return {k: fields[k] for k in ("PG_HOST", "PG_PORT", "PG_PASSWORD")}


def _installed_config(install: _Install, root: Path) -> Path:
    """The smoke configuration, beside the demo's file it includes, in a copy the service's user
    reads (the checkout is under the runner's home), with the federation engine the install's own
    configuration names: the config selects the engine (REQ-2269)."""
    import yaml

    installed = yaml.safe_load(_root(["cat", str(install.config_dir / "provisa.yaml")]))
    engine = {k: v for k, v in installed.items() if k.startswith("federation_engine")}
    assert engine.get("federation_engine") == "trino", installed
    (root / "config").mkdir()
    shutil.copy(REPO / "config" / "provisa-install.yaml", root / "config")
    smoke = root / CONFIG
    smoke.parent.mkdir(parents=True)
    smoke.write_text((REPO / CONFIG).read_text() + "\n" + yaml.safe_dump(engine))
    shutil.copytree(REPO / "demo" / "files", root / "demo" / "files")
    return smoke


def _restart_service(install: _Install) -> float:
    """Restart the service; returns when its API answers /health, with the time it was started."""
    started = time.time()
    _root(["systemctl", "daemon-reload"])
    _root(["systemctl", "restart", _SERVICE])
    url = f"http://127.0.0.1:{install.api}/health"
    deadline = time.monotonic() + _BOOT_SECONDS
    while True:
        active = subprocess.run(["systemctl", "is-active", "--quiet", _SERVICE], check=False)
        assert active.returncode == 0, _journal(started)[-4000:]
        try:
            if httpx.get(url, timeout=5).status_code == 200:
                return started
        except httpx.TransportError:
            pass  # not listening yet: the deadline below decides
        assert time.monotonic() < deadline, (
            f"no /health in {_BOOT_SECONDS}s:\n{_journal(started)[-4000:]}"
        )
        time.sleep(1)


def _journal(since: float) -> str:
    return _root(
        ["journalctl", "-u", _SERVICE, "--no-pager", "-o", "cat", "--since", f"@{int(since)}"]
    )


def _installed_instance() -> Iterator[Instance]:
    install = _Install.from_env()
    root = Path(tempfile.mkdtemp(prefix="provisa-smoke-", dir="/tmp"))
    # The service's user reads the configuration and the demo's files from here.
    root.chmod(0o755)
    log_dir = Path(os.environ.get("PROVISA_SMOKE_LOG_DIR", str(root / "logs")))
    log_dir.mkdir(parents=True, exist_ok=True)
    grpc_port, flight, pgwire, mcp, bolt = lease_ports(5)
    source_dir = install.data_dir / "smoke-source-pg"
    processes: list[subprocess.Popen] = []
    base = {k: v for k, v in os.environ.items() if not k.startswith(("PROVISA_", "PG_", "TENANT_"))}
    dropped_in = source_started = False
    try:
        _python(["demo/files/create_demo_files.py"], base)
        config = _installed_config(install, root)
        seed = root / "seed.sql"
        seed.write_text(_SEED_SOURCE_PG)
        seed.chmod(0o644)
        start = ["-m", "provisa.core.control_plane_pg", "start"]
        source_started = True
        source = _pg_fields(_as_service(install, [*start, str(source_dir)], root))
        _as_service(
            install,
            ["-m", "provisa.core.control_plane_pg", "apply", str(source_dir), "--file", str(seed)],
            root,
        )
        spawned, pet, gql, demo_grpc = _spawn_sides(log_dir, base)
        processes.extend(spawned)
        sides = _Sides(
            f"http://localhost:{pet}/api/v3", f"http://localhost:{gql}/graphql", demo_grpc, source
        )
        env = {
            **_app_env(sides, install.api, grpc_port, flight, pgwire, mcp, bolt),
            "PROVISA_CONFIG": str(config),
            "PROVISA_DEMO_DIR": str(root / "demo" / "files"),
        }
        smoke_env = root / "smoke.env"
        smoke_env.write_text("".join(f"{k}={v}\n" for k, v in env.items()))
        smoke_env.chmod(0o644)
        # A later EnvironmentFile wins over the install's own provisa.env.
        _root(["mkdir", "-p", str(_DROP_IN.parent)])
        _root(
            ["tee", str(_DROP_IN)],
            stdin=f"[Service]\nEnvironmentFile={smoke_env}\n",
        )
        dropped_in = True
        started = _restart_service(install)

        # The service's own control plane, which `provisa run` started under its data directory:
        # a second start of the same directory names the server already running there.
        control = _pg_fields(
            _as_service(install, [*start, str(install.data_dir / "native" / "control-pg")], root)
        )
        database_url = (
            f"postgresql+psycopg://provisa:{control['PG_PASSWORD']}@/provisa"
            f"?host={control['PG_HOST']}&port={control['PG_PORT']}"
        )
        pin = root / "mcp_pin.py"
        shutil.copy(Path(mcp_pin.__file__), pin)
        pin.chmod(0o644)
        _as_service(
            install,
            [
                "-c",
                "import sys; sys.path.insert(0, sys.argv[1]); import mcp_pin; "
                "mcp_pin.seat_principal(sys.argv[2])",
                str(root),
                database_url,
            ],
            root,
        )

        def _restart_backend() -> None:
            nonlocal started
            started = _restart_service(install)

        yield Instance(
            install.api,
            grpc_port,
            flight,
            pgwire,
            mcp,
            bolt,
            install.ui,
            log_dir,
            sides.petstore,
            base,
            restart_backend=_restart_backend,
            backend_log=lambda: _journal(started),
            ui_url=f"http://localhost:{install.ui}",
        )
    finally:
        _stop(processes)
        if dropped_in:
            (log_dir / "backend.log").write_text(_journal(0))
            _root(["rm", "-f", str(_DROP_IN)])
            _root(["systemctl", "daemon-reload"])
            _root(["systemctl", "restart", _SERVICE])
        if source_started:
            _as_service(
                install, ["-m", "provisa.core.control_plane_pg", "stop", str(source_dir)], root
            )
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
    # An installed host serves its own UI; otherwise this checkout's page server is started.
    page = {"PROVISA_SMOKE_UI_URL": instance.ui_url} if instance.ui_url else {}
    done = subprocess.run(
        ["npx", "playwright", "test", "--config", "playwright.smoke.config.ts"],
        cwd=UI,
        env={
            **instance.env,
            **page,
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


def test_the_graphql_demo_source_registers_at_startup(instance: Instance):
    """The boot's graphql-demo auto-registration runs after /health and logs its outcome once:
    the success line, never the failure line."""
    registered = "Auto-registered graphql-demo source"
    failed = "graphql-demo auto-registration failed"
    deadline = time.monotonic() + _BOOT_SECONDS
    text = instance.backend_log()
    while registered not in text and failed not in text:
        assert time.monotonic() < deadline, f"no graphql-demo registration outcome:\n{text[-4000:]}"
        time.sleep(1)
        text = instance.backend_log()
    # The failure line and its traceback, from the start of that line.
    assert failed not in text, text[text.rfind("\n", 0, text.index(failed)) + 1 :][:4000]
    assert registered in text


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
