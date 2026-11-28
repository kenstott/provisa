# Copyright (c) 2026 Kenneth Stott
# Canary: e948fe0f-e39c-4896-bcd8-0ebb1c4e2b19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Every launcher of the demo starts its side servers (petstore, graphql-demo, grpc-demo) and waits
for each to answer before it starts Provisa, failing the start by name when one never answers.

Provisa reads them once at boot -- graphql-demo is introspected by one background attempt
(app_startup._auto_register_graphql_demo), petstore's OpenAPI spec is loaded once -- so a backend
started before they answer logs "graphql-demo auto-registration failed" and serves the demo without
that source until the next start. The launchers: the native demo (start-ui-install.sh --demo and
start-ui.sh, through demo/run-demo-servers.sh), the compose demo overlay
(packaging/linux/first-launch.sh write_demo_overlay), and the smoke harness
(tests/smoke/test_main_is_startable.py)."""

# Requirements: REQ-1989, REQ-1468

from __future__ import annotations

import os
import re
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

from tests.port_lease import lease_ports

_REPO = Path(__file__).resolve().parents[2]
_RUN_DEMO_SERVERS = _REPO / "demo" / "run-demo-servers.sh"
_FIRST_LAUNCH = _REPO / "packaging" / "linux" / "first-launch.sh"

# Stands in for the demo servers: answers every GET and POST with 200 on the port it was given,
# unless FAKE_SILENT names its app (then it runs without listening) or FAKE_EXIT does (then it
# exits at once).
_FAKE_SERVER = r"""
import http.server, os, socket, sys, time
args = sys.argv[1:]
if args[:2] == ["-m", "uvicorn"]:
    app, port = args[args.index("--app-dir") + 1], int(args[args.index("--port") + 1])
else:
    app, port = "grpc_server", int(os.environ["DEMO_GRPC_PORT"])
name = os.path.basename(app.rstrip("/"))
if name in os.environ.get("FAKE_EXIT", "").split(","):
    sys.exit(3)
if name in os.environ.get("FAKE_SILENT", "").split(","):
    time.sleep(600)
    sys.exit(0)
class Handler(http.server.BaseHTTPRequestHandler):
    def _ok(self):
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")
    do_GET = do_POST = _ok
    def log_message(self, *a):
        pass
http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
"""

# Holds a port and ignores SIGTERM: an earlier server that has not let go of its port yet.
_STUBBORN = r"""
import signal, socket, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1])))
s.listen()
print("listening", flush=True)
time.sleep(600)
"""


def _fake_python(tmp_path: Path) -> Path:
    server = tmp_path / "fake_server.py"
    server.write_text(_FAKE_SERVER)
    fake = tmp_path / "fake-python"
    fake.write_text(f'#!/usr/bin/env bash\nexec "{sys.executable}" "{server}" "$@"\n')
    fake.chmod(0o755)
    return fake


def _env(tmp_path: Path, ports: list[int], wait_seconds: int, **extra: str) -> dict[str, str]:
    # Every port is a leased one: the defaults (18080, 4000, 50071) are the maintainer's.
    pet, gql, grpc = ports
    return {
        **os.environ,
        "DEMO_PYTHON": str(_fake_python(tmp_path)),
        "DEMO_LOG_DIR": str(tmp_path / "logs"),
        "DEMO_PETSTORE_PORT": str(pet),
        "DEMO_GRAPHQL_PORT": str(gql),
        "DEMO_GRPC_PORT": str(grpc),
        "DEMO_SERVER_WAIT_SECONDS": str(wait_seconds),
        **extra,
    }


@pytest.fixture
def demo_servers(tmp_path):
    """Runs run-demo-servers.sh on leased ports; stops whatever it started afterwards."""
    ports = lease_ports(3)
    started: list[dict[str, str]] = []

    def run(wait_seconds: int, **extra: str) -> subprocess.CompletedProcess:
        env = _env(tmp_path, ports, wait_seconds, **extra)
        started.append(env)
        return subprocess.run(
            ["bash", str(_RUN_DEMO_SERVERS), "start"],
            env=env,
            capture_output=True,
            text=True,
            timeout=wait_seconds * 6 + 30,
            check=False,
        )

    run.ports = ports  # type: ignore[attr-defined]
    yield run
    for env in started:
        for pidfile in (tmp_path / "logs").glob("*.pid"):
            try:
                os.kill(int(pidfile.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass  # already gone
        subprocess.run(
            ["bash", str(_RUN_DEMO_SERVERS), "stop"], env=env, capture_output=True, check=False
        )


def test_start_returns_once_every_demo_server_answers(demo_servers):
    done = demo_servers(15)
    assert done.returncode == 0, done.stdout + done.stderr
    for name in ("petstore-server", "graphql-server", "grpc-server"):
        assert f"Waiting for {name} OK" in done.stdout.replace(".", ""), done.stdout
    for port in demo_servers.ports:
        socket.create_connection(("127.0.0.1", port), timeout=2).close()


def test_a_graphql_demo_that_never_answers_fails_the_start_by_name(demo_servers):
    began = time.monotonic()
    done = demo_servers(2, FAKE_SILENT="graphql_server")
    assert done.returncode != 0, done.stdout + done.stderr
    assert "Demo server graphql-server did not answer in 2s" in done.stderr, done.stderr
    assert time.monotonic() - began < 20


def test_a_graphql_demo_that_exits_fails_the_start_by_name(demo_servers):
    done = demo_servers(5, FAKE_EXIT="graphql_server")
    assert done.returncode != 0, done.stdout + done.stderr
    assert "Demo server graphql-server exited before it answered" in done.stderr, done.stderr


def test_a_grpc_demo_that_never_listens_fails_the_start_by_name(demo_servers):
    done = demo_servers(2, FAKE_SILENT="grpc_server")
    assert done.returncode != 0, done.stdout + done.stderr
    assert "Demo server grpc-server did not answer in 2s" in done.stderr, done.stderr


def test_an_earlier_server_still_on_the_port_fails_the_start_by_name(demo_servers):
    """An earlier graphql-demo still listening would answer the readiness probe in place of the new
    one, which then fails to bind; the start waits for the port to be let go, and fails if not."""
    gql = demo_servers.ports[1]
    holder = subprocess.Popen(
        [sys.executable, "-c", _STUBBORN, str(gql)], stdout=subprocess.PIPE, text=True
    )
    try:
        assert holder.stdout is not None and holder.stdout.readline().strip() == "listening"
        done = demo_servers(2)
        assert done.returncode != 0, done.stdout + done.stderr
        assert (
            f"Demo server graphql-server: port {gql} is still held by an earlier process"
            in done.stderr
        ), done.stderr
    finally:
        holder.kill()
        holder.wait()


@pytest.mark.parametrize("launcher", ["start-ui-install.sh", "start-ui.sh"])
def test_the_native_launchers_start_the_demo_servers_before_the_backend(launcher):
    lines = (_REPO / launcher).read_text().splitlines()
    assert "set -euo pipefail" in lines[:12], "a failed demo-server start must stop the launch"
    starts = [n for n, line in enumerate(lines) if 'run-demo-servers.sh" start' in line]
    assert len(starts) == 1, starts
    # Not masked: its failure stops the launch under set -e.
    assert re.fullmatch(r'\s*"\$SCRIPT_DIR/demo/run-demo-servers\.sh" start', lines[starts[0]])
    backend = [n for n, line in enumerate(lines) if re.fullmatch(r"_?start_backend", line)]
    assert len(backend) == 1, backend
    assert starts[0] < backend[0]


def _demo_overlay() -> dict:
    script = _FIRST_LAUNCH.read_text()
    begin = script.index("  cat > \"$file\" <<'YAML'\n", script.index("write_demo_overlay() {"))
    body = script[script.index("\n", begin) + 1 : script.index("\nYAML\n", begin)]
    return yaml.safe_load(body)


def test_the_compose_demo_overlay_starts_provisa_after_each_demo_server_is_healthy():
    services = _demo_overlay()["services"]
    mocks = ("petstore-mock", "graphql-demo", "grpc-demo")
    assert services["provisa"]["depends_on"] == {
        name: {"condition": "service_healthy"} for name in mocks
    }
    for name in mocks:
        assert services[name]["healthcheck"]["test"][0] == "CMD", name
    # The request Provisa's registration makes: an introspection POST to graphql-demo.
    probe = services["graphql-demo"]["healthcheck"]["test"][-1]
    assert "http://localhost:4000/graphql" in probe and "__schema" in probe and "data=" in probe


def test_the_smoke_harness_fails_by_name_when_a_side_service_exits(tmp_path):
    from tests.smoke import test_main_is_startable as smoke

    (port,) = lease_ports(1)
    dead = subprocess.Popen([sys.executable, "-c", "raise SystemExit(3)"])
    dead.wait()
    log = tmp_path / "graphql-demo.log"
    log.write_text("boom\n")
    with pytest.raises(AssertionError, match="the graphql-demo side service exited"):
        smoke._await_side("graphql-demo", dead, "graphql", port, log, 5)


def test_the_smoke_harness_fails_by_name_when_a_side_service_never_answers(tmp_path):
    from tests.smoke import test_main_is_startable as smoke

    (port,) = lease_ports(1)
    silent = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    try:
        with pytest.raises(AssertionError, match=f"did not answer on port {port} in 1"):
            smoke._await_side("graphql-demo", silent, "graphql", port, tmp_path / "none.log", 1)
    finally:
        silent.kill()
        silent.wait()


def test_the_smoke_harness_waits_for_each_side_service_before_returning():
    import inspect

    from tests.smoke import test_main_is_startable as smoke

    source = inspect.getsource(smoke._spawn_sides)
    assert "_await_side(" in source
    assert source.index("_await_side(") < source.index("return processes")
    for name, kind in (("petstore", "http"), ("graphql-demo", "graphql"), ("grpc-demo", "tcp")):
        assert f'("{name}", "{kind}",' in source, name
