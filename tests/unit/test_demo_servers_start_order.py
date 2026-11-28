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
(tests/smoke/test_main_is_startable.py).

run-demo-servers.sh signals only the servers its own pid files name, never a process found on a
port: another checkout's demo servers on the same ports belong to another instance."""

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
_IGNORE_TERM = "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
_STUBBORN = (
    r"""
import signal, socket, sys, time
"""
    + _IGNORE_TERM
    + r"""
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1])))
s.listen()
print("listening", flush=True)
time.sleep(600)
"""
)


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
                os.kill(int(pidfile.read_text().splitlines()[0]), signal.SIGKILL)
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


def _started_at(pid: int) -> str:
    """The start time run-demo-servers.sh records for a pid (ps lstart, as $(...) captures it)."""
    out = subprocess.run(
        ["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, check=True
    )
    return out.stdout.rstrip("\n")


def _write_pidfile(tmp_path: Path, name: str, pid: int, started: str) -> Path:
    logs = tmp_path / "logs"
    logs.mkdir(exist_ok=True)
    pidfile = logs / f"{name}.pid"
    pidfile.write_text(f"{pid}\n{started}\n")
    return pidfile


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _stop(tmp_path: Path, ports: list[int]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(_RUN_DEMO_SERVERS), "stop"],
        env=_env(tmp_path, ports, 5),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _hold(port: int, ignore_term: bool = True) -> subprocess.Popen:
    code = _STUBBORN if ignore_term else _STUBBORN.replace(_IGNORE_TERM, "")
    holder = subprocess.Popen(
        [sys.executable, "-c", code, str(port)], stdout=subprocess.PIPE, text=True
    )
    assert holder.stdout is not None and holder.stdout.readline().strip() == "listening"
    return holder


def test_an_earlier_server_still_on_the_port_fails_the_start_by_name(demo_servers, tmp_path):
    """An earlier graphql-demo this checkout started, still listening, would answer the readiness
    probe in place of the new one, which then fails to bind; the start signals it, waits for the
    port to be let go, and fails if not."""
    gql = demo_servers.ports[1]
    holder = _hold(gql)
    try:
        _write_pidfile(tmp_path, "graphql-server", holder.pid, _started_at(holder.pid))
        done = demo_servers(2)
        assert done.returncode != 0, done.stdout + done.stderr
        assert (
            f"Demo server graphql-server: port {gql} is still held by an earlier process"
            in done.stderr
        ), done.stderr
    finally:
        holder.kill()
        holder.wait()


def test_a_port_held_by_a_process_this_checkout_did_not_start_fails_the_start_and_survives(
    demo_servers, tmp_path
):
    """Another checkout's graphql-demo on the port: start names it, fails, and leaves it running;
    stop leaves it running too."""
    gql = demo_servers.ports[1]
    foreign = _hold(gql, ignore_term=False)
    try:
        done = demo_servers(2)
        assert done.returncode != 0, done.stdout + done.stderr
        assert (
            f"Demo server graphql-server: port {gql} is held by another process (pid {foreign.pid})"
            in done.stderr
        ), done.stderr
        assert foreign.poll() is None, "start signalled a process it did not start"
        stopped = _stop(tmp_path, demo_servers.ports)
        assert stopped.returncode == 0, stopped.stdout + stopped.stderr
        time.sleep(0.5)
        assert foreign.poll() is None, "stop signalled a process it did not start"
        socket.create_connection(("127.0.0.1", gql), timeout=2).close()
    finally:
        foreign.kill()
        foreign.wait()


def test_stop_signals_only_the_servers_its_pid_files_name(demo_servers, tmp_path):
    done = demo_servers(15)
    assert done.returncode == 0, done.stdout + done.stderr
    logs = tmp_path / "logs"
    owned = {
        name: int((logs / f"{name}.pid").read_text().splitlines()[0])
        for name in ("petstore-server", "graphql-server", "grpc-server")
    }
    (spare,) = lease_ports(1)
    bystander = _hold(spare, ignore_term=False)
    try:
        stopped = _stop(tmp_path, demo_servers.ports)
        assert stopped.returncode == 0, stopped.stdout + stopped.stderr
        deadline = time.monotonic() + 10
        while any(_alive(pid) for pid in owned.values()) and time.monotonic() < deadline:
            time.sleep(0.1)
        assert not [name for name, pid in owned.items() if _alive(pid)]
        assert not list(logs.glob("*.pid"))
        assert bystander.poll() is None
    finally:
        bystander.kill()
        bystander.wait()


def test_a_pid_file_whose_process_is_gone_signals_nothing(demo_servers, tmp_path):
    gone = subprocess.Popen([sys.executable, "-c", "pass"])
    started = _started_at(gone.pid)
    gone.wait()
    pidfile = _write_pidfile(tmp_path, "petstore-server", gone.pid, started)
    stopped = _stop(tmp_path, demo_servers.ports)
    assert stopped.returncode == 0, stopped.stdout + stopped.stderr
    assert f"pid file {pidfile} names no process this checkout started" in stopped.stderr
    assert not pidfile.exists()


@pytest.mark.parametrize("recorded", ["Thu Jan  1 00:00:00 1970", None])
def test_a_pid_file_whose_pid_now_names_another_process_signals_nothing(
    demo_servers, tmp_path, recorded
):
    """The pid was reused: the process it names now started at another time than the one recorded
    (or the file records no start time), so it is not this checkout's server."""
    other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    try:
        pidfile = tmp_path / "logs" / "graphql-server.pid"
        if recorded is None:
            pidfile.parent.mkdir(exist_ok=True)
            pidfile.write_text(f"{other.pid}\n")
        else:
            _write_pidfile(tmp_path, "graphql-server", other.pid, recorded)
        stopped = _stop(tmp_path, demo_servers.ports)
        assert stopped.returncode == 0, stopped.stdout + stopped.stderr
        assert "names no process this checkout started" in stopped.stderr
        time.sleep(0.5)
        assert other.poll() is None, "stop signalled a reused pid"
        assert not pidfile.exists()
    finally:
        other.kill()
        other.wait()


def test_no_launcher_kills_a_demo_server_by_port():
    script = _RUN_DEMO_SERVERS.read_text()
    assert "pkill" not in script and "killall" not in script and "fuser" not in script
    assert not re.search(r"lsof[^\n]*\|\s*xargs\s+kill", script)
    for launcher in ("start-ui-install.sh", "start-ui.sh"):
        for port_var in ("PET_PORT", "GQL_PORT", "GRPC_PORT", "18080", "4000", "50071"):
            for line in (_REPO / launcher).read_text().splitlines():
                if port_var in line:
                    assert "kill" not in line, (launcher, line)


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
