# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d3c58-9b2a-4e07-a4c1-7d5e0b8f2a39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Boot ``uvicorn main:app --workers N`` against a FRESH control plane and measure it (REQ-1900).

The test instance only: its own database on the Postgres it is pointed at, its own org, its own
data directory, and every port leased from ``tests/port_lease.py``. Used two ways:

* by the multi-worker boot tests, as :class:`WorkerBoot`;
* as a script — the reproducer for boot time and listener behaviour::

      .venv/bin/python3 -m tests.integration.worker_boot_harness --workers 4 --pg-port 21100

  prints time-to-first-ready, time-to-all-ready, the per-worker startup phase table and which
  worker processes listen on (and accept connections on) each protocol port.
"""

# Requirements: REQ-1900

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

import sqlalchemy as sa
import yaml

from tests.port_lease import lease_ports

_REPO_ROOT = Path(__file__).parents[2]

_PHASE_RE = re.compile(
    r"startup phase (?P<name>.+?)\s+\+\s*(?P<delta>[\d.]+)s \(total\s+(?P<total>[\d.]+)s\) "
    r"pid=(?P<pid>\d+)"
)
# How long the supervisor waits on a worker's ping answer. It also bounds how long a worker killed
# with that ping unanswered goes unnoticed: the supervisor holds the pipe's other end open.
WORKER_HEALTHCHECK_S = 120

_READY_RE = re.compile(r"startup phase worker\s+ready pid=(\d+)")
_TRANSPORTS = ("http", "pgwire", "flight", "grpc", "bolt", "mcp", "airport")
# A direct do_get ticket, as Flight clients send it: the two rows create_database() seeds.
FLIGHT_QUERY = "SELECT id, region FROM sales.orders"


def _config(pg_host: str, pg_port: int, database: str) -> dict:
    return {
        "sources": [
            {
                "id": "sales-pg",
                "type": "postgresql",
                "host": pg_host,
                "port": pg_port,
                "database": database,
                "username": "provisa",
                "password": "${env:PG_PASSWORD}",
            }
        ],
        "domains": [{"id": "sales", "description": "boot harness"}],
        "naming": {"domain_prefix": True},
        "auth": {"provider": "none"},
        "tables": [
            {
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "integer",
                        "visible_to": ["org_admin", "analyst"],
                    },
                    {
                        "name": "region",
                        "data_type": "varchar",
                        "visible_to": ["org_admin", "analyst"],
                    },
                ],
            }
        ],
        # org_admin is the reserved administrative role (REQ-1349): every org has it and a config
        # file may not declare it, so only the harness's own role is listed.
        "roles": [
            # No full_results: the default row limit applies to this role's queries.
            {"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]},
        ],
    }


def _group_members(pgid: int) -> list[int]:
    """Live processes in process group ``pgid``. Listed and signalled one by one rather than with
    ``killpg``: darwin answers EPERM for a group whose only remaining members are exited
    processes not yet collected, which is the normal state right after a clean stop."""
    listed = subprocess.run(
        ["ps", "-axo", "pid=,pgid=,stat="], capture_output=True, text=True, check=True
    )
    members = []
    for line in listed.stdout.splitlines():
        pid, group, state = line.split()
        if int(group) == pgid and not state.startswith("Z"):
            members.append(int(pid))
    return members


# The boot org a launch serves unless its environment names another (ORG_ID).
_BOOT_ORG = "default"


class WorkerBoot:
    """One ``uvicorn --workers N`` launch on a fresh control-plane database."""

    def __init__(
        self,
        workers: int,
        *,
        pg_host: str,
        pg_port: int,
        pg_user: str = "provisa",
        pg_password: str = "provisa",
        admin_database: str = "provisa",
        engine: str = "duckdb",
        redirect_endpoint: str | None = None,
        database: str | None = None,
        data_dir: str | None = None,
        extra_config: dict | None = None,
        env: dict[str, str] | None = None,
        per_worker_http: bool = False,
    ) -> None:
        self.workers = workers
        # Launch as start-ui-install.sh does for several workers on Linux: uvicorn's supervisor
        # on a unix socket, each worker on its own SO_REUSEPORT socket on the HTTP port.
        self._per_worker_http = per_worker_http
        self._extra_config = extra_config or {}
        # Extra environment for the server, applied last.
        self._extra_env = dict(env or {})
        # The boot org the launch serves: the harness's own, unless the test's environment names
        # another (it is applied after the harness's, see start()).
        self.org_id = self._extra_env.get("ORG_ID", _BOOT_ORG)
        self._pg = (pg_host, pg_port, pg_user, pg_password)
        self._base = f"postgresql+psycopg://{pg_user}:{pg_password}@{pg_host}:{pg_port}"
        self._admin_url = f"{self._base}/{admin_database}"
        # A relaunch against the SAME control plane passes the first launch's database + data dir.
        self._owns_database = database is None
        self.database = database or f"wboot_{uuid.uuid4().hex[:10]}"
        self.url = f"{self._base}/{self.database}"
        self._engine = engine
        self._owns_data_dir = data_dir is None
        self.data_dir = data_dir or tempfile.mkdtemp(prefix="provisa-wboot-")
        ports = lease_ports(len(_TRANSPORTS) + 1)
        self.ports = dict(zip(_TRANSPORTS, ports))
        # Nothing listens here: the redirect endpoint a deployment configures but does not run.
        self._dead_port = ports[-1]
        self._redirect_endpoint = redirect_endpoint
        self.log_path = Path(self.data_dir) / f"backend-{uuid.uuid4().hex[:6]}.log"
        self._proc: subprocess.Popen | None = None
        self._started = 0.0

    # -- lifecycle ---------------------------------------------------------------------------

    def create_database(self) -> None:
        admin = sa.create_engine(self._admin_url, isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            conn.execute(sa.text(f'CREATE DATABASE "{self.database}"'))
        admin.dispose()
        own = sa.create_engine(self.url, isolation_level="AUTOCOMMIT")
        with own.connect() as conn:
            conn.execute(
                sa.text("CREATE TABLE public.orders (id integer PRIMARY KEY, region text)")
            )
            conn.execute(sa.text("INSERT INTO public.orders VALUES (1, 'east'), (2, 'west')"))
        own.dispose()

    def drop_database(self) -> None:
        admin = sa.create_engine(self._admin_url, isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{self.database}" WITH (FORCE)'))
        admin.dispose()

    def start(self) -> None:
        host, port, user, password = self._pg
        cfg_path = Path(self.data_dir) / "provisa.yaml"
        cfg_path.write_text(
            yaml.safe_dump({**_config(host, port, self.database), **self._extra_config})
        )
        redirect = self._redirect_endpoint or f"http://127.0.0.1:{self._dead_port}"
        env = {
            **os.environ,
            "TENANT_DATABASE_URL": self.url,
            "PLATFORM_DATABASE_URL": self.url,
            "PG_HOST": host,
            "PG_PORT": str(port),
            "PG_USER": user,
            "PG_PASSWORD": password,
            "PG_DATABASE": self.database,
            "ORG_ID": _BOOT_ORG,
            "PROVISA_ENGINE": self._engine,
            "PROVISA_CONFIG": str(cfg_path),
            "PROVISA_IDP": "",
            "PROVISA_DEMO": "false",
            "PROVISA_REDIS_EMBEDDED": "1",
            "PROVISA_DATA_DIR": self.data_dir,
            "PROVISA_HOME": self.data_dir,
            # The launch's workers share ONE key store, as the workers of a real host do: the
            # file keystore under the launch's own data dir (REQ-1802). The test session's
            # keyring (tests/conftest.py) is a dict inside one process — inherited, it would give
            # every worker a master key no other worker can see, which no real keyring does.
            "PYTHON_KEYRING_BACKEND": "keyring.backends.fail.Keyring",
            # Licensing writes here, not to the user's ~/.provisa (provisa/licensing/home.py) —
            # also when this harness is run as a script, outside the test session's own sandbox.
            "PROVISA_LICENSING_SANDBOX_DIR": os.path.join(self.data_dir, "licensing"),
            "PROVISA_MATERIALIZE_URL": f"duckdb:///{self.data_dir}/store.duckdb",
            "PROVISA_REDIRECT_ENABLED": "true",
            "PROVISA_REDIRECT_ENDPOINT": redirect,
            "PROVISA_REDIRECT_ACCESS_KEY": "minioadmin",
            "PROVISA_REDIRECT_SECRET_KEY": "minioadmin",
            # What a launcher of several workers exports (start-ui-install.sh): the launch's
            # identity and its worker count, inherited by every worker.
            "PROVISA_WORKERS": str(self.workers),
            "PROVISA_LAUNCH_ID": uuid.uuid4().hex,
            "FLIGHT_PORT": str(self.ports["flight"]),
            "GRPC_PORT": str(self.ports["grpc"]),
            "PROVISA_PGWIRE_PORT": str(self.ports["pgwire"]),
            "PROVISA_BOLT_PORT": str(self.ports["bolt"]),
            "PROVISA_MCP_PORT": str(self.ports["mcp"]),
            "PROVISA_MCP_HOST": "127.0.0.1",
            "PROVISA_AIRPORT_PORT": str(self.ports["airport"]),
            "OTEL_SDK_DISABLED": "true",
            **(
                {"PROVISA_HTTP_LISTEN": f"127.0.0.1:{self.ports['http']}"}
                if self._per_worker_http
                else {}
            ),
            **self._extra_env,
        }
        self._log = open(self.log_path, "w")
        self._started = time.monotonic()
        self._proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "main:app",
                "--workers",
                str(self.workers),
                *self._bind_args(),
                # A test pins a keep-alive connection to one worker and comes back to it later;
                # uvicorn's default closes an idle connection after 5 s.
                "--timeout-keep-alive",
                "600",
                # uvicorn's supervisor kills a worker that does not answer its ping within 5 s; on
                # a machine other test sessions are loading, a worker's startup takes longer.
                "--timeout-worker-healthcheck",
                str(WORKER_HEALTHCHECK_S),
            ],
            cwd=str(_REPO_ROOT),
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            # Its own session, so the launch is one process group — the supervisor and every
            # worker it spawns — and stop() can end all of it (see stop()).
            start_new_session=True,
        )

    def _bind_args(self) -> list[str]:
        if self._per_worker_http:
            return ["--uds", str(Path(self.data_dir) / "sup.sock")]
        return ["--host", "127.0.0.1", f"--port={self.ports['http']}"]

    def stop(self) -> None:
        """End the launch: the supervisor is asked to stop (it stops its workers), and then
        whatever is left of the launch's process group is killed.

        Killing the supervisor alone — what a stop that outlasted its wait used to do — leaves
        its workers running: a worker does not exit when its supervisor dies. Each orphan then
        retried this launch's dropped database at the session's Postgres port for as long as the
        machine stayed up, and later sessions leased that port number for something else."""
        if self._proc is not None:
            launch = self._proc.pid  # start_new_session: the supervisor leads the launch's group
            self._proc.terminate()
            try:
                self._proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                pass  # still running: it is ended with the rest of its group below
            for pid in _group_members(launch):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    continue  # ended between the listing and now
            self._proc.wait(timeout=20)
            self._proc = None
            self._log.close()
            self._keep_log()

    def _keep_log(self) -> None:
        """Copy the launch's log where the run keeps its results, when it names such a place
        (``PROVISA_TEST_SERVER_LOG_DIR``; the CI lanes do, and upload it). The log lives in the
        launch's data directory, which cleanup() removes: a worker that died or dropped a
        connection in a lane left nothing to read (config-sync, run 37718641831: "Remote end
        closed connection without response", and no log of the server that closed it)."""
        kept = os.environ.get("PROVISA_TEST_SERVER_LOG_DIR")
        if kept and self.log_path.exists():
            Path(kept).mkdir(parents=True, exist_ok=True)
            shutil.copy(self.log_path, Path(kept) / f"{self.database}-{self.log_path.name}")

    def cleanup(self) -> None:
        self.stop()
        if self._owns_database:
            self.drop_database()
        if self._owns_data_dir:
            shutil.rmtree(self.data_dir, ignore_errors=True)

    # -- observation -------------------------------------------------------------------------

    def log_text(self) -> str:
        return self.log_path.read_text(errors="replace")

    def health(self) -> dict | None:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{self.ports['http']}/health", timeout=3
            ) as resp:
                return json.loads(resp.read())
        except (OSError, ValueError):
            return None

    def _alive(self) -> None:
        assert self._proc is not None
        if self._proc.poll() is not None:
            raise RuntimeError(
                f"server exited (code {self._proc.returncode}):\n{self.log_text()[-4000:]}"
            )

    def wait_first_ready(self, timeout: float = 300.0) -> float:
        """Seconds from launch until /health answers."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._alive()
            if self.health() is not None:
                return time.monotonic() - self._started
            time.sleep(0.1)
        raise RuntimeError(f"no worker became healthy in {timeout}s:\n{self.log_text()[-4000:]}")

    def ready_pids(self) -> set[int]:
        """Worker processes that logged the end of their startup."""
        return {int(p) for p in _READY_RE.findall(self.log_text())}

    def _accepting(self) -> bool:
        """Whether the launch accepts on its HTTP port: with a socket per worker (REQ-1900), every
        worker listens on it -- any one accepting says nothing about the others, each of which
        binds only after its own startup returns; with one shared socket, a connection is
        accepted."""
        if self._per_worker_http:
            pids = set(self.worker_pids())
            return len(pids) == self.workers and self.listeners(self.ports["http"]) == pids
        try:
            socket.create_connection(("127.0.0.1", self.ports["http"]), timeout=1).close()
        except OSError:
            return False
        return True

    def wait_all_ready(self, timeout: float = 600.0) -> float:
        """Seconds from launch until every worker has finished its startup (each worker's own
        "worker ready" log line — independent of what /health reports) AND the launch accepts
        connections on its HTTP port (every worker listening on it, when each has its own socket).

        The ready line is logged inside the application's startup. A single uvicorn process
        binds its socket only after that startup has returned — after the line, the worker's
        registration in the control plane and whatever else the loop runs first — so a request
        sent the moment the line appears can be refused."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._alive()
            if len(self.ready_pids()) >= self.workers and self._accepting():
                return time.monotonic() - self._started
            time.sleep(0.1)
        raise RuntimeError(
            f"{len(self.ready_pids())}/{self.workers} workers ready after {timeout}s:\n"
            f"{self.log_text()[-4000:]}"
        )

    def phases(self) -> dict[int, list[tuple[str, float]]]:
        """pid -> [(phase, seconds)] from the lifespan's own "startup phase" log lines."""
        out: dict[int, list[tuple[str, float]]] = {}
        for m in _PHASE_RE.finditer(self.log_text()):
            out.setdefault(int(m["pid"]), []).append((m["name"].strip(), float(m["delta"])))
        return out

    def worker_pids(self) -> list[int]:
        return [int(p) for p in re.findall(r"Started server process \[(\d+)\]", self.log_text())]

    def listeners(self, port: int) -> set[int]:
        """PIDs holding a LISTEN socket on ``port``."""
        out = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-Fp"], capture_output=True, text=True
        ).stdout
        return {int(line[1:]) for line in out.splitlines() if line.startswith("p")}

    def wildcard_listening_ports(self, pid: int) -> set[int]:
        """Ports ``pid`` LISTENs on for every interface (not loopback-only)."""
        out = subprocess.run(
            ["lsof", "-nP", "-a", "-p", str(pid), "-iTCP", "-sTCP:LISTEN", "-Fn"],
            capture_output=True,
            text=True,
        ).stdout
        return {
            int(line.rsplit(":", 1)[1])
            for line in out.splitlines()
            if line.startswith("n") and line[1:].rsplit(":", 1)[0] in ("*", "[::]", "0.0.0.0")
        }

    def accepting_pids(self, port: int, connections: int = 50) -> dict[int, int]:
        """Open ``connections`` concurrent TCP connections to ``port`` and hold them; return
        {server pid: connections it accepted}, read from the kernel's socket table."""
        socks = []
        try:
            for _ in range(connections):
                s = socket.create_connection(("127.0.0.1", port), timeout=5)
                socks.append(s)
            time.sleep(1.0)
            out = subprocess.run(
                ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:ESTABLISHED", "-Fpn"],
                capture_output=True,
                text=True,
            ).stdout
        finally:
            for s in socks:
                s.close()
        counts: dict[int, int] = {}
        pid = 0
        for line in out.splitlines():
            if line.startswith("p"):
                pid = int(line[1:])
            # The server end of a connection is the one whose LOCAL address is the port.
            elif line.startswith("n") and re.search(rf":{port}->", line) and pid != os.getpid():
                counts[pid] = counts.get(pid, 0) + 1
        return counts


def _request(name: str, port: int) -> None:
    """One real request on ``name``'s protocol; raises when the listener does not answer it."""
    if name == "http":
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=10) as resp:
            assert resp.status == 200
    elif name == "pgwire":
        import psycopg

        with psycopg.connect(
            host="127.0.0.1",
            port=port,
            user="org_admin",
            password="x",
            dbname="provisa",
            connect_timeout=10,
            autocommit=True,
        ) as conn:
            assert conn.execute("SELECT 1").fetchone() == (1,)
    elif name == "flight":
        import pyarrow.flight as fl

        client = fl.connect(f"grpc://127.0.0.1:{port}")
        try:
            ticket = fl.Ticket(json.dumps({"query": FLIGHT_QUERY, "role": "org_admin"}).encode())
            table = client.do_get(ticket, fl.FlightCallOptions(timeout=30)).read_all()
            assert table.num_rows == 2, table
        finally:
            client.close()
    elif name == "airport":
        import pyarrow.flight as fl

        client = fl.connect(f"grpc://127.0.0.1:{port}")
        try:
            client.wait_for_available(timeout=10)
        finally:
            client.close()
    elif name == "grpc":
        import grpc

        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=10)
    elif name == "bolt":
        with socket.create_connection(("127.0.0.1", port), timeout=10) as s:
            # Bolt handshake: magic preamble + four proposed versions; the server answers with
            # the 4-byte version it picked.
            s.sendall(bytes.fromhex("6060b017") + bytes.fromhex("00000405000004040000000400000003"))
            assert len(s.recv(4)) == 4
    elif name == "mcp":
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/mcp",
            data=json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-03-26",
                        "capabilities": {},
                        "clientInfo": {"name": "wboot", "version": "0"},
                    },
                }
            ).encode(),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            assert resp.status == 200


def requests_served(name: str, port: int, n: int) -> tuple[int, str]:
    """Send ``n`` real requests, concurrently; return (how many were answered, first failure).

    Flight runs 4 at a time: the server caps concurrent Flight streams (REQ-1905) and refuses the
    rest, which is the cap working, not a listener failing."""
    from concurrent.futures import ThreadPoolExecutor

    def _one(_: int) -> str:
        try:
            _request(name, port)
        except Exception as exc:  # reported in the table, not swallowed
            return f"{type(exc).__name__}: {exc}"[:120]
        return ""

    with ThreadPoolExecutor(max_workers=4 if name == "flight" else n) as pool:
        failures = [f for f in pool.map(_one, range(n))]
    bad = [f for f in failures if f]
    return n - len(bad), (bad[0] if bad else "")


def _print_phase_table(boot: WorkerBoot) -> None:
    phases = boot.phases()
    names: list[str] = []
    for rows in phases.values():
        for name, _ in rows:
            if name not in names:
                names.append(name)
    pids = sorted(phases)
    print("\nphase".ljust(34) + "".join(f"{p:>9}" for p in pids) + f"{'sum':>9}")
    for name in names:
        vals = [dict(phases[p]).get(name, 0.0) for p in pids]
        print(name.ljust(33) + "".join(f"{v:9.2f}" for v in vals) + f"{sum(vals):9.2f}")


def _print_listener_table(boot: WorkerBoot, connections: int) -> None:
    workers = set(boot.worker_pids())
    print(f"\ntransport  port   listening-workers  accepting-workers ({connections} conns)  split")
    for name in _TRANSPORTS:
        port = boot.ports[name]
        listening = boot.listeners(port)
        if name == "http":
            # uvicorn's parent binds; every worker inherits and accepts on the same socket.
            listening = listening | workers if listening else listening
        accepted = boot.accepting_pids(port, connections)
        served, failure = requests_served(name, port, connections)
        print(
            f"{name:<10} {port:<6} {len(listening & workers) or len(listening):<18} "
            f"{len(accepted):<28} {sorted(accepted.values(), reverse=True)} "
            f"requests answered {served}/{connections} {failure}"
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--pg-host", default=os.environ.get("PG_HOST", "127.0.0.1"))
    ap.add_argument("--pg-port", type=int, required=True)
    ap.add_argument("--engine", default="duckdb")
    ap.add_argument("--redirect-endpoint", default=None)
    ap.add_argument("--connections", type=int, default=50)
    ap.add_argument("--second-boot", action="store_true", help="relaunch on the same control plane")
    ap.add_argument("--keep-log", action="store_true")
    args = ap.parse_args()

    boot = WorkerBoot(
        args.workers,
        pg_host=args.pg_host,
        pg_port=args.pg_port,
        engine=args.engine,
        redirect_endpoint=args.redirect_endpoint,
    )
    boot.create_database()
    try:
        boot.start()
        first = boot.wait_first_ready()
        every = boot.wait_all_ready()
        print(
            f"workers={args.workers} time-to-first-ready={first:.1f}s time-to-all-ready={every:.1f}s"
        )
        print(f"/health: {boot.health()}")
        _print_phase_table(boot)
        _print_listener_table(boot, args.connections)
        errors = [ln for ln in boot.log_text().splitlines() if "ERROR" in ln or "Traceback" in ln]
        print(f"\nerror lines in log: {len(errors)}")
        for ln in errors[:20]:
            print("  " + ln[:240])
        if args.second_boot:
            boot.stop()
            again = WorkerBoot(
                args.workers,
                pg_host=args.pg_host,
                pg_port=args.pg_port,
                engine=args.engine,
                redirect_endpoint=args.redirect_endpoint,
                database=boot.database,
                data_dir=boot.data_dir,
            )
            try:
                again.start()
                first = again.wait_first_ready()
                every = again.wait_all_ready()
                print(
                    f"\nsecond boot: time-to-first-ready={first:.1f}s time-to-all-ready={every:.1f}s"
                )
                print(f"/health: {again.health()}")
                _print_phase_table(again)
            finally:
                again.stop()
        if args.keep_log:
            kept = Path.home() / ".provisa" / "wboot-last.log"
            kept.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(boot.log_path, kept)
            print(f"log kept at {kept}")
    finally:
        boot.cleanup()


if __name__ == "__main__":
    main()
