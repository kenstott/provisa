# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1

"""A dedicated, isolated Provisa server subprocess for integration tests.

Some integration tests need a specific federation engine (e.g. Trino for the Arrow
Flight transport) or their own control-plane org — they must NOT share the global
in-process ``state`` singleton, ``org_default``, or a live demo server on :8000.
This spawns a self-contained ``uvicorn main:app`` subprocess with its own ORG_ID,
config-replace, auth disabled, and free ports, then tears it down (process killed,
org schema dropped, temp config removed) so the shared PG is left exactly as found.
"""

from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parents[2]


def free_port() -> int:
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def drop_org_schema(org_id: str) -> None:
    """Drop org_<id> (+ its mv_cache) so the shared PG is left as found."""
    import asyncpg

    conn = await asyncpg.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
        database=os.environ.get("PG_DATABASE", "provisa"),
    )
    try:
        # Drop every schema the server derives from the org (org_<id>, plus _mv_cache /
        # _gql_cache and any other suffix) so the shared PG is left exactly as found.
        rows = await conn.fetch(
            "SELECT nspname FROM pg_namespace WHERE nspname = $1 OR nspname LIKE $2",
            f"org_{org_id}",
            f"org_{org_id}_%",
        )
        for row in rows:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{row["nspname"]}" CASCADE')
    finally:
        await conn.close()


class IsolatedServer:
    """Manages one isolated Provisa server subprocess. ``start()`` blocks until the
    HTTP /health (and, if requested, the Arrow Flight port) is reachable."""

    def __init__(
        self,
        org_id: str,
        *,
        engine: str = "trino",
        await_flight: bool = False,
        await_grpc: bool = False,
        enable_bolt: bool = False,
        enable_pgwire: bool = False,
        config: str = "config/provisa.yaml",
        control_plane: str = "postgres",
        materialize_store_url: str | None = None,
    ) -> None:
        self.org_id = org_id
        self._engine = engine
        self._await_flight = await_flight
        self._await_grpc = await_grpc
        self._enable_bolt = enable_bolt
        self._enable_pgwire = enable_pgwire
        self._config = config
        self._control_plane = control_plane
        self._materialize_store_url = materialize_store_url
        self.http_port = free_port()
        self.flight_port = free_port()
        self.bolt_port = free_port() if enable_bolt else 0
        self.pgwire_port = free_port() if enable_pgwire else 0
        self.grpc_port = free_port()
        self._proc: subprocess.Popen | None = None
        self._cfg_path: str | None = None
        self._tmpdir: tempfile.TemporaryDirectory | None = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.http_port}"

    def _write_config(self) -> str:
        # A temp copy of the base config with auth disabled (provider: none → unsecured dev
        # mode) so tests reach admin/Flight/Bolt endpoints without credentials.
        base = Path(self._config)
        if not base.is_absolute():
            base = _REPO_ROOT / base
        with open(base) as f:
            cfg = yaml.safe_load(f)
        cfg.setdefault("auth", {})["provider"] = "none"
        if self._materialize_store_url is not None:
            cfg["materialize_store_url"] = self._materialize_store_url
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
        yaml.safe_dump(cfg, tmp)
        tmp.close()
        return tmp.name

    def _control_plane_env(self) -> dict:
        """Env overriding the control-plane store. ``sqlite`` points the tenant + platform planes
        at throwaway SQLite files (file-per-org backend) and uses embedded redis — no Postgres."""
        if self._control_plane != "sqlite":
            return {}
        self._tmpdir = tempfile.TemporaryDirectory()
        d = Path(self._tmpdir.name)
        return {
            "TENANT_DATABASE_URL": f"sqlite+aiosqlite:///{d / 'tenant.db'}",
            "PLATFORM_DATABASE_URL": f"sqlite+aiosqlite:///{d / 'platform.db'}",
            "PROVISA_REDIS_EMBEDDED": "1",
        }

    def start(self, *, timeout: float = 120.0) -> None:
        self._cfg_path = self._write_config()
        env = {
            **os.environ,
            **self._control_plane_env(),
            "ORG_ID": self.org_id,
            "PROVISA_ENGINE": self._engine,
            "PROVISA_CONFIG": self._cfg_path,
            "PROVISA_CONFIG_REPLACE": "true",
            "PROVISA_IDP": "",
            "PG_PASSWORD": os.environ.get("PG_PASSWORD", "provisa"),
            "FLIGHT_PORT": str(self.flight_port),
            "GRPC_PORT": str(self.grpc_port),
            "PROVISA_PGWIRE_PORT": str(self.pgwire_port),
            "PROVISA_BOLT_PORT": str(self.bolt_port),
            "OTEL_SDK_DISABLED": "true",
        }
        self._stderr_file = tempfile.NamedTemporaryFile(
            prefix="isolated-server-", suffix=".stderr", delete=False
        )
        self._proc = subprocess.Popen(
            [
                str(_REPO_ROOT / ".venv" / "bin" / "uvicorn"),
                "main:app",
                "--host",
                "127.0.0.1",
                f"--port={self.http_port}",
            ],
            cwd=str(_REPO_ROOT),
            env=env,
            stdout=subprocess.DEVNULL,
            # Capture stderr to a file (not a PIPE — a healthy long-running server
            # would fill the pipe buffer and deadlock) so an early exit surfaces WHY.
            stderr=self._stderr_file,
        )
        deadline = time.monotonic() + timeout
        while True:
            if self._proc.poll() is not None:
                self._stderr_file.flush()
                _err = Path(self._stderr_file.name).read_text(errors="replace")
                raise RuntimeError(
                    f"isolated server exited early (code {self._proc.returncode}):\n{_err[-3000:]}"
                )
            try:
                with urllib.request.urlopen(f"{self.base_url}/health", timeout=3):
                    break
            except Exception:
                if time.monotonic() >= deadline:
                    self.stop_process()
                    raise RuntimeError("isolated server did not become healthy in time")
                time.sleep(1)
        if self._await_flight:
            self._await_port(self.flight_port, deadline, "Arrow Flight")
        if self._enable_bolt:
            self._await_port(self.bolt_port, deadline, "Bolt")
        if self._enable_pgwire:
            self._await_port(self.pgwire_port, deadline, "pgwire")
        if self._await_grpc:
            self._await_port(self.grpc_port, deadline, "gRPC")

    def _await_port(self, port: int, deadline: float, label: str) -> None:
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return
            except OSError:
                if self._proc is not None and self._proc.poll() is not None:
                    raise RuntimeError(f"isolated server exited before {label} bind")
                time.sleep(1)
        self.stop_process()
        raise RuntimeError(f"isolated server {label} port never bound")

    def stop_process(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=10)
            except Exception:
                self._proc.kill()
            self._proc = None
        if self._cfg_path is not None:
            Path(self._cfg_path).unlink(missing_ok=True)
            self._cfg_path = None
        if self._tmpdir is not None:
            self._tmpdir.cleanup()
            self._tmpdir = None
        _sf = getattr(self, "_stderr_file", None)
        if _sf is not None:
            _sf.close()
            Path(_sf.name).unlink(missing_ok=True)
            self._stderr_file = None
