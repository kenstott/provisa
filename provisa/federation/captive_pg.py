# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2f61a4-0b7e-4c39-95e1-a3c4d7f0b218
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1874: the captive Postgres+wrappers lifecycle.

A source type reached through Supabase ``wrappers`` gets its OWN small Postgres+wrappers
container (never shared across two sources: ``wrappers`` is one ``.so`` per backend process, so a
crash tied to one source's connection would take the other down with it). The container is the
pgwire-replica shape of REQ-954/955/956 with a real Postgres in place of the Calcite bundle:
start it on a unique port, health-check it, stop it on demand. The engine then reaches it as an
ordinary ``postgresql`` endpoint through the existing connector.

``wrappers`` has no macOS or Windows build, so the container is always Linux and needs a local
container runtime. Its absence is refused by name (:class:`ContainerRuntimeMissing`), never
assumed. Per REQ-1874's UX constraint every message a user can see names the source and the
runtime only, never ``wrappers``, FDWs or the captive instance.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import secrets
import subprocess  # noqa: S404 - drives the operator's docker CLI with fixed argument lists
import time
from typing import Awaitable, Callable

from provisa.federation.pgwire_replica import ServerNotServing, bind_host

#: The port Postgres listens on inside the container; published on the source's own host port.
CONTAINER_PG_PORT = 5432
#: The official postgres image's superuser and database (its entrypoint's contract).
CAPTIVE_USER = "postgres"
CAPTIVE_DATABASE = "postgres"
CAPTIVE_READY_SECONDS = 90  # first start runs initdb inside the container
_LOG_TAIL_LINES = 40
_NAME_PREFIX = "provisa-pg-"

Docker = Callable[..., str]
Probe = Callable[[str, int, str, str, str], Awaitable[bool]]


class DockerFailed(Exception):
    """The docker CLI ran and refused: its stderr."""


class ContainerRuntimeMissing(ServerNotServing):
    """This host has no usable container runtime, which the source requires (REQ-1874)."""

    def __init__(self, reason: str, source_id: str | None = None) -> None:
        self.reason = reason
        who = f"source {source_id!r} " if source_id else "this source "
        super().__init__(
            f"{who}requires a local container runtime (Docker or a compatible runtime), "
            f"and none is usable on this host: {reason}"
        )


class CaptiveContainerError(ServerNotServing):
    """The source's container could not be started, did not become ready, or was misused."""


def _run_docker(args: tuple[str, ...], env: dict[str, str] | None = None) -> str:
    """Run ``docker <args>``; a non-zero exit is :class:`DockerFailed` with its stderr. ``env``
    adds to this process's environment (a ``-e NAME`` flag reads its value from there)."""
    full_env = None if env is None else {**os.environ, **env}
    proc = subprocess.run(  # noqa: S603 - fixed docker argv, no shell
        ["docker", *args], capture_output=True, text=True, env=full_env, check=False
    )
    if proc.returncode != 0:
        raise DockerFailed(proc.stderr.strip() or f"docker {args[0]} exited {proc.returncode}")
    return proc.stdout


def require_container_runtime(
    *, docker: Docker = _run_docker, source_id: str | None = None
) -> None:
    """Refuse, by name, a host with no container runtime: no ``docker`` CLI, or one whose daemon
    does not answer."""
    try:
        docker(("version", "--format", "{{.Server.Version}}"))
    except FileNotFoundError as exc:
        raise ContainerRuntimeMissing("the docker command is not installed", source_id) from exc
    except DockerFailed as exc:
        raise ContainerRuntimeMissing(str(exc), source_id) from exc


async def _pg_probe(host: str, port: int, user: str, password: str, database: str) -> bool:
    """Whether Postgres answers a query there. A query, not a TCP connect: the runtime's port
    proxy accepts connections before Postgres listens, and initdb's temporary server refuses
    them."""
    import asyncpg

    try:
        conn = await asyncpg.connect(
            host=host, port=port, user=user, password=password, database=database, timeout=2
        )
    except (OSError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError):
        return False
    try:
        return await conn.fetchval("SELECT 1") == 1
    finally:
        await conn.close()


def container_name(org_id: str, source_id: str) -> str:
    """The container one source runs in on this instance. Keyed by the instance's data
    directory, this node, the org and the source, so two sources, the same source id in two
    orgs, and the same source in two instances on one machine never share one."""
    from provisa.core.org_files import data_dir, node_id

    key = "\0".join((str(data_dir()), node_id(), org_id, source_id))
    return _NAME_PREFIX + hashlib.sha256(key.encode()).hexdigest()[:20]


class CaptivePostgres:  # REQ-1874
    """One wrapped source's dedicated Postgres+wrappers container: start, health, stop.

    ``docker`` (the CLI runner) and ``probe`` (the readiness query) are injectable so the
    lifecycle is testable without a runtime."""

    def __init__(
        self,
        *,
        org_id: str,
        source_id: str,
        image: str,
        port: int,
        docker: Docker = _run_docker,
        probe: Probe = _pg_probe,
    ) -> None:
        if not image:
            raise ValueError("a captive Postgres needs its image")
        self._org_id = org_id
        self._source_id = source_id
        self._image = image
        self._port = port
        self._docker = docker
        self._probe = probe
        self._name = container_name(org_id, source_id)
        self._password = secrets.token_urlsafe(24)
        self._started = False

    @property
    def container_name(self) -> str:
        return self._name

    @property
    def password(self) -> str:
        return self._password

    @property
    def host(self) -> str:
        """Where this host reaches the published port: the deployment's bind host, else
        loopback (the pgwire-replica rule, REQ-955)."""
        return bind_host() or "127.0.0.1"

    @property
    def port(self) -> int:
        return self._port

    def _exists(self) -> bool:
        try:
            self._docker(("inspect", "--format", "{{.State.Running}}", self._name))
        except DockerFailed:
            return False
        return True

    def _running(self) -> bool:
        try:
            out = self._docker(("inspect", "--format", "{{.State.Running}}", self._name))
        except DockerFailed:
            return False
        return out.strip() == "true"

    def start(self) -> None:
        """Start the source's container on its own port. The runtime is checked first and its
        absence refused by name. A container already under this source's name on this instance
        was left by a process that died without stopping it (the name is this instance's own,
        see :func:`container_name`), and is replaced: a start always begins from a fresh one."""
        if self._started:
            raise CaptiveContainerError(f"source {self._source_id!r} is already started")
        require_container_runtime(docker=self._docker, source_id=self._source_id)
        if self._exists():
            self._docker(("rm", "-f", self._name))
        args = (
            "run",
            "-d",
            "--name",
            self._name,
            "--label",
            f"provisa.source={self._org_id}/{self._source_id}",
            "-e",
            "POSTGRES_PASSWORD",  # value from the CLI's environment, never on the command line
            "-p",
            f"{self.host}:{self._port}:{CONTAINER_PG_PORT}",
            self._image,
        )
        try:
            self._docker(args, {"POSTGRES_PASSWORD": self._password})
        except DockerFailed as exc:
            raise CaptiveContainerError(
                f"source {self._source_id!r} could not be started: {exc}"
            ) from exc
        self._started = True

    async def health(self) -> bool:
        """Whether the started container's Postgres answers a query on the published port."""
        if not self._started:
            raise CaptiveContainerError(f"source {self._source_id!r} health checked before start")
        return await self._probe(
            self.host, self._port, CAPTIVE_USER, self._password, CAPTIVE_DATABASE
        )

    def log_tail(self) -> str:
        try:
            return self._docker(("logs", "--tail", str(_LOG_TAIL_LINES), self._name))
        except DockerFailed as exc:
            return f"(its log could not be read: {exc})"

    async def wait_ready(
        self, timeout: float = CAPTIVE_READY_SECONDS, interval: float = 0.5
    ) -> None:
        """Return once healthy. A container that exits, or is not ready within ``timeout``, is
        a :class:`CaptiveContainerError` carrying the tail of its log."""
        deadline = time.monotonic() + timeout
        while True:
            if await self.health():
                return
            if not self._running():
                raise CaptiveContainerError(
                    f"source {self._source_id!r} stopped while starting:\n{self.log_tail()}"
                )
            if time.monotonic() >= deadline:
                raise CaptiveContainerError(
                    f"source {self._source_id!r} not ready after {timeout}s:\n{self.log_tail()}"
                )
            await asyncio.sleep(interval)

    def stop(self) -> None:
        """Remove the container (idempotent: a no-op when not started)."""
        if not self._started:
            return
        self._docker(("rm", "-f", self._name))
        self._started = False
