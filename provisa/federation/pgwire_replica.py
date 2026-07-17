# Copyright (c) 2026 Kenneth Stott
# Canary: 0f7d220b-476b-47c5-ad8a-b649b3d4d25f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Connector pgwire replica strategy (REQ-954/955/956).

files / sharepoint / splunk sources are reached by NO federation engine's own connectors. To
materialize them, Provisa lands a replica through the connector's BUNDLED Calcite pgwire server:

1. RESOLVE + CACHE the bundle via the runtime_deps system — pgwire-file / -sharepoint / -splunk from
   the pinned kenstott/calcite release, fetched on demand (REQ-956).
2. CONFIGURE the server: write ``model/model.json`` into the bundle from the Source config — the
   source-specific creds/paths per connector (REQ-955).
3. LIFECYCLE: start the server on a UNIQUE ``--port`` / ``--calcite-child`` pair (default 5433 /
   127.0.0.1:5533), health-check it, and stop it on demand (REQ-955).
4. LAND: connect to it as a generic PostgreSQL endpoint and SELECT from the connector schema to land
   rows into the materialize store (REQ-954).

Every failure is LOUD: an unknown/creds-missing source, a port collision, or a missing bundle raises
— never a silent fallback, a partial config, or an empty snapshot.
"""

from __future__ import annotations

import json
import socket
import subprocess  # noqa: S404 - launches the pinned first-party pgwire bundle launcher
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from provisa.core.secrets import resolve_secrets
from provisa.runtime_deps import BundleResolver, BundleSpec, bundle_spec_for

# The pgwire-replica source types (mirror of strategy._CONNECTOR_PGWIRE_REPLICA). A type here has a
# bundled Calcite pgwire server and is landed through this module when no engine connector reaches it.
PGWIRE_REPLICA_TYPES = frozenset({"files", "sharepoint", "splunk"})

# Default ports (REQ-955): the pgwire endpoint (--port) and the Calcite child JVM (--calcite-child).
# Each source gets a UNIQUE pair allocated up from these bases so servers never collide.
PGWIRE_DEFAULT_PORT = 5433
CALCITE_CHILD_DEFAULT_HOST = "127.0.0.1"
CALCITE_CHILD_DEFAULT_PORT = 5533
_PORT_SCAN_LIMIT = 512  # how far up from a base to probe before giving up (fail loud)

# The Calcite schema factory per connector — the ``model.json`` ``factory`` for the bundle's adapter.
_SCHEMA_FACTORY: dict[str, str] = {
    "files": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "sharepoint": "org.apache.calcite.adapter.sharepoint.SharepointSchemaFactory",
    "splunk": "org.apache.calcite.adapter.splunk.SplunkSchemaFactory",
}


class MissingConnectorConfig(Exception):  # REQ-955
    """A pgwire-replica source is missing required creds/paths for its ``model.json`` operand. Raised
    on fail-loud config resolution; never a partial or defaulted operand."""


class PortAllocationError(Exception):  # REQ-955
    """No free port was found scanning up from a base — a port-isolation failure, raised loud."""


class ServerLifecycleError(Exception):  # REQ-955
    """An invalid pgwire server lifecycle transition (start-when-running, health-before-start)."""


def _source_type(source: Any) -> str:
    stype = source.type
    return stype.value if hasattr(stype, "value") else str(stype)


def _rs(value: str | None) -> str:
    """Resolve a secret reference (``${env:...}``) to its value; ``None`` → empty string."""
    return resolve_secrets(value) if value else ""


def schema_name(source: Any) -> str:
    """The Calcite schema the bundle exposes the source's tables under — the sql-normalized id."""
    return source.id.replace("-", "_")


# -- model.json operand builders (REQ-955) -------------------------------------


def _files_operand(source: Any) -> dict:
    """files → ``directory`` (local crawl) OR ``storageType`` + ``storageConfig`` (S3, AWS env vars).
    executionEngine defaults to PARQUET. Neither a directory nor a storageType is a config error."""
    mapping = {k: _rs(v) if isinstance(v, str) else v for k, v in (source.mapping or {}).items()}
    operand: dict = {"executionEngine": mapping.get("execution_engine", "PARQUET")}
    storage_type = mapping.get("storage_type")
    if storage_type:
        operand["storageType"] = storage_type
        operand["storageConfig"] = mapping.get("storage_config", {})
        return operand
    directory = _rs(source.path)
    if not directory:
        raise MissingConnectorConfig(
            f"files source {source.id!r}: requires 'path' (directory) or mapping.storage_type (S3)"
        )
    operand["directory"] = directory
    return operand


def _sharepoint_operand(source: Any) -> dict:
    """sharepoint → siteUrl + tenantId + clientId + (clientSecret OR cert OR device-code). Missing
    siteUrl, tenant/client, or every auth method is a config error (REQ-955)."""
    mapping = {k: _rs(v) if isinstance(v, str) else v for k, v in (source.mapping or {}).items()}
    site_url = _rs(source.base_url) or _rs(source.host)
    if not site_url:
        raise MissingConnectorConfig(f"sharepoint source {source.id!r}: requires siteUrl")
    tenant_id = _rs(source.database)
    client_id = _rs(source.username)
    if not tenant_id or not client_id:
        raise MissingConnectorConfig(
            f"sharepoint source {source.id!r}: requires tenantId (database) and clientId (username)"
        )
    operand: dict = {"siteUrl": site_url, "tenantId": tenant_id, "clientId": client_id}
    client_secret = _rs(source.password)
    cert_path = mapping.get("certificate_path")
    if client_secret:
        operand["clientSecret"] = client_secret
    elif cert_path:
        operand["certificatePath"] = cert_path
        if mapping.get("certificate_password"):
            operand["certificatePassword"] = mapping["certificate_password"]
    elif mapping.get("use_device_code"):
        operand["authType"] = "DEVICE_CODE"
    else:
        raise MissingConnectorConfig(
            f"sharepoint source {source.id!r}: requires clientSecret, a certificate, or device-code"
        )
    return operand


def _splunk_operand(source: Any) -> dict:
    """splunk → url (or host/port/protocol) + (token OR username/password) + optional app. A missing
    url/host, or no token and no username/password pair, is a config error (REQ-955)."""
    mapping = {k: _rs(v) if isinstance(v, str) else v for k, v in (source.mapping or {}).items()}
    host = _rs(source.host)
    url = _rs(source.base_url)
    if not url and host:
        protocol = mapping.get("protocol", "https")
        port = source.port or 8089
        url = f"{protocol}://{host}:{port}"
    if not url:
        raise MissingConnectorConfig(f"splunk source {source.id!r}: requires url or host")
    operand: dict = {"url": url}
    token = _rs(source.password) if mapping.get("use_token", True) else ""
    if token:
        operand["token"] = token
    else:
        username = _rs(source.username)
        password = _rs(source.password)
        if not username or not password:
            raise MissingConnectorConfig(
                f"splunk source {source.id!r}: requires token or username/password"
            )
        operand["username"] = username
        operand["password"] = password
    if source.database:
        operand["app"] = source.database
    return operand


_OPERAND_BUILDERS: dict[str, Callable[[Any], dict]] = {
    "files": _files_operand,
    "sharepoint": _sharepoint_operand,
    "splunk": _splunk_operand,
}


def build_model_json(source: Any) -> dict:
    """The Calcite ``model.json`` for a pgwire-replica source (REQ-955): one custom schema whose
    operand carries the source-specific creds/paths. A non-replica source type is a caller error."""
    stype = _source_type(source)
    builder = _OPERAND_BUILDERS.get(stype)
    if builder is None:
        raise MissingConnectorConfig(f"source type {stype!r} is not a pgwire-replica connector")
    schema = schema_name(source)
    return {
        "version": "1.0",
        "defaultSchema": schema,
        "schemas": [
            {
                "name": schema,
                "type": "custom",
                "factory": _SCHEMA_FACTORY[stype],
                "operand": builder(source),
            }
        ],
    }


# -- port allocation (REQ-955) -------------------------------------------------


@dataclass(frozen=True)
class PortPair:
    pgwire_port: int
    calcite_child_host: str
    calcite_child_port: int


def _port_is_free(port: int) -> bool:
    """Whether ``port`` can be bound on loopback right now — the real free-port probe."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
        return True


class PortAllocator:  # REQ-955
    """Hands each source a UNIQUE (--port, --calcite-child) pair so servers run concurrently without
    collision. Deterministic + idempotent per source id; scans up from the bases past any in-use or
    already-assigned port. ``is_free`` is injectable (tests force collisions without real sockets)."""

    def __init__(
        self,
        *,
        pgwire_base: int = PGWIRE_DEFAULT_PORT,
        calcite_base: int = CALCITE_CHILD_DEFAULT_PORT,
        calcite_host: str = CALCITE_CHILD_DEFAULT_HOST,
        is_free: Callable[[int], bool] | None = None,
    ) -> None:
        self._pgwire_base = pgwire_base
        self._calcite_base = calcite_base
        self._calcite_host = calcite_host
        self._is_free = is_free if is_free is not None else _port_is_free
        self._assigned: dict[str, PortPair] = {}
        self._used: set[int] = set()

    def allocate(self, source_id: str) -> PortPair:
        """The stable port pair for ``source_id`` — allocated once, returned unchanged thereafter."""
        existing = self._assigned.get(source_id)
        if existing is not None:
            return existing
        pgwire_port = self._next(self._pgwire_base)
        calcite_port = self._next(self._calcite_base)
        pair = PortPair(pgwire_port, self._calcite_host, calcite_port)
        self._assigned[source_id] = pair
        return pair

    def _next(self, base: int) -> int:
        port = base
        limit = base + _PORT_SCAN_LIMIT
        while port <= limit:
            if port not in self._used and self._is_free(port):
                self._used.add(port)
                return port
            port += 1
        raise PortAllocationError(
            f"no free port found scanning {base}..{limit} for pgwire replica server"
        )


# -- server lifecycle (REQ-955) ------------------------------------------------


def _spawn_process(command: list[str], cwd: Path) -> Any:
    """Launch the pgwire bundle launcher as a child process (the real spawn)."""
    return subprocess.Popen(command, cwd=str(cwd))  # noqa: S603 - args are code-built, not user input


def _tcp_health(host: str, port: int) -> bool:
    """Whether the pgwire endpoint accepts a TCP connection (the real health probe)."""
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


class PgwireServer:  # REQ-955
    """Lifecycle for one source's bundled Calcite pgwire server: write model.json, start, health,
    stop. The launcher (``bin/pgwire-<connector>``) takes only ``--port`` and ``--calcite-child``
    (REQ-955) and reads ``model/model.json`` from the bundle. ``spawn`` / ``health_check`` are
    injectable so lifecycle is testable without a real JVM/subprocess."""

    def __init__(
        self,
        *,
        bundle_dir: str | Path,
        spec: BundleSpec,
        model: dict,
        ports: PortPair,
        spawn: Callable[[list[str], Path], Any] | None = None,
        health_check: Callable[[str, int], bool] | None = None,
    ) -> None:
        self._bundle_dir = Path(bundle_dir)
        self._spec = spec
        self._model = model
        self._ports = ports
        self._spawn = spawn if spawn is not None else _spawn_process
        self._health = health_check if health_check is not None else _tcp_health
        self._proc: Any = None

    @property
    def model_path(self) -> Path:
        return self._bundle_dir / "model" / "model.json"

    @property
    def ports(self) -> PortPair:
        return self._ports

    def command(self) -> list[str]:
        """The launcher invocation — only ``--port`` and ``--calcite-child`` (REQ-955)."""
        launcher = self._bundle_dir / "bin" / self._spec.artifact_name
        child = f"{self._ports.calcite_child_host}:{self._ports.calcite_child_port}"
        return [str(launcher), "--port", str(self._ports.pgwire_port), "--calcite-child", child]

    def write_model(self) -> Path:
        """Write ``model/model.json`` into the bundle from the source config (REQ-955)."""
        path = self.model_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self._model, indent=2))
        return path

    def start(self) -> None:
        """Write the model and spawn the server. Starting a running server is a lifecycle error."""
        if self._proc is not None:
            raise ServerLifecycleError(
                f"pgwire server on port {self._ports.pgwire_port} already running"
            )
        self.write_model()
        self._proc = self._spawn(self.command(), self._bundle_dir)

    def health(self) -> bool:
        """Whether the started server's pgwire endpoint is accepting connections."""
        if self._proc is None:
            raise ServerLifecycleError("pgwire server health checked before start")
        return self._health(self._ports.calcite_child_host, self._ports.pgwire_port)

    def stop(self) -> None:
        """Terminate the server (idempotent — a no-op if not running)."""
        if self._proc is None:
            return
        self._proc.terminate()
        self._proc = None


# -- land via SELECT (REQ-954) -------------------------------------------------


async def _pg_connect(host: str, port: int) -> Any:
    """Open a generic PostgreSQL connection to the pgwire endpoint (the real connect)."""
    import asyncpg

    return await asyncpg.connect(host=host, port=port, user="provisa", database="provisa")


async def land_via_select(
    ports: PortPair,
    schema: str,
    table: str,
    *,
    connect: Callable[[str, int], Any] | None = None,
) -> list[dict]:
    """Connect to the pgwire endpoint as generic PostgreSQL and SELECT the connector table's rows
    (REQ-954). Returns the rows as dicts keyed by column name; the caller lands them into the store."""
    do_connect = connect if connect is not None else _pg_connect
    conn = await do_connect(ports.calcite_child_host, ports.pgwire_port)
    try:
        rows = await conn.fetch(f'SELECT * FROM "{schema}"."{table}"')
        return [dict(row) for row in rows]
    finally:
        await conn.close()


# -- orchestration + engine integration ----------------------------------------


def needs_pgwire_replica(source: Any, engine: Any) -> bool:
    """Whether ``source`` must be landed through the pgwire replica on ``engine`` (REQ-954): a
    pgwire-replica type the engine reaches through NONE of its own connectors. When the engine has a
    connector for the type (e.g. Trino's file/sharepoint/splunk), that native path is used instead."""
    if _source_type(source) not in PGWIRE_REPLICA_TYPES:
        return False
    connectors = getattr(engine, "connectors", None)
    if connectors is None:
        return True  # a connector-less engine (native store) always needs the pgwire bridge
    return connectors.get(_source_type(source)) is None


class ConnectorReplica:  # REQ-954/955/956
    """Per-source pgwire replica: resolve+cache the bundle (956), allocate ports + start/health/stop
    the server (955), and land rows via SELECT (954). Starts the server lazily on first ``load`` and
    reuses it; ``close`` stops it. Every seam (resolver, allocator, spawn, health, connect) is
    injectable so the whole flow is unit-testable without a network, JVM, or real Postgres."""

    def __init__(
        self,
        source: Any,
        *,
        resolver: BundleResolver | None = None,
        allocator: PortAllocator | None = None,
        spawn: Callable[[list[str], Path], Any] | None = None,
        health_check: Callable[[str, int], bool] | None = None,
        connect: Callable[[str, int], Any] | None = None,
        version: str | None = None,
    ) -> None:
        self._source = source
        self._resolver = resolver if resolver is not None else BundleResolver()
        self._allocator = allocator if allocator is not None else PortAllocator()
        self._spawn = spawn
        self._health = health_check
        self._connect = connect
        self._spec: BundleSpec = (
            bundle_spec_for(_source_type(source), version=version)
            if version is not None
            else bundle_spec_for(_source_type(source))
        )
        self._server: PgwireServer | None = None

    @property
    def spec(self) -> BundleSpec:
        return self._spec

    def _ensure_server(self) -> PgwireServer:
        if self._server is not None:
            return self._server
        bundle_dir = self._resolver.resolve(self._spec)  # REQ-956 (resolve + cache)
        ports = self._allocator.allocate(self._source.id)  # REQ-955 (unique ports)
        server = PgwireServer(
            bundle_dir=bundle_dir,
            spec=self._spec,
            model=build_model_json(self._source),  # REQ-955 (config)
            ports=ports,
            spawn=self._spawn,
            health_check=self._health,
        )
        server.start()  # REQ-955 (lifecycle)
        self._server = server
        return server

    async def load(self, table: Any) -> list[dict]:
        """Land the connector table's current rows: start the server if needed, then SELECT (REQ-954).
        ``table`` may be a registered Table (``.table_name``) or a bare table-name string."""
        server = self._ensure_server()
        if not server.health():
            raise ServerLifecycleError(
                f"pgwire server for {self._source.id!r} is not healthy on port "
                f"{server.ports.pgwire_port}"
            )
        table_name = getattr(table, "table_name", table)
        return await land_via_select(
            server.ports, schema_name(self._source), table_name, connect=self._connect
        )

    def close(self) -> None:
        """Stop the server (idempotent)."""
        if self._server is not None:
            self._server.stop()
            self._server = None


def make_pgwire_loader(
    *,
    resolver: BundleResolver | None = None,
    allocator: PortAllocator | None = None,
    spawn: Callable[[list[str], Path], Any] | None = None,
    health_check: Callable[[str, int], bool] | None = None,
    connect: Callable[[str, int], Any] | None = None,
) -> Callable[[Any, Any], Any]:
    """Build a TYPE-level ``adapter_loaders`` row-fetch for pgwire-replica sources (REQ-954), fitting
    the ``SourceRowLoader`` adapter seam ``async (source, table) -> list[dict]``. One
    ``ConnectorReplica`` (one server) is created + reused per source id, so several sources of the same
    type each get their own server on its own allocated port (the shared allocator keeps them unique)."""
    alloc = allocator if allocator is not None else PortAllocator()
    replicas: dict[str, ConnectorReplica] = {}

    async def _load(source: Any, table: Any) -> list[dict]:
        replica = replicas.get(source.id)
        if replica is None:
            replica = ConnectorReplica(
                source,
                resolver=resolver,
                allocator=alloc,
                spawn=spawn,
                health_check=health_check,
                connect=connect,
            )
            replicas[source.id] = replica
        return await replica.load(table)

    return _load
