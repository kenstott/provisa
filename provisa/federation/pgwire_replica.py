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
import os
import signal
import socket
import threading
import subprocess  # noqa: S404 - launches the pinned first-party pgwire bundle launcher
import time
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import Any, Callable

from provisa.core.secrets import resolve_secrets
from provisa.federation.cloudops import cloudops_settings
from provisa.federation.connector_config import MissingConnectorConfig
from provisa.federation.salesforce import salesforce_login_url
from provisa.runtime_deps import BundleResolver, BundleSpec, bundle_spec_for

# The pgwire-replica source types (mirror of strategy._CONNECTOR_PGWIRE_REPLICA). A type here has a
# bundled Calcite pgwire server and is landed through this module when no engine connector reaches it.
PGWIRE_REPLICA_TYPES = frozenset(
    {"files", "sharepoint", "splunk", "salesforce", "cloudops", "govdata"}
)
# Types whose bundle is a complete install — its own model of SEVERAL schemas (AskAmerica: sec,
# econ, ...) and the catalog prebuilt for exactly that model beside it. Such a server is the
# installed bundle started by its own launcher AS IT IS: nothing of it is copied, linked or
# rewritten, since the adapter finds its prebuilt catalog by the model file's bytes and builds
# it afresh (minutes) for any other model. One source of such a type runs from the one install.
# Every other type's model is written from the source into a state directory of its own and has
# ONE schema, named after the source id.
BUNDLE_MODEL_TYPES = frozenset({"govdata"})

# Default ports (REQ-955): the pgwire endpoint (--port) and the Calcite child JVM (--calcite-child).
# Each source gets a UNIQUE pair allocated up from these bases so servers never collide.
PGWIRE_DEFAULT_PORT = 5433
CALCITE_CHILD_DEFAULT_HOST = "127.0.0.1"
CALCITE_CHILD_DEFAULT_PORT = 5533
_PORT_SCAN_LIMIT = 512  # how far up from a base to probe before giving up (fail loud)
SERVER_STOP_SECONDS = 30  # SIGTERM grace before the group is killed
SERVER_READY_SECONDS = (
    90  # a bundled JVM + Calcite schema boot; the probe measured ~10s on a laptop
)
# Upper bound on one replica SELECT against the pgwire server. Without it a wedged Calcite server
# blocks the caller forever (asyncpg's command timeout defaults to none). A request's own budget
# (REQ-1882 request deadline), when one is bound, tightens it.
LAND_FETCH_SECONDS = 600


def _fetch_timeout() -> float:
    from provisa.core import request_deadline

    budget = request_deadline.remaining()
    return LAND_FETCH_SECONDS if budget is None else min(LAND_FETCH_SECONDS, budget)


# The Calcite schema factory per connector — the ``model.json`` ``factory`` for the bundle's adapter.
_SCHEMA_FACTORY: dict[str, str] = {
    "files": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "sharepoint": "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory",
    "splunk": "org.apache.calcite.adapter.splunk.SplunkSchemaFactory",
    "salesforce": "org.apache.calcite.adapter.salesforce.SalesforceSchemaFactory",
    "cloudops": "org.apache.calcite.adapter.ops.CloudOpsSchemaFactory",
}


class PortAllocationError(Exception):  # REQ-955
    """No free port was found scanning up from a base — a port-isolation failure, raised loud."""


class ServerNotServing(Exception):  # REQ-955
    """A source's pgwire server is not accepting connections: it is still starting, did not
    start in time, or exited. The source's tables cannot be read through it now; nothing else
    is affected."""


class ServerLifecycleError(ServerNotServing):  # REQ-955
    """An invalid pgwire server lifecycle transition (start-when-running, health-before-start)."""


class ServerExited(ServerNotServing):  # REQ-955
    """The pgwire server's process ended before it accepted connections: its start failed. Not
    "still starting" — nothing is booting any more — so a discovery call reports it, with the exit
    code and the end of the server's own log, instead of asking to be polled again."""

    def __init__(self, source_id: str, code: int, log_tail: str) -> None:
        super().__init__(
            f"the connector server for {source_id!r} exited with code {code} before it accepted "
            f"connections; its log ends:\n{log_tail}"
        )
        self.source_id = source_id
        self.code = code


#: How much of a failed server's log its error carries.
_LOG_TAIL_LINES = 20


class InstalledBundleInUse(ServerNotServing):
    """A second source of a type that runs from its one installed bundle (AskAmerica). One
    source of the type is served per instance; the other is refused naming the one that holds
    the bundle."""

    def __init__(self, source_id: str, source_type: str, held_by: str) -> None:
        super().__init__(
            f"{source_type} source {source_id!r} cannot be started: source {held_by!r} already "
            f"runs the one installed {source_type} server of this instance"
        )
        self.source_id = source_id
        self.held_by = held_by


class ServerCatalogFailed(ServerNotServing):
    """The server accepted connections but could not prepare its catalog — the first catalog
    query, which an engine's attach depends on, failed. Not asked again until the server is
    restarted (its source saved again)."""

    def __init__(self, source_id: str, error: BaseException) -> None:
        super().__init__(
            f"the connector server for {source_id!r} could not prepare its catalog: "
            f"{type(error).__name__}: {error}"
        )
        self.source_id = source_id


class SourceStillStartingError(ServerNotServing):  # REQ-1824
    """A files/sharepoint/splunk source's bundled Calcite server hasn't finished starting yet —
    raised by a DISCOVERY call (schema/table/column introspection) that chose not to wait the full
    SERVER_READY_SECONDS a real query needs. Not a failure: the message is machine-parseable (a
    fixed ``STARTING:`` prefix) so the frontend can recognize it and poll again shortly instead of
    surfacing it as a hard error."""

    def __init__(self, source_id: str, *, preparing_catalog: bool = False) -> None:
        # Two waits, told apart for whoever is waiting: the server has not opened its port
        # yet, or it is listening and preparing its catalog (``ConnectorReplica.await_catalog``).
        doing = (
            "is listening and answering its first catalog query"
            if preparing_catalog
            else "is still starting up"
        )
        super().__init__(f"STARTING: {source_id!r}'s connector {doing} — try again shortly.")
        self.source_id = source_id
        self.preparing_catalog = preparing_catalog


def _source_type(source: Any) -> str:
    stype = source.type
    return stype.value if hasattr(stype, "value") else str(stype)


def _rs(value: str | None) -> str:
    """Resolve a secret reference (``${env:...}``) to its value; ``None`` → empty string."""
    return resolve_secrets(value) if value else ""


def schema_name(source: Any) -> str:
    """The Calcite schema the bundle exposes the source's tables under — the sql-normalized id."""
    return source.id.replace("-", "_")


def serves_table_schemas(source: Any) -> bool:
    """Whether the source's server exposes each table under the table's OWN schema (a bundle
    model of several schemas, ``BUNDLE_MODEL_TYPES``) rather than under :func:`schema_name`."""
    return _source_type(source) in BUNDLE_MODEL_TYPES


def remote_schema(source: Any, table_schema: str | None) -> str:
    """The schema of the source's server a table is read from: the table's own schema where the
    server serves several (:func:`serves_table_schemas`), else the one :func:`schema_name`. A
    table of a several-schema source that names no schema is a caller error, never a guess."""
    if not serves_table_schemas(source):
        return schema_name(source)
    if not table_schema:
        raise MissingConnectorConfig(
            f"{_source_type(source)} source {source.id!r}: a table of it is read from its own "
            "schema, and none was given"
        )
    return table_schema


# -- model.json operand builders (REQ-955) -------------------------------------


def _files_operand(source: Any) -> dict:
    """files → ``directory`` (local crawl) OR ``storageType`` + ``storageConfig`` (S3, AWS env vars).
    executionEngine defaults to DUCKDB — verified live: PARQUET (the prior default) silently
    discovered zero tables for a plain csv directory (demo/files/northwind), and TrinoFilesConnector
    already avoids PARQUET for the same reason its own comment gives (Hadoop's
    UserGroupInformation.getCurrentUser() throws under JDK 25's removed Security Manager). DUCKDB
    also reproduces LINQ4J's camelCase-header → snake_case-column convention Provisa's column
    naming relies on elsewhere (customerID -> customer_id), which PARQUET does not."""
    mapping = {k: _rs(v) if isinstance(v, str) else v for k, v in (source.mapping or {}).items()}
    # recursive=True unconditionally, matching TrinoFilesConnector.details() (trino_connectors.py)
    # exactly — REQ-1730 engine parity: a files source registered once must answer the same query
    # under either engine. Without this, DuckDB's ATTACH silently stopped at the top level of the
    # directory (Calcite's FileSchemaFactory default is non-recursive), so a source with files
    # nested even one subdirectory deep listed fewer tables here than the same source on Trino.
    operand: dict = {
        "executionEngine": mapping.get("execution_engine", "DUCKDB"),
        "recursive": True,
    }
    # REQ-1785: Calcite's own periodic-refresh machinery (FileSchema.startPeriodicRefresh(),
    # RefreshInterval.parse()) -- a duration string like "5 minutes"/"30 seconds". Opt-in via
    # mapping.refresh_interval since most files sources are static; omitted entirely (not a
    # default like executionEngine/recursive above) so FileSchema's own null-check
    # (`if (refreshInterval != null)`) skips the background thread unless a source asks for it.
    refresh_interval = mapping.get("refresh_interval")
    if refresh_interval:
        operand["refreshInterval"] = refresh_interval
    # REQ-1960: a declared crawl lands web pages' tables and data files in the directory before
    # the adapter looks for files. A crawl writes to a local directory, which the adapter
    # refuses otherwise by name.
    from provisa.file_source.crawl import crawl_operand

    crawl = crawl_operand(source.id, mapping)
    if crawl is not None:
        operand["crawl"] = crawl
    storage_type = mapping.get("storage_type")
    if storage_type:
        operand["storageType"] = storage_type
        operand["storageConfig"] = mapping.get("storage_config", {})
        return operand
    raw_path = _rs(source.path)
    if not raw_path:
        raise MissingConnectorConfig(
            f"files source {source.id!r}: requires 'path' (directory) or mapping.storage_type (S3)"
        )
    # Calcite's FileSchemaFactory ``directory`` operand is a plain directory, not a glob — verified
    # live: a trailing ``**`` (or any wildcard) passed through literally makes Calcite look for a
    # subdirectory actually named "**", finding nothing. The Sources form's "Path / Glob" field
    # still accepts a glob (kept for parity with TrinoFilesConnector, which DOES glob-match), so
    # strip any wildcard segment here the same way the pre-REQ-1690 DuckDB connector used to.
    parts = PurePath(raw_path).parts
    dir_parts: list[str] = []
    for part in parts:
        if any(c in part for c in ("*", "?", "[")):
            break
        dir_parts.append(part)
    directory = str(PurePath(*dir_parts)) if dir_parts else raw_path
    # Also verified live: a relative directory resolves against the bundled Calcite child JVM's OWN
    # cwd (its bundle cache directory), not this process's — an unqualified "demo/files/northwind"
    # found nothing even with the glob already stripped. Absolute-ize against this process's cwd,
    # which is where a source's ``path`` is meant to be interpreted from everywhere else in Provisa.
    resolved_dir = str(Path(directory).resolve())
    operand["directory"] = resolved_dir
    # REQ-788: each file_glob table of this source is a glob-url table of the file adapter, which
    # merges the matched files into one logical table (identical column sets, refused by name
    # otherwise; the optional source-file column). The glob is relative to the source directory.
    glob_tables = getattr(source, "file_glob_tables", None) or []
    tables = []
    for spec in glob_tables:
        entry = {
            "name": spec["name"],
            "url": str(Path(resolved_dir) / spec["file_glob"]),
        }
        if spec.get("source_file_column"):
            entry["sourceFileColumn"] = spec["source_file_column"]
        tables.append(entry)
    if tables:
        operand["tables"] = tables
    return operand


def _sharepoint_operand(source: Any) -> dict:
    """sharepoint → siteUrl + tenantId + clientId + (clientSecret OR cert OR device-code). Missing
    siteUrl, tenant/client, or every auth method is a config error (REQ-955).

    Certificate auth carries three hard contracts imposed by the Calcite adapter (REQ-1693):

    * ``authType`` must be emitted as ``CERTIFICATE``. ``SharePointAuthFactory.createAuth`` reads
      ``authType`` and falls back to ``CLIENT_CREDENTIALS`` when it is absent, so a cert operand
      without it dies with "CLIENT_CREDENTIALS auth requires clientId, clientSecret, and tenantId".
    * ``certificatePath`` must be ABSOLUTE. The pgwire server runs with its bundle directory as cwd,
      so a relative path resolves against the cache dir and the PFX is not found.
    * ``certificatePassword`` must be PRESENT, even for a password-less PFX, where it is the empty
      string. ``createCertificateAuth`` rejects a null password and ``CertificateAuth`` calls
      ``certificatePassword.toCharArray()``. Absence of the mapping key is a config error, not an
      empty password — the operator must say which one they mean.
    """
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
        if not PurePath(cert_path).is_absolute():
            raise MissingConnectorConfig(
                f"sharepoint source {source.id!r}: mapping.certificate_path must be an absolute "
                f"path (the pgwire server's cwd is its bundle directory), got {cert_path!r}"
            )
        cert_password = mapping.get("certificate_password")
        if not isinstance(cert_password, str):
            raise MissingConnectorConfig(
                f"sharepoint source {source.id!r}: certificate auth requires "
                f"mapping.certificate_password as a string; use an empty string for a "
                f"password-less PFX (got {type(cert_password).__name__})"
            )
        operand["authType"] = "CERTIFICATE"
        operand["certificatePath"] = cert_path
        operand["certificatePassword"] = cert_password
    elif mapping.get("use_device_code"):
        operand["authType"] = "DEVICE_CODE"
    else:
        raise MissingConnectorConfig(
            f"sharepoint source {source.id!r}: requires clientSecret, a certificate, or device-code"
        )
    return operand


def _splunk_operand(source: Any) -> dict:
    """splunk → url (or host/port/protocol) + (token OR username/password) + optional app,
    disableSslValidation (REQ-724) and datamodelFilter. A missing url/host, or no token and no
    username/password pair, is a config error (REQ-955)."""
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
    # The same two mapping keys the Trino connector carries (trino_connectors.TrinoSplunkConnector):
    # a self-signed Splunk cert (REQ-724) and a discovery filter. SplunkSchemaFactory casts
    # ``disableSslValidation`` to Boolean and ``datamodelFilter`` to String, so the operand carries
    # a real bool, never the "true" string a properties file would use.
    if mapping.get("disable_ssl_validation"):
        operand["disableSslValidation"] = True
    datamodel_filter = mapping.get("datamodel_filter")
    if datamodel_filter:
        operand["datamodelFilter"] = datamodel_filter
    return operand


SALESFORCE_AUTH_TYPES = ("CLIENT_CREDENTIALS", "USERNAME_PASSWORD", "ACCESS_TOKEN")
DESCRIBE_CACHE_DIR_NAME = "describe-cache"


def _salesforce_operand(source: Any) -> dict:
    """salesforce → loginUrl + one complete credential set (REQ-1946), chosen by
    ``mapping.auth_type``: the connected app's consumer key and secret (``CLIENT_CREDENTIALS``,
    the default the form writes); a username and password with the consumer key and secret and an
    optional security token (``USERNAME_PASSWORD``); or a pre-issued access token with its
    instance URL (``ACCESS_TOKEN``). An incomplete set is a config error naming what is missing.

    ``lowercaseAliases`` is always false: the adapter otherwise registers each sObject a second
    time under its lower-case name, and an engine that imports the whole schema would hold every
    sObject twice."""
    mapping = {k: _rs(v) if isinstance(v, str) else v for k, v in (source.mapping or {}).items()}
    who = f"salesforce source {source.id!r}"
    operand: dict = {"loginUrl": salesforce_login_url(source), "lowercaseAliases": False}
    auth_type = mapping.get("auth_type", "CLIENT_CREDENTIALS")
    if auth_type not in SALESFORCE_AUTH_TYPES:
        raise MissingConnectorConfig(
            f"{who}: mapping.auth_type {auth_type!r} is not one of {SALESFORCE_AUTH_TYPES}"
        )
    if auth_type == "ACCESS_TOKEN":
        access_token = mapping.get("access_token")
        instance_url = mapping.get("instance_url")
        if not access_token:
            raise MissingConnectorConfig(f"{who}: access-token auth requires the access token")
        if not instance_url:
            raise MissingConnectorConfig(f"{who}: access-token auth requires the instance URL")
        operand["accessToken"] = access_token
        operand["instanceUrl"] = instance_url
    else:
        client_id = _rs(source.username)
        client_secret = _rs(source.password)
        if not client_id:
            raise MissingConnectorConfig(f"{who}: requires the connected app's consumer key")
        if not client_secret:
            raise MissingConnectorConfig(f"{who}: requires the connected app's consumer secret")
        operand["clientId"] = client_id
        operand["clientSecret"] = client_secret
        if auth_type == "USERNAME_PASSWORD":
            username = mapping.get("sf_username")
            password = mapping.get("sf_password")
            if not username:
                raise MissingConnectorConfig(f"{who}: username-password auth requires the username")
            if not password:
                raise MissingConnectorConfig(f"{who}: username-password auth requires the password")
            operand["username"] = username
            operand["password"] = password
            if mapping.get("security_token"):
                operand["securityToken"] = mapping["security_token"]
    if mapping.get("api_version"):
        operand["apiVersion"] = mapping["api_version"]
    return operand


_OPERAND_BUILDERS: dict[str, Callable[[Any], dict]] = {
    "files": _files_operand,
    "sharepoint": _sharepoint_operand,
    "splunk": _splunk_operand,
    "salesforce": _salesforce_operand,
    "cloudops": cloudops_settings,
}


def build_model_json(source: Any, *, state_dir: Path | None = None) -> dict:
    """The Calcite ``model.json`` for a pgwire-replica source (REQ-955): one custom schema whose
    operand carries the source-specific creds/paths. A non-replica source type is a caller error.

    ``state_dir`` is the directory the source's server runs in (:func:`server_state_dir`). A
    Salesforce model keeps its describe cache there (REQ-1946) — the adapter's own default is a
    directory under the user's home that every server on the machine would share — so a
    Salesforce model built without one is refused. A type that runs on its bundle's own model
    (``BUNDLE_MODEL_TYPES``) has none built."""
    stype = _source_type(source)
    if stype in BUNDLE_MODEL_TYPES:
        raise MissingConnectorConfig(
            f"{stype} source {source.id!r}: its server runs on its bundle's own model; "
            "none is built for it"
        )
    builder = _OPERAND_BUILDERS.get(stype)
    if builder is None:
        raise MissingConnectorConfig(f"source type {stype!r} is not a pgwire-replica connector")
    schema = schema_name(source)
    operand = builder(source)
    if stype == "salesforce":
        if state_dir is None:
            raise MissingConnectorConfig(
                f"salesforce source {source.id!r}: the model needs the server's state directory "
                "for its describe cache"
            )
        operand["describeCacheDirectory"] = str(Path(state_dir) / DESCRIBE_CACHE_DIR_NAME)
    return {
        "version": "1.0",
        "defaultSchema": schema,
        "schemas": [
            {
                "name": schema,
                "type": "custom",
                "factory": _SCHEMA_FACTORY[stype],
                "operand": operand,
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


OWNER_PID_SINCE = (0, 82, 1)  # the first pgwire-calcite release whose launcher takes --owner-pid


def bundle_supports_owner_pid(version: str) -> bool:
    """Whether the bundle release ``version`` (``engine-v0.82.1``) accepts ``--owner-pid``. An
    older launcher rejects the unknown flag outright, so the flag is a versioned contract."""
    parts = tuple(int(p) for p in version.removeprefix("engine-v").split("."))
    return parts >= OWNER_PID_SINCE


class _ProcessGroup:
    """The launcher and everything it spawns (the JVM starts the Python server as its own child,
    and that child holds the port): ``terminate`` signals the whole session so the port is freed,
    ``wait`` blocks until the launcher has exited. A process that ignores SIGTERM is killed."""

    def __init__(self, proc: subprocess.Popen) -> None:
        self._proc = proc

    def terminate(self) -> None:
        try:
            os.killpg(self._proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return  # already gone

    def exit_code(self) -> int | None:
        """The launcher's exit code once it has exited, else None."""
        return self._proc.poll()

    def wait(self, timeout: float) -> None:
        try:
            self._proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self._proc.pid, signal.SIGKILL)
            self._proc.wait(timeout=timeout)


#: Where a spawned pgwire server's own output goes, inside its bundle directory. Appended to, so
#: two servers out of one bundle interleave into one file rather than truncating each other.
SERVER_LOG_NAME = "pgwire-server.log"


def server_environment(source: Any, bundle_dir: Path) -> dict[str, str] | None:
    """What the source's server needs in its environment beyond this process's own, or None
    when its model carries everything (every type but AskAmerica, whose bundled model reads its
    credentials and its catalog by name from the environment). For AskAmerica the bundle's seed
    is installed first (``askamerica.install_seed``): the server is started on that catalog, and
    a bundle that carries none is refused here, by name."""
    if _source_type(source) == "govdata":
        from provisa.federation.askamerica import (
            install_seed,
            server_environment as _askamerica_environment,
        )

        return _askamerica_environment(source, catalog=install_seed(bundle_dir))
    return None


def _spawn_process(command: list[str], cwd: Path, env: dict[str, str] | None = None) -> Any:
    """Launch the pgwire bundle launcher as a child process (the real spawn) in its own session,
    so stopping it stops the server child that actually listens.

    Its output goes to a FILE in the bundle directory, never to our own stdout. Two reasons, and
    the first is a correctness one. This child is in its own session precisely so that it does not
    die with us, which means a process that is SIGKILLed -- no shutdown hook, no ``atexit`` --
    leaves it running while it still holds the stream it inherited. A supervisor waiting for our
    output to close then waits forever: Playwright's webServer teardown hung exactly this way,
    long after its last test had passed, on a JVM that had outlived the backend that spawned it.
    Owning the handle means an orphan can hold nothing of ours open. The second reason is that the
    server logs every statement it serves, which buried the actual test output when it did share
    our stream -- and the log is more useful next to the bundle anyway.
    """
    log = open(cwd / SERVER_LOG_NAME, "a")  # noqa: SIM115 - handed to the child; closed below
    try:
        return _ProcessGroup(
            subprocess.Popen(  # noqa: S603 - args are code-built, not user input
                command,
                cwd=str(cwd),
                start_new_session=True,
                stdout=log,
                stderr=subprocess.STDOUT,
                # ``env``: added to this process's own, for a server whose model reads names
                # from its environment (:func:`server_environment`). None inherits unchanged.
                env=None if env is None else {**os.environ, **env},
            )
        )
    finally:
        # The child holds its own duplicate of the descriptor; ours would otherwise keep the file
        # open for the life of this process and, worse, be inherited by every later spawn.
        log.close()


def _tcp_health(host: str, port: int) -> bool:
    """Whether the pgwire endpoint accepts a TCP connection (the real health probe)."""
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


def server_state_dir(bundle_dir: Path, version: str, source_id: str) -> Path:  # REQ-955
    """The directory one source's server runs in on this instance: its own ``model/model.json``,
    its working directory (where the Calcite adapter keeps its per-schema state, ``.aperio/`` and
    ``catalog-cache-*.pkl``) and its log — with the bundle's read-only parts (launcher, jars,
    runtimes) linked in, never copied.

    The bundle is a machine-wide download cache shared by every Provisa on the machine, and its
    launcher reads ``$HERE/model/model.json`` from the directory above its ``bin``. Run from the
    bundle itself, two servers of one connector — two sources, or two instances — shared one
    model and one adapter state keyed by schema name, so one served the other's files. Linking
    ``bin`` makes ``$HERE`` this directory. Kept under the instance's data directory
    (``$PROVISA_DATA_DIR``, else ``~/.provisa``, as the instance's other per-instance state is),
    by bundle version and source id (the key a server's ports are allocated by)."""
    data_dir = Path(os.environ.get("PROVISA_DATA_DIR") or (Path.home() / ".provisa"))
    state = data_dir / "pgwire" / version / source_id
    state.mkdir(parents=True, exist_ok=True)
    for entry in Path(bundle_dir).iterdir():
        name = entry.name
        # State a server writes is its own; the bundle's copy (from a run before this) is not.
        if name in ("model", SERVER_LOG_NAME) or name.startswith((".", "catalog-cache-")):
            continue
        link = state / name
        if not link.is_symlink() and not link.exists():
            link.symlink_to(entry)
    return state


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
        model: dict | None,
        ports: PortPair,
        spawn: Callable[..., Any] | None = None,
        health_check: Callable[[str, int], bool] | None = None,
        port_is_free: Callable[[int], bool] | None = None,
        environment: dict[str, str] | None = None,
    ) -> None:
        self._bundle_dir = Path(bundle_dir)
        self._spec = spec
        self._model = model
        self._ports = ports
        # Handed to the spawn only when the server needs one (:func:`server_environment`).
        self._environment = environment
        self._spawn = spawn if spawn is not None else _spawn_process
        self._health = health_check if health_check is not None else _tcp_health
        # The port-release probe stop() waits on; injectable so a faked server never consults
        # the machine's real port state (a stray JVM on 5433 failed unit tests otherwise).
        self._port_is_free = port_is_free if port_is_free is not None else _port_is_free
        self._proc: Any = None

    @property
    def model_path(self) -> Path:
        return self._bundle_dir / "model" / "model.json"

    @property
    def ports(self) -> PortPair:
        return self._ports

    def command(self) -> list[str]:
        """The launcher invocation: ``--port`` and ``--calcite-child`` (REQ-955), plus
        ``--owner-pid`` on a bundle that knows it (engine-v0.82.1 and later), so the server
        stops itself when this process dies without running its shutdown — a SIGKILLed host
        (a test runner's teardown, an OOM kill) otherwise stranded every server it started."""
        launcher = self._bundle_dir / "bin" / self._spec.artifact_name
        child = f"{self._ports.calcite_child_host}:{self._ports.calcite_child_port}"
        cmd = [str(launcher), "--port", str(self._ports.pgwire_port), "--calcite-child", child]
        if bundle_supports_owner_pid(self._spec.version):
            cmd += ["--owner-pid", str(os.getpid())]
        return cmd

    def write_model(self) -> Path:
        """Write ``model/model.json`` into the bundle from the source config (REQ-955)."""
        path = self.model_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self._model, indent=2))
        return path

    def start(self) -> None:
        """Write the model (when one is built for the source) and spawn the server. Starting a
        running server is a lifecycle error."""
        if self._proc is not None:
            raise ServerLifecycleError(
                f"pgwire server on port {self._ports.pgwire_port} already running"
            )
        if self._model is not None:  # None: the installed bundle's own model, left as it is
            self.write_model()
        if self._environment is None:
            self._proc = self._spawn(self.command(), self._bundle_dir)
        else:
            self._proc = self._spawn(self.command(), self._bundle_dir, self._environment)

    def health(self) -> bool:
        """Whether the started server's pgwire endpoint is accepting connections."""
        if self._proc is None:
            raise ServerLifecycleError("pgwire server health checked before start")
        return self._health(self._ports.calcite_child_host, self._ports.pgwire_port)

    def exit_code(self) -> int | None:
        """The started server's exit code once its process has ended, else None."""
        if self._proc is None:
            raise ServerLifecycleError("pgwire server exit checked before start")
        return self._proc.exit_code()

    def log_tail(self) -> str:
        """The last lines of the server's own output (written next to its working directory)."""
        lines = (self._bundle_dir / SERVER_LOG_NAME).read_text(errors="replace").splitlines()
        return "\n".join(lines[-_LOG_TAIL_LINES:])

    def stop(self) -> None:
        """Terminate the server and wait for it to exit, so its port is free for the next start
        (idempotent — a no-op if not running)."""
        if self._proc is None:
            return
        self._proc.terminate()
        self._proc.wait(SERVER_STOP_SECONDS)
        self._proc = None
        # The launcher exiting is not the port being free: the Python server it spawned closes
        # its listener on its own schedule. A start on this port before that would bind nothing
        # and the next attach would refuse — so stop() returns only once the port is released.
        deadline = time.monotonic() + SERVER_STOP_SECONDS
        while not self._port_is_free(self._ports.pgwire_port):
            if time.monotonic() >= deadline:
                raise ServerLifecycleError(
                    f"pgwire server port {self._ports.pgwire_port} still bound "
                    f"{SERVER_STOP_SECONDS}s after the launcher exited"
                )
            time.sleep(0.1)


def _prepare_catalog(ports: PortPair) -> None:
    """Send the server its first catalog query, as the user the engines attach as, and wait for
    the answer (see ``ConnectorReplica.await_catalog``). One round trip on a connection of its
    own (the module's generic PostgreSQL connect), closed when the answer comes or the query
    fails. Run on the preparation's own thread, so it has an event loop of its own."""
    import asyncio

    async def _ask() -> None:
        conn = await _pg_connect(ports.calcite_child_host, ports.pgwire_port)
        try:
            await conn.fetch("SELECT count(*) FROM pg_catalog.pg_class")
        finally:
            await conn.close()

    asyncio.run(_ask())


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
        rows = await conn.fetch(f'SELECT * FROM "{schema}"."{table}"', timeout=_fetch_timeout())
        return [dict(row) for row in rows]
    finally:
        await conn.close()


# -- orchestration + engine integration ----------------------------------------


async def land_via_select_keys(
    ports: PortPair,
    schema: str,
    table: str,
    pk_columns: list[str],
    keys: list[tuple[Any, ...]],
    *,
    connect: Callable[[str, int], Any] | None = None,
) -> list[dict]:
    """Connect to the pgwire endpoint as generic PostgreSQL and SELECT exactly the rows whose
    ``pk_columns`` match one of ``keys`` (REQ-1865 keyed fetch) — the same connection
    ``land_via_select`` uses, a bound ``= ANY($1)`` predicate instead of a full-table scan.
    Single-column only, mirroring the other keyed loaders' own single-column contract."""
    if len(pk_columns) != 1:
        raise ValueError(
            f"pgwire-replica keyed fetch on a composite PK {pk_columns!r} is not implemented "
            "(REQ-1865)"
        )
    if not keys:
        return []
    do_connect = connect if connect is not None else _pg_connect
    conn = await do_connect(ports.calcite_child_host, ports.pgwire_port)
    try:
        col = pk_columns[0]
        values = [k[0] for k in keys]
        rows = await conn.fetch(
            f'SELECT * FROM "{schema}"."{table}" WHERE "{col}" = ANY($1)',
            values,
            timeout=_fetch_timeout(),
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


def needs_pgwire_replica(source: Any, engine: Any) -> bool:
    """Whether ``source`` must be landed through the pgwire replica on ``engine`` (REQ-954): a
    pgwire-replica type the engine does not read LIVE through a connector of its own. Trino's
    file/sharepoint/splunk connectors and DuckDB's pgwire attaches (REQ-1690) read in place, so
    no replica is landed there; an engine whose only entry for the type is the land placeholder
    ``complete_reach``/``build_*_engine`` synthesize (a FETCH ``WarehouseNativeConnector``) needs
    the bridge — that placeholder IS the landing path, not a reader (issue #114)."""
    from provisa.federation.strategy import engine_attaches

    if _source_type(source) not in PGWIRE_REPLICA_TYPES:
        return False
    return not engine_attaches(engine, _source_type(source))


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
        spawn: Callable[..., Any] | None = None,
        health_check: Callable[[str, int], bool] | None = None,
        connect: Callable[[str, int], Any] | None = None,
        version: str | None = None,
        port_is_free: Callable[[int], bool] | None = None,
        prepare_catalog: Callable[[PortPair], None] | None = None,
    ) -> None:
        self._source = source
        self._resolver = resolver if resolver is not None else BundleResolver()
        self._allocator = allocator if allocator is not None else PortAllocator()
        self._spawn = spawn
        self._health = health_check
        self._port_is_free = port_is_free
        self._connect = connect
        self._spec: BundleSpec = (
            bundle_spec_for(_source_type(source), version=version)
            if version is not None
            else bundle_spec_for(_source_type(source))
        )
        self._server: PgwireServer | None = None
        # The server's catalog, prepared once per server off any request (:meth:`await_catalog`).
        self._prepare_catalog = prepare_catalog if prepare_catalog is not None else _prepare_catalog
        self._catalog_ready = False
        self._catalog_error: BaseException | None = None
        self._catalog_thread: threading.Thread | None = None
        # Which server the preparation in flight belongs to: one outlived by its server (the
        # server was stopped under it) reports to no one.
        self._catalog_server = 0
        # A start and the first endpoint request can come from different threads; one server.
        self._start_lock = threading.Lock()

    @property
    def spec(self) -> BundleSpec:
        return self._spec

    @property
    def source_type(self) -> str:
        return _source_type(self._source)

    def _ensure_server(self) -> PgwireServer:
        with self._start_lock:
            return self._ensure_server_locked()

    def _ensure_server_locked(self) -> PgwireServer:
        if self._server is not None:
            return self._server
        bundle_dir = self._resolver.resolve(self._spec)  # REQ-956 (resolve + cache)
        ports = self._allocator.allocate(self._source.id)  # REQ-955 (unique ports)
        model: dict | None
        if _source_type(self._source) in BUNDLE_MODEL_TYPES:
            # The installed bundle, started by its own launcher as it is: its model and the
            # catalog prebuilt for that model are used where they were installed.
            run_dir, model = Path(bundle_dir), None
            # The subject map is held to a record of what this release serves; a bundle whose
            # model (read here, never changed) serves anything else is refused by name rather
            # than started.
            from provisa.govdata.subjects import require_recorded_schemas

            require_recorded_schemas(
                json.loads((run_dir / "model" / "model.json").read_text()), self._spec.version
            )
        else:
            # The server runs from its own state directory, the bundle's code linked in: the
            # shared bundle is never written to (one model and one adapter state per server).
            run_dir = server_state_dir(bundle_dir, self._spec.version, self._source.id)
            model = build_model_json(self._source, state_dir=run_dir)  # REQ-955 (config)
        server = PgwireServer(
            bundle_dir=run_dir,
            spec=self._spec,
            model=model,
            ports=ports,
            spawn=self._spawn,
            health_check=self._health,
            port_is_free=self._port_is_free,
            environment=server_environment(self._source, Path(bundle_dir)),
        )
        server.start()  # REQ-955 (lifecycle)
        self._server = server
        return server

    def start(self) -> None:
        """Start the server if it is not running; does not wait for it to listen."""
        self._ensure_server()

    def endpoint(self, *, timeout: float | None = None) -> PortPair:
        """The healthy server's endpoint: start it if needed and wait for its listener (the JVM
        takes seconds to bind); a server that never answers is loud (REQ-955).

        ``timeout`` (REQ-1824) overrides the default ``SERVER_READY_SECONDS`` wait — a real query
        (the only caller that needs this default) has no useful way to proceed without the
        attach, so it should keep waiting the full budget; a discovery/introspection caller should
        pass a short value and treat the resulting ``ServerLifecycleError`` as "still starting",
        not a hard failure. Never starts a SECOND server or restarts a slow one: the JVM keeps
        booting in the background regardless of this call's own timeout, cached in ``_ENDPOINTS``,
        so a later poll (same or longer timeout) picks up wherever it actually is."""
        server = self._ensure_server()
        wait = SERVER_READY_SECONDS if timeout is None else timeout
        deadline = time.monotonic() + wait
        while not server.health():
            code = server.exit_code()
            if code is not None:
                # A start that failed: forgotten, so a later call starts the server afresh.
                self._server = None
                raise ServerExited(self._source.id, code, server.log_tail())
            if time.monotonic() >= deadline:
                raise ServerLifecycleError(
                    f"pgwire server for {self._source.id!r} did not accept connections on port "
                    f"{server.ports.pgwire_port} within {wait}s"
                )
            time.sleep(0.25)
        return server.ports

    async def load(self, table: Any) -> list[dict]:
        """Land the connector table's current rows: start the server if needed, then SELECT (REQ-954).
        ``table`` may be a registered Table (``.table_name``) or a bare table-name string."""
        ports = self.endpoint()
        table_name = getattr(table, "table_name", table)
        return await land_via_select(
            ports, self._schema_of(table), table_name, connect=self._connect
        )

    def _schema_of(self, table: Any) -> str:
        """The server schema ``table`` is read from (:func:`remote_schema`)."""
        return remote_schema(self._source, getattr(table, "schema_name", None))

    async def load_keys(
        self, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        """Exactly the rows whose ``pk_columns`` match one of ``keys`` (REQ-1865 keyed fetch) —
        the ``load_keys`` counterpart to :meth:`load`, same server-start-if-needed shape."""
        ports = self.endpoint()
        table_name = getattr(table, "table_name", table)
        return await land_via_select_keys(
            ports, self._schema_of(table), table_name, pk_columns, keys, connect=self._connect
        )

    def require_serving(self) -> None:
        """Raise if a server was started for the source and cannot be read through yet: not
        accepting connections (``SourceStillStartingError``), exited (``ServerExited``, with its
        log's end), or listening with its catalog still being prepared (:meth:`await_catalog`).
        Starts no server and waits for nothing."""
        server = self._server
        if server is None:
            return
        if server.health():
            self.await_catalog(server.ports, 0)
            return
        code = server.exit_code()
        if code is not None:
            raise ServerExited(self._source.id, code, server.log_tail())
        raise SourceStillStartingError(self._source.id)

    def await_catalog(self, ports: PortPair, timeout: float) -> None:
        """Return once the listening server's catalog is prepared; until then raise
        ``SourceStillStartingError``, having waited at most ``timeout`` seconds.

        A server's FIRST catalog query can take far longer than any later one (a server whose
        catalog was not prebuilt for its model builds it then). An engine's attach issues that
        query, and an attach runs on the request path under the engine's one attach lock, so an
        attach that paid for it held every other statement of every other source. Instead the
        query is sent here, once, from a thread of its own (``_prepare_catalog``); every attach
        and statement meanwhile is answered "still starting" at once, and the attach that
        follows finds the catalog prepared. A failed preparation is ``ServerCatalogFailed`` and
        is not sent again until the server is restarted."""
        with self._start_lock:
            if self._catalog_ready:
                return
            if self._catalog_error is None and self._catalog_thread is None:
                thread = threading.Thread(
                    target=self._prepare_catalog_once,
                    args=(ports, self._catalog_server),
                    daemon=True,
                    name=f"pgwire-catalog:{self._source.id}",
                )
                self._catalog_thread = thread
                thread.start()
            thread = self._catalog_thread
        if thread is not None and timeout > 0:
            thread.join(timeout)
        if self._catalog_ready:
            return
        if self._catalog_error is not None:
            raise ServerCatalogFailed(self._source.id, self._catalog_error)
        raise SourceStillStartingError(self._source.id, preparing_catalog=True)

    def _prepare_catalog_once(self, ports: PortPair, server: int) -> None:
        error: BaseException | None = None
        try:
            self._prepare_catalog(ports)
        except Exception as exc:  # noqa: BLE001 - kept and reported by name (ServerCatalogFailed)
            error = exc
        with self._start_lock:
            if server != self._catalog_server:
                return  # its server was stopped: the next one's catalog is prepared afresh
            self._catalog_error = error
            self._catalog_ready = error is None

    def close(self) -> None:
        """Stop the server (idempotent). The next server's catalog is prepared afresh."""
        if self._server is not None:
            self._server.stop()
            self._server = None
        with self._start_lock:
            # A preparation in flight loses its connection with the server and reports to no one.
            self._catalog_server += 1
            self._catalog_ready = False
            self._catalog_error = None
            self._catalog_thread = None


# -- live endpoint for an engine that ATTACHes the pgwire server (REQ-1690) --------------------

# One replica (one server) per source id, shared by every engine connection that attaches it; the
# shared allocator keeps concurrent sources on distinct ports. Stopped by ``stop_all_servers``.
_ENDPOINT_ALLOCATOR = PortAllocator()
_ENDPOINTS: dict[str, ConnectorReplica] = {}
# One server per source id: an engine attach, a discovery call and a write can each be the first
# to ask for a source's endpoint, from different threads.
_ENDPOINTS_LOCK = threading.Lock()


def _endpoint_replica(source: Any) -> ConnectorReplica:
    """The one replica for ``source``."""
    with _ENDPOINTS_LOCK:
        replica = _ENDPOINTS.get(source.id)
        if replica is None:
            stype = _source_type(source)
            if stype in BUNDLE_MODEL_TYPES:
                # One source of such a type runs from the one installed bundle.
                for held_by, other in _ENDPOINTS.items():
                    if other.source_type == stype:
                        raise InstalledBundleInUse(source.id, stype, held_by)
            replica = ConnectorReplica(source, allocator=_ENDPOINT_ALLOCATOR)
            _ENDPOINTS[source.id] = replica
        _reap_on_exit()
    return replica


def start_endpoint(source: Any) -> None:
    """Start (once) the source's pgwire server without waiting for it to listen (REQ-1946): a
    Salesforce server describes every sObject first, which takes minutes on a large org, so it is
    started when the source is registered or loaded rather than by the first statement that
    needs it. A bundle or config error is raised here; readiness is asked for by
    :func:`ensure_endpoint` / :func:`ensure_endpoint_for_discovery`."""
    _endpoint_replica(source).start()


#: Types whose server takes minutes to first accept a connection (AskAmerica's mounts every
#: schema it serves from object storage), so it is started when the source is saved rather than
#: by the first discovery call or statement that needs it.
STARTED_WHEN_SAVED = frozenset({"govdata"})


def start_when_saved(source: Any) -> None:
    """Start ``source``'s server in the background if its type is one started at save
    (``STARTED_WHEN_SAVED``). A failure to start is logged under the task's name; the discovery
    call or statement that needs the server reports it again."""
    if _source_type(source) not in STARTED_WHEN_SAVED:
        return
    import asyncio

    from provisa.core.connection_loop import spawn_background

    spawn_background(asyncio.to_thread(start_endpoint, source), name=f"pgwire-server:{source.id}")


def require_serving(source_id: str, source_type: str) -> None:
    """Refuse, by name, a statement that reads a source whose server takes minutes to start
    (``STARTED_WHEN_SAVED``) while that server is not yet serving — rather than hand the engine
    a statement naming a relation it could not attach. No server started for the source in this
    process is not a refusal: the engine's attach starts one."""
    if source_type not in STARTED_WHEN_SAVED:
        return
    replica = _ENDPOINTS.get(source_id)
    if replica is not None:
        replica.require_serving()


def server_start_errors() -> tuple[type[BaseException], ...]:
    """What starting a source's server, or waiting for it, raises when the source cannot be
    read through it: the server is not serving, its bundle is not available for this host, its
    configuration is incomplete, or (AskAmerica) its key is missing or refused. An engine's
    attach treats each as "this table is not queryable now" — logged by name and skipped, so
    one source's server never fails a statement that does not read it."""
    from provisa.federation.askamerica import (
        AskAmericaKeyMissing,
        AskAmericaKeyRefused,
        AskAmericaUnavailable,
        BundleSeedMissing,
    )
    from provisa.govdata.subjects import BundleSchemasChanged
    from provisa.runtime_deps.pgwire_bundles import BundleUnavailable

    return (
        ServerNotServing,
        BundleSchemasChanged,
        BundleUnavailable,
        MissingConnectorConfig,
        AskAmericaKeyMissing,
        AskAmericaKeyRefused,
        AskAmericaUnavailable,
        BundleSeedMissing,
    )


#: Whether :func:`stop_all_servers` has been registered to run when this interpreter exits.
#: Registered on the FIRST server start rather than at import, so a process that never attaches a
#: replica registers nothing.
_ATEXIT_REGISTERED = False


def _reap_on_exit() -> None:
    """Make this interpreter's exit stop the servers it started, however it exits.

    A pgwire server is spawned into its OWN session (``start_new_session=True``) so that stopping
    it can signal its whole process group without signalling this process's. The cost of that
    detachment is that it does NOT die with its parent: an interpreter that exits without reaching
    the app's shutdown hook -- a pytest run, a CLI invocation, a server whose supervisor loses
    patience mid-shutdown and sends SIGKILL -- left a JVM running with nobody to stop it, holding
    its port and its parent's inherited stdout. Observed as a Playwright run that never exited
    after its last test passed: the webServer was gone and the JVM it started still held the
    output stream open.

    ``atexit`` is the backstop, not the plan: the app's own shutdown still stops these explicitly
    (provisa/api/app.py), and neither this nor anything else can run on SIGKILL of THIS process.
    """
    global _ATEXIT_REGISTERED
    if _ATEXIT_REGISTERED:
        return
    import atexit

    atexit.register(stop_all_servers)
    _ATEXIT_REGISTERED = True


def ensure_endpoint(source: Any, *, timeout: float | None = None) -> PortPair:
    """Start (once) the source's Calcite pgwire server and return the endpoint the engine attaches
    (REQ-1690). The server must be healthy before the ATTACH, so an unhealthy start is loud here.

    Every pgwire server counts its tables' rows on its first catalog query, so for every type
    the endpoint is returned only once that is done (``ConnectorReplica.await_catalog``), within
    the same bound as the wait for the port; past it the answer is ``SourceStillStartingError``.

    ``timeout`` (REQ-1824): see ``ConnectorReplica.endpoint``. A type whose server takes
    minutes to start (``STARTED_WHEN_SAVED``) is not waited for by an attach: it was started
    when its source was saved, and until it listens AND its catalog is prepared
    (``ConnectorReplica.await_catalog``) the attach is answered ``SourceStillStartingError`` at
    once rather than holding its caller — and the engine's attach pass — for the wait."""
    replica = _endpoint_replica(source)
    if _source_type(source) not in STARTED_WHEN_SAVED:
        # A server that starts in seconds is waited for, as before — and its catalog within the
        # same bound, where an attach used to wait for it without one.
        ports = replica.endpoint(timeout=timeout)
        replica.await_catalog(ports, SERVER_READY_SECONDS if timeout is None else timeout)
        return ports
    if timeout is None:
        # An attach: answered at once, never holding the engine's attach pass.
        try:
            ports = replica.endpoint(timeout=DISCOVERY_READY_SECONDS)
        except ServerLifecycleError as exc:
            raise SourceStillStartingError(source.id) from exc
        replica.await_catalog(ports, 0)
        return ports
    ports = replica.endpoint(timeout=timeout)
    replica.await_catalog(ports, timeout)  # listening is not yet readable: see await_catalog
    return ports


# REQ-1824: how long a DISCOVERY call (schema/table/column introspection) waits for the bundled
# server before reporting "still starting" instead of hanging the request — a real query keeps
# using SERVER_READY_SECONDS via plain ensure_endpoint()/endpoint(), since it has no useful way to
# proceed without the attach either way.
DISCOVERY_READY_SECONDS = 3


def ensure_endpoint_for_discovery(source: Any) -> PortPair:
    """Like ``ensure_endpoint``, but for a discovery/introspection call only: waits at most
    ``DISCOVERY_READY_SECONDS`` (not the full ``SERVER_READY_SECONDS`` a real query needs) and
    raises ``SourceStillStartingError`` — not ``ServerLifecycleError`` — if the server isn't ready
    yet, so callers can tell "still booting, poll again" apart from a genuine startup failure.
    Never starts a second server: the same cached ``ConnectorReplica`` keeps booting in the
    background regardless of how many discovery calls time out waiting on it."""
    try:
        return ensure_endpoint(source, timeout=DISCOVERY_READY_SECONDS)
    except ServerLifecycleError as exc:
        raise SourceStillStartingError(source.id) from exc


def stop_all_servers() -> None:
    """Stop every pgwire server started for a live attach (app shutdown)."""
    for replica in _ENDPOINTS.values():
        replica.close()
    _ENDPOINTS.clear()


def stop_endpoint(source_id: str) -> None:
    """Stop and forget one source's pgwire server, if one was ever started (REQ-1690 delete gap).

    ``ensure_endpoint`` starts a server ONCE per source id and caches it in ``_ENDPOINTS`` for the
    life of this process — every later ATTACH (a fresh DuckDB introspection call, a recreated
    source reusing the same id, an edited path/mapping) reuses that same already-running JVM
    without ever rebuilding ``model.json``. Deleting a source must drop this too, or a source
    recreated under the same id keeps serving whatever schema the first server was started with,
    however stale (confirmed live: a deleted-and-recreated `files` source, same name/id/path,
    that was never re-scanned for its new format because the old server was still answering)."""
    with _ENDPOINTS_LOCK:
        replica = _ENDPOINTS.pop(source_id, None)
    if replica is not None:
        replica.close()


def make_pgwire_loader(
    *,
    resolver: BundleResolver | None = None,
    allocator: PortAllocator | None = None,
    spawn: Callable[..., Any] | None = None,
    health_check: Callable[[str, int], bool] | None = None,
    connect: Callable[[str, int], Any] | None = None,
    port_is_free: Callable[[int], bool] | None = None,
    replicas: dict[str, "ConnectorReplica"] | None = None,
) -> Callable[[Any, Any], Any]:
    """Build a TYPE-level ``adapter_loaders`` row-fetch for pgwire-replica sources (REQ-954), fitting
    the ``SourceRowLoader`` adapter seam ``async (source, table) -> list[dict]``. One
    ``ConnectorReplica`` (one server) is created + reused per source id, so several sources of the same
    type each get their own server on its own allocated port (the shared allocator keeps them unique).

    ``replicas`` (REQ-1865): pass the SAME dict given to :func:`make_pgwire_keyed_loader` for this
    type so the whole-table and keyed loaders share one server per source id instead of each
    starting its own -- defaults to a private dict when this loader is built alone."""
    alloc = allocator if allocator is not None else PortAllocator()
    _replicas: dict[str, ConnectorReplica] = replicas if replicas is not None else {}

    async def _load(source: Any, table: Any) -> list[dict]:
        replica = _replicas.get(source.id)
        if replica is None:
            replica = ConnectorReplica(
                source,
                resolver=resolver,
                allocator=alloc,
                spawn=spawn,
                health_check=health_check,
                connect=connect,
                port_is_free=port_is_free,
            )
            _replicas[source.id] = replica
        return await replica.load(table)

    return _load


def make_pgwire_keyed_loader(
    *,
    resolver: BundleResolver | None = None,
    allocator: PortAllocator | None = None,
    spawn: Callable[..., Any] | None = None,
    health_check: Callable[[str, int], bool] | None = None,
    connect: Callable[[str, int], Any] | None = None,
    port_is_free: Callable[[int], bool] | None = None,
    replicas: dict[str, "ConnectorReplica"] | None = None,
) -> Callable[[Any, Any, list[str], list[tuple[Any, ...]]], Any]:
    """Build a TYPE-level ``keyed_adapter_loaders`` row-fetch for pgwire-replica sources (REQ-1865)
    -- the ``load_keys`` counterpart to :func:`make_pgwire_loader`. Pass the SAME ``replicas`` dict
    (and allocator/resolver/etc.) given to this source's :func:`make_pgwire_loader` call, or a
    second server starts for the same source id."""
    alloc = allocator if allocator is not None else PortAllocator()
    _replicas: dict[str, ConnectorReplica] = replicas if replicas is not None else {}

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        replica = _replicas.get(source.id)
        if replica is None:
            replica = ConnectorReplica(
                source,
                resolver=resolver,
                allocator=alloc,
                spawn=spawn,
                health_check=health_check,
                connect=connect,
                port_is_free=port_is_free,
            )
            _replicas[source.id] = replica
        return await replica.load_keys(table, pk_columns, keys)

    return _load
