# Copyright (c) 2026 Kenneth Stott
# Canary: 4173ac49-a012-4e8c-b1f1-789ab7e0fb35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A built code host attached by the federation engine as a Postgres catalog (REQ-2100).

A built source's host (provisa/codehost/front.py) speaks pgwire on its one port and serves each
table function as a table, its parameters as ``_nf_`` columns. Each engine attaches it as it
attaches any PostgreSQL server -- DuckDB's postgres extension, Trino's postgresql connector
(trino_connectors.TrinoCodeHostConnector), postgres_fdw -- over TLS, presenting the organisation's
identity (the only certificate the host accepts) and verifying the host by the certificate pinned
when it was paired (sslmode=verify-ca), from files written where the engine reads them. The schema
build writes them (codesource.attach.prepare) and tells the connector where they are and which
schemas each source's tables are registered under (``provisa.schemas``), which the host serves each
function in. Which code tables are attached rather than landed is codesource.attach's.

REQ-2267 part 3: a DEV-mode source's code runs in this process's worker pool, and its PostgreSQL
face is its pgwire sidecar (provisa/codesource/pgwire_sidecar.py), launched like the other pgwire
replicas. Each engine attaches it as it attaches a built host, at the address the sidecar gives
for engines (``Endpoint.engine_host``), in plain PostgreSQL as the other pgwire replicas are
reached: the sidecar is bound to loopback or to the address the deployment names for its engines
alone, and checks no credentials. The schema build tells the connector each dev-mode source's
prepared code and schemas (:meth:`CodeHost.remember_dev`); the attach starts its sidecar on
demand. The sidecar module registers how a sidecar is started here (:func:`register_sidecar`),
since this module is read by the model and the compiler and keeps out of the executor's imports.
"""

# Requirements: REQ-2100, REQ-2267
from __future__ import annotations

import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from provisa.compiler.naming import attach_catalog, engine_attach_name
from provisa.federation.connector_base import (
    Capability,
    Connector,
    Mechanism,
    ProbeResult,
    probe_pg_extension,
)

#: The database name a host answers to: it serves one database, its functions.
DATABASE = "code"
USER = "provisa"
#: The directory the app shares with its Postgres engine's server: on the app's side, and where
#: the server reads it (the same directory where both run on one machine).
PG_FILES_ENV = "PROVISA_PG_ENGINE_FILES"
PG_FILES_AT_ENV = "PROVISA_PG_ENGINE_FILES_AT"


def postgres_shares_files() -> bool:
    """Whether the app shares a directory with its Postgres engine's server."""
    return bool(os.environ.get(PG_FILES_ENV)) and bool(os.environ.get(PG_FILES_AT_ENV))


@dataclass(frozen=True)
class Prepared:
    """What the schema build wrote for one organisation, as the engine reads it: its identity's
    certificate and key (PEM, and the key as PKCS-8 DER, the JDBC driver's form); each built
    source's host certificate, by source; and the schemas each source's tables are registered
    under, by source."""

    certificate: str
    key: str
    key_der: str
    roots: dict[str, str] = field(default_factory=dict)
    schemas: dict[str, str] = field(default_factory=dict)

    def root(self, source_id: str) -> str:
        if source_id not in self.roots:
            raise RuntimeError(
                f"code source {source_id!r}: its host's certificate is not recorded; connect it "
                "again"
            )
        return self.roots[source_id]

    def schemas_of(self, source_id: str) -> str:
        if source_id not in self.schemas:
            raise ValueError(f"code source {source_id!r} has no registered table to attach")
        return self.schemas[source_id]


@dataclass(frozen=True)
class DevSource:
    """REQ-2267: a dev-mode source as the engine attaches it: its prepared code
    (codesource.source.Prepared), which its sidecar serves, and the schemas its tables are
    registered under."""

    prepared: Any
    schemas: str


@dataclass(frozen=True)
class Target:
    """Where an engine's attach of a code source dials, and how: ``tls`` is the built host's
    identity (None for a dev-mode source's sidecar, reached in plain PostgreSQL)."""

    host: str
    port: int
    schemas: str
    tls: Prepared | None


_lock = threading.Lock()
#: What the schema build wrote, by (engine, source type, organisation): one map, so every instance
#: of a connector -- the engine's, and the Trino catalog registry's (trino_connectors) -- reads it.
_PREPARED: dict[tuple[str, str, str], Prepared] = {}
#: REQ-2267: each dev-mode source the schema build named, by (engine, source type, organisation).
_DEV: dict[tuple[str, str, str], dict[str, DevSource]] = {}
#: REQ-2267: ``prepared -> (host an engine reaches the sidecar by, its port)``, starting it if it is
#: not running. Set by provisa.codesource.pgwire_sidecar at import.
_sidecar: Callable[[Any], tuple[str, int]] | None = None


def register_sidecar(resolver: Callable[[Any], tuple[str, int]]) -> None:
    global _sidecar
    _sidecar = resolver


def sidecar_endpoint(prepared: Any) -> tuple[str, int]:
    """The (host, port) an engine dials for a dev-mode source's sidecar, started on demand."""
    if _sidecar is None:
        raise RuntimeError(
            f"the pgwire sidecar of code source {prepared.source_id!r} cannot be started: "
            "provisa.codesource.pgwire_sidecar has not been imported in this process"
        )
    return _sidecar(prepared)


def is_dev(source: Any) -> bool:
    """Whether code source ``source`` is in dev mode, its code in this process's worker: any mode
    but built (remote_sources.mode_of: a mapping naming none is dev)."""
    return (getattr(source, "mapping", None) or {}).get("mode") != "built"


def _org() -> str:
    from provisa.core.request_context import require_current_org

    return require_current_org()


def _remote(source: Any) -> tuple[str, int]:
    remote = (getattr(source, "mapping", None) or {}).get("remote") or {}
    if not remote.get("host") or not remote.get("port"):
        raise ValueError(f"code source {source.id!r} names no remote host: connect it again")
    from provisa.security import sandbox_egress

    # REQ-2006: a held org's remote host is checked at each attach, as it was when it paired.
    sandbox_egress.require_allowed(remote["host"], int(remote["port"]))
    return remote["host"], int(remote["port"])


class CodeHost:
    """What every engine's attach of a built host shares: the files and schemas the schema build
    wrote, by organisation. ``files_for`` names where this engine reads them
    (codesource.engine_identity)."""

    files_for = ""
    #: A code source's own reach lands it (its worker's rows); a code source asks for this attach
    #: instead, of a built host or of a dev-mode sidecar (FederationEngine.asks_in_place).
    opt_in_reach = True
    #: Whether this engine attaches a BUILT source's host, presenting the organisation's TLS
    #: identity. One that cannot (ClickHouse) attaches dev-mode sidecars alone, and its built
    #: sources keep their type's reach: they land.
    attaches_built = True

    def attaches(self, source: Any) -> bool:
        """Whether this engine attaches ``source`` rather than landing it: a dev-mode source's
        sidecar always, a built source's host where the engine presents the TLS identity."""
        return self.attaches_built or is_dev(source)

    def _key(self, org_id: str) -> tuple[str, str, str]:
        return (getattr(self, "engine", ""), getattr(self, "source_type", ""), org_id)

    def remember(self, org_id: str, prepared: Prepared) -> None:
        with _lock:
            _PREPARED[self._key(org_id)] = prepared

    def ready(self) -> bool:
        """Whether the schema build wrote the bound organisation's identity for this engine."""
        with _lock:
            return self._key(_org()) in _PREPARED

    def remember_dev(self, org_id: str, sources: dict[str, DevSource]) -> None:
        """REQ-2267: the organisation's dev-mode sources, as the schema build last named them."""
        with _lock:
            _DEV[self._key(org_id)] = dict(sources)

    def dev_ready(self, source_id: str) -> bool:
        """Whether the schema build named the dev-mode source for this engine."""
        with _lock:
            return source_id in _DEV.get(self._key(_org()), {})

    def dev_source(self, source_id: str) -> DevSource:
        org_id = _org()
        with _lock:
            found = _DEV.get(self._key(org_id), {}).get(source_id)
        if found is None:
            raise RuntimeError(
                f"code source {source_id!r} of organisation {org_id!r}: the schema build has not "
                f"named it for the {getattr(self, 'engine', '')} engine yet"
            )
        return found

    def identity_given(self) -> bool:
        """Whether this engine, one that attaches built hosts (``attaches_built``), can be given
        the identity a built host accepts now. A dev-mode source's sidecar needs none (REQ-2267),
        so an engine that cannot still attaches those, and refuses each built source by name."""
        return True

    def refuse_built(self, source: Any) -> None:
        """Refuse by name the attach of built source ``source`` on an engine that cannot be given
        the identity its host accepts (:meth:`identity_given`)."""
        raise RuntimeError(
            f"code source {source.id!r} is built: the {getattr(self, 'engine', '')} engine "
            "cannot be given the identity its host accepts"
        )

    def target(self, source: Any) -> Target:
        """Where the engine's attach of ``source`` dials: a built source's host over TLS, or a
        dev-mode source's sidecar (REQ-2267), started now if it is not running."""
        if is_dev(source):
            dev = self.dev_source(source.id)
            host, port = sidecar_endpoint(dev.prepared)
            return Target(host, port, dev.schemas, None)
        if not self.identity_given():
            self.refuse_built(source)
        host, port = _remote(source)
        files = self.prepared()
        return Target(host, port, files.schemas_of(source.id), files)

    def prepared(self) -> Prepared:
        org_id = _org()
        with _lock:
            found = _PREPARED.get(self._key(org_id))
        if found is None:
            raise RuntimeError(
                f"organisation {org_id!r}: its remote-host identity is not written for the "
                f"{getattr(self, 'engine', '')} engine yet (the schema build writes it)"
            )
        return found


class DuckDBCodeHostConnector(CodeHost, Connector):
    """DuckDB's postgres extension, its identity in a directory of this process."""

    engine = "duckdb"
    extension = "postgres"
    mechanism = Mechanism.ATTACH_R
    materialized_store = False
    files_for = "process"

    def __init__(self, source_type: str) -> None:
        self.source_type = source_type

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    async def probe(self, fetch) -> ProbeResult:
        return ProbeResult(True, "the postgres extension, loaded for every Postgres attach")

    def details(self, source: Any) -> dict:
        at = self.target(source)
        if at.tls is None:
            security = "sslmode=disable"
        else:
            security = (
                f"sslmode=verify-ca sslrootcert={at.tls.root(source.id)} "
                f"sslcert={at.tls.certificate} sslkey={at.tls.key}"
            )
        dsn = (
            f"host={at.host} port={at.port} dbname={DATABASE} user={USER} {security} "
            f"options='-c provisa.schemas={at.schemas}'"
        )
        alias = f"_src_{source.id}"
        quoted = dsn.replace("'", "''")
        return {
            "attach": f"ATTACH '{quoted}' AS \"{alias}\" (TYPE postgres, READ_ONLY)",
            "raw_alias": alias,
        }


class PgCodeHostConnector(CodeHost, Connector):
    """postgres_fdw, its identity in the directory Provisa shares with the engine's server."""

    engine = "postgres"
    mechanism = Mechanism.ATTACH_R
    materialized_store = False
    files_for = "postgres"

    def __init__(self, source_type: str) -> None:
        self.source_type = source_type

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    async def probe(self, fetch) -> ProbeResult:
        # REQ-2267: postgres_fdw alone; whether built hosts are attached too is per source
        # (identity_given), since a dev-mode source's sidecar needs no identity directory.
        return await probe_pg_extension(fetch, "postgres_fdw", auto_create=True)

    def identity_given(self) -> bool:
        """A built host's identity is written in the directory Provisa shares with the server."""
        return postgres_shares_files()

    def refuse_built(self, source: Any) -> None:
        raise RuntimeError(
            f"code source {source.id!r} is built, and Provisa shares no directory with this "
            "Postgres engine's server for the identity its host accepts: set "
            f"{PG_FILES_ENV} (Provisa's side) and {PG_FILES_AT_ENV} (the server's) to one "
            "directory both reach"
        )

    def details(self, source: Any) -> dict:
        at = self.target(source)
        server = engine_attach_name("fdw_codehost", attach_catalog(source))
        local_schema = engine_attach_name("fdw_codehost", attach_catalog(source))
        schema = getattr(source, "schema_name")
        local_schema = f"{local_schema}_{schema}"
        options = f"-c provisa.schemas={at.schemas}"
        if at.tls is None:
            security = "sslmode 'disable'"
        else:
            security = (
                f"sslmode 'verify-ca', sslrootcert '{at.tls.root(source.id)}', "
                f"sslcert '{at.tls.certificate}', sslkey '{at.tls.key}'"
            )
        server_ddl = [
            "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
            f'CREATE SERVER IF NOT EXISTS "{server}" FOREIGN DATA WRAPPER postgres_fdw '
            f"OPTIONS (host '{at.host}', port '{at.port}', dbname '{DATABASE}', {security}, "
            f"options '{options}')",
            # The host signs in by certificate alone, the sidecar checks none: no password.
            f'CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER "{server}" '
            f"OPTIONS (user '{USER}', password_required 'false')",
            f'CREATE SCHEMA IF NOT EXISTS "{local_schema}"',
        ]
        return {
            "attach_ddl": [
                *server_ddl,
                f'IMPORT FOREIGN SCHEMA "{schema}" FROM SERVER "{server}" INTO "{local_schema}"',
            ],
            "local_schema": local_schema,
            "server_ddl_for_copy": server_ddl,
            "server": server,
            "remote_schema": schema,
        }


def trino_details(connector: CodeHost, source: Any) -> dict:
    """Trino's postgresql connector: a built host with its identity in the engine tree it
    mounts, a dev-mode source's sidecar in plain PostgreSQL (REQ-2267)."""
    from urllib.parse import quote

    at = connector.target(source)
    options = quote(f"-c provisa.schemas={at.schemas}")
    if at.tls is None:
        security = "sslmode=disable"
    else:
        security = (
            f"sslmode=verify-ca&sslrootcert={quote(at.tls.root(source.id))}"
            f"&sslcert={quote(at.tls.certificate)}&sslkey={quote(at.tls.key_der)}"
        )
    url = f"jdbc:postgresql://{at.host}:{at.port}/{DATABASE}?{security}&options={options}"
    return {
        "connection-url": url,
        "connection-user": USER,
        # The host's names are the code's own: mixed case, matched as the engine folds them.
        "case-insensitive-name-matching": "true",
        # Its row estimate is the count it last answered (pg_class.reltuples).
        "statistics.enabled": "true",
    }
