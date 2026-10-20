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
"""

# Requirements: REQ-2100
from __future__ import annotations

import os
import threading
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


_lock = threading.Lock()


def _org() -> str:
    from provisa.core.request_context import require_current_org

    return require_current_org()


def _remote(source: Any) -> tuple[str, int]:
    remote = (getattr(source, "mapping", None) or {}).get("remote") or {}
    if not remote.get("host") or not remote.get("port"):
        raise ValueError(f"code source {source.id!r} names no remote host: connect it again")
    return remote["host"], int(remote["port"])


class CodeHost:
    """What every engine's attach of a built host shares: the files and schemas the schema build
    wrote, by organisation. ``files_for`` names where this engine reads them
    (codesource.engine_identity)."""

    files_for = ""
    #: A code source's own reach lands it (its worker's rows); the engine attaches its host only
    #: for a built source, which asks for it by its mode (FederationEngine.asks_in_place).
    opt_in_reach = True

    def _held(self) -> dict[str, Prepared]:
        with _lock:
            return self.__dict__.setdefault("_prepared", {})

    def remember(self, org_id: str, prepared: Prepared) -> None:
        held = self._held()
        with _lock:
            held[org_id] = prepared

    def ready(self) -> bool:
        """Whether the schema build wrote the bound organisation's identity for this engine."""
        return _org() in self._held()

    def prepared(self) -> Prepared:
        org_id = _org()
        found = self._held().get(org_id)
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
        host, port = _remote(source)
        files = self.prepared()
        dsn = (
            f"host={host} port={port} dbname={DATABASE} user={USER} sslmode=verify-ca "
            f"sslrootcert={files.root(source.id)} sslcert={files.certificate} sslkey={files.key} "
            f"options='-c provisa.schemas={files.schemas_of(source.id)}'"
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
        if not postgres_shares_files():
            return ProbeResult(
                False,
                "Provisa shares no directory with this engine's server for the identity a built "
                "code host accepts",
                f"set {PG_FILES_ENV} (Provisa's side) and {PG_FILES_AT_ENV} (the server's) to "
                "one directory both reach",
            )
        return await probe_pg_extension(fetch, "postgres_fdw", auto_create=True)

    def details(self, source: Any) -> dict:
        host, port = _remote(source)
        files = self.prepared()
        server = engine_attach_name("fdw_codehost", attach_catalog(source))
        local_schema = engine_attach_name("fdw_codehost", attach_catalog(source))
        schema = getattr(source, "schema_name")
        local_schema = f"{local_schema}_{schema}"
        options = f"-c provisa.schemas={files.schemas_of(source.id)}"
        server_ddl = [
            "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
            f'CREATE SERVER IF NOT EXISTS "{server}" FOREIGN DATA WRAPPER postgres_fdw '
            f"OPTIONS (host '{host}', port '{port}', dbname '{DATABASE}', sslmode 'verify-ca', "
            f"sslrootcert '{files.root(source.id)}', sslcert '{files.certificate}', "
            f"sslkey '{files.key}', options '{options}')",
            # The host signs in by certificate alone: no password to require.
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
    """Trino's postgresql connector, its identity in the engine tree it mounts."""
    from urllib.parse import quote

    host, port = _remote(source)
    files = connector.prepared()
    options = quote(f"-c provisa.schemas={files.schemas_of(source.id)}")
    url = (
        f"jdbc:postgresql://{host}:{port}/{DATABASE}?sslmode=verify-ca"
        f"&sslrootcert={quote(files.root(source.id))}&sslcert={quote(files.certificate)}"
        f"&sslkey={quote(files.key_der)}&options={options}"
    )
    return {
        "connection-url": url,
        "connection-user": USER,
        # The host's names are the code's own: mixed case, matched as the engine folds them.
        "case-insensitive-name-matching": "true",
        # Its row estimate is the count it last answered (pg_class.reltuples).
        "statistics.enabled": "true",
    }
