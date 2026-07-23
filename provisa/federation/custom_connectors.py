# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e1a70-2b85-4d63-8f01-6a7d3e50c9f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Config-driven custom source connectors for the native federation engines (REQ-1177).

An operator declares a connector for a new source_type in config/custom_connectors.yaml — no code — and
the native pg / duckdb engines gain reachability to it. The descriptor is bounded to what each engine's
extension API standardises:

  * POSTGRES is GENERIC (SQL/MED, an ISO standard): every FDW uses the same DDL shape
    (CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…) + CREATE USER MAPPING + IMPORT FOREIGN SCHEMA,
    or an explicit CREATE FOREIGN TABLE when the FDW lacks IMPORT). The descriptor supplies only the
    per-FDW variance: extension name, SERVER option keys, the USER MAPPING keys, a supports_import flag,
    and (when not importing) table OPTIONS. So an arbitrary standard-conforming FDW is drivable.

  * DUCKDB is a CURATED UNION of the two mechanisms the engine already drives — ATTACH-TYPE
    (INSTALL/LOAD + ATTACH …) and SCAN table-function (CREATE VIEW … AS SELECT * FROM <fn>(…)). The
    descriptor picks a mechanism and its template; an extension exposing neither is unsupported.

Availability is verified at attach time against each engine's STANDARD discovery catalog — the pg probe
checks pg_available_extensions/pg_extension; the duckdb probe INSTALL/LOADs and checks duckdb_functions()
for the declared symbol — failing loud when the declared extension is not installable (no silent skip)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from provisa.federation.connector_base import Capability, Connector, Mechanism, ProbeResult
from provisa.federation.connector_duckdb import _DuckDBExtensionConnector, _probe_pg_extension

if TYPE_CHECKING:
    from provisa.core.models import Source

_DEFAULT_CONFIG = "config/custom_connectors.yaml"


def _config_path() -> Path:
    """The custom-connectors config path. PROVISA_CUSTOM_CONNECTORS overrides for tests/BYO layouts."""
    return Path(os.environ.get("PROVISA_CUSTOM_CONNECTORS", _DEFAULT_CONFIG))


def _source_fields(source: Any) -> dict[str, str]:
    """The substitution vocabulary a descriptor template may reference — source attributes as strings
    (None → ''), plus every federation_hints key. Templates use {host}/{port}/{database}/{path}/… ."""
    fields: dict[str, str] = {}
    for name in ("id", "host", "port", "database", "username", "password", "path",
                 "schema_name", "table_name"):
        val = getattr(source, name, None)
        fields[name] = "" if val is None else str(val)
    for k, v in (getattr(source, "federation_hints", None) or {}).items():
        fields[str(k)] = "" if v is None else str(v)
    return fields


def _fmt(template: str, fields: dict[str, str]) -> str:
    """Substitute {field} placeholders. A template referencing an unknown field fails LOUD (KeyError),
    surfacing a descriptor/source mismatch rather than silently emitting a broken statement."""
    return template.format(**fields)


def _opts(options: dict[str, str], fields: dict[str, str]) -> str:
    """Render an OPTIONS(...) body: ``key 'value', …`` with each value templated."""
    return ", ".join(f"{k} '{_fmt(v, fields)}'" for k, v in options.items())


class GenericPgFdwConnector(Connector):  # REQ-1177
    """A config-declared Postgres FDW connector — emits standard SQL/MED DDL from a descriptor."""

    def __init__(self, d: dict) -> None:
        self.engine = "postgres"
        self.source_type = d["source_type"]
        self.key = d.get("key") or f"custom_{d['source_type']}"
        self.mechanism = Mechanism(d.get("mechanism", "attach_rw"))
        self._fdw: str = d["extension"]
        self._server_options: dict[str, str] = d.get("server_options", {})
        self._user_mapping: dict[str, str] | None = d.get("user_mapping")
        self._supports_import: bool = d.get("supports_import", True)
        self._table_options: dict[str, str] = d.get("table_options", {})
        self._remote_schema: str = d.get("remote_schema", "public")

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, write=self.mechanism == Mechanism.ATTACH_RW)

    async def probe(self, fetch) -> ProbeResult:  # standard PG discovery — pg_available_extensions
        return await _probe_pg_extension(fetch, self._fdw, auto_create=True)

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        server = f"fdw_{source.id}"
        ddl = [
            f"CREATE EXTENSION IF NOT EXISTS {self._fdw}",
            f"CREATE SERVER IF NOT EXISTS {server} FOREIGN DATA WRAPPER {self._fdw} "
            f"OPTIONS ({_opts(self._server_options, fields)})",
        ]
        if self._user_mapping is not None:
            # A bare (no-OPTIONS) user mapping is the SQL/MED form a no-auth FDW needs (mongo_fdw against
            # an unauthenticated MongoDB): the mapping must EXIST, but an empty username/password would
            # make the driver attempt a failing auth. `user_mapping: {}` ⇒ bare; keys ⇒ OPTIONS(...).
            um = f"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER {server}"
            if self._user_mapping:
                um += f" OPTIONS ({_opts(self._user_mapping, fields)})"
            ddl.append(um)
        if self._supports_import:
            local_schema = f"fdw_{source.id}"
            ddl += [
                f"CREATE SCHEMA IF NOT EXISTS {local_schema}",
                f"IMPORT FOREIGN SCHEMA {_fmt(self._remote_schema, fields)} "
                f"FROM SERVER {server} INTO {local_schema}",
            ]
            return {"attach_ddl": ddl, "local_schema": local_schema}
        # No IMPORT: the pg runtime completes an explicit CREATE FOREIGN TABLE from column metadata,
        # binding these per-table OPTIONS (e.g. mongo_fdw's database/collection).
        return {
            "server_ddl": ddl,
            "server": server,
            "table_options": f"OPTIONS ({_opts(self._table_options, fields)})",
        }


class GenericDuckDbAttachConnector(_DuckDBExtensionConnector):  # REQ-1177
    """A config-declared DuckDB ATTACH-type connector (INSTALL/LOAD + templated ATTACH)."""

    def __init__(self, d: dict) -> None:
        self.source_type = d["source_type"]
        self.key = d.get("key") or f"custom_{d['source_type']}"
        self.extension = d["extension"]
        self.probe_symbol = d["probe_symbol"]
        self.install_from_community = d.get("install_from_community", True)
        self.mechanism = Mechanism(d.get("mechanism", "attach_rw"))
        self._attach_template: str = d["attach_template"]
        self._remote_schema: str | None = d.get("remote_schema")

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        alias = f"_src_{source.id}"
        fields["alias"] = alias
        out: dict = {"attach": _fmt(self._attach_template, fields), "raw_alias": alias}
        if self._remote_schema:
            out["remote_schema"] = self._remote_schema
        return out


class GenericDuckDbScanConnector(_DuckDBExtensionConnector):  # REQ-1177
    """A config-declared DuckDB SCAN-type connector (INSTALL/LOAD + a read table-function view)."""

    def __init__(self, d: dict) -> None:
        self.source_type = d["source_type"]
        self.key = d.get("key") or f"custom_{d['source_type']}"
        self.extension = d["extension"]
        self.probe_symbol = d["probe_symbol"]
        self.install_from_community = d.get("install_from_community", True)
        self.mechanism = Mechanism.SCAN  # a scanner view reads the file/object in place
        self._scan_template: str = d["scan_template"]

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        return {"view_ddl": f"CREATE VIEW {source.id} AS SELECT * FROM {_fmt(self._scan_template, fields)}"}


async def _probe_clickhouse_engine(fetch, ch_engine: str) -> ProbeResult:  # REQ-1178
    """Probe a ClickHouse integration table engine against its STANDARD discovery catalog
    (system.table_engines) — present ⇒ available, absent ⇒ unavailable with remediation."""
    rows = await fetch(f"SELECT 1 FROM system.table_engines WHERE name = '{ch_engine}'")
    if rows:
        return ProbeResult(True, f"ClickHouse engine {ch_engine} available")
    return ProbeResult(
        False,
        f"ClickHouse engine {ch_engine} not present in this build",
        f"this ClickHouse must be built with the {ch_engine} integration engine",
    )


class _GenericClickHouseConnector(Connector):  # REQ-1178
    """Shared base for config-declared ClickHouse connectors — probes system.table_engines."""

    engine = "clickhouse"

    def __init__(self, d: dict) -> None:
        self.source_type = d["source_type"]
        self.key = d.get("key") or f"custom_{d['source_type']}"
        self._ch_engine: str = d["ch_engine"]
        self._engine_template: str = d["engine_template"]

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    async def probe(self, fetch) -> ProbeResult:
        return await _probe_clickhouse_engine(fetch, self._ch_engine)


class GenericClickHouseDatabaseConnector(_GenericClickHouseConnector):  # REQ-1178
    """CREATE DATABASE … ENGINE = <Engine>(…) — a relational engine that auto-exposes every remote
    table (PostgreSQL/MySQL/SQLite shape)."""

    mechanism = Mechanism.ATTACH_RW

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, write=True)

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        local_schema = f"ch_{source.id}"
        return {
            "attach_ddl": [
                f'CREATE DATABASE IF NOT EXISTS "{local_schema}" '
                f"ENGINE = {_fmt(self._engine_template, fields)}"
            ],
            "local_schema": local_schema,
        }


class GenericClickHouseTableConnector(_GenericClickHouseConnector):  # REQ-1178
    """CREATE TABLE … ENGINE = <Engine>(…) — a per-table engine whose columns the registry supplies
    (MongoDB/Redis shape). The engine_template may carry a {table} placeholder the runtime binds."""

    mechanism = Mechanism.ATTACH_RW

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        fields["table"] = "{table}"  # runtime binds the collection/table name; keep it a placeholder
        return {"engine_clause": _fmt(self._engine_template, fields), "requires_columns": True}


class GenericClickHouseScanConnector(_GenericClickHouseConnector):  # REQ-1178
    """CREATE TABLE … ENGINE = <Engine>(…) with ClickHouse inferring the schema — a file/lake/URL
    engine read in place (S3/URL/File/Iceberg/Delta/Hudi shape)."""

    mechanism = Mechanism.SCAN

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        return {"engine_clause": _fmt(self._engine_template, fields), "infer": True, "validate": True}


_KINDS = {
    "pg_fdw": GenericPgFdwConnector,
    "duckdb_attach": GenericDuckDbAttachConnector,
    "duckdb_scan": GenericDuckDbScanConnector,
    "clickhouse_database": GenericClickHouseDatabaseConnector,
    "clickhouse_table": GenericClickHouseTableConnector,
    "clickhouse_scan": GenericClickHouseScanConnector,
}


def load_custom_connectors(engine: str) -> list[Connector]:
    """Build the config-declared connectors for ``engine`` ("postgres" | "duckdb"). Absent config → []
    (no custom connectors is normal). An unknown ``kind`` fails loud — a descriptor typo must not be a
    silent no-op that leaves a source_type quietly unreachable."""
    path = _config_path()
    if not path.exists():
        return []
    data = yaml.safe_load(path.read_text()) or {}
    out: list[Connector] = []
    for d in data.get("connectors", []):
        if d.get("engine") != engine:
            continue
        kind = d["kind"]
        if kind not in _KINDS:
            raise ValueError(
                f"custom connector for source_type {d.get('source_type')!r}: unknown kind {kind!r} "
                f"(expected one of {sorted(_KINDS)})"
            )
        out.append(_KINDS[kind](d))
    return out
