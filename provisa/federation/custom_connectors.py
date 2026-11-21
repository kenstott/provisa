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
import string
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from provisa.compiler.naming import attach_catalog, engine_attach_name
from provisa.federation.connector_base import Capability, Connector, Mechanism, ProbeResult
from provisa.federation.connector_base import probe_pg_extension
from provisa.federation.clickhouse_quoting import TABLE_PLACEHOLDER, ch_ident, ch_lit
from provisa.federation.connector_duckdb import _DuckDBExtensionConnector
from provisa.federation.duckdb_quoting import ident, lit
from provisa.federation.pg_quoting import pg_ident, pg_lit

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
    for name in (
        "id",
        "host",
        "port",
        "database",
        "username",
        "password",
        "path",
        "schema_name",
        "table_name",
    ):
        val = getattr(source, name, None)
        fields[name] = "" if val is None else str(val)
    for k, v in (getattr(source, "federation_hints", None) or {}).items():
        fields[str(k)] = "" if v is None else str(v)
    return fields


# -- the template contract (SEC-C6b) ------------------------------------------------------------
#
# A template takes raw values: it never quotes a placeholder itself. Two template shapes exist.
#
#   * A VALUE template (a pg_fdw OPTIONS value, a pg_fdw remote_schema) is plain text, not SQL.
#     ``{field}`` is replaced by the raw value and the whole result is then written as one literal
#     (an option value) or one identifier (the remote schema) with the engine's quoting helper.
#   * A CLAUSE template (duckdb attach_template / scan_template, clickhouse engine_template) is a
#     SQL fragment. Each placeholder is written by the engine's quoting helper where it stands:
#     ``{field}`` as a string literal, ``{field:ident}`` as an identifier, ``{field:int}`` as an
#     integer (a value that is not one is refused). A value composed with other text is composed in
#     SQL (``'ducklake:' || {path}``, ``concat({host}, ':', {port})``). A placeholder inside quotes
#     in a clause template would be written into a literal or a name it can end, so such a template
#     is refused when the descriptor is read.

_QUOTERS: dict[str, tuple[Callable[[str], str], Callable[..., str]]] = {
    "postgres": (pg_lit, pg_ident),
    "duckdb": (lit, ident),
    "clickhouse": (ch_lit, ch_ident),
}


class _ClauseFormatter(string.Formatter):
    """Writes each placeholder of a clause template through ``engine``'s quoting helpers."""

    def __init__(self, engine: str) -> None:
        self._lit, self._ident = _QUOTERS[engine]

    def convert_field(self, value: Any, conversion: str | None) -> Any:
        if conversion is not None:
            raise ValueError(f"a template placeholder takes no conversion (!{conversion})")
        return value

    def format_field(self, value: Any, format_spec: str) -> str:
        text = str(value)
        if format_spec == "":
            return self._lit(text)
        if format_spec == "ident":
            return self._ident(text)
        if format_spec == "int":
            return str(int(text))
        raise ValueError(
            f"template placeholder spec {format_spec!r}: expected none (a literal), "
            "'ident' or 'int'"
        )


def _check_clause_template(template: str, engine: str) -> str:
    """``template`` when no placeholder of it stands inside quotes; else ValueError. A ClickHouse
    quoted string or name reads a backslash escape, so the character after one is skipped there."""
    quote: str | None = None
    for literal_text, field, _spec, _conversion in string.Formatter().parse(template):
        escaped = False
        for ch in literal_text:
            if escaped:
                escaped = False
            elif quote is not None and ch == "\\" and engine == "clickhouse":
                escaped = True
            elif quote is None and ch in "'\"`":
                quote = ch
            elif ch == quote:
                quote = None
        if field is not None and quote is not None:
            raise ValueError(
                f"template {template!r}: placeholder {{{field}}} stands inside {quote}...{quote}; "
                "a template takes raw values and Provisa quotes each one, so write it unquoted"
            )
    return template


def _clause(template: str, engine: str, fields: dict[str, str]) -> str:
    """A clause template with each placeholder written by ``engine``'s quoting helpers. A template
    naming an unknown field fails LOUD (KeyError): a descriptor/source mismatch, not broken DDL."""
    return _ClauseFormatter(engine).vformat(template, (), fields)


def _value(template: str, fields: dict[str, str]) -> str:
    """A value template with ``{field}`` replaced by the raw value: text, quoted by the caller as one
    literal or identifier. An unknown field fails LOUD (KeyError)."""
    return string.Formatter().vformat(template, (), fields)


def _pg_options(options: dict[str, str], fields: dict[str, str]) -> str:
    """An OPTIONS(...) body: ``"key" 'value', …``, each value a value template written as a literal."""
    return ", ".join(f"{pg_ident(k)} {pg_lit(_value(v, fields))}" for k, v in options.items())


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
        return await probe_pg_extension(fetch, self._fdw, auto_create=True)

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        server = engine_attach_name("fdw", attach_catalog(source))
        ddl = [
            f"CREATE EXTENSION IF NOT EXISTS {pg_ident(self._fdw)}",
            f"CREATE SERVER IF NOT EXISTS {pg_ident(server)} "
            f"FOREIGN DATA WRAPPER {pg_ident(self._fdw)} "
            f"OPTIONS ({_pg_options(self._server_options, fields)})",
        ]
        if self._user_mapping is not None:
            # A bare (no-OPTIONS) user mapping is the SQL/MED form a no-auth FDW needs (mongo_fdw against
            # an unauthenticated MongoDB): the mapping must EXIST, but an empty username/password would
            # make the driver attempt a failing auth. `user_mapping: {}` ⇒ bare; keys ⇒ OPTIONS(...).
            um = f"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER {pg_ident(server)}"
            if self._user_mapping:
                um += f" OPTIONS ({_pg_options(self._user_mapping, fields)})"
            ddl.append(um)
        if self._supports_import:
            local_schema = engine_attach_name("fdw", attach_catalog(source))
            ddl += [
                f"CREATE SCHEMA IF NOT EXISTS {pg_ident(local_schema)}",
                f"IMPORT FOREIGN SCHEMA {pg_ident(_value(self._remote_schema, fields))} "
                f"FROM SERVER {pg_ident(server)} INTO {pg_ident(local_schema)}",
            ]
            return {"attach_ddl": ddl, "local_schema": local_schema}
        # No IMPORT: the pg runtime completes an explicit CREATE FOREIGN TABLE from column metadata,
        # binding these per-table OPTIONS (e.g. mongo_fdw's database/collection).
        return {
            "server_ddl": ddl,
            "server": server,
            "table_options": f"OPTIONS ({_pg_options(self._table_options, fields)})",
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
        self._attach_template: str = _check_clause_template(d["attach_template"], "duckdb")
        self._remote_schema: str | None = d.get("remote_schema")

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        alias = f"_src_{source.id}"
        fields["alias"] = alias
        out: dict = {"attach": _clause(self._attach_template, "duckdb", fields), "raw_alias": alias}
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
        self._scan_template: str = _check_clause_template(d["scan_template"], "duckdb")

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        scan = _clause(self._scan_template, "duckdb", fields)
        return {"view_ddl": f"CREATE VIEW {ident(source.id)} AS SELECT * FROM {scan}"}


async def _probe_clickhouse_engine(fetch, ch_engine: str) -> ProbeResult:  # REQ-1178
    """Probe a ClickHouse integration table engine against its STANDARD discovery catalog
    (system.table_engines) — present ⇒ available, absent ⇒ unavailable with remediation."""
    rows = await fetch(f"SELECT 1 FROM system.table_engines WHERE name = {ch_lit(ch_engine)}")
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
        self._engine_template: str = _check_clause_template(d["engine_template"], "clickhouse")

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
        local_schema = engine_attach_name("ch", attach_catalog(source))
        return {
            "attach_ddl": [
                f"CREATE DATABASE IF NOT EXISTS {ch_ident(local_schema)} "
                f"ENGINE = {_clause(self._engine_template, 'clickhouse', fields)}"
            ],
            "local_schema": local_schema,
        }


class GenericClickHouseTableConnector(_GenericClickHouseConnector):  # REQ-1178
    """CREATE TABLE … ENGINE = <Engine>(…) — a per-table engine whose columns the registry supplies
    (MongoDB/Redis shape). The engine_template may carry a {table} placeholder the runtime binds."""

    mechanism = Mechanism.ATTACH_RW

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        # The runtime binds the collection/table name: {table} is written as the literal
        # '{table}', which clickhouse_quoting.bind_table replaces with the name as a literal.
        fields["table"] = TABLE_PLACEHOLDER
        return {
            "engine_clause": _clause(self._engine_template, "clickhouse", fields),
            "requires_columns": True,
        }


class GenericClickHouseScanConnector(_GenericClickHouseConnector):  # REQ-1178
    """CREATE TABLE … ENGINE = <Engine>(…) with ClickHouse inferring the schema — a file/lake/URL
    engine read in place (S3/URL/File/Iceberg/Delta/Hudi shape)."""

    mechanism = Mechanism.SCAN

    def details(self, source: Source) -> dict:
        fields = _source_fields(source)
        return {
            "engine_clause": _clause(self._engine_template, "clickhouse", fields),
            "infer": True,
            "validate": True,
        }


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
    # REQ-1669: includes-aware, so a wrapper config's fragments are seen.
    from provisa.core.config_loader import read_config_with_includes

    data = read_config_with_includes(path)
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
