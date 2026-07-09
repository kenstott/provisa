# Copyright (c) 2026 Kenneth Stott
# Canary: 2a9f4c18-6b73-4e05-8d21-9c0e3b74f186
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-906: seven DuckDB community-extension federation connectors.

MSSQL, MongoDB, Snowflake, BigQuery, Firebird, Google Sheets, Airport — each references an
external source via ATTACH (in place, never landed) and is backed by a DuckDB extension
installed from the COMMUNITY registry. The probe is LOAD-ONLY (REQ-904): INSTALL ... FROM
community + LOAD, then assert the scanner/attach symbol is registered — it never opens a live
source. Pure unit test: the async ``probe(fetch)`` is driven by a fake DuckDB fetch callable.
"""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import (
    DuckDBAirportConnector,
    DuckDBBigQueryConnector,
    DuckDBFirebirdConnector,
    DuckDBGsheetsConnector,
    DuckDBMongoConnector,
    DuckDBMssqlConnector,
    DuckDBSnowflakeConnector,
    Mechanism,
)

# (connector class, source_type, key, extension, probe_symbol)
_COMMUNITY = [
    (DuckDBMssqlConnector, "sqlserver", "duckdb_mssql", "mssql", "mssql_scan"),
    (DuckDBMongoConnector, "mongodb", "duckdb_mongo", "mongo", "mongo_scan"),
    (DuckDBSnowflakeConnector, "snowflake", "duckdb_snowflake", "snowflake", "snowflake_query"),
    (DuckDBBigQueryConnector, "bigquery", "duckdb_bigquery", "bigquery", "bigquery_scan"),
    (DuckDBFirebirdConnector, "firebird", "duckdb_firebird", "firebird", "firebird_scan"),
    (DuckDBGsheetsConnector, "google_sheets", "duckdb_gsheets", "gsheets", "read_gsheet"),
    (DuckDBAirportConnector, "airport", "duckdb_airport", "airport", "airport_take_flight"),
]

_IDS = [c[0].__name__ for c in _COMMUNITY]


class _FakeDuckDB:
    """Async ``fetch(sql)`` emulating a DuckDB session for the load-only probe.

    ``registered`` is the set of function names duckdb_functions() reports after LOAD;
    ``load_fails`` makes LOAD raise (extension missing/incompatible).
    """

    def __init__(self, *, registered: set[str] | None = None, load_fails: bool = False):
        self._registered = set(registered or ())
        self._load_fails = load_fails
        self.installs: list[str] = []

    async def __call__(self, sql: str):
        if sql.startswith("INSTALL"):
            self.installs.append(sql)
            return []
        if sql.startswith("LOAD"):
            if self._load_fails:
                raise RuntimeError("extension not found")
            return []
        if "duckdb_functions()" in sql:
            fn = sql.split("function_name = '")[1].split("'")[0]
            return [{"n": 1 if fn in self._registered else 0}]
        return []


# ---- identity / packaging (REQ-906 / REQ-899) --------------------------------


@pytest.mark.parametrize("cls,stype,key,ext,sym", _COMMUNITY, ids=_IDS)
def test_connector_identity_and_community_install(cls, stype, key, ext, sym):
    c = cls()
    assert c.engine == "duckdb"
    assert c.source_type == stype
    assert c.key == key
    assert c.extension == ext
    assert c.probe_symbol == sym
    assert c.mechanism is Mechanism.ATTACH_RW  # referenced in place, never landed
    assert c.install_from_community is True  # community registry
    assert c._install_sql() == f"INSTALL {ext} FROM community"


def test_all_seven_community_connectors_are_present():
    assert len(_COMMUNITY) == 7
    assert {stype for _, stype, *_ in _COMMUNITY} == {
        "sqlserver",
        "mongodb",
        "snowflake",
        "bigquery",
        "firebird",
        "google_sheets",
        "airport",
    }


# ---- load-only probe (REQ-904) ----------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("spec", _COMMUNITY, ids=_IDS)
async def test_probe_available_when_extension_loads_and_symbol_registers(spec):
    cls, ext, sym = spec[0], spec[3], spec[4]
    fetch = _FakeDuckDB(registered={sym})
    result = await cls().probe(fetch)
    assert result.available is True
    assert fetch.installs == [f"INSTALL {ext} FROM community"]  # staged from community, not core


@pytest.mark.asyncio
@pytest.mark.parametrize("spec", _COMMUNITY, ids=_IDS)
async def test_probe_unavailable_when_symbol_not_registered(spec):
    # Extension loads but the scanner symbol is absent (wrong build) -> fail closed.
    cls = spec[0]
    result = await cls().probe(_FakeDuckDB(registered=set()))
    assert result.available is False
    assert result.remediation


@pytest.mark.asyncio
@pytest.mark.parametrize("spec", _COMMUNITY, ids=_IDS)
async def test_probe_unavailable_when_extension_fails_to_load(spec):
    cls, ext = spec[0], spec[3]
    result = await cls().probe(_FakeDuckDB(load_fails=True))
    assert result.available is False
    assert ext in result.remediation  # remediation tells the operator how to stage it


# ---- attach DDL references the source in place (REQ-906) ----------------------


def _src(sid, type_, **kw):
    fields = {"host": "h", "port": 1, "database": "db", "username": "u", "password": "p", **kw}
    return Source(id=sid, type=type_, **fields)


def test_mssql_attach_uses_tds_dsn():
    d = DuckDBMssqlConnector().details(
        _src("sql", SourceType.sqlserver, host="mssqlhost", port=1433)
    )
    assert "ATTACH" in d["attach"] and "(TYPE mssql)" in d["attach"]
    assert "Server=mssqlhost,1433" in d["attach"]


def test_snowflake_attach_is_read_only_via_secret():
    d = DuckDBSnowflakeConnector().details(_src("sf", SourceType.snowflake))
    assert d["secret"] == "sf_sf"
    assert "TYPE snowflake" in d["attach"] and "READ_ONLY" in d["attach"]


def test_bigquery_attach_reads_project_from_federation_hints():
    d = DuckDBBigQueryConnector().details(
        _src("bq", SourceType.bigquery, federation_hints={"project": "my-gcp"})
    )
    assert "project=my-gcp" in d["attach"] and "TYPE bigquery" in d["attach"]


def test_gsheets_view_reads_spreadsheet_id_from_hints_and_has_no_pushdown():
    conn = DuckDBGsheetsConnector()
    assert conn.capability().predicate_pushdown is False
    d = conn.details(
        _src("gs", SourceType.google_sheets, federation_hints={"spreadsheet_id": "abc123"})
    )
    assert "read_gsheet('abc123')" in d["view_ddl"]


def test_airport_attach_uses_base_url():
    d = DuckDBAirportConnector().details(
        _src("air", SourceType.airport, base_url="grpc://flight:8815")
    )
    assert "ATTACH 'grpc://flight:8815'" in d["attach"] and "TYPE AIRPORT" in d["attach"]


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
