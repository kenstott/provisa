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
from provisa.federation.connector import Mechanism
from provisa.federation.connector_duckdb import (
    DuckDBAirportConnector,
    DuckDBBigQueryConnector,
    DuckDBFirebirdConnector,
    DuckDBGsheetsConnector,
    DuckDBMongoConnector,
    DuckDBMssqlConnector,
    DuckDBSnowflakeConnector,
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
    # Scanner-view types (Google Sheets read_gsheet) declare SCAN; ATTACH-catalog types declare
    # ATTACH_RW — both read in place, never landed (REQ-951).
    expected = Mechanism.SCAN if stype == "google_sheets" else Mechanism.ATTACH_RW
    assert c.mechanism is expected
    assert c.reads_in_place is True
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
    assert "TYPE snowflake" in d["attach"] and "READ_ONLY" in d["attach"]


def test_snowflake_attach_creates_the_secret_it_names():
    """The attach names a SECRET, so it creates it first, from the source: the account (the
    Account URL's host, without the snowflakecomputing.com domain), user, password and database,
    and the warehouse and role its federation hints carry. Without it every attach failed
    ("Snowflake profile not found: sf_<source>") and no Snowflake table was queryable."""
    d = DuckDBSnowflakeConnector().details(
        _src(
            "sf",
            SourceType.snowflake,
            host="xy12345.us-east-1.snowflakecomputing.com",
            database="SALES",
            username="reader",
            password="it's secret",
            federation_hints={"warehouse": "COMPUTE_WH", "role": "ANALYST"},
        )
    )
    create, attach = (s.strip() for s in d["attach"].split(";"))
    assert create == (
        "CREATE OR REPLACE SECRET \"sf_sf\" (TYPE snowflake, ACCOUNT 'xy12345.us-east-1', "
        "USER 'reader', PASSWORD 'it''s secret', DATABASE 'SALES', WAREHOUSE 'COMPUTE_WH', "
        "ROLE 'ANALYST')"
    )
    assert attach == 'ATTACH \'\' AS "_src_sf" (TYPE snowflake, SECRET "sf_sf", READ_ONLY)'


def test_snowflake_secret_names_no_warehouse_or_role_the_source_has_none_of():
    d = DuckDBSnowflakeConnector().details(_src("sf", SourceType.snowflake, host="acct"))
    assert "ACCOUNT 'acct'" in d["attach"]
    assert "WAREHOUSE" not in d["attach"] and "ROLE" not in d["attach"]


def test_bigquery_attach_reads_project_from_federation_hints():
    d = DuckDBBigQueryConnector().details(
        _src("bq", SourceType.bigquery, federation_hints={"project": "my-gcp"})
    )
    assert "project=my-gcp" in d["attach"] and "TYPE bigquery" in d["attach"]


def test_gsheets_view_reads_spreadsheet_id_from_database_and_has_no_pushdown():  # REQ-1742
    # source.database is the Sources form's "Metadata Sheet ID" field — the shape the UI actually
    # sends, and what TrinoGsheetsConnector already reads. federation_hints["spreadsheet_id"] is
    # never populated by anything (fixed after always raising KeyError).
    conn = DuckDBGsheetsConnector()
    assert conn.capability().predicate_pushdown is False
    d = conn.details(_src("gs", SourceType.google_sheets, database="abc123"))
    assert "read_gsheet('abc123')" in d["view_ddl"]


def test_airport_attach_uses_base_url():
    d = DuckDBAirportConnector().details(
        _src("air", SourceType.airport, base_url="grpc://flight:8815")
    )
    assert "ATTACH 'grpc://flight:8815'" in d["attach"] and "TYPE AIRPORT" in d["attach"]


def test_airport_attach_derives_location_from_host_port_when_no_base_url():  # REQ-1730
    """The generic Sources form has no base_url field (SourceInput carries host/port, never
    base_url) — airport must be registerable from plain host+port alone, the same
    override-else-derive shape neo4j_config_from_source uses."""
    d = DuckDBAirportConnector().details(
        _src("air2", SourceType.airport, host="flight", port=8815, base_url=None)
    )
    assert "ATTACH 'grpc://flight:8815'" in d["attach"] and "TYPE AIRPORT" in d["attach"]


if __name__ == "__main__":
    pytest.main([__file__, "-q"])


# ---- the TDS warehouses: one mssql connector, signed in by type ----------------


def test_fabric_and_synapse_ride_the_mssql_connector_on_duckdb_and_read_in_place():
    from provisa.federation.engine import build_duckdb_engine, live_source_types

    engine = build_duckdb_engine()
    for stype in ("sqlserver", "fabric", "synapse"):
        connector = engine.connectors[stype]
        assert type(connector) is DuckDBMssqlConnector, stype
        assert connector.source_type == stype and connector.extension == "mssql"
        assert connector.reads_in_place, stype
    # Each instance is reported and disabled under a key of its own.
    keys = [engine.connectors[t].key for t in ("sqlserver", "fabric", "synapse")]
    assert keys == ["duckdb_mssql", "duckdb_mssql_fabric", "duckdb_mssql_synapse"]
    assert {"sqlserver", "fabric", "synapse"} <= set(live_source_types("duckdb"))


def test_the_mssql_connector_attaches_no_other_type():
    with pytest.raises(ValueError, match="does not attach 'oracle'"):
        DuckDBMssqlConnector("oracle")


def test_a_fabric_source_naming_a_service_principal_signs_in_as_it():
    d = DuckDBMssqlConnector("fabric").details(
        _src(
            "fab",
            SourceType.fabric,
            host="ws.datawarehouse.fabric.microsoft.com",
            port=1433,
            database="sales",
            federation_hints={"tenant_id": "t-1", "client_id": "c-1", "client_secret": "s'cret"},
        )
    )
    attach = d["attach"]
    assert attach.startswith(
        'CREATE OR REPLACE SECRET "az_fab" (TYPE azure, PROVIDER service_principal'
    )
    assert "TENANT_ID 't-1'" in attach and "CLIENT_ID 'c-1'" in attach
    # A quote in the secret is escaped, not left to end the literal.
    assert "CLIENT_SECRET 's''cret'" in attach
    assert (
        "ATTACH 'Server=ws.datawarehouse.fabric.microsoft.com,1433;Database=sales' "
        "AS \"_src_fab\" (TYPE mssql, AZURE_SECRET 'az_fab')"
    ) in attach
    # Entra: no SQL login is sent.
    assert "User Id" not in attach and "Password" not in attach
    assert d["raw_alias"] == "_src_fab"


def test_a_synapse_source_naming_no_identity_signs_in_as_the_ambient_one():
    d = DuckDBMssqlConnector("synapse").details(
        _src(
            "syn",
            SourceType.synapse,
            host="ws-ondemand.sql.azuresynapse.net",
            port=0,
            database="db",
        )
    )
    assert "PROVIDER credential_chain, CHAIN 'cli'" in d["attach"]
    # No port given: TDS's own.
    assert "Server=ws-ondemand.sql.azuresynapse.net,1433;Database=db" in d["attach"]


def test_trino_keeps_landing_fabric_and_synapse():
    # Trino's sqlserver connector signs in with a SQL login only (connection-user/password); a
    # Fabric or Synapse endpoint takes a Microsoft Entra token, so Trino reaches them by replica.
    from provisa.federation.engine import live_source_types, reachable_source_types

    assert "sqlserver" in live_source_types("trino")
    for stype in ("fabric", "synapse"):
        assert stype not in live_source_types("trino")
        assert stype in reachable_source_types("trino")
