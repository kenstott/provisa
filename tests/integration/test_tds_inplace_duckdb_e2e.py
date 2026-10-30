# Copyright (c) 2026 Kenneth Stott
# Canary: 858b5ef0-c7e0-4646-8f84-9dcb1cdddcd5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: a Microsoft Fabric Warehouse read IN PLACE by the DuckDB engine, over TDS.

Fabric speaks TDS like SQL Server, but signs in only with a Microsoft Entra token. DuckDB's mssql
extension takes one through an Azure secret: a service principal, or the ambient credential chain
(``az login``). This attaches the lane's warehouse with that secret and reads it through the
extension's own catalog: its schemas, its tables and their columns, and rows of a table.

Skipped without FABRIC_SQL_SERVER / FABRIC_DATABASE. The lane's capacity is resumed first and
paused after (tests/integration/fabric_capacity.py)."""

from __future__ import annotations

import os

import pytest

from tests.env_creds import unset

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

duckdb = pytest.importorskip("duckdb", reason="duckdb required")

_HAVE = bool(os.environ.get("FABRIC_SQL_SERVER") and os.environ.get("FABRIC_DATABASE"))
pytestmark.append(
    pytest.mark.skipif(
        not _HAVE, reason=f"not set: {unset('FABRIC_SQL_SERVER', 'FABRIC_DATABASE')}"
    )
)

_SCH = "provisa_tds_inplace_it"


@pytest.fixture(scope="module")
def warehouse():
    """The lane's warehouse, its capacity up, with one small table to read; dropped and the
    capacity paused after (REQ-254), however setup or the tests end.

    Everything that can resume the capacity -- the explicit resume, and the runtime, which
    resumes it on connect (REQ-1775) -- and everything that can fail after it runs inside
    ``capacity_resumed()``, so the pause is never skipped."""
    from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime
    from tests.integration.fabric_capacity import capacity_resumed
    from tests.integration.live_teardown import run_every

    with capacity_resumed():
        rt = MssqlWarehouseRuntime(
            server=os.environ["FABRIC_SQL_SERVER"],
            database=os.environ["FABRIC_DATABASE"],
            engine_name="fabric",
        )
        try:
            cur = rt.connection.cursor()
            try:
                cur.execute(f"IF SCHEMA_ID('{_SCH}') IS NULL EXEC('CREATE SCHEMA [{_SCH}]')")
                cur.execute(f"DROP TABLE IF EXISTS [{_SCH}].[orders]")
                cur.execute(
                    f"CREATE TABLE [{_SCH}].[orders] (id INT, region VARCHAR(16), total DECIMAL(10,2))"
                )
                cur.execute(
                    f"INSERT INTO [{_SCH}].[orders] VALUES (1,'east',10.50),(2,'west',20.00)"
                )
                rt.connection.commit()
            finally:
                cur.close()
            yield rt
        finally:
            run_every(
                [
                    (f"drop schema {_SCH}", lambda: _drop_schema(rt)),
                    ("close the runtime", rt.close),
                ]
            )


def _drop_schema(rt) -> None:
    cur = rt.connection.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS [{_SCH}].[orders]")
        cur.execute(f"DROP SCHEMA IF EXISTS [{_SCH}]")
        rt.connection.commit()
    finally:
        cur.close()


def _attached(alias: str = "fab"):
    con = duckdb.connect()
    con.execute("INSTALL mssql FROM community")
    con.execute("LOAD mssql")
    con.execute("CREATE SECRET fab_azure (TYPE azure, PROVIDER credential_chain, CHAIN 'cli')")
    server = os.environ["FABRIC_SQL_SERVER"]
    database = os.environ["FABRIC_DATABASE"]
    con.execute(
        f"ATTACH 'Server={server},1433;Database={database}' AS {alias} "
        "(TYPE mssql, AZURE_SECRET 'fab_azure')"
    )
    return con


def test_the_mssql_extension_signs_in_to_fabric_and_reads_its_catalog(warehouse):  # noqa: ARG001 -- the fixture holds the table
    con = _attached()
    schemas = [
        r[0]
        for r in con.execute(
            "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'fab'"
        ).fetchall()
    ]
    assert _SCH in schemas, schemas
    tables = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'fab' AND schema_name = ?",
            [_SCH],
        ).fetchall()
    ]
    assert tables == ["orders"], tables
    columns = con.execute(
        "SELECT column_name, data_type FROM duckdb_columns() "
        "WHERE database_name = 'fab' AND schema_name = ? AND table_name = 'orders' "
        "ORDER BY column_index",
        [_SCH],
    ).fetchall()
    assert [c[0] for c in columns] == ["id", "region", "total"], columns
    rows = con.execute(
        f'SELECT id, region, total FROM fab."{_SCH}".orders WHERE region = ? ORDER BY id', ["west"]
    ).fetchall()
    assert [(r[0], r[1], float(r[2])) for r in rows] == [(2, "west", 20.0)]


def test_the_duckdb_engine_s_fabric_connector_attaches_the_warehouse_in_place(warehouse):
    """The product's own attach: the DuckDB engine's fabric connector, for a source that names no
    service principal (the lane signs in as ``az login``), run as the runtime runs it."""
    from provisa.core.models import Source, SourceType
    from provisa.federation.engine import build_duckdb_engine

    connector = build_duckdb_engine().connectors["fabric"]
    source = Source(
        id="fab-inplace",
        type=SourceType.fabric,
        host=os.environ["FABRIC_SQL_SERVER"],
        port=1433,
        database=os.environ["FABRIC_DATABASE"],
        username="",
        password="",
    )
    details = connector.details(source)
    con = duckdb.connect()
    con.execute("INSTALL mssql FROM community")
    con.execute("LOAD mssql")
    con.execute(details["attach"])
    alias = details["raw_alias"]
    tables = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = ?",
            [alias, _SCH],
        ).fetchall()
    ]
    assert tables == ["orders"], tables
    rows = con.execute(f'SELECT count(*), sum(total) FROM "{alias}"."{_SCH}".orders').fetchall()
    assert rows[0][0] == 2 and float(rows[0][1]) == 30.5
