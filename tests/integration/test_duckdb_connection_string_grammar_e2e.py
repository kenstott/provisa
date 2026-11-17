# Copyright (c) 2026 Kenneth Stott
# Canary: 9e3c1a5b-7d2f-4c86-b4e1-2f8a6d0c3b57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a value carrying a connection string's own separators reaches the server whole.

The DuckDB engine hands the mssql and firebird extensions a connection string they parse again
(ADO.NET ``Key=Value;`` and a ``firebird://`` URL). A sign-in made for this test has a password
holding each string's separators (``;``, ``=``, braces; ``:``, ``@``, ``/``, ``?``, ``%``,
``+``, ``&``) and, for SQL Server, a database whose name holds them too. The connector's own
ATTACH (``details()["attach"]``, unmodified) signs in and reads a row: the value was read back
whole by the extension, so it added and changed no setting. The SQL Server and Firebird
containers are the test stack's (requires_sqlserver / requires_firebird).
"""

from __future__ import annotations

import os
import subprocess
import uuid

import duckdb
import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector_duckdb import DuckDBFirebirdConnector, DuckDBMssqlConnector

pytestmark = [pytest.mark.integration]

_SA_PASSWORD = "Provisa_2026!"
#: A SQL Server password meeting its policy and holding every ADO.NET separator.
_MSSQL_PASSWORD = "Pq1;Database=master;Encrypt=no}x{y}}z="
#: A database name holding them too (no ``]``, so it is written in brackets as is).
_MSSQL_DATABASE = "cg;Encrypt=no}x{=y"
#: A Firebird password holding every separator of the extension's URL.
_FIREBIRD_PASSWORD = "p:q@r/s?role=x&t%41+u;v'w"


def _source(**fields) -> Source:
    return Source(id=f"grammar_{uuid.uuid4().hex[:6]}", **fields)


def _sa_execute(statements: list[str]) -> None:
    import pyodbc

    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=localhost,{os.environ['SQLSERVER_PORT']};"
        f"UID=sa;PWD={_SA_PASSWORD};Encrypt=yes;TrustServerCertificate=yes",
        autocommit=True,
    )
    try:
        cur = conn.cursor()
        for statement in statements:
            cur.execute(statement)
    finally:
        conn.close()


@pytest.fixture
def mssql_login():
    login = f"grammar_{uuid.uuid4().hex[:8]}"
    password = _MSSQL_PASSWORD.replace("'", "''")
    _sa_execute(
        [
            f"IF DB_ID(N'{_MSSQL_DATABASE}') IS NOT NULL DROP DATABASE [{_MSSQL_DATABASE}]",
            f"CREATE DATABASE [{_MSSQL_DATABASE}]",
            f"CREATE LOGIN [{login}] WITH PASSWORD = N'{password}', CHECK_POLICY = OFF",
            f"USE [{_MSSQL_DATABASE}]; CREATE USER [{login}] FOR LOGIN [{login}]; "
            "CREATE TABLE dbo.probe (id INT); INSERT INTO dbo.probe VALUES (7); "
            f"GRANT SELECT ON dbo.probe TO [{login}]",
        ]
    )
    try:
        yield login
    finally:
        _sa_execute(
            [
                f"ALTER DATABASE [{_MSSQL_DATABASE}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
                f"DROP DATABASE [{_MSSQL_DATABASE}]",
                f"DROP LOGIN [{login}]",
            ]
        )


@pytest.mark.requires_sqlserver
def test_a_sql_server_password_and_database_holding_separators_sign_in(mssql_login):
    connector = DuckDBMssqlConnector("sqlserver")
    source = _source(
        type=SourceType.sqlserver,
        host="localhost",
        port=int(os.environ["SQLSERVER_PORT"]),
        database=_MSSQL_DATABASE,
        username=mssql_login,
        password=_MSSQL_PASSWORD,
        federation_hints={"trust_server_certificate": "true"},
    )
    details = connector.details(source)
    con = duckdb.connect()
    try:
        for statement in connector._bring_up():
            con.execute(statement)
        con.execute(details["attach"])
        rows = con.execute(f'SELECT id FROM "{details["raw_alias"]}".dbo.probe').fetchall()
    finally:
        con.close()
    assert rows == [(7,)]


def _firebird_isql_as_sysdba(container_id: str, db_path: str, script: str) -> None:
    # Through the container's server (localhost:), not embedded: the server holds the security
    # database, and an embedded isql cannot open it beside the server.
    subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            container_id,
            "/usr/local/firebird/bin/isql",
            "-user",
            "SYSDBA",
            "-password",
            "masterkey",
            f"localhost:{db_path}",
        ],  # fmt: skip
        input=script,
        capture_output=True,
        text=True,
        check=True,
    )


@pytest.mark.requires_firebird
def test_a_firebird_password_holding_url_separators_signs_in():
    from tests.integration.test_firebird_source_e2e import (
        _FB_DB_PATH_IN_CONTAINER,
        _FB_HOST,
        _FB_PORT,
        _ensure_firebird_client_lib,
        _firebird_container_id,
    )

    os.environ["DUCKDB_FIREBIRD_CLIENT_LIBRARY"] = _ensure_firebird_client_lib()
    container_id = _firebird_container_id()
    user = f"GRAMMAR{uuid.uuid4().hex[:6].upper()}"
    table = f"PROBE_{user}"
    password = _FIREBIRD_PASSWORD.replace("'", "''")
    _firebird_isql_as_sysdba(
        container_id,
        _FB_DB_PATH_IN_CONTAINER,
        f"CREATE USER {user} PASSWORD '{password}';\n"
        f"CREATE TABLE {table} (id INTEGER);\nCOMMIT;\n"
        f"INSERT INTO {table} VALUES (7);\nGRANT SELECT ON {table} TO USER {user};\nCOMMIT;\n",
    )
    try:
        connector = DuckDBFirebirdConnector()
        source = _source(
            type=SourceType.firebird,
            host=_FB_HOST,
            port=_FB_PORT,
            username=user,
            password=_FIREBIRD_PASSWORD,
            path=_FB_DB_PATH_IN_CONTAINER,
        )
        details = connector.details(source)
        con = duckdb.connect()
        try:
            for statement in connector._bring_up():
                con.execute(statement)
            con.execute(details["attach"])
            rows = con.execute(f'SELECT id FROM "{details["raw_alias"]}".{table}').fetchall()
        finally:
            con.close()
        assert [tuple(r) for r in rows] == [(7,)]
    finally:
        _firebird_isql_as_sysdba(
            container_id,
            _FB_DB_PATH_IN_CONTAINER,
            f"DROP TABLE {table};\nCOMMIT;\nDROP USER {user};\nCOMMIT;\n",
        )
