# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e5f1a-4c2d-4e9b-a7f6-0d1c9e2b8a54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: closing a DuckDB connection that attached a SQL Server source signs its sessions out.

The DuckDB mssql extension keeps its connection pools in a process-wide registry that only a
DETACH empties (``MSSQLCatalog::OnDetach`` -> ``RemovePool``): a connection closed without one
left its pool's sessions signed in to the server for the life of the process, so every runtime
rebuild and every scratch read of a SQL Server source added sessions (R511-S6A). After the
federation runtime's close and after a scratch read, the server holds no session in the source's
database. The SQL Server container is the test stack's (requires_sqlserver).
"""

from __future__ import annotations

import os
import time
import uuid
from types import SimpleNamespace

import pytest

from provisa.core.models import Source, SourceType
from provisa.events.source_loader import _duckdb_extension_scratch_read
from provisa.federation.connector_duckdb import DuckDBMssqlConnector
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

pytestmark = [pytest.mark.integration, pytest.mark.requires_sqlserver]

_SA_PASSWORD = "Provisa_2026!"
_PASSWORD = "Detach_Leak_2026!"
#: The server processes a logout after its client closed the socket: the bound on that wait.
_LOGOUT_SECONDS = 10.0


def _sa_connect():
    import pyodbc

    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=localhost,{os.environ['SQLSERVER_PORT']};"
        f"UID=sa;PWD={_SA_PASSWORD};Encrypt=yes;TrustServerCertificate=yes",
        autocommit=True,
    )


def _sa_execute(statements: list[str]) -> None:
    conn = _sa_connect()
    try:
        cur = conn.cursor()
        for statement in statements:
            cur.execute(statement)
            while cur.nextset():  # drain a batch's further results before the next statement
                pass
    finally:
        conn.close()


def _sessions_in(database: str) -> list[tuple]:
    conn = _sa_connect()
    try:
        rows = (
            conn.cursor()
            .execute(
                "SELECT session_id, login_name, program_name, status FROM sys.dm_exec_sessions "
                "WHERE database_id = DB_ID(?)",
                database,
            )
            .fetchall()
        )
        return [tuple(row) for row in rows]
    finally:
        conn.close()


def _sessions_after_logouts(database: str) -> list[tuple]:
    """The sessions still in ``database`` once the server has had ``_LOGOUT_SECONDS`` to process
    the logouts of sockets just closed; an empty list as soon as it holds none."""
    deadline = time.monotonic() + _LOGOUT_SECONDS
    sessions = _sessions_in(database)
    while sessions and time.monotonic() < deadline:
        time.sleep(0.2)
        sessions = _sessions_in(database)
    return sessions


@pytest.fixture
def mssql_database():
    """A database of its own with a one-row table, and a login reading it: (database, login)."""
    suffix = uuid.uuid4().hex[:8]
    database, login = f"detach_leak_{suffix}", f"detach_leak_{suffix}"
    _sa_execute(
        [
            f"CREATE DATABASE [{database}]",
            f"CREATE LOGIN [{login}] WITH PASSWORD = N'{_PASSWORD}', CHECK_POLICY = OFF",
            f"USE [{database}]; CREATE USER [{login}] FOR LOGIN [{login}]; "
            "CREATE TABLE dbo.probe (id INT); INSERT INTO dbo.probe VALUES (7); "
            f"GRANT SELECT ON dbo.probe TO [{login}]",
        ]
    )
    try:
        yield database, login
    finally:
        _sa_execute(
            [
                f"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                f"DROP DATABASE [{database}]",
                f"DROP LOGIN [{login}]",
            ]
        )


def _source(database: str, login: str) -> Source:
    return Source(
        id=f"leak_{uuid.uuid4().hex[:6]}",
        type=SourceType.sqlserver,
        host="localhost",
        port=int(os.environ["SQLSERVER_PORT"]),
        database=database,
        username=login,
        password=_PASSWORD,
        federation_hints={"trust_server_certificate": "true"},
    )


def test_a_runtime_close_leaves_no_session_in_the_source_database(mssql_database):
    database, login = mssql_database
    source = _source(database, login)
    table = SimpleNamespace(
        id=source.id,
        type=source.type,
        catalog=source.id,
        host=source.host,
        port=source.port,
        database=source.database,
        username=source.username,
        password=source.password,
        federation_hints=source.federation_hints,
        mapping={},
        max_live_concurrency=None,
        schema_name="dbo",
        table_name="probe",
    )
    runtime = DuckDBFederationRuntime()
    try:
        runtime.attach_source(table)
        runtime._on_instance(f'SELECT id FROM "{source.id}"."dbo"."probe"')
        assert _sessions_in(database), "the attach signed no session in"
    finally:
        runtime.close()

    assert _sessions_after_logouts(database) == []


def test_a_scratch_read_leaves_no_session_in_the_source_database(mssql_database):
    database, login = mssql_database
    source = _source(database, login)
    connector = DuckDBMssqlConnector("sqlserver")
    details = {
        **connector.details(source),
        "extension": connector.extension,
        "install_from_community": connector.install_from_community,
    }

    rows = _duckdb_extension_scratch_read(
        details, f'SELECT id FROM "{details["raw_alias"]}"."dbo"."probe"'
    )

    assert rows == [{"id": 7}]
    assert _sessions_after_logouts(database) == []
