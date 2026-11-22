# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1c8e43-2a9d-4b70-9e15-d3b7a0c42f86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a NULL result of a typed expression on a URL-driven engine is described over
pgwire with the PostgreSQL type the statement declares (REQ-2167). The statement is written for the
engine, run by the engine's own runtime (SqlAlchemyFederationRuntime) on a real MariaDB (the mysql
dialect) and SQL Server, and its rows described by pgwire's result. These engines give their rows
no column types; a column holding NULL alone was described as text (oid 25): the polyfill triage
of 2026-11-22 (group G2) counted 547 such wrong answers on mysql, tidb, oracle and mssql."""

# Requirements: REQ-2167

from __future__ import annotations

import os

import pytest

from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime
from provisa.pgwire.server import ProvisaQueryResult
from provisa.transpiler.transpile import transpile

pytestmark = [pytest.mark.integration]

_INT4, _INT8, _FLOAT8, _DATE = 23, 20, 701, 1082

#: Each statement and the type PostgreSQL 17 describes its NULL result with.
_STATEMENTS = {
    "SELECT NULL::int4 AS v": _INT4,
    "SELECT NULL::date AS v": _DATE,
    'SELECT pg_catalog."abs"(NULL::bigint) AS v': _INT8,
    'SELECT pg_catalog."acos"(NULL::double precision) AS v': _FLOAT8,
}
#: A function returning date, where the dialect has one of the same name.
_DATE_CALL = {'SELECT pg_catalog."date"(NULL::timestamp without time zone) AS v': _DATE}


def _described(runtime: SqlAlchemyFederationRuntime, dialect: str, sql: str) -> int | None:
    stream = runtime.run_sync(transpile(sql, dialect))
    result = ProvisaQueryResult(stream, sql)
    try:
        return result.column_type_oid(0)
    finally:
        result.close()


def _check(url: str, dialect: str, statements: dict[str, int]) -> None:
    runtime = SqlAlchemyFederationRuntime(url=url)
    try:
        got = {sql: _described(runtime, dialect, sql) for sql in statements}
    finally:
        runtime.close()
    assert got == statements


@pytest.mark.requires_mariadb
def test_null_results_on_mysql_are_described_as_declared():
    url = f"mysql+pymysql://root:provisa@localhost:{os.environ['MARIADB_PORT']}/provisa"
    _check(url, "mysql", {**_STATEMENTS, **_DATE_CALL})


@pytest.mark.requires_sqlserver
def test_null_results_on_mssql_are_described_as_declared():
    url = (
        f"mssql+pyodbc://sa:Provisa_2026%21@localhost:{os.environ['SQLSERVER_PORT']}/master"
        "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes"
    )
    _check(url, "tsql", _STATEMENTS)
