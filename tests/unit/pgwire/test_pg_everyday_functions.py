# Copyright (c) 2026 Kenneth Stott
# Canary: 15f0c3d8-a26e-4b71-9d40-e8b57c1a3f96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The everyday pg_catalog functions (REQ-2171): each statement run as a client runs it, over pgwire
on the default engine, and answered as PostgreSQL 17.11 answered it, by type, value and error."""

# Requirements: REQ-2167, REQ-2171
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from provisa.core.pg_enum import SourceEnum
from provisa.transpiler.pg_forms_types import REFUSED
from tests.unit.pgwire.everyday_function_statements import PROBES, STATEMENTS
from tests.unit.pgwire.pg17_everyday_function_answers import RECORDED
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401


#: The enum of SETUP, under the numbers the recording's database gave it.
_ENUM = SourceEnum("conformance", "probe_enum", 16412, 16411, ("a", "b", "c"))


def _rows(res: Any) -> list[Any]:
    return [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]


def _message(res: Any) -> str:
    return (res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b"").decode()


def _problem(conn: Any, sql: str) -> str | None:
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    failed = res.status == pq.ExecStatus.FATAL_ERROR
    if failed and _message(res).endswith(REFUSED):
        return None
    kind, first, second = RECORDED[sql]
    if kind == "err":
        if not failed:
            return f"answered {_rows(res)}, PostgreSQL {first} {second}"
        said = ((res.error_field(pq.DiagnosticField.SQLSTATE) or b"").decode(), _message(res))
        return None if said == (first, second) else f"{said}, PostgreSQL {first} {second}"
    if failed:
        return (res.error_message or b"").decode()[:300]
    if res.ftype(0) != first:
        return f"type {res.ftype(0)} rows {_rows(res)}, PostgreSQL {first} {second}"
    return None if _rows(res) == second else f"{_rows(res)}, PostgreSQL {second}"


def _source_enum() -> None:
    """The enum a source declares, in the catalog DuckDB attaches the source under."""
    from provisa.api.app import state

    state.pg_source_enums = {(_ENUM.schema, _ENUM.name): _ENUM}
    runtime = state.federation_engine._rt
    connection = runtime._con
    connection.execute("ATTACH IF NOT EXISTS ':memory:' AS \"_src_probe\"")
    connection.execute('CREATE SCHEMA IF NOT EXISTS "_src_probe".conformance')
    connection.execute(
        "CREATE TYPE IF NOT EXISTS \"_src_probe\".conformance.probe_enum AS ENUM ('a', 'b', 'c')"
    )


@pytest.mark.parametrize("family", STATEMENTS)
def test_every_statement_answers_as_postgresql_17_answered(conn: Any, family: str):
    _source_enum()
    problems = {}
    for sql in (*STATEMENTS[family], *PROBES[family]):
        problem = _problem(conn, sql)
        if problem is not None:
            problems[sql] = problem
    assert not problems
