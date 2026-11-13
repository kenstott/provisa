# Copyright (c) 2026 Kenneth Stott
# Canary: 83b6d0f2-e917-4a35-b6c8-1f4a7e2d9c30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The everyday pg_catalog functions on an engine that is not DuckDB (REQ-2171): each statement of
tests/unit/pgwire/everyday_function_statements.py, written as Provisa writes it for the engine and
run there, against PostgreSQL 17's recorded answers. A call is answered as PostgreSQL answers it or
refused by name: never another value."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import os
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.pg_forms_types import REFUSED
from provisa.transpiler.transpile import transpile
from tests.integration.every_case import failures_of_every_case
from tests.unit.pgwire.everyday_function_statements import STATEMENTS
from tests.unit.pgwire.pg17_everyday_function_answers import RECORDED

pytestmark = [pytest.mark.integration]

#: What a statement holds that a dialect has no value of. ClickHouse has no NULL of an array.
_NO_VALUE = {"clickhouse": ("NULL::integer[]",)}


def _text(value: Any) -> str | None:
    """An engine's value in PostgreSQL's text form, to compare with a recorded answer."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (list, tuple)):
        return "{" + ",".join("NULL" if v is None else str(_text(v)) for v in value) + "}"
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _problem(run: Callable[[str], list[Any]], dialect: str, sql: str) -> str | None:
    kind, _, want = RECORDED[sql]
    try:
        written = transpile(sql, dialect)
    except FunctionRefused:
        return "refused"
    except PostgresError as error:
        said = (error.sqlstate, str(error))
        return None if kind == "err" and said == RECORDED[sql][1:] else f"{said}"
    try:
        rows = run(written)
    except Exception as error:
        postgres = from_engine(error)
        if postgres is None:
            return f"{written}: {str(error)[:300]}"
        said = (postgres.sqlstate, str(postgres))
        if said[1].endswith(REFUSED):
            return "refused"
        return None if kind == "err" and said == RECORDED[sql][1:] else f"{said} of {written}"
    if kind == "err":
        return f"answered {rows} of {written}, PostgreSQL {want}"
    got = [_text(row[-1]) for row in rows]
    return None if got == want else f"{got} of {written}, PostgreSQL {want}"


def _check(run: Callable[[str], list[Any]], dialect: str, family: str) -> None:
    served, refused, problems = 0, [], {}
    for sql in STATEMENTS[family]:
        if any(value in sql for value in _NO_VALUE.get(dialect, ())):
            continue
        problem = _problem(run, dialect, sql)
        if problem == "refused":
            refused.append(sql)
        elif problem is None:
            served += 1
        else:
            problems[sql] = problem
    print(f"{family} on {dialect}: {served} served, {len(refused)} refused by name")
    assert not problems


@pytest.mark.requires_trino
@pytest.mark.parametrize("family", STATEMENTS)
def test_everyday_functions_on_trino(trino_conn, family):
    def run(sql: str) -> list[Any]:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    _check(run, "trino", family)


@pytest.mark.requires_clickhouse
def test_everyday_functions_on_clickhouse():
    """One test over every family: requires_clickhouse boots the engine for each test
    (tests/integration/every_case.py)."""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )

    def run(sql: str) -> list[Any]:
        return list(client.query(sql).result_rows)

    assert (
        failures_of_every_case(STATEMENTS, lambda family: _check(run, "clickhouse", family)) == {}
    )
