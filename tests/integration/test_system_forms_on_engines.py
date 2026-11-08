# Copyright (c) 2026 Kenneth Stott
# Canary: 90b3f6c1-2e47-4da8-85c6-f1a07d3e29b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pg_forms_system on an engine that is not DuckDB (REQ-2171): each statement of system_corpus as
Provisa writes it for the engine, run there, against postgres:17.11's recorded answers.
tests/unit/pgwire/test_pg_system_forms.py proves the same on DuckDB."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.pg_forms_enum import NOT_SUPPORTED
from provisa.transpiler.pg_forms_types import REFUSED
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.pg17_system_answers import RECORDED

pytestmark = [pytest.mark.integration]


def _agrees(got: Any, want: tuple[Any, Any, Any], function_refused: bool) -> bool:
    if isinstance(got, Exception):
        error = got if isinstance(got, PostgresError) else from_engine(got)
        if error is None:
            return False
        said = want[0] == "err" and (error.sqlstate, str(error)) == (want[1], want[2])
        return said or (function_refused and str(error).endswith(REFUSED))
    return want[0] == "ok" and want[2] == [None if cell is None else str(cell) for cell in got]


def _refused_by_decision(got: Any, sql: str, dialect: str) -> bool:
    """REQ-2167 (amended 2026-11-03, Privileges, visibility and notifications): what a decision does
    not support, pg_notify among it, is refused by name on every engine whatever PostgreSQL
    answered, as test_pg_system_forms holds on DuckDB."""
    named = [name for name in NOT_SUPPORTED if f'"{name}"(' in sql]
    refused = isinstance(got, FunctionRefused)
    return bool(named) and refused and str(got) == f"{named[0]}() has no form on {dialect}"


def _check(run: Callable[[str], Any], dialect: str, written_from_a_value: bool) -> None:
    wrong = {}
    for sql, want in RECORDED.items():
        try:
            got = run(transpile(sql, dialect))
        except Exception as raised:  # noqa: BLE001 -- the engine's refusal is the answer compared
            got = raised
        refused = written_from_a_value and "regexp_replace" in sql and want[0] == "err"
        if not (_refused_by_decision(got, sql, dialect) or _agrees(got, want, refused)):
            wrong[sql] = str(got)[:300]
    assert not wrong, f"{len(wrong)} wrong {list(wrong.items())[:8]}"


@pytest.mark.requires_trino
def test_system_forms_on_trino(trino_conn):
    def run(sql: str) -> Any:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall()]

    _check(run, "trino", False)


@pytest.mark.requires_clickhouse
def test_system_forms_on_clickhouse():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    # A value PostgreSQL's message is written from is refused there: throwIf's is a constant.
    _check(lambda sql: [row[0] for row in client.query(sql).result_rows], "clickhouse", True)
