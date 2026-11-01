# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2e9f14-a783-4d06-8b1e-e09d3c7a6f51
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's hash functions and aggregate state functions on an engine that is not DuckDB
(REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED): the statements of hash_corpus and aggregate_corpus,
written as Provisa writes them for the engine and run there, against PostgreSQL 17's recorded
answers. tests/unit/pgwire/test_pg_hash_forms.py proves the same on DuckDB."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import os
from collections import Counter
from collections.abc import Callable
from typing import Any

import pytest

from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.aggregate_corpus import index_statements, same_answer, statements
from tests.unit.pgwire.pg17_aggregate_answers import RECORDED as STATES
from tests.unit.pgwire.pg17_hash_answers import RECORDED as HASHES

pytestmark = [pytest.mark.integration]

RECORDABLE = frozenset((*statements(), *index_statements()))


def _function(sql: str) -> str:
    return sql.split("(")[0].removeprefix("SELECT ").strip('"').removeprefix('pg_catalog."')


#: The statements of each function, since one engine takes minutes over all of them.
_BY_FUNCTION: dict[str, list[str]] = {}
for _sql in HASHES:
    _BY_FUNCTION.setdefault(_function(_sql), []).append(_sql)


def _check_hashes(run: Callable[[str], Any], dialect: str, function: str) -> None:
    wrong = {}
    for sql in _BY_FUNCTION[function]:
        want = HASHES[sql][1]
        try:
            got = run(transpile(sql, dialect))
        except Exception as raised:  # noqa: BLE001 -- every statement is reported, not the first
            wrong[sql] = str(raised)[:300]
            continue
        if (None if got is None else str(got)) != want:
            wrong[sql] = got
    assert not wrong, _report(wrong)


def _report(wrong: dict[str, Any]) -> str:
    """How many statements of each function differ, and the first of them."""
    functions = Counter(sql.split("(")[0] for sql in wrong)
    return f"{len(wrong)} wrong {functions.most_common()} {list(wrong.items())[:8]}"


def _check_states(run: Callable[[str], Any], dialect: str) -> None:
    wrong = {}
    for sql, want in STATES.items():
        if sql not in RECORDABLE:
            continue
        try:
            got = run(transpile(sql, dialect))
        except Exception as raised:  # noqa: BLE001 -- the engine's refusal is the answer compared
            got = raised
        if not same_answer(sql, got, want):
            wrong[sql] = str(got)[:300]
    assert not wrong, _report(wrong)


@pytest.mark.requires_trino
@pytest.mark.parametrize("function", sorted(_BY_FUNCTION))
def test_hashes_on_trino(trino_conn, function: str):
    def run(sql: str) -> Any:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()[0][0]

    _check_hashes(run, "trino", function)


@pytest.mark.requires_trino
def test_state_and_index_functions_on_trino(trino_conn):
    def run(sql: str) -> Any:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()[0][0]

    _check_states(run, "trino")


def _clickhouse() -> Callable[[str], Any]:
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )

    return lambda sql: client.query(sql).result_rows[0][0]


@pytest.mark.requires_clickhouse
@pytest.mark.parametrize("function", sorted(_BY_FUNCTION))
def test_hashes_on_clickhouse(function: str):
    _check_hashes(_clickhouse(), "clickhouse", function)


@pytest.mark.requires_clickhouse
def test_state_and_index_functions_on_clickhouse():
    _check_states(_clickhouse(), "clickhouse")
