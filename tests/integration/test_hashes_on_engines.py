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
from collections.abc import Callable
from typing import Any

import pytest

from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.aggregate_corpus import same_answer
from tests.unit.pgwire.pg17_aggregate_answers import RECORDED as STATES
from tests.unit.pgwire.pg17_hash_answers import RECORDED as HASHES

pytestmark = [pytest.mark.integration]


def _check_hashes(run: Callable[[str], Any], dialect: str) -> None:
    wrong = {}
    for sql, (_, want) in HASHES.items():
        got = run(transpile(sql, dialect))
        if (None if got is None else str(got)) != want:
            wrong[sql] = got
    assert not wrong


def _check_states(run: Callable[[str], Any], dialect: str) -> None:
    wrong = {}
    for sql, want in STATES.items():
        try:
            got = run(transpile(sql, dialect))
        except Exception as raised:  # noqa: BLE001 -- the engine's refusal is the answer compared
            got = raised
        if not same_answer(got, want):
            wrong[sql] = got
    assert not wrong


@pytest.mark.requires_trino
def test_hashes_on_trino(trino_conn):
    def run(sql: str) -> Any:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()[0][0]

    _check_hashes(run, "trino")
    _check_states(run, "trino")


@pytest.mark.requires_clickhouse
def test_hashes_on_clickhouse():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )

    def run(sql: str) -> Any:
        return client.query(sql).result_rows[0][0]

    _check_hashes(run, "clickhouse")
    _check_states(run, "clickhouse")
