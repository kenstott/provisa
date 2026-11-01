# Copyright (c) 2026 Kenneth Stott
# Canary: 0f7a2c58-e419-4d63-b8a6-72d51e93c4af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The type operators family on the engines DuckDB's replay does not reach (REQ-2171, AN ANSWER THAT
DIFFERS IS REFUSED): each statement of type_io_corpus as Provisa writes it for the engine, run there,
against postgres:17.11's recorded answer. tests/unit/pgwire/test_pg_type_io_forms.py proves the same
on DuckDB."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import pytest

from provisa.core.pg_error import from_engine
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.pg17_type_io_answers import RECORDED
from tests.unit.pgwire.type_io_corpus import (
    REFUSALS,
    REFUSED,
    by_function,
    verdict,
)

pytestmark = [pytest.mark.integration]

_FUNCTIONS = by_function(RECORDED)
_BYTEA = 17


def _difference(run: Callable[[str, int | None], Any], dialect: str, sql: str) -> str | None:
    try:
        got = run(transpile(sql, dialect), RECORDED[sql][0])
    except Exception as raised:  # noqa: BLE001 -- every statement is reported, not the first
        refusal = from_engine(raised)
        if refusal is None:
            return f"not raised as PostgreSQL's: {str(raised)[:300]}"
        return verdict(sql, RECORDED[sql], None, f"{refusal.sqlstate}: {refusal}")
    return verdict(sql, RECORDED[sql], got, None)


def _check(run: Callable[[str, int | None], Any], dialect: str, function: str) -> None:
    found = {sql: _difference(run, dialect, sql) for sql in _FUNCTIONS[function]}
    assert {s: why for s, why in found.items() if why not in (None, REFUSED)} == {}
    assert sum(why == REFUSED for why in found.values()) <= REFUSALS.get(function, 0)


@pytest.mark.requires_trino
@pytest.mark.parametrize("function", sorted(_FUNCTIONS))
def test_trino_answers_as_postgresql_17_answered(trino_conn, function: str):
    def run(sql: str, _: int | None) -> Any:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()[0][0]

    _check(run, "trino", function)


@pytest.mark.requires_clickhouse
@pytest.mark.parametrize("function", sorted(_FUNCTIONS))
def test_clickhouse_answers_as_postgresql_17_answered(function: str):
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )

    def run(sql: str, oid: int | None) -> Any:
        if oid != _BYTEA:
            return client.query(sql).result_rows[0][0]
        # ClickHouse has no bytes but a String, which a text reader decodes.
        digits = client.query(f"SELECT hex(v) FROM ({sql})").result_rows[0][0]
        return None if digits is None else bytes.fromhex(digits)

    _check(run, "clickhouse", function)
