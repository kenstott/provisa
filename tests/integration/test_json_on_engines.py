# Copyright (c) 2026 Kenneth Stott
# Canary: 8f3d27c6-e14b-4a05-96bd-3a71c5f0d842
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's json functions on a real engine (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED): each
statement of tests/unit/pgwire/pg17_json_answers.py, written as Provisa writes it for the engine and
run there, its cells as pgwire would serve them, against PostgreSQL 17's recorded answer."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import Any

import pytest

from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile, transpile_to_trino
from tests.unit.pgwire.pg17_json_answers import RECORDED
from tests.unit.pgwire.pg17_json_matrix import MATRIX, SERVED, outcome, text_of

pytestmark = [pytest.mark.integration]


def _served(sql: str, rows: list[Any]) -> list[tuple]:
    casts = result_casts(sql) or ()

    def cell(index: int, value: Any) -> str | None:
        cast = casts[index] if index < len(casts) else None
        return text_of(cast.coerce(value) if cast is not None and cast.coerce else value)

    return [tuple(cell(i, value) for i, value in enumerate(row)) for row in rows]


def _replayed(written: Callable[[str], str], run: Callable[[str], list[Any]]) -> dict[str, str]:
    """Each statement's outcome, where it is not an answer PostgreSQL gave."""
    found: dict[str, str] = {}
    for sql, recorded in RECORDED.items():
        try:
            found[sql] = outcome(sql, recorded, _served(sql, run(written(sql))), False)
        except FunctionRefused:
            found[sql] = outcome(sql, recorded, None, True)
        except Exception:  # noqa: BLE001 -- any error of the engine's is this statement's outcome
            found[sql] = outcome(sql, recorded, None, False)
    return {sql: said for sql, said in found.items() if said != SERVED}


@pytest.mark.requires_trino
def test_json_on_trino(trino_conn):
    def run(sql: str) -> list[Any]:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    found = _replayed(transpile_to_trino, run)
    assert found == MATRIX["trino"], json.dumps({"trino": found})


@pytest.mark.requires_clickhouse
def test_json_on_clickhouse():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )

    def written(sql: str) -> str:
        return transpile(sql, "clickhouse")

    found = _replayed(written, lambda sql: list(client.query(sql).result_rows))
    assert found == MATRIX["clickhouse"], json.dumps({"clickhouse": found})
