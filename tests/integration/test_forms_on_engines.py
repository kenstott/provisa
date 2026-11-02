# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7e91f4-8a35-4b60-9d12-e6f04b7a3c58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's numeric, string and date functions on a real engine (REQ-2171, A REWRITE IS A
SQLGLOT EXPANSION, AN ANSWER THAT DIFFERS IS REFUSED): each call of
tests/unit/pgwire/pg17_forms_answers.py, written as Provisa writes it for the engine and run there,
its cells as pgwire would serve them, against PostgreSQL 17's recorded answer. What an engine does
not serve is pg17_forms_matrix.py's, by statement: replayed, never derived."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import Any

import pytest

from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.pg_xml import XmlFunctionRefused
from provisa.transpiler.transpile import transpile, transpile_to_trino
from tests.unit.pgwire.pg17_forms_answers import RECORDED
from tests.unit.pgwire.pg17_forms_matrix import DIFFERS, ERROR, MATRIX, REFUSED, compared, text_of

pytestmark = [pytest.mark.integration]


#: What each statement that is not served answered, for a failure to say.
_SAID: dict[str, str] = {}


def _served(sql: str, rows: list[Any], oids: list[int]) -> list[tuple]:
    casts = result_casts(sql) or ()

    def cell(index: int, value: Any) -> str | None:
        cast = casts[index] if index < len(casts) else None
        return text_of(
            cast.coerce(value) if cast is not None and cast.coerce else value, oids[index]
        )

    return [tuple(cell(i, value) for i, value in enumerate(row)) for row in rows]


def replayed(written: Callable[[str], str], run: Callable[[str], list[Any]]) -> dict[str, str]:
    """Each statement's outcome, where it is not the answer PostgreSQL gave."""
    found: dict[str, str] = {}
    _SAID.clear()
    for sql, (oids, want, values) in RECORDED.items():
        try:
            rows = run(written(sql))
        except (FunctionRefused, XmlFunctionRefused):
            found[sql] = REFUSED
        except Exception as error:  # noqa: BLE001 -- any error of the engine's is its outcome
            found[sql] = ERROR
            _SAID[sql] = str(error)[:200]
        else:
            served = _served(sql, rows, oids)
            if not compared(oids, want, served, values, sql):
                found[sql] = DIFFERS
                _SAID[sql] = f"{served[:2]}, PostgreSQL {want[:2]}"
    return found


def trino_replay(conn: Any) -> dict[str, str]:
    def run(sql: str) -> list[Any]:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    return replayed(transpile_to_trino, run)


def clickhouse_replay(client: Any) -> dict[str, str]:
    def written(sql: str) -> str:
        return transpile(sql, "clickhouse")

    return replayed(written, lambda sql: list(client.query(sql).result_rows))


@pytest.mark.requires_trino
def test_forms_on_trino(trino_conn):
    found = trino_replay(trino_conn)
    assert found == MATRIX["trino"], json.dumps({"trino": found, "said": _SAID})


@pytest.mark.requires_clickhouse
def test_forms_on_clickhouse():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    found = clickhouse_replay(client)
    assert found == MATRIX["clickhouse"], json.dumps({"clickhouse": found, "said": _SAID})
