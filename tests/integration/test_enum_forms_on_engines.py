# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1e8b3a-92f4-47d0-a3e5-0d7f24b6c918
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The enum functions over a source's labels on Trino and on ClickHouse's own server (REQ-2171),
against postgres:17.11's recorded answers. Neither engine holds the type: the statement carries the
labels, as provisa.transpiler.pg_forms_enum writes them for every engine. DuckDB and chdb replay
the same statements in tests/unit/pgwire/test_pg_enum_forms.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import pytest

from provisa.core.pg_enum import array_text
from provisa.transpiler.pg_forms_enum import with_source_enums
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.pg17_everyday_function_answers import RECORDED
from tests.unit.pgwire.test_pg_enum_forms import ENUM_STATEMENTS, ENUMS

pytestmark = [pytest.mark.integration]


def _shown(value: Any) -> str | None:
    return array_text(list(value)) if isinstance(value, (list, tuple)) else value


def _check(run: Callable[[str], list[Any]], dialect: str) -> None:
    wrong = {}
    for sql in ENUM_STATEMENTS:
        written = transpile(with_source_enums(sql, ENUMS), dialect)
        try:
            got = [_shown(row[-1]) for row in run(written)]
        except Exception as raised:  # noqa: BLE001 -- the engine's refusal is the difference shown
            got = str(raised)[:300]
        if got != RECORDED[sql][2]:
            wrong[sql] = f"{got} of {written}"
    assert not wrong


@pytest.mark.requires_trino
def test_enum_forms_on_trino(trino_conn):
    def run(sql: str) -> list[Any]:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    _check(run, "trino")


@pytest.mark.requires_clickhouse
def test_enum_forms_on_clickhouse():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    _check(lambda sql: list(client.query(sql).result_rows), "clickhouse")
