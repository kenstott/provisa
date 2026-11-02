# Copyright (c) 2026 Kenneth Stott
# Canary: f03a8e65-c1d7-4b29-a6e4-19d7b5c2e086
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The text search functions on Trino and ClickHouse against what postgres:17.11 answered (REQ-2167,
REQ-2171): each statement of the corpus, as Provisa writes it for the engine, run there. DuckDB's
replay is tests/unit/pgwire/test_pg_text_search_forms.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import os
from typing import Any

import pytest

from tests.unit.pgwire.text_search_replay import BY_FUNCTION, problems

pytestmark = [pytest.mark.integration]


def _check(function: str, dialect: str, run: Any) -> None:
    wrong, refused = problems(function, dialect, run)
    print("REFUSED", dialect, function, refused)  # noqa: T201 -- the run's record
    assert not wrong, f"{len(wrong)} differ on {dialect}: " + "\n".join(wrong[:30])


@pytest.mark.requires_trino
@pytest.mark.parametrize("function", sorted(BY_FUNCTION))
def test_text_search_on_trino(trino_conn, function: str):
    def run(sql: str) -> list[Any]:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    _check(function, "trino", run)


@pytest.mark.requires_clickhouse
@pytest.mark.parametrize("function", sorted(BY_FUNCTION))
def test_text_search_on_clickhouse(function: str):
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    _check(function, "clickhouse", lambda sql: list(client.query(sql).result_rows))
