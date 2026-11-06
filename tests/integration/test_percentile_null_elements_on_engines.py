# Copyright (c) 2026 Kenneth Stott
# Canary: b2d84f17-5c3e-4a96-8e01-7f6a9c2d4e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""percentile_cont and percentile_disc of an array holding a NULL fraction, on Trino and ClickHouse
(REQ-2171): the NULL answers NULL in its place and the others their percentiles, as PostgreSQL 17
answers. tests/unit/test_percentile_forms_on_engines.py proves the same on DuckDB."""

# Requirements: REQ-2171

from __future__ import annotations

import os

import pytest

from provisa.transpiler.transpile import transpile
from tests.unit.test_percentile_forms_on_engines import NO_KEYS, NULL_ELEMENTS, agrees

pytestmark = [pytest.mark.integration]


def _floats(cells: object) -> list[float | None]:
    return [None if cell is None else float(cell) for cell in cells]  # type: ignore[union-attr]


@pytest.mark.requires_trino
def test_trino_answers_a_null_element_in_its_place(trino_conn):
    wrong = {}
    for sql, want in NULL_ELEMENTS.items():
        cursor = trino_conn.cursor()
        cursor.execute(transpile(sql, "trino"))
        got = _floats(cursor.fetchall()[0][0])
        if not agrees(got, want):
            wrong[sql] = got
    assert not wrong, wrong


@pytest.mark.requires_trino
def test_trino_answers_null_over_no_keys(trino_conn):
    """No rows, or only NULL keys: NULL, as PostgreSQL answers, not Trino's refusal of the index 0
    the line between two keys reads over none (the profiler's quantiles of an empty column)."""
    got = {}
    for sql in NO_KEYS:
        cursor = trino_conn.cursor()
        cursor.execute(transpile(sql, "trino"))
        got[sql] = cursor.fetchall()[0][0]
    assert got == dict.fromkeys(NO_KEYS), got


@pytest.mark.requires_clickhouse
def test_clickhouse_answers_a_null_element_in_its_place():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    wrong = {}
    for sql, want in NULL_ELEMENTS.items():
        got = _floats(client.query(transpile(sql, "clickhouse")).result_rows[0][0])
        if not agrees(got, want):
            wrong[sql] = got
    assert not wrong, wrong
