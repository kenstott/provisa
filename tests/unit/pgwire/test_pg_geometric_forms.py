# Copyright (c) 2026 Kenneth Stott
# Canary: d2c6f8a1-4e07-4b39-91d6-58a3e7b0c214
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The geometric forms on DuckDB against what PostgreSQL 17.11 answered (REQ-2171, AN ANSWER THAT
DIFFERS IS REFUSED). Trino and ClickHouse replay them in tests/integration/test_geometric_on_engines.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from typing import Any

import duckdb
import pytest

from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.geometric_corpus import statements
from tests.unit.pgwire.pg17_geometric_answers import RECORDED


def _text(value: Any) -> str | None:
    if isinstance(value, bool):
        return "t" if value else "f"
    return None if value is None else str(value)


def test_the_corpus_is_what_was_recorded():
    assert list(RECORDED) == statements()


def test_duckdb_answers_as_postgresql_17_answered():
    connection = duckdb.connect()
    wrong = {}
    for sql, (_, want) in RECORDED.items():
        try:
            got = _text(connection.execute(transpile(sql, "duckdb")).fetchone()[0])
        except duckdb.Error as raised:
            state = (want or "ERROR ?").split()[-1]
            got = want if f"{state}:" in str(raised) else f"ERROR {raised}"[:120]
        if got != want:
            wrong[sql] = (got, want)
    assert not wrong, f"{len(wrong)} of {len(RECORDED)} {list(wrong.items())[:10]}"


@pytest.mark.parametrize(
    "sql, want",
    [
        ("SELECT point_add('(1,2)'::point, '(0.1,0.2)'::point)", "(1.1,2.2)"),
        ("SELECT npoints('((0,0),(1,0),(1,1))'::polygon)", 3),
        ("SELECT point_eq(NULL::point, '(1,1)'::point)", None),
    ],
)
def test_a_form_writes_postgresqls_text(sql: str, want: Any):
    assert duckdb.connect().execute(transpile(sql, "duckdb")).fetchone()[0] == want


def test_a_shape_with_an_infinity_is_refused_by_name():
    with pytest.raises(duckdb.Error, match="point_add does not read this value"):
        duckdb.connect().execute(
            transpile("SELECT point_add('(inf,1)'::point, '(1,1)'::point)", "duckdb")
        )
