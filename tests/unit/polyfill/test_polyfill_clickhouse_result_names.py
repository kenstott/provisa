# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1e8b47-2a93-4c6f-b071-9e4a3c8d2f15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two result columns of one name on ClickHouse (REQ-2171): PostgreSQL answers them (both
``j -> 1`` and ``j -> '1'`` are ?column?), ClickHouse refuses two expressions of one alias
(MULTIPLE_EXPRESSIONS_FOR_ALIAS, on 24.3's analyzer and 26's alike) and reads a column of an
alias's name as that alias. Each statement is answered on the embedded engine (chdb) as
PostgreSQL answers it."""

# Requirements: REQ-2171

from __future__ import annotations

import chdb
import pytest
import sqlglot

from provisa.transpiler.polyfill.clickhouse import distinct_result_names
from provisa.transpiler.transpile import transpile


def _rows(sql: str) -> list[list[str]]:
    written = transpile(sql, "clickhouse", generated=False)
    answered = chdb.query(written, "TabSeparated").bytes().decode()
    return [line.split("\t") for line in answered.splitlines()]


@pytest.mark.parametrize(
    ("sql", "rows"),
    [
        # PostgreSQL 17's recorded answer (tests/unit/pgwire/pg17_json_answers.py).
        (
            "SELECT '{\"1\":2}'::jsonb -> i::int, '{\"1\":2}'::jsonb -> i::text "
            "FROM (VALUES (1)) t(i)",
            [["\\N", "2"]],
        ),
        ("SELECT 1 AS a, 2 AS a", [["1", "2"]]),
        ("SELECT a, b AS a FROM (VALUES (1, 2)) t(a, b)", [["1", "2"]]),
        ("SELECT b AS a, a FROM (VALUES (1, 2)) t(a, b)", [["2", "1"]]),
        ("SELECT 1 AS a, 2 AS a, 3 AS a, 4 AS a_1", [["1", "2", "3", "4"]]),
    ],
)
def test_columns_of_one_name_answer_as_postgres(sql, rows):
    assert _rows(sql) == rows


def test_a_name_held_once_is_kept():
    tree = distinct_result_names(sqlglot.parse_one("SELECT 1 AS a, 2 AS b, c FROM t"))
    assert [item.alias_or_name for item in tree.selects] == ["a", "b", "c"]


def test_the_first_of_aliases_keeps_the_name_and_no_new_name_is_taken():
    tree = distinct_result_names(sqlglot.parse_one("SELECT 1 AS a, 2 AS a, 3 AS a_1"))
    assert [item.alias_or_name for item in tree.selects] == ["a", "a_2", "a_1"]
