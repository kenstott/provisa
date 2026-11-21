# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7d9e21-5a4c-4f68-9c10-7e2a8d5b6f43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""EXTRACT(YEAR) and date_part('year') of a column on Trino answer PostgreSQL's year before Christ."""

import pytest

from provisa.transpiler.transpile import transpile

_YEAR = "EXTRACT(YEAR FROM d)"
_CORRECTED = f"CASE WHEN {_YEAR} <= 0 THEN {_YEAR} - 1 ELSE {_YEAR} END"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT EXTRACT(YEAR FROM d) AS v FROM t",
        "SELECT date_part('year', d) AS v FROM t",
    ],
)
def test_year_of_a_column_counts_bc_as_postgres(sql: str) -> None:
    assert transpile(sql, "trino") == f"SELECT {_CORRECTED} AS v FROM t"


def test_other_parts_unchanged() -> None:
    assert (
        transpile("SELECT EXTRACT(MONTH FROM d) AS v FROM t", "trino")
        == "SELECT EXTRACT(MONTH FROM d) AS v FROM t"
    )


def test_other_engines_unchanged() -> None:
    assert "CASE" not in transpile("SELECT EXTRACT(YEAR FROM d) AS v FROM t", "duckdb")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT EXTRACT(YEAR FROM DATE '4713-01-01') AS v",
        "SELECT date_part('year', DATE '4713-01-01') AS v",
        "SELECT EXTRACT(YEAR FROM d) AS v FROM t",
        "SELECT date_part('year', d) AS v FROM t",
    ],
)
def test_year_is_corrected_exactly_once(sql: str) -> None:
    assert transpile(sql, "trino").count("<= 0") == 1
