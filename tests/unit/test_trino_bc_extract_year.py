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
import sqlglot

from provisa.transpiler.transpile import transpile

_YEAR = "YEAR(d)"
_CORRECTED = f"CASE WHEN {_YEAR} <= 0 THEN {_YEAR} - 1 ELSE {_YEAR} END"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT EXTRACT(YEAR FROM d) AS v FROM t",
        "SELECT date_part('year', d) AS v FROM t",
    ],
)
def test_year_of_a_column_counts_bc_as_postgres(sql: str) -> None:
    assert transpile(sql, "trino", generated=False) == f"SELECT {_CORRECTED} AS v FROM t"


def test_other_parts_unchanged() -> None:
    assert (
        transpile("SELECT EXTRACT(MONTH FROM d) AS v FROM t", "trino", generated=False)
        == "SELECT EXTRACT(MONTH FROM d) AS v FROM t"
    )


def test_other_engines_unchanged() -> None:
    assert "CASE" not in transpile(
        "SELECT EXTRACT(YEAR FROM d) AS v FROM t", "duckdb", generated=False
    )


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
    assert transpile(sql, "trino", generated=False).count("<= 0") == 1


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT EXTRACT(YEAR FROM d) AS v FROM t",
        "SELECT date_part('year', d) AS v FROM t",
        """SELECT pg_catalog."date_part"('year'::text, '4713-01-01 BC'::date) AS v""",
        """SELECT pg_catalog."extract"('year'::text, '4713-01-01 00:00:00 BC'::timestamp) AS v""",
        """SELECT pg_catalog."date_part"('year'::text, '4713-01-01 00:00:00+00 BC'::timestamptz) AS v""",
    ],
)
def test_regenerated_sql_is_not_corrected_again(sql: str) -> None:
    """The pipeline parses and generates Trino SQL more than once; run 44952615830 read -4716 for
    4713 BC after four corrections. Generating the corrected SQL again leaves it as it is."""
    once = transpile(sql, "trino", generated=False)
    again = once
    for _ in range(4):
        again = sqlglot.transpile(again, read="trino", write="trino")[0]
    assert again == once
    assert again.count("<= 0") == 1


@pytest.mark.parametrize(
    "value",
    [
        "'4713-01-01 BC'::date",
        "'0001-01-01 BC'::date",
        "'0001-01-01'::date",
        "'2024-06-15 12:30:45'::timestamp",
        "'4713-01-01 00:00:00+00 BC'::timestamptz",
    ],
)
def test_year_of_a_literal_is_corrected_once(value: str) -> None:
    out = transpile(f"SELECT date_part('year', {value}) AS v", "trino", generated=False)
    assert out.count("<= 0") == 1
    assert "EXTRACT(YEAR" not in out


@pytest.mark.parametrize("part", ["month", "day", "quarter", "hour", "minute", "dow", "doy"])
def test_parts_other_than_year_are_not_corrected(part: str) -> None:
    out = transpile(f"SELECT EXTRACT({part.upper()} FROM d) AS v FROM t", "trino", generated=False)
    assert "<= 0" not in out
