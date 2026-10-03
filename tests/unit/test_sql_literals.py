# Copyright (c) 2026 Kenneth Stott
# Canary: f007db22-c9f2-43a7-8c4b-78d9bd4e1697
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The one rule for writing a value into SQL text: each dialect's own escaping.

A value that tries to end its literal and add SQL is data in every dialect, the ones that read a
backslash as an escape included. Proven by running the literal on real engines in this process
(DuckDB, ClickHouse through chdb) and by parsing it back in the others' grammars; a value a
dialect cannot hold is refused by name."""

from __future__ import annotations

import datetime as dt
import math
import uuid
from decimal import Decimal

import pytest

from provisa.compiler.sql_literals import UnencodableLiteral, sql_literal

HOSTILE = [
    "x') OR 1=1 --",
    "x\\') OR 1=1 --",
    "x\\' OR 1=1 --",
    "back\\slash",
    "trailing\\",
    "it's",
    "line\nbreak",
    "tab\there",
    "東京",
    "",
]


@pytest.mark.parametrize("value", HOSTILE)
def test_a_string_round_trips_as_data_on_duckdb(value):
    import duckdb

    con = duckdb.connect()
    assert con.execute(f"SELECT {sql_literal(value, 'duckdb')}").fetchone() == (value,)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_string_round_trips_as_data_on_clickhouse(value):
    from chdb import session

    s = session.Session()
    try:
        out = s.query(f"SELECT {sql_literal(value, 'clickhouse')} AS v FORMAT JSONEachRow").bytes()
    finally:
        s.close()
    import json

    assert json.loads(out) == {"v": value}


@pytest.mark.parametrize(
    "dialect",
    ["postgres", "trino", "mysql", "snowflake", "bigquery", "databricks", "druid", "tsql"],
)
@pytest.mark.parametrize("value", HOSTILE)
def test_a_string_parses_back_to_itself_in_each_dialect(dialect, value):
    import sqlglot
    import sqlglot.expressions as exp

    tree = sqlglot.parse_one(f"SELECT {sql_literal(value, dialect)} AS v", read=dialect)
    (literal,) = list(tree.find_all(exp.Literal))
    assert literal.this == value
    assert not list(tree.find_all(exp.Or))  # nothing outside the literal


def test_pinot_takes_the_standard_rule():
    assert sql_literal("x\\') OR 1=1 --", "pinot") == "'x\\'') OR 1=1 --'"


def test_scalars_take_their_literal_forms():
    assert sql_literal(None, "clickhouse") == "NULL"
    assert sql_literal(True, "postgres") == "TRUE"
    assert sql_literal(7, "mysql") == "7"
    assert sql_literal(2.5, "trino") == "2.5"
    assert sql_literal(Decimal("1.25"), "duckdb") == "1.25"
    assert sql_literal(dt.date(2026, 10, 3), "postgres") == "CAST('2026-10-03' AS DATE)"
    assert sql_literal(dt.datetime(2026, 10, 3, 8, 0), "trino") == (
        "CAST('2026-10-03T08:00:00' AS TIMESTAMP)"
    )
    assert sql_literal(uuid.UUID(int=1), "postgres") == "'00000000-0000-0000-0000-000000000001'"
    assert sql_literal(b"\x01\xff", "trino") == "X'01ff'"
    assert sql_literal(b"\x01\xff", "postgres") == "'\\x01ff'"
    assert sql_literal({"a": "it's"}, "postgres") == "'{\"a\": \"it''s\"}'"
    assert sql_literal(math.nan, "trino") == "nan()"
    assert sql_literal(-math.inf, "postgres") == "'-Infinity'::float8"


@pytest.mark.parametrize(
    ("value", "dialect", "reason"),
    [
        ("a\x00b", "postgres", "NUL"),
        ("a\x00b", "clickhouse", "NUL"),
        (math.nan, "clickhouse", "nan has no literal"),
        (math.inf, "mysql", "inf has no literal"),
        (Decimal("NaN"), "postgres", "not a finite number"),
        (b"\x01", "clickhouse", "binary values"),
        (object(), "postgres", "no literal form"),
    ],
)
def test_a_value_the_dialect_cannot_hold_is_refused_by_name(value, dialect, reason):
    with pytest.raises(UnencodableLiteral, match=reason):
        sql_literal(value, dialect)
