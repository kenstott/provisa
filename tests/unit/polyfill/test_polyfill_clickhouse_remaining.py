# Copyright (c) 2026 Kenneth Stott
# Canary: 9e2c5a71-4b38-4d16-8f0a-3c7d1e6b2a94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The rest of ClickHouse's wrong answers of run 43361055823, as grid rows (REQ-2236, REQ-2171).

1. A strict call of a NULL array is the NULL it answers, ClickHouse having no NULL of an array: a
   set-returning call answers no rows, not one NULL, so it is left to its own row
   (generate_subscripts' refuses it on ClickHouse). The rule is the grid's, not function_macros'.
2. ClickHouse reads a text of a day its month does not have as a day of the next month: a cast of
   text to a date, and pg_input_is_valid's check of one, answer PostgreSQL's error (NULL for a
   try-cast), and a text not in ISO form is refused by name.
3. date_trunc is dateTrunc's start as DateTime64(6, 'UTC'): ClickHouse's DateTime reaches Arrow as
   its count of seconds and holds no day before 1970. One builder, for the grid's row and for
   every other writer of it (semantic_dialect, zoned_instants).
4. lag and lead out of the partition, and first_value, last_value and nth_value of a NULL or an
   empty frame, answer NULL: ClickHouse's answer the type's default, and its first_value and
   last_value skip a NULL."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import pyarrow as pa
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler import function_macros
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.transpile import transpile


def _clickhouse(sql: str) -> str:
    return transpile(sql, "clickhouse")


def _features() -> set[str]:
    return {f.name for f in GRID.features}


# --- 1. a NULL array ----------------------------------------------------------------------------


def test_the_null_array_rule_is_the_grids_alone():
    from provisa.transpiler.polyfill import clickhouse

    assert clickhouse.NULL_ARRAY in GRID.features
    assert not hasattr(function_macros, "_of_a_null_array")
    assert not hasattr(function_macros, "_strict_arities")


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."generate_subscripts"(NULL::integer[], NULL::integer) AS v',
        'SELECT pg_catalog."generate_subscripts"(NULL::integer[], NULL::integer, '
        "NULL::boolean) AS v",
        'SELECT pg_catalog."pg_options_to_table"(NULL::text[]) AS v',
    ],
)
def test_a_set_returning_call_of_a_null_array_is_not_one_null_row(sql):
    with pytest.raises(FunctionRefused) as refused:
        _clickhouse(sql)
    assert "has no form on clickhouse" in str(refused.value)


@pytest.mark.parametrize(
    ("sql", "named"),
    [
        ("SELECT cardinality(NULL::integer[])", "cardinality"),
        ('SELECT pg_catalog."array_ndims"(NULL::integer[]) AS v', "v"),
        ('SELECT pg_catalog."array_position"(NULL::integer[], 1) AS v', "v"),
        ('SELECT pg_catalog."array_cat"(NULL::integer[], NULL::integer[]) AS v', "v"),
        ('SELECT pg_catalog."cardinality"(NULL::integer[]) AS v', "v"),
    ],
)
def test_a_strict_call_of_a_null_array_is_still_its_null(sql, named):
    written = sqlglot.parse_one(_clickhouse(sql), read="clickhouse")
    assert isinstance(written, exp.Select)
    (column,) = written.selects
    assert isinstance(column.unalias(), exp.Null)
    assert column.alias_or_name == named


def test_a_null_array_on_another_engine_is_unchanged():
    sql = 'SELECT pg_catalog."cardinality"(NULL::integer[]) AS v'
    assert "NULL AS v" not in transpile(sql, "duckdb")


# --- 2. a text cast to a date -------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT '2024-02-30'::date AS v",
        "SELECT CAST('2023-02-29' AS date) AS v",
        "SELECT date '2024-04-31' AS v",
    ],
)
def test_a_day_its_month_does_not_have_is_postgresqls_error(sql):
    with pytest.raises(RowRefused) as refused:
        _clickhouse(sql)
    assert refused.value.sqlstate == "22008"
    assert "date/time field value out of range" in str(refused.value)


@pytest.mark.parametrize("text", ["2024-02-29", "2024-2-3", "1960-05-05"])
def test_a_day_that_exists_is_its_date(text):
    written = _clickhouse(f"SELECT '{text}'::date AS v")
    assert "toDate32" in written
    assert "Nullable(DATE)" not in written


@pytest.mark.parametrize("text", ["Feb 3 2024", "20240203", "2024/02/03", "epoch"])
def test_a_text_not_in_iso_form_is_refused_by_name(text):
    with pytest.raises(RowRefused) as refused:
        _clickhouse(f"SELECT '{text}'::date AS v")
    assert refused.value.sqlstate == "0A000"
    assert "date" in str(refused.value)


def test_a_try_cast_of_a_day_its_month_does_not_have_is_null():
    """pg_input_is_valid's check, a null-on-failure cast: NULL, so the check is false."""
    written = _clickhouse("SELECT NOT TRY_CAST('2024-02-30' AS date) IS NULL AS v")
    assert "2024-02-30" not in written
    assert "CAST(NULL AS Nullable(DATE))" in written


def test_a_try_cast_of_a_day_that_exists_reads_it():
    written = _clickhouse("SELECT NOT TRY_CAST('2024-02-29' AS date) IS NULL AS v")
    assert "toDate32('2024-02-29')" in written


@pytest.mark.parametrize(("text", "valid"), [("2024-02-30", False), ("2024-02-29", True)])
def test_pg_input_is_valid_as_pgwire_writes_it(text, valid):
    """pgwire's check (catalog_server_answers.input_check), written as the semantic SQL keeps it
    (TRY_CAST): false of a day that does not exist, where ClickHouse's next day answered true
    (run 43361055823)."""
    from provisa.pgwire.catalog_server_answers import input_check

    check = input_check(exp.Literal.string(text), exp.Literal.string("date"))
    written = _clickhouse(f"SELECT {check.sql('postgres')} AS v")
    if valid:
        assert f"toDate32('{text}')" in written
    else:
        assert text not in written and "CAST(NULL AS Nullable(DATE))" in written


def test_a_cast_of_a_column_to_a_date_is_unchanged():
    assert "accurateCast" not in _clickhouse("SELECT CAST(x AS date) AS v FROM t")


@pytest.mark.parametrize("dialect", ["duckdb", "trino"])
def test_a_date_cast_on_another_engine_is_unchanged(dialect):
    assert "toDate32" not in transpile("SELECT '2024-02-30'::date AS v", dialect)


# --- 3. date_trunc --------------------------------------------------------------------------------


def test_date_trunc_is_a_row_of_the_grid():
    assert "date_trunc" in _features()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT date_trunc('year', '1960-05-05'::timestamp) AS v",
        "SELECT pg_catalog.\"date_trunc\"('year'::text, '1960-05-05'::timestamp) AS v",
    ],
)
def test_date_trunc_of_a_statement_is_a_datetime64(sql):
    written = _clickhouse(sql)
    assert "toDateTime64(dateTrunc('year', " in written
    assert ", 6, 'UTC')" in written
    assert "toDateTime(" not in written


def test_date_trunc_of_null_is_a_null_datetime64():
    written = _clickhouse('SELECT pg_catalog."date_trunc"(NULL::text, NULL::timestamp) AS v')
    assert "DateTime64(6, 'UTC')" in written
    assert "Nullable(DateTime)" not in written


def test_date_trunc_written_by_sqlglot_is_the_same_builders():
    """The semantic layer's path: sqlglot's ClickHouse generator, not a pg statement."""
    import provisa.transpiler.semantic_dialect  # noqa: F401 -- its ClickHouse transforms

    node = exp.TimestampTrunc(
        this=exp.cast(exp.Literal.string("1960-05-05"), "TIMESTAMP"), unit=exp.var("YEAR")
    )
    written = node.sql(dialect="clickhouse")
    assert written.startswith("toDateTime64(dateTrunc('year', ")
    assert written.endswith(", 6, 'UTC')")


def test_date_trunc_of_an_instant_is_the_same_builders():
    """zoned_instants' start in the session's zone: the builder's form (its text read back, the
    unit in capitals, which dateTrunc takes), of the instant held as DateTime64, not DateTime."""
    written = _clickhouse("SELECT date_trunc('year', '1960-05-05 00:00:00+00'::timestamptz) AS v")
    assert "todatetime64(datetrunc('year', " in written.lower()
    assert "Nullable(DateTime)" not in written
    assert "toDateTime(" not in written


def test_date_trunc_of_a_second_is_the_same_builders():
    written = _clickhouse("SELECT date_trunc('second', '1960-05-05 10:00:01.5'::timestamp) AS v")
    assert "fromUnixTimestamp64Micro(" in written and ", 'UTC')" in written
    assert "Nullable(DateTime)" not in written


def test_one_builder(monkeypatch):
    """The grid's row, sqlglot's generator (the semantic layer's path) and zoned_instants (an
    instant's start, and a plain one of a second) each call the grid's one builder."""
    from provisa.transpiler.polyfill import clickhouse

    built = clickhouse.truncated
    calls: list[str] = []

    def counted(unit: str, value: exp.Expr, zone: str = "UTC") -> exp.Expr:
        calls.append(unit.lower())
        return built(unit, value, zone)

    monkeypatch.setattr(clickhouse, "truncated", counted)
    _clickhouse("SELECT date_trunc('year', '1960-05-05'::timestamp) AS v")
    exp.TimestampTrunc(this=exp.column("c"), unit=exp.var("MONTH")).sql(dialect="clickhouse")
    _clickhouse("SELECT date_trunc('hour', '1960-05-05 00:00:00+00'::timestamptz) AS v")
    _clickhouse("SELECT date_trunc('second', '1960-05-05 10:00:01.5'::timestamp) AS v")
    assert {"year", "month", "hour", "second"} <= set(calls)


# --- 4. a window function's default -------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "function"),
    [
        ("SELECT lag(5) OVER () AS v", "lagInFrame"),
        ('SELECT pg_catalog."lag"(NULL::integer) OVER () AS v', "lagInFrame"),
        ("SELECT lead(x, 2) OVER (ORDER BY y) AS v FROM t", "leadInFrame"),
        ("SELECT lead(x, 2, 0) OVER (PARTITION BY z ORDER BY y) AS v FROM t", "leadInFrame"),
    ],
)
def test_lag_and_lead_are_null_past_the_partition(sql, function):
    written = _clickhouse(sql)
    assert f"{function}(toNullable(" in written
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in written


@pytest.mark.parametrize("function", ["first_value", "last_value"])
def test_first_and_last_value_keep_a_null(function):
    written = _clickhouse(f"SELECT {function}(x) OVER (ORDER BY y) AS v FROM t")
    assert "RESPECT NULLS" in written.upper()
    assert "toNullable(x)" in written


def test_nth_value_is_null_past_its_frame():
    assert "toNullable(5)" in _clickhouse("SELECT nth_value(5, 2) OVER () AS v")


def test_an_unaliased_lag_is_named_lag():
    written = sqlglot.parse_one(_clickhouse("SELECT lag(5) OVER ()"), read="clickhouse")
    assert isinstance(written, exp.Select)
    (column,) = written.selects
    assert column.alias_or_name == "lag"


@pytest.mark.parametrize("dialect", ["duckdb", "trino"])
def test_a_window_function_on_another_engine_is_unchanged(dialect):
    written = transpile("SELECT lag(x) OVER (ORDER BY y) AS v FROM t", dialect)
    assert "InFrame" not in written and "toNullable" not in written


# --- the cached replay of an Arrow result -----------------------------------------------------


def test_a_cached_arrow_replay_names_float_and_nested_lists_as_arrays():
    """arrow_type_name names a list of floats, and a list of lists, as its array type (not JSON),
    for a cached Arrow entry replayed with no column types as for a live one."""
    from provisa.cache.raw_sql import arrow_entry, entry_as_result

    table = pa.table(
        {
            "f": pa.array([[1.5, None]], pa.list_(pa.float64())),
            "n": pa.array([[[1, 2], [3]]], pa.list_(pa.list_(pa.int32()))),
            "r": pa.array([[1.5]], pa.list_(pa.float32())),
        }
    )
    entry = arrow_entry(table.schema, table.to_batches())
    assert entry_as_result(entry, None).column_types == ["DOUBLE[]", "INTEGER[]", "REAL[]"]


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."array_agg"(c1 ORDER BY c1) AS v FROM (VALUES (3::integer), '
        "((-7)::integer)) AS t(c1)",
        "SELECT array_agg(x ORDER BY y DESC) AS v FROM t",
    ],
)
def test_an_ordered_array_agg_is_refused_by_name_on_clickhouse(sql):
    """Run 43382046085: ClickHouse has no ORDER BY in an aggregate's call (Code 62)."""
    with pytest.raises(RowRefused, match="array_agg"):
        _clickhouse(sql)
