# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1f8e27-9a4d-4b63-8e0a-2d7b6c9f13a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The classification sample is non-null values per column (REQ-2147, amended 2026-11-15).

Up to N non-null values of each column, gathered for every column in ONE read of the run's sample,
which is the read's ceiling on rows; a column with fewer than MIN_TESTED values there is reported as
too few values to classify, not as matching no kind. Run against an in-process DuckDB after the
transpile the pipeline does, and asserted on the statement each engine is given.
"""

# Requirements: REQ-2147

from __future__ import annotations

import random
import types
from typing import Any, cast

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.profiler import classifications, run
from provisa.profiler.classify import MIN_TESTED, judge, kind_of
from provisa.profiler.statement import (
    SAMPLE_ROWS,
    ColumnSpec,
    Sample,
    classification_sql,
    key_ranges,
    parse_classification_result,
)

_CITIES = ("Paris", "London", "Tokyo", "Berlin", "Madrid", "Toronto", "Chicago", "Mumbai")
_COLUMNS = [
    ColumnSpec("id", "integer", "numeric", "id"),
    ColumnSpec("city", "varchar", "text", "city"),
    ColumnSpec("home", "varchar", "text", "home"),
    ColumnSpec("late", "varchar", "text", "late"),
]
# The engines the profiler runs its statements on, by the dialect each is given.
_ENGINES = ("duckdb", "trino", "clickhouse", "postgres")


@pytest.fixture
def con():
    """d.visits: 20,000 rows. city is 95% null (a world city on every 20th row); home is a city on
    every row; late is set only on ids 15,000 and above."""
    db = duckdb.connect()
    db.execute("CREATE SCHEMA d")
    cities = "[" + ", ".join(f"'{c}'" for c in _CITIES) + "]"
    db.execute(
        "CREATE TABLE d.visits AS SELECT range::INTEGER AS id, "
        f"CASE WHEN range % 20 = 0 THEN {cities}[1 + (range // 20) % 8] END AS city, "
        f"{cities}[1 + range % 8] AS home, "
        f"CASE WHEN range >= 15000 THEN {cities}[1 + range % 8] END AS late "
        "FROM range(20000)"
    )
    return db


def _read(con, sql: str):
    res = con.execute(sqlglot.transpile(sql, read="postgres", write="duckdb")[0])
    return parse_classification_result([d[0] for d in res.description], res.fetchall(), _COLUMNS)


def _row_sample_values(con, per_column: int) -> list[str | None]:
    """city's values as the row sample read them before the amendment: ``per_column`` rows drawn
    at random, each column's value on them, nulls included."""
    rows = con.execute(
        f"SELECT city FROM d.visits USING SAMPLE reservoir({per_column} ROWS) REPEATABLE (7)"
    ).fetchall()
    return [r[0] for r in rows]


def test_a_95_percent_null_column_gets_its_kind_where_the_row_sample_did_not(con):
    per_column = 40
    kinds = (kind_of("city"),)
    # The row sample: 40 rows of a column 95% null hold about 2 values, under MIN_TESTED.
    assert judge("text", _row_sample_values(con, per_column), kinds).too_few
    values, rows = _read(con, classification_sql("d.visits", _COLUMNS, Sample("whole"), per_column))
    assert rows == 20000
    assert len(values["city"]) == per_column
    city = judge("text", list(values["city"]), kinds)
    assert not city.too_few
    assert [m.kind.id for m in city.matches] == ["city"]


def test_a_fully_populated_column_yields_as_many_values_as_before(con):
    # Before the amendment a fully populated column yielded one value per sampled row: N.
    values, _ = _read(con, classification_sql("d.visits", _COLUMNS, Sample("whole"), 500))
    assert len(values["home"]) == 500
    assert len(values["id"]) == 500
    # Never a null among the values; the sparse column has its N too.
    assert all(v is not None for v in values["city"]) and len(values["city"]) == 500


def test_the_read_never_goes_past_its_ceiling_the_runs_sample(con):
    # The run's sample is the ceiling on rows: a key-range sample of ids below 15,000 holds no
    # value of late however many the table has, so late has too few values -- not no kind.
    ranges = key_ranges(0, 14999, 0.2, random.Random(5))
    sample = Sample("key_range", 0.2, "id", ranges)
    values, rows = _read(con, classification_sql("d.visits", _COLUMNS, sample, 500))
    held = sum(b - a + 1 for a, b in ranges)
    assert rows == held
    assert values["late"] == []
    assert len(values["home"]) == 500
    late = judge("text", list(values["late"]), (kind_of("city"),))
    assert late.too_few and late.values == 0 and late.matches == ()


def test_a_sample_with_every_value_null_still_says_how_many_rows_it_held(con):
    sparse = [ColumnSpec("late", "varchar", "text", "late")]
    sql = classification_sql("d.visits", sparse, Sample("key_range", 0.1, "id", ((0, 99),)), 500)
    res = con.execute(sqlglot.transpile(sql, read="postgres", write="duckdb")[0])
    values, rows = parse_classification_result(
        [d[0] for d in res.description], res.fetchall(), sparse
    )
    assert (values["late"], rows) == ([], 100)


def test_too_few_values_is_not_the_same_statement_as_no_kind():
    kinds = (kind_of("city"),)
    few = judge("text", ["Paris"] * (MIN_TESTED - 1) + [None] * 50, kinds)
    none_matched = judge("text", ["not a city"] * MIN_TESTED, kinds)
    assert (few.too_few, few.values, few.matches) == (True, MIN_TESTED - 1, ())
    assert (none_matched.too_few, none_matched.values, none_matched.matches) == (
        False,
        MIN_TESTED,
        (),
    )
    j = run.ColumnClasses
    assert run.classification_outcome(j("c", "c", few)) == "too_few_values"
    assert run.classification_outcome(j("c", "c", none_matched)) == "no_kind"
    matched = judge("text", ["Paris"] * MIN_TESTED, kinds)
    assert run.classification_outcome(j("c", "c", matched)) == "matched"


@pytest.mark.parametrize("dialect", _ENGINES)
@pytest.mark.parametrize(
    "sample",
    [Sample("whole"), Sample("block", 0.1), Sample("random", 0.1)],
    ids=["whole", "block", "random"],
)
def test_the_statement_is_one_read_of_the_table_on_every_engine(dialect, sample):
    sql = classification_sql("d.visits", _COLUMNS, sample, 500)
    physical = sqlglot.transpile(sql, read="postgres", write=dialect)[0]
    tree = sqlglot.parse_one(physical, read=dialect)
    # One statement, one reference to the table, no set operation and no per-column query.
    assert len(sqlglot.parse(physical, read=dialect)) == 1
    tables = [t.name for t in tree.find_all(exp.Table)]
    assert tables == ["visits"]
    assert not list(tree.find_all(exp.Union))
    # Every column's running count of non-null values shares the one random order.
    windows = list(tree.find_all(exp.Window))
    orders = {w.args["order"].sql(dialect=dialect) for w in windows if w.args.get("order")}
    assert len(orders) == 1
    for i in range(len(_COLUMNS)):
        assert f'COUNT(o."c{i}") OVER (ORDER BY o."__order"' in physical
        assert f's."n{i}" <= 500' in physical


def test_each_engine_is_given_the_statement_in_its_own_dialect():
    sql = classification_sql("d.visits", _COLUMNS, Sample("block", 0.1), 500)
    physical = {d: sqlglot.transpile(sql, read="postgres", write=d)[0] for d in _ENGINES}
    assert 'RANDOM() AS "__order"' in physical["duckdb"]
    assert 'RANDOM() AS "__order"' in physical["postgres"]
    assert 'RAND() AS "__order"' in physical["trino"]
    assert 'randCanonical() AS "__order"' in physical["clickhouse"]
    assert "TABLESAMPLE SYSTEM (10.0 PERCENT)" in physical["duckdb"]
    assert "TABLESAMPLE SYSTEM (10.0)" in physical["trino"]
    assert "SAMPLE SYSTEM 10.0 PERCENT" in physical["clickhouse"]
    assert f'COUNT(*) OVER () AS "{SAMPLE_ROWS}"' in physical["postgres"]


# -- the run's side ---------------------------------------------------------------------------------


def _target():
    return types.SimpleNamespace(
        pgwire_name="sales.visits",
        table_name="visits",
        columns=[
            ColumnSpec("city", "varchar", "text", "city"),
            ColumnSpec("note", "varchar", "text", "note"),
            ColumnSpec("opened", "date", "temporal", "opened"),
        ],
    )


@pytest.mark.asyncio
async def test_the_run_records_too_few_values_on_the_columns_row_not_as_no_kind(monkeypatch):
    seen: list[str] = []

    async def _further(sql, sample, where, table_name):
        seen.append(sql)
        # city has 12 values in the sample; note only 3.
        rows = [("Paris", "x" if i < 3 else None, 4000) for i in range(12)]
        return ["city", "note", SAMPLE_ROWS], rows

    monkeypatch.setattr(run, "_run_further", _further)
    read = types.SimpleNamespace(sample=Sample("block", 0.1), where="direct postgresql")
    judged, attempts = await run.read_classifications(
        cast(Any, _target()), cast(Any, read), (kind_of("city"),), 500
    )
    assert len(seen) == 1  # one read for every column
    assert '"opened"' not in seen[0]
    assert attempts == [{"statement": "classification", "percent": 10.0, "rows": 4000}]
    by = {j.column: j for j in judged}
    assert [m.kind.id for m in by["city"].matches] == ["city"]
    assert by["note"].too_few and by["note"].judgement.values == 3
    (suggestion,) = run.suggestions_of(judged)
    assert suggestion.column == "city"

    results = {
        "columns": [
            {"column_name": n, "classification": None, "classification_values": None}
            for n in ("city", "note", "opened")
        ]
    }
    run.classified_columns(results, judged)
    assert [(r["classification"], r["classification_values"]) for r in results["columns"]] == [
        ("matched", 12),
        ("too_few_values", 3),
        (None, None),
    ]


class _NoWrites:
    """A model-store connection on which any write fails the test."""

    def __getattr__(self, name: str):
        raise AssertionError(f"classify_table touched the model store: {name}")


@pytest.mark.asyncio
async def test_classify_table_returns_kinds_and_too_few_and_records_nothing(monkeypatch):
    statements: list[str] = []

    class _Acquire:
        async def __aenter__(self):
            return _NoWrites()

        async def __aexit__(self, *exc):
            return False

    state = types.SimpleNamespace(model_db=types.SimpleNamespace(acquire=lambda: _Acquire()))

    async def _tags(conn, table_id):
        assert isinstance(conn, _NoWrites) and table_id == 9
        return {}

    async def _kinds(model_db):
        return (kind_of("city"),)

    target = _target()
    target.meta = object()
    target.key = None

    async def _governed(sql):
        statements.append(sql)
        return ["row_count"], [(4000,)]

    async def _route(sql):
        statements.append(sql)
        return types.SimpleNamespace(route="engine")

    async def _sample_for(state_, target_, route, row_count, fraction, rng):
        assert (route, row_count) == ("engine", 4000)
        return Sample("block", fraction), types.SimpleNamespace(where="duckdb postgresql")

    async def _further(sql, sample, where, table_name):
        statements.append(sql)
        assert sample == Sample("block", 0.25) and where == "duckdb postgresql"
        rows = [("Paris", "x" if i < 3 else None, 1000) for i in range(12)]
        return ["city", "note", SAMPLE_ROWS], rows

    def _refuse(*a, **k):
        raise AssertionError("classify_table recorded something")

    monkeypatch.setattr(run, "column_tags", _tags)
    monkeypatch.setattr(classifications, "enabled_kinds", _kinds)
    monkeypatch.setattr(run, "resolve_target", lambda *a: target)
    monkeypatch.setattr(run, "_governed", _governed)
    monkeypatch.setattr(run, "_route", _route)
    monkeypatch.setattr(run, "_sample_for", _sample_for)
    monkeypatch.setattr(run, "_run_further", _further)
    monkeypatch.setattr(run, "write_results", _refuse)
    monkeypatch.setattr(classifications, "record", _refuse)

    judged = await run.classify_table(
        cast(Any, state),
        table_id=9,
        table_name="visits",
        values_per_column=500,
        # 4000 rows x 3 columns = 12,000 cells over a 3,000-cell budget: a quarter is sampled.
        sample_above_cells=3000,
    )
    by = {j.column: j for j in judged}
    assert [m.kind.id for m in by["city"].matches] == ["city"]
    assert by["note"].too_few and by["note"].judgement.values == 3
    assert "opened" not in by
    # Counted, routed to choose the sample, then the one classification read.
    assert statements[0].startswith("SELECT COUNT(*) AS row_count")
    assert (
        len(statements) == 3
        and statements[1].count("__order")
        and statements[2].count("TABLESAMPLE SYSTEM (25.0)")
    )


@pytest.mark.asyncio
async def test_classify_table_reads_a_table_within_its_budget_whole(monkeypatch):
    class _Acquire:
        async def __aenter__(self):
            return _NoWrites()

        async def __aexit__(self, *exc):
            return False

    state = types.SimpleNamespace(model_db=types.SimpleNamespace(acquire=lambda: _Acquire()))
    seen: list[Sample] = []

    async def _tags(conn, table_id):
        return {}

    async def _kinds(model_db):
        return (kind_of("city"),)

    async def _governed(sql):
        return ["row_count"], [(30,)]

    async def _further(sql, sample, where, table_name):
        seen.append(sample)
        return ["city", "note", SAMPLE_ROWS], [("Paris", None, 30)] * 4

    monkeypatch.setattr(run, "column_tags", _tags)
    monkeypatch.setattr(classifications, "enabled_kinds", _kinds)
    monkeypatch.setattr(run, "resolve_target", lambda *a: _target())
    monkeypatch.setattr(run, "_governed", _governed)
    monkeypatch.setattr(run, "_run_further", _further)
    judged = await run.classify_table(
        cast(Any, state),
        table_id=9,
        table_name="visits",
        values_per_column=500,
        sample_above_cells=None,
    )
    assert seen == [Sample("whole")]
    assert all(j.too_few for j in judged)  # 4 values each: too few, not no kind
