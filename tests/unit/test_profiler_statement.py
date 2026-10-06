# Copyright (c) 2026 Kenneth Stott
# Canary: a83c6e19-5d27-4f40-b9e1-0f7d2c4a8b65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one profile statement (REQ-1934), run against an in-process DuckDB.

The statement is written in the governed dialect (postgres) and transpiled as the pipeline does, so
these tests see the measures an engine actually returns for it.
"""

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.profiler.run import result_rows, sample_fraction, Target
from provisa.profiler.schema import RESULT_KINDS, field_names
from provisa.profiler.statement import (
    ColumnSpec,
    FanoutSpec,
    family_of,
    parse_profile_result,
    profile_sql,
)

_COLUMNS = [
    ColumnSpec("id", "integer", "numeric", "id"),
    ColumnSpec("amount", "double", "numeric", "amount"),
    ColumnSpec("code", "varchar", "text", "code"),
    ColumnSpec("placed", "timestamp", "temporal", "placed"),
    ColumnSpec("paid", "boolean", "boolean", "paid"),
]
_FANOUT = [FanoutSpec("lines", "d.lines", "id", "order_id")]


@pytest.fixture
def con():
    db = duckdb.connect()
    db.execute("CREATE SCHEMA d")
    db.execute(
        "CREATE TABLE d.orders AS SELECT range::INTEGER AS id, "
        "CASE WHEN range % 10 = 0 THEN NULL ELSE (range % 7)::DOUBLE END AS amount, "
        "'ORD-' || range::VARCHAR AS code, "
        "TIMESTAMP '2024-01-01' + to_seconds(range * 3600) AS placed, "
        "range % 2 = 0 AS paid FROM range(500)"
    )
    # orders 0..299 have three lines each; 300..499 have none.
    db.execute(
        "CREATE TABLE d.lines AS SELECT range AS line_id, range % 300 AS order_id FROM range(900)"
    )
    return db


def _run(con, sql: str):
    res = con.execute(sqlglot.transpile(sql, read="postgres", write="duckdb")[0])
    return [d[0] for d in res.description], res.fetchall()


def test_one_statement_profiles_every_column_and_relationship(con):
    sql = profile_sql("d.orders", _COLUMNS, _FANOUT, None, 100)
    assert len(sqlglot.parse(sql, read="postgres")) == 1
    names, rows = _run(con, sql)
    agg = parse_profile_result(names, rows, _COLUMNS, _FANOUT)
    assert agg.profiled_rows == 500
    by = {c.spec.name: c for c in agg.columns}

    assert (by["id"].distinct, by["id"].vmin, by["id"].vmax, by["id"].m1) == (
        500,
        0.0,
        499.0,
        249.5,
    )
    amount = by["amount"]
    assert (amount.non_null, amount.distinct, amount.integers) == (450, 7, 450)
    assert len(amount.quantiles or []) == 101
    # amount has 8 groups including null — a low-cardinality column keeps every value.
    assert len(amount.values) == 8

    code = by["code"]
    assert (code.min_text, code.length_min, code.length_max) == ("ORD-0", 5, 7)
    assert code.shapes[:3] == [("AAA-999", 400), ("AAA-99", 90), ("AAA-9", 10)]

    placed = by["placed"]
    assert placed.min_text == "2024-01-01 00:00:00"
    assert by["paid"].values == [("false", 250), ("true", 250)]

    fan = agg.fanouts[0]
    assert (fan.parents, fan.mean, fan.max, fan.childless) == (500, 1.8, 3, 200)


def test_a_sample_reads_a_fraction_of_the_rows(con):
    names, rows = _run(con, profile_sql("d.orders", _COLUMNS, [], 0.2, 100))
    agg = parse_profile_result(names, rows, _COLUMNS, [])
    assert 0 < agg.profiled_rows < 500


def test_the_sample_budget_is_in_cells_so_a_wide_table_samples_fewer_rows():
    assert sample_fraction(1000, 4, None) is None
    assert sample_fraction(1000, 4, 4000) is None
    assert sample_fraction(1000, 4, 1000) == 0.25
    # The same budget over a table ten times as wide samples a tenth as many rows.
    assert sample_fraction(1000, 40, 1000) == 0.025


def test_every_result_row_has_its_kinds_shipped_fields(con):
    from datetime import UTC, datetime

    names, rows = _run(con, profile_sql("d.orders", _COLUMNS, _FANOUT, None, 100))
    agg = parse_profile_result(names, rows, _COLUMNS, _FANOUT)
    target = Target(1, "orders", "d.orders", _COLUMNS, _FANOUT, {"code": {"pii"}})
    out = result_rows(target, agg, "r1", datetime.now(UTC), 100)
    assert set(out) == set(RESULT_KINDS) - {"runs"}
    for kind, kind_rows in out.items():
        assert kind_rows, kind
        assert all(tuple(r) == field_names(kind) for r in kind_rows)
    freq = [
        r for r in out["top_values"] if r["column_name"] == "amount" and r["kind"] == "frequency"
    ]
    assert len(freq) == 8
    # id has 500 distinct values: its top values are kept, its full frequency table is not.
    assert not [
        r for r in out["top_values"] if r["column_name"] == "id" and r["kind"] == "frequency"
    ]
    assert len([r for r in out["fanout"] if r["relationship"] == "lines"]) == 101


def test_a_table_with_no_readable_column_cannot_be_profiled():
    with pytest.raises(ValueError, match="no column the org admin can read"):
        profile_sql("d.orders", [], [], None, 100)


def test_a_relationship_key_the_org_admin_cannot_read_is_refused():
    with pytest.raises(ValueError, match="parent key 'hidden'"):
        profile_sql(
            "d.orders", _COLUMNS, [FanoutSpec("x", "d.lines", "hidden", "order_id")], None, 100
        )


@pytest.mark.parametrize(
    "data_type,family",
    [
        ("integer", "numeric"),
        ("numeric(10,2)", "numeric"),
        ("timestamptz", "temporal"),
        ("date", "temporal"),
        ("varchar", "text"),
        ("boolean", "boolean"),
        ("jsonb", "other"),
    ],
)
def test_family_of(data_type, family):
    assert family_of(data_type) == family


def test_the_low_cardinality_threshold_is_the_profilers_run_default(con):
    """amount has 8 value groups (7 values and null): its full frequency table is kept under a
    threshold of 8 and above, and not under 7."""
    from datetime import UTC, datetime

    target = Target(1, "orders", "d.orders", _COLUMNS, [], {})

    def frequencies(low: int) -> int:
        names, rows = _run(con, profile_sql("d.orders", _COLUMNS, [], None, low))
        agg = parse_profile_result(names, rows, _COLUMNS, [])
        out = result_rows(target, agg, "r", datetime.now(UTC), low)
        return len(
            [
                r
                for r in out["top_values"]
                if r["column_name"] == "amount" and r["kind"] == "frequency"
            ]
        )

    assert frequencies(8) == 8
    assert frequencies(7) == 0
