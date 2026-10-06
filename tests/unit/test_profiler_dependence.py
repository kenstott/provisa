# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9a51-4f83-4b16-b0d7-e5a1c3f8d294
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Dependence between columns (REQ-1934 DEPENDENCE BETWEEN COLUMNS), its statements run against an
in-process DuckDB as the pipeline transpiles them."""

from __future__ import annotations

import json

import duckdb
import pytest
import sqlglot

from provisa.profiler import dependence as dep
from provisa.profiler.statement import ColumnSpec, Sample

_OWN = [
    ColumnSpec("id", "integer", "numeric", "id"),
    ColumnSpec("region", "varchar", "text", "region"),
    ColumnSpec("amount", "double", "numeric", "amount"),
    ColumnSpec("noise", "double", "numeric", "noise"),
    ColumnSpec("customer_id", "integer", "numeric", "customer_id"),
    ColumnSpec("code", "varchar", "text", "code"),
]
_PARENT = dep.ParentSpec(
    relationship="customer",
    table="d.customers",
    table_id=2,
    child_key="customer_id",
    parent_key="id",
    columns=[
        ColumnSpec("id", "integer", "numeric", "id"),
        ColumnSpec("tier", "varchar", "text", "tier"),
    ],
)


@pytest.fixture
def con():
    db = duckdb.connect()
    db.execute("CREATE SCHEMA d")
    # tier is fixed by the customer; region follows tier; amount follows region; noise follows nothing.
    db.execute(
        "CREATE TABLE d.customers AS SELECT range AS id, "
        "CASE WHEN range % 2 = 0 THEN 'gold' ELSE 'basic' END AS tier FROM range(50)"
    )
    db.execute(
        "CREATE TABLE d.orders AS SELECT range AS id, "
        "CASE WHEN (range % 50) % 2 = 0 THEN 'east' ELSE 'west' END AS region, "
        "CASE WHEN (range % 50) % 2 = 0 THEN 100.0 ELSE 10.0 END + (range % 7) AS amount, "
        "((range * 7919) % 101)::DOUBLE AS noise, "
        "range % 50 AS customer_id, "
        "'C-' || range::VARCHAR AS code "
        "FROM range(1000)"
    )
    return db


def _run(con, sql: str):
    res = con.execute(sqlglot.transpile(sql, read="postgres", write="duckdb")[0])
    return [d[0] for d in res.description], res.fetchall()


def _columns(con):
    names, rows = _run(con, dep.distinct_sql(_PARENT, [_PARENT.columns[1]]))
    parent_counts = dep.parse_distinct(names, rows[0], [_PARENT.columns[1]])
    own = list(zip(_OWN, [1000, 2, 14, 101, 50, 1000], [1.0] * 6))
    return dep.choose_columns(
        own, [(_PARENT, parent_counts)], max_numbers=20, max_distinct=20, max_categories=10
    )


def test_columns_enter_as_numbers_or_categories_within_the_bounds(con):
    cols = _columns(con)
    assert [(c.name, c.kind) for c in cols] == [
        ("id", "number"),
        ("region", "category"),
        ("amount", "number"),
        ("noise", "number"),
        ("customer_id", "number"),
        ("customer.id", "number"),
        ("customer.tier", "category"),
    ]
    # code has a distinct value per row: not a category. At most two numbers when bounded so.
    own = list(zip(_OWN, [1000, 2, 14, 101, 50, 1000], [1.0] * 6))
    bounded = dep.choose_columns(own, [], max_numbers=2, max_distinct=20, max_categories=10)
    assert [c.name for c in bounded] == ["id", "region", "amount"]


def test_categories_are_bounded_keeping_the_most_frequently_held():
    """REQ-1934: the run default bounds the category columns entered; the ones holding a value in
    the most rows are kept, then the fewest distinct values, then column order -- and the columns
    still enter in column order."""
    specs = [ColumnSpec(n, "varchar", "text", n) for n in ("a", "b", "c", "d")]
    own = list(zip(specs, [5, 3, 4, 2], [0.5, 1.0, 1.0, 0.9]))
    kept = dep.choose_columns(own, [], max_numbers=20, max_distinct=20, max_categories=2)
    assert [c.name for c in kept] == ["b", "c"]
    parent = dep.ParentSpec("p", "d.p", 2, "a", "id", [ColumnSpec("t", "varchar", "text", "t")])
    with_parent = dep.choose_columns(
        own, [(parent, {"t": (2, 1.0)})], max_numbers=20, max_distinct=20, max_categories=2
    )
    # b, c and p.t all hold a value in every row; b and p.t have the fewest values.
    assert [c.name for c in with_parent] == ["b", "p.t"]


def test_rank_correlation_correlation_ratio_and_the_dependency_network(con):
    cols = _columns(con)
    sql = dep.pairs_sql("d.orders", _OWN, [_PARENT], cols, Sample("whole"))
    assert len(sqlglot.parse(sql, read="postgres")) == 1
    pairs = dep.parse_pairs(*_run(con, sql), cols)
    idx = {c.name: i for i, c in enumerate(cols)}
    by = {(cols[a].name, cols[b].name): s for (a, b), s in pairs.items()}
    assert by[("id", "customer_id")].rows == 1000
    # region (category) and amount: region explains nearly all of amount's spread.
    region_amount = by[("region", "amount")]
    assert dep.correlation_ratio(region_amount) > 0.95
    # A category enters as the middle of its share's slice: east (first) low, west high.
    assert dep.spearman(region_amount) == pytest.approx(-1.0, abs=0.15)
    assert abs(dep.spearman(by[("amount", "noise")])) < 0.2

    singles = dep.single_candidates(pairs, cols)
    best_for_amount = singles[idx["amount"]][0]
    assert cols[best_for_amount.parents[0]].name in (
        "region",
        "customer.tier",
        "customer_id",
        "customer.id",
    )
    triples = dep.triples_for(singles)
    tsql = dep.triples_sql("d.orders", _OWN, [_PARENT], cols, triples, Sample("whole"))
    pair_sets = dep.pair_candidates(dep.parse_triples(*_run(con, tsql), triples))
    chosen = dep.network(singles, pair_sets)
    # region is fully told by the customer's tier, through the relationship (and by amount).
    tier = next(c for c in singles[idx["region"]] if cols[c.parents[0]].name == "customer.tier")
    assert tier.uncertainty == pytest.approx(1.0)
    assert chosen[idx["region"]].uncertainty == pytest.approx(1.0)
    # noise is told by nothing: no parent set pays for its states.
    assert idx["noise"] not in chosen

    rows = dep.dependency_rows(singles, pair_sets, chosen, cols)
    region_rows = [r for r in rows if r["column_name"] == "region"]
    assert region_rows[0]["rank"] == 1
    assert sum(r["in_network"] for r in region_rows) == 1
    joints = dep.joint_rows(chosen, cols)
    region_joint = [r for r in joints if r["column_name"] == "region"]
    assert sum(r["row_count"] for r in region_joint) == 1000
    assert {r["target_value"] for r in region_joint} == {"east", "west"}
    corr = dep.correlation_rows(pairs, cols)
    eta = next(
        r
        for r in corr
        if r["measure"] == "correlation_ratio"
        and r["other_column"] == "amount"
        and r["column_name"] == "region"
    )
    assert json.loads(eta["involved_columns"]) == ["region", "amount"]


def test_mutual_information_of_independent_columns_is_zero():
    joint = {("a", "x"): 25, ("a", "y"): 25, ("b", "x"): 25, ("b", "y"): 25}
    assert dep.mutual_information(joint, 0) == pytest.approx(0.0)
    determined = {("a", "x"): 50, ("b", "y"): 50}
    assert dep.mutual_information(determined, 0) == pytest.approx(dep.entropy({"a": 50, "b": 50}))
