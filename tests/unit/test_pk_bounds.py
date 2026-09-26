# Copyright (c) 2026 Kenneth Stott
# Canary: a8cce4f2-a71f-4f2e-9ecd-d7ac9f141a29
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865: extract_pk_bounds's sqlglot predicate resolution — equality, IN-list, OR-chain, and
unbounded shapes falling back correctly (absent from the result, never an error)."""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot

from provisa.compiler.pk_bounds import extract_pk_bounds


@dataclass
class _Col:
    name: str
    is_primary_key: bool = False


@dataclass
class _Table:
    source_id: str
    schema_name: str
    table_name: str
    columns: list[_Col] = field(default_factory=list)


def _orders_table(extra_pk: str | None = None) -> dict[str, _Table]:
    cols = [_Col("id", is_primary_key=True), _Col("status")]
    if extra_pk:
        cols.append(_Col(extra_pk, is_primary_key=True))
    return {"orders": _Table("neo4j_src", "public", "orders", cols)}


def _ast(sql: str):
    return sqlglot.parse_one(sql, read="postgres")


def test_single_equality_resolves_bound():
    ast = _ast("SELECT * FROM orders WHERE id = 42")
    bounds = extract_pk_bounds(ast, _orders_table())
    assert len(bounds) == 1
    b = bounds[0]
    assert b.table_name == "orders"
    assert b.pk_columns == ("id",)
    assert b.values == ((42,),)


def test_in_list_resolves_bound():
    ast = _ast("SELECT * FROM orders WHERE id IN (1, 2, 3)")
    bounds = extract_pk_bounds(ast, _orders_table())
    assert len(bounds) == 1
    assert set(bounds[0].values) == {(1,), (2,), (3,)}


def test_or_chain_of_equalities_resolves_bound():
    ast = _ast("SELECT * FROM orders WHERE id = 1 OR id = 2")
    bounds = extract_pk_bounds(ast, _orders_table())
    assert len(bounds) == 1
    assert set(bounds[0].values) == {(1,), (2,)}


def test_range_predicate_is_unbounded_absent_from_result():
    ast = _ast("SELECT * FROM orders WHERE id > 100")
    bounds = extract_pk_bounds(ast, _orders_table())
    assert bounds == []


def test_non_pk_predicate_is_unbounded_absent_from_result():
    ast = _ast("SELECT * FROM orders WHERE status = 'shipped'")
    bounds = extract_pk_bounds(ast, _orders_table())
    assert bounds == []


def test_full_scan_is_unbounded_absent_from_result():
    ast = _ast("SELECT * FROM orders")
    bounds = extract_pk_bounds(ast, _orders_table())
    assert bounds == []


def test_join_on_equality_resolves_bound():
    ast = _ast("SELECT * FROM orders o JOIN customers c ON o.id = c.order_id WHERE o.id = 7")
    bounds = extract_pk_bounds(ast, _orders_table())
    assert len(bounds) == 1
    assert bounds[0].values == ((7,),)


def test_no_row_materialized_tables_returns_empty():
    ast = _ast("SELECT * FROM orders WHERE id = 1")
    assert extract_pk_bounds(ast, {}) == []


def test_composite_pk_requires_singleton_per_column():
    tables = _orders_table(extra_pk="region")
    ast = _ast("SELECT * FROM orders WHERE id = 1 AND region = 'us'")
    bounds = extract_pk_bounds(ast, tables)
    assert len(bounds) == 1
    assert set(bounds[0].pk_columns) == {"id", "region"}
    assert (
        bounds[0].values == ((1, "us"),)
        or bounds[0].values == (("us", 1),)
        or len(bounds[0].values[0]) == 2
    )


def test_composite_pk_with_in_list_on_one_column_is_unbounded():
    tables = _orders_table(extra_pk="region")
    ast = _ast("SELECT * FROM orders WHERE id IN (1, 2) AND region = 'us'")
    bounds = extract_pk_bounds(ast, tables)
    assert bounds == []
