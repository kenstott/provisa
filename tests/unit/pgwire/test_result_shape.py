# Copyright (c) 2026 Kenneth Stott
# Canary: 1f7c4e2a-9d3b-4a68-b5e1-0c8f6d2a7b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement's result columns are derived from registered metadata alone (REQ-589, amended
2026-10-01): registered columns keep the registry's own type, computed expressions are typed by
annotation, and a column that cannot be typed is an error naming it."""

# Requirements: REQ-589

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.pgwire.result_shape import UnderivableColumn, derive_result_shape, table_columns

_COLUMNS = {
    1: [
        ColumnMetadata(column_name="order_id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="amount", data_type="numeric(18,2)", is_nullable=True),
        ColumnMetadata(column_name="region", data_type="varchar", is_nullable=True),
        ColumnMetadata(
            column_name="placed", data_type="timestamp with time zone", is_nullable=True
        ),
        ColumnMetadata(column_name="blob", data_type="LowCardinality(Nonsense)", is_nullable=True),
    ],
    2: [
        ColumnMetadata(column_name="id", data_type="bigint", is_nullable=False),
        ColumnMetadata(column_name="name", data_type="text", is_nullable=True),
    ],
}
_TABLES = {"sales.orders": 1, "orders": 1, "sales.customers": 2, "customers": 2}
_CTX = SimpleNamespace(physical_to_sql={}, virtual_columns={})


def _shape(sql: str, ctx=_CTX):
    return derive_result_shape(sql, _TABLES, ctx, _COLUMNS)


def test_registered_columns_keep_the_registrys_own_type():
    assert _shape(
        "SELECT order_id, amount AS total, placed FROM sales.orders WHERE order_id = 7"
    ) == [
        ("order_id", "integer"),
        ("total", "numeric(18,2)"),
        ("placed", "timestamp with time zone"),
    ]


def test_a_star_expands_to_the_tables_registered_columns_in_order():
    assert [name for name, _ in _shape("SELECT * FROM orders")] == [
        "order_id",
        "amount",
        "region",
        "placed",
        "blob",
    ]


def test_a_column_with_an_unparsable_registered_type_still_describes_by_that_type():
    assert _shape("SELECT blob FROM sales.orders") == [("blob", "LowCardinality(Nonsense)")]


def test_aggregates_and_expressions_are_typed_without_a_source():
    assert _shape(
        "SELECT count(*), max(order_id) AS top, region || '-x' AS tag, order_id + 1, "
        "CAST(amount AS TEXT), CASE WHEN order_id > 1 THEN 'a' ELSE 'b' END FROM sales.orders"
    ) == [
        ("count", "BIGINT"),
        ("top", "INT"),
        ("tag", "VARCHAR"),
        ("?column?", "INT"),
        ("amount", "TEXT"),
        ("case", "VARCHAR"),
    ]


def test_joined_tables_resolve_each_column_to_its_own_table():
    assert _shape(
        "SELECT o.order_id, c.name, c.id FROM sales.orders AS o JOIN customers AS c ON c.id = o.order_id"
    ) == [("order_id", "integer"), ("name", "text"), ("id", "bigint")]


def test_a_cte_and_a_subquery_carry_types_through():
    assert _shape(
        "WITH w AS (SELECT region, count(*) AS n FROM orders GROUP BY region) SELECT * FROM w"
    ) == [("region", "VARCHAR"), ("n", "BIGINT")]
    assert _shape("SELECT x.order_id FROM (SELECT order_id FROM orders) AS x") == [
        ("order_id", "INT")
    ]


def test_a_union_takes_its_shape_from_its_first_branch():
    assert _shape("SELECT order_id FROM orders UNION ALL SELECT id FROM customers") == [
        ("order_id", "INT")
    ]


def test_an_untyped_null_or_parameter_is_text_as_postgres_resolves_it():
    assert _shape(
        "SELECT NULL AS masked, $1 AS given, $2::int FROM orders JOIN customers ON true"
    ) == [
        ("masked", "text"),
        ("given", "text"),
        ("int4", "INT"),
    ]


def test_an_underivable_column_is_an_error_naming_it():
    with pytest.raises(UnderivableColumn, match="'mystery'.*UNDECLARED_UDF"):
        _shape("SELECT undeclared_udf(order_id) AS mystery FROM sales.orders")


def test_an_expression_over_an_unparsable_type_is_an_error_not_a_guess():
    with pytest.raises(UnderivableColumn, match="'doubled'"):
        _shape("SELECT blob * 2 AS doubled FROM sales.orders")


def test_a_write_describes_no_columns_and_returning_describes_the_tables():
    assert _shape("DELETE FROM sales.orders WHERE order_id = 9") == []
    assert _shape(
        "UPDATE sales.orders SET region = 'x' WHERE order_id = 1 RETURNING order_id, region"
    ) == [
        ("order_id", "integer"),
        ("region", "varchar"),
    ]
    with pytest.raises(UnderivableColumn, match="RETURNING"):
        _shape("DELETE FROM sales.orders RETURNING order_id + 1")


def test_a_statement_that_is_not_a_query_cannot_be_described():
    with pytest.raises(UnderivableColumn, match="Command"):
        _shape("VACUUM orders")


def test_only_columns_the_role_can_see_are_described_under_their_exposed_names():
    ctx = SimpleNamespace(
        physical_to_sql={(1, "order_id"): "orderId", (1, "region"): "region"},
        virtual_columns={1: {"_source": "orders"}},
    )
    assert table_columns(1, ctx, _COLUMNS) == {
        "orderId": "integer",
        "region": "varchar",
        "_source": "varchar",
    }
    assert _shape("SELECT * FROM sales.orders", ctx) == [
        ("orderId", "integer"),
        ("region", "varchar"),
        ("_source", "varchar"),
    ]
