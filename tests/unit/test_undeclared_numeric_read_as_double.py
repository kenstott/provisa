# Copyright (c) 2026 Kenneth Stott
# Canary: ff1331bf-6381-4cfe-95ed-01d78593372e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1961: a numeric column the model records no size for is floating point on every read
path — and it is the source or engine that makes it so.

The governed statement's own projection casts such a column to double, so the value arrives
as its database's nearest double and nothing converts it afterwards (Arrow's column-wise cast
of a decimal gives 0.30000000000000004 for 0.3; a database's cast gives 0.3). Only the answer's
projection is cast: a filter, a join, an aggregate or any other expression over the column
computes on the exact value. A sized numeric is never cast.
"""

# Requirements: REQ-1961

from __future__ import annotations

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance, govern_fragment
from provisa.security.masking import MaskingRule, MaskType
from tests.write_governance import grants_over

_D = "DOUBLE PRECISION"


def _gov(**masks: MaskingRule) -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"sales.orders": 1, "orders": 1, "sales.refunds": 2}
    gov.all_columns = {
        1: [("id", "integer"), ("ratio", "numeric"), ("amount", "numeric(18,2)"), ("f", "double")],
        2: [("id", "integer"), ("ratio", "decimal")],
    }
    gov.masking_rules = {(1, name): (rule, "numeric") for name, rule in masks.items()}
    return gov


@pytest.mark.parametrize(
    ("sql", "governed"),
    [
        (
            "SELECT id, ratio, amount, f FROM sales.orders",
            f"SELECT id, CAST(ratio AS {_D}) AS ratio, amount, f FROM sales.orders",
        ),
        (
            "SELECT o.ratio AS r FROM sales.orders o",
            f"SELECT CAST(o.ratio AS {_D}) AS r FROM sales.orders AS o",
        ),
        (
            'SELECT "ratio" FROM sales.orders',
            f'SELECT CAST("ratio" AS {_D}) AS "ratio" FROM sales.orders',
        ),
    ],
)
def test_the_answers_projection_reads_an_undeclared_numeric_as_double(sql, governed):
    assert apply_governance(sql, _gov()) == governed


def test_a_star_is_expanded_and_its_undeclared_numeric_cast():
    """Governance expands a star wherever the registry knows the table's columns — the same
    registry that says the column is a numeric with no size — so the one column can be cast."""
    assert apply_governance("SELECT * FROM sales.orders", _gov()) == (
        'SELECT "orders"."id", '
        f'CAST("orders"."ratio" AS {_D}) AS "ratio", "orders"."amount", "orders"."f" '
        "FROM sales.orders"
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT SUM(ratio) AS total FROM sales.orders",
        "SELECT ratio * 2 AS twice FROM sales.orders",
        "SELECT id FROM sales.orders WHERE ratio > 0.5 ORDER BY ratio",
        "SELECT COALESCE(ratio, 0) AS r FROM sales.orders",
        "SELECT amount FROM sales.orders",
    ],
)
def test_an_expression_over_the_column_and_a_sized_numeric_are_untouched(sql):
    assert apply_governance(sql, _gov()) == sql


def test_only_the_answer_is_cast_not_a_subquery_a_cte_or_a_filter():
    governed = apply_governance(
        "WITH t AS (SELECT ratio FROM sales.orders) "
        "SELECT o.ratio FROM sales.orders AS o "
        "WHERE o.ratio IN (SELECT ratio FROM sales.refunds) AND EXISTS(SELECT ratio FROM t)",
        _gov(),
    )
    assert governed.count("CAST(") == 1
    assert f"SELECT CAST(o.ratio AS {_D}) AS ratio FROM sales.orders AS o" in governed


def test_each_branch_of_a_set_operation_that_is_the_answer_is_cast():
    assert apply_governance(
        "SELECT ratio FROM sales.orders UNION ALL SELECT ratio FROM sales.refunds", _gov()
    ) == (
        f"SELECT CAST(ratio AS {_D}) AS ratio FROM sales.orders "
        f"UNION ALL SELECT CAST(ratio AS {_D}) AS ratio FROM sales.refunds"
    )


def test_a_fragment_of_a_larger_statement_is_not_cast():
    assert govern_fragment("SELECT ratio FROM sales.orders", _gov()) == (
        "SELECT ratio FROM sales.orders"
    )


def test_a_write_is_not_cast():
    gov = _gov()
    gov.can_write = True
    gov.write_grants = {2: grants_over(gov.role_id, ["id", "ratio"])}
    gov.write_ops = {2: frozenset({"insert"})}
    governed = apply_governance(
        "INSERT INTO sales.refunds (id, ratio) SELECT id, ratio FROM sales.orders", gov
    )
    assert "CAST(" not in governed


# --- masks -----------------------------------------------------------------------------------------


def test_a_constant_mask_is_still_the_columns_number_and_is_cast():
    gov = _gov(ratio=MaskingRule(mask_type=MaskType.constant, value=0))
    assert apply_governance("SELECT ratio FROM sales.orders", gov) == (
        f"SELECT CAST(0 AS {_D}) AS ratio FROM sales.orders"
    )
    assert f'CAST(0 AS {_D}) AS "ratio"' in apply_governance("SELECT * FROM sales.orders", gov)


def test_a_null_constant_mask_is_cast():
    gov = _gov(ratio=MaskingRule(mask_type=MaskType.constant, value=None))
    assert apply_governance("SELECT ratio FROM sales.orders", gov) == (
        f"SELECT CAST(NULL AS {_D}) AS ratio FROM sales.orders"
    )


def test_a_regex_mask_has_made_the_column_text_and_is_not_cast():
    gov = _gov(ratio=MaskingRule(mask_type=MaskType.regex, pattern="[0-9]", replace="#"))
    governed = apply_governance("SELECT ratio FROM sales.orders", gov)
    assert "CAST(" not in governed and "PROVISA_REGEX_REPLACE(ratio" in governed
    assert "CAST(" not in apply_governance("SELECT * FROM sales.orders", gov)


def test_a_truncate_mask_has_made_the_column_something_else_and_is_not_cast():
    gov = _gov(ratio=MaskingRule(mask_type=MaskType.truncate, precision="month"))
    governed = apply_governance("SELECT ratio FROM sales.orders", gov)
    assert "CAST(" not in governed and "DATE_TRUNC('MONTH', ratio)" in governed


def test_a_fake_is_read_through_its_projection_and_cast():
    """A fake is not substituted per reference (``_maybe_mask_column`` leaves the column), so
    the answer reads the projection's fake as the column is read."""
    from provisa.compiler.stage2 import _as_read
    from sqlglot import exp

    gov = _gov(ratio=MaskingRule(mask_type=MaskType.fake))
    col = exp.column("ratio", "o")
    read = _as_read(col, col, {"o": 1}, gov)
    assert read.sql(dialect="postgres") == f"CAST(o.ratio AS {_D})"


# --- what the answer's shape says, and what each engine makes of the cast ---------------------------


def test_governing_twice_changes_nothing_more():
    once = apply_governance("SELECT ratio FROM sales.orders", _gov())
    assert apply_governance(once, _gov()) == once


def test_the_shape_of_the_governed_statement_says_double_precision():
    """So pgwire's Describe and its Execute agree, and no read path casts afterwards."""
    from types import SimpleNamespace

    from provisa.compiler.result_shape import RegisteredType, derive_result_shape

    columns = {
        1: [
            SimpleNamespace(column_name=name, data_type=declared)
            for name, declared in _gov().all_columns[1]
        ]
    }
    governed = apply_governance("SELECT id, ratio, amount FROM sales.orders", _gov())
    shape = derive_result_shape(
        governed,
        {"sales.orders": 1, "orders": 1},
        SimpleNamespace(
            physical_to_sql={},
            virtual_columns={},
            columns_outside_relation=set(),
            tables={},
            nested_columns={},
        ),
        columns,
    )
    assert shape == [("id", "integer"), ("ratio", _D), ("amount", "numeric(18,2)")]
    assert not isinstance(shape[1][1], RegisteredType)
    assert isinstance(shape[2][1], RegisteredType)


@pytest.mark.parametrize(
    ("dialect", "cast"),
    [
        ("postgres", "CAST(ratio AS DOUBLE PRECISION)"),
        ("duckdb", "CAST(ratio AS DOUBLE)"),
        ("trino", "CAST(ratio AS DOUBLE)"),
        ("mysql", "CAST(ratio AS DOUBLE)"),
        ("snowflake", "CAST(ratio AS DOUBLE)"),
        ("bigquery", "CAST(ratio AS FLOAT64)"),
        ("clickhouse", "CAST(ratio AS Nullable(Float64))"),
        ("tsql", "CAST(ratio AS FLOAT)"),
        ("oracle", "CAST(ratio AS DOUBLE PRECISION)"),
        ("sqlite", "CAST(ratio AS REAL)"),
    ],
)
def test_the_cast_is_written_in_each_routes_own_dialect(dialect, cast):
    import sqlglot

    governed = apply_governance("SELECT ratio FROM sales.orders", _gov())
    assert cast in sqlglot.transpile(governed, read="postgres", write=dialect)[0]


def test_duckdbs_own_cast_gives_the_nearest_double():
    """What the cast buys, on the engine that runs in-process: 0.3 arrives as 0.3."""
    import duckdb

    conn = duckdb.connect()
    conn.execute("CREATE TABLE orders (ratio DECIMAL(10,1))")
    conn.execute("INSERT INTO orders VALUES (0.3), (0.1)")
    assert conn.execute("SELECT CAST(ratio AS DOUBLE) FROM orders ORDER BY 1").fetchall() == [
        (0.1,),
        (0.3,),
    ]
