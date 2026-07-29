# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1e9c47-3a52-4f08-9d76-c84a0e21f5b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1317/REQ-1318: metric query expansion and metric-composed view generation."""

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.compiler.metric_expand import (
    expand_metric_calls_in_sql,
    expand_metric_query,
    generate_view_metrics_sql,
)
from provisa.core.models import Metric, ViewMetricsSpec

METRICS = {
    "net_revenue": Metric(
        name="net_revenue", expression="SUM(orders.amount) - SUM(orders.refunds)"
    ),
    "order_count": Metric(name="order_count", expression="COUNT(orders.id)"),
}

TABLES = {
    "orders": {
        "id": 1,
        "columns": ["id", "customer_id", "amount", "refunds", "region", "status"],
    },
    "customers": {"id": 2, "columns": ["id", "segment"]},
}

RELATIONSHIPS = [
    {
        "source_table_id": 1,
        "target_table_id": 2,
        "source_column": "customer_id",
        "target_column": "id",
    }
]


def _duck():
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE orders (id INT, customer_id INT, amount DOUBLE, refunds DOUBLE, "
        "region VARCHAR, status VARCHAR)"
    )
    conn.execute(
        "INSERT INTO orders VALUES "
        "(1, 10, 100, 5, 'east', 'complete'), "
        "(2, 10, 200, 0, 'east', 'complete'), "
        "(3, 20, 50, 10, 'west', 'complete'), "
        "(4, 20, 75, 0, 'west', 'pending')"
    )
    conn.execute("CREATE TABLE customers (id INT, segment VARCHAR)")
    conn.execute("INSERT INTO customers VALUES (10, 'enterprise'), (20, 'smb')")
    return conn


def _expand(sql: str):
    tree = sqlglot.parse_one(sql, read="postgres")
    return expand_metric_query(tree, METRICS, TABLES, RELATIONSHIPS)


# ── expand_metric_query (REQ-1317) ────────────────────────────────────────────


def test_no_metrics_reference_returns_none():
    assert _expand("SELECT region FROM orders") is None


def test_expand_one_dimension_computes_grouped_values():
    out = _expand("SELECT region, value FROM metrics.net_revenue")
    assert out is not None
    rows = dict(_duck().execute(out.sql(dialect="duckdb")).fetchall())
    assert rows == {"east": 295.0, "west": 115.0}


def test_expand_metric_name_accepted_as_value_column():
    out = _expand("SELECT region, net_revenue FROM metrics.net_revenue")
    assert out is not None
    rows = dict(_duck().execute(out.sql(dialect="duckdb")).fetchall())
    assert rows == {"east": 295.0, "west": 115.0}


def test_expand_two_dimensions_with_one_hop_join():
    out = _expand("SELECT region, segment, value FROM metrics.net_revenue")
    assert out is not None
    sql = out.sql(dialect="duckdb")
    rows = {(r[0], r[1]): r[2] for r in _duck().execute(sql).fetchall()}
    assert rows == {("east", "enterprise"): 295.0, ("west", "smb"): 115.0}


def test_expand_where_passthrough():
    out = _expand("SELECT region, value FROM metrics.net_revenue WHERE status = 'complete'")
    assert out is not None
    rows = dict(_duck().execute(out.sql(dialect="duckdb")).fetchall())
    assert rows == {"east": 295.0, "west": 40.0}


def test_expand_unknown_metric_is_named_error():
    with pytest.raises(ValueError, match="unknown metric 'nope'"):
        _expand("SELECT region, value FROM metrics.nope")


def test_expand_unresolvable_dimension_is_named_error():
    with pytest.raises(ValueError, match="'bogus'"):
        _expand("SELECT bogus, value FROM metrics.net_revenue")


def test_expand_select_star_rejected():
    with pytest.raises(ValueError, match="SELECT \\*"):
        _expand("SELECT * FROM metrics.net_revenue")


# ── generate_view_metrics_sql (REQ-1318) ─────────────────────────────────────


def test_generate_view_metrics_sql_multi_metric():
    spec = ViewMetricsSpec(
        metrics=["net_revenue", "order_count"], dimensions=["region", "segment"]
    )
    sql = generate_view_metrics_sql(spec, METRICS, TABLES, RELATIONSHIPS)
    rows = {(r[0], r[1]): (r[2], r[3]) for r in _duck().execute(sql).fetchall()}
    assert rows == {
        ("east", "enterprise"): (295.0, 2),
        ("west", "smb"): (115.0, 2),
    }


def test_generate_view_metrics_sql_filters_anded():
    spec = ViewMetricsSpec(
        metrics=["net_revenue"],
        dimensions=["region"],
        filters=["status = 'complete'", "amount > 60"],
    )
    sql = generate_view_metrics_sql(spec, METRICS, TABLES, RELATIONSHIPS)
    rows = dict(_duck().execute(sql).fetchall())
    assert rows == {"east": 295.0}


def test_generate_view_metrics_sql_unknown_metric():
    spec = ViewMetricsSpec(metrics=["ghost"], dimensions=["region"])
    with pytest.raises(ValueError, match="unknown metric 'ghost'"):
        generate_view_metrics_sql(spec, METRICS, TABLES, RELATIONSHIPS)


def test_generate_view_metrics_sql_unresolvable_dimension():
    spec = ViewMetricsSpec(metrics=["net_revenue"], dimensions=["planet"])
    with pytest.raises(ValueError, match="'planet'"):
        generate_view_metrics_sql(spec, METRICS, TABLES, RELATIONSHIPS)


# ── expand_metric_calls_in_sql (REQ-1318) ────────────────────────────────────


def test_expand_metric_calls_inlines_expression():
    sql, names = expand_metric_calls_in_sql(
        "SELECT region, metric('net_revenue') AS nr FROM orders GROUP BY region", METRICS
    )
    assert names == ["net_revenue"]
    rows = dict(_duck().execute(sql).fetchall())
    assert rows == {"east": 295.0, "west": 115.0}


def test_expand_metric_calls_no_calls_untouched():
    original = "SELECT region FROM orders"
    sql, names = expand_metric_calls_in_sql(original, METRICS)
    assert sql == original
    assert names == []


def test_expand_metric_calls_unknown_name():
    with pytest.raises(ValueError, match="unknown metric 'ghost'"):
        expand_metric_calls_in_sql("SELECT metric('ghost') FROM orders", METRICS)
