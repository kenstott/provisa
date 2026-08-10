# Copyright (c) 2026 Kenneth Stott
# Canary: 7a06ff66-0973-4905-99ec-758d32701f9d
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared GROUP BY prompt guidance and semantic validator for every SQL-generating NL path.

Used by both `provisa/nl/prompt.py` (the six-target NL pipeline's "sql" instructions,
though that target is currently bypassed in favor of `_generate_sql_from_nl`) and
`provisa/api/data/endpoint_dev.py` (`_run_sql_generation_loop`, the SQL Explorer /
`_generate_sql_from_nl` path) so the two SQL generators can't silently drift apart on
this specific, previously-observed failure mode.
"""

# Requirements: REQ-355, REQ-356, REQ-464

from __future__ import annotations

# The example uses a schema (orders/customers/regions) deliberately unrelated to any
# bundled demo dataset (e.g. the pet_store fixture) — an example drawn from demo table
# names would bias the model toward pattern-matching those specific names rather than
# generalizing the underlying GROUP BY rule to arbitrary schemas.
GROUP_BY_GUIDANCE = (
    "If the question asks to group/aggregate by a dimension (e.g. 'by customer') AND also wants "
    "'details' — extra columns from the grouped table itself, or rows from another joined table "
    "— do NOT use GROUP BY at all. GROUP BY forces every raw SELECT column to become part of the "
    "grouping key, which either fragments the result (one row per unique detail combination "
    "instead of one row per dimension) or requires wrapping every detail column in an aggregate "
    "function, which is unnecessarily complex and engine-fragile (e.g. DISTINCT inside "
    "json_agg/jsonb_agg is rejected by this engine). Instead, SELECT one row per dimension "
    "directly from the dimension table, with the aggregate measure and any one-to-many detail "
    "list computed as scalar correlated subqueries in the SELECT list, each correlated back to "
    "the outer row by the dimension's key. Only use GROUP BY when the question wants pure "
    "aggregate measures and no extra detail columns.\n"
    "Match the aggregation to the actual cardinality — do not reach for json_agg by default. If a "
    "joined table has (or is constrained to have) at most one matching row per dimension row — a "
    "1:1 or many:1 relationship, e.g. each order has exactly one customer — just JOIN it directly "
    "(or use a plain scalar correlated subquery selecting its columns) with no aggregate function "
    "at all; there's nothing to collapse. Only use json_agg/array_agg, wrapped in a scalar "
    "correlated subquery, when a joined table can genuinely have MULTIPLE matching rows per "
    "dimension row (a real 1:N relationship, e.g. one customer has many orders) — a single SELECT "
    "column can only hold one value per row, so multiple rows must be collapsed into one JSON "
    "array/scalar to fit that slot.\n"
    "Example — question: 'show aggregate count of orders by customer with customer and region "
    "details'. WRONG (GROUP BY forces customers.name/email and regions.* into the grouping key, "
    "fragmenting one row per customer into one row per customer+region+detail combo):\n"
    "  SELECT customers.id AS customer_id, customers.name, customers.email, "
    "regions.id AS region_id, regions.name AS region_name, COUNT(orders.id) AS order_count\n"
    "  FROM customers JOIN orders ON customers.id = orders.customer_id "
    "JOIN regions ON orders.region_id = regions.id\n"
    "  GROUP BY customers.id, customers.name, customers.email, regions.id, regions.name\n"
    "RIGHT (no GROUP BY; one row per customer directly from customers; the aggregate count and "
    "the one-to-many region list are each a scalar correlated subquery keyed on customers.id):\n"
    "  SELECT customers.id AS customer_id, customers.name, customers.email,\n"
    "    (SELECT COUNT(*) FROM orders WHERE orders.customer_id = customers.id) AS order_count,\n"
    "    (SELECT json_agg(jsonb_build_object('id', regions.id, 'name', regions.name))\n"
    "     FROM orders JOIN regions ON orders.region_id = regions.id\n"
    "     WHERE orders.customer_id = customers.id) AS region_details\n"
    "  FROM customers"
)


def check_group_by_semantics(stmt) -> str | None:  # REQ-464
    """Return an error message if a SELECT's raw column list disagrees with its GROUP BY.

    Every raw (non-aggregated) projection must be exactly one of the GROUP BY
    expressions; anything else silently becomes an extra grouping key and
    fragments the aggregate result.
    """
    import sqlglot.expressions as exp

    for select in stmt.find_all(exp.Select):
        group = select.args.get("group")
        if not group:
            continue
        group_keys = {g.sql(dialect="postgres") for g in group.expressions}
        for projection in select.expressions:
            target = projection.this if isinstance(projection, exp.Alias) else projection
            if isinstance(target, exp.AggFunc):
                continue
            if isinstance(target, exp.Anonymous) and target.this.lower() in (
                "json_agg",
                "array_agg",
                "string_agg",
            ):
                continue
            if isinstance(target, (exp.Literal, exp.Star)):
                continue
            if target.sql(dialect="postgres") in group_keys:
                continue
            return (
                f"GROUP BY error: column '{target.sql(dialect='postgres')}' is selected raw "
                "but is not one of the GROUP BY columns and is not wrapped in an aggregate "
                "function (COUNT/SUM/AVG/MIN/MAX) or json_agg/array_agg/string_agg. Either "
                "add it to GROUP BY (if it's a grouping dimension) or aggregate it — do not "
                "select it bare."
            )
    return None


def check_distinct_json_agg(stmt) -> str | None:  # REQ-464
    """Return an error message if DISTINCT is used inside json_agg/jsonb_agg.

    This engine rejects DISTINCT inside JSON_AGG/JSONB_AGG ("DISTINCT is not supported for
    non-aggregation functions"). Columns from the grouped table are functionally dependent on
    the group key and need no dedup (wrap each in max()/min() instead); columns from a joined
    one-to-many table should use plain json_agg without DISTINCT.
    """
    import sqlglot.expressions as exp

    for node in stmt.find_all((exp.JSONArrayAgg, exp.Anonymous)):
        if isinstance(node, exp.Anonymous) and node.this.lower() not in ("json_agg", "jsonb_agg"):
            continue
        if node.find(exp.Distinct):
            name = node.this if isinstance(node, exp.Anonymous) else "json_agg"
            return (
                f"{name} error: DISTINCT is not supported inside JSON_AGG/JSONB_AGG on this "
                "engine. If the aggregated columns come from the same table as the GROUP BY key, "
                "they are functionally dependent on it (one value per group) — wrap each column "
                "individually in max() or min() and build the JSON object outside the aggregate "
                "instead of using json_agg(DISTINCT jsonb_build_object(...)). If they come from a "
                "joined one-to-many table, use plain json_agg(jsonb_build_object(...)) with no "
                "DISTINCT."
            )
    return None
