# Copyright (c) 2026 Kenneth Stott
# Canary: 6cf4a8da-a7e9-4c66-a038-586b4b46b44b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.compiler.sql_rewrite.propagate_literal_join_predicates (REQ-1880).

Pure SQL-string-in, SQL-string-out -- no live engine, no Docker/network.
"""

from __future__ import annotations

import sqlglot

from provisa.compiler.sql_rewrite import propagate_literal_join_predicates

# The far table ("d", order_docs) is the only one whose connector has
# predicate_pushdown=True, join_pushdown=False (PgWrappersMongoDbConnector's declared shape).
_ELIGIBLE = {("perf_bench", "order_docs")}
_TYPES = {
    ("perf_bench", "orders", "order_id"): "integer",
    ("perf_bench", "order_docs", "order_id"): "integer",
}


def _norm(sql: str) -> str:
    return sqlglot.parse_one(sql, read="postgres").sql(dialect="postgres")


def test_inner_join_equality_propagates() -> None:
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id = 1"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id = 1 AND d.order_id = 1"
    )


def test_inner_join_between_propagates() -> None:
    sql = (
        'SELECT o.order_id, d.status FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id BETWEEN 1 AND 5"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(
        'SELECT o.order_id, d.status FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id BETWEEN 1 AND 5 AND d.order_id BETWEEN 1 AND 5"
    )


def test_inner_join_in_list_propagates() -> None:
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id IN (1, 2, 3)"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id IN (1, 2, 3) AND d.order_id IN (1, 2, 3)"
    )


def test_left_outer_join_does_not_propagate() -> None:
    # LEFT JOIN preserves a non-matching orders row with d.order_id NULL-extended. Propagating
    # `d.order_id = 1` would turn that preserved-but-non-matching row into one the WHERE clause
    # then excludes (NULL = 1 is NULL, not true) -- changing the result set. Must not propagate.
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'LEFT JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id = 1"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(sql)


def test_right_outer_join_does_not_propagate() -> None:
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'RIGHT JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id = 1"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(sql)


def test_full_outer_join_does_not_propagate() -> None:
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'FULL OUTER JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id = 1"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(sql)


def test_correlated_predicate_does_not_propagate() -> None:
    # o.order_id = o.customer_id references another column, not a literal -- never a "constant".
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id = o.customer_id"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(sql)


def test_now_function_predicate_does_not_propagate() -> None:
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.created_at = now()"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    # unchanged ⇒ the function returns the ORIGINAL text verbatim, not a re-rendered parse (which
    # would normalize now() -> CURRENT_TIMESTAMP and mask whether a rewrite actually happened).
    assert out == sql


def test_mismatched_types_does_not_propagate() -> None:
    # order_id is integer on the driving side but varchar on the target side -- a literal
    # comparison would not mean the same thing on both sides, so the propagation is skipped.
    types = {
        ("perf_bench", "orders", "order_id"): "integer",
        ("perf_bench", "order_docs", "order_id"): "varchar",
    }
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id = 1"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, types)
    assert out == _norm(sql)


def test_missing_column_types_does_not_propagate() -> None:
    # No type info at all for either side -- conservative default: skip, don't guess compatible.
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id = 1"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, None)
    assert out == _norm(sql)


def test_ineligible_target_does_not_propagate() -> None:
    # order_events' connector isn't in eligible_targets (e.g. it has join_pushdown=True already,
    # or predicate_pushdown=False) -- applying the rewrite there would be pure overhead/no benefit.
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_events" AS e ON e.order_id = o.order_id '
        "WHERE o.order_id = 1"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(sql)


def test_or_wrapped_predicate_does_not_propagate() -> None:
    # o.order_id = 1 only holds when the OR is true via THAT branch -- not guaranteed for every
    # row satisfying the whole WHERE clause, so it is never treated as an unconditional literal.
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id = 1 OR o.status = 'shipped'"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(sql)


def test_three_way_join_only_propagates_to_eligible_target() -> None:
    # Matches the real federated_join bench query shape (orders/order_events/order_docs): only
    # the mongo-backed order_docs (predicate_pushdown=True, join_pushdown=False) is eligible.
    sql = (
        'SELECT o.order_id, e.event_type, d.status FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_events" AS e ON e.order_id = o.order_id '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id BETWEEN 1 AND 5"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(
        'SELECT o.order_id, e.event_type, d.status FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_events" AS e ON e.order_id = o.order_id '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id BETWEEN 1 AND 5 AND d.order_id BETWEEN 1 AND 5"
    )


def test_no_join_is_a_no_op() -> None:
    sql = 'SELECT * FROM "perf_bench"."orders" AS o WHERE o.order_id = 1'
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(sql)


def test_empty_eligible_targets_is_a_no_op_short_circuit() -> None:
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id = 1"
    )
    assert propagate_literal_join_predicates(sql, "postgres", set(), _TYPES) == sql
