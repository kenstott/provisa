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


# --- REQ-1880 (amended): correlated select-list subqueries (GraphQL nested relationships) ---

_CORR = (
    'SELECT "t0"."order_id", (SELECT json_object(KEY \'status\' VALUE "t2"."status") '
    'FROM "perf_bench"."order_docs" "t2" WHERE "t2"."order_id" = "t0"."order_id" LIMIT 1) AS "d" '
    'FROM "perf_bench"."orders" "t0" WHERE "t0"."order_id" >= 1 AND "t0"."order_id" <= 1000'
)


def test_range_propagates_into_a_correlated_select_list_subquery() -> None:
    out = propagate_literal_join_predicates(
        'SELECT t0.order_id, (SELECT max(t2.status) FROM "perf_bench"."order_docs" AS t2 '
        "WHERE t2.order_id = t0.order_id) AS d "
        'FROM "perf_bench"."orders" AS t0 WHERE t0.order_id BETWEEN 1 AND 1000',
        "postgres",
        _ELIGIBLE,
        _TYPES,
    )
    assert out == _norm(
        'SELECT t0.order_id, (SELECT max(t2.status) FROM "perf_bench"."order_docs" AS t2 '
        "WHERE t2.order_id = t0.order_id AND t2.order_id BETWEEN 1 AND 1000) AS d "
        'FROM "perf_bench"."orders" AS t0 WHERE t0.order_id BETWEEN 1 AND 1000'
    )


def test_the_graphql_nested_relationship_shape_gets_the_outer_bounds() -> None:
    """Separate >= / <= conjuncts (what the GraphQL compiler emits) each propagate."""
    out = propagate_literal_join_predicates(_CORR, "postgres", _ELIGIBLE, _TYPES)
    inner = sqlglot.parse_one(out, read="postgres").expressions[1].find(sqlglot.exp.Select)
    where = inner.args["where"].sql(dialect="postgres")
    assert '"t2"."order_id" >= 1' in where and '"t2"."order_id" <= 1000' in where


def test_a_subquery_on_an_ineligible_table_is_left_alone() -> None:
    out = propagate_literal_join_predicates(_CORR, "postgres", {("perf_bench", "other")}, _TYPES)
    assert out == _CORR


def test_an_uncorrelated_subquery_is_left_alone() -> None:
    sql = (
        'SELECT t0.order_id, (SELECT max(t2.status) FROM "perf_bench"."order_docs" AS t2 '
        "WHERE t2.status = 'x') AS d "
        'FROM "perf_bench"."orders" AS t0 WHERE t0.order_id = 5'
    )
    assert propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES) == sql


def test_an_outer_predicate_inside_an_or_does_not_propagate_into_a_subquery() -> None:
    sql = (
        'SELECT t0.order_id, (SELECT max(t2.status) FROM "perf_bench"."order_docs" AS t2 '
        "WHERE t2.order_id = t0.order_id) AS d "
        'FROM "perf_bench"."orders" AS t0 WHERE t0.order_id = 5 OR t0.amount > 3'
    )
    assert propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES) == sql


def test_incompatible_column_types_do_not_propagate_into_a_subquery() -> None:
    types = {**_TYPES, ("perf_bench", "order_docs", "order_id"): "varchar"}
    assert propagate_literal_join_predicates(_CORR, "postgres", _ELIGIBLE, types) == _CORR


def test_a_range_comparison_propagates_across_an_inner_join() -> None:
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE o.order_id >= 10 AND 20 >= o.order_id"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert out == _norm(
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d.order_id = o.order_id '
        "WHERE ((o.order_id >= 10 AND 20 >= o.order_id) AND d.order_id >= 10) AND 20 >= d.order_id"
    )


# --- Result preservation: run original vs propagated SQL on real data (in-memory DuckDB) ---

import duckdb  # noqa: E402
import pytest  # noqa: E402


@pytest.fixture(scope="module")
def _db():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA perf_bench")
    con.execute("CREATE TABLE perf_bench.orders (order_id INTEGER, amount INTEGER)")
    con.execute("CREATE TABLE perf_bench.order_docs (order_id INTEGER, status VARCHAR)")
    con.execute("INSERT INTO perf_bench.orders SELECT i, i % 7 FROM range(1, 60) t(i)")
    # docs: gaps (no doc for multiples of 5), duplicates for multiples of 3, NULL keys, out-of-range
    con.execute(
        "INSERT INTO perf_bench.order_docs "
        "SELECT i, 's' || (i % 4) FROM range(1, 80) t(i) WHERE i % 5 <> 0 "
        "UNION ALL SELECT i, 'dup' FROM range(1, 80) t(i) WHERE i % 3 = 0 "
        "UNION ALL SELECT NULL, 'nullkey' FROM range(3)"
    )
    yield con
    con.close()


def _rows(con, sql: str) -> list:
    duck = sqlglot.transpile(sql, read="postgres", write="duckdb")[0]
    return sorted(con.execute(duck).fetchall(), key=repr)


_SEMANTIC_CASES = [
    # correlated select-list subqueries
    "SELECT t0.order_id, (SELECT max(t2.status) FROM perf_bench.order_docs AS t2 "
    "WHERE t2.order_id = t0.order_id) AS d FROM perf_bench.orders AS t0 "
    "WHERE t0.order_id >= 3 AND t0.order_id <= 40",
    "SELECT t0.order_id, (SELECT count(*) FROM perf_bench.order_docs AS t2 "
    "WHERE t2.order_id = t0.order_id) AS n FROM perf_bench.orders AS t0 "
    "WHERE t0.order_id IN (5, 6, 9, 55)",
    "SELECT t0.order_id, (SELECT t2.status FROM perf_bench.order_docs AS t2 "
    "WHERE t2.order_id = t0.order_id ORDER BY t2.status LIMIT 1) AS d "
    "FROM perf_bench.orders AS t0 WHERE t0.order_id BETWEEN 10 AND 30",
    # predicate on an outer LEFT-joined side, subquery correlated to the preserved side
    "SELECT t0.order_id, (SELECT max(t2.status) FROM perf_bench.order_docs AS t2 "
    "WHERE t2.order_id = t0.order_id) AS d FROM perf_bench.orders AS t0 "
    "LEFT JOIN perf_bench.order_docs AS x ON x.order_id = t0.order_id "
    "WHERE t0.order_id < 20",
    # LEFT join: must NOT be propagated (would drop preserved rows)
    "SELECT o.order_id, d.status FROM perf_bench.orders AS o "
    "LEFT JOIN perf_bench.order_docs AS d ON d.order_id = o.order_id "
    "WHERE o.order_id BETWEEN 1 AND 25",
    # INNER join ranges
    "SELECT o.order_id, d.status FROM perf_bench.orders AS o "
    "JOIN perf_bench.order_docs AS d ON d.order_id = o.order_id "
    "WHERE o.order_id >= 4 AND o.order_id < 33",
    # predicate under OR: must not propagate
    "SELECT t0.order_id, (SELECT max(t2.status) FROM perf_bench.order_docs AS t2 "
    "WHERE t2.order_id = t0.order_id) AS d FROM perf_bench.orders AS t0 "
    "WHERE t0.order_id = 5 OR t0.amount = 3",
]


@pytest.mark.parametrize("sql", _SEMANTIC_CASES)
def test_propagation_never_changes_the_result(_db, sql: str) -> None:
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert _rows(_db, out) == _rows(_db, sql)


def test_a_left_join_is_never_propagated_across() -> None:
    sql = _SEMANTIC_CASES[4]
    assert propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES) == sql


# --- Bind parameters ($N): propagated only when the engine binds by number ---

_PARAM_CORR = (
    "SELECT t0.order_id, (SELECT max(t2.status) FROM perf_bench.order_docs AS t2 "
    "WHERE t2.order_id = t0.order_id) AS d FROM perf_bench.orders AS t0 "
    "WHERE t0.order_id >= $1 AND t0.order_id <= $2"
)


def test_a_bind_parameter_is_not_propagated_by_default() -> None:
    assert propagate_literal_join_predicates(_PARAM_CORR, "postgres", _ELIGIBLE, _TYPES) == (
        _PARAM_CORR
    )


def test_a_bind_parameter_propagates_when_the_engine_binds_by_number() -> None:
    out = propagate_literal_join_predicates(
        _PARAM_CORR, "postgres", _ELIGIBLE, _TYPES, allow_params=True
    )
    inner = sqlglot.parse_one(out, read="postgres").expressions[1].find(sqlglot.exp.Select)
    where = inner.args["where"].sql(dialect="postgres")
    assert "t2.order_id >= $1" in where and "t2.order_id <= $2" in where


def test_a_propagated_bind_parameter_returns_the_same_rows(_db) -> None:
    out = propagate_literal_join_predicates(
        _PARAM_CORR, "postgres", _ELIGIBLE, _TYPES, allow_params=True
    )
    params = [3, 40]

    def run(sql: str) -> list:
        duck = sqlglot.transpile(sql, read="postgres", write="duckdb")[0]
        return sorted(_db.execute(duck, params).fetchall(), key=repr)

    assert run(out) == run(_PARAM_CORR)
