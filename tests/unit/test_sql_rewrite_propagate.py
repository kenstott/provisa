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


# --- P10: a key-bounded nested read on an engine with no decorrelation is read once ---

_LIST = (
    'SELECT "t0"."order_id", COALESCE((SELECT json_agg(json_object(KEY \'status\' VALUE '
    '"t2"."status")) FROM "perf_bench"."order_docs" "t2" WHERE "t2"."order_id" = "t0"."order_id"), '
    'CAST(\'[]\' AS JSON)) AS "d" FROM "perf_bench"."orders" "t0" '
    'WHERE "t0"."order_id" >= 1 AND "t0"."order_id" <= 1000'
)


def _once(sql: str) -> str:
    return propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES, read_once=True)


def test_a_key_bounded_list_is_one_grouped_read_joined_by_key() -> None:
    assert _once(_LIST) == _norm(
        'SELECT "t0"."order_id", COALESCE("_read_0"."_value", CAST(\'[]\' AS JSON)) AS "d" '
        'FROM "perf_bench"."orders" "t0" LEFT JOIN (SELECT "t2"."order_id" AS "_key", '
        'json_agg(json_object(KEY \'status\' VALUE "t2"."status")) AS "_value" '
        'FROM "perf_bench"."order_docs" "t2" '
        'WHERE "t2"."order_id" >= 1 AND "t2"."order_id" <= 1000 GROUP BY "t2"."order_id") '
        'AS "_read_0" ON "_read_0"."_key" = "t0"."order_id" '
        'WHERE "t0"."order_id" >= 1 AND "t0"."order_id" <= 1000'
    )


def test_a_key_bounded_to_one_takes_the_head_of_each_key() -> None:
    out = _once(_CORR)
    tree = sqlglot.parse_one(out, read="postgres")
    assert 'SELECT DISTINCT ON ("t2"."order_id") "t2"."order_id" AS "_key"' in out
    assert tree.find(sqlglot.exp.Limit) is None
    assert tree.expressions[1].sql(dialect="postgres") == '"_read_0"."_value" AS "d"'


def test_a_read_is_left_correlated_unless_asked() -> None:
    assert "LEFT JOIN" not in propagate_literal_join_predicates(
        _LIST, "postgres", _ELIGIBLE, _TYPES
    )


def test_a_governed_condition_moves_with_the_read() -> None:
    sql = _LIST.replace(
        'WHERE "t2"."order_id" = "t0"."order_id"',
        'WHERE "t2"."status" = \'open\' AND "t2"."order_id" = "t0"."order_id"',
    )
    derived = next(
        s
        for s in sqlglot.parse_one(_once(sql), read="postgres").find_all(sqlglot.exp.Subquery)
        if s.alias == "_read_0"
    )
    assert '"t2"."status" = \'open\'' in derived.sql(dialect="postgres")


def test_an_open_range_stays_correlated() -> None:
    sql = _LIST.replace(' AND "t0"."order_id" <= 1000', "")
    assert "LEFT JOIN" not in _once(sql)


def test_a_read_with_another_outer_reference_stays_correlated() -> None:
    sql = _LIST.replace(
        'WHERE "t2"."order_id" = "t0"."order_id"',
        'WHERE "t2"."order_id" = "t0"."order_id" AND "t2"."status" = "t0"."state"',
    )
    out = _once(sql)
    assert "LEFT JOIN" not in out
    # Refused, it keeps every condition it came with.
    assert '"t2"."order_id" = "t0"."order_id"' in out


def test_a_per_parent_limit_stays_correlated() -> None:
    sql = (
        'SELECT "t0"."order_id", COALESCE((SELECT json_agg(_t) FROM (SELECT json_object(KEY '
        '\'status\' VALUE "t2"."status") AS _t FROM "perf_bench"."order_docs" "t2" '
        'WHERE "t2"."order_id" = "t0"."order_id" LIMIT 5) _sub), CAST(\'[]\' AS JSON)) AS "d" '
        'FROM "perf_bench"."orders" "t0" WHERE "t0"."order_id" = 1'
    )
    assert "LEFT JOIN" not in _once(sql)


def test_a_count_keeps_its_zero() -> None:
    sql = (
        'SELECT "t0"."order_id", (SELECT count(*) FROM "perf_bench"."order_docs" "t2" '
        'WHERE "t2"."order_id" = "t0"."order_id") AS "d" FROM "perf_bench"."orders" "t0" '
        'WHERE "t0"."order_id" = 1'
    )
    assert "LEFT JOIN" not in _once(sql)


def test_a_bind_parameter_bound_reads_once_where_the_engine_binds_by_number() -> None:
    sql = _LIST.replace(">= 1", ">= $1").replace("<= 1000", "<= $2")
    out = propagate_literal_join_predicates(
        sql, "postgres", _ELIGIBLE, _TYPES, allow_params=True, read_once=True
    )
    assert '"t2"."order_id" >= $1' in out and "LEFT JOIN (" in out


# --- REQ-039: stage 2 reads every registered table through the role's governed relation ---

# The perf bench's federated_join as stage 2 governs it: each table stands as a derived table
# under its own alias. Propagation stopped here (the bench's federated_join went 95 -> 459 ms on
# DuckDB): only a bare table was read as a table.
_GOVERNED = (
    'SELECT o.order_id, d.status FROM (SELECT "o"."order_id" AS "order_id", "o"."amount" AS '
    '"amount" FROM "perf_bench"."orders" AS "o") AS o JOIN (SELECT "d"."order_id" AS "order_id", '
    '"d"."status" AS "status" FROM "perf_bench"."order_docs" AS "d") AS d '
    "ON d.order_id = o.order_id WHERE o.order_id BETWEEN 1 AND 1000"
)


def test_a_governed_relation_is_propagated_to_as_its_table() -> None:
    out = propagate_literal_join_predicates(_GOVERNED, "postgres", _ELIGIBLE, _TYPES)
    where = sqlglot.parse_one(out, read="postgres").args["where"].this.sql(dialect="postgres")
    assert where == "o.order_id BETWEEN 1 AND 1000 AND d.order_id BETWEEN 1 AND 1000"


def test_a_masked_join_column_is_not_propagated_to() -> None:
    """A governed relation serving its join column masked: the predicate would be on the mask's
    value, not the table's column."""
    masked = _GOVERNED.replace(
        '(SELECT "d"."order_id" AS "order_id"', '(SELECT "d"."order_id" % 10 AS "order_id"'
    )
    assert propagate_literal_join_predicates(masked, "postgres", _ELIGIBLE, _TYPES) == masked


def test_a_renamed_join_column_is_typed_by_the_column_it_serves() -> None:
    """The predicate lands on the far side's own join column, by the name the relation serves it
    under, typed as the table column it is."""
    renamed = _GOVERNED.replace('"d"."order_id" AS "order_id"', '"d"."order_id" AS "doc_order"')
    renamed = renamed.replace("ON d.order_id = o.order_id", "ON d.doc_order = o.order_id")
    out = propagate_literal_join_predicates(renamed, "postgres", _ELIGIBLE, _TYPES)
    assert out.endswith("WHERE o.order_id BETWEEN 1 AND 1000 AND d.doc_order BETWEEN 1 AND 1000")


def test_a_governed_relation_in_a_correlated_subquery_gets_the_outer_bounds() -> None:
    sql = (
        'SELECT t0.order_id, (SELECT max(t2.status) FROM (SELECT "t2"."order_id" AS "order_id", '
        '"t2"."status" AS "status" FROM "perf_bench"."order_docs" AS "t2") AS t2 '
        "WHERE t2.order_id = t0.order_id) AS d "
        'FROM (SELECT "t0"."order_id" AS "order_id" FROM "perf_bench"."orders" AS "t0") AS t0 '
        "WHERE t0.order_id BETWEEN 1 AND 1000"
    )
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, _TYPES)
    assert "t2.order_id = t0.order_id AND t2.order_id BETWEEN 1 AND 1000" in out


def test_the_far_join_column_receives_the_predicate_by_its_own_name() -> None:
    sql = (
        'SELECT o.order_id FROM "perf_bench"."orders" AS o '
        'JOIN "perf_bench"."order_docs" AS d ON d."DocOrder" = o.order_id '
        "WHERE o.order_id = 5"
    )
    types = {**_TYPES, ("perf_bench", "order_docs", "docorder"): "integer"}
    out = propagate_literal_join_predicates(sql, "postgres", _ELIGIBLE, types)
    assert out.endswith('WHERE o.order_id = 5 AND d."DocOrder" = 5')
