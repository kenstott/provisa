# Copyright (c) 2026 Kenneth Stott
# Canary: 2af8ab62-fcda-4876-9364-1040f6919d99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLGlot-based SQL transpilation (REQ-066, REQ-068).

Supports PG SQL → Trino, and PG SQL → any target dialect for direct execution.
"""

# Requirements: REQ-066, REQ-068, REQ-229

import sqlglot

from provisa.transpiler.transpile_correlated import (
    rewrite_correlated_subqueries_for_trino,
)
import sqlglot.expressions as exp


# Valid SQLGlot write dialects for target sources
SUPPORTED_DIALECTS: set[str] = {
    "trino",
    "postgres",
    "mysql",
    "tsql",
    "duckdb",
    "snowflake",
    "bigquery",
    "clickhouse",
    "databricks",  # REQ-987: Databricks as a first-class federation engine (SQLGlot databricks dialect)
    "spark",  # Databricks/Spark SQL variant
}


def transpile_to_trino(pg_sql: str) -> str:  # REQ-066, REQ-068
    """Transpile PostgreSQL-dialect SQL to Trino SQL."""
    pg_sql = rewrite_correlated_subqueries_for_trino(pg_sql)
    result = transpile(pg_sql, "trino")
    result = _rewrite_json_build_object_for_trino(result)
    result = _rewrite_json_arrayagg_for_trino(result)
    return _rewrite_to_json_for_trino(result)


def _rewrite_to_json_for_trino(sql: str) -> str:
    """Replace to_json(x) with CAST(x AS JSON) for Trino.

    Trino has no to_json; its CAST(x AS JSON) already ENCODES a scalar/struct as a JSON value (the
    same semantics to_json gives on Postgres/DuckDB). The Cypher map-literal path emits to_json so the
    parse-semantics engines (Postgres/DuckDB) encode bare strings correctly; here it maps back to the
    Trino spelling."""
    # Parse failure must fail loud: returning input skips the required Trino rewrite.
    tree = sqlglot.parse_one(sql, read="trino")

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(node, exp.Anonymous) and node.name.upper() == "TO_JSON":
            if len(node.expressions) == 1:
                return exp.Cast(
                    this=node.expressions[0], to=exp.DataType(this=exp.DataType.Type.JSON)
                )
        return node

    return tree.transform(_transform).sql(dialect="trino")


def _rewrite_json_build_object_for_trino(sql: str) -> str:
    """Replace JSON_BUILD_OBJECT(k, v, ...) with JSON_OBJECT('k': v, ...) for Trino."""
    # Parse failure must fail loud: returning input skips the required Trino rewrite.
    tree = sqlglot.parse_one(sql, read="trino")

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(node, exp.Anonymous) and node.name.upper() == "JSON_BUILD_OBJECT":
            exprs = node.expressions
            pairs = [
                exp.JSONKeyValue(this=exprs[i], expression=exprs[i + 1])
                for i in range(0, len(exprs) - 1, 2)
            ]
            return exp.JSONObject(expressions=pairs)
        return node

    return tree.transform(_transform).sql(dialect="trino")


def _rewrite_json_arrayagg_for_trino(sql: str) -> str:
    """Replace JSON_ARRAYAGG(x) with json_format(CAST(ARRAY_AGG(JSON_PARSE(x)) AS JSON)).

    Trino 480 does not register json_arrayagg. The json_format wrapper produces
    VARCHAR so the result is safe to embed as a VALUE in JSON_OBJECT (Trino cannot
    coerce a JSON-typed array to varchar, which causes INVALID_CAST_ARGUMENT when
    the aggregate result is used inside an outer json_object(...) call).
    The Python _convert_value layer parses the VARCHAR back to a Python list/dict.
    """
    # Parse failure must fail loud: returning input skips the required Trino rewrite.
    tree = sqlglot.parse_one(sql, read="trino")

    json_type = exp.DataType(this=exp.DataType.Type.JSON)

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        inner = None
        if (
            isinstance(node, exp.Anonymous)
            and node.name.upper() == "JSON_ARRAYAGG"
            and node.expressions
        ):
            inner = node.expressions[0]
        elif isinstance(node, exp.JSONArrayAgg):
            inner = node.this
        if inner is not None:
            return exp.Anonymous(
                this="json_format",
                expressions=[
                    exp.Cast(
                        this=exp.ArrayAgg(
                            this=exp.Anonymous(this="JSON_PARSE", expressions=[inner.copy()])
                        ),
                        to=json_type.copy(),
                    )
                ],
            )
        return node

    return tree.transform(_transform).sql(dialect="trino")


def rewrite_json_object_to_build_object(pg_sql: str) -> str:  # REQ-902
    """Collapse SQL-standard JSON_OBJECT('k': v, ...) into flat json_build_object('k', v, ...).

    pg_duckdb's transparent execution path cannot emit PostgreSQL 16 nested-JSON syntax
    (JSON_OBJECT with colon separators); its DuckDB executor rejects the colon form and requires the
    flat json_build_object(k, v) form. json_build_object is valid in BOTH plain PostgreSQL and
    pg_duckdb, so this rewrite is safe for the whole pg engine regardless of which sources a query
    touches. Applied by PgBackend.transpile_physical after the base postgres transpile.
    """
    # Parse failure must fail loud: returning input skips the required pg_duckdb JSON rewrite.
    tree = sqlglot.parse_one(pg_sql, read="postgres")

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(node, exp.JSONObject):
            args: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]
            for kv in node.expressions or []:
                if not isinstance(kv, exp.JSONKeyValue):
                    return node  # unexpected shape — leave untouched, fail loud downstream
                args.append(kv.this)
                args.append(kv.expression)
            return exp.Anonymous(this="json_build_object", expressions=args)
        return node

    # JSON_OBJECT nodes nest (a value can be a subquery selecting another JSON_OBJECT); one transform
    # pass rewrites only nodes visited before their parent is replaced, so iterate until none remain.
    while list(tree.find_all(exp.JSONObject)):
        tree = tree.transform(_transform)
    return tree.sql(dialect="postgres")


def transpile(pg_sql: str, target_dialect: str) -> str:  # REQ-066, REQ-068, REQ-229
    """Transpile PostgreSQL-dialect SQL to a target dialect.

    Args:
        pg_sql: SQL string in PostgreSQL dialect with double-quoted identifiers.
        target_dialect: SQLGlot dialect name (e.g. "trino", "postgres", "mysql").

    Returns:
        SQL string in target dialect.
    """
    results = sqlglot.transpile(pg_sql, read="postgres", write=target_dialect)
    if not results:
        raise ValueError(f"SQLGlot produced no output for: {pg_sql!r}")
    from provisa.observability.stage_trace import trace_stage

    trace_stage(f"transpile.{target_dialect}", results[0])
    return results[0]


# ── CTE rewriter ──────────────────────────────────────────────────────────────
# Trino does not support correlated scalar subqueries in SELECT.
# _rewrite_correlated_json_to_ctes hoists each json_object/json_agg correlated
# subquery from SELECT into a CTE, then replaces it with a LEFT JOIN reference.
