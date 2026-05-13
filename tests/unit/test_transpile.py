# Copyright (c) 2026 Kenneth Stott
# Canary: fc40f392-67e8-47c8-b0be-564b35677d9a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for SQLGlot PG SQL → target dialect transpilation."""

from provisa.transpiler.transpile import (
    transpile,
    transpile_to_trino,
    rewrite_correlated_subqueries_for_trino,
)


class TestTranspileToTrino:
    def test_simple_select(self):
        pg = 'SELECT "id", "amount" FROM "public"."orders"'
        trino_sql = transpile_to_trino(pg)
        assert "id" in trino_sql.lower()
        assert "amount" in trino_sql.lower()
        assert "orders" in trino_sql.lower()

    def test_where_with_placeholder(self):
        pg = 'SELECT "id" FROM "public"."orders" WHERE "region" = $1'
        trino_sql = transpile_to_trino(pg)
        assert "region" in trino_sql.lower()

    def test_left_join(self):
        pg = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id"'
        )
        trino_sql = transpile_to_trino(pg)
        assert "left" in trino_sql.lower() or "LEFT" in trino_sql
        assert "join" in trino_sql.lower()

    def test_limit_offset(self):
        pg = 'SELECT "id" FROM "public"."orders" LIMIT 10 OFFSET 20'
        trino_sql = transpile_to_trino(pg)
        assert "10" in trino_sql
        assert "20" in trino_sql

    def test_order_by(self):
        pg = 'SELECT "id" FROM "public"."orders" ORDER BY "created_at" DESC'
        trino_sql = transpile_to_trino(pg)
        assert "desc" in trino_sql.lower()

    def test_in_clause(self):
        pg = 'SELECT "id" FROM "public"."orders" WHERE "region" IN ($1, $2)'
        trino_sql = transpile_to_trino(pg)
        assert "in" in trino_sql.lower()

    def test_is_null(self):
        pg = 'SELECT "id" FROM "public"."orders" WHERE "region" IS NULL'
        trino_sql = transpile_to_trino(pg)
        assert "is null" in trino_sql.lower()

    def test_empty_sql_returns_empty(self):
        result = transpile_to_trino("")
        assert result == ""

    def test_combined_query(self):
        pg = (
            'SELECT "t0"."id", "t0"."amount", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id" '
            'WHERE "t0"."region" = $1 '
            'ORDER BY "t0"."amount" DESC '
            "LIMIT 10 OFFSET 5"
        )
        trino_sql = transpile_to_trino(pg)
        lower = trino_sql.lower()
        assert "left" in lower
        assert "join" in lower
        assert "order by" in lower or "order" in lower
        assert "10" in trino_sql


class TestMultiDialect:
    """Test PG SQL → various target dialects (REQ-068)."""

    PG_SQL = 'SELECT "id", "name" FROM "public"."orders" WHERE "id" = $1 LIMIT 10'

    def test_to_postgres(self):
        result = transpile(self.PG_SQL, "postgres")
        assert "id" in result.lower()
        assert "orders" in result.lower()

    def test_to_mysql(self):
        result = transpile(self.PG_SQL, "mysql")
        assert "orders" in result.lower()
        # MySQL uses backticks or no quotes
        assert "10" in result

    def test_to_tsql(self):
        result = transpile(self.PG_SQL, "tsql")
        assert "orders" in result.lower()

    def test_to_duckdb(self):
        result = transpile(self.PG_SQL, "duckdb")
        assert "orders" in result.lower()

    def test_to_snowflake(self):
        result = transpile(self.PG_SQL, "snowflake")
        assert "orders" in result.lower()

    def test_to_bigquery(self):
        result = transpile(self.PG_SQL, "bigquery")
        assert "orders" in result.lower()


class TestRewriteCorrelatedSubqueries:
    """Unit tests for rewrite_correlated_subqueries_for_trino (REQ-066 general path)."""

    # ── passthrough ──────────────────────────────────────────────────────────

    def test_no_correlated_subquery_unchanged(self):
        sql = "SELECT p.name FROM pets p"
        assert rewrite_correlated_subqueries_for_trino(sql) == sql

    def test_non_select_unchanged(self):
        sql = "INSERT INTO foo VALUES (1)"
        assert rewrite_correlated_subqueries_for_trino(sql) == sql

    def test_invalid_sql_unchanged(self):
        sql = "NOT VALID SQL %%% ###"
        assert rewrite_correlated_subqueries_for_trino(sql) == sql

    def test_uncorrelated_subquery_unchanged(self):
        # Subquery has no WHERE correlation to outer table
        sql = "SELECT p.name, (SELECT MAX(e.salary) FROM employees e) AS max_sal FROM pets p"
        result = rewrite_correlated_subqueries_for_trino(sql)
        assert result == sql

    # ── scalar correlated subquery ────────────────────────────────────────────

    def test_scalar_correlated_produces_cte(self):
        sql = "SELECT p.name, (SELECT e.last_name FROM employees e WHERE e.id = p.employee_id) AS emp FROM pets p"
        result = rewrite_correlated_subqueries_for_trino(sql)
        assert "WITH" in result
        assert "_grel_0" in result
        assert "LEFT JOIN" in result.upper()
        assert "ARBITRARY" in result.upper()
        assert "GROUP BY" in result.upper()

    def test_scalar_correlated_cte_contains_join_key(self):
        sql = "SELECT p.name, (SELECT e.last_name FROM employees e WHERE e.id = p.employee_id) AS emp FROM pets p"
        result = rewrite_correlated_subqueries_for_trino(sql)
        # join key column aliased as _jk0
        assert "_jk0" in result
        # ARBITRARY wraps the selected column
        assert "ARBITRARY" in result.upper()

    def test_scalar_correlated_replacement_references_cte(self):
        sql = "SELECT p.name, (SELECT e.last_name FROM employees e WHERE e.id = p.employee_id) AS emp FROM pets p"
        result = rewrite_correlated_subqueries_for_trino(sql)
        assert "_grel_0._val" in result or ("_grel_0" in result and "_val" in result)

    # ── local filter preserved ────────────────────────────────────────────────

    def test_local_filter_stays_in_cte_where(self):
        sql = "SELECT p.name, (SELECT e.last_name FROM employees e WHERE e.id = p.employee_id AND e.active = TRUE) AS emp FROM pets p"
        result = rewrite_correlated_subqueries_for_trino(sql)
        # local condition ends up inside the CTE, not in the LEFT JOIN ON
        assert "TRUE" in result.upper()
        # only one join key (_jk0 for e.id)
        assert "_jk0" in result
        assert "_jk1" not in result

    # ── aggregate (json_agg) path ─────────────────────────────────────────────

    def test_json_agg_uses_group_by_no_arbitrary(self):
        sql = "SELECT p.name, (SELECT JSON_AGG(JSON_OBJECT('n': e.last_name)) FROM employees e WHERE e.dept_id = p.dept_id) AS emps FROM pets p"
        result = rewrite_correlated_subqueries_for_trino(sql)
        assert "GROUP BY" in result.upper()
        assert "ARBITRARY" not in result.upper()

    def test_json_agg_cte_placed_before_join(self):
        sql = "SELECT p.name, (SELECT JSON_AGG(JSON_OBJECT('n': e.last_name)) FROM employees e WHERE e.dept_id = p.dept_id) AS emps FROM pets p"
        result = rewrite_correlated_subqueries_for_trino(sql)
        # CTE must appear before the SELECT
        with_pos = result.find("WITH")
        select_pos = result.rfind("SELECT")
        assert with_pos < select_pos

    # ── json_object (many-to-one) ─────────────────────────────────────────────

    def test_json_object_correlated_rewrites(self):
        sql = "SELECT p.name, (SELECT JSON_OBJECT('breed': b.name) FROM breeds b WHERE b.id = p.breed_id) AS breed_obj FROM pets p"
        result = rewrite_correlated_subqueries_for_trino(sql)
        assert "WITH" in result
        assert "_grel_0" in result
        assert "ARBITRARY" in result.upper()

    # ── multiple correlated subqueries ───────────────────────────────────────

    def test_two_correlated_subqueries_produce_two_ctes(self):
        sql = (
            "SELECT p.name,"
            " (SELECT e.last_name FROM employees e WHERE e.id = p.employee_id) AS emp,"
            " (SELECT b.name FROM breeds b WHERE b.id = p.breed_id) AS breed"
            " FROM pets p"
        )
        result = rewrite_correlated_subqueries_for_trino(sql)
        assert "_grel_0" in result
        assert "_grel_1" in result

    # ── hot table CTEs preserved in order ────────────────────────────────────

    def test_existing_hot_ctes_remain_first(self):
        sql = (
            "WITH _hot_shelter__employees AS (VALUES (1, 'Smith')) "
            "SELECT p.name, (SELECT e.last_name FROM employees e WHERE e.id = p.employee_id) AS emp "
            "FROM pets p"
        )
        result = rewrite_correlated_subqueries_for_trino(sql)
        hot_pos = result.find("_hot_shelter__employees")
        rel_pos = result.find("_grel_0")
        assert hot_pos != -1
        assert rel_pos != -1
        assert hot_pos < rel_pos

    # ── sampling wrapper ──────────────────────────────────────────────────────

    def test_sampling_wrapper_ctes_hoisted_to_outer(self):
        sql = (
            "SELECT * FROM ("
            "SELECT p.name, (SELECT e.last_name FROM employees e WHERE e.id = p.employee_id) AS emp "
            "FROM pets p"
            ") AS _sample LIMIT 100"
        )
        result = rewrite_correlated_subqueries_for_trino(sql)
        # CTE must be at the top-level WITH, before the outer SELECT *
        assert result.startswith("WITH ")
        assert "_grel_0" in result
        assert "LIMIT 100" in result

    def test_sampling_wrapper_inner_still_has_join(self):
        sql = (
            "SELECT * FROM ("
            "SELECT p.name, (SELECT e.last_name FROM employees e WHERE e.id = p.employee_id) AS emp "
            "FROM pets p"
            ") AS _sample LIMIT 50"
        )
        result = rewrite_correlated_subqueries_for_trino(sql)
        assert "LEFT JOIN" in result.upper()

    def test_sampling_wrapper_no_correlated_unchanged(self):
        sql = "SELECT * FROM (SELECT p.name FROM pets p) AS _sample LIMIT 10"
        assert rewrite_correlated_subqueries_for_trino(sql) == sql

    # ── transpile_to_trino integration (rewired) ──────────────────────────────

    def test_transpile_to_trino_handles_correlated(self):
        # transpile_to_trino now routes through the general rewriter
        sql = "SELECT p.name, (SELECT e.last_name FROM employees e WHERE e.id = p.employee_id) AS emp FROM pets p"
        result = transpile_to_trino(sql)
        assert "WITH" in result
        assert "_grel_0" in result
        assert "LEFT JOIN" in result.upper()

    def test_transpile_to_trino_plain_query_unaffected(self):
        sql = 'SELECT "id", "name" FROM "public"."orders" LIMIT 10'
        result = transpile_to_trino(sql)
        assert "orders" in result.lower()
        assert "WITH" not in result
