# Copyright (c) 2025 Kenneth Stott
# Canary: fc40f392-67e8-47c8-b0be-564b35677d9a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for SQLGlot PG SQL → target dialect transpilation."""

import pytest

from provisa.transpiler.transpile import transpile, transpile_to_trino


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
            'LIMIT 10 OFFSET 5'
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
