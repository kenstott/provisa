# Copyright (c) 2026 Kenneth Stott
# Canary: 8dfcf1a4-bd0f-4b1e-b0cc-dd2005340b30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the SQLGlot-based SQL transpiler (transpile.py + router.py)."""

from __future__ import annotations

import pytest

from provisa.transpiler.transpile import (
    SUPPORTED_DIALECTS,
    transpile,
    transpile_to_trino,
)
from provisa.transpiler.router import (
    Route,
    RouteDecision,
    API_SOURCES,
    VIRTUAL_SOURCES,
    decide_route,
)
from provisa.executor.drivers.registry import has_driver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TYPES: dict[str, str] = {
    "pg1": "postgresql",
    "pg2": "postgresql",
    "mysql1": "mysql",
    "mssql1": "sqlserver",
    "duck1": "duckdb",
    "sf1": "snowflake",
    "bq1": "bigquery",
    "mongo1": "mongodb",
    "cass1": "cassandra",
    "kafka1": "kafka",
    "oapi1": "openapi",
}

_DIALECTS: dict[str, str] = {
    "pg1": "postgres",
    "pg2": "postgres",
    "mysql1": "mysql",
    "mssql1": "tsql",
    "duck1": "duckdb",
    "sf1": "snowflake",
    "bq1": "bigquery",
    "mongo1": "mongodb",
    "cass1": "cassandra",
    "kafka1": "kafka",
    "oapi1": "openapi",
}


# ---------------------------------------------------------------------------
# TestSupportedDialects
# ---------------------------------------------------------------------------


class TestSupportedDialects:
    def test_all_expected_dialects_present(self):
        assert "trino" in SUPPORTED_DIALECTS
        assert "postgres" in SUPPORTED_DIALECTS
        assert "mysql" in SUPPORTED_DIALECTS
        assert "tsql" in SUPPORTED_DIALECTS
        assert "duckdb" in SUPPORTED_DIALECTS
        assert "snowflake" in SUPPORTED_DIALECTS
        assert "bigquery" in SUPPORTED_DIALECTS

    def test_dialect_count(self):
        assert len(SUPPORTED_DIALECTS) == 7


# ---------------------------------------------------------------------------
# TestTranspileFunction — basic dialect conversions
# ---------------------------------------------------------------------------


class TestTranspileFunction:
    """Test the core transpile() function against every supported dialect."""

    def test_postgres_passthrough(self):
        sql = 'SELECT "id", "name" FROM "users"'
        result = transpile(sql, "postgres")
        assert "id" in result
        assert "name" in result
        assert "users" in result

    def test_to_mysql(self):
        sql = 'SELECT "id" FROM "orders" WHERE "amount" > 100'
        result = transpile(sql, "mysql")
        assert "orders" in result
        assert "100" in result

    def test_to_tsql(self):
        sql = 'SELECT "first_name" FROM "employees" LIMIT 10'
        result = transpile(sql, "tsql")
        # TSQL uses TOP instead of LIMIT
        assert "first_name" in result or "TOP" in result.upper()

    def test_to_trino(self):
        sql = 'SELECT "id", "value" FROM "metrics"'
        result = transpile(sql, "trino")
        assert "id" in result
        assert "metrics" in result

    def test_to_duckdb(self):
        sql = 'SELECT "col" FROM "tbl" WHERE "col" IS NOT NULL'
        result = transpile(sql, "duckdb")
        assert "col" in result
        # DuckDB may render IS NOT NULL or NOT ... IS NULL; both are semantically equivalent
        assert ("IS NOT NULL" in result) or ("IS NULL" in result)

    def test_to_snowflake(self):
        sql = 'SELECT "account_id", "revenue" FROM "sales"'
        result = transpile(sql, "snowflake")
        assert "account_id" in result
        assert "sales" in result

    def test_to_bigquery(self):
        sql = 'SELECT "user_id", "event_type" FROM "events"'
        result = transpile(sql, "bigquery")
        assert "user_id" in result
        assert "events" in result

    def test_transpile_to_trino_convenience(self):
        sql = 'SELECT "id" FROM "products"'
        result = transpile_to_trino(sql)
        assert "products" in result

    def test_returns_string(self):
        result = transpile("SELECT 1", "postgres")
        assert isinstance(result, str)

    def test_empty_select_produces_no_useful_sql(self):
        """SQLGlot with an empty string produces an empty or whitespace-only result."""
        result = transpile("", "postgres")
        # SQLGlot returns an empty string for empty input rather than raising
        assert result.strip() == ""


# ---------------------------------------------------------------------------
# TestCTEs
# ---------------------------------------------------------------------------


class TestCTEs:
    def test_cte_to_postgres(self):
        sql = """
        WITH ranked AS (
            SELECT "id", "score", ROW_NUMBER() OVER (ORDER BY "score" DESC) AS "rn"
            FROM "results"
        )
        SELECT "id", "score" FROM ranked WHERE "rn" = 1
        """
        result = transpile(sql, "postgres")
        assert "ranked" in result
        assert "score" in result

    def test_cte_to_trino(self):
        sql = """
        WITH top_orders AS (
            SELECT "customer_id", SUM("amount") AS "total"
            FROM "orders"
            GROUP BY "customer_id"
        )
        SELECT "customer_id", "total" FROM top_orders ORDER BY "total" DESC
        """
        result = transpile(sql, "trino")
        assert "top_orders" in result
        assert "customer_id" in result

    def test_cte_to_duckdb(self):
        sql = """
        WITH summary AS (SELECT COUNT(*) AS "cnt" FROM "events")
        SELECT "cnt" FROM summary
        """
        result = transpile(sql, "duckdb")
        assert "cnt" in result

    def test_multiple_ctes(self):
        sql = """
        WITH a AS (SELECT "id" FROM "t1"),
             b AS (SELECT "id" FROM "t2")
        SELECT a."id" FROM a JOIN b ON a."id" = b."id"
        """
        result = transpile(sql, "postgres")
        assert "a" in result
        assert "b" in result


# ---------------------------------------------------------------------------
# TestSubqueries
# ---------------------------------------------------------------------------


class TestSubqueries:
    def test_subquery_in_from(self):
        sql = """
        SELECT sub."id"
        FROM (SELECT "id" FROM "orders" WHERE "amount" > 100) AS sub
        """
        result = transpile(sql, "postgres")
        assert "orders" in result

    def test_subquery_in_where(self):
        sql = """
        SELECT "id" FROM "users"
        WHERE "id" IN (SELECT "user_id" FROM "admins")
        """
        result = transpile(sql, "trino")
        assert "admins" in result
        assert "IN" in result.upper()

    def test_correlated_subquery_to_mysql(self):
        sql = """
        SELECT "id" FROM "products" p
        WHERE "price" > (SELECT AVG("price") FROM "products")
        """
        result = transpile(sql, "mysql")
        assert "AVG" in result.upper()
        assert "price" in result

    def test_subquery_in_select_list(self):
        sql = """
        SELECT "id",
               (SELECT COUNT(*) FROM "orders" WHERE "user_id" = "users"."id") AS "order_count"
        FROM "users"
        """
        result = transpile(sql, "postgres")
        assert "order_count" in result


# ---------------------------------------------------------------------------
# TestWindowFunctions
# ---------------------------------------------------------------------------


class TestWindowFunctions:
    def test_row_number_to_postgres(self):
        sql = """
        SELECT "id", ROW_NUMBER() OVER (PARTITION BY "dept" ORDER BY "salary" DESC) AS "rn"
        FROM "employees"
        """
        result = transpile(sql, "postgres")
        assert "ROW_NUMBER" in result.upper()
        assert "dept" in result

    def test_rank_to_trino(self):
        sql = """
        SELECT "id", RANK() OVER (ORDER BY "score" DESC) AS "rank"
        FROM "scores"
        """
        result = transpile(sql, "trino")
        assert "RANK" in result.upper()

    def test_lag_lead_to_duckdb(self):
        sql = """
        SELECT "ts",
               LAG("value") OVER (ORDER BY "ts") AS "prev_val",
               LEAD("value") OVER (ORDER BY "ts") AS "next_val"
        FROM "time_series"
        """
        result = transpile(sql, "duckdb")
        assert "prev_val" in result
        assert "next_val" in result

    def test_sum_over_partition_to_bigquery(self):
        sql = """
        SELECT "region",
               "revenue",
               SUM("revenue") OVER (PARTITION BY "region") AS "region_total"
        FROM "sales"
        """
        result = transpile(sql, "bigquery")
        assert "region_total" in result

    def test_window_frame_to_snowflake(self):
        sql = """
        SELECT "id",
               SUM("amount") OVER (ORDER BY "id" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "rolling"
        FROM "txns"
        """
        result = transpile(sql, "snowflake")
        assert "rolling" in result


# ---------------------------------------------------------------------------
# TestTypeCoercions
# ---------------------------------------------------------------------------


class TestTypeCoercions:
    def test_cast_integer_postgres(self):
        sql = 'SELECT CAST("value" AS INTEGER) FROM "t"'
        result = transpile(sql, "postgres")
        assert "INTEGER" in result.upper() or "INT" in result.upper()

    def test_cast_to_text_trino(self):
        sql = 'SELECT CAST("id" AS TEXT) FROM "users"'
        result = transpile(sql, "trino")
        assert "VARCHAR" in result.upper() or "CAST" in result.upper()

    def test_cast_timestamp_mysql(self):
        sql = 'SELECT CAST("created_at" AS TIMESTAMP) FROM "orders"'
        result = transpile(sql, "mysql")
        assert "created_at" in result

    def test_boolean_literal(self):
        sql = "SELECT TRUE AS \"active\", FALSE AS \"inactive\" FROM \"t\""
        result = transpile(sql, "postgres")
        assert "active" in result

    def test_interval_expression(self):
        sql = "SELECT NOW() - INTERVAL '7 days' AS \"week_ago\""
        result = transpile(sql, "postgres")
        assert "week_ago" in result


# ---------------------------------------------------------------------------
# TestRouterDecideRoute — routing decisions
# ---------------------------------------------------------------------------


class TestRouterSingleSourceDirect:
    def test_postgresql_routes_direct(self):
        d = decide_route({"pg1"}, _TYPES, _DIALECTS)
        assert d.route == Route.DIRECT
        assert d.source_id == "pg1"
        assert d.dialect == "postgres"

    @pytest.mark.skipif(not has_driver("mysql"), reason="aiomysql not installed")
    def test_mysql_routes_direct(self):
        d = decide_route({"mysql1"}, _TYPES, _DIALECTS)
        assert d.route == Route.DIRECT
        assert d.dialect == "mysql"

    @pytest.mark.skipif(not has_driver("duckdb"), reason="duckdb driver not installed")
    def test_duckdb_routes_direct(self):
        d = decide_route({"duck1"}, _TYPES, _DIALECTS)
        assert d.route == Route.DIRECT
        assert d.dialect == "duckdb"


class TestRouterVirtualSources:
    def test_cassandra_routes_trino(self):
        d = decide_route({"cass1"}, _TYPES, _DIALECTS)
        assert d.route == Route.TRINO
        assert d.source_id is None
        assert "cassandra" in d.reason

    def test_mongodb_routes_trino(self):
        d = decide_route({"mongo1"}, _TYPES, _DIALECTS)
        assert d.route == Route.TRINO

    def test_kafka_routes_trino(self):
        d = decide_route({"kafka1"}, _TYPES, _DIALECTS)
        assert d.route == Route.TRINO

    def test_snowflake_without_driver_routes_trino(self):
        d = decide_route({"sf1"}, _TYPES, _DIALECTS)
        assert d.route == Route.TRINO

    def test_bigquery_without_driver_routes_trino(self):
        d = decide_route({"bq1"}, _TYPES, _DIALECTS)
        assert d.route == Route.TRINO


class TestRouterAPIRoute:
    def test_openapi_routes_api(self):
        d = decide_route({"oapi1"}, _TYPES, _DIALECTS)
        assert d.route == Route.API
        assert d.source_id == "oapi1"

    def test_api_sources_set_populated(self):
        assert "openapi" in API_SOURCES
        assert "graphql_api" in API_SOURCES
        assert "grpc_api" in API_SOURCES


class TestRouterMultiSource:
    def test_two_pg_sources_routes_trino(self):
        d = decide_route({"pg1", "pg2"}, _TYPES, _DIALECTS)
        assert d.route == Route.TRINO
        assert "multi-source" in d.reason

    def test_pg_plus_mongo_routes_trino(self):
        d = decide_route({"pg1", "mongo1"}, _TYPES, _DIALECTS)
        assert d.route == Route.TRINO


class TestRouterStewardHints:
    def test_steward_trino_overrides_direct(self):
        d = decide_route({"pg1"}, _TYPES, _DIALECTS, steward_hint="trino")
        assert d.route == Route.TRINO
        assert "steward" in d.reason

    def test_steward_direct_on_pg(self):
        d = decide_route({"pg1"}, _TYPES, _DIALECTS, steward_hint="direct")
        assert d.route == Route.DIRECT
        assert d.source_id == "pg1"

    def test_steward_direct_on_nosql_falls_through(self):
        """NoSQL has no direct driver; steward hint is ignored."""
        d = decide_route({"cass1"}, _TYPES, _DIALECTS, steward_hint="direct")
        assert d.route == Route.TRINO

    def test_steward_direct_on_multi_source_ignored(self):
        d = decide_route({"pg1", "pg2"}, _TYPES, _DIALECTS, steward_hint="direct")
        assert d.route == Route.TRINO


class TestRouterMutations:
    def test_mutation_always_direct(self):
        d = decide_route({"pg1"}, _TYPES, _DIALECTS, is_mutation=True)
        assert d.route == Route.DIRECT
        assert "mutation" in d.reason

    def test_mutation_routes_direct_even_for_nosql(self):
        d = decide_route({"mongo1"}, _TYPES, _DIALECTS, is_mutation=True)
        assert d.route == Route.DIRECT
        assert d.source_id == "mongo1"


class TestRouterJSONExtract:
    def test_json_extract_non_pg_dialect_routes_trino(self):
        """JSON path extraction on non-PG dialect forces Trino."""
        types = {"mysql1": "mysql"}
        dialects = {"mysql1": "mysql"}
        d = decide_route({"mysql1"}, types, dialects, has_json_extract=True)
        assert d.route == Route.TRINO
        assert "JSON" in d.reason or "json" in d.reason

    def test_json_extract_pg_dialect_stays_direct(self):
        """PG supports ->> natively; direct route is kept."""
        d = decide_route({"pg1"}, _TYPES, _DIALECTS, has_json_extract=True)
        assert d.route == Route.DIRECT


class TestRouterRouteDecision:
    def test_route_decision_is_frozen(self):
        d = decide_route({"pg1"}, _TYPES, _DIALECTS)
        assert isinstance(d, RouteDecision)
        with pytest.raises(Exception):
            d.route = Route.TRINO  # type: ignore[misc]

    def test_reason_is_always_non_empty_string(self):
        for sid, stype in _TYPES.items():
            d = decide_route({sid}, _TYPES, _DIALECTS)
            assert d.reason and isinstance(d.reason, str)

    def test_virtual_sources_set_populated(self):
        assert "cassandra" in VIRTUAL_SOURCES
        assert "mongodb" in VIRTUAL_SOURCES
        assert "kafka" in VIRTUAL_SOURCES
        assert "delta_lake" in VIRTUAL_SOURCES
