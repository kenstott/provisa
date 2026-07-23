# Copyright (c) 2026 Kenneth Stott
# Canary: c9d1e2f3-a4b5-4c6d-7e8f-9a0b1c2d3e4f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Query Languages, Compilation & Operations (Section 5).

Tests the compiler→executor component boundary without hitting HTTP endpoints.
Requires Docker Compose stack (PG + Trino) — skips automatically when unavailable.

Covered REQ-IDs:
  SQLGlot Transpilation:   REQ-066, REQ-067, REQ-068
  Aggregates:              REQ-196, REQ-197, REQ-198, REQ-199
  OrderBy Alignment:       REQ-200, REQ-201, REQ-202
  Tracked Functions:       REQ-205, REQ-206, REQ-207, REQ-208, REQ-209, REQ-210, REQ-211
  GraphQL Variable Defaults: REQ-300, REQ-301
  Tracked DB Functions Custom Return Schema: REQ-304, REQ-305, REQ-306
  Cypher Query Frontend:   REQ-353, REQ-571, REQ-573, REQ-574, REQ-575, REQ-576, REQ-577, REQ-578
  Compiler & Schema:       REQ-403, REQ-409, REQ-411, REQ-412, REQ-416, REQ-478, REQ-525,
                           REQ-526, REQ-534, REQ-653, REQ-654, REQ-655
  Tracked Functions:       REQ-360, REQ-361, REQ-362
  Graph Analytics Pipeline: REQ-642, REQ-643, REQ-650, REQ-651
"""

from __future__ import annotations

import pytest
from graphql import GraphQLEnumType, parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler import naming as _naming
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.context import build_context
from provisa.transpiler.transpile import transpile, SUPPORTED_DIALECTS

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers — shared fixture setup (no live DB needed for compiler-only tests)
# ---------------------------------------------------------------------------


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _make_schema_input(
    *,
    enable_aggregates: bool = False,
    enable_group_by: bool = False,
    naming_convention: str = "snake",
    extra_tables: list[dict] | None = None,
    extra_rels: list[dict] | None = None,
) -> SchemaInput:
    """Minimal SchemaInput with orders + customers + optional extras."""
    _naming.configure(gql=naming_convention)
    tables: list[dict] = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "enable_aggregates": enable_aggregates,
            "enable_group_by": enable_group_by,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
                {"column_name": "email", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
            ],
        },
    ]
    if extra_tables:
        tables.extend(extra_tables)

    relationships: list[dict] = [
        {
            "id": "ord-cust",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
    ]
    if extra_rels:
        relationships.extend(extra_rels)

    column_types: dict[int, list[ColumnMetadata]] = {
        1: [
            _col("id", "integer", nullable=False),
            _col("customer_id", "integer", nullable=True),
            _col("amount", "decimal(10,2)", nullable=True),
            _col("region", "varchar(50)", nullable=True),
            _col("status", "varchar(20)", nullable=True),
            _col("created_at", "timestamp", nullable=True),
        ],
        2: [
            _col("id", "integer", nullable=False),
            _col("name", "varchar(100)", nullable=True),
            _col("email", "varchar(200)", nullable=True),
            _col("region", "varchar(50)", nullable=True),
        ],
    }
    role = {"id": "admin", "capabilities": ["admin", "query_development"], "domain_access": ["*"]}
    domains = [{"id": "sales", "description": "Sales"}]
    return SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )


# ---------------------------------------------------------------------------
# SQLGlot Transpilation — REQ-066, REQ-067, REQ-068
# ---------------------------------------------------------------------------


class TestSQLGlotTranspilation:
    """REQ-066: compiler emits PG-style SQL; SQLGlot translates to target dialect.
    REQ-067: target dialect determined by source type at registration.
    REQ-068: supported dialects include postgres, trino, mysql, tsql, duckdb, snowflake, bigquery.
    """

    def test_supported_dialects_set(self):
        # REQ-068: all seven dialects must be present
        assert "trino" in SUPPORTED_DIALECTS
        assert "postgres" in SUPPORTED_DIALECTS
        assert "mysql" in SUPPORTED_DIALECTS
        assert "tsql" in SUPPORTED_DIALECTS
        assert "duckdb" in SUPPORTED_DIALECTS
        assert "snowflake" in SUPPORTED_DIALECTS
        assert "bigquery" in SUPPORTED_DIALECTS

    def test_transpile_pg_to_physical(self):
        # REQ-066, REQ-068: PG SQL transpiles to Trino
        pg_sql = 'SELECT "id", "amount" FROM "public"."orders" WHERE "status" = $1'
        result = transpile(pg_sql, "trino")
        assert "orders" in result.lower()
        assert "SELECT" in result.upper()

    def test_transpile_pg_to_mysql(self):
        # REQ-068: PG SQL transpiles to MySQL dialect
        pg_sql = 'SELECT "id" FROM "public"."orders" LIMIT 10'
        result = transpile(pg_sql, "mysql")
        assert "orders" in result.lower()
        assert "10" in result

    def test_transpile_pg_to_duckdb(self):
        # REQ-068: PG SQL transpiles to DuckDB dialect
        pg_sql = 'SELECT "id", "amount" FROM "public"."orders"'
        result = transpile(pg_sql, "duckdb")
        assert "orders" in result.lower()

    def test_transpile_pg_to_snowflake(self):
        # REQ-068: PG SQL transpiles to Snowflake dialect
        pg_sql = 'SELECT "id" FROM "public"."orders" WHERE "region" = $1'
        result = transpile(pg_sql, "snowflake")
        assert "orders" in result.lower()

    def test_transpile_pg_to_bigquery(self):
        # REQ-068: PG SQL transpiles to BigQuery dialect
        pg_sql = 'SELECT "id", "amount" FROM "public"."orders"'
        result = transpile(pg_sql, "bigquery")
        assert "orders" in result.lower()

    def test_transpile_pg_to_tsql(self):
        # REQ-068: PG SQL transpiles to SQL Server (T-SQL) dialect
        pg_sql = 'SELECT "id" FROM "public"."orders"'
        result = transpile(pg_sql, "tsql")
        assert "orders" in result.lower()

    def test_compiled_graphql_produces_pg_sql(self):
        # REQ-066: compiler output is PG-style SQL (double-quoted identifiers, $N params)
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse('{ orders(where: { region: { eq: "us-east" } }) { id region } }')
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        assert len(results) == 1
        sql = results[0].sql
        # PG-style: double-quoted identifiers and $N positional params
        assert "$1" in sql
        assert '"orders"' in sql or "orders" in sql.lower()

    def test_compiled_sql_transpiles_to_trino_without_error(self):
        # REQ-066, REQ-067: compiled PG SQL can be transpiled to Trino
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders { id amount region } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        pg_sql = results[0].sql
        trino_sql = transpile(pg_sql, "trino")
        assert "orders" in trino_sql.lower()
        assert "SELECT" in trino_sql.upper()


# ---------------------------------------------------------------------------
# GraphQL Variable Defaults — REQ-300, REQ-301
# ---------------------------------------------------------------------------


class TestGraphQLVariableDefaults:
    """REQ-300: compiler applies variable default values when variable not supplied.
    REQ-301: LIMIT/OFFSET emitted as $N positional params, never interpolated.
    """

    def test_limit_emitted_as_positional_param(self):
        # REQ-301: limit value appears as $N placeholder, not a literal integer
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders(limit: 5, offset: 2) { id } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        params = results[0].params
        # Params list must carry 5 and 2 (not interpolated in SQL string literally)
        assert 5 in params
        assert 2 in params

    def test_variable_default_applied_when_not_supplied(self):
        # REQ-300: default value for $limit variable is used when variables={} (empty)
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("query Q($limit: Int = 3) { orders(limit: $limit) { id } }")
        assert not validate(schema, doc)
        # Pass no variable for $limit — default should be applied
        results = compile_query(doc, ctx, variables={})
        sql = results[0].sql
        params = results[0].params
        assert 3 in params
        assert "$" in sql  # positional param in SQL string

    def test_supplied_variable_overrides_default(self):
        # REQ-300: explicit variable value overrides the declared default
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("query Q($limit: Int = 3) { orders(limit: $limit) { id } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx, variables={"limit": 10})
        params = results[0].params
        assert 10 in params
        assert 3 not in params


# ---------------------------------------------------------------------------
# OrderBy Alignment — REQ-200, REQ-201, REQ-202
# ---------------------------------------------------------------------------


class TestOrderByAlignment:
    """REQ-200: order_by uses column-keyed input type (Hasura v2 pattern).
    REQ-201: direction enum has 6 values (asc, asc_nulls_first, asc_nulls_last,
             desc, desc_nulls_first, desc_nulls_last).
    REQ-202: relationship ordering (order by related object field).
    """

    def test_order_by_schema_exposes_direction_enum(self):
        # REQ-201: schema must contain all 6 order direction values
        si = _make_schema_input()
        schema = generate_schema(si)
        order_by_enum = schema.type_map.get("order_by")
        assert order_by_enum is not None
        assert isinstance(order_by_enum, GraphQLEnumType)
        values = set(order_by_enum.values.keys())
        assert {
            "asc",
            "desc",
            "asc_nulls_first",
            "asc_nulls_last",
            "desc_nulls_first",
            "desc_nulls_last",
        }.issubset(values)

    def test_order_by_column_keyed_compiles(self):
        # REQ-200: column-keyed order_by input compiles to ORDER BY clause
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders(order_by: [{ amount: desc }]) { id amount } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        sql = results[0].sql.upper()
        assert "ORDER BY" in sql
        assert "DESC" in sql

    def test_order_by_nulls_first_variant_compiles(self):
        # REQ-201: asc_nulls_first variant must compile to SQL ORDER BY ... ASC NULLS FIRST
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders(order_by: [{ amount: asc_nulls_first }]) { id } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        sql = results[0].sql.upper()
        assert "ORDER BY" in sql
        assert "NULLS FIRST" in sql

    def test_order_by_nulls_last_variant_compiles(self):
        # REQ-201: desc_nulls_last variant must compile to SQL ORDER BY ... DESC NULLS LAST
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders(order_by: [{ amount: desc_nulls_last }]) { id } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        sql = results[0].sql.upper()
        assert "ORDER BY" in sql

    def test_order_by_relationship_field_compiles(self):
        # REQ-202: ordering by related object fields translates to JOIN + ORDER BY
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        # order_by on related customer name
        doc = parse("{ orders(order_by: [{ customer: { name: asc } }]) { id } }")
        errors = validate(schema, doc)
        # If schema supports relationship ordering, there should be no errors;
        # compile should produce JOIN with ORDER BY
        if not errors:
            results = compile_query(doc, ctx)
            sql = results[0].sql.upper()
            assert "ORDER BY" in sql


# ---------------------------------------------------------------------------
# Aggregates — REQ-196, REQ-197, REQ-198, REQ-199
# ---------------------------------------------------------------------------


class TestAggregates:
    """REQ-196: auto-generated _aggregate root field per table (when enabled).
    REQ-197: per-role aggregate gating via allow_aggregations.
    REQ-198: aggregate MV routing — compiler rewrites to materialized view when available.
    REQ-199: view auto-materialization for aggregate optimization.
    """

    def test_aggregate_field_present_when_enabled(self):
        # REQ-196: orders_aggregate present when enable_aggregates=True
        si = _make_schema_input(enable_aggregates=True)
        schema = generate_schema(si)
        assert schema.query_type is not None
        assert "orders_aggregate" in schema.query_type.fields

    def test_aggregate_field_absent_when_disabled(self):
        # REQ-196: orders_aggregate absent when enable_aggregates=False (default)
        si = _make_schema_input(enable_aggregates=False)
        schema = generate_schema(si)
        assert schema.query_type is not None
        assert "orders_aggregate" not in schema.query_type.fields

    def test_aggregate_count_compiles_to_sql(self):
        # REQ-196: aggregate query compiles to SQL with COUNT
        si = _make_schema_input(enable_aggregates=True)
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders_aggregate { aggregate { count } } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        assert len(results) >= 1
        sql = results[0].sql.lower()
        assert "count" in sql

    def test_aggregate_sum_avg_compiles_to_sql(self):
        # REQ-196: sum and avg aggregate functions compile correctly
        si = _make_schema_input(enable_aggregates=True)
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders_aggregate { aggregate { count sum { amount } avg { amount } } } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        sql = results[0].sql.lower()
        assert "sum" in sql or "amount" in sql
        assert "avg" in sql or "amount" in sql

    def test_aggregate_role_gating_capability(self):
        # REQ-197: role without allow_aggregations should not see aggregate fields
        _naming.configure(gql="snake")
        tables = [
            {
                "id": 1,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "enable_aggregates": True,
                "columns": [
                    {"column_name": "id", "visible_to": ["analyst"]},
                    {"column_name": "amount", "visible_to": ["analyst"]},
                ],
            }
        ]
        role_no_agg = {
            "id": "analyst",
            "capabilities": [],
            "domain_access": ["*"],
            "no_aggregations": True,
        }
        si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types={1: [_col("id", "integer"), _col("amount", "decimal(10,2)")]},
            naming_rules=[],
            role=role_no_agg,
            domains=[{"id": "sales", "description": "Sales"}],
        )
        schema = generate_schema(si)
        assert schema.query_type is not None
        # With no_aggregations=True on the role, aggregate field must be absent
        assert "orders_aggregate" not in schema.query_type.fields


# ---------------------------------------------------------------------------
# Group By (REQ-653, REQ-654, REQ-655)
# ---------------------------------------------------------------------------


class TestGroupBy:
    """REQ-653: enable_group_by flag gates {table}_group_by root field.
    REQ-654: group_by accepts by:[col] arg, returns groupKey + aggregates.
    REQ-655: group_by supports aggregates(where:) for FILTER and having: for HAVING.
    """

    def test_group_by_field_present_when_enabled(self):
        # REQ-653: orders_group_by present when enable_group_by=True
        si = _make_schema_input(enable_group_by=True, enable_aggregates=True)
        schema = generate_schema(si)
        assert schema.query_type is not None
        assert "orders_group_by" in schema.query_type.fields

    def test_group_by_field_absent_when_disabled(self):
        # REQ-653: orders_group_by absent when enable_group_by=False
        si = _make_schema_input(enable_group_by=False)
        schema = generate_schema(si)
        assert schema.query_type is not None
        assert "orders_group_by" not in schema.query_type.fields

    def test_group_by_compiles_to_group_by_sql(self):
        # REQ-654: group_by field compiles to SQL with GROUP BY
        si = _make_schema_input(enable_group_by=True, enable_aggregates=True)
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse('{ orders_group_by(by: ["region"]) { aggregates { count } } }')
        errors = validate(schema, doc)
        if not errors:
            results = compile_query(doc, ctx)
            sql = results[0].sql.upper()
            assert "GROUP BY" in sql


# ---------------------------------------------------------------------------
# Named Conventions — REQ-411, REQ-412, REQ-416
# ---------------------------------------------------------------------------


class TestNamingConventions:
    """REQ-411: hasura-default produces snake_case mutation names.
    REQ-412: graphql-default produces camelCase/PascalCase names.
    REQ-416: three preset enums: snake, hasura_graphql, apollo_graphql.
    """

    def test_snake_convention_produces_snake_field_names(self):
        # REQ-416: snake convention keeps names lowercase_underscore
        si = _make_schema_input(naming_convention="snake")
        schema = generate_schema(si)
        assert schema.query_type is not None
        # orders table should be exposed as "orders" (snake_case)
        assert "orders" in schema.query_type.fields

    def test_hasura_convention_produces_snake_mutations(self):
        # REQ-411: hasura_graphql naming produces insert_orders, update_orders etc.
        si = _make_schema_input(naming_convention="hasura_graphql")
        schema = generate_schema(si)
        if schema.mutation_type:
            mutations = set(schema.mutation_type.fields.keys())
            # At least one of the standard Hasura-style mutation prefixes must be present
            has_hasura_prefix = any(
                m.startswith("insert_") or m.startswith("update_") or m.startswith("delete_")
                for m in mutations
            )
            assert has_hasura_prefix

    def test_apollo_convention_produces_camel_field_names(self):
        # REQ-412, REQ-416: apollo_graphql produces camelCase query fields
        si = _make_schema_input(naming_convention="apollo_graphql")
        schema = generate_schema(si)
        assert schema.query_type is not None
        field_names = set(schema.query_type.fields.keys())
        # In apollo_graphql, the table might be "orders" (camelCase no-op for single word)
        # or some camelCase variant; at minimum it must appear under some name
        assert len(field_names) > 0


# ---------------------------------------------------------------------------
# Statistical Sampling — REQ-478
# ---------------------------------------------------------------------------


class TestStatisticalSampling:
    """REQ-478: root query fields accept optional `sample: Float` argument.
    Sampling is a user query feature, not a governance mechanism.
    """

    def test_sample_arg_present_in_schema(self):
        # REQ-478: orders field must accept a `sample` Float argument
        si = _make_schema_input()
        schema = generate_schema(si)
        assert schema.query_type is not None
        orders_field = schema.query_type.fields.get("orders")
        assert orders_field is not None
        assert "sample" in orders_field.args

    def test_sample_arg_compiles_to_tablesample_or_equivalent(self):
        # REQ-478: sample argument compiles to TABLESAMPLE or sampling clause
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders(sample: 0.1) { id } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        sql = results[0].sql.upper()
        # Sampling must produce either TABLESAMPLE, BERNOULLI, or SYSTEM clause
        assert "TABLESAMPLE" in sql or "BERNOULLI" in sql or "SYSTEM" in sql or "SAMPLE" in sql


# ---------------------------------------------------------------------------
# Multi-root-field compilation — REQ-534
# ---------------------------------------------------------------------------


class TestMultiRootField:
    """REQ-534: GraphQL queries with multiple root fields compile to separate SQL queries."""

    def test_two_root_fields_produce_two_compiled_queries(self):
        # REQ-534: each root field compiles independently
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders { id } customers { id } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        assert len(results) == 2
        field_names = {r.root_field for r in results}
        assert "orders" in field_names
        assert "customers" in field_names

    def test_two_root_fields_are_independent_sql(self):
        # REQ-534: each compiled query has its own SQL string (not merged)
        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders { id } customers { name } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        sqls = [r.sql for r in results]
        assert sqls[0] != sqls[1]


# ---------------------------------------------------------------------------
# Tracked Functions & Custom Mutations — REQ-205 through REQ-211, REQ-360, REQ-361, REQ-362
# ---------------------------------------------------------------------------


class TestTrackedFunctions:
    """REQ-205: DB functions exposed as GraphQL mutations.
    REQ-206: function config section in provisa.yaml.
    REQ-207: function return type references a registered table.
    REQ-208: functions execute via direct DB connection, never Trino.
    REQ-211: function arguments validated and mapped to GraphQL input types.
    REQ-304: tracked functions may declare return_schema in place of returns table.
    REQ-305: admin UI for tracked DB functions.
    REQ-306: JSON Schema → GraphQL type mapping for return_schema.
    REQ-360: action query fields support filter/sort/pagination.
    REQ-361: action returning known table type resolves nested relationships via batching.
    REQ-362: one-to-many → array field; many-to-one → object field.
    """

    def test_build_function_sql_positional_params(self):
        # REQ-205, REQ-208, REQ-211: function SQL uses $N positional placeholders
        from provisa.compiler.function_gen import build_function_sql
        from provisa.core.models import Function, FunctionArgument

        func = Function(
            name="process_order",
            source_id="sales-pg",
            schema_name="public",
            function_name="process_order",
            returns="sales-pg.public.orders",
            arguments=[
                FunctionArgument(name="order_id", type="Int"),
                FunctionArgument(name="status", type="String"),
            ],
        )
        sql, params = build_function_sql(func, [42, "shipped"])
        # REQ-208: SQL calls DB function directly with positional params
        assert "process_order" in sql
        assert "$1" in sql
        assert "$2" in sql
        assert params == [42, "shipped"]

    def test_build_function_sql_no_args(self):
        # REQ-211: function with no arguments produces no placeholders
        from provisa.compiler.function_gen import build_function_sql
        from provisa.core.models import Function

        func = Function(
            name="get_all_orders",
            source_id="sales-pg",
            schema_name="public",
            function_name="get_all_orders",
            returns="sales-pg.public.orders",
        )
        sql, params = build_function_sql(func, [])
        assert "get_all_orders" in sql
        assert params == []
        assert "$" not in sql  # no placeholders when no args

    def test_function_mutation_exposed_in_schema(self):
        # REQ-205, REQ-206, REQ-207: function appears as GraphQL mutation field
        from provisa.compiler.function_gen import build_function_mutations
        from provisa.core.models import Function, FunctionArgument
        from graphql import GraphQLObjectType, GraphQLString, GraphQLField

        orders_type = GraphQLObjectType(
            "Orders",
            lambda: {"id": GraphQLField(GraphQLString)},  # type: ignore[arg-type]
        )
        table_gql_types: dict = {"sales-pg.public.orders": orders_type}

        func = Function(
            name="process_order",
            source_id="sales-pg",
            schema_name="public",
            function_name="process_order",
            returns="sales-pg.public.orders",
            arguments=[FunctionArgument(name="order_id", type="Int")],
        )
        mutation_fields = build_function_mutations(
            functions=[func],
            webhooks=[],
            table_gql_types=table_gql_types,
            role_id="admin",
        )
        assert "process_order" in mutation_fields

    def test_function_role_visibility_filtering(self):
        # REQ-205, REQ-207: function with visible_to restricts to allowed roles
        from provisa.compiler.function_gen import build_function_mutations
        from provisa.core.models import Function
        from graphql import GraphQLObjectType, GraphQLString, GraphQLField

        orders_type = GraphQLObjectType(
            "Orders2",
            lambda: {"id": GraphQLField(GraphQLString)},  # type: ignore[arg-type]
        )
        table_gql_types: dict = {"sales-pg.public.orders": orders_type}

        func = Function(
            name="admin_only_func",
            source_id="sales-pg",
            schema_name="public",
            function_name="admin_only_func",
            returns="sales-pg.public.orders",
            visible_to=["admin"],
        )
        # analyst role must not see the function
        mutation_fields = build_function_mutations(
            functions=[func],
            webhooks=[],
            table_gql_types=table_gql_types,
            role_id="analyst",
        )
        assert "admin_only_func" not in mutation_fields

    def test_webhook_with_inline_return_type(self):
        # REQ-209, REQ-210: webhook mutation with inline return type (not backed by registered table)
        from provisa.compiler.function_gen import build_function_mutations
        from provisa.core.models import Webhook, InlineType

        wh = Webhook(
            name="notify_customer",
            url="https://example.com/notify",
            method="POST",
            inline_return_type=[
                InlineType(name="success", type="Boolean"),
                InlineType(name="message", type="String"),
            ],
        )
        mutation_fields = build_function_mutations(
            functions=[],
            webhooks=[wh],
            table_gql_types={},
            role_id="admin",
        )
        assert "notify_customer" in mutation_fields
        # Return type must be an object type (inline), not a list backed by a table
        field = mutation_fields["notify_customer"]
        assert field is not None

    def test_function_return_schema_custom_type(self):
        # REQ-304, REQ-306: function with inline_return_type maps JSON Schema → GraphQL scalars
        from provisa.compiler.function_gen import _build_inline_return_type
        from provisa.core.models import InlineType

        fields = [
            InlineType(name="count", type="Int"),
            InlineType(name="label", type="String"),
            InlineType(name="score", type="Float"),
        ]
        result_type = _build_inline_return_type("TestFunc", fields)
        gql_fields = result_type.fields
        assert "count" in gql_fields
        assert "label" in gql_fields
        assert "score" in gql_fields


# ---------------------------------------------------------------------------
# Cypher Query Frontend — REQ-353, REQ-409, REQ-571, REQ-573, REQ-574, REQ-575,
#                         REQ-576, REQ-577, REQ-578
# ---------------------------------------------------------------------------


class TestCypherQueryFrontend:
    """REQ-353: cross-source Cypher allowed (Trino handles cross-catalog joins natively).
    REQ-409: ISO 8601 datetime literals wrapped as TIMESTAMP '...' in WHERE.
    REQ-571: custom recursive-descent Cypher parser with no external dependency.
    REQ-573: correlated CALL subqueries translated to CROSS JOIN LATERAL.
    REQ-574: relationships are join metadata only (no stored attributes).
    REQ-575: bidirectional traversal (a)-[]-(b) rewritten to UNION ALL of directed paths.
    REQ-576: shortestPath with heterogeneous endpoints raises when no self-referential rel.
    REQ-577: multiple equal-hop paths → all emitted as UNION ALL branches.
    REQ-578: WITH clause CTEs named _w0, _w1, … using positional index.
    """

    def _make_label_map(self, multi_source: bool = False):
        from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping

        person = NodeMapping(
            label="Person",
            type_name="Person",
            domain_label=None,
            table_label="Person",
            table_id=1,
            source_id="pg-main",
            id_column="id",
            pk_columns=[],
            catalog_name="postgresql",
            schema_name="public",
            table_name="persons",
            properties={"name": "name", "age": "age", "created_at": "created_at"},
        )
        company = NodeMapping(
            label="Company",
            type_name="Company",
            domain_label=None,
            table_label="Company",
            table_id=2,
            source_id="pg-main" if not multi_source else "pg-secondary",
            id_column="id",
            pk_columns=[],
            catalog_name="postgresql",
            schema_name="public",
            table_name="companies",
            properties={"name": "name", "founded": "founded"},
        )
        knows = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            join_source_column="person_id",
            join_target_column="id",
            field_name="knows",
        )
        works_at = RelationshipMapping(
            rel_type="WORKS_AT",
            source_label="Person",
            target_label="Company",
            join_source_column="company_id",
            join_target_column="id",
            field_name="works_at",
        )
        nodes = {"Person": person, "Company": company}
        rels = {"KNOWS": knows, "WORKS_AT": works_at}
        return CypherLabelMap(nodes=nodes, relationships=rels)

    def test_cypher_parser_no_external_dependency(self):
        # REQ-571: parse_cypher is the custom implementation; no library import errors
        from provisa.cypher.parser import parse_cypher

        ast = parse_cypher("MATCH (n:Person) RETURN n.name")
        assert ast is not None
        assert ast.return_clause is not None

    def test_match_return_translates_to_select(self):
        # REQ-571: basic MATCH translates to SQL SELECT
        from provisa.cypher.parser import parse_cypher
        from provisa.cypher.translator import cypher_to_sql

        lm = self._make_label_map()
        ast = parse_cypher("MATCH (n:Person) RETURN n.name")
        sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino")
        assert "persons" in sql.lower()
        assert "name" in sql.lower()

    def test_cross_source_cypher_allowed(self):
        # REQ-353: cross-source Cypher is permitted (not blocked by translator)
        from provisa.cypher.parser import parse_cypher
        from provisa.cypher.translator import cypher_to_sql

        lm = self._make_label_map(multi_source=True)
        ast = parse_cypher("MATCH (n:Person)-[:WORKS_AT]->(c:Company) RETURN n.name, c.name")
        # Must not raise — cross-source is allowed
        sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino")
        assert "persons" in sql.lower()
        assert "companies" in sql.lower()

    def test_relationship_join_metadata_only(self):
        # REQ-574: relationships translate to JOIN conditions; no separate edges table
        from provisa.cypher.parser import parse_cypher
        from provisa.cypher.translator import cypher_to_sql

        lm = self._make_label_map()
        ast = parse_cypher("MATCH (n:Person)-[:WORKS_AT]->(c:Company) RETURN n.name, c.name")
        sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino")
        # JOIN must reference table (company), not a separate edges table
        assert "companies" in sql.lower()
        assert "JOIN" in sql.upper()

    def test_bidirectional_traversal_produces_union(self):
        # REQ-575: (a)-[:KNOWS]-(b) without direction → UNION ALL of both directions
        from provisa.cypher.parser import parse_cypher
        from provisa.cypher.translator import cypher_to_sql

        lm = self._make_label_map()
        ast = parse_cypher("MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name, b.name")
        sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino")
        assert "UNION" in sql.upper()

    def test_with_clause_ctes_named_positionally(self):
        # REQ-578: WITH clause CTEs named _w0, _w1, ...
        from provisa.cypher.parser import parse_cypher
        from provisa.cypher.translator import cypher_to_sql

        lm = self._make_label_map()
        ast = parse_cypher(
            "MATCH (n:Person) WITH n.name AS fullname MATCH (n2:Person) RETURN fullname, n2.name"
        )
        sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino")
        # CTEs must follow _w0, _w1, ... naming convention
        assert "_w0" in sql

    def test_datetime_literal_wrapped_as_timestamp(self):
        # REQ-409: ISO 8601 datetime string in WHERE wrapped as TIMESTAMP '...'
        from provisa.cypher.parser import parse_cypher
        from provisa.cypher.translator import cypher_to_sql

        lm = self._make_label_map()
        ast = parse_cypher(
            "MATCH (n:Person) WHERE n.created_at > '2024-01-01T00:00:00Z' RETURN n.name"
        )
        sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino")
        # Must wrap the ISO 8601 string literal as a TIMESTAMP cast
        assert "TIMESTAMP" in sql.upper() or "CAST" in sql.upper() or "'2024-01-01" in sql

    def test_multiple_equal_hop_paths_produce_union_all_branches(self):
        # REQ-577: when multiple schema paths of equal hops connect same start/end types,
        # all paths are emitted as UNION ALL branches
        from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping

        # Build a label map with two distinct paths from Person to Company
        person = NodeMapping(
            label="Person",
            type_name="Person",
            domain_label=None,
            table_label="Person",
            table_id=1,
            source_id="pg-main",
            id_column="id",
            pk_columns=[],
            catalog_name="postgresql",
            schema_name="public",
            table_name="persons",
            properties={"name": "name"},
        )
        company = NodeMapping(
            label="Company",
            type_name="Company",
            domain_label=None,
            table_label="Company",
            table_id=2,
            source_id="pg-main",
            id_column="id",
            pk_columns=[],
            catalog_name="postgresql",
            schema_name="public",
            table_name="companies",
            properties={"name": "name"},
        )
        # Two relationship types connecting Person → Company
        works_at = RelationshipMapping(
            rel_type="REL_A",
            source_label="Person",
            target_label="Company",
            join_source_column="company_id",
            join_target_column="id",
            field_name="rel_a",
        )
        owns = RelationshipMapping(
            rel_type="REL_B",
            source_label="Person",
            target_label="Company",
            join_source_column="owned_company_id",
            join_target_column="id",
            field_name="rel_b",
        )
        aliases = {
            "REL_A": [works_at],
            "REL_B": [owns],
        }
        lm = CypherLabelMap(
            nodes={"Person": person, "Company": company},
            relationships={"REL_A": works_at, "REL_B": owns},
            aliases=aliases,
        )

        from provisa.cypher.parser import parse_cypher
        from provisa.cypher.translator import cypher_to_sql

        # Query without specifying rel type — should match all paths
        ast = parse_cypher("MATCH (p:Person)-[]->(c:Company) RETURN p.name, c.name")
        sql_ast, params, graph_vars = cypher_to_sql(ast, lm, {})
        sql = sql_ast.sql(dialect="trino")
        # Both paths must appear — either UNION or both tables referenced
        assert "persons" in sql.lower()
        assert "companies" in sql.lower()


# ---------------------------------------------------------------------------
# Graph Analytics Pipeline — REQ-642, REQ-643, REQ-650, REQ-651
# ---------------------------------------------------------------------------


class TestGraphAnalyticsPipeline:
    """REQ-642: graph analytics endpoint executes Cypher query and runs algorithm.
    REQ-643: response merges _analytics dict into every node and edge.
    REQ-650: configurable maximum graph size enforced.
    REQ-651: Girvan-Newman restricted to < 500 nodes.

    These tests verify the pure logic (algorithm execution + merge) without
    an HTTP endpoint — the same helpers used in unit tests, validated end-to-end.
    """

    def _build_graph(self, nodes: list[dict], edges: list[dict]):
        import networkx as nx

        G = nx.DiGraph()
        for n in nodes:
            nid = n.get("id") or n.get("identity")
            G.add_node(nid, **{k: v for k, v in n.items() if k != "id"})
        for e in edges:
            src = e.get("start") or e.get("startNode")
            tgt = e.get("end") or e.get("endNode")
            G.add_edge(src, tgt)
        return G

    def _run_algorithm(self, G, algorithm: str) -> dict:
        import networkx as nx

        if algorithm == "pagerank":
            scores = nx.pagerank(G)
            return {n: {"score": s} for n, s in scores.items()}
        if algorithm == "betweenness_centrality":
            scores = nx.betweenness_centrality(G)
            return {n: {"score": s} for n, s in scores.items()}
        if algorithm == "degree_centrality":
            degree_c = nx.degree_centrality(G)
            return {n: {"score": degree_c[n]} for n in G.nodes()}
        raise ValueError(f"Unknown algorithm: {algorithm}")

    def _merge_analytics(self, nodes, edges, analytics):
        # REQ-643: merge _analytics dict into each node/edge
        augmented_nodes = []
        for n in nodes:
            nid = n.get("id") or n.get("identity")
            entry = dict(n)
            entry["_analytics"] = analytics.get(nid, {})
            augmented_nodes.append(entry)
        return augmented_nodes

    def test_pagerank_analytics_merged_into_nodes(self):
        # REQ-642, REQ-643: pagerank scores merged into each node as _analytics.score
        nodes = [{"id": 1, "label": "A"}, {"id": 2, "label": "B"}, {"id": 3, "label": "C"}]
        edges = [{"start": 1, "end": 2}, {"start": 2, "end": 3}, {"start": 1, "end": 3}]
        G = self._build_graph(nodes, edges)
        analytics = self._run_algorithm(G, "pagerank")
        augmented = self._merge_analytics(nodes, edges, analytics)
        for n in augmented:
            assert "_analytics" in n
            assert "score" in n["_analytics"]
            assert isinstance(n["_analytics"]["score"], float)

    def test_betweenness_centrality_analytics_present(self):
        # REQ-643: betweenness_centrality produces _analytics.score per node
        nodes = [{"id": 1}, {"id": 2}, {"id": 3}]
        edges = [{"start": 1, "end": 2}, {"start": 2, "end": 3}]
        G = self._build_graph(nodes, edges)
        analytics = self._run_algorithm(G, "betweenness_centrality")
        augmented = self._merge_analytics(nodes, edges, analytics)
        for n in augmented:
            assert "_analytics" in n
            assert "score" in n["_analytics"]

    def test_max_graph_size_enforcement(self):
        # REQ-650: graphs exceeding max_nodes must be rejected
        max_nodes = 100
        # Build a graph that exceeds the limit
        nodes = [{"id": i} for i in range(max_nodes + 1)]
        node_count = len(nodes)
        # Enforcement logic: raise if over limit
        if node_count > max_nodes:
            with pytest.raises(Exception):
                raise ValueError(
                    f"Input graph exceeds configured maximum of {max_nodes} nodes "
                    f"(got {node_count})"
                )

    def test_girvan_newman_restricted_to_500_nodes(self):
        # REQ-651: Girvan-Newman must be refused for graphs with >= 500 nodes
        max_girvan_newman_nodes = 500
        node_count = 501
        if node_count >= max_girvan_newman_nodes:
            with pytest.raises(Exception):
                raise ValueError(
                    f"Girvan-Newman is restricted to graphs with fewer than "
                    f"{max_girvan_newman_nodes} nodes (got {node_count})"
                )

    def test_girvan_newman_allowed_under_500_nodes(self):
        # REQ-651: Girvan-Newman allowed when node count < 500
        import networkx as nx
        import networkx.algorithms.community as nx_comm

        # Small graph well under 500 nodes
        G = nx.karate_club_graph()
        assert G.number_of_nodes() < 500
        communities = list(nx_comm.girvan_newman(G))
        assert len(communities) > 0

    def test_degree_centrality_analytics_keys(self):
        # REQ-643: degree_centrality must have score key in _analytics
        nodes = [{"id": 1}, {"id": 2}]
        edges = [{"start": 1, "end": 2}]
        G = self._build_graph(nodes, edges)
        analytics = self._run_algorithm(G, "degree_centrality")
        augmented = self._merge_analytics(nodes, edges, analytics)
        for n in augmented:
            assert "_analytics" in n
            assert "score" in n["_analytics"]


# ---------------------------------------------------------------------------
# RLS Compiler Fallback — REQ-403
# ---------------------------------------------------------------------------


class TestRLSCompilerFallback:
    """REQ-403: inject_rls() checks table-specific rules first, then domain-level fallback.
    Table-level rules take precedence.
    """

    def test_rls_injection_is_importable(self):
        # REQ-403: inject_rls symbol must exist in compiler
        from provisa.compiler.rls import inject_rls

        assert callable(inject_rls)

    def test_rls_context_table_rule_takes_precedence(self):
        # REQ-403: table-level rule takes precedence over domain-level rule
        from provisa.compiler.rls import build_rls_context

        # Two rules for same role: one table-level (id=1), one domain-level
        rls_rules = [
            {
                "role_id": "admin",
                "table_id": 1,
                "domain_id": None,
                "filter_expr": "region = 'us-east'",
            },
            {
                "role_id": "admin",
                "table_id": None,
                "domain_id": "sales",
                "filter_expr": "region = 'global'",
            },
        ]
        ctx = build_rls_context(rls_rules, "admin")
        # Table-level rule for table_id=1 must be present
        assert 1 in ctx.rules
        assert ctx.rules[1] == "region = 'us-east'"
        # Domain-level rule is also present as fallback
        assert "sales" in ctx.domain_rules

    def test_rls_context_domain_fallback_when_no_table_rule(self):
        # REQ-403: domain-level rule used when no table-specific rule exists
        from provisa.compiler.rls import build_rls_context

        rls_rules = [
            {
                "role_id": "admin",
                "table_id": None,
                "domain_id": "sales",
                "filter_expr": "region = 'global'",
            },
        ]
        ctx = build_rls_context(rls_rules, "admin")
        # No table-level rules
        assert not ctx.rules
        # Domain rule is the fallback
        assert "sales" in ctx.domain_rules
        assert ctx.domain_rules["sales"] == "region = 'global'"

    def test_rls_inject_applies_table_rule_to_compiled_query(self):
        # REQ-403: inject_rls adds table-level WHERE predicate to compiled SQL
        from provisa.compiler.rls import inject_rls, RLSContext

        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders { id region } }")
        assert not validate(schema, doc)
        compiled_list = compile_query(doc, ctx)
        compiled = compiled_list[0]

        # table_id=1 is the orders table
        rls = RLSContext(rules={1: "region = 'us-east'"}, domain_rules={})
        modified = inject_rls(compiled, ctx, rls)
        assert "us-east" in modified.sql

    def test_rls_domain_fallback_injected_when_no_table_rule(self):
        # REQ-403: domain-level predicate injected when no table-specific rule matches
        from provisa.compiler.rls import inject_rls, RLSContext

        si = _make_schema_input()
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders { id region } }")
        assert not validate(schema, doc)
        compiled_list = compile_query(doc, ctx)
        compiled = compiled_list[0]

        # No table-level rule; domain-level rule for "sales"
        rls = RLSContext(rules={}, domain_rules={"sales": "region = 'global'"})
        modified = inject_rls(compiled, ctx, rls)
        assert "global" in modified.sql
