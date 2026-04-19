# Copyright (c) 2026 Kenneth Stott
# Canary: 6a38e7bb-28a0-42f2-8282-0771bd127040
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for GraphQL schema generation from real Trino metadata."""

from pathlib import Path

import pytest
import pytest_asyncio
from graphql import (
    GraphQLEnumType,
    GraphQLField,
    GraphQLInputObjectType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    assert_valid_schema,
    print_schema,
)

from provisa.compiler.introspect import introspect_table_columns
from provisa.compiler.naming import source_to_catalog
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.core.config_loader import load_config, parse_config
from provisa.core.db import init_schema
from provisa.core.repositories import (
    domain as domain_repo,
    relationship as rel_repo,
    role as role_repo,
    source as source_repo,
    table as table_repo,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()
FIXTURE_CONFIG = Path(__file__).parent.parent / "fixtures" / "sample_config.yaml"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _init_schema(pg_pool):
    await init_schema(pg_pool, SCHEMA_SQL)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _load_config(pg_pool, _init_schema):
    """Load sample config into PG once per module."""
    async with pg_pool.acquire() as conn:
        # Clean first
        await conn.execute("""
            TRUNCATE rls_rules, relationships, table_columns,
                     registered_tables, naming_rules, roles, domains, sources
            CASCADE
        """)
        config = parse_config(FIXTURE_CONFIG)
        await load_config(config, conn)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def schema_input(pg_pool, trino_conn, _load_config) -> dict:
    """Build SchemaInput from loaded config + real Trino metadata."""
    async with pg_pool.acquire() as conn:
        tables = await table_repo.list_all(conn)
        rels = await rel_repo.list_all(conn)
        roles = await role_repo.list_all(conn)
        domains = await domain_repo.list_all(conn)
        sources = await source_repo.list_all(conn)
        naming_rules = [
            dict(r) for r in await conn.fetch("SELECT pattern, replacement FROM naming_rules")
        ]

    column_types = {}
    for table in tables:
        catalog = source_to_catalog(table["source_id"])
        cols = introspect_table_columns(
            trino_conn, catalog, table["schema_name"], table["table_name"]
        )
        column_types[table["id"]] = cols

    return {
        "tables": tables,
        "rels": rels,
        "roles": {r["id"]: r for r in roles},
        "domains": domains,
        "column_types": column_types,
        "naming_rules": naming_rules,
    }


def _unwrap_list_type(field: GraphQLField) -> GraphQLObjectType:
    """Unwrap List(NonNull(T)) → T."""
    return field.type.of_type.of_type


def _make_schema_input(schema_input: dict, role_id: str) -> SchemaInput:
    return SchemaInput(
        tables=schema_input["tables"],
        relationships=schema_input["rels"],
        column_types=schema_input["column_types"],
        naming_rules=schema_input["naming_rules"],
        role=schema_input["roles"][role_id],
        domains=schema_input["domains"],
    )


class TestSchemaGenValid:
    def test_admin_schema_validates(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        assert_valid_schema(schema)

    def test_analyst_schema_validates(self, schema_input):
        si = _make_schema_input(schema_input, "analyst")
        schema = generate_schema(si)
        assert_valid_schema(schema)


class TestSchemaGenObjectTypes:
    def test_admin_sees_all_three_tables(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        query = schema.query_type
        assert query is not None
        field_names = set(query.fields.keys())
        assert "orders" in field_names
        assert "customers" in field_names
        assert "products" in field_names

    def test_analyst_sees_only_sales_analytics_domain(self, schema_input):
        """Analyst has access to sales-analytics domain only, not product-catalog."""
        si = _make_schema_input(schema_input, "analyst")
        schema = generate_schema(si)
        query = schema.query_type
        field_names = set(query.fields.keys())
        assert "orders" in field_names
        assert "customers" in field_names
        # products is in product-catalog domain — analyst can't see it
        assert "products" not in field_names

    def test_orders_type_has_correct_fields(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_field = schema.query_type.fields["orders"]
        orders_type = _unwrap_list_type(orders_field)
        assert isinstance(orders_type, GraphQLObjectType)
        field_names = set(orders_type.fields.keys())
        assert "id" in field_names
        assert "customer_id" in field_names
        assert "amount" in field_names
        assert "region" in field_names


class TestSchemaGenColumnVisibility:
    def test_admin_sees_amount_column(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_type = _unwrap_list_type(schema.query_type.fields["orders"])
        assert "amount" in orders_type.fields

    def test_analyst_cannot_see_amount_column(self, schema_input):
        """Amount is visible_to [admin] only, not analyst."""
        si = _make_schema_input(schema_input, "analyst")
        schema = generate_schema(si)
        orders_type = _unwrap_list_type(schema.query_type.fields["orders"])
        assert "amount" not in orders_type.fields

    def test_analyst_can_see_region_column(self, schema_input):
        si = _make_schema_input(schema_input, "analyst")
        schema = generate_schema(si)
        orders_type = _unwrap_list_type(schema.query_type.fields["orders"])
        assert "region" in orders_type.fields

    def test_admin_sees_email_on_customers(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        customers_type = _unwrap_list_type(schema.query_type.fields["customers"])
        assert "email" in customers_type.fields

    def test_analyst_cannot_see_email_on_customers(self, schema_input):
        """Email is visible_to [admin] only."""
        si = _make_schema_input(schema_input, "analyst")
        schema = generate_schema(si)
        customers_type = _unwrap_list_type(schema.query_type.fields["customers"])
        assert "email" not in customers_type.fields


class TestSchemaGenRelationships:
    def test_orders_has_customers_relationship(self, schema_input):
        """orders → customers (many-to-one) should produce an object field."""
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_type = _unwrap_list_type(schema.query_type.fields["orders"])
        assert "customers" in orders_type.fields
        customers_field = orders_type.fields["customers"]
        # many-to-one: single object, not a list
        assert isinstance(customers_field.type, GraphQLObjectType)

    def test_relationship_visible_when_both_domains_accessible(self, schema_input):
        """Admin has '*' domain access — should see the relationship."""
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_type = _unwrap_list_type(schema.query_type.fields["orders"])
        assert "customers" in orders_type.fields

    def test_analyst_sees_relationship_within_domain(self, schema_input):
        """Both orders and customers are in sales-analytics — analyst should see it."""
        si = _make_schema_input(schema_input, "analyst")
        schema = generate_schema(si)
        orders_type = _unwrap_list_type(schema.query_type.fields["orders"])
        assert "customers" in orders_type.fields


class TestSchemaGenQueryArgs:
    def test_query_field_has_limit_offset(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_field = schema.query_type.fields["orders"]
        assert "limit" in orders_field.args
        assert "offset" in orders_field.args

    def test_query_field_has_where_arg(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_field = schema.query_type.fields["orders"]
        assert "where" in orders_field.args
        where_type = orders_field.args["where"].type
        assert isinstance(where_type, GraphQLInputObjectType)

    def test_where_has_column_filters(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_field = schema.query_type.fields["orders"]
        where_type = orders_field.args["where"].type
        assert "id" in where_type.fields
        assert "region" in where_type.fields

    def test_where_has_and_or_combinators(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_field = schema.query_type.fields["orders"]
        where_type = orders_field.args["where"].type
        assert "_and" in where_type.fields
        assert "_or" in where_type.fields

    def test_query_field_has_order_by(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_field = schema.query_type.fields["orders"]
        assert "order_by" in orders_field.args

    def test_order_by_has_column_fields(self, schema_input):
        """Hasura v2 pattern: each column is a field with OrderDirection type."""
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_field = schema.query_type.fields["orders"]
        order_by_list_type = orders_field.args["order_by"].type
        # List(NonNull(OrdersOrderBy))
        order_by_type = order_by_list_type.of_type.of_type
        # At least one column field should exist with OrderDirection enum type
        assert len(order_by_type.fields) > 0
        enum_fields = [
            (name, f) for name, f in order_by_type.fields.items()
            if isinstance(f.type, GraphQLEnumType)
        ]
        assert len(enum_fields) > 0
        for field_name, field_def in enum_fields:
            assert "asc" in field_def.type.values
            assert "desc" in field_def.type.values
            assert "asc_nulls_first" in field_def.type.values


class TestSchemaGenNaming:
    def test_naming_rules_applied(self, schema_input):
        """Sample config has rule ^prod_pg_ → ''. Since table names don't match,
        names should remain unchanged."""
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        # Table names are 'orders', 'customers', 'products' — no prefix match
        assert "orders" in schema.query_type.fields

    def test_type_names_are_pascal_case(self, schema_input):
        si = _make_schema_input(schema_input, "admin")
        schema = generate_schema(si)
        orders_type = _unwrap_list_type(schema.query_type.fields["orders"])
        assert orders_type.name == "Orders"
