# Copyright (c) 2026 Kenneth Stott
# Canary: 702fa3b5-977d-4433-9fbf-b7f306a828eb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The standard introspection query is answered with no error, by the admin schema and by a
role's data schema.

An input field of the admin schema defaulted to ``{}`` on the JSON scalar. A default value is
printed when a client asks for ``defaultValue``, that one cannot be printed, and so every full
introspection of the admin API carried "Cannot convert value to AST: {}": GraphiQL, code
generators and any client that introspects were handed an error. Both schemas are walked here so
the next default that cannot be printed fails by the name of its field."""

from __future__ import annotations

from graphql import (
    GraphQLInputObjectType,
    GraphQLObjectType,
    GraphQLSchema,
    Undefined,
    get_introspection_query,
    graphql_sync,
)
from graphql.utilities import ast_from_value

from provisa.compiler import naming as _naming
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema


def _unprintable_defaults(schema: GraphQLSchema) -> list[str]:
    """Every input field and argument whose default value cannot be written as GraphQL."""
    found: list[str] = []

    def _check(where: str, node) -> None:
        if node.default_value is Undefined:
            return
        try:
            ast_from_value(node.default_value, node.type)
        except TypeError as exc:
            found.append(f"{where}: {exc}")

    for name, kind in schema.type_map.items():
        if isinstance(kind, GraphQLInputObjectType):
            for field_name, field in kind.fields.items():
                _check(f"{name}.{field_name}", field)
        elif isinstance(kind, GraphQLObjectType):
            for field_name, field in kind.fields.items():
                for arg_name, arg in field.args.items():
                    _check(f"{name}.{field_name}({arg_name})", arg)
    for directive in schema.directives:
        for arg_name, arg in directive.args.items():
            _check(f"@{directive.name}({arg_name})", arg)
    return found


def _col(name: str, data_type: str) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)


def _data_schema() -> GraphQLSchema:
    """A role's data schema over two writable, related tables with columns of every common type,
    so its queries, mutations, filters, orderings and aggregates are all there to introspect."""
    _naming.configure(gql="snake")
    writable = {"write_ops": ["delete", "insert", "update"], "write_returns_rows": True}
    types = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(12,2)"),
            _col("region", "varchar(40)"),
            _col("placed", "timestamp"),
            _col("day", "date"),
            _col("paid", "boolean"),
            _col("details", "json"),
            _col("score", "double"),
        ],
        2: [_col("id", "integer"), _col("name", "varchar(100)")],
    }
    tables = [
        {
            "id": table_id,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": name,
            "columns": [{"column_name": c.column_name, "visible_to": ["admin"]} for c in columns],
            "write_refused_forms": [],
            **writable,
        }
        for table_id, name, columns in ((1, "orders", types[1]), (2, "customers", types[2]))
    ]
    relationships = [
        {
            "id": "orders-to-customers",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        }
    ]
    return generate_schema(
        SchemaInput(
            tables=tables,
            relationships=relationships,
            column_types=types,
            naming_rules=[],
            role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "sales", "description": "Sales"}],
        )
    )


async def test_the_admin_schema_answers_the_standard_introspection_query_without_error():
    from provisa.api.admin.schema import admin_schema

    answered = await admin_schema.execute(get_introspection_query())
    assert answered.errors is None, [str(e)[:200] for e in answered.errors or []]
    assert answered.data is not None and answered.data["__schema"]["queryType"]["name"] == "Query"


def test_every_default_in_the_admin_schema_can_be_printed():
    from provisa.api.admin.schema import admin_schema

    assert _unprintable_defaults(admin_schema._schema) == []  # noqa: SLF001


def test_a_data_schema_answers_the_standard_introspection_query_without_error():
    schema = _data_schema()
    assert schema.mutation_type is not None  # the writable tables' mutations are in the walk
    answered = graphql_sync(schema, get_introspection_query())
    assert answered.errors is None, [str(e)[:200] for e in answered.errors or []]
    assert _unprintable_defaults(schema) == []


def test_a_data_product_saved_without_custom_properties_has_none():
    """The input's field is nullable with a null default; the resolver reads absent as none."""
    from provisa.api.admin.types import DataProductInput

    field = next(
        f
        for f in DataProductInput.__strawberry_definition__.fields  # type: ignore[attr-defined]
        if f.python_name == "custom_properties"
    )
    assert field.default is None
