# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9b2c71-5e0a-4d8e-9b61-2a7c4e8d1f05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table whose naming convention is a Hasura preset is served in Hasura's shapes (REQ-194,
maintainer 2026-11-22): its root fields and types are the names ``hasura_table_names`` gives,
its arguments and operators are Hasura's, and the compiler reads them with Hasura's semantics,
so a client moving from Hasura keeps its queries. A table under any other convention is served
exactly as before."""

# Requirements: REQ-194

from __future__ import annotations

import pytest
from graphql import GraphQLEnumType, GraphQLNonNull, get_named_type, parse, validate

from provisa.compiler import naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query

_COLUMNS = ["id", "customer_name", "total"]

#: Each Hasura preset with the Hasura convention it serves.
_PRESETS = [("hasura_graphql", "hasura-default"), ("hasura_graphql_default", "graphql-default")]


def _schema_input(convention: str | None, alias: str | None = None) -> SchemaInput:
    tables = [
        {
            "id": 1,
            "source_id": "pg",
            "domain_id": "store",
            "schema_name": "public",
            "table_name": "order_lines",
            "alias": alias,
            "gql_naming_convention": convention,
            "enable_aggregates": True,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"], "is_primary_key": True},
                {"column_name": "customer_name", "visible_to": ["admin"]},
                {"column_name": "total", "visible_to": ["admin"]},
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
            "write_grants": [
                {
                    "role": "admin",
                    "operation": op,
                    "columns": [] if op == "delete" else _COLUMNS,
                    "rows": None,
                    "check": None,
                    "presets": [],
                }
                for op in ("insert", "update", "delete")
            ],
        }
    ]
    types = [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="customer_name", data_type="varchar", is_nullable=True),
        ColumnMetadata(column_name="total", data_type="integer", is_nullable=True),
    ]
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types={1: types},
        naming_rules=[],
        role={"id": "admin", "capabilities": ["write"], "domain_access": ["*"]},
        domains=[{"id": "store", "description": "Store"}],
        source_types={"pg": "postgresql"},
    )


def _built(convention: str | None, alias: str | None = None):
    naming.configure(gql="apollo_graphql", sql="snake")
    si = _schema_input(convention, alias)
    return generate_schema(si), build_context(si)


def _compiled(built, text: str):
    schema, ctx = built
    doc = parse(text)
    errors = validate(schema, doc)
    assert not errors, errors
    (compiled,) = compile_query(doc, ctx)
    return compiled


@pytest.mark.parametrize(("preset", "conv"), _PRESETS)
def test_the_introspected_names_are_hasura_table_names(preset, conv):
    schema, _ = _built(preset)
    expected = naming.hasura_table_names(conv, "order_lines")
    assert schema.query_type is not None and schema.mutation_type is not None
    query, mutation = set(schema.query_type.fields), set(schema.mutation_type.fields)
    assert {expected.select, expected.select_by_pk, expected.select_aggregate} <= query
    assert {
        expected.insert,
        expected.insert_one,
        expected.update,
        expected.update_by_pk,
        expected.delete,
        expected.delete_by_pk,
    } == mutation
    for kind in (
        "type",
        "bool_exp",
        "order_by",
        "insert_input",
        "set_input",
        "mutation_response",
        "pk_columns_input",
        "on_conflict",
        "constraint",
        "update_column",
        "select_column",
        "aggregate",
        "aggregate_fields",
    ):
        assert getattr(expected, kind) in schema.type_map, kind


@pytest.mark.parametrize(("preset", "conv"), _PRESETS)
def test_a_select_takes_hasuras_arguments(preset, conv):
    schema, _ = _built(preset)
    names = naming.hasura_table_names(conv, "order_lines")
    assert schema.query_type is not None
    args = schema.query_type.fields[names.select].args

    def arg(name: str):
        return args[naming.hasura_argument_name(conv, name)]

    for name in ("where", "order_by", "distinct_on", "limit", "offset"):
        assert naming.hasura_argument_name(conv, name) in args, name
    assert get_named_type(arg("where").type).name == names.bool_exp
    assert get_named_type(arg("order_by").type).name == names.order_by
    assert get_named_type(arg("distinct_on").type).name == names.select_column


@pytest.mark.parametrize(("preset", "conv"), _PRESETS)
def test_a_column_is_compared_by_hasuras_operators(preset, conv):
    schema, _ = _built(preset)
    names = naming.hasura_table_names(conv, "order_lines")
    where = schema.type_map[names.bool_exp]
    assert {"_and", "_or", "_not"} <= set(where.fields)  # type: ignore[union-attr]
    comparison = get_named_type(where.fields["id"].type)  # type: ignore[union-attr]
    assert comparison.name == naming.hasura_comparison_type(conv, "Int")
    ops = set(comparison.fields)  # type: ignore[union-attr]
    assert {"_eq", "_neq", "_gt", "_gte", "_lt", "_lte", "_in"} <= ops
    assert naming.hasura_argument_name(conv, "_is_null") in ops


@pytest.mark.parametrize(("preset", "conv"), _PRESETS)
def test_order_by_values_are_hasuras(preset, conv):
    schema, _ = _built(preset)
    names = naming.hasura_table_names(conv, "order_lines")
    order_by = schema.type_map[names.order_by]
    direction = get_named_type(order_by.fields["id"].type)  # type: ignore[union-attr]
    assert isinstance(direction, GraphQLEnumType)
    assert direction.name == naming.hasura_order_by_type(conv)
    assert set(direction.values) == {
        naming.hasura_enum_value(conv, v, "order_by") for v in naming.HASURA_ORDER_DIRECTIONS
    }


@pytest.mark.parametrize(("preset", "conv"), _PRESETS)
def test_by_pk_takes_the_primary_key_and_answers_one_row(preset, conv):
    schema, _ = _built(preset)
    names = naming.hasura_table_names(conv, "order_lines")
    assert schema.query_type is not None
    field = schema.query_type.fields[names.select_by_pk]
    assert set(field.args) == {"id"}
    assert isinstance(field.args["id"].type, GraphQLNonNull)
    assert not isinstance(field.type, GraphQLNonNull)
    assert field.type.name == names.type  # type: ignore[union-attr]


def test_a_hasura_default_query_compiles_with_hasuras_semantics():
    compiled = _compiled(
        _built("hasura_graphql"),
        "{ order_lines(where: {_and: [{id: {_eq: 1}}, {total: {_gt: 5}}]}, "
        "order_by: {total: desc}, distinct_on: [customer_name], limit: 5, offset: 2) "
        "{ id total } }",
    )
    assert '"id" = ' in compiled.sql and '"total" > ' in compiled.sql
    assert 'ORDER BY "total" DESC' in compiled.sql
    assert 'DISTINCT ON ("customer_name")' in compiled.sql
    assert "LIMIT $3 OFFSET $4" in compiled.sql
    assert compiled.params == [1, 5, 5, 2]
    assert not compiled.single_row


def test_a_graphql_default_query_compiles_with_hasuras_semantics():
    compiled = _compiled(
        _built("hasura_graphql_default"),
        "{ orderLines(where: {customerName: {_isNull: false}}, "
        "orderBy: [{total: DESC_NULLS_LAST}], distinctOn: [customerName]) { id customerName } }",
    )
    assert '"customer_name" IS NOT NULL' in compiled.sql
    assert '"total" DESC NULLS LAST' in compiled.sql
    assert 'DISTINCT ON ("customer_name")' in compiled.sql


@pytest.mark.parametrize(
    ("preset", "text"),
    [
        ("hasura_graphql", "{ order_lines_by_pk(id: 7) { id customer_name } }"),
        ("hasura_graphql_default", "{ orderLinesByPk(id: 7) { id customerName } }"),
    ],
)
def test_by_pk_compiles_to_the_row_of_its_key(preset, text):
    compiled = _compiled(_built(preset), text)
    assert '"id" = ' in compiled.sql
    assert compiled.params == [7]
    assert compiled.single_row


def test_an_aggregate_takes_a_hasura_where():
    compiled = _compiled(
        _built("hasura_graphql"),
        "{ order_lines_aggregate(where: {total: {_gte: 1}}) "
        "{ aggregate { count sum { total } } } }",
    )
    assert '"total" >= ' in compiled.sql
    assert compiled.params == [1]


def test_by_pk_answers_the_row_or_null():
    from provisa.executor.json_encoders import single_row_body

    assert single_row_body(b'[{"id":7}]') == b'{"id":7}'
    assert single_row_body(b"[]") == b"null"


def test_graphql_default_keeps_a_custom_table_name_as_written():
    schema, _ = _built("hasura_graphql_default", alias="order_line_items")
    expected = naming.hasura_table_names(
        "graphql-default", "order_lines", custom_name="order_line_items"
    )
    assert schema.query_type is not None and schema.mutation_type is not None
    assert expected.select == "order_line_items"
    assert {expected.select, expected.select_by_pk, expected.select_aggregate} <= set(
        schema.query_type.fields
    )
    assert expected.insert_one in schema.mutation_type.fields
    assert expected.bool_exp in schema.type_map


def test_a_provisa_convention_is_served_as_before():
    schema, _ = _built("apollo_graphql")
    assert schema.query_type is not None
    assert "orderLinesByPk" not in schema.query_type.fields
    assert "OrderLinesWhere" in schema.type_map
    assert "OrderLines_bool_exp" not in schema.type_map
    where = schema.type_map["OrderLinesWhere"]
    assert get_named_type(where.fields["id"].type).name == "IntFilter"  # type: ignore[union-attr]


def test_an_imported_graphql_default_source_is_the_graphql_default_preset():
    assert naming.preset_of("graphql-default") == "hasura_graphql_default"
    assert naming.preset_of("hasura-default") == "hasura_graphql"
    assert naming.hasura_convention_of("graphql-default") == "graphql-default"
    assert naming.hasura_convention_of("apollo_graphql") is None
