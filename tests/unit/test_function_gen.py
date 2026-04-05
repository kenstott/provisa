# Copyright (c) 2026 Kenneth Stott
# Canary: 0bca2cd4-77d2-4eb6-89bb-694ead31ac70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for function_gen — GraphQL mutation generation for tracked functions/webhooks."""

import pytest
from graphql import (
    GraphQLField,
    GraphQLFloat,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
)

from provisa.compiler.function_gen import (
    build_function_mutations,
    build_function_sql,
)
from provisa.core.models import (
    Function,
    FunctionArgument,
    InlineType,
    Webhook,
)


def _make_table_type(name: str = "Orders") -> GraphQLObjectType:
    return GraphQLObjectType(
        name,
        lambda: {
            "id": GraphQLField(GraphQLNonNull(GraphQLInt)),
            "amount": GraphQLField(GraphQLFloat),
            "region": GraphQLField(GraphQLString),
        },
    )


class TestBuildFunctionMutations:
    def test_function_generates_mutation_field(self):
        func = Function(
            name="process_order",
            source_id="sales-pg",
            function_name="process_order",
            returns="sales-pg.public.orders",
            arguments=[
                FunctionArgument(name="order_id", type="Int"),
                FunctionArgument(name="note", type="String"),
            ],
            visible_to=["admin"],
        )
        table_types = {"sales-pg.public.orders": _make_table_type()}

        fields = build_function_mutations(
            functions=[func], webhooks=[], table_gql_types=table_types, role_id="admin",
        )

        assert "process_order" in fields
        field = fields["process_order"]
        # Return type should be [Orders!]
        assert isinstance(field.type, GraphQLList)
        assert isinstance(field.type.of_type, GraphQLNonNull)
        # Args
        assert "order_id" in field.args
        assert "note" in field.args

    def test_function_filtered_by_role(self):
        func = Function(
            name="admin_func",
            source_id="pg",
            function_name="admin_func",
            returns="pg.public.orders",
            visible_to=["admin"],
        )
        table_types = {"pg.public.orders": _make_table_type()}

        fields = build_function_mutations(
            functions=[func], webhooks=[], table_gql_types=table_types, role_id="analyst",
        )
        assert "admin_func" not in fields

    def test_function_visible_when_no_role_filter(self):
        func = Function(
            name="open_func",
            source_id="pg",
            function_name="open_func",
            returns="pg.public.orders",
            visible_to=[],
        )
        table_types = {"pg.public.orders": _make_table_type()}

        fields = build_function_mutations(
            functions=[func], webhooks=[], table_gql_types=table_types, role_id="anyone",
        )
        assert "open_func" in fields

    def test_function_skipped_when_table_type_missing(self):
        func = Function(
            name="missing_table_func",
            source_id="pg",
            function_name="fn",
            returns="pg.public.nonexistent",
        )

        fields = build_function_mutations(
            functions=[func], webhooks=[], table_gql_types={},
        )
        assert "missing_table_func" not in fields

    def test_webhook_with_table_return(self):
        wh = Webhook(
            name="fetch_external",
            url="https://api.example.com/data",
            returns="pg.public.orders",
            arguments=[FunctionArgument(name="id", type="Int")],
        )
        table_types = {"pg.public.orders": _make_table_type()}

        fields = build_function_mutations(
            functions=[], webhooks=[wh], table_gql_types=table_types,
        )

        assert "fetch_external" in fields
        field = fields["fetch_external"]
        assert isinstance(field.type, GraphQLList)
        assert "id" in field.args

    def test_webhook_with_inline_return_type(self):
        wh = Webhook(
            name="check_status",
            url="https://api.example.com/status",
            inline_return_type=[
                InlineType(name="status", type="String"),
                InlineType(name="code", type="Int"),
            ],
        )

        fields = build_function_mutations(
            functions=[], webhooks=[wh], table_gql_types={},
        )

        assert "check_status" in fields
        field = fields["check_status"]
        # Should be an object type with the inline fields
        assert isinstance(field.type, GraphQLObjectType)
        assert field.type.name == "check_statusResult"
        result_fields = field.type.fields
        assert "status" in result_fields
        assert "code" in result_fields

    def test_webhook_with_no_return_type_falls_back_to_json(self):
        wh = Webhook(
            name="fire_and_forget",
            url="https://api.example.com/fire",
        )

        fields = build_function_mutations(
            functions=[], webhooks=[wh], table_gql_types={},
        )

        assert "fire_and_forget" in fields

    def test_webhook_role_filtering(self):
        wh = Webhook(
            name="secret_hook",
            url="https://api.example.com/secret",
            visible_to=["admin"],
        )

        fields = build_function_mutations(
            functions=[], webhooks=[wh], table_gql_types={}, role_id="analyst",
        )
        assert "secret_hook" not in fields

    def test_mixed_functions_and_webhooks(self):
        func = Function(
            name="db_action",
            source_id="pg",
            function_name="do_thing",
            returns="pg.public.orders",
        )
        wh = Webhook(
            name="api_action",
            url="https://api.example.com/thing",
            inline_return_type=[InlineType(name="ok", type="Boolean")],
        )
        table_types = {"pg.public.orders": _make_table_type()}

        fields = build_function_mutations(
            functions=[func], webhooks=[wh], table_gql_types=table_types,
        )

        assert "db_action" in fields
        assert "api_action" in fields

    def test_mutations_integrate_into_schema(self):
        """Verify generated fields can be added to a valid GraphQL schema."""
        func = Function(
            name="run_report",
            source_id="pg",
            function_name="run_report",
            returns="pg.public.orders",
            arguments=[FunctionArgument(name="year", type="Int")],
        )
        table_types = {"pg.public.orders": _make_table_type()}

        fields = build_function_mutations(
            functions=[func], webhooks=[], table_gql_types=table_types,
        )

        query_type = GraphQLObjectType("Query", {"dummy": GraphQLField(GraphQLString)})
        mutation_type = GraphQLObjectType("Mutation", lambda: fields)
        schema = GraphQLSchema(query=query_type, mutation=mutation_type)
        assert schema.mutation_type is not None
        assert "run_report" in schema.mutation_type.fields


class TestBuildFunctionSql:
    def test_no_args(self):
        func = Function(
            name="refresh_all",
            source_id="pg",
            function_name="refresh_all",
            returns="pg.public.orders",
        )
        sql, params = build_function_sql(func, [])
        assert sql == 'SELECT * FROM "public"."refresh_all"()'
        assert params == []

    def test_positional_args(self):
        func = Function(
            name="process",
            source_id="pg",
            function_name="process_order",
            returns="pg.public.orders",
            arguments=[
                FunctionArgument(name="order_id", type="Int"),
                FunctionArgument(name="note", type="String"),
            ],
        )
        sql, params = build_function_sql(func, [42, "rush"])
        assert sql == 'SELECT * FROM "public"."process_order"($1, $2)'
        assert params == [42, "rush"]

    def test_custom_schema(self):
        func = Function(
            name="fn",
            source_id="pg",
            schema="reporting",
            function_name="gen_report",
            returns="pg.reporting.reports",
        )
        sql, params = build_function_sql(func, [2024])
        assert sql == 'SELECT * FROM "reporting"."gen_report"($1)'
        assert params == [2024]


class TestArgumentTypeResolution:
    def test_all_scalar_types_resolve(self):
        """All documented scalar type names should resolve without error."""
        from provisa.compiler.function_gen import _resolve_scalar

        for type_name in ["String", "Int", "Float", "Boolean", "DateTime", "Date", "BigInt", "JSON"]:
            scalar = _resolve_scalar(type_name)
            assert scalar is not None

    def test_unknown_type_raises(self):
        from provisa.compiler.function_gen import _resolve_scalar

        with pytest.raises(ValueError, match="Unknown argument type"):
            _resolve_scalar("UnknownType")
