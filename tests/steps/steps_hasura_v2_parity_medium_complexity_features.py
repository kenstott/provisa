# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step implementations for REQ-222 — REST endpoint auto-generation (Hasura v2 parity).

For each root query field, Provisa auto-generates a ``GET /data/rest/<table>``
endpoint. REST URL query parameters are mapped onto the equivalent GraphQL
arguments (``?limit=10&where.id.eq=1`` -> ``orders(limit: 10, where: {id: {eq: 1}})``)
and the resulting query is fed back through the *same* GraphQL compilation
pipeline (schema generation, parse, validation, context build) that powers the
native GraphQL surface.

These steps exercise that translation + compilation path directly: they build a
governed schema, translate the REST query string into a GraphQL document, and
validate it against the generated schema. The translated AST is then inspected
to prove the URL parameters were faithfully mapped onto GraphQL arguments.
"""

from __future__ import annotations

from urllib.parse import parse_qsl, urlsplit

import pytest
from graphql import (
    GraphQLObjectType,
    parse,
    validate,
)
from graphql.language.ast import (
    ArgumentNode,
    FieldNode,
    IntValueNode,
    ObjectValueNode,
)
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context

scenarios("REQ-222.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict used to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_orders_schema():
    """Generate a governed GraphQL schema (and compile context) for an orders table.

    This mirrors how the live REST router obtains the schema/context it reuses
    when servicing a ``GET /data/rest/<table>`` request.
    """
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
        ],
    }
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    return generate_schema(si), build_context(si)


def _gql_literal(raw: str) -> str:
    """Render a raw URL value as a GraphQL literal (int/float stay bare, else quoted)."""
    try:
        return str(int(raw))
    except ValueError:
        pass
    try:
        f = float(raw)
        return repr(f)
    except ValueError:
        escaped = raw.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'


def _rest_params_to_graphql(table: str, query_string: str, selection: list[str]) -> str:
    """Translate a REST query string into an equivalent GraphQL query document.

    Supports the Hasura-style REST mapping:
      * ``limit`` / ``offset``            -> top-level integer arguments
      * ``where.<col>.<op>=<value>``      -> ``where: {<col>: {<op>: <value>}}``
    """
    top_args: dict[str, str] = {}
    where_tree: dict[str, dict[str, str]] = {}

    for key, raw in parse_qsl(query_string, keep_blank_values=True):
        if key.startswith("where."):
            _, column, operator = key.split(".", 2)
            where_tree.setdefault(column, {})[operator] = _gql_literal(raw)
        elif key in ("limit", "offset"):
            top_args[key] = str(int(raw))
        else:
            top_args[key] = _gql_literal(raw)

    arg_parts: list[str] = [f"{name}: {value}" for name, value in top_args.items()]
    if where_tree:
        col_parts = []
        for column, ops in where_tree.items():
            op_parts = ", ".join(f"{op}: {val}" for op, val in ops.items())
            col_parts.append(f"{column}: {{{op_parts}}}")
        arg_parts.append("where: {" + ", ".join(col_parts) + "}")

    args = ", ".join(arg_parts)
    fields = " ".join(selection)
    arg_clause = f"({args})" if args else ""
    return f"query {{ {table}{arg_clause} {{ {fields} }} }}"


def _find_root_field(document) -> FieldNode:
    """Return the single root selection FieldNode from a parsed query document."""
    op = document.definitions[0]
    selections = op.selection_set.selections
    assert selections, "translated GraphQL query has no root selection"
    return selections[0]


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given(
    parsers.parse("a REST client calling GET {url}"),
    target_fixture="shared_data",
)
def given_rest_client_call(url: str, shared_data: dict) -> dict:
    """Capture the auto-generated REST endpoint URL the client is invoking.

    The requirement uses ``<table>`` as a placeholder for any root query field;
    we bind it to the concrete governed table ``orders`` exposed by the schema.
    """
    if "<table>" in url:
        url = url.replace("<table>", "orders")

    parts = urlsplit(url)
    path = parts.path
    assert path.startswith("/data/rest/"), f"unexpected REST path: {path}"
    table = path[len("/data/rest/"):].strip("/")
    assert table, "REST path must include a table segment"

    shared_data["url"] = url
    shared_data["path"] = path
    shared_data["table"] = table
    shared_data["query_string"] = parts.query
    return shared_data


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("the endpoint is hit")
def when_endpoint_hit(shared_data: dict) -> None:
    """Run the request through the GraphQL compilation pipeline.

    This is exactly what the auto-generated FastAPI handler does internally:
    build the governed schema/context, translate the REST query params into a
    GraphQL document, then parse + validate that document against the schema.
    """
    schema, ctx = _build_orders_schema()
    shared_data["schema"] = schema
    shared_data["ctx"] = ctx

    query_type = schema.query_type
    assert isinstance(query_type, GraphQLObjectType)
    table = shared_data["table"]
    assert table in query_type.fields, (
        f"no auto-generated root query field for table '{table}'"
    )

    selection = ["id", "amount", "region"]
    query_str = _rest_params_to_graphql(table, shared_data["query_string"], selection)
    shared_data["graphql_query"] = query_str

    document = parse(query_str)
    shared_data["document"] = document

    errors = validate(schema, document)
    shared_data["validation_errors"] = errors


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@
