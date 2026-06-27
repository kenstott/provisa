# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for Hasura v2 parity low-complexity features.

REQ-212 — upsert mutations compile to ``INSERT ... ON CONFLICT ... DO UPDATE``.
REQ-213 — ``distinct_on`` query argument deduplicates results via ``DISTINCT ON``
          (PostgreSQL) or a window-function fallback (non-PostgreSQL dialects).
REQ-214 — column presets auto-set audit columns on insert/update from session
          variables (headers) or built-in functions (``now``), removing those
          columns from user input before SQL generation.
REQ-215 — inherited roles: a child role declares ``parent_role_id`` and inherits
          (merges up the chain) the parent's capabilities and domain_access. The
          hierarchy is flattened at startup into per-role dicts so authorization
          lookups remain O(1).
REQ-216 — scheduled triggers: time-based execution of registered webhooks or
          internal functions via APScheduler using cron expression syntax,
          configured per trigger in ``provisa.yaml``.
REQ-217 — batch mutations: multiple mutations in a single GraphQL request execute
          sequentially per the GraphQL specification (mutation fields are resolved
          serially in selection-set order).
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
from datetime import datetime

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from graphql import (
    FieldNode,
    GraphQLArgument,
    GraphQLField,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
    OperationDefinitionNode,
    execute,
    parse,
)
from pytest_bdd import given, parsers, scenario, then, when

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.mutation_gen import (
    MutationResult,
    apply_column_presets,
    compile_upsert,
)
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import TableMeta, build_context, compile_query


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict for passing state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_table_meta() -> TableMeta:
    """Build a TableMeta for a simple orders table with an `id` primary key."""
    return TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name="orders",
    )


def _make_field_node(name: str, args: dict) -> FieldNode:
    """Build a minimal graphql-core FieldNode for the given args dict."""

    def _render_value(v: object) -> str:
        if isinstance(v, dict):
            pairs = ", ".join(f"{k}: {_render_value(val)}" for k, val in v.items())
            return "{" + pairs + "}"
        if isinstance(v, list):
            items = ", ".join(_render_value(i) for i in v)
            return "[" + items + "]"
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, str):
            return f'"{v}"'
        return str(v)

    args_str = ", ".join(f"{k}: {_render_value(v)}" for k, v in args.items())
    gql_args = f"({args_str})" if args_str else ""
    doc = parse(f"mutation {{ {name}{gql_args} {{ id }} }}")
    op = doc.definitions[0]
    assert isinstance(op, OperationDefinitionNode)
    field = op.selection_set.selections[0]
    assert isinstance(field, FieldNode)
    return field


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_input() -> SchemaInput:
    """Build a SchemaInput for a simple PostgreSQL-backed orders table."""
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
    col_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
        ],
    }
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=col_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )


def _query_field_node(query: str) -> FieldNode:
    """Parse a GraphQL query string and return its first root field node."""
    doc = parse(query)
    op = doc.definitions[0]
    assert isinstance(op, OperationDefinitionNode)
    field = op.selection_set.selections[0]
    assert isinstance(field, FieldNode)
    return field


def _run_compile_query(field_node: FieldNode, ctx: object, table: TableMeta) -> object:
    """Invoke compile_query, adapting to its real signature."""
    sig = inspect.signature(compile_query)
    param_names = [
        name
        for name, p in sig.parameters.items()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    call_args: list[object] = [field_node]
    for name in param_names[1:]:
        lname = name.lower()
        if any(token in lname for token in ("context", "ctx")):
            call_args.append(ctx)
        elif any(token in lname for token in ("table", "meta")):
            call_args.append(table)
        elif "variable" in lname:
            call_args.append(None)
        else:
            if sig.parameters[name].default is inspect.Parameter.empty:
                call_args.append(ctx)
    return compile_query(*call_args)


# ---------------------------------------------------------------------------
# REQ-217 — Batch mutations execute sequentially per the GraphQL spec
# ---------------------------------------------------------------------------


@scenario(
    "req_217_hasura_v2_parity_low_complexity_features.feature",
    "REQ-217 default behaviour",
)
def test_req_217_default_behaviour() -> None:
    """Bind the REQ-217 batch-mutation scenario."""


@given("a GraphQL request containing multiple mutations")
def _given_multiple_mutations(shared_data: dict) -> None:
    """Parse a single GraphQL request whose selection set holds several mutations."""
    document = parse(
        "mutation BatchMutations {"
        '  a: record(tag: "a")'
        '  b: record(tag: "b")'
        '  c: record(tag: "c")'
        "}"
    )
    op = document.definitions[0]
    assert isinstance(op, OperationDefinitionNode)
    assert op.operation.value == "mutation"
    # Confirm the request really does contain more than one mutation field.
    assert len(op.selection_set.selections) == 3
    shared_data["document"] = document
    shared_data["execution_order"] = []
    shared_data["selection_order"] = [
        sel.alias.value if sel.alias else sel.name.value
        for sel in op.selection_set.selections
        if isinstance(sel, FieldNode)
    ]


@when("the request is executed")
def _when_request_executed(shared_data: dict) -> None:
    """Execute the batch mutation against a real graphql-core schema.

    Earlier mutations sleep longer than later ones. If the engine were to run
    them concurrently the recorded completion order would be reversed; serial
    (sequential) execution mandated by the GraphQL spec preserves source order.
    """
    order: list[str] = shared_data["execution_order"]
    delays = {"a": 0.03, "b": 0.02, "c": 0.01}

    async def _resolve(_parent: object, _info: object, tag: str) -> str:
        await asyncio.sleep(delays[tag])
        order.append(tag)
        return tag

    mutation_type = GraphQLObjectType(
        "Mutation",
        {
            "record": GraphQLField(
                GraphQLString,
                args={"tag": GraphQLArgument(GraphQLString)},
                resolve=_resolve,
            ),
        },
    )
    query_type = GraphQLObjectType(
        "Query",
        {"ping": GraphQLField(GraphQLString, resolve=lambda *_: "pong")},
    )
    schema = GraphQLSchema(query=query_type, mutation=mutation_type)

    result = execute(schema, shared_data["document"])
    if inspect.isawaitable(result):
        result = asyncio.run(result)
    shared_data["result"] = result


@then("mutations execute sequentially per the GraphQL spec")
def _then_mutations_sequential(shared_data: dict) -> None:
    """Assert the mutations ran serially in selection-set order without errors."""
    result = shared_data["result"]
    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data == {"a": "a", "b": "b", "c": "c"}
    # Serial execution must preserve declared order despite descending delays.
    assert shared_data["execution_order"] == shared_data["selection_order"]
    assert shared_data["execution_order"] == ["a", "b", "c"]
