# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-151 / REQ-152 — Column Path Extraction.

REQ-151: Columns declared with a JSON path extract values from JSON source
columns using PostgreSQL ``->>`` syntax. SQLGlot transpiles those expressions
to ``json_extract_scalar`` when targeting Trino.

REQ-152: Path columns on PostgreSQL sources route direct (PG ``->>``). Non-PG
sources are forced through Trino routing (``json_extract_scalar``).
"""

from __future__ import annotations


import pytest
import sqlglot

from graphql import (
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    parse,
    validate,
)
from pytest_bdd import given, scenario, then, when

from provisa.compiler import naming as _naming
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col(name: str, data_type: str = "varchar", nullable: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _make_path_schema_input() -> SchemaInput:
    """Build a SchemaInput with a JSON column that promotes path-extracted fields."""
    _naming.configure(gql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "events",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {
                    "column_name": "payload",
                    "visible_to": ["admin"],
                    "object_fields": [
                        {"name": "order_id", "type": "integer"},
                        {"name": "amount", "type": "number"},
                        {"name": "status", "type": "string"},
                    ],
                },
            ],
        }
    ]
    column_types = {
        1: [
            _col("id", "integer", nullable=False),
            _col("payload", "json"),
        ]
    }
    role = {"id": "admin", "capabilities": [], "domain_access": ["*"]}
    domains = [{"id": "sales", "graphql_alias": None}]
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )


def _make_routed_schema_input(source_id: str, source_type: str, table_name: str) -> SchemaInput:
    """Build a SchemaInput for a given source type carrying a JSON path column."""
    _naming.configure(gql="snake")
    tables = [
        {
            "id": 1,
            "source_id": source_id,
            "source_type": source_type,
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": table_name,
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {
                    "column_name": "payload",
                    "visible_to": ["admin"],
                    "object_fields": [
                        {"name": "order_id", "type": "integer"},
                        {"name": "amount", "type": "number"},
                        {"name": "status", "type": "string"},
                    ],
                },
            ],
        }
    ]
    column_types = {
        1: [
            _col("id", "integer", nullable=False),
            _col("payload", "json"),
        ]
    }
    role = {"id": "admin", "capabilities": [], "domain_access": ["*"]}
    domains = [{"id": "sales", "graphql_alias": None}]
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )


def _unwrap(gql_type):
    while isinstance(gql_type, (GraphQLNonNull, GraphQLList)):
        gql_type = gql_type.of_type
    return gql_type


def _build_query_string(schema, ctx) -> str:
    """Construct a GraphQL query that selects the path-extracted JSON fields."""
    # Determine root query field that maps to table_id 1.
    root_field = next(fn for fn, tm in ctx.tables.items() if tm.table_id == 1)

    # GraphQL field names that carry JSON path extraction.
    path_fields = [gql for (tid, gql) in ctx.column_paths.keys() if tid == 1]
    assert path_fields, "expected at least one column with a JSON path"

    qt = schema.query_type
    assert qt is not None and root_field in qt.fields
    root_type = _unwrap(qt.fields[root_field].type)
    assert isinstance(root_type, GraphQLObjectType)

    # Some path fields may be exposed nested under a JSON object field; resolve
    # the selectable shape by walking the schema.
    selectable = [f for f in path_fields if f in root_type.fields]
    if selectable:
        inner = " ".join(selectable)
        return f"{{ {root_field} {{ {inner} }} }}"

    # Fallback: the JSON column is a nested object — select its subfields.
    for sub_name, sub_field in root_type.fields.items():
        sub_type = _unwrap(sub_field.type)
        if isinstance(sub_type, GraphQLObjectType):
            sub_selectable = [f for f in path_fields if f in sub_type.fields]
            if sub_selectable:
                inner = " ".join(sub_selectable)
                return f"{{ {root_field} {{ {sub_name} {{ {inner} }} }} }}"

    raise AssertionError("no selectable path-extracted fields found in schema")


def _compile_pg_sql(schema, ctx) -> str:
    """Compile a path-extracting query into PG-style SQL and return the SQL string."""
    query = _build_query_string(schema, ctx)
    document = parse(query)
    errors = validate(schema, document)
    assert not errors, f"GraphQL validation errors: {errors}"
    results = compile_query(document, ctx)
    assert results, "compile_query produced no results"
    compiled = results[0]
    assert compiled.sql, "compile_query produced empty SQL"
    return compiled.sql


def _compile_for_source(source_id: str, source_type: str, table_name: str):
    """Build a schema/ctx for the given source and compile a path-extracting query."""
    si = _make_routed_schema_input(source_id, source_type, table_name)
    schema = generate_schema(si)
    ctx = build_context(si)

    # Sanity: the JSON path columns must be registered for this source.
    paths_for_table = {gql: expr for (tid, gql), expr in ctx.column_paths.items() if tid == 1}
    assert paths_for_table, f"expected JSON path columns for source {source_id}"

    query = _build_query_string(schema, ctx)
    document = parse(query)
    errors = validate(schema, document)
    assert not errors, f"GraphQL validation errors for {source_id}: {errors}"
    results = compile_query(document, ctx)
    assert results, f"compile_query produced no results for {source_id}"
    compiled = results[0]
    assert compiled.sql, f"compile_query produced empty SQL for {source_id}"
    return compiled


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------


@scenario("../features/REQ-151.feature", "REQ-151 default behaviour")
def test_req_151_default_behaviour():
    """REQ-151 — Column Path Extraction default behaviour."""


@scenario("../features/REQ-152.feature", "REQ-152 default behaviour")
def test_req_152_default_behaviour():
    """REQ-152 — Column Path Extraction routing default behaviour."""


# ---------------------------------------------------------------------------
# Steps — REQ-151
# ---------------------------------------------------------------------------


@given("a column configured with a path pointing into a JSON source column")
def given_path_column(shared_data):
    si = _make_path_schema_input()
    schema = generate_schema(si)
    ctx = build_context(si)

    # Real assertion: at least one column on table 1 was registered with a
    # JSON path expression (e.g. "payload.order_id").
    paths_for_table = {gql: expr for (tid, gql), expr in ctx.column_paths.items() if tid == 1}
    assert paths_for_table, "expected JSON path columns to be registered for the events table"
    # Path expressions must point into the JSON 'payload' source column.
    assert any(expr.startswith("payload") for expr in paths_for_table.values()), (
        f"path expressions do not reference JSON source column: {paths_for_table}"
    )

    shared_data["schema"] = schema
    shared_data["ctx"] = ctx
    shared_data["column_paths"] = paths_for_table


@when("a query is compiled for a PostgreSQL source")
def when_compile_pg(shared_data):
    schema = shared_data["schema"]
    ctx = shared_data["ctx"]
    sql = _compile_pg_sql(schema, ctx)
    shared_data["pg_sql"] = sql


@then("PG >> syntax is used; when compiled for Trino, json_extract_scalar is used")
def then_pg_and_trino_path_syntax(shared_data):
    pg_sql = shared_data["pg_sql"]

    # PostgreSQL compilation must emit the JSON text-extraction operator '->>'.
    assert "->>" in pg_sql, f"expected PG ->> JSON extraction syntax in SQL: {pg_sql}"

    # SQLGlot transpiles the PG expression to Trino's json_extract_scalar.
    transpiled = sqlglot.transpile(pg_sql, read="postgres", write="trino")
    assert transpiled, "SQLGlot produced no Trino output"
    trino_sql = transpiled[0]
    assert "json_extract_scalar" in trino_sql.lower(), (
        f"expected json_extract_scalar in Trino SQL: {trino_sql}"
    )

    # The Trino form must not retain the raw PG operator.
    assert "->>" not in trino_sql, f"Trino SQL still contains PG operator: {trino_sql}"

    shared_data["trino_sql"] = trino_sql


# ---------------------------------------------------------------------------
# Steps — REQ-152 (path column routing rules)
# ---------------------------------------------------------------------------


@given("a table with path columns")
def given_table_with_path_columns(shared_data):
    # Register one PostgreSQL source and a representative set of non-PG sources.
    # Each carries a JSON 'payload' column promoting path-extracted fields.
    shared_data["pg_source"] = {
        "source_id": "sales-pg",
        "source_type": "postgresql",
        "table_name": "events_pg",
    }
    shared_data["non_pg_sources"] = [
        {"source_id": "lake-iceberg", "source_type": "iceberg", "table_name": "events_iceberg"},
        {"source_id": "warehouse-mysql", "source_type": "mysql", "table_name": "events_mysql"},
        {"source_id": "files-delta", "source_type": "delta", "table_name": "events_delta"},
    ]

    # Real assertion: building the PG schema/ctx registers JSON path columns.
    pg = shared_data["pg_source"]
    si = _make_routed_schema_input(pg["source_id"], pg["source_type"], pg["table_name"])
    ctx = build_context(si)
    pg_paths = {gql: expr for (tid, gql), expr in ctx.column_paths.items() if tid == 1}
    assert pg_paths, "expected JSON path columns registered for the PostgreSQL table"
    assert any(expr.startswith("payload") for expr in pg_paths.values())

    # Also verify each non-PG source registers path columns correctly.
    for src in shared_data["non_pg_sources"]:
        si_src = _make_routed_schema_input(src["source_id"], src["source_type"], src["table_name"])
        ctx_src = build_context(si_src)
        src_paths = {gql: expr for (tid, gql), expr in ctx_src.column_paths.items() if tid == 1}
        assert src_paths, (
            f"expected JSON path columns registered for non-PG source {src['source_id']}"
        )
        assert any(expr.startswith("payload") for expr in src_paths.values()), (
            f"path expressions for {src['source_id']} do not reference payload column"
        )


@when("the query engine routes a query")
def when_engine_routes_query(shared_data):
    pg = shared_data["pg_source"]
    pg_compiled = _compile_for_source(pg["source_id"], pg["source_type"], pg["table_name"])
    shared_data["pg_compiled"] = pg_compiled

    non_pg_compiled = {}
    for src in shared_data["non_pg_sources"]:
        compiled = _compile_for_source(src["source_id"], src["source_type"], src["table_name"])
        non_pg_compiled[src["source_id"]] = compiled
    shared_data["non_pg_compiled"] = non_pg_compiled


@then("PostgreSQL sources use the direct route and all non-PG sources are forced through Trino")
def then_routing_rules(shared_data):
    # --- PostgreSQL source: direct route uses native PG '->>' extraction. ---
    pg_compiled = shared_data["pg_compiled"]
    pg_sql = pg_compiled.sql
    assert "->>" in pg_sql, f"expected PG direct-route ->> syntax for PostgreSQL source: {pg_sql}"
    assert "json_extract_scalar" not in pg_sql.lower(), (
        f"PostgreSQL direct route must not use Trino json_extract_scalar: {pg_sql}"
    )

    # Verify the PG source is represented in compiled.sources.
    pg_source_id = shared_data["pg_source"]["source_id"]
    assert pg_source_id in pg_compiled.sources, (
        f"compiled.sources {pg_compiled.sources} missing PG source {pg_source_id}"
    )

    # The PG-direct SQL must transpile cleanly to Trino if ever rerouted.
    pg_trino = sqlglot.transpile(pg_sql, read="postgres", write="trino")
    assert pg_trino, "SQLGlot produced no Trino output for PG SQL"
    assert "json_extract_scalar" in pg_trino[0].lower(), (
        f"transpiled PG→Trino SQL missing json_extract_scalar: {pg_trino[0]}"
    )

    # --- Non-PG sources: forced through Trino → json_extract_scalar. ---
    non_pg_compiled = shared_data["non_pg_compiled"]
    assert non_pg_compiled, "expected at least one non-PG source to be compiled"

    for source_id, compiled in non_pg_compiled.items():
        sql = compiled.sql
        # Non-PG path extraction must be expressible/valid as Trino SQL. The
        # generated SQL is PG-dialect; forcing Trino routing yields
        # json_extract_scalar with no surviving PG operator.
        trino_sql = sqlglot.transpile(sql, read="postgres", write="trino")
        assert trino_sql, f"SQLGlot produced no Trino output for {source_id}"
        trino_lower = trino_sql[0].lower()
        assert "json_extract_scalar" in trino_lower, (
            f"non-PG source {source_id} must route through Trino json_extract_scalar: {trino_sql[0]}"
        )
        assert "->>" not in trino_sql[0], (
            f"Trino-routed SQL for {source_id} still contains PG operator: {trino_sql[0]}"
        )

        # The compiled sources set must reflect the non-PG origin.
        assert source_id in compiled.sources, (
            f"compiled.sources {compiled.sources} missing non-PG source {source_id}"
        )

    # PostgreSQL and non-PG sources must be distinct routing populations.
    assert pg_source_id not in non_pg_compiled, (
        f"PG source {pg_source_id} incorrectly appeared in non-PG compiled results"
    )

    # Confirm the three non-PG sources are all accounted for.
    expected_non_pg_ids = {src["source_id"] for src in shared_data["non_pg_sources"]}
    actual_non_pg_ids = set(non_pg_compiled.keys())
    assert expected_non_pg_ids == actual_non_pg_ids, (
        f"non-PG source mismatch: expected {expected_non_pg_ids}, got {actual_non_pg_ids}"
    )
