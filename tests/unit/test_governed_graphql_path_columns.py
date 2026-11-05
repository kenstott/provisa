# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1b7c93-2d4a-4f68-8c3e-a7f9d0b6e214
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL or SQL read of a column registered at a path, compiled and governed (REQ-039, REQ-151,
REQ-152 amended 2026-11-05: PATHS ARE SERVED BY THE GOVERNED RELATION).

The governed relation serves a path column the way its source stores it: by its name on a source
whose connector resolves the path (MongoDB below), and as the extraction from its JSON column on a
SQL database (PostgreSQL below). Every read names the column by its name and goes through that
relation; the GraphQL compile does no extraction of its own.

MongoDB:
The governed relation serves a path column by its own name and serves no document column: every
engine's relation of the table but Trino's exposes it so (DuckDB's attach names the nested field's
column apart, #266; a replica lands it by name), and on Trino the routed statement reads it at its
path (``sql_rewrite.read_document_paths``). So the GraphQL compile reads a path column by its name,
as a SQL read does. Each case runs on both shapes of the table: the one the governed statement
reads (the column by name) and Trino's (the document's nested fields in a ROW, a STRUCT here)."""

# Requirements: REQ-039, REQ-151, REQ-152

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import lower_to_catalog_physical, read_document_paths
from provisa.compiler.stage2 import apply_governance, build_governance_context

_ROLE = {"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]}
# (name, type, path): the perf bench's order_docs, and a one-key path at the document's _id as
# the storefront and reviews demos register product_id.
_COLUMNS = [
    ("order_id", "integer", None),
    ("status", "varchar", None),
    ("customer_tier", "varchar", "customer_profile.tier"),
    ("doc_key", "integer", "_id"),
]
_TABLE = {
    "id": 3,
    "source_id": "mongo",
    "domain_id": "pb",
    "schema_name": "db",
    "table_name": "order_docs",
    "columns": [
        {"column_name": n, "visible_to": ["analyst"], **({"path": p} if p else {})}
        for n, _, p in _COLUMNS
    ],
    "write_ops": [],
    "write_returns_rows": False,
    "write_refused_forms": [],
}
_PATHS = {("mongo", "db", "order_docs"): {n: p for n, _, p in _COLUMNS if p}}
_DOCS = [(1, "open", "gold", 101), (2, "closed", "silver", 102), (3, "open", "gold", 103)]


def _governed(query: str) -> tuple[str, str, list]:
    """The compiled GraphQL SQL, the governed statement and its parameters."""
    si = SchemaInput(
        tables=[_TABLE],
        relationships=[],
        column_types={
            3: [
                ColumnMetadata(column_name=n, data_type=t, is_nullable=True) for n, t, _ in _COLUMNS
            ]
        },
        naming_rules=[],
        role=_ROLE,
        domains=[{"id": "pb", "description": "path columns"}],
        source_types={"mongo": "mongodb"},
    )
    schema, ctx = generate_schema(si), build_context(si)
    gov = build_governance_context("analyst", RLSContext(rules={}), {}, ctx, [_TABLE], _ROLE, [])
    (compiled,) = compile_query(parse_query(schema, query, ctx=ctx), ctx)
    governed = lower_to_catalog_physical(apply_governance(compiled.sql, gov), ctx)
    return compiled.sql, governed, list(compiled.params)


def _con(trino_shape: bool) -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS mongo")
    c.execute("CREATE SCHEMA mongo.db")
    if trino_shape:
        c.execute(
            "CREATE TABLE mongo.db.order_docs (_id INTEGER, order_id INTEGER, status VARCHAR, "
            "customer_profile STRUCT(tier VARCHAR))"
        )
        for oid, status, tier, key in _DOCS:
            c.execute(
                "INSERT INTO mongo.db.order_docs VALUES (?, ?, ?, {'tier': ?})",
                [key, oid, status, tier],
            )
    else:
        c.execute(
            "CREATE TABLE mongo.db.order_docs (order_id INTEGER, status VARCHAR, "
            "customer_tier VARCHAR, doc_key INTEGER)"
        )
        c.executemany("INSERT INTO mongo.db.order_docs VALUES (?, ?, ?, ?)", _DOCS)
    return c


def _rows(query: str, trino_shape: bool) -> list[tuple]:
    _, governed, params = _governed(query)
    if trino_shape:
        governed = read_document_paths(governed, "postgres", _PATHS)
    sql = sqlglot.transpile(governed, read="postgres", write="duckdb")[0]
    return sorted(_con(trino_shape).execute(sql, params).fetchall())


def test_a_path_column_is_compiled_by_its_name():
    compiled, governed, _ = _governed("query { orderDocs { orderId customerTier } }")
    assert compiled == 'SELECT "order_id", "customer_tier" FROM "db"."order_docs"'
    assert "customer_profile" not in governed


def test_a_one_key_path_is_compiled_by_its_name():
    compiled, _, _ = _governed("query { orderDocs { docKey status } }")
    assert compiled == 'SELECT "doc_key", "status" FROM "db"."order_docs"'


def test_an_aliased_path_column_is_compiled_by_its_name():
    compiled, _, _ = _governed("query { orderDocs { orderId tier: customerTier } }")
    assert compiled == 'SELECT "order_id", "customer_tier" AS "tier" FROM "db"."order_docs"'


@pytest.mark.parametrize("trino_shape", [False, True], ids=["by_name", "trino_row"])
def test_a_read_path_column_answers_its_value(trino_shape):
    rows = _rows("query { orderDocs { orderId customerTier } }", trino_shape)
    assert rows == [(oid, tier) for oid, _, tier, _ in _DOCS]


@pytest.mark.parametrize("trino_shape", [False, True], ids=["by_name", "trino_row"])
def test_a_filter_on_a_path_column_reads_its_value(trino_shape):
    query = 'query { orderDocs(where: {customerTier: {eq: "gold"}}) { orderId customerTier } }'
    rows = _rows(query, trino_shape)
    assert rows == [(oid, tier) for oid, _, tier, _ in _DOCS if tier == "gold"]


@pytest.mark.parametrize("trino_shape", [False, True], ids=["by_name", "trino_row"])
def test_a_one_key_path_reads_its_top_level_field(trino_shape):
    rows = _rows("query { orderDocs { docKey status } }", trino_shape)
    assert rows == sorted((key, status) for _, status, _, key in _DOCS)


# --- a SQL read of the MongoDB table: the path column by its name, through the same relation -----


def _governed_sql(statement: str) -> str:
    """``statement`` -- a SQL read as a client writes it -- governed for the analyst."""
    si = SchemaInput(
        tables=[_TABLE],
        relationships=[],
        column_types={
            3: [
                ColumnMetadata(column_name=n, data_type=t, is_nullable=True) for n, t, _ in _COLUMNS
            ]
        },
        naming_rules=[],
        role=_ROLE,
        domains=[{"id": "pb", "description": "path columns"}],
        source_types={"mongo": "mongodb"},
    )
    ctx = build_context(si)
    gov = build_governance_context("analyst", RLSContext(rules={}), {}, ctx, [_TABLE], _ROLE, [])
    return lower_to_catalog_physical(apply_governance(statement, gov), ctx)


def test_a_sql_read_serves_a_path_column_by_its_name():
    governed = _governed_sql("SELECT order_id, customer_tier FROM db.order_docs")
    assert '"order_docs"."customer_tier" AS "customer_tier"' in governed
    assert "customer_profile" not in governed and "->>" not in governed


@pytest.mark.parametrize("trino_shape", [False, True], ids=["by_name", "trino_row"])
def test_a_sql_filter_on_a_path_column_reads_its_value(trino_shape):
    governed = _governed_sql(
        "SELECT order_id, customer_tier FROM db.order_docs WHERE customer_tier = 'gold'"
    )
    if trino_shape:
        governed = read_document_paths(governed, "postgres", _PATHS)
    sql = sqlglot.transpile(governed, read="postgres", write="duckdb")[0]
    rows = sorted(_con(trino_shape).execute(sql).fetchall())
    assert rows == [(oid, tier) for oid, _, tier, _ in _DOCS if tier == "gold"]


# --- a SQL database: the path extracted from its JSON column inside the relation ---------------

# The three registered forms of a path into a PostgreSQL JSON column ``payload``
# ({"order_id": ..., "total": ...}): (columns, the column read, the name read).
#  - REQ-151's: the JSON column itself registered at a one-key path, exposed under its alias;
#  - REQ-136's: the JSON column registered at ``payload.total``, exposed under its own name;
#  - named: a column of its own name registered at ``payload.total``, beside the JSON column.
_PG_FORMS = {
    "one_key_alias": (
        [{"column_name": "payload", "path": "order_id", "alias": "order_id"}],
        "order_id",
        "'order_id'",
    ),
    "json_column_at_path": (
        [{"column_name": "payload", "path": "payload.total"}],
        "payload",
        "'total'",
    ),
    "named": (
        [{"column_name": "payload"}, {"column_name": "total", "path": "payload.total"}],
        "total",
        "'total'",
    ),
}
_PG_ROWS = [(1, '{"order_id": "a1", "total": "10"}'), (2, '{"order_id": "b2", "total": "20"}')]


def _pg_table(columns: list[dict]) -> dict:
    return {
        "id": 5,
        "source_id": "sales-pg",
        "domain_id": "pb",
        "schema_name": "public",
        "table_name": "events",
        "columns": [{**c, "visible_to": ["analyst"]} for c in [{"column_name": "id"}, *columns]],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _pg_context(form: str):
    columns, _, _ = _PG_FORMS[form]
    table = _pg_table(columns)
    types = {"id": "integer", "payload": "json", "total": "varchar"}
    si = SchemaInput(
        tables=[table],
        relationships=[],
        column_types={
            5: [
                ColumnMetadata(
                    column_name=c["column_name"],
                    data_type=types[c["column_name"]],
                    is_nullable=True,
                )
                for c in table["columns"]
            ]
        },
        naming_rules=[],
        role=_ROLE,
        domains=[{"id": "pb", "description": "path columns"}],
        source_types={"sales-pg": "postgresql"},
    )
    ctx = build_context(si)
    gov = build_governance_context("analyst", RLSContext(rules={}), {}, ctx, [table], _ROLE, [])
    return generate_schema(si), ctx, gov


def _pg_rows(governed: str, params: list | None = None) -> list[tuple]:
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS sales_pg")
    c.execute("CREATE SCHEMA sales_pg.public")
    c.execute("CREATE TABLE sales_pg.public.events (id INTEGER, payload JSON)")
    c.executemany("INSERT INTO sales_pg.public.events VALUES (?, ?)", _PG_ROWS)
    sql = sqlglot.transpile(governed, read="postgres", write="duckdb")[0]
    return sorted(c.execute(sql, params or []).fetchall())


def _pg_graphql(form: str, where: str = "") -> tuple[str, str, list, list]:
    """The compiled GraphQL SQL, the governed statement, its parameters and its columns."""
    schema, ctx, gov = _pg_context(form)
    query = f"query {{ events{where} {{ id {_PG_FORMS[form][1]} }} }}"
    (compiled,) = compile_query(parse_query(schema, query, ctx=ctx), ctx)
    governed = lower_to_catalog_physical(apply_governance(compiled.sql, gov), ctx)
    return compiled.sql, governed, list(compiled.params), compiled.columns


def _pg_sql(form: str, where: str = "") -> str:
    _, ctx, gov = _pg_context(form)
    name = _PG_FORMS[form][1]
    return lower_to_catalog_physical(
        apply_governance(f"SELECT id, {name} FROM public.events{where}", gov), ctx
    )


def _expected(form: str) -> list[tuple]:
    key = "order_id" if form == "one_key_alias" else "total"
    return [(i, {"order_id": ["a1", "b2"], "total": ["10", "20"]}[key][i - 1]) for i, _ in _PG_ROWS]


@pytest.mark.parametrize("form", list(_PG_FORMS))
def test_a_graphql_read_names_a_sql_databases_path_column_and_the_relation_extracts_it(form):
    compiled, governed, _, columns = _pg_graphql(form)
    name, key = _PG_FORMS[form][1], _PG_FORMS[form][2]
    # the GraphQL compile reads the column by its name and extracts nothing
    assert compiled == f'SELECT "id", "{name}" FROM "public"."events"'
    # the relation serves the path read from the JSON column
    assert f'"events"."payload" ->> {key}' in governed
    # its value is the text at the path, not a JSON document
    assert not next(c for c in columns if c.column == name).json


@pytest.mark.parametrize("form", list(_PG_FORMS))
def test_a_graphql_read_of_a_sql_databases_path_column_answers_its_value(form):
    _, governed, params, _ = _pg_graphql(form)
    assert _pg_rows(governed, params) == _expected(form)


def test_a_graphql_filter_on_a_sql_databases_path_column_reads_its_value():
    # The named form: its column's GraphQL type is its own (text), which a filter compares.
    _, governed, params, _ = _pg_graphql("named", '(where: {total: {eq: "20"}})')
    assert '"events"."payload" ->> \'total\' AS "total"' in governed
    assert _pg_rows(governed, params) == [(2, "20")]


# The value each form reads on row 2, which an eq filter on the path column picks out.
_ROW_2 = {"one_key_alias": "b2", "json_column_at_path": "20", "named": "20"}


@pytest.mark.parametrize("form", list(_PG_FORMS))
def test_a_sql_databases_path_column_is_a_string_in_the_graphql_schema(form):
    # Served as the text at its path (->>), whatever the type of the JSON column it is read from.
    from graphql import GraphQLString, get_named_type

    schema, _, _ = _pg_context(form)
    events = get_named_type(schema.query_type.fields["events"].type)
    assert get_named_type(events.fields[_PG_FORMS[form][1]].type) is GraphQLString


@pytest.mark.parametrize("form", list(_PG_FORMS))
def test_a_graphql_eq_filter_on_a_sql_databases_path_column_compares_its_text(form):
    name, value = _PG_FORMS[form][1], _ROW_2[form]
    _, governed, params, _ = _pg_graphql(form, f'(where: {{{name}: {{eq: "{value}"}}}})')
    assert _pg_rows(governed, params) == [(2, value)]


@pytest.mark.parametrize("form", list(_PG_FORMS))
def test_a_sql_read_of_a_sql_databases_path_column_goes_through_the_same_relation(form):
    governed = _pg_sql(form)
    assert f'"events"."payload" ->> {_PG_FORMS[form][2]}' in governed
    assert _pg_rows(governed) == _expected(form)


@pytest.mark.parametrize("form", list(_PG_FORMS))
def test_a_sql_filter_on_a_sql_databases_path_column_reads_its_value(form):
    name = _PG_FORMS[form][1]
    value = "b2" if form == "one_key_alias" else "20"
    governed = _pg_sql(form, f" WHERE {name} = '{value}'")
    assert _pg_rows(governed) == [(2, value)]


def _named_path_input(source_type: str = "") -> SchemaInput:
    """The named form's table, on ``sales-pg`` of ``source_type`` (none: a type not known)."""
    table = _pg_table(_PG_FORMS["named"][0])
    return SchemaInput(
        tables=[table],
        relationships=[],
        column_types={
            5: [
                ColumnMetadata(column_name=n, data_type=t, is_nullable=True)
                for n, t in [("id", "integer"), ("payload", "json"), ("total", "varchar")]
            ]
        },
        naming_rules=[],
        role=_ROLE,
        domains=[{"id": "pb", "description": "path columns"}],
        source_types={"sales-pg": source_type} if source_type else {},
    )


def test_a_path_column_on_a_source_of_unknown_type_is_refused():
    with pytest.raises(ValueError, match="source's type is not known"):
        build_context(_named_path_input())


# --- each source type declares how it serves a path column (core.source_registry) -------------

# The connector sources served through the pgwire bridge read a table by its registered column
# names and are never given a column's path, so they serve no path column.
_SERVES_NONE = ["servicenow", "salesforce", "splunk", "sharepoint", "cloudops", "files", "govdata"]


@pytest.mark.parametrize("source_type", _SERVES_NONE)
def test_a_path_column_on_a_source_type_that_serves_none_is_refused(source_type):
    with pytest.raises(ValueError, match=f"{source_type!r} serves no path column"):
        build_context(_named_path_input(source_type))


@pytest.mark.parametrize(
    "source_type",
    [
        "mongodb",
        "elasticsearch",
        "qdrant",
        "cosmosdb",
        "jira",
        "quickbooks",
        "openapi",
        "graphql_remote",
        "grpc_remote",
        "ingest",
        "python",
        "node",
        "go",
    ],
)
def test_a_source_whose_connector_resolves_the_path_serves_it_by_its_name(source_type):
    from provisa.compiler.sql_types import extracts_paths

    assert extracts_paths(source_type, "t") is False


@pytest.mark.parametrize("source_type", ["postgresql", "mysql", "snowflake", "iceberg", "duckdb"])
def test_a_sql_database_extracts_the_path_from_its_json_column(source_type):
    from provisa.compiler.sql_types import extracts_paths

    assert extracts_paths(source_type, "t") is True


def test_every_declaration_names_a_source_type_and_a_way_of_serving():
    from provisa.core.models import SourceType
    from provisa.core.source_registry import PATH_COLUMNS, PATHS_EXTRACTED, PATHS_RESOLVED

    assert set(PATH_COLUMNS) <= {t.value for t in SourceType}
    assert set(PATH_COLUMNS.values()) <= {PATHS_RESOLVED, PATHS_EXTRACTED}
