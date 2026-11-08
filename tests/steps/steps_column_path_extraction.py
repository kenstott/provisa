# Copyright (c) 2026 Kenneth Stott
# Canary: 596be67d-a519-416a-9cd1-c962c0bad998
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-151 / REQ-152 — Column Path Extraction.

Amended 2026-11-05, PATHS ARE SERVED BY THE GOVERNED RELATION: the governed relation serves each
path column the way its source stores it -- as the extraction from its JSON column on a SQL
database (PostgreSQL ``->>``, which SQLGlot transpiles to ``json_extract_scalar`` for Trino), and
by its name where the source's connector resolves the path (MongoDB). Every read names a path
column by its name and goes through that one relation; the GraphQL compile does no extraction.

REQ-151: a column at a path into a PostgreSQL JSON column is read by name, extracted with ``->>``
inside the governed relation, and that extraction reads as ``json_extract_scalar`` on Trino.

REQ-152: a PostgreSQL source's governed read keeps the PG extraction (its direct route); every
other SQL database's transpiles to Trino's ``json_extract_scalar``; a connector-resolved source's
relation serves the column by its name.
"""

from __future__ import annotations

import pytest
import sqlglot
from graphql import parse, validate
from pytest_bdd import given, scenario, then, when

from tests.helpers import registry_write_ops, registry_write_returns_rows
from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.stage2 import apply_governance, build_governance_context

_ROLE = {"description": "Test role.", "id": "admin", "capabilities": [], "domain_access": ["*"]}


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


def _table(source_id: str, source_type: str, table_name: str) -> dict:
    """A table whose JSON ``payload`` holds an order; ``order_id`` is registered at its path."""
    return {
        "id": 1,
        "source_id": source_id,
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": table_name,
        "write_ops": registry_write_ops(source_type),
        "write_returns_rows": registry_write_returns_rows(source_type),
        "write_refused_forms": [],
        "columns": [
            {"column_name": "id", "visible_to": ["admin"]},
            {"column_name": "payload", "visible_to": ["admin"]},
            {"column_name": "order_id", "visible_to": ["admin"], "path": "payload.order_id"},
        ],
    }


def _registered(source_id: str, source_type: str, table_name: str) -> dict:
    """The schema, compilation context and admin's governance context of the table on a source
    of ``source_type``."""
    _naming.configure(gql="snake")
    table = _table(source_id, source_type, table_name)
    si = SchemaInput(
        tables=[table],
        relationships=[],
        column_types={
            1: [_col("id", "integer", nullable=False), _col("payload", "json"), _col("order_id")]
        },
        naming_rules=[],
        role=_ROLE,
        domains=[{"id": "sales", "graphql_alias": None}],
        source_types={source_id: source_type},
    )
    ctx = build_context(si)
    gov = build_governance_context("admin", RLSContext(rules={}), {}, ctx, [table], _ROLE, [])
    return {"schema": generate_schema(si), "ctx": ctx, "gov": gov, "source_id": source_id}


def _read(registered: dict) -> dict:
    """A GraphQL read of the path column, compiled and governed."""
    ctx = registered["ctx"]
    root_field = next(fn for fn, tm in ctx.tables.items() if tm.table_id == 1)
    document = parse(f"{{ {root_field} {{ id order_id }} }}")
    errors = validate(registered["schema"], document)
    assert not errors, f"GraphQL validation errors: {errors}"
    (compiled,) = compile_query(document, ctx)
    return {
        "compiled": compiled,
        "governed": apply_governance(compiled.sql, registered["gov"]),
    }


def _trino(sql: str) -> str:
    (out,) = sqlglot.transpile(sql, read="postgres", write="trino")
    return out


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
    registered = _registered("sales-pg", "postgresql", "events")
    # The governed relation reads order_id from the JSON column payload at the key order_id.
    assert registered["gov"].stored_paths[1] == {"order_id": ("payload", ["order_id"])}
    shared_data["registered"] = registered


@when("a query is compiled for a PostgreSQL source")
def when_compile_pg(shared_data):
    shared_data.update(_read(shared_data["registered"]))


@then("PG >> syntax is used; when compiled for Trino, json_extract_scalar is used")
def then_pg_and_trino_path_syntax(shared_data):
    compiled, governed = shared_data["compiled"].sql, shared_data["governed"]
    # The GraphQL compile names the column and extracts nothing ...
    assert compiled == 'SELECT "id", "order_id" FROM "public"."events"', compiled
    # ... the governed relation extracts it from its JSON column with PG ->> ...
    assert '"events"."payload" ->> \'order_id\' AS "order_id"' in governed, governed
    # ... which SQLGlot transpiles to Trino's json_extract_scalar.
    trino_sql = _trino(governed)
    assert """JSON_EXTRACT_SCALAR("events"."payload", '$.order_id')""" in trino_sql, trino_sql
    assert "->>" not in trino_sql, trino_sql


# ---------------------------------------------------------------------------
# Steps — REQ-152 (path column routing rules)
# ---------------------------------------------------------------------------


@given("a table with path columns")
def given_table_with_path_columns(shared_data):
    # One PostgreSQL source, SQL databases of other types, and a source whose connector
    # resolves the path; each registers order_id at its path in payload.
    shared_data["pg"] = _registered("sales-pg", "postgresql", "events_pg")
    shared_data["non_pg"] = {
        sid: _registered(sid, stype, name)
        for sid, stype, name in [
            ("lake-iceberg", "iceberg", "events_iceberg"),
            ("warehouse-mysql", "mysql", "events_mysql"),
            ("files-delta", "delta_lake", "events_delta"),
        ]
    }
    shared_data["connector"] = _registered("docs-mongo", "mongodb", "events_mongo")
    for registered in [shared_data["pg"], *shared_data["non_pg"].values()]:
        assert registered["gov"].stored_paths[1] == {"order_id": ("payload", ["order_id"])}
    # A connector-resolved source's relation holds order_id by its name: nothing to extract.
    assert 1 not in shared_data["connector"]["gov"].stored_paths


@when("the query engine routes a query")
def when_engine_routes_query(shared_data):
    shared_data["pg_read"] = _read(shared_data["pg"])
    shared_data["non_pg_reads"] = {sid: _read(r) for sid, r in shared_data["non_pg"].items()}
    shared_data["connector_read"] = _read(shared_data["connector"])


@then("PostgreSQL sources use the direct route and all non-PG sources are forced through Trino")
def then_routing_rules(shared_data):
    # --- PostgreSQL: the governed read keeps PG's own ->>, its direct route's syntax. ---
    pg = shared_data["pg_read"]
    assert '"payload" ->> \'order_id\' AS "order_id"' in pg["governed"], pg["governed"]
    assert "json_extract_scalar" not in pg["governed"].lower()
    assert "sales-pg" in pg["compiled"].sources
    assert "json_extract_scalar" in _trino(pg["governed"]).lower()

    # --- Every other SQL database: the same relation, read on Trino as json_extract_scalar. ---
    for source_id, read in shared_data["non_pg_reads"].items():
        assert 'SELECT "id", "order_id" FROM' in read["compiled"].sql, read["compiled"].sql
        assert "->>" not in read["compiled"].sql
        trino_sql = _trino(read["governed"])
        assert "json_extract_scalar(" in trino_sql.lower(), trino_sql
        assert "->>" not in trino_sql, trino_sql
        assert source_id in read["compiled"].sources
    assert set(shared_data["non_pg_reads"]) == {"lake-iceberg", "warehouse-mysql", "files-delta"}

    # --- A connector-resolved source: its relation serves the column by its name. ---
    mongo = shared_data["connector_read"]["governed"]
    assert '"order_id" AS "order_id"' in mongo, mongo
    assert "->>" not in mongo and "json_extract" not in _trino(mongo).lower(), mongo
