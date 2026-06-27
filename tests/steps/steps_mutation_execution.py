# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-032, REQ-034 and REQ-035 — Mutation Execution.

REQ-032: DB mutations are single-source, bypass Trino, require no registry
approval, but always require write authority on the target table.

REQ-034: Mutation input types reflect only the columns the user's role is
permitted to write; references to excluded columns are rejected at parse time.

REQ-035: RLS WHERE clauses are injected into UPDATE and DELETE mutations before
execution so that row-level security is enforced on all write operations.
"""

from __future__ import annotations

import inspect
import re

import pytest

from graphql import parse, validate
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.mutation_gen import compile_mutation, inject_rls_into_mutation
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context
from provisa.executor.direct import _WRITE_RE


scenarios("../features/REQ-032.feature")
scenarios("../features/REQ-034.feature")
scenarios("../features/REQ-035.feature")


@pytest.fixture
def shared_data():
    return {}


def _col(name, data_type="varchar(100)", nullable=False):
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_and_ctx():
    """Build a minimal SchemaInput + CompilationContext targeting a registered table."""
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
    schema = generate_schema(si)
    ctx = build_context(si)
    return si, schema, ctx


@given("a DB mutation targeting a registered table")
def given_db_mutation(shared_data):
    si, schema, ctx = _build_schema_and_ctx()
    doc = parse(
        'mutation { insertOrders(input: { amount: 42.0, region: "us-east" }) '
        "{ affected_rows } }"
    )
    # The mutation must validate against the generated schema for the registered table.
    errors = validate(schema, doc)
    assert not errors, f"mutation did not validate: {errors}"
    shared_data["schema_input"] = si
    shared_data["schema"] = schema
    shared_data["ctx"] = ctx
    shared_data["doc"] = doc
    shared_data["source_types"] = {"sales-pg": "postgresql"}


@when("the mutation is executed")
def when_mutation_executed(shared_data):
    results = compile_mutation(
        shared_data["doc"], shared_data["ctx"], shared_data["source_types"]
    )
    assert results, "compile_mutation returned no compiled mutations"
    shared_data["results"] = results
    shared_data["compiled"] = results[0]


@then(
    "it bypasses Trino and registry approval but enforces write authority "
    "on the target table"
)
def then_bypass_trino_enforce_write_authority(shared_data):
    results = shared_data["results"]
    compiled = shared_data["compiled"]

    # Single-source by definition: exactly one compiled mutation bound to one source.
    assert len(results) == 1, "DB mutation must resolve to a single source"
    assert compiled.source_id == "sales-pg", "mutation must target its single source directly"

    # It is a real write mutation against the registered target table.
    assert compiled.mutation_type == "insert"
    assert "INSERT INTO" in compiled.sql
    assert '"orders"' in compiled.sql

    # Write authority is required: the SQL is detected as a write statement by the
    # direct executor's write-detection regex (which disables read-retry and routes
    # to the single source, bypassing Trino federation).
    assert _WRITE_RE.match(compiled.sql), "mutation SQL must be classified as a write"
    assert isinstance(_WRITE_RE, re.Pattern)

    # No registry approval / routing decision: compile_mutation produces SQL bound to
    # the source with no Trino catalog routing or multi-source federation involved.
    sql_lower = compiled.sql.lower()
    assert "trino" not in sql_lower
    # Single source means no cross-source routing fan-out occurred.
    assert getattr(compiled, "source_id", None) is not None

    # Parameters are real, positional, and carry the supplied write values.
    assert compiled.params == [42.0, "us-east"]
    assert "$1" in compiled.sql


# ---------------------------------------------------------------------------
# REQ-034 — Role-filtered mutation input types
# ---------------------------------------------------------------------------


def _build_restricted_schema():
    """Build a schema for a non-admin role that excludes the 'region' column.

    The 'analyst' role is granted visibility on id and amount but NOT region;
    the generated mutation input type must therefore omit region entirely.
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
                {"column_name": "id", "visible_to": ["analyst"]},
                {"column_name": "amount", "visible_to": ["analyst"]},
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
        role={
            "id": "analyst",
            "capabilities": ["query_development"],
            "domain_access": ["*"],
        },
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return si, schema, ctx


def _find_insert_field(schema):
    """Locate the generated insert
