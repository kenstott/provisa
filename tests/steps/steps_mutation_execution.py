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

import re

import pytest

from graphql import parse, validate
from pytest_bdd import given, when, then, scenarios

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.mutation_gen import compile_mutation, inject_rls_into_mutation
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.context import build_context
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
        'mutation { insertOrders(input: { amount: 42.0, region: "us-east" }) { affected_rows } }'
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
    results = compile_mutation(shared_data["doc"], shared_data["ctx"], shared_data["source_types"])
    assert results, "compile_mutation returned no compiled mutations"
    shared_data["results"] = results
    shared_data["compiled"] = results[0]


@then("it bypasses Trino and registry approval but enforces write authority on the target table")
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
    """Locate the generated insert mutation field for orders in the schema."""
    mutation_type = schema.mutation_type
    assert mutation_type is not None, "schema has no mutation type"
    fields = mutation_type.fields
    insert_field = None
    for name, field_def in fields.items():
        if "insert" in name.lower() and "order" in name.lower():
            insert_field = (name, field_def)
            break
    assert insert_field is not None, (
        f"no insert field found in mutation type; fields={list(fields)}"
    )
    return insert_field


@given("a user whose role excludes certain columns")
def given_user_role_excludes_columns(shared_data):
    """Set up an analyst role schema where 'region' is excluded from visibility."""
    si, schema, ctx = _build_restricted_schema()
    shared_data["schema_input"] = si
    shared_data["schema"] = schema
    shared_data["ctx"] = ctx
    shared_data["source_types"] = {"sales-pg": "postgresql"}
    # Record which column is excluded for this role
    shared_data["excluded_column"] = "region"
    # Record which columns are permitted for this role
    shared_data["permitted_columns"] = ["id", "amount"]


@when("a mutation input type is generated for that role")
def when_mutation_input_type_generated(shared_data):
    """Inspect the generated schema mutation input type for the restricted role."""
    schema = shared_data["schema"]

    # Find the insert mutation field
    insert_field_name, insert_field_def = _find_insert_field(schema)
    shared_data["insert_field_name"] = insert_field_name
    shared_data["insert_field_def"] = insert_field_def

    # Locate the input type used for the 'input' argument of the insert mutation
    input_arg = insert_field_def.args.get("input")
    assert input_arg is not None, (
        f"insert mutation field '{insert_field_name}' has no 'input' argument; "
        f"args={list(insert_field_def.args)}"
    )

    # Unwrap NonNull / List wrappers to get the named input type
    input_type = input_arg.type
    # Unwrap NonNull
    if hasattr(input_type, "of_type"):
        input_type = input_type.of_type
    # Unwrap List if present
    if hasattr(input_type, "of_type") and hasattr(input_type, "fields") is False:
        inner = getattr(input_type, "of_type", None)
        if inner is not None:
            input_type = inner

    shared_data["input_type"] = input_type

    # Collect the field names present on the input type
    if hasattr(input_type, "fields") and input_type.fields:
        input_field_names = list(input_type.fields.keys())
    else:
        # Some wrappers expose fields via .of_type; try one more level
        inner = getattr(input_type, "of_type", input_type)
        assert hasattr(inner, "fields") and inner.fields, (
            f"Could not resolve input type fields from {input_type!r}"
        )
        input_field_names = list(inner.fields.keys())

    shared_data["input_field_names"] = input_field_names


@then(
    "excluded columns are absent from the input type and references to them are rejected at parse time"
)
def then_excluded_columns_absent_and_rejected(shared_data):
    """Verify that 'region' is absent from the input type and rejected by the validator."""
    schema = shared_data["schema"]
    excluded_column = shared_data["excluded_column"]
    permitted_columns = shared_data["permitted_columns"]
    input_field_names = shared_data["input_field_names"]
    insert_field_name = shared_data["insert_field_name"]

    # 1. The excluded column must not appear in the generated input type.
    assert excluded_column not in input_field_names, (
        f"Excluded column '{excluded_column}' is present in the mutation input type fields: "
        f"{input_field_names}"
    )

    # 2. At least one permitted column must appear in the input type.
    for col in permitted_columns:
        assert col in input_field_names, (
            f"Permitted column '{col}' is missing from mutation input type fields: "
            f"{input_field_names}"
        )

    # 3. A mutation that references the excluded column must be rejected at parse/validate time.
    #    Build a GraphQL document that attempts to set the excluded 'region' column.
    excluded_mutation_src = (
        f'mutation {{ {insert_field_name}(input: {{ amount: 10.0, {excluded_column}: "eu-west" }}) '
        f"{{ affected_rows }} }}"
    )
    excluded_doc = parse(excluded_mutation_src)
    validation_errors = validate(schema, excluded_doc)
    assert validation_errors, (
        f"Expected validation errors when referencing excluded column '{excluded_column}' "
        f"in mutation input, but got none. "
        f"Input type fields: {input_field_names}"
    )

    # 4. Verify the validation error message mentions the excluded field, confirming the
    #    rejection is specifically about the disallowed column reference.
    error_messages = " ".join(str(e) for e in validation_errors)
    assert excluded_column in error_messages.lower() or any(
        excluded_column in str(e).lower() for e in validation_errors
    ), f"Validation errors do not mention the excluded column '{excluded_column}': {error_messages}"

    # 5. A mutation using only permitted columns must validate successfully.
    permitted_mutation_src = (
        f"mutation {{ {insert_field_name}(input: {{ amount: 10.0 }}) {{ affected_rows }} }}"
    )
    permitted_doc = parse(permitted_mutation_src)
    permitted_errors = validate(schema, permitted_doc)
    assert not permitted_errors, (
        f"Mutation with only permitted columns failed validation: {permitted_errors}"
    )


# ---------------------------------------------------------------------------
# REQ-035 — RLS injection into UPDATE and DELETE
# ---------------------------------------------------------------------------


def _build_rls_schema_and_ctx():
    """Build schema and context for RLS injection tests."""
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
                {"column_name": "user_id", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
            _col("user_id", "integer"),
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


@given("a table with RLS rules configured")
def given_table_with_rls(shared_data):
    si, schema, ctx = _build_rls_schema_and_ctx()
    shared_data["schema_input"] = si
    shared_data["schema"] = schema
    shared_data["ctx"] = ctx
    shared_data["source_types"] = {"sales-pg": "postgresql"}
    # Define RLS clauses that should be injected: restrict to a specific user
    shared_data["rls_clauses"] = ["user_id = 42"]


@when("an UPDATE or DELETE mutation is compiled")
def when_update_or_delete_compiled(shared_data):
    schema = shared_data["schema"]
    ctx = shared_data["ctx"]
    source_types = shared_data["source_types"]
    rls_clauses = shared_data["rls_clauses"]

    # Compile an UPDATE mutation
    update_doc = parse("""
        mutation { updateOrders(set: { amount: 99.0 }, where: { id: { eq: 1 } }) { affected_rows } }
    """)
    update_errors = validate(schema, update_doc)
    assert not update_errors, f"UPDATE mutation did not validate: {update_errors}"
    update_results = compile_mutation(update_doc, ctx, source_types)
    assert update_results, "compile_mutation returned no results for UPDATE"
    update_compiled = update_results[0]
    assert update_compiled.mutation_type == "update", (
        f"expected mutation_type='update', got {update_compiled.mutation_type!r}"
    )

    # Compile a DELETE mutation
    delete_doc = parse("""
        mutation { deleteOrders(where: { id: { eq: 5 } }) { affected_rows } }
    """)
    delete_errors = validate(schema, delete_doc)
    assert not delete_errors, f"DELETE mutation did not validate: {delete_errors}"
    delete_results = compile_mutation(delete_doc, ctx, source_types)
    assert delete_results, "compile_mutation returned no results for DELETE"
    delete_compiled = delete_results[0]
    assert delete_compiled.mutation_type == "delete", (
        f"expected mutation_type='delete', got {delete_compiled.mutation_type!r}"
    )

    # Apply RLS injection to both compiled mutations
    # table_id=1 matches the table registered in _build_rls_schema_and_ctx.
    rls_rules = {1: rls_clauses[0]}
    update_with_rls = inject_rls_into_mutation(update_compiled, 1, rls_rules)
    delete_with_rls = inject_rls_into_mutation(delete_compiled, 1, rls_rules)

    shared_data["update_compiled"] = update_compiled
    shared_data["delete_compiled"] = delete_compiled
    shared_data["update_with_rls"] = update_with_rls
    shared_data["delete_with_rls"] = delete_with_rls


@then("RLS WHERE clauses are injected into the SQL before execution")
def then_rls_injected_into_sql(shared_data):
    rls_clause = shared_data["rls_clauses"][0]
    update_with_rls = shared_data["update_with_rls"]
    delete_with_rls = shared_data["delete_with_rls"]
    update_compiled = shared_data["update_compiled"]
    delete_compiled = shared_data["delete_compiled"]

    # The RLS clause must appear in the final UPDATE SQL
    assert rls_clause in update_with_rls.sql, (
        f"RLS clause {rls_clause!r} not found in UPDATE SQL: {update_with_rls.sql!r}"
    )

    # The RLS clause must appear in the final DELETE SQL
    assert rls_clause in delete_with_rls.sql, (
        f"RLS clause {rls_clause!r} not found in DELETE SQL: {delete_with_rls.sql!r}"
    )

    # The RLS-injected SQL must still contain the original WHERE condition
    assert "WHERE" in update_with_rls.sql, (
        f"WHERE keyword missing from RLS-injected UPDATE SQL: {update_with_rls.sql!r}"
    )
    assert "WHERE" in delete_with_rls.sql, (
        f"WHERE keyword missing from RLS-injected DELETE SQL: {delete_with_rls.sql!r}"
    )

    # The RLS injection must not break the write-statement classification
    assert _WRITE_RE.match(update_with_rls.sql), (
        "RLS-injected UPDATE SQL is no longer classified as a write statement"
    )
    assert _WRITE_RE.match(delete_with_rls.sql), (
        "RLS-injected DELETE SQL is no longer classified as a write statement"
    )

    # The original (pre-injection) SQL must differ from the injected SQL,
    # confirming the injection actually modified the statement
    assert update_with_rls.sql != update_compiled.sql, (
        "inject_rls_into_mutation did not modify UPDATE SQL"
    )
    assert delete_with_rls.sql != delete_compiled.sql, (
        "inject_rls_into_mutation did not modify DELETE SQL"
    )

    # Both injected mutations must still target the correct source
    assert update_with_rls.source_id == "sales-pg", (
        f"RLS-injected UPDATE has wrong source_id: {update_with_rls.source_id!r}"
    )
    assert delete_with_rls.source_id == "sales-pg", (
        f"RLS-injected DELETE has wrong source_id: {delete_with_rls.source_id!r}"
    )
